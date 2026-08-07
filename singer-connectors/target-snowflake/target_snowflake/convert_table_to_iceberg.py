import re
from typing import Dict, List, Tuple, Union

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from singer import get_logger


ICEBERG_DATA_TYPES = {
    'TEXT': 'VARCHAR',
    'TIMESTAMP_TZ': 'TIMESTAMP_LTZ(6)',
    'TIMESTAMP_LTZ': 'TIMESTAMP_LTZ(6)',
    'TIMESTAMP_NTZ': 'TIMESTAMP_NTZ(6)',
    'VARIANT': 'TEXT',
}

IDENTIFIER_PATTERN = r'\s*(?:"((?:""|[^"])+)"|([A-Za-z_][A-Za-z0-9_$]*))\s*'
FQTN_PATTERN = re.compile(rf'^{IDENTIFIER_PATTERN}\.{IDENTIFIER_PATTERN}\.{IDENTIFIER_PATTERN}$')


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class CopyNativeToIceberg:
    """Copy an existing native Snowflake table into a managed Iceberg table."""

    def __init__(self, connection_config, fqtn=None, eventual='NATIVE'):
        """
        connection_config:      Snowflake connection details
        fqtn:                   Fully qualified table name to be converted
        """
        self.logger = get_logger('copy_copy_native_to_iceberg')
        self.logger.info('Initializing CopyNativeToIceberg for table: %s', fqtn)
        self.connection_config = connection_config
        self.fqtn = fqtn
        self.eventual = eventual

        if eventual not in ('NATIVE', 'ICEBERG'):
            raise ValueError('EVENTUAL type of fqtn must be NATIVE or ICEBERG')

        self.logger.warning(
            'Replication and all writes to %s must be stopped before conversion. '
            'Run this utility with a role that can read every row and unmasked value. '
            'The role must have CREATE ICEBERG TABLE on its schema. --eventual ICEBERG cutover, or recovery '
            'that renames a native table, also requires ownership of that native table. '
            'TIMESTAMP_TZ becomes TIMESTAMP_LTZ(6); TIMESTAMP_NTZ, TIMESTAMP_LTZ, and TIME are limited '
            'to microsecond precision; original timezone offsets are lost and VARIANT becomes TEXT. '
            'Reapply metadata not copied by this utility, including grants, policies, tags, comments, '
            'nullability, and defaults.',
            fqtn,
        )

        if self._recover_interrupted_conversion():
            return

        native_columns, iceberg_columns = self.get_columns()
        primary_key = self.get_pk()

        if eventual == 'NATIVE':
            self.logger.info('Creating an Iceberg companion table for %s', fqtn)
        else:
            self.logger.info('Creating a staged Iceberg replacement for %s', fqtn)

        self._drop_iceberg_staging()
        try:
            query = self.get_create_iceberg(iceberg_columns, primary_key)
            self.logger.info(query)
            result = self.query(query)
            self.logger.info(result)

            query = self.get_query_copy_to_iceberg(native_columns)
            self.logger.info(query)
            result = self.query(query)
            self.logger.info(result)
        except Exception:
            self._drop_staging_best_effort('staging load failure')
            raise

        if eventual == 'ICEBERG':
            self._promote_iceberg()

    def _conversion_state(self):
        database, schema_name, table_name = self.parse_fqtn(self.fqtn)
        schema_fqtn = '.'.join(self._quote_identifier(identifier) for identifier in (database, schema_name))
        rows = self.query(
            f'SHOW TABLES IN SCHEMA {schema_fqtn} '
            f'STARTS WITH {self._sql_string_literal(table_name)}'
        )
        native_names = set()
        iceberg_names = set()
        for row in rows:
            name = row.get('name', row.get('NAME'))
            is_iceberg = row.get('is_iceberg', row.get('IS_ICEBERG'))
            if name is None or is_iceberg is None:
                raise RuntimeError('SHOW TABLES did not return name and is_iceberg metadata')
            if is_iceberg is True or str(is_iceberg).upper() in ('Y', 'TRUE'):
                iceberg_names.add(name)
            else:
                native_names.add(name)

        return {
            'original_native': table_name in native_names,
            'native_backup': f'{table_name}_NATIVE' in native_names,
            'original_iceberg': table_name in iceberg_names,
            'iceberg_staging': f'{table_name}_ICEBERG' in iceberg_names,
        }

    def _conversion_state_best_effort(self, operation):
        try:
            return self._conversion_state()
        except Exception:
            self.logger.exception(
                'Failed to inspect table state after %s; preserving Iceberg staging and native backup',
                operation,
            )
            return None

    @staticmethod
    def _state_is(
        state,
        original_native=False,
        native_backup=False,
        original_iceberg=False,
        iceberg_staging=False,
    ):
        return state == {
            'original_native': original_native,
            'native_backup': native_backup,
            'original_iceberg': original_iceberg,
            'iceberg_staging': iceberg_staging,
        }

    def _manual_recovery_error(self):
        return RuntimeError(
            f'Cannot safely recover conversion state for {self.fqtn}; inspect the original, '
            '_NATIVE, and _ICEBERG tables before retrying'
        )

    def _recover_interrupted_conversion(self):
        state = self._conversion_state()
        if state['original_iceberg']:
            self.logger.info('Table %s is already Iceberg; no conversion is required', self.fqtn)
            return True

        if state['original_native']:
            if state['native_backup']:
                raise self._manual_recovery_error()
            return False

        if state['native_backup'] and state['iceberg_staging']:
            if self.eventual == 'ICEBERG':
                self._resume_iceberg_promotion()
            else:
                self._restore_native_for_retry()
            return True

        if state['native_backup'] and not state['iceberg_staging']:
            self._restore_native_for_retry()
            return False

        raise self._manual_recovery_error()

    def _resume_iceberg_promotion(self):
        promote_iceberg = (
            f'ALTER ICEBERG TABLE {self._qualified_name("_ICEBERG")} '
            f'RENAME TO {self._qualified_name()}'
        )
        self.logger.warning('Resuming a previously loaded Iceberg promotion for %s', self.fqtn)
        try:
            self.query(promote_iceberg)
        except Exception:
            state = self._conversion_state_best_effort('resumed Iceberg promotion')
            if state and state['original_iceberg']:
                self.logger.warning('Resumed Iceberg promotion committed despite a client error')
                return

            if self._state_is(state, native_backup=True, iceberg_staging=True):
                try:
                    self._restore_native_for_retry()
                except Exception:
                    self.logger.exception(
                        'Failed to restore native table after resumed Iceberg promotion; '
                        'preserving Iceberg staging and native backup'
                    )
                else:
                    self._drop_staging_best_effort(
                        'resumed Iceberg promotion failure after native restoration'
                    )
                raise

            self.logger.error('Resumed Iceberg promotion is incomplete; manual recovery is required')
            raise

    def _restore_native_for_retry(self):
        restore_native = f'ALTER TABLE {self._qualified_name("_NATIVE")} RENAME TO {self._qualified_name()}'
        self.logger.warning('Restoring the native table name for %s', self.fqtn)
        try:
            self.query(restore_native)
        except Exception:
            state = self._conversion_state_best_effort('startup native-name restoration')
            if (
                state
                and state['original_native']
                and not state['native_backup']
                and not state['original_iceberg']
            ):
                self.logger.warning('Native-name restoration committed despite a client error')
                return
            self.logger.error('Native-name restoration is incomplete; manual recovery is required')
            raise

    def _drop_iceberg_staging(self):
        query = f'DROP ICEBERG TABLE IF EXISTS {self._qualified_name("_ICEBERG")}'
        self.logger.info(query)
        result = self.query(query)
        self.logger.info(result)

    def _drop_staging_best_effort(self, operation):
        try:
            self._drop_iceberg_staging()
        except Exception:
            self.logger.exception('Failed to drop Iceberg staging table after %s', operation)

    def _promote_iceberg(self):
        native_table = self._qualified_name()
        native_backup = self._qualified_name('_NATIVE')
        iceberg_staging = self._qualified_name('_ICEBERG')

        rename_native = f'ALTER TABLE {native_table} RENAME TO {native_backup}'
        self.logger.info(rename_native)
        try:
            result = self.query(rename_native)
            self.logger.info(result)
        except Exception as rename_error:
            state = self._conversion_state_best_effort('native rename')
            rename_rejected = isinstance(rename_error, snowflake.connector.errors.ProgrammingError)
            if rename_rejected and self._state_is(
                state,
                original_native=True,
                iceberg_staging=True,
            ):
                self._drop_staging_best_effort('native rename failure')
                raise
            if self._state_is(state, native_backup=True, iceberg_staging=True):
                self.logger.warning('Native rename committed despite a client error; continuing Iceberg promotion')
            else:
                self.logger.error(
                    'Native rename outcome is inconclusive; preserving Iceberg staging and native backup'
                )
                raise

        promote_iceberg = f'ALTER ICEBERG TABLE {iceberg_staging} RENAME TO {native_table}'
        self.logger.info(promote_iceberg)
        try:
            result = self.query(promote_iceberg)
            self.logger.info(result)
        except Exception:
            state = self._conversion_state_best_effort('Iceberg promotion')
            if state and state['original_iceberg']:
                self.logger.warning('Iceberg promotion committed despite a client error')
                return

            if self._state_is(state, original_native=True, iceberg_staging=True):
                self._drop_staging_best_effort('Iceberg promotion failure after native restoration')
                raise

            if not self._state_is(state, native_backup=True, iceberg_staging=True):
                self.logger.error(
                    'Iceberg promotion outcome is inconclusive; preserving Iceberg staging and native backup'
                )
                raise

            restore_native = f'ALTER TABLE {native_backup} RENAME TO {native_table}'
            try:
                self.query(restore_native)
            except Exception:
                recovery_state = self._conversion_state_best_effort('native-name restoration')
                if self._state_is(
                    recovery_state,
                    original_native=True,
                    iceberg_staging=True,
                ):
                    self._drop_staging_best_effort(
                        'Iceberg promotion failure after native restoration'
                    )
                else:
                    self.logger.exception(
                        'Failed to restore native table %s after Iceberg promotion failed; '
                        'preserving Iceberg staging and native backup',
                        native_table,
                    )
            else:
                self._drop_staging_best_effort('Iceberg promotion failure after native restoration')
            raise

    def check_iceberg(self) -> bool:
        """Return whether the exact source table is already an Iceberg table."""
        database, schema_name, table_name = self.parse_fqtn(self.fqtn)
        schema_fqtn = '.'.join(self._quote_identifier(identifier) for identifier in (database, schema_name))

        self.logger.info('Checking if table %s is an Iceberg table', self.fqtn)
        results = self.query(
            f'SHOW TERSE ICEBERG TABLES LIKE {self._sql_string_literal(table_name)} IN SCHEMA {schema_fqtn}'
        )
        return any(row.get('name', row.get('NAME')) == table_name for row in results)

    def parse_fqtn(self, fqtn: str) -> Tuple[str, str, str]:
        """Parse a Snowflake FQTN into its resolved identifier names."""
        if not isinstance(fqtn, str) or not fqtn.strip():
            raise ValueError('FQTN must be a non-empty string')

        match = FQTN_PATTERN.fullmatch(fqtn)
        if not match:
            raise ValueError(
                f"Invalid FQTN format: '{fqtn}'. "
                'Expected format: database.schema.table with each identifier optionally double-quoted'
            )

        identifiers = []
        matched_groups = match.groups()
        for index in range(0, len(matched_groups), 2):
            quoted_identifier, unquoted_identifier = matched_groups[index:index + 2]
            if quoted_identifier is not None:
                identifiers.append(quoted_identifier.replace('""', '"'))
            else:
                identifiers.append(unquoted_identifier.upper())

        return identifiers[0], identifiers[1], identifiers[2]

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _sql_string_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _qualified_name(self, table_suffix: str = '') -> str:
        database, schema_name, table_name = self.parse_fqtn(self.fqtn)
        identifiers = (database, schema_name, f'{table_name}{table_suffix}')
        return '.'.join(self._quote_identifier(identifier) for identifier in identifiers)

    def get_columns(self):
        """Return independent native and Iceberg-compatible column metadata."""
        database, schema_name, table_name = self.parse_fqtn(self.fqtn)
        query = (
            'SELECT "COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE", '
            '"DATETIME_PRECISION" '
            f'FROM {self._quote_identifier(database)}."INFORMATION_SCHEMA"."COLUMNS" '
            f'WHERE "TABLE_SCHEMA" = {self._sql_string_literal(schema_name)} '
            f'AND "TABLE_NAME" = {self._sql_string_literal(table_name)} '
            'ORDER BY "ORDINAL_POSITION"'
        )

        native_columns = self.query(query)
        iceberg_columns = []
        for native_column in native_columns:
            iceberg_column = native_column.copy()
            iceberg_column['DATA_TYPE'] = self._iceberg_data_type(native_column)
            iceberg_columns.append(iceberg_column)

        return native_columns, iceberg_columns

    @staticmethod
    def _iceberg_data_type(native_column):
        if native_column['DATA_TYPE'] == 'NUMBER':
            precision = native_column.get('NUMERIC_PRECISION')
            scale = native_column.get('NUMERIC_SCALE')
            return f'NUMBER({38 if precision is None else precision},{0 if scale is None else scale})'

        if native_column['DATA_TYPE'] == 'TIME':
            return 'TIME(6)'

        return ICEBERG_DATA_TYPES.get(native_column['DATA_TYPE'], native_column['DATA_TYPE'])

    def get_pk(self):
        """Return primary-key columns in Snowflake key sequence order."""
        queries = [
            f'SHOW PRIMARY KEYS IN TABLE {self._qualified_name()};',
            'SELECT "column_name" AS "COLUMN_NAME" FROM TABLE(RESULT_SCAN(-1)) ORDER BY "key_sequence";',
        ]
        return self.query(queries)

    def get_create_iceberg(self, columns, primary_key):
        """Generate CREATE ICEBERG TABLE SQL."""
        destination = self._qualified_name('_ICEBERG')

        column_defs = []
        for column in columns:
            column_name = self._quote_identifier(column['COLUMN_NAME'])
            column_defs.append(f"{column_name} {column['DATA_TYPE']}")

        statement = f'CREATE ICEBERG TABLE {destination} ({", ".join(column_defs)}'
        if primary_key:
            pk_columns = [self._quote_identifier(row['COLUMN_NAME']) for row in primary_key]
            statement += f', PRIMARY KEY ({", ".join(pk_columns)})'
        statement += ')'

        statement += ' DATA_RETENTION_TIME_IN_DAYS=1'
        statement += " TARGET_FILE_SIZE='16MB'"
        statement += ' ENABLE_DATA_COMPACTION=TRUE'
        return statement

    def get_query_copy_to_iceberg(self, native_columns):
        """Generate SQL that copies native data with required Iceberg casts."""
        select_columns = []
        for column in native_columns:
            column_name = self._quote_identifier(column['COLUMN_NAME'])
            if column['DATA_TYPE'] == 'TIMESTAMP_TZ':
                select_columns.append(f'TO_TIMESTAMP_LTZ({column_name}) AS {column_name}')
            elif column['DATA_TYPE'] == 'TIME':
                select_columns.append(f'CAST({column_name} AS TIME(6)) AS {column_name}')
            else:
                select_columns.append(column_name)

        destination = self._qualified_name('_ICEBERG')
        source = self._qualified_name()

        return f'INSERT INTO {destination} SELECT {", ".join(select_columns)} FROM {source}'

    def open_connection(self):
        """Open Snowflake connection."""
        return snowflake.connector.connect(
            user=self.connection_config['user'],
            authenticator='SNOWFLAKE_JWT',
            private_key=self._pem2der(self.connection_config['private_key']),
            account=self.connection_config['account'],
            database=self.connection_config['dbname'],
            warehouse=self.connection_config['warehouse'],
            role=self.connection_config.get('role'),
            autocommit=True,
            session_parameters={
                'QUOTED_IDENTIFIERS_IGNORE_CASE': 'FALSE',
                'QUERY_TAG': f'copy_native_to_iceberg: {self.fqtn}',
            },
        )

    def _pem2der(self, pem_file: str, password: str = None) -> bytes:
        """Convert Key PEM format to DER format."""
        with open(pem_file, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=password,
            )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def query(self, query: Union[str, List[str]]) -> List[Dict]:
        """Run an SQL query in Snowflake."""
        result = []

        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cursor:
                if isinstance(query, list):
                    self.logger.debug('Starting Transaction')
                    cursor.execute('START TRANSACTION')
                    queries = query
                else:
                    queries = [query]

                for sql in queries:
                    cursor.execute(sql)
                    result = cursor.fetchall()

        return result
