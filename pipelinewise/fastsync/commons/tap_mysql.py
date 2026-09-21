import csv
import datetime
import decimal
import glob
import logging
import os
import pymysql
import pymysql.cursors

from argparse import Namespace
from typing import Tuple, Dict, Callable
from pymysql import InterfaceError, OperationalError, Connection

from ...utils import safe_column_name
from . import split_gzip, utils
from .partial_sync_boundary import PartialSyncBoundary
from .source_transformations import (
    UnsupportedSourceTransformation,
    compile_source_select,
    requires_regex_support,
    validate_bookmark_column,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_CHARSET = 'utf8mb4'
DEFAULT_EXPORT_BATCH_ROWS = 50000
MARIADB_ENGINE = 'mariadb'
MYSQL_ENGINE = 'mysql'
DEFAULT_USE_GTID = False
DEFAULT_NET_WRITE_TIMEOUT_SQL = 'SET @@session.net_write_timeout=3600'
DEFAULT_SESSION_SQLS = [
    'SET @@session.time_zone="+0:00"',
    'SET @@session.wait_timeout=28800',
    'SET @@session.net_read_timeout=3600',
    DEFAULT_NET_WRITE_TIMEOUT_SQL,
    'SET @@session.innodb_lock_wait_timeout=3600',
]
MARIADB_MAX_STATEMENT_TIME_SQL = 'SET @@session.max_statement_time=0'
MYSQL_MAX_EXECUTION_TIME_SQL = 'SET @@session.max_execution_time=0'
_REPORTED_SESSION_ENGINE_SELECTIONS = set()


def resolve_source_engine(connection, configured_engine=None):
    """Return the configured engine or detect it from the server handshake."""
    if configured_engine is None:
        engine = MARIADB_ENGINE if 'mariadb' in connection.get_server_info().lower() else MYSQL_ENGINE
        engine_source = 'detected'
    else:
        engine = str(configured_engine).lower()
        engine_source = 'configured'

    selection = (engine, engine_source)
    if selection not in _REPORTED_SESSION_ENGINE_SELECTIONS:
        LOGGER.info('Using %s source engine (%s)', engine, engine_source)
        _REPORTED_SESSION_ENGINE_SELECTIONS.add(selection)
    else:
        LOGGER.debug('Using %s source engine (%s)', engine, engine_source)
    return engine


def default_session_sqls(engine):
    """Return default session SQL compatible with the resolved source engine."""
    session_sqls = list(DEFAULT_SESSION_SQLS)
    if engine == MARIADB_ENGINE:
        session_sqls.append(MARIADB_MAX_STATEMENT_TIME_SQL)
    elif engine == MYSQL_ENGINE:
        session_sqls.append(MYSQL_MAX_EXECUTION_TIME_SQL)
    return session_sqls


def _create_csv_writer(output):
    """Keep SQL NULL distinct from an empty string in Snowflake CSV loads."""
    return csv.writer(
        output,
        delimiter=',',
        quotechar='"',
        quoting=csv.QUOTE_NOTNULL,
    )


class FastSyncTapMySql:
    """
    Common functions for fastsync from a MySQL database
    """

    def __init__(self, connection_config: dict, tap_type_to_target_type: Callable, target_quote=None):
        self._configured_engine = connection_config.get('engine')
        self._resolved_engine = None
        self.connection_config = connection_config.copy()
        self.connection_config['charset'] = connection_config.get(
            'charset', DEFAULT_CHARSET
        )
        self.connection_config['export_batch_rows'] = connection_config.get(
            'export_batch_rows', DEFAULT_EXPORT_BATCH_ROWS
        )
        self.connection_config['session_sqls'] = connection_config.get(
            'session_sqls', []
        )
        self.connection_config['use_gtid'] = connection_config.get(
            'use_gtid', DEFAULT_USE_GTID
        )
        self.tap_type_to_target_type = tap_type_to_target_type
        self.target_quote = target_quote
        self.source_transformations = None
        self.target_iceberg_version = None
        self.conn = None
        self.conn_unbuffered = None
        self.is_replica = False
        self._mariadb_json_aliases_enabled = False
        self._source_regex_verified = False

    @property
    def is_mariadb(self) -> bool:
        """
        Property method, to find if the engine is mariadb or not
        Returns: bool
        """
        return self.source_engine == MARIADB_ENGINE

    @property
    def source_engine(self) -> str:
        """Return the resolved engine, with the legacy fallback before opening."""
        if self._resolved_engine is not None:
            return self._resolved_engine
        if self._configured_engine is not None:
            return str(self._configured_engine).lower()
        return MYSQL_ENGINE

    @property
    def uses_mariadb_json_aliases(self) -> bool:
        """Return whether MariaDB JSON aliases should map to Iceberg VARIANT."""
        iceberg_version = self.connection_config.get('iceberg_version')
        return (
            self.is_mariadb
            and (
                self._mariadb_json_aliases_enabled
                or (
                    self.connection_config.get('target_table_format') == 'iceberg'
                    and isinstance(iceberg_version, int)
                    and iceberg_version == 3
                )
            )
        )

    def set_mariadb_json_aliases_enabled(self, enabled: bool) -> None:
        """Set JSON-alias mapping from the route's validated target format."""
        self._mariadb_json_aliases_enabled = bool(enabled)

    def get_connection_parameters(self, prioritize_primary: bool = False) -> Tuple[dict, bool]:
        """
        Method to get connection parameters
        Connection is either to the primary or a replica if its credentials are given

        Args: prioritize_primary (bool): Flag to force getting the primary's connection details even if replica have
        been provided

        Returns:
            Tuple of Dict with credentials and flag of whether the connection is to replica or not.
        """

        is_replica = False

        if prioritize_primary:
            host = self.connection_config['host']
            port = int(self.connection_config['port'])
            user = self.connection_config['user']
            password = self.connection_config['password']
        else:
            if 'replica_host' in self.connection_config:
                is_replica = True

            host = self.connection_config.get('replica_host', self.connection_config['host'])
            port = int(self.connection_config.get('replica_port', self.connection_config['port']))
            user = self.connection_config.get('replica_user', self.connection_config['user'])
            password = self.connection_config.get('replica_password', self.connection_config['password'])

        charset = self.connection_config['charset']

        return ({
                    'host': host,
                    'port': port,
                    'user': user,
                    'password': password,
                    'charset': charset,
                }, is_replica)

    def open_connections(self):
        """
        Open connection to primary or replica depending on the config
        This sets the instance attributes "conn" and "conn_unbuffered"
        """

        # Fastsync is using replica_{host|port|user|password} values from the config by default
        # to avoid making heavy load on the primary source database when syncing large tables
        #
        # If replica_{host|port|user|password} values are not defined in the config then it's
        # using the normal credentials to connect

        conn_params, is_replica = self.get_connection_parameters()

        self.is_replica = is_replica

        self.conn: Connection = pymysql.connect(
            **conn_params,
            cursorclass=pymysql.cursors.DictCursor,
            ssl={'': True}
        )
        if self._resolved_engine is None:
            self._resolved_engine = resolve_source_engine(
                self.conn, self._configured_engine
            )
            self.connection_config['engine'] = self._resolved_engine
        self.conn_unbuffered: Connection = pymysql.connect(
            **conn_params,
            cursorclass=pymysql.cursors.SSCursor,
            ssl={'': True}
        )
        self._source_regex_verified = False

        # Set session variables by running a list of SQLs which is defined
        # in the optional session_sqls connection parameters
        self.run_session_sqls()

    def run_session_sqls(self):
        """
        Run list of SQLs from the "session_sqls" optional connection parameter
        """
        configured_session_sqls = self.connection_config.get('session_sqls')
        session_sqls = [
            (sql, sql in (MARIADB_MAX_STATEMENT_TIME_SQL, MYSQL_MAX_EXECUTION_TIME_SQL))
            for sql in default_session_sqls(self.source_engine)
        ]
        session_sqls.extend((sql, False) for sql in (
            configured_session_sqls if isinstance(configured_session_sqls, list) else []
        ))

        warnings = []
        for sql, optional_timeout in session_sqls:
            for conn in (self.conn, self.conn_unbuffered):
                try:
                    self._run_session_sql(conn, sql)
                except pymysql.err.OperationalError as exc:
                    if not optional_timeout or not exc.args or exc.args[0] != 1193:
                        raise
                    warnings.append(
                        f'Built-in timeout not applied: {sql}; server does not support this variable. '
                        'Check the configured source engine.'
                    )
                except pymysql.err.InternalError:
                    warnings.append(f'Could not set session variable: {sql}')

        if warnings:
            LOGGER.warning(
                'Encountered non-fatal errors when configuring session that could impact performance:'
            )
        for warning in warnings:
            LOGGER.warning(warning)

    @staticmethod
    def _run_session_sql(conn, sql):
        """Initialize directly: query reconnects would recursively initialize sessions."""
        with conn.cursor() as cursor:
            cursor.execute(sql)

    def close_connections(self, silent=False):
        """
        Close and clear both buffered and unbuffered connections.
        """
        connections = (
            ('buffered', self.conn),
            ('unbuffered', self.conn_unbuffered),
        )
        self.conn = None
        self.conn_unbuffered = None

        for connection_name, connection in connections:
            if connection is None:
                continue
            try:
                connection.close()
            except Exception as exc:
                if not silent:
                    LOGGER.exception(exc)
                    LOGGER.info('%s connection seems to be already closed.', connection_name.capitalize())

    def query(self, query, conn=None, params=None, return_as_cursor=False, n_retry=1):
        """
        Run query
        """
        LOGGER.info('Running query: %s', query)
        if conn is None:
            conn = self.conn

        try:
            with conn.cursor() as cur:
                cur.execute(query, params)

                if return_as_cursor:
                    return cur

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []
        except (InterfaceError, OperationalError) as exc:
            LOGGER.exception(
                'Exception happened during running a query. Number of retries: %s. %s',
                n_retry,
                exc,
            )
            if n_retry > 0:
                LOGGER.info('Reopening the connections.')
                self.close_connections(silent=True)
                self.open_connections()
                LOGGER.info('Retrying to run a query.')
                return self.query(
                    query,
                    params=params,
                    return_as_cursor=return_as_cursor,
                    n_retry=n_retry - 1,
                )

            raise exc

    def fetch_current_log_pos(self) -> Dict:
        """
        Get the actual binlog position in MySQL

        Returns: log coordinates in a dictionary, could be GTID pos or binlog file and pos.
        """
        if self.connection_config['use_gtid']:
            return self._get_current_gtid_pos()

        return self._get_binlog_coordinates()

    def _get_current_gtid_pos(self) -> Dict:
        """
        Get the current GTID position in server

        Raises:
            Exception: if GTID is not found on the server
        Returns: Dict with GTID position
        Examples:
            {
                "gtid": "0-1774983-23",
                "gtid_complete": True
            }
        """
        if self.is_mariadb:
            bookmark = self.__find_mariadb_gtid_pos()
        else:
            bookmark = self.__find_mysql_gtid_pos()
        return {**bookmark, 'gtid_complete': True}

    def _query_binlog_status(self, statement: str, legacy_statement: str):
        """Use current MySQL syntax, falling back only when it is unsupported."""
        if self.is_mariadb:
            return self.query(legacy_statement)
        try:
            return self.query(statement)
        except pymysql.err.ProgrammingError as exc:
            if exc.args[0] != 1064:
                raise
            return self.query(legacy_statement)

    def _get_binlog_coordinates(self) -> Dict:
        """
        Get the actual binlog file and position in server

        Raises:
            Exception: if binlog is not enabled
        Returns: a dict with binlog coordinate and version
        Examples:
             {
                "log_file": "binlog.00001",
                "log_pos": 334,
                "version": 1,
             }
        """
        if self.is_replica:
            LOGGER.debug('Connecting to replica to get binlog coordinates...')
            result = self._query_binlog_status('SHOW REPLICA STATUS', 'SHOW SLAVE STATUS')
            if len(result) == 0:
                raise Exception('MySQL binary logging is not enabled.')
            if len(result) != 1:
                raise Exception('FastSync requires a replica with a single replication channel.')
            binlog_pos = result[0]
            # Received events may still be absent from the replica snapshot.
            # Resume Singer from the applied coordinates to replay that gap.
            log_file = binlog_pos.get('Relay_Source_Log_File', binlog_pos.get('Relay_Master_Log_File'))
            log_pos = binlog_pos.get('Exec_Source_Log_Pos', binlog_pos.get('Exec_Master_Log_Pos'))
            if not log_file or not log_pos:
                raise Exception('MySQL replica has no applied binary log coordinates.')
            version = binlog_pos.get('version', 1)

        else:
            LOGGER.debug('Connecting to primary to get binlog coordinates...')
            result = self._query_binlog_status('SHOW BINARY LOG STATUS', 'SHOW MASTER STATUS')
            if len(result) == 0:
                raise Exception('MySQL binary logging is not enabled.')
            binlog_pos = result[0]
            log_file = binlog_pos.get('File')
            log_pos = binlog_pos.get('Position')
            version = binlog_pos.get('version', 1)

        return {
            'log_file': log_file,
            'log_pos': log_pos,
            'version': version,
        }

    def fetch_current_incremental_key_pos(self, table, replication_key):
        """
        Get the actual incremental key position in the table
        """
        validate_bookmark_column(table, replication_key, self.source_transformations)
        result = self.query(
            f'SELECT MAX({replication_key}) AS key_value FROM {table}'
        )
        if not result:
            raise Exception(
                f'Cannot get replication key value for table: {table}'
            )

        mysql_key_value = result[0].get('key_value')
        key_value = mysql_key_value

        # Convert msyql data/datetime format to JSON friendly values
        if isinstance(mysql_key_value, datetime.datetime):
            key_value = mysql_key_value.isoformat()

        elif isinstance(mysql_key_value, datetime.date):
            key_value = mysql_key_value.isoformat() + 'T00:00:00'

        elif isinstance(mysql_key_value, decimal.Decimal):
            key_value = float(mysql_key_value)

        return {
            'replication_key': replication_key,
            'replication_key_value': key_value,
            'version': 1,
        }

    def get_primary_keys(self, table_name):
        """
        Get the primary key of a table
        """
        table_dict = utils.tablename_to_dict(table_name)
        sql = f"SHOW KEYS FROM `{table_dict['schema_name']}`.`{table_dict['table_name']}` WHERE Key_name = 'PRIMARY'"

        pk_specs = self.query(sql)
        if len(pk_specs) > 0:
            return [
                safe_column_name(k.get('Column_name'), self.target_quote)
                for k in pk_specs
            ]

        return None

    def get_table_columns(self, table_name, max_num=None, date_type='date', *, metadata_query=None):
        """
        Get MySQL table column details from information_schema
        """
        table_dict = utils.tablename_to_dict(table_name)

        if max_num:
            decimals = len(max_num.split('.')[1]) if '.' in max_num else 0
            decimal_format = f"""
              CONCAT('GREATEST(LEAST({max_num}, ROUND(`', column_name, '`, {decimals})), -{max_num})')
            """
            integer_format = """
              CONCAT('`', column_name, '`')
            """
        else:
            decimal_format = """
              CONCAT('`', column_name, '`')
            """
            integer_format = decimal_format

        schema_name = table_dict.get('schema_name')
        table_name = table_dict.get('table_name')

        data_type_projection = 'data_type'
        json_alias_projection = ''
        columns_relation = 'information_schema.columns'
        if self.uses_mariadb_json_aliases:
            data_type_projection = (
                "CASE WHEN is_json_alias THEN 'json' ELSE data_type END"
            )
            json_alias_projection = """,
                            (c.data_type = 'longtext' AND EXISTS (
                                SELECT 1
                                FROM information_schema.table_constraints AS tc
                                JOIN information_schema.check_constraints AS cc
                                  ON cc.constraint_schema = tc.constraint_schema
                                 AND cc.constraint_name = tc.constraint_name
                                WHERE tc.table_schema = c.table_schema
                                  AND tc.table_name = c.table_name
                                  AND tc.constraint_type = 'CHECK'
                                  AND REPLACE(LOWER(cc.check_clause), ' ', '') =
                                      CONCAT('json_valid(`',
                                             REPLACE(LOWER(c.column_name), '`', '``'),
                                             '`)')
                            )) AS is_json_alias"""
            columns_relation += ' AS c'

        # Keep the legacy NUL removal separate from CSV quoting; every other
        # text character must reach csv.writer unchanged.

        sql = f"""
                SELECT column_name AS column_name,
                    {data_type_projection} AS data_type,
                    column_type AS column_type,
                    safe_sql_value AS safe_sql_value
                FROM (SELECT column_name,
                            data_type,
                            column_type,
                            CASE
                            WHEN data_type IN ('blob', 'tinyblob', 'mediumblob', 'longblob')
                                    THEN CONCAT('REPLACE(hex(`', column_name, '`)', ", '\n', ' ')")
                            WHEN data_type IN ('binary', 'varbinary')
                                    THEN concat('REPLACE(REPLACE(hex(`',COLUMN_NAME,'`)', ", '\n', ' '), '\r', '')")
                            WHEN data_type IN ('bit')
                                    THEN concat('cast(`', column_name, '` AS unsigned)')
                            WHEN data_type IN ('date')
                                    THEN concat('CASE WHEN YEAR(`', column_name, '`) = 0 OR MONTH(`', column_name, '`) NOT BETWEEN 1 AND 12 OR DAY(`', column_name, '`) = 0 OR DAY(`', column_name, '`) > DAY(LAST_DAY(DATE_FORMAT(`', column_name, '`, "%Y-%m-01"))) THEN NULL ELSE CAST(`', column_name, '` AS {date_type}) END')
                            WHEN data_type IN ('datetime', 'timestamp')
                                    THEN concat('CASE WHEN YEAR(`', column_name, '`) = 0 OR MONTH(`', column_name, '`) NOT BETWEEN 1 AND 12 OR DAY(`', column_name, '`) = 0 OR DAY(`', column_name, '`) > DAY(LAST_DAY(DATE_FORMAT(`', column_name, '`, "%Y-%m-01"))) THEN NULL ELSE `', column_name, '` END')
                            WHEN LOWER(column_type) REGEXP '^tinyint[(]1[)]( unsigned)?( zerofill)?$'
                                    THEN concat('CASE WHEN `' , column_name , '` is null THEN null WHEN `' , column_name , '` = 0 THEN 0 ELSE 1 END')
                            WHEN column_type IN ('geometry', 'point', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'multipolygon', 'geometrycollection', 'geomcollection')
                                    THEN concat('ST_AsGeoJSON(', column_name, ')')
                            WHEN column_name = 'raw_data_hash'
                                    THEN concat('REPLACE(REPLACE(hex(`', column_name, '`)', ", '\n', ' '), '\r', '')")
                            WHEN data_type IN ('double', 'numeric', 'float', 'decimal', 'real')
                                    THEN {decimal_format}
                            WHEN data_type IN ('smallint', 'integer', 'bigint', 'mediumint', 'int')
                                    THEN {integer_format}
                            ELSE concat('REPLACE(cast(`', column_name, '` AS char CHARACTER SET utf8mb4)', ", CHAR(0), '')")
                                END AS safe_sql_value{json_alias_projection},
                            ordinal_position
                    FROM {columns_relation}
                    WHERE table_schema = %s
                        AND table_name = %s) x
                ORDER BY
                        ordinal_position
            """  # noqa: E501

        # DATE_FORMAT tokens are literals, not PyMySQL parameter placeholders.
        sql = sql.replace('%Y-%m-01', '%%Y-%%m-01')
        query = self.query if metadata_query is None else metadata_query
        return query(sql, params=(schema_name, table_name))

    def map_table_columns(self, columns):
        """Map already-read metadata without connections or primary-key queries."""
        return [
            '{} {}'.format(
                safe_column_name(column.get('column_name'), self.target_quote),
                self.tap_type_to_target_type(column.get('data_type'), column.get('column_type')),
            )
            for column in columns
        ]

    def map_column_types_to_target(self, table_name):
        """
        Map MySQL column types to equivalent types in target
        """
        mysql_columns = self.get_table_columns(table_name)
        return {
            'columns': self.map_table_columns(mysql_columns),
            'primary_key': self.get_primary_keys(table_name),
            'source_column_names': [
                column.get('column_name') for column in mysql_columns
            ],
        }

    def copy_table(
            self,
            table_name,
            path,
            max_num=None,
            date_type='date',
            split_large_files=False,
            split_file_chunk_size_mb=1000,
            split_file_max_chunks=20,
            compress=True,
            boundary=None,
    ):
        """
        Export data from table to a zipped csv
        Args:
            table_name: Fully qualified table name to export
            path: Path where to create the zip file(s) with the exported data
            split_large_files: Split large files to multiple pieces and create multiple zip files
                               with -partXYZ postfix in the filename. (Default: False)
            split_file_chunk_size_mb: File chunk sizes if `split_large_files` enabled. (Default: 1000)
            split_file_max_chunks: Max number of chunks if `split_large_files` enabled. (Default: 20)
        """
        table_columns = self.get_table_columns(table_name, max_num, date_type)
        column_safe_sql_values = [c.get('safe_sql_value') for c in table_columns]

        # If self.get_table_columns returns zero row then table not exist
        if len(column_safe_sql_values) == 0:
            raise Exception('{} table not found.'.format(table_name))

        source_boundary = (
            boundary.source_sql(
                'mysql',
                [column.get('column_name') for column in table_columns],
            )
            if boundary is not None
            else None
        )

        table_dict = utils.tablename_to_dict(table_name)

        metadata_columns = [
            "CONVERT_TZ( NOW(),@@session.time_zone,'+00:00') AS `_SDC_EXTRACTED_AT`",
            "CONVERT_TZ( NOW(),@@session.time_zone,'+00:00') AS `_SDC_BATCHED_AT`",
            'null AS `_SDC_DELETED_AT`'
        ]
        column_safe_sql_values += metadata_columns

        sql_template = """SELECT {}
        FROM `{}`.`{}` {}
        """
        export_batch_rows = self.connection_config['export_batch_rows']
        exported_rows = 0
        with self.conn_unbuffered.cursor() as cur:
            where_clause = (
                cur.mogrify(
                    source_boundary.statement,
                    source_boundary.parameters,
                )
                if source_boundary is not None
                else ''
            )
            sql = sql_template.format(
                ','.join(column_safe_sql_values),
                table_dict['schema_name'],
                table_dict['table_name'],
                where_clause,
            )
            transformed = self._compile_source_projection(table_name, table_columns, where_clause)
            if transformed is not None:
                sql = f'SELECT _ppw_export.*, {",".join(metadata_columns)} FROM ({transformed}) AS _ppw_export'
            cur.execute(sql)
            gzip_splitter = split_gzip.open(
                path,
                mode='wt',
                chunk_size_mb=split_file_chunk_size_mb,
                max_chunks=split_file_max_chunks if split_large_files else 0,
                compress=compress,
            )

            with gzip_splitter as split_gzip_files:
                writer = _create_csv_writer(split_gzip_files)

                while True:
                    rows = cur.fetchmany(export_batch_rows)

                    # No more rows to fetch, stop loop
                    if not rows:
                        break

                    # Log export status
                    exported_rows += len(rows)
                    if len(rows) == export_batch_rows:
                        # Then we believe this to be just an interim batch and not the final one so report on progress

                        LOGGER.info(
                            'Exporting batch from %s to %s rows from %s...',
                            (exported_rows - export_batch_rows),
                            exported_rows,
                            table_name,
                        )
                    # Write rows to file in one go
                    writer.writerows(rows)

                LOGGER.info(
                    'Exported total of %s rows from %s...', exported_rows, table_name
                )

    def _compile_source_projection(self, table_name, table_columns, where_clause=''):
        """Share projection validation between recovery preflight and export."""
        if self.source_transformations is None:
            return None
        columns = [dict(column, target_type=self.tap_type_to_target_type(
            column['data_type'], column['column_type']
        )) for column in table_columns]
        table_dict = utils.tablename_to_dict(table_name)
        table_reference = '.'.join(
            '`' + table_dict[key].replace('`', '``') + '`'
            for key in ('schema_name', 'table_name')
        )
        projection = compile_source_select(
            table_name, table_reference, where_clause, columns,
            self.source_transformations, 'mariadb' if self.is_mariadb else 'mysql', self.target_iceberg_version,
        )
        if requires_regex_support(table_name, self.source_transformations):
            self._validate_source_regex_support()
        return projection

    def _validate_source_regex_support(self):
        """Check the export connection's regex engine without reading source rows."""
        if self._source_regex_verified:
            return
        message = (
            'Source regex conditions require MySQL ICU or MariaDB PCRE support. '
            'The regex capability check failed; use a supported source server before retrying.'
        )
        try:
            with self.conn_unbuffered.cursor() as cursor:
                cursor.execute(
                    'SELECT CONVERT(%s USING utf8mb4) COLLATE utf8mb4_bin '
                    'REGEXP CONVERT(%s USING utf8mb4)',
                    ('a', r'(?-x)\A(?:a)\z'),
                )
                result = cursor.fetchone()
        except pymysql.MySQLError as exc:
            if not exc.args or exc.args[0] not in (1064, 1139, 1305):
                raise
            raise UnsupportedSourceTransformation(message) from exc
        if result != (1,):
            raise UnsupportedSourceTransformation(message)
        self._source_regex_verified = True

    def validate_source_transformations(self, table_name):
        """Reject invalid rules before binding a new Iceberg recovery attempt."""
        if self.source_transformations is not None:
            self._compile_source_projection(table_name, self.get_table_columns(table_name))

    def export_source_table_data(
            self, args: Namespace, tap_id: str,
            boundary: PartialSyncBoundary = None) -> list:
        """Export source table data"""
        filename = utils.gen_export_filename(tap_id=tap_id, table=args.table, sync_type='partialsync')
        filepath = os.path.join(args.temp_dir, filename)

        # Exporting table data

        self.copy_table(
            args.table,
            filepath,
            split_large_files=args.target.get('split_large_files'),
            split_file_chunk_size_mb=args.target.get('split_file_chunk_size_mb'),
            split_file_max_chunks=args.target.get('split_file_max_chunks'),
            boundary=boundary,
        )
        file_parts = glob.glob(f'{filepath}*')
        return file_parts

    def __find_mariadb_gtid_pos(self) -> Dict[str, str]:
        """
        Finds the current GTID pos in mariadb
        Returns: Dict with gtid key
        Raises: Exception if GTID is not enabled or not found
        """
        if self.is_replica:
            LOGGER.info('Connecting to replica to get gtid...')
            result = self.query('select @@gtid_slave_pos as current_gtids;')

        else:
            LOGGER.info('Connecting to primary to get gtid...')
            result = self.query('select @@gtid_current_pos as current_gtids;')

        if not result or not result[0].get('current_gtids'):
            raise Exception('GTID is not enabled.')

        gtids = result[0]['current_gtids']
        LOGGER.info('Using GTID(s) %s for state bookmark', gtids)
        return {'gtid': gtids}

    def __find_mysql_gtid_pos(self) -> Dict[str, str]:
        """
        Find all executed MySQL GTIDs, including previous primaries and gaps.
        Returns: Dict with gtid key
        Raises: Exception if GTID is not enabled or not found
       """

        result = self.query('select @@gtid_mode as gtid_mode;')

        if not result or result[0].get('gtid_mode') != 'ON':
            raise Exception('GTID mode is not enabled.')

        result = self.query('select @@GLOBAL.gtid_executed as current_gtids;')

        if not result or not result[0].get('current_gtids'):
            raise Exception('No GTID was found with "@@GLOBAL.gtid_executed".')

        gtids = result[0]['current_gtids']
        LOGGER.info('Using GTID(s) %s for state bookmark', gtids)
        return {'gtid': gtids}
