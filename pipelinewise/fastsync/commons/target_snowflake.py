import logging
import os
import json
import boto3
import snowflake.connector

from typing import List, Dict, Optional
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.storage_client import SnowflakeFileEncryptionMaterial

from . import utils
from .snowflake_sql_client import SnowflakeSqlClient
from .snowflake_types import SNOWFLAKE_MAX_VARCHAR
from .transform_utils import TransformationHelper, SQLFlavor
from pipelinewise.utils import pem2der

LOGGER = logging.getLogger(__name__)

# tone down snowflake connector logging level.
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)


# pylint: disable=missing-function-docstring,too-many-arguments
class FastSyncTargetSnowflake(SnowflakeSqlClient):  # pylint: disable=too-many-public-methods
    """
    Common functions for fastsync to Snowflake
    """

    # pylint: disable=invalid-name
    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config)
        self.transformation_config = transformation_config

        # Get the required parameters from config file and/or environment variables
        aws_profile = self.connection_config.get('aws_profile') or os.environ.get(
            'AWS_PROFILE'
        )
        aws_access_key_id = self.connection_config.get(
            'aws_access_key_id'
        ) or os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = self.connection_config.get(
            'aws_secret_access_key'
        ) or os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_session_token = self.connection_config.get(
            'aws_session_token'
        ) or os.environ.get('AWS_SESSION_TOKEN')

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )
        # AWS Profile based authentication
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)

        # Create the s3 client
        self.s3 = aws_session.client(
            's3',
            region_name=self.connection_config.get('s3_region_name'),
            endpoint_url=self.connection_config.get('s3_endpoint_url'),
        )

    def create_query_tag(self, query_tag_props: dict = None) -> str:
        query_tag = {
            'ppw_component': 'fastsync',
            'tap_id': self.connection_config.get('tap_id'),
            'database': self.connection_config['dbname'],
            'schema': None,
            'table': None,
        }
        if isinstance(query_tag_props, dict):
            for key in (
                'schema',
                'table',
                'load_id',
                'attempt_id',
                'phase',
                'publication_method',
                'target',
            ):
                if key in query_tag_props:
                    query_tag[key] = query_tag_props[key]

        return json.dumps(query_tag)

    sql_logger = LOGGER
    ignore_cleanup_errors = True

    def _connect(self, **kwargs):
        return snowflake.connector.connect(**kwargs)

    def _private_key(self):
        return pem2der(self.connection_config['private_key'])

    def _get_s3_key(self, file):
        """Return the deterministic staging key before an upload is attempted."""
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        return '{}{}'.format(s3_key_prefix, os.path.basename(file))

    def upload_to_s3(self, file, tmp_dir=None):
        bucket = self.connection_config['s3_bucket']
        s3_acl = self.connection_config.get('s3_acl')
        s3_key = self._get_s3_key(file)

        LOGGER.info(
            'Uploading to S3 bucket: %s, local file: %s, S3 key: %s',
            bucket,
            file,
            s3_key,
        )

        # Encrypt csv if client side encryption enabled
        master_key = self.connection_config.get('client_side_encryption_master_key', '')
        if master_key != '':
            # Encrypt the file
            LOGGER.info('Encrypting file %s...', file)
            encryption_material = SnowflakeFileEncryptionMaterial(
                query_stage_master_key=master_key, query_id='', smk_id=0
            )
            encryption_metadata, encrypted_file = SnowflakeEncryptionUtil.encrypt_file(
                encryption_material, file, tmp_dir=tmp_dir
            )

            # Upload to s3
            extra_args = {'ACL': s3_acl} if s3_acl else {}

            # Send key and iv in the metadata, that will be required to decrypt and upload the encrypted file
            extra_args['Metadata'] = {
                'x-amz-key': encryption_metadata.key,
                'x-amz-iv': encryption_metadata.iv,
            }
            try:
                self.s3.upload_file(encrypted_file, bucket, s3_key, ExtraArgs=extra_args)
            finally:
                try:
                    os.remove(encrypted_file)
                except OSError:
                    # Once upload succeeds the caller needs the key so it can either
                    # publish or roll the object back; local cleanup is best-effort.
                    LOGGER.warning(
                        'Failed to remove encrypted staging file %s',
                        encrypted_file,
                        exc_info=True,
                    )

        # Upload to S3 without encrypting
        else:
            extra_args = {'ACL': s3_acl} if s3_acl else None
            self.s3.upload_file(file, bucket, s3_key, ExtraArgs=extra_args)

        return s3_key

    def copy_to_archive(self, source_s3_key, tap_id, table):
        """Copy load file to archive folder with metadata added"""
        table_dict = utils.tablename_to_dict(table)
        archive_table = table_dict.get('table_name')
        archive_schema = table_dict.get('schema_name', '')

        # Retain same filename
        archive_file_basename = os.path.basename(source_s3_key)

        # Get archive s3 prefix from config, defaulting to 'archive' if not specified
        archive_s3_prefix = self.connection_config.get(
            'archive_load_files_s3_prefix', 'archive'
        )

        source_s3_bucket = self.connection_config.get('s3_bucket')

        # Combine existing metadata with archive related headers
        metadata = self.s3.head_object(Bucket=source_s3_bucket, Key=source_s3_key).get(
            'Metadata', {}
        )
        metadata.update(
            {
                'tap': tap_id,
                'schema': archive_schema,
                'table': archive_table,
                'archived-by': 'pipelinewise_fastsync_postgres_to_snowflake',
            }
        )

        # Get archive s3 bucket from config, defaulting to same bucket used for Snowflake imports if not specified
        archive_s3_bucket = self.connection_config.get(
            'archive_load_files_s3_bucket', source_s3_bucket
        )

        archive_key = '{}/{}/{}/{}'.format(
            archive_s3_prefix, tap_id, archive_table, archive_file_basename
        )
        copy_source = '{}/{}'.format(source_s3_bucket, source_s3_key)
        LOGGER.info('Archiving %s to %s', copy_source, archive_key)

        self.s3.copy_object(
            CopySource=copy_source,
            Bucket=archive_s3_bucket,
            Key=archive_key,
            Metadata=metadata,
            MetadataDirective='REPLACE',
        )

    def create_schema(self, schema):
        sql = 'CREATE SCHEMA IF NOT EXISTS {}'.format(schema)
        self.query(sql, query_tag_props={'schema': schema})

    def drop_table(
        self,
        target_schema,
        table_name,
        is_temporary=False,
        max_attempts=1,
        staging_table_name=None,
    ):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = (
            table_dict.get('table_name')
            if not is_temporary
            else staging_table_name or table_dict.get('temp_table_name')
        )

        sql = 'DROP TABLE IF EXISTS {}."{}"'.format(target_schema, target_table.upper())
        for attempt in range(1, max_attempts + 1):
            try:
                self.query(
                    sql,
                    query_tag_props={'schema': target_schema, 'table': table_name},
                )
                return
            except Exception as exc:
                if attempt == max_attempts:
                    raise
                LOGGER.warning(
                    'Snowflake staging cleanup retry %s/%s failed for %s.%s: %s',
                    attempt,
                    max_attempts,
                    target_schema,
                    target_table,
                    exc,
                )

    # pylint: disable=too-many-positional-arguments
    def create_table(
        self,
        target_schema: str,
        table_name: str,
        columns: List[str],
        primary_key: Optional[List[str]],
        is_temporary: bool = False,
        sort_columns=False,
        allow_replace_table=True,
        normalize_primary_keys=True,
        staging_table_name=None,
    ):

        target_table = self._target_table_name(
            table_name,
            is_temporary,
            staging_table_name,
        )
        target_existed = (
            self.table_exists(target_schema, table_name, is_temporary)
            if normalize_primary_keys == 'if_created'
            else False
        )

        # skip the EXTRACTED, BATCHED and DELETED columns in case they exist because they gonna be added later
        columns = [
            c
            for c in columns
            if not (
                c.startswith(utils.SDC_EXTRACTED_AT)
                or c.startswith(utils.SDC_BATCHED_AT)
                or c.startswith(utils.SDC_DELETED_AT)
            )
        ]

        columns += [
            f'{utils.SDC_EXTRACTED_AT} TIMESTAMP_NTZ',
            f'{utils.SDC_BATCHED_AT} TIMESTAMP_NTZ',
            f'{utils.SDC_DELETED_AT} VARCHAR',
        ]

        # We need the sort the columns for some taps( for now tap-s3-csv)
        # because later on when copying a csv file into Snowflake
        # the csv file columns need to be in the same order as the the target table that will be created below
        if sort_columns:
            columns.sort()

        full_table_name = self._get_full_qualified_table_name(target_schema, target_table)

        sql_columns = ','.join(columns)
        sql = (
            f'CREATE '
            f'{"OR REPLACE TABLE" if allow_replace_table else "TABLE IF NOT EXISTS"} '
            f'{full_table_name} ({sql_columns}'
            f'{f", PRIMARY KEY ({",".join(primary_key)}))" if primary_key else ")"}'
        )

        self.query(
            sql, query_tag_props={'schema': target_schema, 'table': target_table}
        )

        if normalize_primary_keys and not target_existed:
            self._drop_pk_non_nullability(target_schema, target_table, primary_key)

    @staticmethod
    def _target_table_name(table_name, is_temporary, staging_table_name=None):
        table_dict = utils.tablename_to_dict(table_name)
        if is_temporary:
            return staging_table_name or table_dict.get('temp_table_name')
        return table_dict.get('table_name')

    def table_exists(self, target_schema, table_name, is_temporary=False):
        """Return whether the exact standard or staging table already exists."""
        table_dict = utils.tablename_to_dict(table_name)
        target_table = (
            table_dict.get('temp_table_name')
            if is_temporary
            else table_dict.get('table_name')
        ).upper()
        quoted_schema = target_schema.upper().replace('"', '""')
        table_prefix = target_table.replace("'", "''")
        rows = self.query(
            f'SHOW TABLES IN SCHEMA "{quoted_schema}" '
            f"STARTS WITH '{table_prefix}'",
            query_tag_props={'schema': target_schema, 'table': table_name},
        )
        return any(
            row.get('name', row.get('NAME')) == target_table
            for row in rows
        )

    def _drop_pk_non_nullability(self, target_schema: str, target_table: str, primary_keys: Optional[List[str]]):
        """
        Drop non-null constraints on PK columns in the given table

        Args:
            target_schema: schema name where table is
            target_table: table name to alter
            primary_keys: list of primary key columns of the table, column are uppercase and wrapped in double quotes
        """
        if not primary_keys:
            return

        full_table_name = self._get_full_qualified_table_name(target_schema, target_table)

        for p_key in primary_keys:
            sql = f'alter table {full_table_name} alter column {p_key.upper()} drop not null;'
            self.query(sql, query_tag_props={'schema': target_schema, 'table': target_table})

    @staticmethod
    def _get_full_qualified_table_name(schema: str, table: str) -> str:
        """
        Constructs the full qualified table name as "SCHEMA_NAME"."TABLE_NAME"
        Args:
            schema: schema name in SF
            table: table name in SF

        Returns: str: full qualified name
        """
        return f'"{schema.upper()}"."{table.upper()}"'

    # pylint: disable=too-many-locals
    def copy_to_table(
        self,
        s3_key,
        target_schema,
        table_name,
        size_bytes,
        is_temporary,
        skip_csv_header=False,
        staging_table_name=None,
    ):
        LOGGER.info('Loading %s into Snowflake...', s3_key)
        table_dict = utils.tablename_to_dict(table_name)
        target_table = (
            table_dict.get('table_name')
            if not is_temporary
            else staging_table_name or table_dict.get('temp_table_name')
        )
        inserts = 0

        stage = self.connection_config['stage']
        # Keep Snowflake's default explicit: unquoted empty fields are NULL, while
        # quoted empty fields remain empty strings.
        sql = (
            f'COPY INTO {target_schema}."{target_table.upper()}" FROM \'@{stage}/{s3_key}\''
            f' FILE_FORMAT = (type=CSV escape=NONE escape_unenclosed_field=\'\\x1e\''
            f' field_optionally_enclosed_by=\'\"\' empty_field_as_null=TRUE'
            f' skip_header={int(skip_csv_header)}'
            f' compression=GZIP binary_format=HEX)'
        )

        # Get number of inserted records - COPY does insert only
        results = self.query(
            sql, query_tag_props={'schema': target_schema, 'table': target_table}
        )
        if len(results) > 0:
            inserts = sum([file_part.get('rows_loaded', 0) for file_part in results])

        LOGGER.info(
            'Loading into %s."%s": %s',
            target_schema,
            target_table.upper(),
            json.dumps(
                {
                    'inserts': inserts,
                    'updates': 0,
                    'file_parts': len(results),
                    'size_bytes': size_bytes,
                }
            ),
        )
        return inserts

    # grant_... functions are common functions called by utils.py: grant_privilege function
    # "to_group" is not used here but exists for compatibility reasons with other database types
    # pylint: disable=unused-argument
    def grant_select_on_table(
        self, target_schema, table_name, role, is_temporary, to_group=False
    ):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = (
                table_dict.get('table_name')
                if not is_temporary
                else table_dict.get('temp_table_name')
            )
            sql = 'GRANT SELECT ON TABLE {}."{}" TO ROLE {}'.format(
                target_schema, target_table.upper(), role
            )
            self.query(
                sql, query_tag_props={'schema': target_schema, 'table': table_name}
            )

    # pylint: disable=unused-argument
    def grant_usage_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT USAGE ON SCHEMA {} TO ROLE {}'.format(target_schema, role)
            self.query(sql, query_tag_props={'schema': target_schema})

    # pylint: disable=unused-argument
    def grant_select_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ROLE {}'.format(
                target_schema, role
            )
            self.query(sql, query_tag_props={'schema': target_schema})

    def obfuscate_columns(
        self,
        target_schema: str,
        table_name: str,
        staging_table_name=None,
    ):
        """
        Apply any configured transformations to the given table
        Args:
            target_schema: target schema name
            table_name: table name
        """
        LOGGER.info('Starting obfuscation rules...')

        table_dict = utils.tablename_to_dict(table_name)
        temp_table = staging_table_name or table_dict.get('temp_table_name')
        transformations = self.transformation_config.get('transformations', [])

        # Input table_name is formatted as {{schema}}.{{table}}
        # Stream name in taps transformation.json is formatted as {{schema}}-{{table}}
        #
        # We need to convert to the same format to find the transformation
        # has that has to be applied
        tap_stream_name_by_table_name = (
            '{}-{}'.format(table_dict['schema_name'], table_dict['table_name'])
            if table_dict['schema_name'] is not None
            else table_dict['table_name']
        )

        # Find obfuscation rules for the current table
        # trans_map = self.__get_stream_transformation_map(tap_stream_name_by_table_name, transformations)
        trans_map = TransformationHelper.get_trans_in_sql_flavor(
            tap_stream_name_by_table_name, transformations, SQLFlavor('snowflake')
        )

        self.__apply_transformations(trans_map, target_schema, temp_table)

        LOGGER.info('Obfuscation rules applied.')

    @staticmethod
    def _merge_tables_query(schema, source_table, target_table, columns, primary_keys):
        on_clause = ' AND '.join(
            [f'"{source_table.upper()}".{p.upper()} = "{target_table.upper()}".{p.upper()}' for p in primary_keys]
        )
        update_clause = ', '.join(
            [f'"{target_table.upper()}".{c.upper()} = "{source_table.upper()}".{c.upper()}' for c in columns]
        )
        columns_for_insert = ', '.join([f'{c.upper()}' for c in columns])
        values = ', '.join([f'"{source_table.upper()}".{c.upper()}' for c in columns])

        return f'MERGE INTO {schema}."{target_table.upper()}" USING {schema}."{source_table.upper()}"' \
               f' ON {on_clause}' \
               f' WHEN MATCHED THEN UPDATE SET {update_clause}' \
               f' WHEN NOT MATCHED THEN INSERT ({columns_for_insert})' \
               f' VALUES ({values})'

    def merge_tables(self, schema, source_table, target_table, columns, primary_keys):
        self.query(self._merge_tables_query(schema, source_table, target_table, columns, primary_keys))

    @staticmethod
    def _partial_hard_delete_query(schema, table, where_clause_sql):
        return f'DELETE FROM {schema}."{table.upper()}"{where_clause_sql} AND _SDC_DELETED_AT IS NOT NULL'

    def partial_hard_delete(self, schema, table, where_clause_sql):
        self.query(self._partial_hard_delete_query(schema, table, where_clause_sql))

    def publish_partial_sync(
        self,
        schema,
        source_table,
        target_table,
        columns,
        primary_keys,
        where_clause_sql,
        hard_delete,
    ):
        """Atomically mark, merge, and optionally delete one partial range."""
        queries = [
            f'UPDATE {schema}."{target_table.upper()}" SET _SDC_DELETED_AT = CURRENT_TIMESTAMP()'
            f'{where_clause_sql} AND _SDC_DELETED_AT IS NULL',
            self._merge_tables_query(schema, source_table, target_table, columns, primary_keys),
        ]
        if hard_delete:
            queries.append(self._partial_hard_delete_query(schema, target_table, where_clause_sql))

        self.execute_transaction(queries, query_tag_props={'schema': schema, 'table': target_table})

    def swap_tables(self, schema, table_name, cleanup_old_table=True) -> None:
        """
        Swaps given target table with its temp version and drops the latter
        Args:
            schema: Snowflake schema name where table is
            table_name: Target table name

        """
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name')
        temp_table = table_dict.get('temp_table_name')

        # Swap tables and drop the old target now held under the temp name.
        self.query(
            f'ALTER TABLE {schema}."{temp_table.upper()}" SWAP WITH {schema}."{target_table.upper()}"',
            query_tag_props={'schema': schema, 'table': target_table},
        )

        if cleanup_old_table:
            # Cleanup is part of successful publication. If it cannot complete,
            # the caller withholds state and retries this idempotent range.
            self.drop_table(
                schema,
                table_name,
                is_temporary=True,
                max_attempts=3,
            )

    def add_columns(self, schema: str, table_name: str, adding_columns: dict) -> None:
        if adding_columns:
            add_columns_list = [f'{column_name} {column_type}' for column_name, column_type in adding_columns.items()]
            add_clause = ', '.join(add_columns_list)
            query = f'ALTER TABLE {schema}."{table_name.upper()}" ADD {add_clause}'
            self.query(query)

    def widen_varchar_columns(
        self,
        schema: str,
        table_name: str,
        column_names: List[str],
    ) -> None:
        """Widen existing native text columns before PartialSync publication."""
        full_table_name = self._get_full_qualified_table_name(schema, table_name)
        for column_name in column_names:
            escaped_column_name = column_name.replace('"', '""')
            quoted_column = f'"{escaped_column_name}"'
            self.query(
                f'ALTER TABLE {full_table_name} ALTER COLUMN {quoted_column} '
                f'SET DATA TYPE {SNOWFLAKE_MAX_VARCHAR}',
                query_tag_props={'schema': schema, 'table': table_name},
            )

    def __apply_transformations(
        self, transformations: List[Dict], target_schema: str, table_name: str
    ) -> None:
        """
        Generate and execute the SQL queries based on the given transformations.
        Args:
            transformations: List of dictionaries in the form {"trans": "", conditions: "... AND ..."}
            target_schema: name of the target schema where the table lives
            table_name: the table name on which we want to apply the transformations
        """
        full_qual_table_name = f'"{target_schema.upper()}"."{table_name.upper()}"'

        if transformations:
            all_cols_update_sql = ''

            # Conditional transformations will have to be executed one at time separately

            for trans_item in transformations:

                # If we have conditions, then we need to construct the query and execute it to transform the
                # single column conditionally
                if trans_item['conditions']:
                    sql = (
                        f'UPDATE {full_qual_table_name} '
                        f'SET {trans_item["trans"]} WHERE {trans_item["conditions"]};'
                    )

                    self.query(
                        sql,
                        query_tag_props={'schema': target_schema, 'table': table_name},
                    )

                # Otherwise, we can add this column to a general UPDATE query with no predicates
                else:

                    # if the variable is empty, then initialize it otherwise append the
                    # current transformation to it
                    if not all_cols_update_sql:
                        all_cols_update_sql = trans_item['trans']
                    else:
                        all_cols_update_sql = (
                            f'{all_cols_update_sql}, {trans_item["trans"]}'
                        )

            # If we have some non-conditional transformations then construct and execute a query
            if all_cols_update_sql:
                all_cols_update_sql = (
                    f'UPDATE {full_qual_table_name} SET {all_cols_update_sql};'
                )

                self.query(
                    all_cols_update_sql,
                    query_tag_props={'schema': target_schema, 'table': table_name},
                )
