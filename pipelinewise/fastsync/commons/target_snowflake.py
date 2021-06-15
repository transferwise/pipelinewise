import logging
import os
import json
import boto3
import snowflake.connector

from typing import List, Dict
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.remote_storage_util import \
    SnowflakeFileEncryptionMaterial

from . import utils
from .transform_utils import TransformationHelper, SQLFlavor

LOGGER = logging.getLogger(__name__)

# tone down snowflake connector logging level.
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)


# pylint: disable=missing-function-docstring,no-self-use,too-many-arguments
class FastSyncTargetSnowflake:
    """
    Common functions for fastsync to Snowflake
    """

    # pylint: disable=invalid-name
    def __init__(self, connection_config, transformation_config=None):
        self.connection_config = connection_config
        self.transformation_config = transformation_config

        # Get the required parameters from config file and/or environment variables
        aws_profile = self.connection_config.get('aws_profile') or os.environ.get('AWS_PROFILE')
        aws_access_key_id = self.connection_config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = self.connection_config.get('aws_secret_access_key') or \
                                os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_session_token = self.connection_config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token
            )
        # AWS Profile based authentication
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)

        # Create the s3 client
        self.s3 = aws_session.client('s3',
                                     region_name=self.connection_config.get('s3_region_name'),
                                     endpoint_url=self.connection_config.get('s3_endpoint_url'))

    def create_query_tag(self, query_tag_props: dict = None) -> str:
        schema = None
        table = None

        if isinstance(query_tag_props, dict):
            schema = query_tag_props.get('schema')
            table = query_tag_props.get('table')

        return json.dumps({'ppw_component': 'fastsync',
                           'tap_id': self.connection_config.get('tap_id'),
                           'database': self.connection_config['dbname'],
                           'schema': schema,
                           'table': table})

    def open_connection(self, query_tag_props=None):
        return snowflake.connector.connect(user=self.connection_config['user'],
                                           password=self.connection_config['password'],
                                           account=self.connection_config['account'],
                                           database=self.connection_config['dbname'],
                                           warehouse=self.connection_config['warehouse'],
                                           autocommit=True,
                                           session_parameters={
                                               # Quoted identifiers should be case sensitive
                                               'QUOTED_IDENTIFIERS_IGNORE_CASE': 'FALSE',
                                               'QUERY_TAG': self.create_query_tag(query_tag_props)
                                           })

    def query(self, query, params=None, query_tag_props=None):
        LOGGER.debug('Running query: %s', query)
        with self.open_connection(query_tag_props) as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(query, params)

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []

    def upload_to_s3(self, file, tmp_dir=None):
        bucket = self.connection_config['s3_bucket']
        s3_acl = self.connection_config.get('s3_acl')
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        s3_key = '{}{}'.format(s3_key_prefix, os.path.basename(file))

        LOGGER.info('Uploading to S3 bucket: %s, local file: %s, S3 key: %s', bucket, file, s3_key)

        # Encrypt csv if client side encryption enabled
        master_key = self.connection_config.get('client_side_encryption_master_key', '')
        if master_key != '':
            # Encrypt the file
            LOGGER.info('Encrypting file %s...', file)
            encryption_material = SnowflakeFileEncryptionMaterial(
                query_stage_master_key=master_key,
                query_id='',
                smk_id=0
            )
            encryption_metadata, encrypted_file = SnowflakeEncryptionUtil.encrypt_file(
                encryption_material,
                file,
                tmp_dir=tmp_dir
            )

            # Upload to s3
            extra_args = {'ACL': s3_acl} if s3_acl else dict()

            # Send key and iv in the metadata, that will be required to decrypt and upload the encrypted file
            extra_args['Metadata'] = {
                'x-amz-key': encryption_metadata.key,
                'x-amz-iv': encryption_metadata.iv
            }
            self.s3.upload_file(encrypted_file, bucket, s3_key, ExtraArgs=extra_args)

            # Remove the uploaded encrypted file
            os.remove(encrypted_file)

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

        archive_metadata = {
            'tap': tap_id,
            'schema': archive_schema,
            'table': archive_table,
            'archived-by': 'pipelinewise_fastsync_postgres_to_snowflake'
        }

        archive_file = source_s3_key.split('/')[-1]
        archive_folder = 'archive/{}/{}'.format(tap_id, archive_table)
        archive_key = '{}/{}'.format(archive_folder, archive_file)

        LOGGER.info('Archiving %s to %s', source_s3_key, archive_key)
        bucket = self.connection_config['s3_bucket']
        copy_source = '{}/{}'.format(bucket, source_s3_key)
        self.s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=archive_key, Metadata=archive_metadata,
                            MetadataDirective='REPLACE')

    def create_schema(self, schema):
        sql = 'CREATE SCHEMA IF NOT EXISTS {}'.format(schema)
        self.query(sql, query_tag_props={'schema': schema})

    def drop_table(self, target_schema, table_name, is_temporary=False):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        sql = 'DROP TABLE IF EXISTS {}."{}"'.format(target_schema, target_table.upper())
        self.query(sql, query_tag_props={'schema': target_schema, 'table': table_name})

    def create_table(self, target_schema: str, table_name: str, columns: List[str], primary_key: List[str],
                     is_temporary: bool = False, sort_columns=False):

        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        # skip the EXTRACTED, BATCHED and DELETED columns in case they exist because they gonna be added later
        columns = [c for c in columns if not (c.startswith(utils.SDC_EXTRACTED_AT) or
                                              c.startswith(utils.SDC_BATCHED_AT) or
                                              c.startswith(utils.SDC_DELETED_AT))]

        columns += [f'{utils.SDC_EXTRACTED_AT} TIMESTAMP_NTZ',
                    f'{utils.SDC_BATCHED_AT} TIMESTAMP_NTZ',
                    f'{utils.SDC_DELETED_AT} VARCHAR']

        # We need the sort the columns for some taps( for now tap-s3-csv)
        # because later on when copying a csv file into Snowflake
        # the csv file columns need to be in the same order as the the target table that will be created below
        if sort_columns:
            columns.sort()

        sql_columns = ','.join(columns)
        sql_primary_keys = ','.join(primary_key) if primary_key else None
        sql = f'CREATE OR REPLACE TABLE {target_schema}."{target_table.upper()}" (' \
              f'{sql_columns}' \
              f'{f", PRIMARY KEY ({sql_primary_keys}))" if primary_key else ")"}'

        self.query(sql, query_tag_props={'schema': target_schema, 'table': target_table})

    # pylint: disable=too-many-locals
    def copy_to_table(self, s3_key, target_schema, table_name, size_bytes, is_temporary, skip_csv_header=False):
        LOGGER.info('Loading %s into Snowflake...', s3_key)
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
        inserts = 0

        stage = self.connection_config['stage']
        sql = f'COPY INTO {target_schema}."{target_table.upper()}" FROM \'@{stage}/{s3_key}\'' \
              f' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\'' \
              f' field_optionally_enclosed_by=\'\"\' skip_header={int(skip_csv_header)}' \
              f' compression=GZIP binary_format=HEX)'

        # Get number of inserted records - COPY does insert only
        results = self.query(sql, query_tag_props={'schema': target_schema, 'table': target_table})
        if len(results) > 0:
            inserts = sum([file_part.get('rows_loaded', 0) for file_part in results])

        LOGGER.info('Loading into %s."%s": %s',
                    target_schema,
                    target_table.upper(),
                    json.dumps({'inserts': inserts,
                                'updates': 0,
                                'file_parts': len(results),
                                'size_bytes': size_bytes}))

    # grant_... functions are common functions called by utils.py: grant_privilege function
    # "to_group" is not used here but exists for compatibility reasons with other database types
    # "to_group" is for databases that can grant to users and groups separately like Amazon Redshift
    # pylint: disable=unused-argument
    def grant_select_on_table(self, target_schema, table_name, role, is_temporary, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
            sql = 'GRANT SELECT ON {}."{}" TO ROLE {}'.format(target_schema, target_table.upper(), role)
            self.query(sql, query_tag_props={'schema': target_schema, 'table': table_name})

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
            sql = 'GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ROLE {}'.format(target_schema, role)
            self.query(sql, query_tag_props={'schema': target_schema})

    def obfuscate_columns(self, target_schema: str, table_name: str):
        """
        Apply any configured transformations to the given table
        Args:
            target_schema: target schema name
            table_name: table name
        """
        LOGGER.info('Starting obfuscation rules...')

        table_dict = utils.tablename_to_dict(table_name)
        temp_table = table_dict.get('temp_table_name')
        transformations = self.transformation_config.get('transformations', [])

        # Input table_name is formatted as {{schema}}.{{table}}
        # Stream name in taps transformation.json is formatted as {{schema}}-{{table}}
        #
        # We need to convert to the same format to find the transformation
        # has that has to be applied
        tap_stream_name_by_table_name = '{}-{}'.format(table_dict['schema_name'], table_dict['table_name']) \
            if table_dict['schema_name'] is not None else table_dict['table_name']

        # Find obfuscation rules for the current table
        # trans_map = self.__get_stream_transformation_map(tap_stream_name_by_table_name, transformations)
        trans_map = TransformationHelper.get_trans_in_sql_flavor(
            tap_stream_name_by_table_name,
            transformations,
            SQLFlavor('snowflake'))

        self.__apply_transformations(trans_map, target_schema, temp_table)

        LOGGER.info('Obfuscation rules applied.')

    def swap_tables(self, schema, table_name) -> None:
        """
        Swaps given target table with its temp version and drops the latter
        Args:
            schema: Snowflake schema name where table is
            table_name: Target table name

        """
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name')
        temp_table = table_dict.get('temp_table_name')

        # Swap tables and drop the temp tamp
        self.query(f'ALTER TABLE {schema}."{temp_table.upper()}" SWAP WITH {schema}."{target_table.upper()}"',
                   query_tag_props={'schema': schema, 'table': target_table})

        self.query(f'DROP TABLE IF EXISTS {schema}."{temp_table.upper()}"',
                   query_tag_props={'schema': schema, 'table': temp_table})

    def __apply_transformations(self, transformations: List[Dict], target_schema: str, table_name: str) -> None:
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
                    sql = f'UPDATE {full_qual_table_name} ' \
                          f'SET {trans_item["trans"]} WHERE {trans_item["conditions"]};'

                    self.query(sql, query_tag_props={'schema': target_schema, 'table': table_name})

                # Otherwise, we can add this column to a general UPDATE query with no predicates
                else:

                    # if the variable is empty, then initialize it otherwise append the
                    # current transformation to it
                    if not all_cols_update_sql:
                        all_cols_update_sql = trans_item['trans']
                    else:
                        all_cols_update_sql = f'{all_cols_update_sql}, {trans_item["trans"]}'

            # If we have some non-conditional transformations then construct and execute a query
            if all_cols_update_sql:
                all_cols_update_sql = f'UPDATE {full_qual_table_name} SET {all_cols_update_sql};'

                self.query(all_cols_update_sql, query_tag_props={'schema': target_schema, 'table': table_name})
