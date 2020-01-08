import os
import time
from typing import List

import boto3
import snowflake.connector
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.remote_storage_util import \
    SnowflakeFileEncryptionMaterial

from . import utils


#pylint: disable=missing-function-docstring,no-self-use,too-many-arguments
class FastSyncTargetSnowflake:
    """
    Common functions for fastsync to Redshift
    """

    EXTRACTED_AT_COLUMN = '_SDC_EXTRACTED_AT'
    BATCHED_AT_COLUMN = '_SDC_BATCHED_AT'
    DELETED_AT_COLUMN = '_SDC_DELETED_AT'

    # pylint: disable=invalid-name
    def __init__(self, connection_config, transformation_config=None):
        self.connection_config = connection_config
        self.transformation_config = transformation_config
        self.s3 = boto3.client('s3',
                               aws_access_key_id=self.connection_config['aws_access_key_id'],
                               aws_secret_access_key=self.connection_config['aws_secret_access_key'])

    def open_connection(self):
        return snowflake.connector.connect(user=self.connection_config['user'],
                                           password=self.connection_config['password'],
                                           account=self.connection_config['account'],
                                           database=self.connection_config['dbname'],
                                           warehouse=self.connection_config['warehouse'],
                                           autocommit=True)

    def query(self, query, params=None):
        utils.log('SNOWFLAKE - Running query: {}'.format(query))
        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(query, params)

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []

    def upload_to_s3(self, file, table):
        bucket = self.connection_config['s3_bucket']
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        s3_key = '{}pipelinewise_{}_{}.csv.gz'.format(s3_key_prefix, table, time.strftime('%Y%m%d-%H%M%S'))

        utils.log('SNOWFLAKE - Uploading to S3 bucket: {}, local file: {}, S3 key: {}'.format(bucket, file, s3_key))

        # Encrypt csv if client side encryption enabled
        master_key = self.connection_config.get('client_side_encryption_master_key', '')
        if master_key != '':
            # Encrypt the file
            utils.log('Encrypting file {}...'.format(file))
            encryption_material = SnowflakeFileEncryptionMaterial(query_stage_master_key=master_key,
                                                                  query_id='',
                                                                  smk_id=0)
            encryption_metadata, encrypted_file = SnowflakeEncryptionUtil.encrypt_file(encryption_material, file)

            # Upload to s3
            # Send key and iv in the metadata, that will be required to decrypt and upload the encrypted file
            metadata = {'x-amz-key': encryption_metadata.key, 'x-amz-iv': encryption_metadata.iv}
            self.s3.upload_file(encrypted_file, bucket, s3_key, ExtraArgs={'Metadata': metadata})

            # Remove the uploaded encrypted file
            os.remove(encrypted_file)

        # Upload to S3 without encrypting
        else:
            self.s3.upload_file(file, bucket, s3_key)

        return s3_key

    def create_schema(self, schema):
        sql = 'CREATE SCHEMA IF NOT EXISTS {}'.format(schema)
        self.query(sql)

    def drop_table(self, target_schema, table_name, is_temporary=False):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        sql = 'DROP TABLE IF EXISTS {}.{}'.format(target_schema, target_table)
        self.query(sql)

    def create_table(self,
                     target_schema: str,
                     table_name: str,
                     columns: List[str],
                     primary_key: str,
                     is_temporary: bool = False,
                     sort_columns=False):

        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        # skip the EXTRACTED, BATCHED and DELETED columns in case they exist because they gonna be added later
        columns = [
            c for c in columns if not (c.startswith(self.EXTRACTED_AT_COLUMN) or c.startswith(self.BATCHED_AT_COLUMN) or
                                       c.startswith(self.DELETED_AT_COLUMN))
        ]

        columns += [
            f'{self.EXTRACTED_AT_COLUMN} TIMESTAMP_NTZ', f'{self.BATCHED_AT_COLUMN} TIMESTAMP_NTZ',
            f'{self.DELETED_AT_COLUMN} VARCHAR'
        ]

        # We need the sort the columns for some taps( for now tap-s3-csv)
        # because later on when copying a csv file into Snowflake
        # the csv file columns need to be in the same order as the the target table that will be created below
        if sort_columns:
            columns.sort()

        sql = f"""CREATE OR REPLACE TABLE {target_schema}.{target_table} (
        {','.join(columns)}
        {f', PRIMARY KEY ({primary_key})' if primary_key else ''})
        """

        self.query(sql)

    def copy_to_table(self, s3_key, target_schema, table_name, is_temporary, skip_csv_header=False):
        utils.log('SNOWFLAKE - Loading {} into Snowflake...'.format(s3_key))
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        aws_access_key_id = self.connection_config['aws_access_key_id']
        aws_secret_access_key = self.connection_config['aws_secret_access_key']
        bucket = self.connection_config['s3_bucket']

        master_key = self.connection_config.get('client_side_encryption_master_key', '')
        if master_key != '':
            sql = """COPY INTO {}.{} FROM @{}/{}
                FILE_FORMAT = (type='CSV' escape='\\x1e' escape_unenclosed_field='\\x1e' 
                field_optionally_enclosed_by='\"' skip_header={} COMPRESSION='GZIP' BINARY_FORMAT='HEX') 
            """.format(
                target_schema,
                target_table,
                self.connection_config['stage'],
                s3_key,
                int(skip_csv_header),
            )
        else:
            sql = """COPY INTO {}.{} FROM 's3://{}/{}'
                CREDENTIALS = (aws_key_id='{}' aws_secret_key='{}')
                FILE_FORMAT = (type='CSV' escape='\\x1e' escape_unenclosed_field='\\x1e' 
                field_optionally_enclosed_by='\"' skip_header={} COMPRESSION='GZIP' BINARY_FORMAT='HEX')
            """.format(
                target_schema,
                target_table,
                bucket,
                s3_key,
                aws_access_key_id,
                aws_secret_access_key,
                int(skip_csv_header),
            )

        self.query(sql)

        utils.log('SNOWFLAKE - Deleting {} from S3...'.format(s3_key))
        self.s3.delete_object(Bucket=bucket, Key=s3_key)

    # grant_... functions are common functions called by utils.py: grant_privilege function
    # "to_group" is not used here but exists for compatibility reasons with other database types
    # "to_group" is for databases that can grant to users and groups separately like Amazon Redshift
    # pylint: disable=unused-argument
    def grant_select_on_table(self, target_schema, table_name, role, is_temporary, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
            sql = 'GRANT SELECT ON {}.{} TO ROLE {}'.format(target_schema, target_table, role)
            self.query(sql)

    # pylint: disable=unused-argument
    def grant_usage_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT USAGE ON SCHEMA {} TO ROLE {}'.format(target_schema, role)
            self.query(sql)

    # pylint: disable=unused-argument
    def grant_select_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ROLE {}'.format(target_schema, role)
            self.query(sql)

    # pylint: disable=duplicate-string-formatting-argument
    def obfuscate_columns(self, target_schema, table_name):
        utils.log('SNOWFLAKE - Applying obfuscation rules')
        table_dict = utils.tablename_to_dict(table_name)
        temp_table = table_dict.get('temp_table_name')
        transformations = self.transformation_config.get('transformations', [])
        trans_cols = []

        # Find obfuscation rule for the current table
        for trans in transformations:
            # Input table_name is formatted as {{schema}}.{{table}}
            # Stream name in taps transformation.json is formatted as {{schema}}-{{table}}
            #
            # We need to convert to the same format to find the transformation
            # has that has to be applied
            tap_stream_name_by_table_name = '{}-{}'.format(table_dict['schema_name'], table_dict['table_name']) \
                if table_dict['schema_name'] is not None else table_dict['table_name']

            if trans.get('tap_stream_name') == tap_stream_name_by_table_name:
                # use safe field id in case the column to transform is has a name of a reserved word
                # fallback to field_id if the safe id doesn't exist
                column = trans.get('safe_field_id', trans.get('field_id'))
                transform_type = trans.get('type')
                if transform_type == 'SET-NULL':
                    trans_cols.append('{} = NULL'.format(column))
                elif transform_type == 'HASH':
                    trans_cols.append('{} = SHA2({}, 256)'.format(column, column))
                elif 'HASH-SKIP-FIRST' in transform_type:
                    skip_first_n = transform_type[-1]
                    trans_cols.append('{} = CONCAT(SUBSTRING({}, 1, {}), SHA2(SUBSTRING({}, {} + 1), 256))'.format(
                        column, column, skip_first_n, column, skip_first_n))
                elif transform_type == 'MASK-DATE':
                    trans_cols.append("{} = TO_CHAR({}::DATE,'YYYY-01-01')::DATE".format(column, column))
                elif transform_type == 'MASK-NUMBER':
                    trans_cols.append('{} = 0'.format(column))

        # Generate and run UPDATE if at least one obfuscation rule found
        if len(trans_cols) > 0:
            sql = 'UPDATE {}.{} SET {}'.format(target_schema, temp_table, ','.join(trans_cols))
            self.query(sql)

    def swap_tables(self, schema, table_name):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name')
        temp_table = table_dict.get('temp_table_name')

        # Swap tables and drop the temp tamp
        self.query('ALTER TABLE {}.{} SWAP WITH {}.{}'.format(schema, temp_table, schema, target_table))
        self.query('DROP TABLE IF EXISTS {}.{}'.format(schema, temp_table))

    # pylint: disable=invalid-name
    def cache_information_schema_columns(self, tables):
        pipelinewise_schema = utils.tablename_to_dict(self.connection_config['stage'])['schema_name']
        schemas_to_cache = utils.get_target_schemas(self.connection_config, tables)

        # Create an empty cache table if not exists
        self.query("""
            CREATE TABLE IF NOT EXISTS {}.columns (table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, data_type VARCHAR)
        """.format(pipelinewise_schema))

        #  Cache table columns from information_schema
        for schema_name in schemas_to_cache:
            utils.log('rebuilding information_schema cache for schema: {}'.format(schemas_to_cache))

            # Delete existing data about the current schema
            self.query("""
                DELETE FROM {}.columns
                WHERE LOWER(table_schema) = '{}'
            """.format(pipelinewise_schema, schema_name.lower()))

            # Insert the latest data from information_schema into the cache table
            self.query("""
                INSERT INTO {}.columns
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE LOWER(table_schema) = '{}'
            """.format(pipelinewise_schema, schema_name.lower()))
