import snowflake.connector
import time
import boto3
import os

import postgres_to_snowflake.utils as utils

class Snowflake:
    def __init__(self, connection_config, transformation_config):
        self.connection_config = connection_config
        self.transformation_config = transformation_config
        self.s3 = boto3.client(
          's3',
          aws_access_key_id=self.connection_config['aws_access_key_id'],
          aws_secret_access_key=self.connection_config['aws_secret_access_key']
        )


    def open_connection(self):
        return snowflake.connector.connect(
            user=self.connection_config['user'],
            password=self.connection_config['password'],
            account=self.connection_config['account'],
            database=self.connection_config['dbname'],
            warehouse=self.connection_config['warehouse']
        )


    def query(self, query, params=None):
        utils.log("SNOWFLAKE - Running query: {}".format(query))
        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(
                    query,
                    params
                )

                if cur.rowcount > 0:
                    return cur.fetchall()
                else:
                    return []


    def upload_to_s3(self, path, table):
        bucket = self.connection_config['s3_bucket']
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        s3_key = "{}pipelinewise_{}_{}.csv.gz".format(s3_key_prefix, table, time.strftime("%Y%m%d-%H%M%S"))

        utils.log("SNOWFLAKE - Uploading to S3 bucket: {}, local file: {}, S3 key: {}".format(bucket, path, s3_key))
        self.s3.upload_file(path, bucket, s3_key)
        return s3_key


    def create_schema(self, schema):
        sql = "CREATE SCHEMA IF NOT EXISTS {}".format(schema)
        self.query(sql)


    def copy_to_table(self, s3_key, target_schema, table_name, is_temporary):
        utils.log("SNOWFLAKE - Loading {} into Snowflake...".format(s3_key))
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('name') if not is_temporary else table_dict.get('temp_name')

        aws_access_key_id=self.connection_config['aws_access_key_id']
        aws_secret_access_key=self.connection_config['aws_secret_access_key']
        bucket = self.connection_config['s3_bucket']

        sql = """COPY INTO {}.{} FROM 's3://{}/{}'
            CREDENTIALS = (aws_key_id='{}' aws_secret_key='{}')
            FILE_FORMAT = (type='CSV' escape='\\x1e' escape_unenclosed_field='\\x1e' field_optionally_enclosed_by='\"')
        """.format(target_schema, target_table, bucket, s3_key, aws_access_key_id, aws_secret_access_key)
        self.query(sql)

        utils.log("SNOWFLAKE - Deleting {} from S3...".format(s3_key))
        self.s3.delete_object(Bucket=bucket, Key=s3_key)


    def grant_select_on_table(self, target_schema, table_name, role, is_temporary):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = table_dict.get('name') if not is_temporary else table_dict.get('temp_name')
            sql = "GRANT SELECT ON {}.{} TO ROLE {}".format(target_schema, target_table, role)
            self.query(sql)


    def grant_usage_on_schema(self, target_schema, role):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = "GRANT USAGE ON SCHEMA {} TO ROLE {}".format(target_schema,role)
            self.query(sql)


    def grant_select_on_schema(self, target_schema, role):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ROLE {}".format(target_schema,role)
            self.query(sql)


    def obfuscate_columns(self, target_schema, table_name):
        utils.log("SNOWFLAKE - Applying obfuscation rules")
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('name')
        temp_table = table_dict.get('temp_name')
        transformations = self.transformation_config.get('transformations', [])
        trans_cols = []

        # Find obfuscation rule for the current table
        for t in transformations:
            if t.get('stream') == target_table:
                column = t.get('fieldId')
                transform_type = t.get('type')
                if transform_type == 'SET-NULL':
                    trans_cols.append("{} = NULL".format(column))
                elif transform_type == 'HASH':
                    trans_cols.append("{} = SHA2({}, 256)".format(column, column))
                elif 'HASH-SKIP-FIRST' in transform_type:
                    skip_first_n = transform_type[-1]
                    trans_cols.append("{} = CONCAT(SUBSTRING({}, 1, {}), SHA2(SUBSTRING({}, {} + 1), 256))".format(column, column, skip_first_n, column, skip_first_n))
                elif transform_type == 'MASK-DATE':
                    trans_cols.append("{} = TO_CHAR({}::DATE,'YYYY-01-01')::DATE".format(column, column))
                elif transform_type == 'MASK-NUMBER':
                    trans_cols.append("{} = 0".format(column))

        # Generate and run UPDATE if at least one obfuscation rule found
        if len(trans_cols) > 0:
            sql = "UPDATE {}.{} SET {}".format(target_schema, temp_table, ','.join(trans_cols))
            self.query(sql)


    def swap_tables(self, schema, table_name):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('name')
        temp_table = table_dict.get('temp_name')

        # Swap tables and drop the temp tamp
        self.query("ALTER TABLE {}.{} SWAP WITH {}.{}".format(schema, temp_table, schema, target_table))
        self.query("DROP TABLE IF EXISTS {}.{}".format(schema, temp_table))
        

