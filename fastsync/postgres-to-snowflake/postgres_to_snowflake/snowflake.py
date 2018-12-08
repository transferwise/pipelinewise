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
        print("SNOWFLAKE - Running query: {}".format(query))
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

        print("SNOWFLAKE - Uploading to S3 bucket: {}, local file: {}, S3 key: {}".format(bucket, path, s3_key))
        self.s3.upload_file(path, bucket, s3_key)
        return s3_key


    def copy_to_table(self, s3_key, target_schema, table_name, is_temporary):
        print("SNOWFLAKE - Loading {} into Snowflake...".format(s3_key))
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('name') if not is_temporary else table_dict.get('temp_name')

        aws_access_key_id=self.connection_config['aws_access_key_id']
        aws_secret_access_key=self.connection_config['aws_secret_access_key']
        bucket = self.connection_config['s3_bucket']
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')

        sql = "COPY INTO {}.{} FROM 's3://{}/{}' CREDENTIALS = (aws_key_id='{}' aws_secret_key='{}')".format(target_schema, target_table, bucket, s3_key, aws_access_key_id, aws_secret_access_key)
        self.query(sql)

        print("SNOWFLAKE - Deleting {} from S3...".format(s3_key))
        self.s3.delete_object(Bucket=bucket, Key=s3_key)


    def obfuscate_columns(self):
        print("SNOWFLAKE - Applying obfuscation rules")


    def swap_tables(self, schema, table_name):
        table_dict = utils.tablename_to_dict(table_name)
        sql = "ALTER TABLE {}.{} SWAP WITH {}.{}".format(schema, table_dict.get('temp_name'), schema, table_dict.get('name'))
        self.query(sql)
        

