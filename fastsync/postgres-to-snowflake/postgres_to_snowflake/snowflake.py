import snowflake.connector
import time
import boto3
import os

class Snowflake:
    def __init__(self, connection_config):
        self.connection_config = connection_config
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

    def copy_to_table(self, s3_key, target_schema, table, is_temporary):
        print("SNOWFLAKE - Loading {} into Snowflake...".format(s3_key))

        table_schema = table.split('.')[0]
        table_name = table.split('.')[1]
        target_table_name = table_name
        if is_temporary:
            target_table_name += '_temp'

        aws_access_key_id=self.connection_config['aws_access_key_id']
        aws_secret_access_key=self.connection_config['aws_secret_access_key']
        bucket = self.connection_config['s3_bucket']
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')

        sql = "COPY INTO {}.{} FROM 's3://{}/{}' CREDENTIALS = (aws_key_id='{}' aws_secret_key='{}')".format(target_schema, target_table_name, bucket, s3_key, aws_access_key_id, aws_secret_access_key)
        self.query(sql)

        print("SNOWFLAKE - Deleting {} from S3...".format(s3_key))
        self.s3.delete_object(Bucket=bucket, Key=s3_key)

    def swap_tables(self, schema, table):
        table_name = table.split('.')[1]
        temp_table_name = '{}_temp'.format(table_name)
        sql = "ALTER TABLE {}.{} SWAP WITH {}.{}".format(schema, temp_table_name, schema, table_name)
        self.query(sql)
        

