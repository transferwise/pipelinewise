import os
import logging
import time
import boto3
import psycopg2
import psycopg2.extras

from . import utils


LOGGER = logging.getLogger(__name__)


# pylint: disable=missing-function-docstring,no-self-use,too-many-arguments
class FastSyncTargetRedshift:
    """
    Common functions for fastsync to Redshift
    """

    # pylint: disable=invalid-name
    def __init__(self, connection_config, transformation_config=None):
        self.connection_config = connection_config
        self.transformation_config = transformation_config

        aws_access_key_id = self.connection_config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = self.connection_config.get('aws_secret_access_key') or \
                                os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_session_token = self.connection_config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')

        # Init S3 client
        # Conditionally pass keys as this seems to affect whether instance credentials
        # are correctly loaded if the keys are None
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token
            )
            credentials = aws_session.get_credentials().get_frozen_credentials()

            # Explicitly set credentials to those fetched from Boto so we can re-use them in COPY SQL if necessary
            self.connection_config['aws_access_key_id'] = credentials.access_key
            self.connection_config['aws_secret_access_key'] = credentials.secret_key
            self.connection_config['aws_session_token'] = credentials.token
        else:
            aws_session = boto3.session.Session()

        self.s3 = aws_session.client('s3')

    def open_connection(self):
        conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
            self.connection_config['host'],
            self.connection_config['dbname'],
            self.connection_config['user'],
            self.connection_config['password'],
            self.connection_config['port']
        )

        return psycopg2.connect(conn_string)

    def query(self, query, params=None):
        LOGGER.debug('Running query: %s', query)
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)

                if cur.rowcount > 0 and cur.description:
                    return cur.fetchall()

                return []

    def upload_to_s3(self, file, table):
        bucket = self.connection_config['s3_bucket']
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        s3_key = '{}pipelinewise_{}_{}.csv.gz'.format(s3_key_prefix, table, time.strftime('%Y%m%d-%H%M%S'))

        LOGGER.info('Uploading to S3 bucket: %s, local file: %s, S3 key: %s', bucket, file, s3_key)

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

    def create_table(self, target_schema, table_name, columns, primary_key, is_temporary=False):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        # pylint: disable=using-constant-test
        if False:
            sql = """CREATE TABLE IF NOT EXISTS {}.{} ({}
            ,_SDC_EXTRACTED_AT TIMESTAMP WITHOUT TIME ZONE
            ,_SDC_BATCHED_AT TIMESTAMP WITHOUT TIME ZONE
            ,_SDC_DELETED_AT CHARACTER VARYING
            , PRIMARY KEY ({}))
            """.format(target_schema, target_table, ', '.join(columns), primary_key)
        else:
            sql = """CREATE TABLE IF NOT EXISTS {}.{} ({}
            ,_SDC_EXTRACTED_AT TIMESTAMP WITHOUT TIME ZONE
            ,_SDC_BATCHED_AT TIMESTAMP WITHOUT TIME ZONE
            ,_SDC_DELETED_AT CHARACTER VARYING
            )
            """.format(target_schema, target_table, ', '.join(columns))
        self.query(sql)

    def copy_to_table(self, s3_key, target_schema, table_name, is_temporary):
        LOGGER.info('Loading %s into Redshift...', s3_key)
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        # Step 1: Generate copy credentials - prefer role if provided, otherwise use access and secret keys
        copy_credentials = """
            iam_role '{aws_role_arn}'
        """.format(aws_role_arn=self.connection_config['aws_redshift_copy_role_arn']) \
            if self.connection_config.get('aws_redshift_copy_role_arn') else """
            ACCESS_KEY_ID '{aws_access_key_id}'
            SECRET_ACCESS_KEY '{aws_secret_access_key}'
            {aws_session_token}
        """.format(
            aws_access_key_id=self.connection_config['aws_access_key_id'],
            aws_secret_access_key=self.connection_config['aws_secret_access_key'],
            aws_session_token="SESSION_TOKEN '{}'".format(self.connection_config['aws_session_token']) \
                if self.connection_config.get('aws_session_token') else '',
        )

        # Step 2: Generate copy options - Override defaults from config.json if defined
        copy_options = self.connection_config.get('copy_options', """
            EMPTYASNULL BLANKSASNULL TRIMBLANKS TRUNCATECOLUMNS
            TIMEFORMAT 'auto'
        """)

        # Step3: Using the built-in CSV COPY option to load
        copy_sql = """COPY {schema}.{table} FROM 's3://{s3_bucket}/{s3_key}'
            {copy_credentials}
            {copy_options}
            CSV GZIP
        """.format(
            schema=target_schema,
            table=target_table,
            s3_bucket=self.connection_config['s3_bucket'],
            s3_key=s3_key,
            copy_credentials=copy_credentials,
            copy_options=copy_options
        )

        self.query(copy_sql)

        LOGGER.info('Deleting %s from S3...', s3_key)
        self.s3.delete_object(Bucket=self.connection_config['s3_bucket'], Key=s3_key)

    def grant_select_on_table(self, target_schema, table_name, grantee, is_temporary, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if grantee:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
            sql = 'GRANT SELECT ON {}.{} TO {} {}'.format(target_schema, target_table, 'GROUP' if to_group else '',
                                                          grantee)
            self.query(sql)

    def grant_usage_on_schema(self, target_schema, grantee, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if grantee:
            sql = 'GRANT USAGE ON SCHEMA {} TO {} {}'.format(target_schema, 'GROUP' if to_group else '', grantee)
            self.query(sql)

    def grant_select_on_schema(self, target_schema, grantee, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if grantee:
            sql = 'GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {} {}'.format(target_schema, 'GROUP' if to_group else '',
                                                                            grantee)
            self.query(sql)

    # pylint: disable=duplicate-string-formatting-argument
    def obfuscate_columns(self, target_schema, table_name):
        LOGGER.info('Applying obfuscation rules')
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
            tap_stream_name_by_table_name = '{}-{}'.format(table_dict.get('schema_name'), table_dict.get('table_name'))
            if trans.get('tap_stream_name') == tap_stream_name_by_table_name:
                column = trans.get('field_id')
                transform_type = trans.get('type')
                if transform_type == 'SET-NULL':
                    trans_cols.append('{} = NULL'.format(column))
                elif transform_type == 'HASH':
                    trans_cols.append('{} = FUNC_SHA1({})'.format(column, column))
                elif 'HASH-SKIP-FIRST' in transform_type:
                    skip_first_n = transform_type[-1]
                    trans_cols.append('{} = CONCAT(SUBSTRING({}, 1, {}), FUNC_SHA1(SUBSTRING({}, {} + 1)))'.format(
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
        self.query('DROP TABLE IF EXISTS {}.{}'.format(schema, target_table))
        self.query('ALTER TABLE {}.{} RENAME TO {}'.format(schema, temp_table, target_table))
