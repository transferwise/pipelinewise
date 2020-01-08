import psycopg2
import psycopg2.extras

from . import utils


#pylint: disable=missing-function-docstring,no-self-use,too-many-arguments
class FastSyncTargetPostgres:
    """
    Common functions for fastsync to Postgres
    """

    def __init__(self, connection_config, transformation_config=None):
        self.connection_config = connection_config
        self.transformation_config = transformation_config

    def open_connection(self):
        conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
            self.connection_config['host'], self.connection_config['dbname'], self.connection_config['user'],
            self.connection_config['password'], self.connection_config['port'])

        return psycopg2.connect(conn_string)

    def query(self, query, params=None):
        utils.log('POSTGRES - Running query: {}'.format(query))
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)

                if cur.rowcount > 0 and cur.description:
                    return cur.fetchall()

                return []

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

        #if primary_key:
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
        utils.log('POSTGRES - Loading {} into Redshift...'.format(s3_key))
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        aws_access_key_id = self.connection_config['aws_access_key_id']
        aws_secret_access_key = self.connection_config['aws_secret_access_key']
        bucket = self.connection_config['s3_bucket']

        sql = """COPY {}.{} FROM 's3://{}/{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            DELIMITER ',' REMOVEQUOTES ESCAPE
            BLANKSASNULL TIMEFORMAT 'auto'
            GZIP
        """.format(target_schema, target_table, bucket, s3_key, aws_access_key_id, aws_secret_access_key)
        self.query(sql)

        utils.log('POSTGRES - Deleting {} from S3...'.format(s3_key))
        #self.s3.delete_object(Bucket=bucket, Key=s3_key)

    # grant_... functions are common functions called by utils.py: grant_privilege function
    # "to_group" is not used here but exists for compatibility reasons with other database types
    # "to_group" is for databases that can grant to users and groups separately like Amazon Redshift
    # pylint: disable=unused-argument
    def grant_select_on_table(self, target_schema, table_name, role, is_temporary, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
            sql = 'GRANT SELECT ON {}.{} TO GROUP {}'.format(target_schema, target_table, role)
            self.query(sql)

    # pylint: disable=unused-argument
    def grant_usage_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT USAGE ON SCHEMA {} TO GROUP {}'.format(target_schema, role)
            self.query(sql)

    # pylint: disable=unused-argument
    def grant_select_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT SELECT ON ALL TABLES IN SCHEMA {} TO GROUP {}'.format(target_schema, role)
            self.query(sql)

    # pylint: disable=duplicate-string-formatting-argument
    def obfuscate_columns(self, target_schema, table_name):
        utils.log('POSTGRES - Applying obfuscation rules')
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
