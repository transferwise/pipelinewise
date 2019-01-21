import json
import boto3
import snowflake.connector
import singer
import collections
import inflection
import re
import itertools
import time

logger = singer.get_logger()


def column_type(schema_property):
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    if 'object' in property_type or 'array' in property_type:
        return 'object'
    elif property_format == 'date-time':
        return 'timestamp_ntz'
    elif 'number' in property_type:
        return 'float'
    elif 'integer' in property_type and 'string' in property_type:
        return 'text'
    elif 'integer' in property_type:
        return 'number'
    elif 'boolean' in property_type:
        return 'boolean'
    else:
        return 'text'


def inflect_column_name(name):
    return inflection.underscore(name)


def safe_column_name(name):
    return '"{}"'.format(name).upper()


def column_clause(name, schema_property):
    return '{} {}'.format(safe_column_name(name), column_type(schema_property))


def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    inflected_key = [inflect_column_name(n) for n in full_key]
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 63 and reducer_index < len(inflected_key):
        reduced_key = re.sub(r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)


def flatten_schema(d, parent_key=[], sep='__'):
    items = []
    for k, v in d['properties'].items():
        new_key = flatten_key(k, parent_key, sep)
        if 'type' in v.keys():
            if 'object' in v['type']:
                items.extend(flatten_schema(v, parent_key + [k], sep=sep).items())
            else:
                items.append((new_key, v))
        else:
            if len(v.values()) > 0:
                if list(v.values())[0][0]['type'] == 'string':
                    list(v.values())[0][0]['type'] = ['null', 'string']
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]['type'] == 'array':
                    list(v.values())[0][0]['type'] = ['null', 'array']
                    items.append((new_key, list(v.values())[0][0]))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError('Duplicate column name produced in schema: {}'.format(k))

    return dict(sorted_items)


def flatten_record(d, parent_key=[], sep='__'):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_record(v, parent_key + [k], sep=sep).items())
        else:
            items.append((new_key, json.dumps(v) if type(v) is list else v))
    return dict(items)


def primary_column_names(stream_schema_message):
    return [safe_column_name(inflect_column_name(p)) for p in stream_schema_message['key_properties']]

def stream_name_to_dict(stream_name, schema_name_postfix = None):
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
    s = stream_name.split('-')
    if len(s) > 1:
        postfix = "" if schema_name_postfix is None else schema_name_postfix
        schema_name = s[0] + postfix
        table_name = '_'.join(s[1:])

    return {
        'schema_name': schema_name,
        'table_name': table_name
    }

class DbSync:
    def __init__(self, connection_config, stream_schema_message=None):
        self.connection_config = connection_config

        # Target schema name can be defined in multiple ways:
        #
        #   1: 'schema' key : Target schema name defined explicitly
        #   2: 'dynamic_schema_name' key: Target schema name derived from the incoming stream id:
        #                                 i.e.: <schema_nama>-<table_name>
        if stream_schema_message is None:
            self.schema_name = None
        elif 'schema' in self.connection_config and self.connection_config['schema'].strip():
            self.schema_name = self.connection_config['schema']
        elif 'dynamic_schema_name' in self.connection_config and self.connection_config['dynamic_schema_name']:
            stream_name = stream_schema_message['stream']
            postfix = self.connection_config['dynamic_schema_name_postfix'] if 'dynamic_schema_name_postfix' in self.connection_config else None

            self.schema_name = stream_name_to_dict(stream_name, postfix)['schema_name']
        else:
            raise Exception("Target schema name not defined in config. Neither 'schema' (string) nor 'dynamic_schema_name' (boolean) keys set in config.")

        self.stream_schema_message = stream_schema_message

        if stream_schema_message is not None:
            self.flatten_schema = flatten_schema(stream_schema_message['schema'])

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
            warehouse=self.connection_config['warehouse'],
            # Use insecure mode to avoid "Failed to get OCSP response" warnings
            #
            # Further info: https://snowflakecommunity.force.com/s/question/0D50Z00008AEhWbSAL/python-snowflake-connector-ocsp-response-warning-message
            # Snowflake is changing certificate authority
            insecure_mode=True
        )

    def query(self, query, params=None):
        logger.info("SNOWFLAKE - Running query: {}".format(query))
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

    def table_name(self, stream_name, is_temporary, without_schema = False):
        stream_dict = stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        sf_table_name = table_name.replace('.', '_').replace('-', '_').lower()

        if is_temporary:
            sf_table_name =  '{}_temp'.format(sf_table_name)

        if without_schema:
            return '{}'.format(sf_table_name)
        else:
            return '{}.{}'.format(self.schema_name, sf_table_name)

    def record_primary_key_string(self, record):
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        flatten = flatten_record(record)
        key_props = [str(flatten[inflect_column_name(p)]) for p in self.stream_schema_message['key_properties']]
        return ','.join(key_props)

    def record_to_csv_line(self, record):
        flatten = flatten_record(record)
        return ','.join(
            [
                json.dumps(flatten[name]) if name in flatten and (flatten[name] == 0 or flatten[name]) else ''
                for name in self.flatten_schema
            ]
        )

    def put_to_stage(self, file, stream, count):
        logger.info("Uploading {} rows to external snowflake stage on S3".format(count))

        # Generating key in S3 bucket 
        bucket = self.connection_config['s3_bucket']
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        s3_key = "{}pipelinewise_{}_{}.csv".format(s3_key_prefix, stream, time.strftime("%Y%m%d-%H%M%S"))
        logger.info("Target S3 bucket: {}, local file: {}, S3 key: {}".format(bucket, file.name, s3_key))

        # Seek to the beginning of the file to upload everything
        file.seek(0)

        # Connect and upload
        self.s3.upload_fileobj(file, bucket, s3_key)
        return s3_key


    def delete_from_stage(self, s3_key):
        logger.info("Deleting {} from external snowflake stage on S3".format(s3_key))
        bucket = self.connection_config['s3_bucket']
        self.s3.delete_object(Bucket=bucket, Key=s3_key)


    def load_csv(self, s3_key, count):
        inserted = 0
        updated = 0
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        logger.info("Loading {} rows into '{}'".format(count, self.table_name(stream, False)))

        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(self.create_table_query(True))
                copy_sql = """COPY INTO {} ({}) FROM 's3://{}/{}'
                    CREDENTIALS = (aws_key_id='{}' aws_secret_key='{}') 
                    FILE_FORMAT = (type='CSV' escape='\\\\' field_optionally_enclosed_by='\"')
                """.format(
                    self.table_name(stream, True),
                    ', '.join(self.column_names()),
                    self.connection_config['s3_bucket'],
                    s3_key,
                    self.connection_config['aws_access_key_id'],
                    self.connection_config['aws_secret_access_key']
                )
                cur.execute(copy_sql)
                
                if len(self.stream_schema_message['key_properties']) > 0:
                    cur.execute(self.update_from_temp_table())
                    updated = cur._total_rowcount
                cur.execute(self.insert_from_temp_table())
                inserted = cur._total_rowcount

                logger.info("INSERTED {} rows, UPDATED {} rows".format(inserted, updated))
                cur.execute(self.drop_temp_table())

    def insert_from_temp_table(self):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'], False)
        temp_table = self.table_name(stream_schema_message['stream'], True)

        if len(stream_schema_message['key_properties']) == 0:
            return """INSERT INTO {} ({})
                    (SELECT s.* FROM {} s)
                    """.format(
                table,
                ', '.join(columns),
                temp_table
            )

        return """INSERT INTO {} ({})
        (SELECT s.* FROM {} s LEFT OUTER JOIN {} t ON {} WHERE {})
        """.format(
            table,
            ', '.join(columns),
            temp_table,
            table,
            self.primary_key_condition('t'),
            self.primary_key_null_condition('t')
        )

    def update_from_temp_table(self):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'], False)
        temp_table = self.table_name(stream_schema_message['stream'], True)
        return """UPDATE {} SET {} FROM {} s
        WHERE {}
        """.format(
            table,
            ', '.join(['{}=s.{}'.format(c, c) for c in columns]),
            temp_table,
            self.primary_key_condition(table)
        )

    def primary_key_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['s.{} = {}.{}'.format(c, right_table, c) for c in names])

    def primary_key_null_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['{}.{} is null'.format(right_table, c) for c in names])

    def drop_temp_table(self):
        stream_schema_message = self.stream_schema_message
        temp_table = self.table_name(stream_schema_message['stream'], True)
        return "DROP TABLE {}".format(temp_table)

    def column_names(self):
        return [safe_column_name(name) for name in self.flatten_schema]

    def create_table_query(self, is_temporary=False):
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        primary_key = ["PRIMARY KEY ({})".format(', '.join(primary_column_names(stream_schema_message)))] \
            if len(stream_schema_message['key_properties']) else []

        return 'CREATE {}TABLE {} ({})'.format(
            'TEMP ' if is_temporary else '',
            self.table_name(stream_schema_message['stream'], is_temporary),
            ', '.join(columns + primary_key)
        )

    def grant_usage_on_schema(self, schema_name, grantee):
        query = "GRANT USAGE ON SCHEMA {} TO ROLE {}".format(schema_name, grantee)
        logger.info("Granting USAGE privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    def grant_select_on_all_tables_in_schema(self, schema_name, grantee):
        query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ROLE {}".format(schema_name, grantee)
        logger.info("Granting SELECT ON ALL TABLES privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    def grant_privilege(self, schema, grantees, grant_method):
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(schema, grantee)
        elif isinstance(grantees, str):
            grant_method(schema, grantees)

    def delete_rows(self, stream):
        table = self.table_name(stream, False)
        query = "DELETE FROM {} WHERE _sdc_deleted_at IS NOT NULL".format(table)
        logger.info("Deleting rows from '{}' table... {}".format(table, query))
        logger.info("DELETE {}".format(len(self.query(query))))

    def create_schema_if_not_exists(self, table_columns_cache=None):
        schema_name = self.schema_name
        schema_rows = 0

        # table_columns_cache is an optional pre-collected list of available objects in snowflake
        if table_columns_cache:
            schema_rows = list(filter(lambda x: x['TABLE_SCHEMA'] == schema_name, table_columns_cache))
        # Query realtime if not pre-collected
        else:
            schema_rows = self.query(
                'SELECT LOWER(schema_name) schema_name FROM information_schema.schemata WHERE LOWER(schema_name) = %s',
                (schema_name.lower(),)
            )

        if len(schema_rows) == 0:
            query = "CREATE SCHEMA IF NOT EXISTS {}".format(schema_name)
            logger.info("Schema '{}' does not exist. Creating... {}".format(schema_name, query))
            self.query(query)

            if 'grant_select_to' in self.connection_config:
                grant_select_to = self.connection_config['grant_select_to']
                self.grant_privilege(schema_name, grant_select_to, self.grant_usage_on_schema)

    def get_tables(self, table_schema=None):
        print("ABCD")
        return self.query("""SELECT LOWER(table_schema) table_schema, LOWER(table_name) table_name
            FROM information_schema.tables
            WHERE LOWER(table_schema) = {}""".format(
                "LOWER(table_schema)" if table_schema is None else "'{}'".format(table_schema.lower())
        ))

    def get_table_columns(self, table_schema=None, table_name=None):
        return self.query("""SELECT LOWER(table_schema) table_schema, LOWER(table_name) table_name, column_name, data_type
            FROM information_schema.columns
            WHERE LOWER(table_schema) = {} AND LOWER(table_name) = {}""".format(
                "LOWER(table_schema)" if table_schema is None else "'{}'".format(table_schema.lower()),
                "LOWER(table_name)" if table_name is None else "'{}'".format(table_name.lower())
        ))

    def update_columns(self, table_columns_cache=None):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, False, True)
        schema_name = self.schema_name
        columns = {}
        if table_columns_cache:
            columns = list(filter(lambda x: x['TABLE_SCHEMA'] == self.schema_name.lower() and x['TABLE_NAME'].lower() == table_name, table_columns_cache))
        else:
            columns = self.get_table_columns(schema_name, table_name)
        columns_dict = {column['COLUMN_NAME'].lower(): column for column in columns}

        columns_to_add = [
            column_clause(
                name,
                properties_schema
            )
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() not in columns_dict
        ]

        for column in columns_to_add:
            self.add_column(column, stream)

        columns_to_replace = [
            (safe_column_name(name), column_clause(
                name,
                properties_schema
            ))
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() in columns_dict and
               columns_dict[name.lower()]['DATA_TYPE'].lower() != column_type(properties_schema).lower()
        ]

        for (column_name, column) in columns_to_replace:
            self.drop_column(column_name, stream)
            self.add_column(column, stream)

    def add_column(self, column, stream):
        add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.table_name(stream, False), column)
        logger.info('Adding column: {}'.format(add_column))
        self.query(add_column)

    def drop_column(self, column_name, stream):
        drop_column = "ALTER TABLE {} DROP COLUMN {}".format(self.table_name(stream, False), column_name)
        logger.info('Dropping column: {}'.format(drop_column))
        self.query(drop_column)

    def sync_table(self, table_columns_cache=None):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, False, True)
        table_name_with_schema = self.table_name(stream, False)
        found_tables = []

        if table_columns_cache:
            found_tables = list(filter(lambda x: x['TABLE_SCHEMA'] == self.schema_name.lower() and x['TABLE_NAME'].lower() == table_name, table_columns_cache))
        else:
            found_tables = [table for table in (self.get_tables(self.schema_name.lower())) if table['TABLE_NAME'].lower() == table_name]

        if len(found_tables) == 0:
            query = self.create_table_query()
            logger.info("Table '{}' does not exist. Creating...".format(table_name_with_schema))
            self.query(query)

            if 'grant_select_to' in self.connection_config:
                grant_select_to = self.connection_config['grant_select_to']
                self.grant_privilege(self.schema_name, grant_select_to, self.grant_select_on_all_tables_in_schema)
        else:
            logger.info("Table '{}' exists".format(table_name_with_schema))
            self.update_columns(table_columns_cache)

