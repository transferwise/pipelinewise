import json
import psycopg2
import psycopg2.extras
import singer
import collections
import inflection
import re
import itertools

logger = singer.get_logger()


def column_type(schema_property):
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    if 'object' in property_type or 'array' in property_type:
        return 'jsonb'
    elif property_format == 'date-time':
        return 'timestamp without time zone'
    elif 'number' in property_type:
        return 'numeric'
    elif 'integer' in property_type and 'string' in property_type:
        return 'character varying'
    elif 'integer' in property_type:
        return 'bigint'
    elif 'boolean' in property_type:
        return 'boolean'
    else:
        return 'character varying'


def inflect_column_name(name):
    return inflection.underscore(name)


def safe_column_name(name):
    return '"{}"'.format(name)


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


class DbSync:
    def __init__(self, connection_config, stream_schema_message):
        self.connection_config = connection_config
        self.schema_name = self.connection_config['schema']
        self.stream_schema_message = stream_schema_message
        self.flatten_schema = flatten_schema(stream_schema_message['schema'])

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
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    query,
                    params
                )

                if cur.rowcount > 0:
                    return cur.fetchall()
                else:
                    return []

    def copy_from(self, file, table):
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.copy_from(file, table)

    def table_name(self, table_name, is_temporary):
        if is_temporary:
            return '{}_temp'.format(table_name)
        else:
            return '{}.{}'.format(self.schema_name, table_name)

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

    def load_csv(self, file, count):
        file.seek(0)
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        logger.info("Loading {} rows into '{}'".format(count, stream))

        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(self.create_table_query(True))
                copy_sql = "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, ESCAPE '\\')".format(
                    self.table_name(stream, True),
                    ', '.join(self.column_names())
                )
                logger.info(copy_sql)
                cur.copy_expert(
                    copy_sql,
                    file
                )
                if len(self.stream_schema_message['key_properties']) > 0:
                    cur.execute(self.update_from_temp_table())
                    logger.info(cur.statusmessage)
                cur.execute(self.insert_from_temp_table())
                logger.info(cur.statusmessage)
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
        query = "GRANT USAGE ON SCHEMA {} TO GROUP {}".format(schema_name, grantee)
        logger.info("Granting USAGE privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    def grant_select_on_all_tables_in_schema(self, schema_name, grantee):
        query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO GROUP {}".format(schema_name, grantee)
        logger.info("Granting SELECT ON ALL TABLES privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    def grant_privilege(self, schema, grantees, grant_method):
        if isinstance(grantees, list):
            map(lambda grantee:grant_method(schema,grantee), grantees)
        elif isinstance(grantees, str):
            grant_method(schema, grantees)

    def create_schema_if_not_exists(self):
        schema_name = self.connection_config['schema']
        schema_rows = self.query(
            'SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s',
            (schema_name,)
        )

        if len(schema_rows) == 0:
            query = "CREATE SCHEMA IF NOT EXISTS {}".format(schema_name)
            logger.info("Schema '{}' does not exist. Creating... {}".format(schema_name, query))
            self.query(query)

            if 'grant_select_to' in self.connection_config:
                grant_select_to = self.connection_config['grant_select_to']
                self.grant_privilege(schema_name, grant_select_to, self.grant_usage_on_schema)

    def get_tables(self):
        return self.query(
            'SELECT table_name FROM information_schema.tables WHERE table_schema = %s',
            (self.schema_name,)
        )

    def get_table_columns(self, table_name):
        return self.query("""SELECT column_name, data_type
      FROM information_schema.columns
      WHERE lower(table_name) = %s AND lower(table_schema) = %s""", (table_name.lower(), self.schema_name.lower()))

    def update_columns(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        columns = self.get_table_columns(stream)
        columns_dict = {column['column_name'].lower(): column for column in columns}

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
               columns_dict[name.lower()]['data_type'].lower() != column_type(properties_schema).lower()
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

    def sync_table(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        found_tables = [table for table in (self.get_tables()) if table['table_name'].lower() == stream.lower()]
        if len(found_tables) == 0:
            query = self.create_table_query()
            logger.info("Table '{}' does not exist. Creating... {}".format(stream, query))
            self.query(query)

            if 'grant_select_to' in self.connection_config:
                grant_select_to = self.connection_config['grant_select_to']
                self.grant_privilege(self.schema_name, grant_select_to, self.grant_select_on_all_tables_in_schema)
        else:
            logger.info("Table '{}' exists".format(stream))
            self.update_columns()

