import json
import sys
import psycopg2
import psycopg2.extras
import inflection
import re
import uuid
import itertools
from datetime import datetime, timedelta, timezone
from collections.abc import MutableMapping
from singer import get_logger
from singer.decimal_support import (
    decimal_float_key,
    decimal_float_value,
    decimal_key,
    decimal_sql_type,
    is_decimal_schema,
    postgres_numeric_scale,
)


def validate_config(config):
    errors = []
    required_config_keys = [
        'host',
        'port',
        'user',
        'password',
        'dbname'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    # Check target schema config
    config_default_target_schema = config.get('default_target_schema', None)
    config_schema_mapping = config.get('schema_mapping', None)
    if not config_default_target_schema and not config_schema_mapping:
        errors.append("Neither 'default_target_schema' (string) nor 'schema_mapping' (object) keys set in config.")

    return errors


def column_type(schema_property, postgres_version=None):
    if is_decimal_schema(schema_property):
        return decimal_sql_type(schema_property, 'postgres', postgres_version=postgres_version)
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    col_type = 'character varying'
    if 'object' in property_type or 'array' in property_type:
        col_type = 'jsonb'

    # Every date-time JSON value is currently mapped to TIMESTAMP WITHOUT TIME ZONE
    #
    # TODO: Detect if timezone postfix exists in the JSON and find if TIMESTAMP WITHOUT TIME ZONE or
    # TIMESTAMP WITH TIME ZONE is the better column type
    elif property_format == 'date-time':
        col_type = 'timestamp without time zone'
    elif property_format == 'date':
        col_type = 'date'
    elif property_format == 'time':
        col_type = 'time without time zone'
    elif 'number' in property_type:
        col_type = 'double precision'
    elif 'integer' in property_type and 'string' in property_type:
        col_type = 'character varying'
    elif 'integer' in property_type:
        if 'maximum' in schema_property:
            if schema_property['maximum'] <= 32767:
                col_type = 'smallint'
            elif schema_property['maximum'] <= 2147483647:
                col_type = 'integer'
            elif schema_property['maximum'] <= 9223372036854775807:
                col_type = 'bigint'
        else:
            col_type = 'numeric'
    elif 'boolean' in property_type:
        col_type = 'boolean'

    get_logger('target_postgres').debug("schema_property: %s -> col_type: %s", schema_property, col_type)

    return col_type


def safe_column_name(name):
    return '"{}"'.format(name).lower()


def column_clause(name, schema_property, postgres_version=None, retained_type=None):
    return '{} {}'.format(
        safe_column_name(name),
        retained_type or column_type(schema_property, postgres_version),
    )


def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    inflected_key = full_key.copy()
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 63 and reducer_index < len(inflected_key):
        reduced_key = re.sub(r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)


def flatten_schema(d, parent_key=[], sep='__', level=0, max_level=0):
    items = []

    if 'properties' not in d:
        return {}

    for k, v in d['properties'].items():
        new_key = flatten_key(k, parent_key, sep)
        if 'type' in v.keys():
            if 'object' in v['type'] and 'properties' in v and level < max_level:
                items.extend(flatten_schema(v, parent_key + [k], sep=sep, level=level + 1, max_level=max_level).items())
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
                elif list(v.values())[0][0]['type'] == 'object':
                    list(v.values())[0][0]['type'] = ['null', 'object']
                    items.append((new_key, list(v.values())[0][0]))

    def key_func(item):
        return item[0]

    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError('Duplicate column name produced in schema: {}'.format(k))

    return dict(sorted_items)


def _should_json_dump_value(key, value, flatten_schema=None):
    if isinstance(value, (dict, list)):
        return True

    if flatten_schema and key in flatten_schema and 'type' in flatten_schema[key]\
            and set(flatten_schema[key]['type']) == {'null', 'object', 'array'}:
        return True

    return False


def flatten_record(d, flatten_schema=None, parent_key=[], sep='__', level=0, max_level=0):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, MutableMapping) and level < max_level:
            items.extend(flatten_record(v, flatten_schema, parent_key + [k], sep=sep, level=level + 1,
                                        max_level=max_level).items())
        else:
            items.append((new_key, json.dumps(v) if _should_json_dump_value(k, v, flatten_schema) else v))
    return dict(items)


def primary_column_names(stream_schema_message):
    return [safe_column_name(p) for p in stream_schema_message['key_properties']]


def stream_name_to_dict(stream_name, separator='-'):
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
    s = stream_name.split(separator)
    if len(s) == 2:
        schema_name = s[0]
        table_name = s[1]
    if len(s) > 2:
        catalog_name = s[0]
        schema_name = s[1]
        table_name = '_'.join(s[2:])

    return {
        'catalog_name': catalog_name,
        'schema_name': schema_name,
        'table_name': table_name
    }


class DbSync:
    def __init__(self, connection_config, stream_schema_message=None):
        """
            connection_config:      Postgres connection details

            stream_schema_message:  An instance of the DbSync class is typically used to load
                                    data only from a certain singer tap stream.

                                    The stream_schema_message holds the destination schema
                                    name and the JSON schema that will be used to
                                    validate every RECORDS messages that comes from the stream.
                                    Schema validation happening before creating CSV and before
                                    uploading data into Postgres.

                                    If stream_schema_message is not defined then we can use
                                    the DbSync instance as a generic purpose connection to
                                    Postgres and can run individual queries. For example
                                    collecting catalog information from Postgres for caching
                                    purposes.
        """
        self.connection_config = connection_config
        self.stream_schema_message = stream_schema_message

        # logger to be used across the class's methods
        self.logger = get_logger('target_postgres')

        # Validate connection configuration
        config_errors = validate_config(connection_config)

        # Exit if config has errors
        if len(config_errors) > 0:
            self.logger.error("Invalid configuration:\n   * %s", '\n   * '.join(config_errors))
            sys.exit(1)

        self.schema_name = None
        self.grantees = None

        # Init stream schema
        if stream_schema_message is not None:
            # Define initial list of indices to created
            self.indices = ['_sdc_deleted_at']

            #  Define target schema name.
            #  --------------------------
            #  Target schema name can be defined in multiple ways:
            #
            #   1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
            #       not specified explicitly for a given stream in the `schema_mapping` object
            #   2: 'schema_mapping' key : Target schema defined explicitly for a given stream.
            #       Example config.json:
            #           "schema_mapping": {
            #               "my_tap_stream_id": {
            #                   "target_schema": "my_postgres_schema",
            #                   "target_schema_select_permissions": [ "role_with_select_privs" ],
            #                   "indices": ["column_1", "column_2s"]
            #               }
            #           }
            config_default_target_schema = self.connection_config.get('default_target_schema', '').strip()
            config_schema_mapping = self.connection_config.get('schema_mapping', {})

            stream_name = stream_schema_message['stream']
            stream_schema_name = stream_name_to_dict(stream_name)['schema_name']
            stream_table_name = stream_name_to_dict(stream_name)['table_name']
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.schema_name = config_schema_mapping[stream_schema_name].get('target_schema')

                # Get indices to create for the target table
                indices = config_schema_mapping[stream_schema_name].get('indices', {})
                if stream_table_name in indices:
                    self.indices.extend(indices.get(stream_table_name, []))

            elif config_default_target_schema:
                self.schema_name = config_default_target_schema

            if not self.schema_name:
                raise Exception("Target schema name not defined in config. Neither 'default_target_schema' (string)"
                                "nor 'schema_mapping' (object) defines target schema for {} stream."
                                .format(stream_name))

            #  Define grantees
            #  ---------------
            #  Grantees can be defined in multiple ways:
            #
            #   1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on
            #       every table to a given role for every incoming stream if not specified explicitly in the
            #       `schema_mapping` object
            #   2: 'target_schema_select_permissions' key : Roles to grant USAGE and SELECT privileges defined
            #       explicitly for a given stream.
            #           Example config.json:
            #               "schema_mapping": {
            #                   "my_tap_stream_id": {
            #                       "target_schema": "my_postgres_schema",
            #                       "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                   }
            #               }
            self.grantees = self.connection_config.get('default_target_schema_select_permissions')
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.grantees = config_schema_mapping[stream_schema_name].get('target_schema_select_permissions',
                                                                              self.grantees)

            self.data_flattening_max_level = self.connection_config.get('data_flattening_max_level', 0)
            self.flatten_schema = flatten_schema(stream_schema_message['schema'],
                                                 max_level=self.data_flattening_max_level)
            self.retained_column_types = {}
            self.effective_key_properties = list(stream_schema_message['key_properties'])

    def open_connection(self):
        conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
            self.connection_config['host'],
            self.connection_config['dbname'],
            self.connection_config['user'],
            self.connection_config['password'],
            self.connection_config['port']
        )

        if 'ssl' in self.connection_config and self.connection_config['ssl'] == 'true':
            conn_string += " sslmode='require'"

        return psycopg2.connect(conn_string)

    def query(self, query, params=None):
        self.logger.debug("Running query: %s", query)
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    query,
                    params
                )

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []

    def table_name(self, stream_name, is_temporary=False, without_schema=False):
        stream_dict = stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        pg_table_name = table_name.replace('.', '_').replace('-', '_').lower()

        if is_temporary:
            return 'tmp_{}'.format(str(uuid.uuid4()).replace('-', '_'))

        if without_schema:
            return f'"{pg_table_name.lower()}"'

        return f'{self.schema_name}."{pg_table_name.lower()}"'

    def record_primary_key_string(self, record):
        key_properties = self.primary_key_properties()
        if not key_properties:
            return None
        flatten = flatten_record(record, self.flatten_schema, max_level=self.data_flattening_max_level)
        try:
            key_props = [
                self._record_key_value(p, flatten[p])
                for p in key_properties
            ]
        except Exception as exc:
            self.logger.info("Cannot find %s primary key(s) in record: %s",
                             key_properties,
                             flatten)
            raise exc
        return key_props[0] if len(key_props) == 1 else json.dumps(key_props, ensure_ascii=False)

    def primary_key_properties(self):
        return getattr(self, 'effective_key_properties', self.stream_schema_message['key_properties'])

    def _record_key_value(self, name, value):
        value = self._binary_key_value(name, value)
        if not is_decimal_schema(self.flatten_schema.get(name)):
            return str(value)
        retained_type = getattr(self, 'retained_column_types', {}).get(name.lower())
        if retained_type in ('real', 'double precision'):
            return decimal_float_key(value, retained_type)
        return decimal_key(value)

    def record_to_csv_line(self, record):
        flatten = flatten_record(record, self.flatten_schema, max_level=self.data_flattening_max_level)
        return ','.join(
            [
                json.dumps(self._retained_decimal_value(name, self._binary_key_value(name, flatten[name])),
                           ensure_ascii=False)
                if name in flatten and (flatten[name] == 0 or flatten[name]) else ''
                for name in self.flatten_schema
            ]
        )

    def _binary_key_value(self, name, value):
        if (getattr(self, 'connection_config', {}).get('source_tap_type') == 'tap-mysql'
                and name in self.primary_key_properties()
                and self.flatten_schema.get(name, {}).get('format') == 'binary'
                and isinstance(value, str)):
            return value.upper()
        return value

    def _retained_decimal_value(self, name, value):
        retained_type = getattr(self, 'retained_column_types', {}).get(name.lower())
        if retained_type not in ('real', 'double precision'):
            return value
        return decimal_float_value(value, retained_type)

    def load_csv(self, file, count, size_bytes):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        self.logger.info("Loading %d rows into '%s'", count, self.table_name(stream, False))

        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                inserts = 0
                updates = 0

                temp_table = self.table_name(stream_schema_message['stream'], is_temporary=True)
                cur.execute(self.create_table_query(table_name=temp_table, is_temporary=True))

                copy_sql = "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, ESCAPE '\\')".format(
                    temp_table,
                    ', '.join(self.column_names())
                )
                self.logger.debug(copy_sql)
                with open(file, "rb") as f:
                    cur.copy_expert(copy_sql, f)
                if self.primary_key_properties():
                    cur.execute(self.update_from_temp_table(temp_table))
                    updates = cur.rowcount
                cur.execute(self.insert_from_temp_table(temp_table))
                inserts = cur.rowcount

                self.logger.info('Loading into %s: %s',
                                 self.table_name(stream, False),
                                 json.dumps({'inserts': inserts, 'updates': updates, 'size_bytes': size_bytes}))

    def insert_from_temp_table(self, temp_table):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'])

        if not self.primary_key_properties():
            return """INSERT INTO {} ({})
                    (SELECT s.* FROM {} s)
                    """.format(table,
                               ', '.join(columns),
                               temp_table)

        return """INSERT INTO {} ({})
        (SELECT s.* FROM {} s LEFT OUTER JOIN {} t ON {} WHERE {})
        """.format(table,
                   ', '.join(columns),
                   temp_table,
                   table,
                   self.primary_key_condition('t'),
                   self.primary_key_null_condition('t'))

    def update_from_temp_table(self, temp_table):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'])

        return """UPDATE {} SET {} FROM {} s
        WHERE {}
        """.format(table,
                   ', '.join(['{}=s.{}'.format(c, c) for c in columns]),
                   temp_table,
                   self.primary_key_condition(table))

    def primary_key_condition(self, right_table):
        names = [safe_column_name(name) for name in self.primary_key_properties()]
        return ' AND '.join(['s.{} = {}.{}'.format(c, right_table, c) for c in names])

    def primary_key_null_condition(self, right_table):
        names = [safe_column_name(name) for name in self.primary_key_properties()]
        return ' AND '.join(['{}.{} is null'.format(right_table, c) for c in names])

    def column_names(self):
        return [safe_column_name(name) for name in self.flatten_schema]

    def create_table_query(self, table_name=None, is_temporary=False):
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema,
                self.decimal_postgres_version(),
                getattr(self, 'retained_column_types', {}).get(name.lower()) if is_temporary else None,
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        keys = [safe_column_name(name) for name in self.primary_key_properties()]
        primary_key = ["PRIMARY KEY ({})".format(', '.join(keys))] if keys else []

        if not table_name:
            gen_table_name = self.table_name(stream_schema_message['stream'], is_temporary=is_temporary)

        return 'CREATE {}TABLE IF NOT EXISTS {} ({})'.format(
            'TEMP ' if is_temporary else '',
            table_name if table_name else gen_table_name,
            ', '.join(columns + primary_key)
        )

    def grant_usage_on_schema(self, schema_name, grantee):
        query = "GRANT USAGE ON SCHEMA {} TO GROUP {}".format(schema_name, grantee)
        self.logger.info("Granting USAGE privilege on '%s' schema to '%s'... %s", schema_name, grantee, query)
        self.query(query)

    def grant_select_on_all_tables_in_schema(self, schema_name, grantee):
        query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO GROUP {}".format(schema_name, grantee)
        self.logger.info("Granting SELECT ON ALL TABLES privilege on '%s' schema to '%s'... %s",
                         schema_name,
                         grantee,
                         query)
        self.query(query)

    @classmethod
    def grant_privilege(cls, schema, grantees, grant_method):
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(schema, grantee)
        elif isinstance(grantees, str):
            grant_method(schema, grantees)

    def create_index(self, stream, column):
        table = self.table_name(stream)
        table_without_schema = self.table_name(stream, without_schema=True)
        index_name = 'i_{}_{}'.format(table_without_schema[:30].replace(' ', '').replace('"', ''),
                                      column.replace(',', '_'))
        query = "CREATE INDEX IF NOT EXISTS {} ON {} ({})".format(index_name, table, column)
        self.logger.info("Creating index on '%s' table on '%s' column(s)... %s", table, column, query)
        self.query(query)

    def create_indices(self, stream):
        if isinstance(self.indices, list):
            for index in self.indices:
                self.create_index(stream, index)

    def delete_rows(self, stream):
        table = self.table_name(stream)
        query = "DELETE FROM {} WHERE _sdc_deleted_at IS NOT NULL".format(table)
        self.logger.info("Deleting rows from '%s' table... %s", table, query)
        self.logger.debug("Running query: %s", query)
        with self.open_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(query)
                deleted_rows = cur.rowcount
        self.logger.info("DELETE %s", deleted_rows)

    def create_schema_if_not_exists(self, table_columns_cache=None):
        schema_name = self.schema_name
        schema_rows = 0

        # table_columns_cache is an optional pre-collected list of available objects in postgres
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
            self.logger.info("Schema '%s' does not exist. Creating... %s", schema_name, query)
            self.query(query)

            self.grant_privilege(schema_name, self.grantees, self.grant_usage_on_schema)

    def get_tables(self):
        return self.query(
            'SELECT table_name FROM information_schema.tables WHERE table_schema = %s',
            (self.schema_name,)
        )

    def get_table_columns(self, table_name):
        return self.query("""SELECT column_name, data_type, numeric_precision, numeric_scale
      FROM information_schema.columns
      WHERE lower(table_name) = %s AND lower(table_schema) = %s""", (table_name.replace("\"", "").lower(),
                                                                     self.schema_name.lower()))

    def _retain_existing_primary_key(self, table_name):
        if getattr(self, 'connection_config', {}).get('source_tap_type') != 'tap-mysql':
            return
        rows = self.query('''SELECT keys.column_name
            FROM information_schema.table_constraints AS constraints
            JOIN information_schema.key_column_usage AS keys
              ON keys.constraint_catalog = constraints.constraint_catalog
             AND keys.constraint_schema = constraints.constraint_schema
             AND keys.constraint_name = constraints.constraint_name
             AND keys.table_schema = constraints.table_schema
             AND keys.table_name = constraints.table_name
            WHERE constraints.constraint_type = 'PRIMARY KEY'
              AND constraints.table_schema = %s AND constraints.table_name = %s
            ORDER BY keys.ordinal_position''', (self.schema_name, table_name.strip('"')))
        stored_keys = {row['column_name'].lower() for row in rows}
        incoming_keys = self.stream_schema_message['key_properties']
        if stored_keys and stored_keys < {name.lower() for name in incoming_keys}:
            # Legacy Singer excluded unsupported MySQL keys; NULL history cannot match the expanded key.
            self.effective_key_properties = [name for name in incoming_keys if name.lower() in stored_keys]
            self.logger.warning(
                'Retaining existing primary key %s for %s; FullSync adopts the complete source key %s',
                self.effective_key_properties, self.stream_schema_message['stream'], incoming_keys,
            )

    def update_columns(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, without_schema=True)
        columns = self.get_table_columns(table_name)
        self._retain_existing_primary_key(table_name)
        columns_dict = {column['column_name'].lower(): column for column in columns}
        mysql_year_keys = {
            name.lower() for name in self.primary_key_properties()
            if getattr(self, 'connection_config', {}).get('source_tap_type') == 'tap-mysql'
            and self.flatten_schema.get(name, {}).get('format') == 'singer.year'
        }
        self.retained_column_types = {
            name.lower(): columns_dict[name.lower()]['data_type']
            for name, properties_schema in self.flatten_schema.items()
            if name.lower() in columns_dict
            and (name.lower() in mysql_year_keys or (
                is_decimal_schema(properties_schema)
                and columns_dict[name.lower()]['data_type'].lower() in ('double precision', 'real')
            ))
        }

        columns_to_add = [
            column_clause(
                name,
                properties_schema,
                self.decimal_postgres_version()
            )
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() not in columns_dict
        ]

        columns_to_replace = [
            (safe_column_name(name), column_clause(
                name,
                properties_schema,
                self.decimal_postgres_version()
            ))
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() in columns_dict and
            name.lower() not in self.retained_column_types and
            not self._column_type_matches(
                columns_dict[name.lower()], properties_schema, self.decimal_postgres_version(),
            )
        ]

        primary_keys = {
            safe_column_name(name) for name in stream_schema_message['key_properties']
            if is_decimal_schema(self.flatten_schema.get(name))
        }
        if any(name in primary_keys for name, _ in columns_to_replace):
            raise ValueError('Cannot version a primary-key column; recreate the table before changing its type')

        for column in columns_to_add:
            self.add_column(column, stream)

        existing_names = set(columns_dict) | {name.lower() for name in self.flatten_schema}
        for (column_name, column) in columns_to_replace:
            archived_name = self.version_column(column_name, stream, existing_names)
            existing_names.add(archived_name)
            self.add_column(column, stream)

    def decimal_postgres_version(self):
        """Probe server capabilities only for declarations introduced in PostgreSQL 15."""
        if hasattr(self, '_decimal_postgres_version'):
            return self._decimal_postgres_version
        needs_version = any(
            is_decimal_schema(schema) and schema['decimal']['scale'] is not None
            and (schema['decimal']['scale'] < 0 or schema['decimal']['scale'] > schema['decimal']['precision'])
            for schema in self.flatten_schema.values()
        )
        self._decimal_postgres_version = (
            int(self.query('SHOW server_version_num')[0]['server_version_num']) if needs_version else None
        )
        return self._decimal_postgres_version

    @staticmethod
    def _column_type_matches(column, schema_property, postgres_version=None):
        """Compare decimal dimensions without changing unrelated legacy type rules."""
        expected = column_type(schema_property, postgres_version).lower()
        current = column['data_type'].lower()
        if is_decimal_schema(schema_property) and current in ('double precision', 'real'):
            return True
        if is_decimal_schema(schema_property) and current in ('numeric', 'decimal'):
            precision = column.get('numeric_precision')
            scale = postgres_numeric_scale(column.get('numeric_scale'))
            if precision is None and scale is None:
                current = 'numeric'
            elif precision is None or scale is None:
                raise ValueError('PostgreSQL returned incomplete decimal precision and scale metadata')
            else:
                current = f'numeric({precision},{scale})'
        return current == expected

    def drop_column(self, column_name, stream):
        drop_column = "ALTER TABLE {} DROP COLUMN {}".format(self.table_name(stream), column_name)
        self.logger.info('Dropping column: %s', drop_column)
        self.query(drop_column)

    def version_column(self, column_name, stream, existing_names=()):
        version_time = datetime.now(timezone.utc)
        base_name = column_name.replace('"', '').encode('utf-8')[:40].decode('utf-8', errors='ignore')
        while True:
            archived_name = f'{base_name}_{version_time.strftime("%Y%m%d_%H%M%S_%f")}'
            if archived_name not in existing_names:
                break
            version_time += timedelta(microseconds=1)
        version_column = (
            f'ALTER TABLE {self.table_name(stream, False)} '
            f'RENAME COLUMN {column_name} TO "{archived_name}"'
        )
        self.logger.info('Versioning column: %s', version_column)
        self.query(version_column)
        return archived_name

    def add_column(self, column, stream):
        add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.table_name(stream), column)
        self.logger.info('Adding column: %s', add_column)
        self.query(add_column)

    def sync_table(self):
        for name, schema in self.flatten_schema.items():
            if is_decimal_schema(schema):
                precision, scale = schema['decimal']['precision'], schema['decimal']['scale']
                expected = 'NUMERIC' if precision is None else f'NUMERIC({precision},{scale})'
                actual = column_type(schema, self.decimal_postgres_version())
                if actual != expected:
                    self.logger.warning(
                        'Decimal fallback for %s.%s: source NUMERIC(%s,%s) maps to %s',
                        self.stream_schema_message['stream'], name,
                        precision, scale, actual,
                    )
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, without_schema=True)
        found_tables = [table for table in (self.get_tables()) if f'"{table["table_name"].lower()}"' == table_name]
        if len(found_tables) == 0:
            query = self.create_table_query()
            self.logger.info("Table '%s' does not exist. Creating... %s", table_name, query)
            self.query(query)

            self.grant_privilege(self.schema_name, self.grantees, self.grant_select_on_all_tables_in_schema)
        else:
            self.logger.info("Table '%s' exists", table_name)
            self.update_columns()
