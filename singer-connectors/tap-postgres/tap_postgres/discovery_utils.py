import collections
import copy

from typing import List, Optional
import psycopg2.extras
from singer import metadata

import tap_postgres.db as post_db

# LogMiner do not support LONG, LONG RAW, CLOB, BLOB, NCLOB, ADT, or COLLECTION datatypes.
Column = collections.namedtuple('Column', [
    "column_name",
    "is_primary_key",
    "sql_data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "is_array",
    "is_enum"

])

INTEGER_TYPES = {'integer', 'smallint', 'bigint'}
FLOAT_TYPES = {'real', 'double precision'}
JSON_TYPES = {'json', 'jsonb'}
BASE_RECURSIVE_SCHEMAS = {
    'sdc_recursive_integer_array': {'type': ['null', 'integer', 'array'],
                                    'items': {'$ref': '#/definitions/sdc_recursive_integer_array'}},
    'sdc_recursive_number_array': {'type': ['null', 'number', 'array'],
                                   'items': {'$ref': '#/definitions/sdc_recursive_number_array'}},
    'sdc_recursive_string_array': {'type': ['null', 'string', 'array'],
                                   'items': {'$ref': '#/definitions/sdc_recursive_string_array'}},
    'sdc_recursive_boolean_array': {'type': ['null', 'boolean', 'array'],
                                    'items': {'$ref': '#/definitions/sdc_recursive_boolean_array'}},
    'sdc_recursive_timestamp_array': {'type': ['null', 'string', 'array'],
                                      'format': 'date-time',
                                      'items': {'$ref': '#/definitions/sdc_recursive_timestamp_array'}},
    'sdc_recursive_object_array': {'type': ['null', 'object', 'array'],
                                   'items': {'$ref': '#/definitions/sdc_recursive_object_array'}}
}


def discover_db(connection, filter_schemas=None, tables: Optional[List[str]] = None):
    """
    Discover streams in the DB cluster
    """
    table_info = produce_table_info(connection, filter_schemas, tables)
    db_streams = discover_columns(connection, table_info)
    return db_streams


def produce_table_info(conn, filter_schemas=None, tables: Optional[List[str]] = None):
    """
    Generates info about tables in the cluster
    """
    # typlen  -1  == variable length arrays
    # typelem != 0 points to subtypes. 23 in the case of arrays
    # so integer arrays are typlen = -1, typelem = 23 because integer types are oid 23

    # this seems to identify all arrays:
    # select typname from pg_attribute  as pga join pg_type as pgt on pgt.oid = pga.atttypid
    # where typlen = -1 and typelem != 0 and pga.attndims > 0;

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
        cur.itersize = post_db.CURSOR_ITER_SIZE
        table_info = {}
        # SELECT CASE WHEN $2.typtype = 'd' THEN $2.typbasetype ELSE $1.atttypid END
        sql = """
SELECT
  pg_class.reltuples::BIGINT                            AS approximate_row_count,
  (pg_class.relkind = 'v' or pg_class.relkind = 'm')    AS is_view,
  n.nspname                                             AS schema_name,
  pg_class.relname                                      AS table_name,
  attname                                               AS column_name,
  i.indisprimary                                        AS primary_key,
  format_type(a.atttypid, NULL::integer)                AS data_type,
  information_schema._pg_char_max_length(CASE WHEN COALESCE(subpgt.typtype, pgt.typtype) = 'd'
                                              THEN COALESCE(subpgt.typbasetype, pgt.typbasetype) ELSE COALESCE(subpgt.oid, pgt.oid)
                                          END,
                                          information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number AS character_maximum_length,
  information_schema._pg_numeric_precision(CASE WHEN COALESCE(subpgt.typtype, pgt.typtype) = 'd'
                                                THEN COALESCE(subpgt.typbasetype, pgt.typbasetype) ELSE COALESCE(subpgt.oid, pgt.oid)
                                            END,
                                           information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number AS numeric_precision,
  information_schema._pg_numeric_scale(CASE WHEN COALESCE(subpgt.typtype, pgt.typtype) = 'd'
                                                THEN COALESCE(subpgt.typbasetype, pgt.typbasetype) ELSE COALESCE(subpgt.oid, pgt.oid)
                                        END,
                                       information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number AS numeric_scale,
  pgt.typcategory                       = 'A' AS is_array,
  COALESCE(subpgt.typtype, pgt.typtype) = 'e' AS is_enum
FROM pg_attribute a
LEFT JOIN pg_type AS pgt ON a.atttypid = pgt.oid
JOIN pg_class
  ON pg_class.oid = a.attrelid
JOIN pg_catalog.pg_namespace n
  ON n.oid = pg_class.relnamespace
LEFT OUTER JOIN pg_index as i
  ON a.attrelid = i.indrelid
 AND a.attnum = ANY(i.indkey)
 AND i.indisprimary = true
LEFT OUTER JOIN pg_type AS subpgt
  ON pgt.typelem = subpgt.oid
 AND pgt.typelem != 0
WHERE attnum > 0
AND NOT a.attisdropped
AND pg_class.relkind IN ('r', 'v', 'm', 'p')
AND n.nspname NOT in ('pg_toast', 'pg_catalog', 'information_schema')
AND has_column_privilege(pg_class.oid, attname, 'SELECT') = true """

        if filter_schemas:
            sql = post_db.filter_schemas_sql_clause(sql, filter_schemas)

        if tables:
            sql = post_db.filter_tables_sql_clause(sql, tables)

        cur.execute(sql)

        for row in cur.fetchall():
            row_count, is_view, schema_name, table_name, *col_info = row

            if table_info.get(schema_name) is None:
                table_info[schema_name] = {}

            if table_info[schema_name].get(table_name) is None:
                table_info[schema_name][table_name] = {'is_view': is_view, 'row_count': row_count, 'columns': {}}

            col_name = col_info[0]

            table_info[schema_name][table_name]['columns'][col_name] = Column(*col_info)

        return table_info


def discover_columns(connection, table_info):
    """
    Generates more info about columns of the given table
    """
    entries = []
    for schema_name in table_info.keys():
        for table_name in table_info[schema_name].keys():

            mdata = {}
            columns = table_info[schema_name][table_name]['columns']
            table_pks = [col_name for col_name, col_info in columns.items() if col_info.is_primary_key]
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(" SELECT current_database()")
                database_name = cur.fetchone()[0]

            metadata.write(mdata, (), 'table-key-properties', table_pks)
            metadata.write(mdata, (), 'schema-name', schema_name)
            metadata.write(mdata, (), 'database-name', database_name)
            metadata.write(mdata, (), 'row-count', table_info[schema_name][table_name]['row_count'])
            metadata.write(mdata, (), 'is-view', table_info[schema_name][table_name].get('is_view'))

            column_schemas = {col_name: schema_for_column(col_info) for col_name, col_info in columns.items()}

            schema = {'type': 'object',
                      'properties': column_schemas,
                      'definitions': {}}

            schema = include_array_schemas(columns, schema)

            for c_name in column_schemas.keys():
                mdata = write_sql_data_type_md(mdata, columns[c_name])

                if column_schemas[c_name].get('type') is None:
                    mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'unsupported')
                    mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', False)
                elif table_info[schema_name][table_name]['columns'][c_name].is_primary_key:
                    mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'automatic')
                    mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', True)
                else:
                    mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'available')
                    mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', True)

            entry = {'table_name': table_name,
                     'stream': table_name,
                     'metadata': metadata.to_list(mdata),
                     'tap_stream_id': post_db.compute_tap_stream_id(schema_name, table_name),
                     'schema': schema}

            entries.append(entry)

    return entries


# pylint: disable=too-many-return-statements,too-many-branches,too-many-statements
def schema_for_column_datatype(col):
    """
    Build json schema for columns with non-array datatype
    """
    schema = {}
    # remove any array notation from type information as we use a separate field for that
    data_type = col.sql_data_type.lower().replace('[]', '')

    if data_type in INTEGER_TYPES:
        schema['type'] = nullable_column('integer', col.is_primary_key)
        schema['minimum'] = -1 * (2 ** (col.numeric_precision - 1))
        schema['maximum'] = 2 ** (col.numeric_precision - 1) - 1
        return schema

    if data_type == 'money':
        schema['type'] = nullable_column('string', col.is_primary_key)
        return schema
    if col.is_enum:
        schema['type'] = nullable_column('string', col.is_primary_key)
        return schema

    if data_type == 'bit' and col.character_maximum_length == 1:
        schema['type'] = nullable_column('boolean', col.is_primary_key)
        return schema

    if data_type == 'boolean':
        schema['type'] = nullable_column('boolean', col.is_primary_key)
        return schema

    if data_type == 'uuid':
        schema['type'] = nullable_column('string', col.is_primary_key)
        return schema

    if data_type == 'interval':
        schema['type'] = nullable_column('string', col.is_primary_key)
        return schema

    if data_type == 'ltree':
        schema['type'] = nullable_column('string', col.is_primary_key)
        return schema

    if data_type == 'hstore':
        schema['type'] = nullable_column('object', col.is_primary_key)
        schema['properties'] = {}
        return schema

    if data_type == 'citext':
        schema['type'] = nullable_column('string', col.is_primary_key)
        return schema

    if data_type in JSON_TYPES:
        schema['type'] = nullable_columns(['object', 'array'], col.is_primary_key)
        return schema

    if data_type == 'numeric':
        schema['type'] = nullable_column('number', col.is_primary_key)
        scale = post_db.numeric_scale(col)
        precision = post_db.numeric_precision(col)

        schema['exclusiveMaximum'] = True
        schema['maximum'] = post_db.numeric_max(precision, scale)
        schema['multipleOf'] = post_db.numeric_multiple_of(scale)
        schema['exclusiveMinimum'] = True
        schema['minimum'] = post_db.numeric_min(precision, scale)
        return schema

    if data_type in {'time without time zone', 'time with time zone'}:
        # times are treated as ordinary strings as they can not possible match RFC3339
        schema['type'] = nullable_column('string', col.is_primary_key)
        schema['format'] = 'time'
        return schema

    if data_type in ('date', 'timestamp without time zone', 'timestamp with time zone'):
        schema['type'] = nullable_column('string', col.is_primary_key)

        schema['format'] = 'date-time'
        return schema

    if data_type in FLOAT_TYPES:
        schema['type'] = nullable_column('number', col.is_primary_key)
        return schema

    if data_type == 'text':
        schema['type'] = nullable_column('string', col.is_primary_key)
        return schema

    if data_type == 'character varying':
        schema['type'] = nullable_column('string', col.is_primary_key)
        if col.character_maximum_length:
            schema['maxLength'] = col.character_maximum_length

        return schema

    if data_type == 'character':
        schema['type'] = nullable_column('string', col.is_primary_key)
        if col.character_maximum_length:
            schema['maxLength'] = col.character_maximum_length
        return schema

    if data_type in {'cidr', 'inet', 'macaddr'}:
        schema['type'] = nullable_column('string', col.is_primary_key)
        return schema

    return schema


def schema_for_column(col_info):
    """
    Built json schema for the give column
    """
    # NB> from the post postgres docs: The current implementation does not enforce the declared number of dimensions
    # either. These means we can say nothing about an array column. its items may be more arrays or primitive types
    # like integers and this can vary on a row by row basis

    column_schema = {'type': ["null", "array"]}
    if not col_info.is_array:
        return schema_for_column_datatype(col_info)

    if col_info.sql_data_type == 'integer[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_integer_array'}
    elif col_info.sql_data_type == 'bigint[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_integer_array'}
    elif col_info.sql_data_type == 'bit[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_boolean_array'}
    elif col_info.sql_data_type == 'boolean[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_boolean_array'}
    elif col_info.sql_data_type == 'character varying[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif col_info.sql_data_type == 'cidr[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif col_info.sql_data_type == 'citext[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif col_info.sql_data_type == 'date[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_timestamp_array'}
    elif col_info.sql_data_type == 'numeric[]':
        scale = post_db.numeric_scale(col_info)
        precision = post_db.numeric_precision(col_info)
        schema_name = schema_name_for_numeric_array(precision, scale)
        column_schema['items'] = {'$ref': f'#/definitions/{schema_name}'}
    elif col_info.sql_data_type == 'double precision[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_number_array'}
    elif col_info.sql_data_type == 'hstore[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_object_array'}
    elif col_info.sql_data_type == 'inet[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif col_info.sql_data_type == 'json[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_object_array'}
    elif col_info.sql_data_type == 'jsonb[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_object_array'}
    elif col_info.sql_data_type == 'mac[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif col_info.sql_data_type == 'money[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif col_info.sql_data_type == 'real[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_number_array'}
    elif col_info.sql_data_type == 'smallint[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_integer_array'}
    elif col_info.sql_data_type == 'text[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif col_info.sql_data_type == 'timestamp without time zone[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_timestamp_array'}
    elif col_info.sql_data_type == 'timestamp with time zone[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_timestamp_array'}
    elif col_info.sql_data_type == 'time[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif col_info.sql_data_type == 'uuid[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    else:
        # custom datatypes like enums
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    return column_schema


# pylint: disable=invalid-name,missing-function-docstring
def nullable_columns(col_types, pk):
    if pk:
        return col_types
    return ['null'] + col_types


def nullable_column(col_type, pk):
    if pk:
        return [col_type]
    return ['null', col_type]


def schema_name_for_numeric_array(precision, scale):
    schema_name = f'sdc_recursive_decimal_{precision}_{scale}_array'
    return schema_name


def include_array_schemas(columns, schema):
    schema['definitions'] = copy.deepcopy(BASE_RECURSIVE_SCHEMAS)

    decimal_array_columns = [key for key, value in columns.items() if value.sql_data_type == 'numeric[]']
    for col in decimal_array_columns:
        scale = post_db.numeric_scale(columns[col])
        precision = post_db.numeric_precision(columns[col])
        schema_name = schema_name_for_numeric_array(precision, scale)
        schema['definitions'][schema_name] = {'type': ['null', 'number', 'array'],
                                              'multipleOf': post_db.numeric_multiple_of(scale),
                                              'exclusiveMaximum': True,
                                              'maximum': post_db.numeric_max(precision, scale),
                                              'exclusiveMinimum': True,
                                              'minimum': post_db.numeric_min(precision, scale),
                                              'items': {'$ref': f'#/definitions/{schema_name}'}}

    return schema


def write_sql_data_type_md(mdata, col_info):
    c_name = col_info.column_name
    if col_info.sql_data_type == 'bit' and col_info.character_maximum_length > 1:
        mdata = metadata.write(mdata, ('properties', c_name),
                               'sql-datatype', f"bit({col_info.character_maximum_length})")
    else:
        mdata = metadata.write(mdata, ('properties', c_name), 'sql-datatype', col_info.sql_data_type)

    return mdata
