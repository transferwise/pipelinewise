# pylint: disable=missing-docstring,too-many-locals

import collections
import itertools
import pendulum
import pymysql

from typing import Optional, Dict, Tuple, Set, List
from singer import metadata, Schema, get_logger
from singer.catalog import Catalog, CatalogEntry

from tap_mysql.connection import connect_with_backoff, MySQLConnection
from tap_mysql.sync_strategies import common

LOGGER = get_logger('tap_mysql')

Column = collections.namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "column_type",
    "column_key"])

pymysql.converters.conversions[pendulum.DateTime] = pymysql.converters.escape_datetime

STRING_TYPES = {
    'char',
    'enum',
    'tinytext',
    'longtext',
    'mediumtext',
    'text',
    'varchar'
}

BYTES_FOR_INTEGER_TYPE = {
    'tinyint': 1,
    'smallint': 2,
    'mediumint': 3,
    'int': 4,
    'bigint': 8
}

BOOL_TYPES = {'bit'}

JSON_TYPES = {'json'}

FLOAT_TYPES = {'float', 'double', 'decimal'}

DATETIME_TYPES = {'datetime', 'timestamp', 'time', 'date'}

BINARY_TYPES = {'binary', 'varbinary'}

SPATIAL_TYPES = {'geometry', 'point', 'linestring',
                 'polygon', 'multipoint', 'multilinestring',
                 'multipolygon', 'geometrycollection'}

# A set of all supported column types listed above
SUPPORTED_COLUMN_TYPES_AGGREGATED = \
    STRING_TYPES \
        .union(FLOAT_TYPES) \
        .union(DATETIME_TYPES) \
        .union(BINARY_TYPES) \
        .union(SPATIAL_TYPES) \
        .union(BOOL_TYPES) \
        .union(JSON_TYPES) \
        .union(BYTES_FOR_INTEGER_TYPE.keys())


def is_supported_column_type(column_datatype: str) -> bool:
    """
    Checks if the given sql datatype is supported

    Args:
        column_datatype: Column sql data type from the catalog metadata

    Returns: True if column type is supported, False otherwise
    """
    return column_datatype in SUPPORTED_COLUMN_TYPES_AGGREGATED


def should_run_discovery(column_names: Set[str], md_map: Dict[Tuple, Dict]) -> bool:
    """
    Checks if we need to run discovery using a given metadata mapping.

    This function is helpful to refresh a stream schema when we detect a new column while syncing.

    The discovery will run if one of the following conditions are met:
        - one of the given columns is not in the given metadata, ie we know nothing about this column
        - the column is selected by default and its type is among the supported sql types.

    Args:
        column_names: A set of column names as strings
        md_map: a stream metadata as a map, usually you get it by running:
        >> import singer
        >> md_map = singer.metadata.to_map(stream_catalog['metadata'])

    Returns: True if we should run discovery, False otherwise

    """
    LOGGER.debug('should_run_discovery with (%s)...', column_names)

    for column_name in column_names:
        md_properties = md_map.get(('properties', column_name))

        # this column doesn't exists in the metadata so we know nothing about it
        # so will have to run discovery
        if not md_properties:
            LOGGER.debug('Will run discovery because `%s` not in stream metadata', column_name)
            return True

        if md_properties['selected-by-default'] and is_supported_column_type(md_properties['datatype']):
            LOGGER.debug('Will run discovery because `%s` is selected by default and of supported type', column_name)
            return True

    return False


def discover_catalog(mysql_conn: MySQLConnection, dbs: str = None, tables: Optional[str] = None):
    """Returns a Catalog describing the structure of the database."""

    if dbs:
        filter_dbs_clause = ",".join([f"'{db_name}'" for db_name in dbs.split(",")])
        table_schema_clause = f"WHERE table_schema IN ({filter_dbs_clause})"
    else:
        table_schema_clause = """
        WHERE table_schema NOT IN (
        'information_schema',
        'performance_schema',
        'mysql',
        'sys'
        )"""

    tables_clause = ''

    if tables is not None and tables != '':
        filter_tables_clause = ",".join([f"'{table_name}'" for table_name in tables.split(",")])
        tables_clause = f" AND table_name IN ({filter_tables_clause})"

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute(f"""
            SELECT table_schema,
                   table_name,
                   table_type,
                   table_rows
                FROM information_schema.tables
                {table_schema_clause}{tables_clause}
            """)

            table_info = {}

            for (db_name, table, table_type, rows) in cur.fetchall():
                if db_name not in table_info:
                    table_info[db_name] = {}

                table_info[db_name][table] = {
                    'row_count': rows,
                    'is_view': table_type == 'VIEW'
                }

            cur.execute(f"""
                SELECT table_schema,
                       table_name,
                       column_name,
                       data_type,
                       character_maximum_length,
                       numeric_precision,
                       numeric_scale,
                       column_type,
                       column_key
                    FROM information_schema.columns
                    {table_schema_clause}{tables_clause}
                    ORDER BY table_schema, table_name
            """)

            columns = []
            rec = cur.fetchone()
            while rec is not None:
                columns.append(Column(*rec))
                rec = cur.fetchone()

            entries = []
            for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
                cols = list(cols)
                (table_schema, table_name) = k

                schema = Schema(type='object',
                                properties={c.column_name: schema_for_column(c) for c in cols})
                mdata = create_column_metadata(cols)
                md_map = metadata.to_map(mdata)

                md_map = metadata.write(md_map,
                                        (),
                                        'database-name',
                                        table_schema)

                is_view = table_info[table_schema][table_name]['is_view']

                if table_schema in table_info and table_name in table_info[table_schema]:
                    row_count = table_info[table_schema][table_name].get('row_count')

                    if row_count is not None:
                        md_map = metadata.write(md_map,
                                                (),
                                                'row-count',
                                                row_count)

                    md_map = metadata.write(md_map,
                                            (),
                                            'is-view',
                                            is_view)

                column_is_key_prop = lambda c, s: (c.column_key == 'PRI' and
                                                   s.properties[c.column_name].inclusion != 'unsupported')

                key_properties = [c.column_name for c in cols if column_is_key_prop(c, schema)]

                if not is_view:
                    md_map = metadata.write(md_map,
                                            (),
                                            'table-key-properties',
                                            key_properties)

                entry = CatalogEntry(
                    table=table_name,
                    stream=table_name,
                    metadata=metadata.to_list(md_map),
                    tap_stream_id=common.generate_tap_stream_id(table_schema, table_name),
                    schema=schema)

                entries.append(entry)

    return Catalog(entries)


def schema_for_column(column):  # pylint: disable=too-many-branches
    """Returns the Schema object for the given Column."""

    data_type = column.data_type.lower()
    column_type = column.column_type.lower()

    inclusion = 'available'
    # We want to automatically include all primary key columns
    if column.column_key.lower() == 'pri':
        inclusion = 'automatic'

    result = Schema(inclusion=inclusion)

    if data_type in BOOL_TYPES or column_type.startswith('tinyint(1)'):
        result.type = ['null', 'boolean']

    elif data_type in BYTES_FOR_INTEGER_TYPE:
        result.type = ['null', 'integer']
        bits = BYTES_FOR_INTEGER_TYPE[data_type] * 8
        if 'unsigned' in column_type:
            result.minimum = 0
            result.maximum = 2 ** bits - 1
        else:
            result.minimum = 0 - 2 ** (bits - 1)
            result.maximum = 2 ** (bits - 1) - 1

    elif data_type in FLOAT_TYPES:
        result.type = ['null', 'number']

        if data_type == 'decimal':
            result.multipleOf = 10 ** (0 - column.numeric_scale)

    elif data_type in JSON_TYPES:
        result.type = ['null', 'object']

    elif data_type in STRING_TYPES:
        result.type = ['null', 'string']
        result.maxLength = column.character_maximum_length

    elif data_type in DATETIME_TYPES:
        result.type = ['null', 'string']

        if data_type == 'time':
            result.format = 'time'
        else:
            result.format = 'date-time'

    elif data_type in BINARY_TYPES:
        result.type = ['null', 'string']
        result.format = 'binary'

    elif data_type in SPATIAL_TYPES:
        result.type = ['null', 'object']
        result.format = 'spatial'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description=f'Unsupported column type {column_type}')
    return result


def create_column_metadata(cols: List[Column]):
    mdata = {}
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    for col in cols:
        schema = schema_for_column(col)
        mdata = metadata.write(mdata,
                               ('properties', col.column_name),
                               'selected-by-default', schema.inclusion != 'unsupported')

        mdata = metadata.write(mdata,
                               ('properties', col.column_name),
                               'sql-datatype', col.column_type.lower())


        mdata = metadata.write(mdata,
                               ('properties', col.column_name),
                               'datatype', col.data_type.lower()
                               )

    return metadata.to_list(mdata)


def resolve_catalog(discovered_catalog, streams_to_sync):
    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams_to_sync:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        discovered_table = discovered_catalog.get_stream(catalog_entry.tap_stream_id)
        database_name = common.get_database_name(catalog_entry)

        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           database_name, catalog_entry.table)
            continue

        selected = {k for k, v in catalog_entry.schema.properties.items()
                    if common.property_is_selected(catalog_entry, k) or k == replication_key}

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            metadata=catalog_entry.metadata,
            stream=catalog_entry.tap_stream_id,
            table=catalog_entry.table,
            schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in columns}
            )
        ))

    return result


def desired_columns(selected, table_schema) -> Set:
    """
    Return the set of column names we need to include in the SELECT.

    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    """
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == 'automatic':
            automatic.add(column)
        elif inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        else:
            raise Exception(f'Unknown inclusion {inclusion}')

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning(
            'Columns %s were selected but are not supported. Skipping them.',
            selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning(
            'Columns %s were selected but do not exist.',
            selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            'Columns %s are primary keys but were not selected. Adding them.',
            not_selected_but_automatic)

    return selected.intersection(available).union(automatic)
