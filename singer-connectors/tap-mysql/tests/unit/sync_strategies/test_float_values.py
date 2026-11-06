"""FLOAT columns read their exact stored value on every replication method."""

from unittest.mock import MagicMock, patch

import pytest
from singer import Schema, metadata
from singer.catalog import Catalog, CatalogEntry

from tap_mysql import discover_utils
from tap_mysql.sync_strategies import common, full_table, incremental


NUMBER = {'type': ['null', 'number']}


def catalog_entry(columns, key_properties=(), **stream_metadata):
    """Build a catalog from {name: (schema, datatype)}; a None datatype omits the metadata key."""
    md_map = {(): {'database-name': 'source', 'table-key-properties': list(key_properties), **stream_metadata}}
    for name, (_, datatype) in columns.items():
        md_map[('properties', name)] = {'selected': True}
        if datatype is not None:
            md_map[('properties', name)]['datatype'] = datatype
    return CatalogEntry(
        tap_stream_id='source-events', table='events', stream='source-events',
        schema=Schema(type='object', properties={
            name: Schema.from_dict(schema) for name, (schema, _) in columns.items()
        }),
        metadata=metadata.to_list(md_map),
    )


def column(name, data_type, column_type=None, key='', length=None, precision=None, scale=None):
    return discover_utils.Column(
        'source', 'events', name, data_type, length, precision, scale, column_type or data_type, key,
    )


def discovered_entry(columns, decimal_target):
    """Build the catalog entry that discovery returns for these information_schema columns."""
    md_map = metadata.to_map(discover_utils.create_column_metadata(columns))
    md_map[()].update({
        'database-name': 'source',
        'table-key-properties': [c.column_name for c in columns if c.column_key == 'PRI'],
    })
    return CatalogEntry(
        tap_stream_id='source-events', table='events', stream='events',
        schema=Schema(type='object', properties={
            c.column_name: discover_utils.schema_for_column(c, decimal_target) for c in columns
        }),
        metadata=metadata.to_list(md_map),
    )


def test_select_promotes_only_float_columns_without_an_alias():
    catalog = catalog_entry({
        'amount': (NUMBER, 'float'),
        'ratio': (NUMBER, 'double'),
        'legacy': (NUMBER, None),
        'payload': ({'type': ['null', 'string'], 'format': 'binary'}, 'varbinary'),
        'shape': ({'type': ['null', 'object'], 'format': 'spatial'}, 'point'),
        'rate%': (NUMBER, 'float'),
    })

    sql = common.generate_select_sql(catalog, ['amount', 'ratio', 'legacy', 'payload', 'shape', 'rate%'])

    assert sql == (
        'SELECT `amount` * 1E0,`ratio`,`legacy`,hex(`payload`) as `payload`,'
        'ST_AsGeoJSON(`shape`) as `shape`,`rate%%` * 1E0 FROM `source`.`events`'
    )


def test_incremental_filters_and_orders_a_float_key_on_the_raw_column():
    catalog = catalog_entry(
        {'id': ({'type': ['integer']}, 'int'), 'amount': (NUMBER, 'float')},
        key_properties=('id',), **{'replication-method': 'INCREMENTAL', 'replication-key': 'amount'},
    )
    state = {'bookmarks': {'source-events': {'replication_key': 'amount', 'replication_key_value': 16777216.0}}}

    with patch('tap_mysql.sync_strategies.incremental.connect_with_backoff'), \
            patch.object(common, 'sync_query') as query, patch('singer.write_message'):
        incremental.sync_table(MagicMock(), catalog, state, ['id', 'amount'])

    assert query.call_args.args[3] == (
        'SELECT `id`,`amount` * 1E0 FROM `source`.`events` '
        'WHERE `amount` >= %(replication_key_value)s ORDER BY `amount` ASC'
    )
    assert query.call_args.args[-1] == {'replication_key_value': 16777216.0}


def test_max_pk_promotes_float_keys_and_orders_by_raw_columns():
    catalog = catalog_entry(
        {'id': ({'type': ['integer']}, 'int'), 'amount': (NUMBER, 'float')}, key_properties=('id', 'amount'),
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (7, 16777216.0)

    assert full_table.get_max_pk_values(cursor, catalog) == {'id': 7, 'amount': 16777216.0}
    assert ' '.join(cursor.execute.call_args.args[0].split()) == (
        'SELECT `id`, `amount` * 1E0 FROM `source`.`events` ORDER BY `id` DESC, `amount` DESC LIMIT 1'
    )


def test_max_pk_without_float_metadata_keeps_the_raw_key():
    catalog = catalog_entry({'amount': (NUMBER, None)}, key_properties=('amount',))
    cursor = MagicMock()
    cursor.fetchone.return_value = None

    assert full_table.get_max_pk_values(cursor, catalog) == {}
    assert ' '.join(cursor.execute.call_args.args[0].split()) == (
        'SELECT `amount` FROM `source`.`events` ORDER BY `amount` DESC LIMIT 1'
    )


@pytest.mark.parametrize('decimal_target', [None, 'snowflake'])
@pytest.mark.parametrize('altered', [
    column('amount', 'decimal', 'decimal(18,2)', precision=18, scale=2),
    column('amount', 'varchar', 'varchar(10)', length=10),
    column('amount', 'bigint'),
], ids=['decimal', 'varchar', 'bigint'])
def test_columns_altered_from_float_before_rediscovery_keep_their_raw_values(altered, decimal_target):
    """A run keeps the input catalog's 'float' metadata but takes each column's schema from live discovery."""
    stale = discovered_entry(
        [column('id', 'float', key='PRI'), column('amount', 'float'), column('rate', 'float')], decimal_target,
    )
    live = discovered_entry([column('id', 'bigint', key='PRI'), altered, column('rate', 'float')], decimal_target)
    resolved = discover_utils.resolve_catalog(Catalog([live]), [stale], decimal_target).streams[0]
    assert metadata.to_map(resolved.metadata)[('properties', 'amount')]['datatype'] == 'float'
    cursor = MagicMock()
    cursor.fetchone.return_value = None

    assert common.generate_select_sql(resolved, ['id', 'amount', 'rate']) == (
        'SELECT `id`,`amount`,`rate` * 1E0 FROM `source`.`events`'
    )
    assert full_table.get_max_pk_values(cursor, resolved) == {}
    assert ' '.join(cursor.execute.call_args.args[0].split()) == (
        'SELECT `id` FROM `source`.`events` ORDER BY `id` DESC LIMIT 1'
    )


@pytest.mark.parametrize('schema', [
    {'type': ['null', 'string'], 'format': 'singer.decimal', 'decimal': {'precision': 18, 'scale': 2}},
    {'type': ['null', 'number'], 'multipleOf': 0.01},
    {'type': ['null', 'string'], 'maxLength': 10},
    {'type': ['null', 'integer'], 'minimum': -2 ** 63, 'maximum': 2 ** 63 - 1},
    {'type': ['null', 'number', 'string']},
    {'type': ['null', 'number'], 'format': 'date-time'},
], ids=['decimal', 'legacy-decimal', 'string', 'integer', 'number-or-string', 'formatted-number'])
def test_float_metadata_is_ignored_unless_the_schema_is_a_plain_number(schema):
    catalog = catalog_entry({'id': (schema, 'float'), 'amount': (schema, 'float')}, key_properties=('id',))
    cursor = MagicMock()
    cursor.fetchone.return_value = None

    assert common.generate_select_sql(catalog, ['id', 'amount']) == 'SELECT `id`,`amount` FROM `source`.`events`'
    full_table.get_max_pk_values(cursor, catalog)
    assert ' '.join(cursor.execute.call_args.args[0].split()) == (
        'SELECT `id` FROM `source`.`events` ORDER BY `id` DESC LIMIT 1'
    )


def test_a_float_key_outside_the_resolved_schema_keeps_the_raw_column():
    catalog = catalog_entry({'amount': (NUMBER, 'float')}, key_properties=('old_id',))
    md_map = metadata.to_map(catalog.metadata)
    md_map[('properties', 'old_id')] = {'datatype': 'float'}
    catalog.metadata = metadata.to_list(md_map)
    cursor = MagicMock()
    cursor.fetchone.return_value = None

    full_table.get_max_pk_values(cursor, catalog)
    assert ' '.join(cursor.execute.call_args.args[0].split()) == (
        'SELECT `old_id` FROM `source`.`events` ORDER BY `old_id` DESC LIMIT 1'
    )
