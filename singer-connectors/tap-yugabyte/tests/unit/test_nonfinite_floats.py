"""NaN and +/-Infinity in real and double precision columns across discovery, SELECT, wal2json and bookmarks."""

import copy
import decimal
import json
import math
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import singer
from jsonschema import Draft7Validator
from singer import metadata

from tap_yugabyte import db, discovery_utils
from tap_yugabyte.sync_strategies import common, full_table, incremental, logical_replication

NAN = float('nan')
INF = float('inf')
WIDENED_ARRAY_TYPE = ['null', 'number', 'string', 'array']
PRECISIONS = {'integer': 32, 'real': 24, 'double precision': 53}
COLUMNS = ['id', 'single', 'double', 'doubles']
WAL_TYPES = {'id': 'double precision', 'single': 'real', 'double': 'double precision', 'doubles': 'double precision[]'}


def column(name, sql_type, primary_key=False):
    return discovery_utils.Column(name, primary_key, sql_type, None, PRECISIONS.get(sql_type.replace('[]', '')),
                                  None, sql_type.endswith('[]'), False, 0 if primary_key else None)


def discover(tables, decimal_target=None):
    """Run real discovery over in-memory catalog rows; the connection only supplies the database name."""
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = ['source']
    table_info = {'public': {
        name: {'is_view': False, 'row_count': 0, 'columns': {col.column_name: col for col in columns}}
        for name, columns in tables.items()
    }}
    return {stream['table_name']: stream
            for stream in discovery_utils.discover_columns(connection, table_info, decimal_target)}


def float_stream(decimal_target='snowflake'):
    return discover({'events': [column('id', 'double precision', primary_key=True), column('single', 'real'),
                                column('double', 'double precision'), column('doubles', 'double precision[]')]},
                    decimal_target)['events']


def emitted(message, stream):
    """Serialize as the tap does, then validate as the targets do."""
    record = json.loads(singer.format_message(message))['record']
    Draft7Validator(stream['schema']).validate(record)
    return record


def select(stream, row):
    return db.selected_row_to_singer_message(stream, row, 1, COLUMNS, None, metadata.to_map(stream['metadata']))


def wal_payload(values, action='I'):
    columns = [{'name': name, 'type': WAL_TYPES[name], 'value': value} for name, value in values.items()]
    return json.dumps({'action': action, 'schema': 'public', 'table': 'events',
                       'identity' if action == 'D' else 'columns': columns})


def consume(stream, payload, config, array_elements=None):
    state = {'bookmarks': {stream['tap_stream_id']: {'version': 1}}}
    with patch.object(logical_replication, 'create_array_elem', return_value=array_elements), \
            patch('singer.write_message') as write:
        logical_replication.consume_message([stream], state, SimpleNamespace(payload=payload, data_start=7),
                                            datetime(2026, 1, 1, tzinfo=timezone.utc), config)
    [message] = [call.args[0] for call in write.call_args_list if isinstance(call.args[0], singer.RecordMessage)]
    return message


@pytest.mark.parametrize('sql_type', ['real', 'double precision'])
@pytest.mark.parametrize(('decimal_target', 'nullable', 'key'), [
    ('snowflake', ['null', 'number', 'string'], ['number', 'string']),
    ('postgres', ['null', 'number', 'string'], ['number', 'string']),
    (None, ['null', 'number'], ['number']),
])
def test_float_columns_accept_tokens_only_on_decimal_routes(sql_type, decimal_target, nullable, key):
    assert discovery_utils.schema_for_column(column('value', sql_type), decimal_target) == {'type': nullable}
    assert discovery_utils.schema_for_column(column('value', sql_type, True), decimal_target) == {'type': key}


def test_float_array_definition_is_widened_per_stream_without_touching_shared_definitions():
    pristine = copy.deepcopy(discovery_utils.BASE_RECURSIVE_SCHEMAS)
    widened = copy.deepcopy(pristine)
    widened['sdc_recursive_number_array']['type'] = WIDENED_ARRAY_TYPE
    streams = discover({
        'doubles': [column('id', 'integer', True), column('vals', 'double precision[]')],
        'reals': [column('id', 'integer', True), column('vals', 'real[]')],
        'integers': [column('id', 'integer', True), column('vals', 'integer[]')],
    }, 'snowflake')
    for table in ('doubles', 'reals'):
        assert streams[table]['schema']['properties']['vals']['items'] == {
            '$ref': '#/definitions/sdc_recursive_number_array',
        }
        assert streams[table]['schema']['definitions'] == widened
    assert streams['integers']['schema']['definitions'] == pristine
    assert discovery_utils.BASE_RECURSIVE_SCHEMAS == pristine


def test_discovery_without_decimal_target_is_unchanged():
    stream = float_stream(None)
    assert stream['schema']['properties'] == {
        'id': {'type': ['number']},
        'single': {'type': ['null', 'number']},
        'double': {'type': ['null', 'number']},
        'doubles': {'type': ['null', 'array'], 'items': {'$ref': '#/definitions/sdc_recursive_number_array'}},
    }
    assert stream['schema']['definitions'] == discovery_utils.BASE_RECURSIVE_SCHEMAS
    assert 'decimal-target' not in metadata.to_map(stream['metadata'])[()]


@pytest.mark.parametrize(('value', 'expected'), [
    (NAN, 'NaN'), (INF, 'Infinity'), (-INF, '-Infinity'), (1.25, 1.25), (None, None),
])
def test_select_spells_nonfinite_floats_and_nested_array_elements_as_ysql_tokens(value, expected):
    stream = float_stream()
    record = emitted(select(stream, [7.5, value, value, [[value, 2.5], [-2.5, value]]]), stream)
    assert record == {'id': 7.5, 'single': expected, 'double': expected,
                      'doubles': [[expected, 2.5], [-2.5, expected]]}


@pytest.mark.parametrize(('key', 'expected'), [(NAN, 'NaN'), (INF, 'Infinity'), (-INF, '-Infinity')])
def test_select_spells_nonfinite_float_primary_keys(key, expected):
    stream = float_stream()
    assert emitted(select(stream, [key, 1.0, 1.0, []]), stream)['id'] == expected


def test_select_keeps_negative_zero_a_signed_json_number():
    stream = float_stream()
    record = emitted(select(stream, [1.0, -0.0, -0.0, [-0.0]]), stream)
    for value in (record['single'], record['double'], record['doubles'][0]):
        assert isinstance(value, float) and math.copysign(1.0, value) == -1.0


def test_select_without_decimal_target_keeps_null_for_nonfinite_floats():
    stream = float_stream(None)
    record = emitted(select(stream, [1.0, NAN, INF, [[-INF, 2.5], [NAN, 1.5]]]), stream)
    assert record == {'id': 1.0, 'single': None, 'double': None, 'doubles': [[None, 2.5], [None, 1.5]]}


@pytest.mark.parametrize(('text', 'expected'), [
    ('NaN', 'NaN'), ('Infinity', 'Infinity'), ('-Infinity', '-Infinity'), ('1.25', 1.25), ('-0', -0.0),
])
def test_wal_numeric_strings_become_ysql_tokens(text, expected):
    stream = float_stream()
    message = consume(stream, wal_payload({'id': '2.5', 'single': text, 'double': text}),
                      {'decimal_target': 'snowflake'})
    record = emitted(message, stream)
    assert (record['id'], record['single'], record['double']) == (2.5, expected, expected)
    if text == '-0':
        assert math.copysign(1.0, record['double']) == -1.0


def test_wal_null_without_numeric_strings_stays_null_and_finite_numbers_stay_floats():
    stream = float_stream()
    payload = ('{"action":"I","schema":"public","table":"events","columns":['
               '{"name":"id","type":"double precision","value":2},'
               '{"name":"single","type":"real","value":null},'
               '{"name":"double","type":"double precision","value":1.25}]}')
    record = emitted(consume(stream, payload, {'decimal_target': 'postgres'}), stream)
    assert record == {'id': 2.0, 'single': None, 'double': 1.25, '_sdc_deleted_at': None}
    assert isinstance(record['id'], float)


@pytest.mark.parametrize('action', ['I', 'U', 'D'])
def test_wal_rows_carry_nonfinite_keys_scalars_and_array_elements(action):
    stream = float_stream()
    values = {'id': 'NaN', 'single': 'Infinity', 'double': '-Infinity', 'doubles': '{{NaN,1.5},{Infinity,-Infinity}}'}
    message = consume(stream, wal_payload(values, action), {'decimal_target': 'snowflake'},
                      [[NAN, 1.5], [INF, -INF]])
    record = emitted(message, stream)
    assert {name: record[name] for name in COLUMNS} == {
        'id': 'NaN', 'single': 'Infinity', 'double': '-Infinity',
        'doubles': [['NaN', 1.5], ['Infinity', '-Infinity']],
    }
    assert (record['_sdc_deleted_at'] is not None) == (action == 'D')


def test_wal_float_array_elements_without_decimal_target_keep_floats():
    stream = float_stream(None)
    md_map = metadata.to_map(stream['metadata'])
    with patch.object(logical_replication, 'create_array_elem', return_value=[[NAN, 1.5], [INF, 2.5]]):
        message = logical_replication.row_to_singer_message(
            stream, ['{{NaN,1.5},{Infinity,2.5}}'], 1, ['doubles'], None, md_map, {})
    assert math.isnan(message.record['doubles'][0][0]) and message.record['doubles'][1] == [INF, 2.5]
    assert emitted(message, stream) == {'doubles': [[None, 1.5], [None, 2.5]]}


def test_unresolved_wal_float_array_widens_only_the_streams_own_definition():
    pristine = copy.deepcopy(discovery_utils.BASE_RECURSIVE_SCHEMAS)
    amount = discovery_utils.Column('amount', False, 'numeric', None, 18, 2, False, False, None)
    stream = discover({'events': [column('id', 'integer', True), amount]}, 'snowflake')['events']
    payload = {'action': 'I', 'schema': 'public', 'table': 'events', 'columns': [
        {'name': 'id', 'type': 'integer', 'value': '1'},
        {'name': 'amount', 'type': 'double precision[]', 'value': '{NaN}'},
    ]}
    with patch.object(logical_replication, 'refresh_streams_schema'), \
            patch.object(logical_replication, 'create_array_elem', return_value=[NAN]), \
            patch.object(common, 'write_schema_message') as schema_write, patch('singer.write_message') as write:
        logical_replication.consume_message(
            [stream], {'bookmarks': {'public-events': {'version': 1}}}, SimpleNamespace(data_start=1),
            datetime(2026, 1, 1, tzinfo=timezone.utc), {'decimal_target': 'snowflake'}, message_payload=payload,
        )
    published = schema_write.call_args.args[0]['schema']
    assert published['definitions']['sdc_recursive_number_array']['type'] == WIDENED_ARRAY_TYPE
    assert emitted(write.call_args.args[0], stream)['amount'] == ['NaN']
    assert discovery_utils.BASE_RECURSIVE_SCHEMAS == pristine


def incremental_stream(decimal_target='snowflake', key_type='double precision'):
    stream = discover({'events': [column('id', 'integer', True), column('key', key_type)]}, decimal_target)['events']
    md_map = metadata.to_map(stream['metadata'])
    md_map[()]['replication-key'] = 'key'
    return stream, md_map


def run_incremental(stream, md_map, state, rows):
    cursor = MagicMock()
    cursor.__iter__.return_value = iter(rows)
    connection = MagicMock()
    connection.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
    with patch.object(db, 'open_connection', return_value=connection), \
            patch.object(db, 'hstore_available', return_value=False), patch('singer.write_message') as write, \
            patch.object(incremental.keyset, 'merge_scan_available', return_value=False):
        state = incremental.sync_table({'limit': None}, stream, state, ['id', 'key'], md_map)
    records = [call.args[0].record for call in write.call_args_list if isinstance(call.args[0], singer.RecordMessage)]
    return state, [record['key'] for record in records], cursor.execute.call_args.args[0]


def test_incremental_keeps_the_last_finite_float_bookmark_and_rereads_nonfinite_keys():
    stream, md_map = incremental_stream()
    rows = [(4, -INF), (1, 1.0), (2, INF), (3, NAN)]
    state, keys, query = run_incremental(stream, md_map, {}, rows)
    assert keys == ['-Infinity', 1.0, 'Infinity', 'NaN']
    assert state['bookmarks']['public-events']['replication_key_value'] == 1.0
    assert '>=' not in query

    state, keys, query = run_incremental(stream, md_map, state, rows[1:])
    assert keys == [1.0, 'Infinity', 'NaN']
    assert state['bookmarks']['public-events']['replication_key_value'] == 1.0
    assert ">= '1.0'::double precision" in query


def test_incremental_without_decimal_target_keeps_skipping_null_float_keys():
    stream, md_map = incremental_stream(None)
    state, keys, _ = run_incremental(stream, md_map, {}, [(1, 1.0), (2, INF), (3, NAN)])
    assert keys == [1.0, None, None]
    assert state['bookmarks']['public-events']['replication_key_value'] == 1.0


def test_incremental_text_key_spelled_like_a_token_is_still_bookmarked():
    stream, md_map = incremental_stream(key_type='text')
    state, keys, _ = run_incremental(stream, md_map, {}, [(1, 'Infinity'), (2, 'NaN')])
    assert keys == ['Infinity', 'NaN']
    assert state['bookmarks']['public-events']['replication_key_value'] == 'NaN'


def test_keyset_bookmarks_keep_float_keys_resumable_as_text():
    """Float bounds bind as the key's own type, and a NULL bound would match no rows on resume."""
    values = [math.nan, math.inf, -math.inf, 1.5, -0.0, 0.1, decimal.Decimal('1.10'), 7, 'key']
    bookmark = full_table._bookmark_values(values)
    assert bookmark == ['NaN', 'Infinity', '-Infinity', '1.5', '-0.0', '0.1', '1.10', 7, 'key']
    state = singer.StateMessage(value={'bookmarks': {'stream': {'max_pk_values': bookmark}}})
    assert json.loads(singer.format_message(state))['value']['bookmarks']['stream']['max_pk_values'] == bookmark


@pytest.mark.parametrize('bookmarks', [
    {'max_pk_values': [None], 'last_pk_fetched': {}},
    {'max_pk_values': [5.0], 'last_pk_fetched': {'3': [None]}},
    {'max_pk_values': [None, 'a'], 'last_pk_fetched': {'1': ['1.5', 'a']}, 'completed_buckets': [0, 2]},
])
def test_null_key_bounds_from_older_releases_restart_the_scan(bookmarks):
    """Older releases saved NaN/Infinity keys as null; resuming from them would load no further rows."""
    state = {'bookmarks': {'public-events': dict(bookmarks, version=7)}}
    state = full_table._discard_null_key_bounds(state, 'public-events')
    stream_state = state['bookmarks']['public-events']
    assert stream_state['max_pk_values'] is None
    assert stream_state['last_pk_fetched'] == {}
    assert stream_state['completed_buckets'] is None
    assert stream_state['version'] == 7


def test_valid_key_bounds_are_kept():
    bookmarks = {'max_pk_values': ['NaN'], 'last_pk_fetched': {'2': ['1.5']}, 'completed_buckets': [1], 'version': 7}
    state = {'bookmarks': {'public-events': dict(bookmarks)}}
    assert full_table._discard_null_key_bounds(state, 'public-events') == {'bookmarks': {'public-events': bookmarks}}
