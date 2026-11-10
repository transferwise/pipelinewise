"""NULL arrays and numeric[] values match FastSync's array_to_json export on decimal routes."""

import copy
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import singer
from jsonschema import Draft7Validator
from singer import metadata
from singer.decimal_support import is_decimal_schema

from tap_yugabyte import db, discovery_utils, keyset
from tap_yugabyte.sync_strategies import full_table, incremental, logical_replication

UUID = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
ARRAY_TYPES = {'ints': 'integer[]', 'texts': 'text[]', 'doubles': 'double precision[]', 'uuids': 'uuid[]'}
ARRAY_VALUES = {'ints': [1, None], 'texts': ['a', None], 'doubles': [[1.5], [-2.5]], 'uuids': [UUID]}
NUMERIC_LITERAL = '{1,1.10,12345678901234567890.123,0.00000012,NaN,Infinity,-Infinity,NULL}'
NUMERIC_TEXT = '[1,1.10,12345678901234567890.123,0.00000012,"NaN","Infinity","-Infinity",null]'
NUMERIC_COLUMNS = ['amounts', 'id', 'scaled', 'updated_at']
PROJECTIONS = {
    ' "amounts" ': ' array_to_json("amounts")::text AS "amounts" ',
    ' "scaled" ': ' array_to_json("scaled")::text AS "scaled" ',
}
DECIMAL_ROUTE = {'decimal_target': 'snowflake'}


class RecordingCursor:
    """Record statements; answer the merge-scan and max-key probes with a single 1 and return no rows."""

    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def __iter__(self):
        return iter([])

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def fetchone(self):
        return (1,)


class RecordingConnection:
    closed = 0

    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, *_args, **_kwargs):
        return self._cursor

    def close(self):
        pass


def column(name, sql_type, primary_key=False, precision=None, scale=None):
    return discovery_utils.Column(name, primary_key, sql_type, None, precision, scale, sql_type.endswith('[]'), False,
                                  0 if primary_key else None)


def discover(columns, decimal_target=None):
    """Run real discovery over in-memory catalog rows; the connection only supplies the database name."""
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = ['source']
    table_info = {'public': {'events': {
        'is_view': False, 'row_count': 0, 'columns': {col.column_name: col for col in columns},
    }}}
    [stream] = discovery_utils.discover_columns(connection, table_info, decimal_target)
    return stream


def array_stream(decimal_target='snowflake'):
    columns = [column(name, sql_type) for name, sql_type in ARRAY_TYPES.items()]
    return discover([column('id', 'integer', True, 32), *columns], decimal_target)


def numeric_stream(decimal_target='snowflake', primary_key=True):
    return discover([column('id', 'integer', primary_key, 32), column('amounts', 'numeric[]'),
                     column('scaled', 'numeric[]', precision=10, scale=2),
                     column('updated_at', 'timestamp without time zone')], decimal_target)


def emitted(message, stream):
    """Serialize as the tap does, then validate as the targets do."""
    record = json.loads(singer.format_message(message))['record']
    Draft7Validator(stream['schema']).validate(record)
    return record


def select(stream, columns, row):
    return db.selected_row_to_singer_message(stream, row, 1, columns, None, metadata.to_map(stream['metadata']))


def project(sql):
    for plain, projected in PROJECTIONS.items():
        sql = sql.replace(plain, projected)
    return sql


def executed_selects(sync, stream, state=None, replication_key=None):
    """Run a sync strategy against recording connections and return its data SELECT statements, sorted."""
    md_map = metadata.to_map(stream['metadata'])
    if replication_key:
        md_map[()]['replication-key'] = replication_key
    cursor = RecordingCursor()
    with patch.object(db, 'open_connection', side_effect=lambda *_args, **_kwargs: RecordingConnection(cursor)), \
            patch.object(db, 'hstore_available', return_value=False), \
            patch.object(keyset, 'validate_index', return_value=(True, None)), patch('singer.write_message'):
        sync({'limit': None, 'keyset_buckets': 2}, stream, copy.deepcopy(state or {}), NUMERIC_COLUMNS, md_map)
    return sorted(sql for sql in cursor.executed if 'SELECT' in sql and '"amounts"' in sql)


def wal_payload(columns, action='I'):
    values = [{'name': name, 'type': sql_type, 'value': value} for name, sql_type, value in columns]
    return json.dumps({'action': action, 'schema': 'public', 'table': 'events',
                       'identity' if action == 'D' else 'columns': values})


def consume(stream, payload, conn_info):
    state = {'bookmarks': {stream['tap_stream_id']: {'version': 1}}}
    with patch('singer.write_message') as write:
        logical_replication.consume_message([stream], state, SimpleNamespace(payload=payload, data_start=7),
                                            datetime(2026, 1, 1, tzinfo=timezone.utc), conn_info)
    [message] = [call.args[0] for call in write.call_args_list if isinstance(call.args[0], singer.RecordMessage)]
    return message


@pytest.mark.parametrize('decimal_target', ['snowflake', 'postgres'])
def test_numeric_arrays_are_discovered_as_json_text_only_on_decimal_routes(decimal_target):
    legacy, marked = numeric_stream(None), numeric_stream(decimal_target)
    for name, definition in (('amounts', 'sdc_recursive_decimal_100_38_array'),
                             ('scaled', 'sdc_recursive_decimal_10_2_array')):
        items = {'$ref': f'#/definitions/{definition}'}
        assert legacy['schema']['properties'][name] == {'type': ['null', 'array'], 'items': items}
        assert marked['schema']['properties'][name] == {
            'type': ['null', 'string', 'array'], 'format': 'singer.json', 'items': items,
        }
        assert db.is_json_text_schema(marked['schema']['properties'][name])
        assert not is_decimal_schema(marked['schema']['properties'][name])
    assert marked['schema']['definitions'] == legacy['schema']['definitions']
    assert marked['schema']['properties']['updated_at'] == legacy['schema']['properties']['updated_at']
    assert array_stream(decimal_target)['schema']['properties'] == array_stream(None)['schema']['properties']


@pytest.mark.parametrize('decimal_target', ['snowflake', 'postgres'])
def test_numeric_array_primary_keys_keep_list_transport(decimal_target):
    """Targets dedup on key text, so equal keys with different display scale must not become JSON text."""
    key = column('amounts', 'numeric[]', primary_key=True)
    schema = discovery_utils.schema_for_column(key, decimal_target)
    assert schema == discovery_utils.schema_for_column(key, None)
    assert not db.is_json_text_schema(schema)


@pytest.mark.parametrize(('schema', 'expected'), [
    ({'type': ['null', 'string', 'array'], 'format': 'singer.json'}, True),
    ({'type': ['null', 'string'], 'format': 'singer.decimal', 'decimal': {'precision': 5, 'scale': 2}}, False),
    ({'type': ['null', 'array'], 'items': {'$ref': '#/definitions/sdc_recursive_decimal_5_2_array'}}, False),
    ('b', False), (None, False), (['singer.json'], False),
])
def test_json_text_schema_detection_tolerates_malformed_schemas(schema, expected):
    assert db.is_json_text_schema(schema) is expected


@pytest.mark.parametrize(('name', 'expected'), [
    ('amounts', ' array_to_json("amounts")::text AS "amounts" '),
    ('odd"name', ' array_to_json("odd""name")::text AS "odd""name" '),
])
def test_json_text_projection_quotes_identifiers_and_returns_text(name, expected):
    assert db.json_text_column_sql(name) == expected


def test_column_helpers_project_only_marked_columns():
    stream = numeric_stream()
    properties, md_map = stream['schema']['properties'], metadata.to_map(stream['metadata'])
    assert db.prepare_columns_for_select_sql('amounts', md_map, properties) == PROJECTIONS[' "amounts" ']
    assert db.prepare_columns_for_select_sql('amounts', md_map) == ' "amounts" '
    assert db.prepare_columns_for_select_sql('updated_at', md_map, properties) == \
        db.prepare_columns_for_select_sql('updated_at', md_map)
    assert 'CASE' in db.prepare_columns_for_select_sql('updated_at', md_map, properties)
    assert db.prepare_view_column_sql('amounts', properties) == PROJECTIONS[' "amounts" ']
    assert db.prepare_view_column_sql('updated_at', properties) == ' "updated_at" '
    assert db.prepare_view_column_sql('amounts') == ' "amounts" '


@pytest.mark.parametrize(('sync', 'primary_key', 'statements'), [
    (full_table.sync_view, False, 1),
    (full_table.sync_table, False, 1),
    (full_table.sync_table, True, 2),
])
def test_view_plain_scan_and_keyset_bucket_selects_change_only_marked_columns(sync, primary_key, statements):
    plain = executed_selects(sync, numeric_stream(None, primary_key))
    marked = executed_selects(sync, numeric_stream('snowflake', primary_key))
    assert len(marked) == statements
    assert marked == sorted(project(sql) for sql in plain)
    assert all(sql.count('array_to_json(') == 2 for sql in marked)
    if primary_key:
        assert all('yb_hash_code("id")' in sql for sql in marked)


@pytest.mark.parametrize(('primary_key', 'order_by'), [(True, ['updated_at', 'id']), (False, ['updated_at'])])
def test_incremental_select_projects_marked_columns_and_keeps_replication_key_sql_raw(primary_key, order_by):
    state = {'bookmarks': {'public-events': {'replication_key_value': '2026-01-01T00:00:00'}}}
    [plain] = executed_selects(incremental.sync_table, numeric_stream(None, primary_key), state, 'updated_at')
    [marked] = executed_selects(incremental.sync_table, numeric_stream('snowflake', primary_key), state, 'updated_at')
    assert marked == project(plain)
    assert marked.count('array_to_json(') == 2
    assert f"{db.prepare_columns_sql('updated_at')} >= '2026-01-01T00:00:00'::timestamp without time zone" in marked
    assert f'ORDER BY {keyset.order_by_sql(order_by)}' in marked
    assert ('UNION ALL' in marked) is primary_key


@pytest.mark.parametrize(('decimal_target', 'null_array'), [('snowflake', None), ('postgres', None), (None, [])])
def test_select_null_arrays_match_fastsync_only_on_decimal_routes(decimal_target, null_array):
    stream = array_stream(decimal_target)
    columns = ['id', *ARRAY_TYPES]
    nulls = emitted(select(stream, columns, [1, *[None] * len(ARRAY_TYPES)]), stream)
    empties = emitted(select(stream, columns, [2, *[[] for _ in ARRAY_TYPES]]), stream)
    values = emitted(select(stream, columns, [3, *ARRAY_VALUES.values()]), stream)
    assert nulls == {'id': 1, **dict.fromkeys(ARRAY_TYPES, null_array)}
    assert empties == {'id': 2, **{name: [] for name in ARRAY_TYPES}}
    assert values == {'id': 3, **ARRAY_VALUES}


@pytest.mark.parametrize('nonfinite_floats', [False, True])
def test_select_helper_default_still_turns_null_arrays_into_empty_lists(nonfinite_floats):
    assert db.selected_value_to_singer_value(None, 'integer[]', nonfinite_floats) == []


@pytest.mark.parametrize('decimal_target', ['snowflake', 'postgres'])
def test_select_passes_projected_numeric_array_text_through_unchanged(decimal_target):
    stream = numeric_stream(decimal_target)
    record = emitted(select(stream, ['id', 'amounts', 'scaled'], [1, NUMERIC_TEXT, None]), stream)
    assert record == {'id': 1, 'amounts': NUMERIC_TEXT, 'scaled': None}
    record = emitted(select(stream, ['id', 'amounts', 'scaled'], [2, '[[1,2],[3,4]]', '[]']), stream)
    assert record == {'id': 2, 'amounts': '[[1,2],[3,4]]', 'scaled': '[]'}


@pytest.mark.parametrize('value', [[Decimal('1.10')], Decimal('1.10'), 1.1])
def test_select_rejects_numeric_arrays_that_bypassed_the_projection_without_echoing_values(value):
    with pytest.raises(TypeError) as raised:
        select(numeric_stream(), ['id', 'amounts'], [1, value])
    assert str(raised.value) == 'Expected JSON text for column amounts'


def test_select_without_decimal_target_keeps_the_legacy_numeric_array_conversion():
    record = select(numeric_stream(None), ['amounts'], [[Decimal('1.10'), Decimal('NaN'), None]]).record
    assert record == {'amounts': [Decimal('1.10'), None, None]}


@pytest.mark.parametrize('sql_type', ARRAY_TYPES.values())
@pytest.mark.parametrize(('conn_info', 'expected'), [
    ({'decimal_target': 'snowflake'}, None), ({'decimal_target': 'postgres'}, None), ({}, []),
])
def test_wal_null_arrays_skip_the_array_round_trip_only_on_decimal_routes(sql_type, conn_info, expected):
    with patch.object(logical_replication, 'create_array_elem', return_value=None) as create:
        assert logical_replication.selected_value_to_singer_value(None, sql_type, conn_info) == expected
    assert create.called is (expected is not None)


@pytest.mark.parametrize('conn_info', [DECIMAL_ROUTE, {}])
def test_wal_empty_arrays_stay_empty_lists(conn_info):
    with patch.object(logical_replication, 'create_array_elem', return_value=[]) as create:
        assert logical_replication.selected_value_to_singer_value('{}', 'integer[]', conn_info) == []
    create.assert_called_once_with('{}', 'integer[]', conn_info)


@pytest.mark.parametrize('action', ['I', 'U', 'D'])
def test_wal_rows_carry_null_and_empty_arrays_like_fastsync(action):
    stream = array_stream()
    columns = [('id', 'integer', '1'), ('ints', 'integer[]', None), ('texts', 'text[]', '{}'),
               ('doubles', 'double precision[]', None), ('uuids', 'uuid[]', '{}')]
    with patch.object(logical_replication, 'create_array_elem', return_value=[]) as create:
        record = emitted(consume(stream, wal_payload(columns, action), DECIMAL_ROUTE), stream)
    assert {name: record[name] for name in ['id', *ARRAY_TYPES]} == {
        'id': 1, 'ints': None, 'texts': [], 'doubles': None, 'uuids': [],
    }
    assert [call.args[:2] for call in create.call_args_list] == [('{}', 'text[]'), ('{}', 'uuid[]')]


def test_wal_numeric_array_helper_skips_null():
    assert logical_replication.numeric_array_json_text(None, 'numeric[]', 'public-events', 'amounts') is None


@pytest.mark.parametrize(('value', 'sql_type'), [
    ('{1.10}', 'integer[]'), ('{1.10}', 'numeric'), ([Decimal('1.10')], 'numeric[]'), (Decimal('1.10'), 'numeric[]'),
])
def test_wal_numeric_array_helper_rejects_unexpected_input_naming_only_stream_and_column(value, sql_type):
    with pytest.raises(ValueError) as raised:
        logical_replication.numeric_array_json_text(value, sql_type, 'public-events', 'amounts')
    assert str(raised.value) == 'Expected numeric[] text in WAL for stream public-events, column amounts'


# Server pairs of arr::text and array_to_json(arr)::text, the same on PostgreSQL and YugabyteDB.
@pytest.mark.parametrize(('literal', 'expected'), [
    (NUMERIC_LITERAL, NUMERIC_TEXT),
    ('{}', '[]'),
    ('{NULL,NULL}', '[null,null]'),
    ('{0,0.000,-0.00110,12.50}', '[0,0.000,-0.00110,12.50]'),
    ('{{1.50,NULL},{NaN,-2}}', '[[1.50,null],["NaN",-2]]'),
    ('{{{7}}}', '[[[7]]]'),
    ('[-3:-2]={1,2}', '[1,2]'),
    ('[0:1][-2:0]={{1.50,1.50,1.50},{1.50,1.50,1.50}}', '[[1.50,1.50,1.50],[1.50,1.50,1.50]]'),
    ('{' + '9' * 60 + '.' + '0' * 40 + '}', '[' + '9' * 60 + '.' + '0' * 40 + ']'),
])
def test_wal_numeric_array_helper_renders_array_to_json_text_without_a_connection(literal, expected):
    with patch.object(db, 'open_connection') as open_connection:
        assert logical_replication.numeric_array_json_text(literal, 'numeric[]', 'public-events', 'amounts') == expected
    open_connection.assert_not_called()


# array_out never writes these; the server would respell or refuse them, so copying them could diverge.
@pytest.mark.parametrize('literal', [
    '', '{', '}', '{}}', '{{}', '{1,}', '{,1}', '{1,,2}', '{1}{2}', '{1},', '{1}x', 'x{1}', '[1:2]{1,2}', '[a:b]={1}',
    '{1e5}', '{+7}', '{007}', '{.5}', '{5.}', '{-0}', '{-0.000}', '{ 1}', '{"1"}', '{null}', '{nan}', '{inf}',
    '{secret-value}',
])
def test_wal_numeric_array_helper_rejects_other_text_naming_only_stream_and_column(literal):
    with pytest.raises(ValueError) as raised:
        logical_replication.numeric_array_json_text(literal, 'numeric[]', 'public-events', 'amounts')
    assert str(raised.value) == 'Unable to render numeric[] from WAL for stream public-events, column amounts'


@pytest.mark.parametrize('action', ['I', 'U', 'D'])
def test_wal_marked_numeric_arrays_are_rendered_without_a_connection_instead_of_converted(action):
    stream = numeric_stream()
    columns = [('id', 'integer', '1'), ('amounts', 'numeric[]', NUMERIC_LITERAL), ('scaled', 'numeric(10,2)[]', None)]
    with patch.object(db, 'open_connection') as open_connection, \
            patch.object(logical_replication, 'create_array_elem') as create:
        record = emitted(consume(stream, wal_payload(columns, action), DECIMAL_ROUTE), stream)
    assert (record['id'], record['amounts'], record['scaled']) == (1, NUMERIC_TEXT, None)
    open_connection.assert_not_called()
    create.assert_not_called()


def test_wal_numeric_arrays_without_decimal_target_keep_the_legacy_conversion():
    stream = numeric_stream(None)
    parsed = {'{1.10,NULL}': ['1.10', None], None: None}
    with patch.object(logical_replication, 'create_array_elem', side_effect=lambda elem, *_: parsed[elem]), \
            patch.object(logical_replication, 'numeric_array_json_text') as render:
        message = logical_replication.row_to_singer_message(
            stream, ['{1.10,NULL}', None], 1, ['amounts', 'scaled'], None, metadata.to_map(stream['metadata']), {})
    assert message.record == {'amounts': [Decimal('1.10'), None], 'scaled': []}
    render.assert_not_called()
