"""NULL arrays and numeric[] values match FastSync's array_to_json export on decimal routes."""

import copy
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psycopg2.errors
import pytest
import singer
from jsonschema import Draft7Validator
from singer import metadata
from singer.decimal_support import is_decimal_schema

from tap_postgres import db, discovery_utils
from tap_postgres.sync_strategies import full_table, incremental, logical_replication

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
RENDER_SQL = 'SELECT array_to_json(%s::numeric[])::text'
DECIMAL_ROUTE = {'decimal_target': 'snowflake'}


class InvalidText(psycopg2.errors.InvalidTextRepresentation):
    """A driver error with the SQLSTATE the server sends; psycopg2 sets pgcode only for server errors."""

    pgcode = '22P02'


def column(name, sql_type, primary_key=False, precision=None, scale=None):
    return discovery_utils.Column(name, primary_key, sql_type, None, precision, scale, sql_type.endswith('[]'), False)


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


def numeric_stream(decimal_target='snowflake'):
    return discover([column('id', 'integer', True, 32), column('amounts', 'numeric[]'),
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
    """Run a sync strategy against a mocked connection and return the SELECT statements it issued."""
    md_map = metadata.to_map(stream['metadata'])
    if replication_key:
        md_map[()]['replication-key'] = replication_key
    connection = MagicMock()
    cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
    with patch.object(db, 'open_connection', return_value=connection), \
            patch.object(db, 'hstore_available', return_value=False), patch('singer.write_message'):
        sync({'limit': None}, stream, copy.deepcopy(state or {}), NUMERIC_COLUMNS, md_map)
    return [call.args[0] for call in cursor.execute.call_args_list if 'SELECT' in call.args[0]]


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


def render_connection(fetched=None, error=None):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (fetched,)
    cursor.execute.side_effect = error
    return connection, cursor


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


@pytest.mark.parametrize('sync', [full_table.sync_table, full_table.sync_view])
def test_table_and_view_selects_change_only_marked_columns(sync):
    [plain] = executed_selects(sync, numeric_stream(None))
    [marked] = executed_selects(sync, numeric_stream())
    assert marked == project(plain)
    assert marked.count('array_to_json(') == 2


def test_incremental_select_projects_marked_columns_and_keeps_replication_key_sql_raw():
    state = {'bookmarks': {'public-events': {'replication_key_value': '2026-01-01T00:00:00'}}}
    [plain] = executed_selects(incremental.sync_table, numeric_stream(None), state, 'updated_at')
    [marked] = executed_selects(incremental.sync_table, numeric_stream(), state, 'updated_at')
    assert marked == project(plain)
    key = db.prepare_columns_sql('updated_at')
    assert f"WHERE {key} >= '2026-01-01T00:00:00'::timestamp without time zone" in marked
    assert f'ORDER BY {key} ASC' in marked


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


def test_wal_numeric_array_helper_skips_null_without_a_connection():
    with patch.object(db, 'open_connection') as open_connection:
        assert logical_replication.numeric_array_json_text(
            None, 'numeric[]', DECIMAL_ROUTE, 'public-events', 'amounts') is None
    open_connection.assert_not_called()


@pytest.mark.parametrize(('value', 'sql_type'), [
    ('{1.10}', 'integer[]'), ('{1.10}', 'numeric'), ([Decimal('1.10')], 'numeric[]'), (Decimal('1.10'), 'numeric[]'),
])
def test_wal_numeric_array_helper_rejects_unexpected_input_naming_only_stream_and_column(value, sql_type):
    with patch.object(db, 'open_connection') as open_connection, pytest.raises(ValueError) as raised:
        logical_replication.numeric_array_json_text(value, sql_type, DECIMAL_ROUTE, 'public-events', 'amounts')
    assert str(raised.value) == 'Expected numeric[] text in WAL for stream public-events, column amounts'
    open_connection.assert_not_called()


def test_wal_numeric_array_helper_binds_the_literal_on_a_primary_connection_and_closes_it():
    connection, cursor = render_connection(NUMERIC_TEXT)
    with patch.object(db, 'open_connection', return_value=connection) as open_connection:
        assert logical_replication.numeric_array_json_text(
            NUMERIC_LITERAL, 'numeric[]', DECIMAL_ROUTE, 'public-events', 'amounts') == NUMERIC_TEXT
    open_connection.assert_called_once_with(DECIMAL_ROUTE, False, True)
    cursor.execute.assert_called_once_with(RENDER_SQL, (NUMERIC_LITERAL,))
    connection.close.assert_called_once_with()


def test_wal_numeric_array_helper_reports_only_the_sqlstate_and_still_closes():
    connection, _ = render_connection(error=InvalidText('invalid input syntax for type numeric: "secret-value"'))
    with patch.object(db, 'open_connection', return_value=connection), pytest.raises(ValueError) as raised:
        logical_replication.numeric_array_json_text(
            '{secret-value}', 'numeric[]', DECIMAL_ROUTE, 'public-events', 'amounts')
    assert str(raised.value) == \
        'Unable to render numeric[] from WAL for stream public-events, column amounts: InvalidText (SQLSTATE 22P02)'
    assert raised.value.__cause__ is None and raised.value.__suppress_context__
    connection.close.assert_called_once_with()


def test_wal_numeric_array_helper_names_a_lost_connection_without_values():
    connection, _ = render_connection(error=psycopg2.OperationalError('server closed the connection: "secret-value"'))
    with patch.object(db, 'open_connection', return_value=connection), pytest.raises(ValueError) as raised:
        logical_replication.numeric_array_json_text(
            '{secret-value}', 'numeric[]', DECIMAL_ROUTE, 'public-events', 'amounts')
    assert str(raised.value) == \
        'Unable to render numeric[] from WAL for stream public-events, column amounts: OperationalError'
    connection.close.assert_called_once_with()


@pytest.mark.parametrize('action', ['I', 'U', 'D'])
def test_wal_marked_numeric_arrays_are_rendered_by_the_server_instead_of_converted(action):
    stream = numeric_stream()
    connection, cursor = render_connection(NUMERIC_TEXT)
    columns = [('id', 'integer', '1'), ('amounts', 'numeric[]', NUMERIC_LITERAL), ('scaled', 'numeric(10,2)[]', None)]
    with patch.object(db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'create_array_elem') as create:
        record = emitted(consume(stream, wal_payload(columns, action), DECIMAL_ROUTE), stream)
    assert (record['id'], record['amounts'], record['scaled']) == (1, NUMERIC_TEXT, None)
    cursor.execute.assert_called_once_with(RENDER_SQL, (NUMERIC_LITERAL,))
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
