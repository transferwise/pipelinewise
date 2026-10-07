"""Exact numeric transport across SELECT, logical replication and checkpoints."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import psycopg2
import singer
from singer import metadata

from tap_postgres import db, discovery_utils, stream_utils
from tap_postgres.sync_strategies import common, incremental, logical_replication


EXACT = '12345678901234567890.123456789012345678'


def column(precision=38, scale=18, sql_type='numeric', primary_key=False):
    return discovery_utils.Column('amount', primary_key, sql_type, None, precision, scale, False, False)


def stream(target='snowflake', precision=38, scale=18):
    root = {'schema-name': 'public', 'database-name': 'source', 'replication-key': 'amount'}
    if target:
        root['decimal-target'] = target
    return {
        'tap_stream_id': 'public-events', 'stream': 'events', 'table_name': 'events',
        'schema': {'type': 'object', 'properties': {
            'amount': discovery_utils.schema_for_column(column(precision, scale), target),
            'approximate': {'type': ['null', 'number']},
        }},
        'metadata': metadata.to_list({
            (): root,
            ('properties', 'amount'): {'sql-datatype': 'numeric', 'selected': True},
            ('properties', 'approximate'): {'sql-datatype': 'double precision', 'selected': True},
        }),
    }


@pytest.mark.parametrize(('precision', 'scale'), [(38, 18), (65, 30), (None, None), (5, -2), (3, 5)])
def test_discovery_keeps_raw_dimensions(precision, scale):
    schema = discovery_utils.schema_for_column(column(precision, scale), 'snowflake')
    assert schema['decimal'] == {'precision': precision, 'scale': scale}
    assert schema['type'] == ['null', 'string']


def test_primary_numeric_key_is_not_nullable():
    assert discovery_utils.schema_for_column(column(primary_key=True), 'snowflake')['type'] == ['string']


@pytest.mark.parametrize(('encoded', 'expected'), [(2046, -2), (2047, -1), (1048, -1000), (1000, 1000)])
def test_discovery_decodes_postgres_signed_numeric_scale(encoded, expected):
    schema = discovery_utils.schema_for_column(column(10, encoded), 'snowflake')
    assert schema['decimal'] == {'precision': 10, 'scale': expected}


def test_disabled_and_approximate_schema_are_unchanged():
    legacy = discovery_utils.schema_for_column(column())
    assert legacy['type'] == ['null', 'number']
    assert legacy['multipleOf'] == 1e-18
    assert 'decimal' not in legacy
    assert discovery_utils.schema_for_column(column(sql_type='double precision'), 'snowflake') == {
        'type': ['null', 'number'],
    }


@pytest.mark.parametrize('value', [Decimal(EXACT), Decimal('NaN'), None])
def test_select_records_preserve_digits(value):
    source = stream()
    record = db.selected_row_to_singer_message(
        source, [value, 1.25], 1, ['amount', 'approximate'], None, metadata.to_map(source['metadata']),
    )
    parsed = json.loads(singer.messages.format_message(record))
    assert parsed['record'] == {'amount': str(value) if value is not None else None, 'approximate': 1.25}


def test_wal_numeric_tokens_keep_digits_but_real_columns_stay_float():
    config = {'decimal_target': 'snowflake'}
    parsed = logical_replication.parse_wal_payload(f'[{EXACT}, 1.25]', config)
    source = stream()
    record = logical_replication.row_to_singer_message(
        source, parsed, 1, ['amount', 'approximate'], None, metadata.to_map(source['metadata']), config,
    )
    assert record.record == {'amount': EXACT, 'approximate': 1.25}
    assert isinstance(record.record['approximate'], float)
    assert isinstance(logical_replication.parse_wal_payload(EXACT, {}), float)


def test_wal_numeric_strings_restore_ordinary_types_and_keep_json_boolean_null():
    source = stream()
    md_map = metadata.to_map(source['metadata'])
    types = {'small': 'smallint', 'big': 'bigint', 'integer': 'integer', 'flag': 'boolean', 'json': 'jsonb',
             'optional': 'numeric'}
    for name, sql_type in types.items():
        source['schema']['properties'][name] = {'type': ['null', 'string']}
        md_map[('properties', name)] = {'sql-datatype': sql_type}
    columns = ['amount', 'approximate', *types]
    record = logical_replication.row_to_singer_message(
        source, [EXACT, '1.25', '12', '9223372036854775807', '1', True, '{"nested":1.25}', None],
        1, columns, None, md_map, {'decimal_target': 'postgres'},
    )
    assert record.record == {'amount': EXACT, 'approximate': 1.25, 'small': 12, 'big': 9223372036854775807,
                             'integer': 1, 'flag': True, 'json': {'nested': 1.25}, 'optional': None}


@pytest.mark.parametrize('target', ['snowflake', 'postgres', None])
def test_numeric_wal_strings_are_requested_only_for_enabled_routes(target):
    cursor = MagicMock()
    logical_replication._start_replication(cursor, [stream()], 'slot', 123, 160000, target)
    options = cursor.start_replication.call_args.kwargs['options']
    assert options.get('numeric-data-types-as-string') is (True if target else None)


def test_old_wal2json_numeric_option_retries_once_without_discarding_other_options():
    cursor = MagicMock()
    cursor.start_replication.side_effect = [
        psycopg2.errors.InvalidParameterValue('unbekannte Ausgabeoption'), None,
    ]
    with patch.object(logical_replication.LOGGER, 'warning') as warning:
        logical_replication._start_replication(cursor, [stream()], 'slot', 123, 160000, 'postgres')
    first, second = [call.kwargs for call in cursor.start_replication.call_args_list]
    assert first['options']['numeric-data-types-as-string'] is True
    assert second['options'] == {key: value for key, value in first['options'].items()
                                 if key != 'numeric-data-types-as-string'}
    assert first['start_lsn'] == second['start_lsn'] == 123
    warning.assert_called_once()


def test_other_replication_errors_do_not_trigger_numeric_compatibility_retry():
    error = psycopg2.OperationalError('disconnected')
    cursor = MagicMock()
    cursor.start_replication.side_effect = error
    with pytest.raises(type(error)):
        logical_replication._start_replication(cursor, [stream()], 'slot', 123, 160000, 'postgres')
    cursor.start_replication.assert_called_once()


def test_invalid_parameter_retry_failure_is_propagated_after_one_retry():
    cursor = MagicMock()
    first = psycopg2.errors.InvalidParameterValue('parametro non valido')
    second = psycopg2.errors.InvalidParameterValue('opzione ancora non valida')
    cursor.start_replication.side_effect = [first, second]
    with pytest.raises(psycopg2.errors.InvalidParameterValue) as error:
        logical_replication._start_replication(cursor, [stream()], 'slot', 123, 160000, 'postgres')
    assert error.value is second
    assert cursor.start_replication.call_count == 2


@pytest.mark.parametrize('action', ['I', 'U', 'D'])
def test_logical_records_keep_exact_numeric_values_for_every_action(action):
    source = stream()
    field = 'identity' if action == 'D' else 'columns'
    payload = (
        '{"action":"' + action + '","schema":"public","table":"events","' + field + '":'
        '[{"name":"amount","type":"numeric(38,18)","value":' + EXACT + '}]}'
    )
    message = SimpleNamespace(payload=payload, data_start=1)
    state = {'bookmarks': {'public-events': {'version': 1}}}
    with patch('singer.write_message') as write:
        logical_replication.consume_message(
            [source], state, message, datetime(2026, 1, 1, tzinfo=timezone.utc), {'decimal_target': 'snowflake'},
        )
    records = [call.args[0] for call in write.call_args_list if isinstance(call.args[0], singer.RecordMessage)]
    assert len(records) == 1
    assert records[0].record['amount'] == EXACT
    assert (records[0].record['_sdc_deleted_at'] is not None) == (action == 'D')


@pytest.mark.parametrize('target', ['snowflake', 'postgres'])
@pytest.mark.parametrize('action', ['I', 'U', 'D'])
def test_unquoted_large_numeric_wal_values_preserve_records_and_native_integer_types(target, action):
    source = stream(target, precision=None, scale=None)
    source['schema']['properties']['id'] = {'type': ['integer']}
    md_map = metadata.to_map(source['metadata'])
    md_map[('properties', 'id')] = {'sql-datatype': 'integer', 'selected': True}
    source['metadata'] = metadata.to_list(md_map)
    value = '9' * 5000
    field = 'identity' if action == 'D' else 'columns'
    payload = ('{"action":"' + action + '","schema":"public","table":"events","' + field + '":['
               '{"name":"id","type":"integer","value":42},'
               '{"name":"amount","type":"numeric","value":' + value + '},'
               '{"name":"approximate","type":"double precision","value":1.25}]}')
    state = {'bookmarks': {'public-events': {'version': 1}}}
    with patch('singer.write_message') as write:
        result = logical_replication.consume_message(
            [source], state, SimpleNamespace(payload=payload, data_start=123),
            datetime(2026, 1, 1, tzinfo=timezone.utc), {'decimal_target': target},
        )
    records = [call.args[0] for call in write.call_args_list if isinstance(call.args[0], singer.RecordMessage)]
    assert len(records) == 1
    assert records[0].record['amount'] == value
    assert records[0].record['id'] == 42 and isinstance(records[0].record['id'], int)
    assert records[0].record['approximate'] == 1.25 and isinstance(records[0].record['approximate'], float)
    assert (records[0].record['_sdc_deleted_at'] is not None) == (action == 'D')
    assert result['bookmarks']['public-events']['lsn'] == 123


def test_unmarked_wal_parsing_keeps_native_numeric_types():
    value = logical_replication.parse_wal_payload('{"integer":42,"floating":1.25}', {})
    assert value == {'integer': 42, 'floating': 1.25}
    assert isinstance(value['integer'], int)
    assert isinstance(value['floating'], float)


@pytest.mark.parametrize('target', ['snowflake', 'postgres', None])
def test_delete_refreshes_changed_decimal_replica_identity_before_serializing(target):
    source = stream(target, precision=18, scale=2)
    source['schema']['properties']['id'] = {'type': ['integer']}
    md_map = metadata.to_map(source['metadata'])
    md_map[()]['table-key-properties'] = ['id']
    md_map[('properties', 'id')] = {'sql-datatype': 'integer', 'inclusion': 'automatic'}
    source['metadata'] = metadata.to_list(md_map)
    # ALTER NUMERIC(18,2) to (19,1) can round 9999999999999999.99 beyond
    # the old type's integer capacity without emitting an UPDATE row event.
    value = '10000000000000000.0'
    payload = (
        '{"action":"D","schema":"public","table":"events","identity":['
        '{"name":"id","type":"integer","value":1},'
        '{"name":"amount","type":"numeric(19,1)","value":' + value + '}]}'
    )

    def refresh(_config, streams):
        assert streams == [source]
        source['schema']['properties']['amount'] = discovery_utils.schema_for_column(column(19, 1), target)

    emitted = []
    state = {'bookmarks': {'public-events': {'version': 1}}}
    config = {'decimal_target': target} if target else {}
    with patch.object(logical_replication, 'refresh_streams_schema', side_effect=refresh) as discover, \
            patch.object(common, 'write_schema_message', side_effect=emitted.append), \
            patch('singer.write_message', side_effect=emitted.append):
        logical_replication.consume_message(
            [source], state, SimpleNamespace(payload=payload, data_start=2),
            datetime(2026, 1, 1, tzinfo=timezone.utc), config,
        )
    assert discover.call_count == (1 if target else 0)
    if target:
        assert emitted[0]['schema']['properties']['amount']['decimal'] == {'precision': 19, 'scale': 1}
        assert emitted[0]['key_properties'] == ['id']
    record = emitted[-1].record
    assert record['amount'] == (value if target else Decimal(value))
    assert record['id'] == 1
    assert record['_sdc_deleted_at'] is not None
    assert state['bookmarks']['public-events']['lsn'] == 2


@pytest.mark.parametrize(('precision', 'scale'), [(65, 30), (None, None), (5, -2), (3, 5)])
def test_wide_numeric_schema_retains_source_dimensions_for_target_fallback(precision, scale):
    source = stream(precision=precision, scale=scale)
    with patch.object(common, 'write_schema_message') as write:
        common.send_schema_message(source, [])
    assert write.call_args.args[0]['schema']['properties']['amount']['decimal'] == {
        'precision': precision, 'scale': scale,
    }


@pytest.mark.parametrize('target', ['snowflake', 'postgres', None])
def test_unselected_columns_retain_established_schema_emission(target):
    source = stream(target, precision=65, scale=30)
    md_map = metadata.to_map(source['metadata'])
    md_map[('properties', 'amount')]['selected'] = False
    md_map[('properties', 'approximate')]['selected'] = False
    source['metadata'] = metadata.to_list(md_map)
    with patch.object(common, 'write_schema_message') as write:
        common.send_schema_message(source, [])
    assert write.call_args.args[0]['schema'] == source['schema']
    assert not common.should_sync_column(md_map, 'amount')
    assert not common.should_sync_column(md_map, 'approximate')


def test_postgres_target_keeps_unbounded_numeric():
    source = stream('postgres', None, None)
    with patch.object(common, 'write_schema_message') as write:
        common.send_schema_message(source, [])
    assert write.call_args.args[0]['schema']['properties']['amount']['decimal'] == {'precision': None, 'scale': None}


def test_incremental_checkpoint_and_resume_query_keep_exact_digits():
    source = stream()
    state = {'bookmarks': {'public-events': {'replication_key_value': EXACT}}}
    cursor = MagicMock()
    cursor.__iter__.return_value = iter([(Decimal(EXACT),)])
    connection = MagicMock()
    connection.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
    with patch.object(db, 'open_connection', return_value=connection), \
            patch.object(db, 'hstore_available', return_value=False), patch('singer.write_message'):
        result = incremental.sync_table(
            {'limit': None}, source, state, ['amount'], metadata.to_map(source['metadata']),
        )
    assert result['bookmarks']['public-events']['replication_key_value'] == EXACT
    assert f"'{EXACT}'::numeric" in cursor.execute.call_args.args[0]


def test_incremental_checkpoint_outside_narrowed_declaration_uses_unbounded_numeric():
    source = stream(precision=2, scale=0)
    state = {'bookmarks': {'public-events': {'replication_key_value': '1000.00'}}}
    cursor = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
    with patch.object(db, 'open_connection', return_value=connection), \
            patch.object(db, 'hstore_available', return_value=False), patch('singer.write_message'):
        incremental.sync_table(
            {'limit': None}, source, state, ['amount'], metadata.to_map(source['metadata']),
        )
    query = cursor.execute.call_args.args[0]
    assert "'1000.00'::numeric" in query
    assert 'numeric(2,0)' not in query


@pytest.mark.parametrize('value', ['9999999999999999.99', '-9999999999999999.99', '0.01'])
def test_legacy_float_bookmark_replays_before_rounded_boundary_and_writes_exact_state(value):
    source = stream(precision=18, scale=2)
    state = {'bookmarks': {'public-events': {'replication_key_value': float(value)}}}
    cursor = MagicMock()
    cursor.__iter__.return_value = iter([(Decimal(value),)])
    connection = MagicMock()
    connection.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
    with patch.object(db, 'open_connection', return_value=connection), \
            patch.object(db, 'hstore_available', return_value=False), patch('singer.write_message'), \
            patch.object(incremental.LOGGER, 'warning') as warning:
        result = incremental.sync_table(
            {'limit': None}, source, state, ['amount'], metadata.to_map(source['metadata']),
        )
    query = cursor.execute.call_args.args[0]
    boundary = query.split(">= '")[1].split("'::numeric")[0]
    assert Decimal(boundary) < Decimal(value)
    assert result['bookmarks']['public-events']['replication_key_value'] == value
    assert warning.call_args.args[1:] == ('public-events', 'amount')


@pytest.mark.parametrize('value', [float('inf'), float('-inf'), float('nan'), 'NaN', 'Infinity', '-Infinity'])
def test_nonfinite_legacy_bookmark_replays_without_filter(value):
    source = stream()
    state = {'bookmarks': {'public-events': {'replication_key_value': value}}}
    cursor = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
    with patch.object(db, 'open_connection', return_value=connection), \
            patch.object(db, 'hstore_available', return_value=False), patch('singer.write_message'), \
            patch.object(incremental.LOGGER, 'warning') as warning:
        incremental.sync_table({'limit': None}, source, state, ['amount'], metadata.to_map(source['metadata']))
    assert 'WHERE' not in cursor.execute.call_args.args[0]
    assert warning.call_args.args[1:] == ('public-events', 'amount')
    assert 'full stream' in warning.call_args.args[0]


def test_invalid_decimal_bookmark_reports_stream_and_column():
    source = stream()
    state = {'bookmarks': {'public-events': {'replication_key_value': 'not decimal'}}}
    with patch('singer.write_message'), pytest.raises(ValueError, match='stream public-events, column amount'):
        incremental.sync_table({'limit': None}, source, state, ['amount'], metadata.to_map(source['metadata']))


def test_changed_numeric_typmod_triggers_schema_refresh():
    source = stream()
    payload = {'columns': [{'name': 'amount', 'type': 'numeric(38,17)'}]}
    assert logical_replication.changed_decimal_columns(payload, source) == {'amount'}
    payload['columns'][0]['type'] = 'numeric(38,18)'
    assert logical_replication.changed_decimal_columns(payload, source) == set()


@pytest.mark.parametrize('action', ['I', 'U', 'D'])
@pytest.mark.parametrize(('sql_type', 'value', 'expected'), [
    ('numeric(19,1)', Decimal('10000000000000000.0'), '10000000000000000.0'),
    ('numeric', Decimal(EXACT), EXACT),
    ('double precision', Decimal('1.25'), 1.25),
    ('integer', 42, 42),
    ('character varying(20)', 'source text', 'source text'),
])
def test_unresolved_wal_schema_refresh_uses_event_type_without_discovery_storm(action, sql_type, value, expected):
    source = stream(precision=18, scale=2)
    payload = {'action': action, 'schema': 'public', 'table': 'events',
               'identity' if action == 'D' else 'columns': [{'name': 'amount', 'type': sql_type, 'value': value}]}
    state = {'bookmarks': {'public-events': {'version': 1}}}
    with patch.object(logical_replication, 'refresh_streams_schema') as discover, \
            patch.object(common, 'write_schema_message') as schema_write, patch('singer.write_message') as write:
        for lsn in range(1, 4):
            logical_replication.consume_message(
                [source], state, SimpleNamespace(data_start=lsn), datetime(2026, 1, 1, tzinfo=timezone.utc),
                {'decimal_target': 'snowflake'}, message_payload=payload,
            )
    discover.assert_called_once()
    schema_write.assert_called_once()
    assert [call.args[0].record['amount'] for call in write.call_args_list] == [expected] * 3
    assert state['bookmarks']['public-events']['lsn'] == 3


@pytest.mark.parametrize('selected', [True, False])
def test_numeric_column_missing_after_discovery_retains_selection_and_source_value(selected):
    source = stream(precision=18, scale=2)
    md_map = metadata.to_map(source['metadata'])
    md_map[('properties', 'amount')]['selected'] = selected
    source['metadata'] = metadata.to_list(md_map)
    payload = {'action': 'I', 'schema': 'public', 'table': 'events',
               'columns': [{'name': 'amount', 'type': 'numeric(19,1)', 'value': Decimal('123.4')}]}

    def incomplete_discovery(_config, _streams):
        source['schema']['properties'].pop('amount')
        source['metadata'] = [item for item in source['metadata'] if item['breadcrumb'] != ['properties', 'amount']]

    with patch.object(logical_replication, 'refresh_streams_schema', side_effect=incomplete_discovery) as discover, \
            patch.object(common, 'write_schema_message'), patch('singer.write_message') as write:
        for lsn in range(1, 4):
            logical_replication.consume_message(
                [source], {'bookmarks': {'public-events': {'version': 1}}}, SimpleNamespace(data_start=lsn),
                datetime(2026, 1, 1, tzinfo=timezone.utc), {'decimal_target': 'snowflake'}, message_payload=payload,
            )
    discover.assert_called_once()
    assert metadata.to_map(source['metadata'])[('properties', 'amount')]['selected'] is selected
    for call in write.call_args_list:
        assert ('amount' in call.args[0].record) is selected
        if selected:
            assert call.args[0].record['amount'] == '123.4'


def test_wal_reconciliation_still_detects_subsequent_decimal_type_changes():
    source = stream(precision=18, scale=2)
    state = {'bookmarks': {'public-events': {'version': 1}}}
    with patch.object(logical_replication, 'refresh_streams_schema') as discover, \
            patch.object(common, 'write_schema_message'), patch('singer.write_message'):
        for lsn, sql_type in enumerate(['numeric(19,1)', 'numeric(19,1)', 'numeric(20,2)', 'numeric(20,2)'], 1):
            logical_replication.consume_message(
                [source], state, SimpleNamespace(data_start=lsn), datetime(2026, 1, 1, tzinfo=timezone.utc),
                {'decimal_target': 'snowflake'}, message_payload={
                    'action': 'I', 'schema': 'public', 'table': 'events',
                    'columns': [{'name': 'amount', 'type': sql_type, 'value': Decimal('123.4')}],
                },
            )
    assert discover.call_count == 2
    assert source['schema']['properties']['amount']['decimal'] == {'precision': 20, 'scale': 2}


def test_refresh_retains_decimal_target_and_user_column_selection():
    source = stream()
    source['metadata'][1]['metadata']['selected'] = False
    with patch.object(stream_utils, 'open_connection'), \
            patch.object(stream_utils, 'discover_db', return_value=[stream()]) as discover:
        stream_utils.refresh_streams_schema({'decimal_target': 'snowflake'}, [source])
    assert discover.call_args.kwargs == {'decimal_target': 'snowflake'}
    assert metadata.to_map(source['metadata'])[('properties', 'amount')]['selected'] is False
