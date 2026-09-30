"""Declared decimals retain digits in discovery, records and resumable state."""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import singer
from pymysql.constants import FIELD_TYPE
from singer import Schema, metadata
from singer.catalog import Catalog, CatalogEntry

from tap_mysql import _discover_catalog, discover_utils
from tap_mysql.sync_strategies import binlog, common, incremental


EXACT = '12345678901234567890.123456789012345678'


def column(precision=38, scale=18, data_type='decimal'):
    return discover_utils.Column(
        'source', 'events', 'amount', data_type, None, precision, scale,
        f'{data_type}({precision},{scale})', '', False,
    )


def entry(target='snowflake', precision=38, scale=18, selected=True):
    schema = discover_utils.schema_for_column(column(precision, scale), target)
    return CatalogEntry(
        tap_stream_id='source-events', table='events', stream='source-events',
        schema=Schema(type='object', properties={'amount': schema}),
        metadata=metadata.to_list({
            (): {'database-name': 'source', 'replication-method': 'INCREMENTAL', 'replication-key': 'amount'},
            ('properties', 'amount'): {'selected': selected, 'inclusion': 'available'},
        }),
    )


def test_discovery_retains_declared_dimensions_without_rejecting_unselected_columns():
    assert discover_utils.schema_for_column(column(65, 30), 'snowflake').to_dict()['decimal'] == {
        'precision': 65, 'scale': 30,
    }
    discovered = Catalog(streams=[entry(precision=65, scale=30)])
    unselected = entry(precision=65, scale=30, selected=False)
    unselected.metadata[0]['metadata'].pop('replication-key')
    assert not discover_utils.resolve_catalog(
        discovered, [unselected], 'snowflake',
    ).streams[0].schema.properties
    assert discover_utils.resolve_catalog(discovered, discovered.streams, 'snowflake').streams
    assert discover_utils.resolve_catalog(discovered, discovered.streams, 'postgres').streams


@pytest.mark.parametrize('target', [None, 'snowflake', 'postgres'])
def test_real_float_columns_keep_legacy_mapping(target):
    assert discover_utils.schema_for_column(column(data_type='double'), target).to_dict() == {
        'type': ['null', 'number'], 'inclusion': 'available',
    }


def test_disabled_decimal_mapping_remains_legacy():
    schema = discover_utils.schema_for_column(column()).to_dict()
    assert schema == {'type': ['null', 'number'], 'multipleOf': 1e-18, 'inclusion': 'available'}
    record = common.row_to_singer_record(entry(None), 1, [Decimal(EXACT)], ['amount'], None)
    assert record.record['amount'] == Decimal(EXACT)


@pytest.mark.parametrize('value', [Decimal(EXACT), None])
@pytest.mark.parametrize('source', ['select', 'binlog'])
def test_exact_wire_value_for_select_and_binlog(value, source):
    catalog = entry()
    if source == 'select':
        record = common.row_to_singer_record(catalog, 1, [value], ['amount'], None)
    else:
        record = binlog.row_to_singer_record(
            catalog, 1, {'amount': FIELD_TYPE.NEWDECIMAL}, {'amount': value}, None,
        )
    assert json.loads(singer.messages.format_message(record))['record']['amount'] == (
        EXACT if value is not None else None
    )


def test_incremental_checkpoint_preserves_digits_and_resume_parameter():
    catalog = entry()
    state = {'bookmarks': {'source-events': {'replication_key': 'amount'}}}
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(Decimal(EXACT),), None]
    with patch('singer.write_message'):
        common.sync_query(cursor, catalog, state, 'SELECT amount', ['amount'], 1, {})
    assert state['bookmarks']['source-events']['replication_key_value'] == EXACT

    connection = MagicMock()
    with patch('tap_mysql.sync_strategies.incremental.connect_with_backoff', return_value=connection), \
            patch('tap_mysql.sync_strategies.incremental.common.sync_query') as query, \
            patch('singer.write_message'):
        incremental.sync_table(MagicMock(), catalog, state, ['amount'])
    assert query.call_args.args[-1] == {'replication_key_value': EXACT}


@pytest.mark.parametrize('value', ['9999999999999999.99', '-9999999999999999.99', '0.01'])
def test_legacy_decimal_checkpoint_replays_before_rounded_boundary(value):
    state = {'bookmarks': {'source-events': {'replication_key': 'amount', 'replication_key_value': float(value)}}}
    connection = MagicMock()
    with patch('tap_mysql.sync_strategies.incremental.connect_with_backoff', return_value=connection), \
            patch.object(common, 'sync_query') as query, patch('singer.write_message'), \
            patch.object(common.LOGGER, 'warning') as warning:
        incremental.sync_table(MagicMock(), entry(precision=18, scale=2), state, ['amount'])
    boundary = query.call_args.args[-1]['replication_key_value']
    assert isinstance(boundary, str)
    assert Decimal(boundary) < Decimal(value)
    assert warning.call_args.args[1:] == ('source-events', 'amount')


@pytest.mark.parametrize('value', [float('inf'), float('-inf'), float('nan'), 'NaN', 'Infinity', '-Infinity'])
def test_nonfinite_legacy_decimal_checkpoint_replays_without_filter(value):
    state = {'bookmarks': {'source-events': {'replication_key': 'amount', 'replication_key_value': value}}}
    with patch('tap_mysql.sync_strategies.incremental.connect_with_backoff'), \
            patch.object(common, 'sync_query') as query, patch('singer.write_message'), \
            patch.object(common.LOGGER, 'warning') as warning:
        incremental.sync_table(MagicMock(), entry(), state, ['amount'])
    assert query.call_args.args[-1] == {}
    assert 'WHERE' not in query.call_args.args[3]
    assert warning.call_args.args[1:] == ('source-events', 'amount')
    assert 'full stream' in warning.call_args.args[0]


def test_invalid_decimal_bookmark_reports_stream_and_column():
    state = {'bookmarks': {'source-events': {'replication_key': 'amount', 'replication_key_value': 'not decimal'}}}
    with pytest.raises(ValueError, match='stream source-events, column amount'):
        incremental.sync_table(MagicMock(), entry(), state, ['amount'])


def test_discovery_option_is_preserved():
    with patch('tap_mysql.discover_catalog') as discovery:
        _discover_catalog(MagicMock(), {'decimal_target': 'snowflake', 'filter_dbs': 'source'})
    assert discovery.call_args.kwargs == {'decimal_target': 'snowflake'}


def test_binlog_detects_changed_scale_without_new_column_names():
    event = SimpleNamespace(columns=[
        SimpleNamespace(name='amount', type=FIELD_TYPE.NEWDECIMAL, precision=38, decimals=17),
    ])
    assert binlog.changed_decimal_columns(event, entry()) == {'amount'}
    event.columns[0].decimals = 18
    assert binlog.changed_decimal_columns(event, entry()) == set()


@pytest.mark.parametrize('name', [None, '', '__dropped_col_2__'])
def test_binlog_decimal_reconciliation_ignores_unnamed_and_dropped_columns(name):
    catalog = entry()
    original = catalog.schema.to_dict()
    event = SimpleNamespace(columns=[
        SimpleNamespace(name=name, type=FIELD_TYPE.NEWDECIMAL, precision=5, decimals=2),
    ])
    assert binlog.changed_decimal_columns(event, catalog) == set()
    binlog.reconcile_binlog_decimal_schema(event, catalog, catalog, 'snowflake')
    assert catalog.schema.to_dict() == original


def test_binlog_rediscovery_preserves_excluded_decimal_column():
    previous = entry(precision=65, scale=30, selected=False)
    discovered = entry(precision=65, scale=30)
    binlog.retain_column_selection(previous, discovered)
    assert not common.property_is_selected(discovered, 'amount')


@pytest.mark.parametrize('selected', [False, True])
def test_binlog_reconciles_historical_decimal_dimensions_and_preserves_selection(selected):
    previous = entry(precision=5, scale=3, selected=selected)
    discovered = entry(precision=5, scale=3)
    event = SimpleNamespace(columns=[
        SimpleNamespace(name='amount', type=FIELD_TYPE.NEWDECIMAL, precision=5, decimals=2),
    ])
    binlog.reconcile_binlog_decimal_schema(event, discovered, previous, 'snowflake')
    binlog.retain_column_selection(previous, discovered)
    assert discovered.schema.properties['amount'].to_dict()['decimal'] == {'precision': 5, 'scale': 2}
    assert common.property_is_selected(discovered, 'amount') == selected
    assert binlog.changed_decimal_columns(event, discovered) == set()
    assert binlog.row_to_singer_record(
        discovered, 1, {'amount': FIELD_TYPE.NEWDECIMAL}, {'amount': Decimal('123.45')}, None,
    ).record == {'amount': '123.45'}
    event.columns[0].decimals = 3
    assert binlog.changed_decimal_columns(event, discovered) == {'amount'}


@pytest.mark.parametrize('field_type,value,expected_type', [
    (FIELD_TYPE.LONG, 123, 'integer'),
    (FIELD_TYPE.DOUBLE, 123.45, 'number'),
    (FIELD_TYPE.VARCHAR, 'historical value', 'string'),
])
def test_binlog_reconciles_nonnumeric_event_against_current_decimal_schema(field_type, value, expected_type):
    previous = entry(precision=5, scale=3)
    discovered = entry(precision=5, scale=3)
    event = SimpleNamespace(columns=[SimpleNamespace(name='amount', type=field_type)])
    binlog.reconcile_binlog_decimal_schema(event, discovered, previous, 'snowflake')
    assert discovered.schema.properties['amount'].type == ['null', expected_type]
    assert binlog.changed_decimal_columns(event, discovered) == set()
    assert binlog.row_to_singer_record(discovered, 1, {'amount': field_type}, {'amount': value}, None).record == {
        'amount': value,
    }


def test_binlog_reconciliation_restores_decimal_omitted_by_live_discovery():
    previous = entry(precision=5, scale=3, selected=False)
    discovered = entry(precision=5, scale=3)
    discovered.schema.properties.clear()
    discovered.metadata = [item for item in discovered.metadata if not item['breadcrumb']]
    event = SimpleNamespace(columns=[
        SimpleNamespace(name='amount', type=FIELD_TYPE.NEWDECIMAL, precision=5, decimals=2),
    ])
    binlog.reconcile_binlog_decimal_schema(event, discovered, previous, 'snowflake')
    binlog.retain_column_selection(previous, discovered)
    assert discovered.schema.properties['amount'].to_dict()['decimal'] == {'precision': 5, 'scale': 2}
    assert not common.property_is_selected(discovered, 'amount')
