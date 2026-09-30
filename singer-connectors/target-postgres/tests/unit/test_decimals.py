"""Exact SQL decimal transport and PostgreSQL schema evolution."""

import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

import target_postgres
from target_postgres.db_sync import DbSync, column_type


def decimal_schema(precision=29, scale=9):
    return {
        'type': ['null', 'string'],
        'format': 'singer.decimal',
        'decimal': {'precision': precision, 'scale': scale},
    }


def make_sync(properties, columns, keys=('id',)):
    sync = object.__new__(DbSync)
    sync.stream_schema_message = {'stream': 'public-items', 'key_properties': list(keys)}
    sync.flatten_schema = properties
    sync.schema_name = 'public'
    sync.data_flattening_max_level = 0
    sync.get_table_columns = Mock(return_value=columns)
    sync.query = Mock()
    sync.logger = Mock()
    sync._decimal_postgres_version = 160000
    return sync


@pytest.mark.parametrize(('precision', 'scale', 'expected'), [
    (29, 9, 'NUMERIC(29,9)'), (65, 30, 'NUMERIC(65,30)'), (None, None, 'NUMERIC'),
    (10, -2, 'NUMERIC(10,-2)'), (2, 4, 'NUMERIC(2,4)'),
])
def test_decimal_mapping_preserves_declared_domain(precision, scale, expected):
    assert column_type(decimal_schema(precision, scale)) == expected


def test_generic_api_number_and_legacy_decimal_string_are_unchanged():
    assert column_type({'type': ['number']}) == 'double precision'
    assert column_type({'type': ['string'], 'format': 'singer.decimal'}) == 'character varying'


@pytest.mark.parametrize(('data_type', 'precision', 'scale'), [
    ('double precision', 53, None), ('numeric', 29, 8), ('numeric', 28, 9),
    ('numeric', 30, 9), ('numeric', None, None),
])
def test_changed_decimal_type_or_dimensions_versions_once(data_type, precision, scale):
    sync = make_sync({'amount': decimal_schema()}, [{
        'column_name': 'amount', 'data_type': data_type, 'numeric_precision': precision, 'numeric_scale': scale,
    }])

    sync.update_columns()

    queries = [call.args[0] for call in sync.query.call_args_list]
    assert len(queries) == 2
    assert queries[0].startswith('ALTER TABLE public."items" RENAME COLUMN "amount" TO "amount_')
    assert queries[1] == 'ALTER TABLE public."items" ADD COLUMN "amount" NUMERIC(29,9)'
    sync.get_table_columns.return_value = [
        {'column_name': 'amount', 'data_type': 'numeric', 'numeric_precision': 29, 'numeric_scale': 9},
        {'column_name': 'amount_20260929_100000_000001', 'data_type': data_type,
         'numeric_precision': precision, 'numeric_scale': scale},
    ]
    sync.query.reset_mock()

    sync.update_columns()

    sync.query.assert_not_called()


@pytest.mark.parametrize(('precision', 'scale'), [(29, 9), (None, None), (10, -2), (2, 4)])
def test_matching_decimal_metadata_keeps_column(precision, scale):
    sync = make_sync({'amount': decimal_schema(precision, scale)}, [{
        'column_name': 'amount', 'data_type': 'numeric', 'numeric_precision': precision, 'numeric_scale': scale,
    }])
    sync.update_columns()
    sync.query.assert_not_called()


def test_decimal_key_change_refused_before_other_additions():
    sync = make_sync({'new_column': {'type': ['string']}, 'id': decimal_schema()}, [
        {'column_name': 'id', 'data_type': 'double precision'},
    ])

    with pytest.raises(ValueError, match='primary-key'):
        sync.update_columns()

    sync.query.assert_not_called()


def test_unmarked_key_evolution_keeps_legacy_behavior():
    sync = make_sync({'id': {'type': ['string']}}, [{'column_name': 'id', 'data_type': 'numeric'}])
    sync.update_columns()
    assert sync.query.call_count == 2
    assert sync.query.call_args.args[0] == 'ALTER TABLE public."items" ADD COLUMN "id" character varying'


def test_versioned_name_avoids_collision_and_utf8_identifier_overflow():
    sync = make_sync({}, [])
    base = 'é' * 31
    existing = 'é' * 20 + '_20260929_100000_123456'
    with patch('target_postgres.db_sync.datetime') as clock:
        clock.now.return_value = datetime(2026, 9, 29, 10, 0, 0, 123456, tzinfo=timezone.utc)
        archived = sync.version_column(f'"{base}"', 'public-items', {existing})

    assert archived == 'é' * 20 + '_20260929_100000_123457'
    assert len(archived.encode('utf-8')) == 63
    assert f'TO "{archived}"' in sync.query.call_args.args[0]


def test_csv_preserves_large_decimal_and_sql_null():
    sync = make_sync({'amount': decimal_schema(65, 30)}, [])
    amount = '12345678901234567890123456789012345.123456789012345678901234567890'
    assert sync.record_to_csv_line({'amount': amount}) == f'"{amount}"'
    assert sync.record_to_csv_line({'amount': None}) == ''
    assert sync.record_to_csv_line({'amount': '0.000'}) == '"0.000"'


def test_decimal_key_identity_is_exact_and_independent_of_text_scale():
    sync = make_sync({'id': decimal_schema()}, [])
    assert sync.record_primary_key_string({'id': '1.0'}) == sync.record_primary_key_string({'id': '1.00'})
    assert sync.record_primary_key_string({'id': '9007199254740992.00'}) != (
        sync.record_primary_key_string({'id': '9007199254740992.01'})
    )


@pytest.mark.parametrize('value', [1.25, '1.1234567891', '100000000000000000000'])
def test_invalid_decimal_record_rejected_even_without_generic_validation(value):
    messages = [
        {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'],
         'schema': {'properties': {'id': {'type': ['integer']}, 'amount': decimal_schema()}}},
        {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, 'amount': value}},
    ]
    with patch('target_postgres.DbSync'), patch('target_postgres.flush_streams') as flush:
        with pytest.raises(ValueError):
            target_postgres.persist_lines({'validate_records': False}, [json.dumps(message) for message in messages])
    flush.assert_not_called()


def test_exact_decimal_string_reaches_target_buffer():
    amount = '12345678901234567890.123456789'
    messages = [
        {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'],
         'schema': {'properties': {'id': {'type': ['integer']}, 'amount': decimal_schema()}}},
        {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, 'amount': amount}},
    ]
    with patch('target_postgres.DbSync') as sync, patch('target_postgres.flush_streams') as flush:
        sync.return_value.record_primary_key_string.return_value = '1'
        flush.return_value = None
        target_postgres.persist_lines({'validate_records': True}, [json.dumps(message) for message in messages])
    assert flush.call_args.args[0]['public-items']['1']['amount'] == amount


@pytest.mark.parametrize('nested_schema', [
    {'type': ['object'], 'properties': {'amount': decimal_schema()}},
    {'type': ['array'], 'items': decimal_schema()},
])
def test_decimal_validation_cache_tracks_nested_fields_and_schema_removal(nested_schema):
    messages = []
    for schema in ({'type': ['string']}, nested_schema, {'type': ['string']}):
        messages.extend([
            {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'],
             'schema': {'properties': {'id': {'type': ['integer']}, 'value': schema}}},
            {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, 'value': None}},
        ])
    with patch('target_postgres.DbSync'), patch('target_postgres.flush_streams', return_value=None), \
            patch('target_postgres.validate_decimal_record') as validate:
        target_postgres.persist_lines({}, [json.dumps(message) for message in messages])
    assert validate.call_count == 1
    assert validate.call_args.args[1]['properties']['value'] == nested_schema


def test_encoded_negative_scale_metadata_does_not_version_column():
    sync = make_sync({'amount': decimal_schema(10, -2)}, [{
        'column_name': 'amount', 'data_type': 'numeric', 'numeric_precision': 10, 'numeric_scale': 2046,
    }])
    sync.update_columns()
    sync.update_columns()
    sync.query.assert_not_called()


@pytest.mark.parametrize(('precision', 'scale'), [(10, -2), (2, 4)])
def test_old_postgres_uses_unbounded_numeric_and_retains_it(precision, scale):
    sync = make_sync({'amount': decimal_schema(precision, scale)}, [{
        'column_name': 'amount', 'data_type': 'numeric', 'numeric_precision': None, 'numeric_scale': None,
    }])
    del sync._decimal_postgres_version
    sync.query.return_value = [{'server_version_num': '140015'}]
    sync.update_columns()
    sync.update_columns()
    sync.query.assert_called_once_with('SHOW server_version_num')
    assert '"amount" NUMERIC' in sync.create_table_query()
    assert f'NUMERIC({precision},{scale})' not in sync.create_table_query()
