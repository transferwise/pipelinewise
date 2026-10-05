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


def test_mysql_year_uses_the_same_smallint_type_as_fastsync():
    assert column_type({
        'type': ['null', 'integer'], 'format': 'singer.year', 'minimum': 0, 'maximum': 2155,
    }) == 'smallint'


@pytest.mark.parametrize(('data_type', 'precision', 'scale'), [
    ('numeric', 29, 8), ('numeric', 28, 9), ('numeric', 30, 9), ('numeric', None, None),
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


@pytest.mark.parametrize('legacy_type', ['double precision', 'real'])
def test_legacy_floating_decimal_column_is_retained_for_target_and_staging(legacy_type):
    sync = make_sync({'amount': decimal_schema()}, [
        {'column_name': 'amount', 'data_type': legacy_type},
    ])

    sync.update_columns()

    sync.query.assert_not_called()
    assert sync.retained_column_types == {'amount': legacy_type}
    assert f'"amount" {legacy_type}' in sync.create_table_query(is_temporary=True)
    assert '"amount" NUMERIC(29,9)' in sync.create_table_query()


@pytest.mark.parametrize(('legacy_type', 'limit'), [
    ('double precision', '1.7976931348623157e+308'),
    ('real', '3.4028234663852886e+38'),
])
def test_retained_floating_decimal_saturates_overflow_without_changing_exact_columns(legacy_type, limit):
    sync = make_sync({'amount': decimal_schema(None, None)}, [
        {'column_name': 'amount', 'data_type': legacy_type},
    ])
    sync.update_columns()

    assert sync.record_to_csv_line({'amount': '1e999'}) == f'"{limit}"'
    assert sync.record_to_csv_line({'amount': '-1e999'}) == f'"-{limit}"'
    assert sync.record_to_csv_line({'amount': '1e-999'}) == '"0"'
    assert sync.record_to_csv_line({'amount': '-1e-999'}) == '"0"'
    for value in ('1.25', 'NaN', 'Infinity', '-Infinity'):
        assert sync.record_to_csv_line({'amount': value}) == f'"{value}"'
    assert sync.record_to_csv_line({'amount': '+Infinity'}) == '"Infinity"'

    exact = make_sync({'amount': decimal_schema(None, None)}, [])
    assert exact.record_to_csv_line({'amount': '1e999'}) == '"1e999"'


@pytest.mark.parametrize('legacy_type', ['double precision', 'real'])
def test_legacy_floating_decimal_key_does_not_block_other_additions(legacy_type):
    sync = make_sync({'new_column': {'type': ['string']}, 'id': decimal_schema()}, [
        {'column_name': 'id', 'data_type': legacy_type},
    ])

    sync.update_columns()

    sync.query.assert_called_once_with('ALTER TABLE public."items" ADD COLUMN "new_column" character varying')
    assert f'"id" {legacy_type}' in sync.create_table_query(is_temporary=True)


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


@pytest.mark.parametrize(('legacy_type', 'first', 'second', 'different'), [
    ('double precision', '9007199254740992', '9007199254740993', '9007199254740994'),
    ('real', '16777216', '16777217', '16777218'),
    ('double precision', '1e999', '2e999', '-1e999'),
    ('real', '0', '-1e-999', '1'),
])
def test_retained_decimal_key_identity_matches_the_stored_float(legacy_type, first, second, different):
    sync = make_sync({'id': decimal_schema(None, None), 'tenant': {'type': ['integer']}}, [
        {'column_name': 'id', 'data_type': legacy_type},
        {'column_name': 'tenant', 'data_type': 'integer'},
    ], keys=('tenant', 'id'))
    sync.update_columns()
    assert sync.record_primary_key_string({'tenant': 1, 'id': first}) == (
        sync.record_primary_key_string({'tenant': 1, 'id': second})
    )
    assert sync.record_primary_key_string({'tenant': 1, 'id': first}) != (
        sync.record_primary_key_string({'tenant': 1, 'id': different})
    )
    assert sync.record_primary_key_string({'tenant': 1, 'id': first}) != (
        sync.record_primary_key_string({'tenant': 2, 'id': second})
    )
    assert sync._retained_decimal_value('id', first) == sync._retained_decimal_value('id', second)


def test_mysql_existing_key_membership_is_retained_across_schema_restarts():
    sync = make_sync({
        'id': {'type': ['integer']},
        'year': {'type': ['integer'], 'format': 'singer.year', 'maximum': 2155},
    }, [{'column_name': 'id', 'data_type': 'numeric'}], keys=('id', 'year'))
    sync.connection_config = {'source_tap_type': 'tap-mysql'}
    sync.query.return_value = [{'column_name': 'id'}]

    sync.update_columns()

    assert sync.primary_key_properties() == ['id']
    assert sync.record_primary_key_string({'id': 1, 'year': 2026}) == '1'
    assert sync.primary_key_condition('t') == 's."id" = t."id"'
    assert sync.primary_key_null_condition('t') == 't."id" is null'
    assert 'PRIMARY KEY ("id")' in sync.create_table_query(is_temporary=True)

    sync.get_table_columns.return_value.append({'column_name': 'year', 'data_type': 'smallint'})
    sync.effective_key_properties = ['id', 'year']
    sync.update_columns()
    assert sync.primary_key_properties() == ['id']


def test_mysql_binary_key_identity_and_csv_use_fastsync_hex_casing():
    sync = make_sync({'id': {'type': ['string'], 'format': 'binary'}}, [
        {'column_name': 'id', 'data_type': 'character varying'},
    ])
    sync.connection_config = {'source_tap_type': 'tap-mysql'}
    sync.query.return_value = [{'column_name': 'id'}]
    sync.update_columns()

    assert sync.record_primary_key_string({'id': '00ff'}) == '00FF'
    assert sync.record_to_csv_line({'id': '00ff'}) == '"00FF"'


def test_mysql_legacy_varchar_year_key_keeps_live_type_in_staging():
    sync = make_sync({'year': {'type': ['integer'], 'format': 'singer.year', 'maximum': 2155}}, [
        {'column_name': 'year', 'data_type': 'character varying'},
    ], keys=('year',))
    sync.connection_config = {'source_tap_type': 'tap-mysql'}
    sync.query.return_value = [{'column_name': 'year'}]
    sync.update_columns()

    assert sync.query.call_count == 1
    assert sync.retained_column_types == {'year': 'character varying'}
    assert '"year" character varying' in sync.create_table_query(is_temporary=True)
    assert '"year" smallint' in sync.create_table_query()
    assert sync.record_primary_key_string({'year': 2026}) == '2026'
    assert sync.record_to_csv_line({'year': 2026}) == '2026'


def test_composite_key_boundaries_do_not_collide_when_set_values_contain_commas():
    sync = make_sync({'first': {'type': ['string']}, 'second': {'type': ['string']}}, [],
                     keys=('first', 'second'))
    assert sync.record_primary_key_string({'first': 'a,b', 'second': 'c'}) != (
        sync.record_primary_key_string({'first': 'a', 'second': 'b,c'})
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
