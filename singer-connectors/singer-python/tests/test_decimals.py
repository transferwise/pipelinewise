from decimal import Decimal, localcontext

import pytest

from singer import Catalog, RecordMessage, Schema, Transformer, format_message, parse_message
from singer.decimal_support import (
    decimal_bookmark,
    decimal_canonical_string,
    decimal_key,
    decimal_schema,
    decimal_sort_key,
    decimal_sql_type,
    decimal_to_string,
    is_decimal_schema,
    postgres_numeric_scale,
    schema_has_decimals,
    validate_decimal_record,
)


def test_declared_decimal_survives_catalog_and_message_roundtrip():
    field = decimal_schema(38, 18)
    catalog = Catalog.from_dict({'streams': [{
        'stream': 'transactions',
        'schema': {'type': 'object', 'properties': {'amount': field}},
    }]})
    schema = catalog.to_dict()['streams'][0]['schema']
    assert schema['properties']['amount'] == field
    exact = '12345678901234567890.123456789012345678'
    record = Transformer().transform({'amount': Decimal(exact)}, schema)
    decoded = parse_message(format_message(RecordMessage('transactions', record)))
    assert decoded.record['amount'] == exact
    validate_decimal_record(decoded.record, schema)


@pytest.mark.parametrize('value', ['12345678901234567890.123456789012345678', '-0.000000000000000001'])
def test_validation_does_not_round_to_decimal_context(value):
    with localcontext() as context:
        context.prec = 2
        assert Decimal(decimal_to_string(Decimal(value), decimal_schema(38, 18))) == Decimal(value)


@pytest.mark.parametrize(('precision', 'scale', 'value'), [
    (4, 2, '99.99'), (4, 2, '-99.99'), (4, 2, '1.2300'), (38, 0, '9' * 38),
    (38, 37, '0.' + '9' * 37), (2, 4, '0.0099'), (2, -2, '9900'), (2, -2, '0'),
])
def test_exact_declared_boundaries(precision, scale, value):
    assert decimal_to_string(value, decimal_schema(precision, scale)) == value


@pytest.mark.parametrize(('precision', 'scale', 'value'), [
    (4, 2, '100.00'), (4, 2, '0.001'), (38, 0, '1' + '0' * 38),
    (2, 4, '0.01'), (2, -2, '9901'), (2, -2, '10000'),
])
def test_out_of_range_values_fail_without_rounding(precision, scale, value):
    with pytest.raises(ValueError, match='does not fit'):
        decimal_to_string(value, decimal_schema(precision, scale))


@pytest.mark.parametrize('value', [1.1, True, b'1.2', 'not-a-number', '1_000', ' 1.0', 'Infinity'])
def test_bounded_decimal_rejects_lossy_or_invalid_values(value):
    with pytest.raises(ValueError):
        decimal_to_string(value, decimal_schema(8, 2))


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-Infinity'])
def test_postgres_unbounded_nonfinite_values(value):
    schema = decimal_schema(None, None, 'postgres')
    assert decimal_to_string(value, schema) == value
    assert decimal_sql_type(schema, 'postgres') == 'NUMERIC'


def test_postgres_bounded_nan_uses_decimal_transport():
    assert decimal_to_string(Decimal('NaN'), decimal_schema(18, 2)) == 'NaN'


def test_decimal_bookmark_extrema_follow_postgres_nonfinite_ordering():
    values = ['NaN', '1.000000000000000001', '-Infinity', 'Infinity', '-0.01', '1.000000000000000000']
    assert sorted(values, key=decimal_sort_key) == [
        '-Infinity', '-0.01', '1.000000000000000000', '1.000000000000000001', 'Infinity', 'NaN',
    ]


@pytest.mark.parametrize(('precision', 'scale', 'expected'), [
    (39, 0, 'FLOAT'), (38, 38, 'FLOAT'), (2, 3, 'NUMERIC(3,3)'),
    (2, -1, 'NUMERIC(3,0)'), (None, None, 'FLOAT'), (65, 30, 'FLOAT'),
])
def test_snowflake_fallback_keeps_original_source_dimensions(precision, scale, expected):
    schema = decimal_schema(precision, scale, 'snowflake')
    assert decimal_sql_type(schema, 'snowflake') == expected
    assert schema['decimal'] == {'precision': precision, 'scale': scale}


@pytest.mark.parametrize(('precision', 'scale'), [(38, 37), (20, 4), (1, 0)])
def test_snowflake_retains_precision_and_scale(precision, scale):
    assert decimal_sql_type(decimal_schema(precision, scale), 'snowflake') == f'NUMERIC({precision},{scale})'


@pytest.mark.parametrize(('precision', 'scale'), [(1000, 1000), (20, -5), (2, 4)])
def test_postgres_retains_legal_declarations(precision, scale):
    assert decimal_sql_type(decimal_schema(precision, scale), 'postgres') == f'NUMERIC({precision},{scale})'


@pytest.mark.parametrize(('precision', 'scale'), [(0, 0), (True, 1), (4, False), (None, 2), (4, None)])
def test_invalid_dimensions_fail(precision, scale):
    with pytest.raises(ValueError):
        decimal_schema(precision, scale)


def test_generic_numbers_and_legacy_decimal_strings_keep_their_types():
    legacy = {'type': ['null', 'string'], 'format': 'singer.decimal'}
    assert not is_decimal_schema(legacy)
    assert Schema.from_dict(legacy).to_dict() == legacy
    schema = {'type': 'object', 'properties': {'number': {'type': 'number'}, 'legacy': legacy}}
    record = Transformer().transform({'number': '1.25', 'legacy': '1.250000'}, schema)
    assert record == {'number': 1.25, 'legacy': '1.250000'}


def test_recursive_wire_validation_requires_strings_and_preserves_null():
    schema = {'type': 'object', 'properties': {
        'items': {'type': 'array', 'items': {'type': 'object', 'properties': {'amount': decimal_schema(8, 2)}}},
    }}
    validate_decimal_record({'items': [{'amount': '1.23'}, {'amount': None}]}, schema)
    with pytest.raises(ValueError, match='strings'):
        validate_decimal_record({'items': [{'amount': 1.23}]}, schema)


def test_marked_transformer_rejects_already_rounded_float():
    with pytest.raises(ValueError, match='Decimal, integer or decimal text'):
        Transformer().transform({'amount': 1.23}, {
            'type': 'object', 'properties': {'amount': decimal_schema(8, 2)},
        })


@pytest.mark.parametrize('values', [
    ['1', '1.0', '1.00', '1e0'], ['0', '-0.00', '0e38'],
    ['12345678901234567890.123456789012345678', '12345678901234567890.12345678901234567800'],
    ['Infinity', '+Infinity'],
])
def test_equal_decimal_keys_coalesce_without_context_rounding(values):
    with localcontext() as context:
        context.prec = 2
        assert len({decimal_key(value) for value in values}) == 1


def test_decimal_keys_preserve_distinct_low_digits():
    assert decimal_key('12345678901234567890.123456789012345678') != \
        decimal_key('12345678901234567890.123456789012345679')


@pytest.mark.parametrize('source', ['9999999999999999.99', '-9999999999999999.99', '0.1', '-0.1'])
def test_legacy_float_bookmark_replays_the_rounded_source_boundary(source):
    schema = decimal_schema(38, 18)
    boundary = decimal_bookmark(float(source), schema)
    assert Decimal(boundary) < Decimal(source)
    assert decimal_bookmark(source, schema) == source


@pytest.mark.parametrize('value', [
    float('inf'), float('-inf'), float('nan'), -float.fromhex('0x1.fffffffffffffp+1023'),
])
def test_unbounded_legacy_bookmark_requests_full_replay(value):
    assert decimal_bookmark(value) is None


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-Infinity'])
def test_nonfinite_decimal_bookmarks_replay_all_rows(value):
    assert decimal_bookmark(value, decimal_schema(None, None)) is None


@pytest.mark.parametrize(('scale', 'expected'), [
    (2046, -2), (1048, -1000), (-2, -2), (0, 0), (1000, 1000), (None, None),
])
def test_postgres_metadata_scale_decoding(scale, expected):
    assert postgres_numeric_scale(scale) == expected


def test_old_postgres_target_uses_unbounded_numeric_for_newer_declarations():
    schema = decimal_schema(10, -2)
    assert decimal_sql_type(schema, 'postgres', postgres_version=140000) == 'NUMERIC'
    assert decimal_sql_type(schema, 'postgres', postgres_version=160000) == 'NUMERIC(10,-2)'
    assert decimal_sql_type(decimal_schema(3, 5), 'postgres', postgres_version=140000) == 'NUMERIC'


def test_lossy_numeric_keys_use_text_without_merging_adjacent_values():
    schema = decimal_schema(65, 30)
    assert decimal_sql_type(schema, 'snowflake', is_key=True) == 'VARCHAR(134217728)'
    first = '12345678901234567890123456789012345.000000000000000000000000000001'
    second = '12345678901234567890123456789012345.000000000000000000000000000002'
    assert float(first) == float(second)
    assert decimal_canonical_string(first) != decimal_canonical_string(second)
    assert decimal_canonical_string('1000.00') == decimal_canonical_string('1e3') == '1000'
    assert decimal_canonical_string('-0.000') == '0'


def test_postgres_numeric_keys_use_lossless_text_only_for_snowflake():
    schema = decimal_schema(10, 2)
    assert decimal_sql_type(schema, 'snowflake', is_key=True, source='postgres') == 'VARCHAR(134217728)'
    assert decimal_sql_type(schema, 'snowflake', source='postgres') == 'NUMERIC(10,2)'
    assert decimal_sql_type(schema, 'snowflake', is_key=True) == 'NUMERIC(10,2)'
    assert decimal_sql_type(schema, 'snowflake', is_key=True, source='mysql') == 'NUMERIC(10,2)'
    assert decimal_sql_type(schema, 'postgres', is_key=True, source='postgres') == 'NUMERIC(10,2)'
    assert decimal_canonical_string('NaN') == 'NaN'


def test_decimal_presence_walk_handles_empty_and_nested_schemas():
    assert not schema_has_decimals({})
    assert not schema_has_decimals({'type': 'object', 'properties': {'n': {'type': 'number'}}})
    assert schema_has_decimals({'type': 'array', 'items': {'properties': {'n': decimal_schema(8, 2)}}})


def test_legacy_unmarked_decimal_serialization_keeps_published_behavior():
    message = RecordMessage('legacy', {'amount': Decimal('1.23')})
    assert parse_message(format_message(message)).record['amount'] == 1.23
