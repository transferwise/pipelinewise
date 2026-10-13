import json
from unittest.mock import Mock

import pytest
import singer
from singer.decimal_support import decimal_schema

import transform_field
from transform_field import TransformField
from transform_field.errors import InvalidTransformationException


def _run(monkeypatch, transformation, amount, when=None):
    output = []
    monkeypatch.setattr(singer, 'write_message', lambda message: output.append(singer.format_message(message)))
    transformer = TransformField({'transformations': [{
        'tap_stream_name': 'transactions', 'field_id': transformation[0], 'type': transformation[1], 'when': when,
    }]})
    schema = {'type': 'object', 'properties': {
        'amount': decimal_schema(38, 18), 'other': {'type': ['null', 'string']},
    }}
    transformer.handle_line(json.dumps({'type': 'SCHEMA', 'stream': 'transactions',
                                        'schema': schema, 'key_properties': []}))
    transformer.handle_line(json.dumps({'type': 'RECORD', 'stream': 'transactions',
                                        'record': {'amount': amount, 'other': 'private'}}))
    transformer.handle_line(json.dumps({'type': 'STATE', 'value': {'bookmark': amount}}))
    transformer.flush()
    return [json.loads(line) for line in output]


def test_transforming_another_field_preserves_all_decimal_digits(monkeypatch):
    exact = '12345678901234567890.123456789012345678'
    messages = _run(monkeypatch, ('other', 'SET-NULL'), exact)
    assert messages[0]['schema']['properties']['amount'] == decimal_schema(38, 18)
    assert messages[1]['record'] == {'amount': exact, 'other': None}
    assert messages[2] == {'type': 'STATE', 'value': {'bookmark': exact}}


@pytest.mark.parametrize(('transformation', 'expected'), [('MASK-NUMBER', '0'), ('SET-NULL', None)])
def test_numeric_transformations_use_decimal_transport(monkeypatch, transformation, expected):
    messages = _run(monkeypatch, ('amount', transformation), '123.450000000000000000')
    assert messages[1]['record']['amount'] == expected


def test_null_decimal_survives_unrelated_transformation(monkeypatch):
    assert _run(monkeypatch, ('other', 'SET-NULL'), None)[1]['record']['amount'] is None


def test_hash_remains_unsupported_for_decimals(monkeypatch):
    with pytest.raises(InvalidTransformationException):
        _run(monkeypatch, ('amount', 'HASH'), '123.45')


def test_invalid_decimal_fails_even_without_record_validation(monkeypatch):
    with pytest.raises(ValueError):
        _run(monkeypatch, ('other', 'SET-NULL'), 123.45)


@pytest.mark.parametrize(('amount', 'condition', 'matches'), [
    ('1.250000000000000000', 1.25, True), ('0.000000000000000000', 0, True),
    ('1.250000000000000000', 2.5, False),
    ('NaN', 'NaN', True),
    ('1.250000000000000000', 'not-a-number', False),
    ('12345678901234567890.123456789012345678', '12345678901234567890.123456789012345678', True),
    ('12345678901234567890.123456789012345678', '12345678901234567890.123456789012345679', False),
])
def test_decimal_conditions_compare_exact_values(monkeypatch, amount, condition, matches):
    messages = _run(monkeypatch, ('other', 'SET-NULL'), amount, [{'column': 'amount', 'equals': condition}])
    assert messages[1]['record']['other'] == (None if matches else 'private')


def test_generic_zero_condition_retains_existing_behavior():
    from transform_field.transform import is_transform_required
    assert not is_transform_required({'amount': 0}, [{'column': 'amount', 'equals': 0}])


def test_decimal_regex_condition_is_rejected(monkeypatch):
    with pytest.raises(InvalidTransformationException, match='regex_match.*decimal field `amount`'):
        _run(monkeypatch, ('other', 'SET-NULL'), '123.45', [{'column': 'amount', 'regex_match': '^123'}])


@pytest.mark.parametrize('field_schema', [
    {'type': ['null', 'number']}, {'type': ['null', 'string'], 'format': 'singer.decimal'},
])
def test_unmarked_schemas_skip_per_record_decimal_validation(monkeypatch, field_schema):
    validate = Mock(wraps=transform_field.validate_decimal_record)
    has_decimals = Mock(wraps=transform_field.schema_has_decimals)
    monkeypatch.setattr(transform_field, 'validate_decimal_record', validate)
    monkeypatch.setattr(transform_field, 'schema_has_decimals', has_decimals)
    monkeypatch.setattr(singer, 'write_message', Mock())
    transformer = TransformField({'transformations': []})
    transformer.handle_line(json.dumps({'type': 'SCHEMA', 'stream': 'items', 'key_properties': [],
                                        'schema': {'properties': {'amount': field_schema}}}))
    for _ in range(3):
        transformer.handle_line(json.dumps({'type': 'RECORD', 'stream': 'items', 'record': {'amount': 1.25}}))
    transformer.flush()
    validate.assert_not_called()
    has_decimals.assert_called_once()


def test_replaced_schema_updates_batch_decimal_validation(monkeypatch):
    validate = Mock(wraps=transform_field.validate_decimal_record)
    monkeypatch.setattr(transform_field, 'validate_decimal_record', validate)
    monkeypatch.setattr(singer, 'write_message', Mock())
    transformer = TransformField({'transformations': []})
    for field_schema, amount in [
        ({'type': ['number']}, 1.25), (decimal_schema(10, 2), '1.25'), ({'type': ['number']}, 1.25),
    ]:
        transformer.handle_line(json.dumps({'type': 'SCHEMA', 'stream': 'items', 'key_properties': [],
                                            'schema': {'properties': {'amount': field_schema}}}))
        transformer.handle_line(json.dumps({'type': 'RECORD', 'stream': 'items', 'record': {'amount': amount}}))
    transformer.flush()
    validate.assert_called_once_with({'amount': '1.25'}, {'properties': {'amount': decimal_schema(10, 2)}})
