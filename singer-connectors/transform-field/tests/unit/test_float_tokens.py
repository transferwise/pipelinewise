"""Floating-point fields carrying nonfinite string tokens keep numeric transformation rules."""

import json
from unittest.mock import patch

import pytest
import singer
from singer import Catalog, Schema

from transform_field import TransformField
from transform_field.errors import InvalidTransformationException

FLOAT_TOKEN_TYPES = [['null', 'number', 'string'], ['number', 'string']]
STRING_ONLY_TRANSFORMATIONS = ['HASH', 'HASH-SKIP-FIRST-2', 'MASK-HIDDEN', 'MASK-STRING-SKIP-ENDS-2']


def _transformer(trans_type):
    return TransformField({'transformations': [{
        'tap_stream_name': 'items', 'field_id': 'amount', 'type': trans_type,
    }]})


def _schema_message(field_type):
    return {'type': 'SCHEMA', 'stream': 'items', 'key_properties': ['id'], 'schema': {'properties': {
        'id': {'type': ['integer']}, 'amount': {'type': field_type},
    }}}


def _validate_catalog(transformer, field_type):
    schemas = {'items': Schema.from_dict(_schema_message(field_type)['schema'])}
    with patch('transform_field.utils.get_stream_schemas', return_value=schemas):
        transformer.validate(Catalog.from_dict({'streams': []}))


@pytest.mark.parametrize('field_type', FLOAT_TOKEN_TYPES)
@pytest.mark.parametrize('trans_type', STRING_ONLY_TRANSFORMATIONS)
def test_string_only_transformations_reject_float_token_types(trans_type, field_type):
    with pytest.raises(InvalidTransformationException, match='non-string field `amount`'):
        _validate_catalog(_transformer(trans_type), field_type)
    with pytest.raises(InvalidTransformationException, match='non-string field `amount`'):
        _transformer(trans_type).handle_line(json.dumps(_schema_message(field_type)))


@pytest.mark.parametrize('trans_type', STRING_ONLY_TRANSFORMATIONS)
def test_string_only_transformations_still_accept_plain_strings(trans_type):
    _validate_catalog(_transformer(trans_type), ['null', 'string'])


@pytest.mark.parametrize('field_type', FLOAT_TOKEN_TYPES)
def test_mask_number_accepts_float_token_types(field_type):
    _validate_catalog(_transformer('MASK-NUMBER'), field_type)


@pytest.mark.parametrize(('trans_type', 'expected'), [('SET-NULL', None), ('MASK-NUMBER', 0)])
def test_numeric_transformations_replace_nonfinite_tokens(monkeypatch, trans_type, expected):
    output = []
    monkeypatch.setattr(singer, 'write_message', output.append)
    values = ['NaN', 'Infinity', '-Infinity', 1.5]
    messages = [
        _schema_message(['null', 'number', 'string']),
        *({'type': 'RECORD', 'stream': 'items', 'record': {'id': index, 'amount': value}}
          for index, value in enumerate(values)),
        {'type': 'STATE', 'value': {'position': 1}},
    ]

    _transformer(trans_type).consume(json.dumps(message) for message in messages)

    assert [message.record for message in output[1:-1]] == [
        {'id': index, 'amount': expected} for index in range(len(values))
    ]
    assert output[-1].value == {'position': 1}


def _conditional_secrets(monkeypatch, field_type, condition, values):
    output = []
    monkeypatch.setattr(singer, 'write_message', output.append)
    transformer = TransformField({'transformations': [{
        'tap_stream_name': 'items', 'field_id': 'secret', 'type': 'SET-NULL', 'when': [condition],
    }]})
    messages = [
        {'type': 'SCHEMA', 'stream': 'items', 'key_properties': ['id'], 'schema': {'properties': {
            'id': {'type': ['integer']}, 'amount': {'type': field_type}, 'secret': {'type': ['null', 'string']},
        }}},
        *({'type': 'RECORD', 'stream': 'items', 'record': {'id': index, 'amount': value, 'secret': 'private'}}
          for index, value in enumerate(values)),
    ]
    transformer.consume(json.dumps(message) for message in messages)
    return [message.record['secret'] for message in output[1:]]


@pytest.mark.parametrize('field_type', [*FLOAT_TOKEN_TYPES, ['null', 'number']])
@pytest.mark.parametrize('condition', [
    {'column': 'amount', 'regex_match': '^-'},
    {'column': 'amount', 'regex_match': 'N'},
    {'column': 'amount', 'equals': 'NaN'},
    {'column': 'amount', 'equals': '-Infinity'},
])
def test_text_conditions_never_match_float_fields(monkeypatch, field_type, condition):
    values = ['NaN', 'Infinity', '-Infinity', -1.5, None]
    assert _conditional_secrets(monkeypatch, field_type, condition, values) == ['private'] * len(values)


@pytest.mark.parametrize('field_type', FLOAT_TOKEN_TYPES)
def test_numeric_equals_condition_matches_finite_floats_only(monkeypatch, field_type):
    values = ['NaN', 1.5, '-Infinity', 2.5]
    condition = {'column': 'amount', 'equals': 1.5}
    assert _conditional_secrets(monkeypatch, field_type, condition, values) == ['private', None, 'private', 'private']


@pytest.mark.parametrize('condition', [{'column': 'amount', 'regex_match': 'N'}, {'column': 'amount', 'equals': 'NaN'}])
def test_text_conditions_still_match_string_fields(monkeypatch, condition):
    assert _conditional_secrets(monkeypatch, ['null', 'string'], condition, ['NaN', 'other']) == [None, 'private']
