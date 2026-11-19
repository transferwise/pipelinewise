"""Retained MariaDB JSON TEXT rules keep bytes and declare their target requirement."""

import copy
import hashlib
import json

import pytest
import singer
from singer import Catalog, Schema

from transform_field import TransformField, transform
from transform_field.errors import InvalidTransformationException

JSON_SCHEMA = {'type': ['null', 'object', 'array', 'string', 'number', 'boolean'], 'format': 'mariadb-json'}
ROOTS = ['  { "private" : "初", "amount": 1.5000 }  ', '[ 1, null ]', '"escaped\\ntext"',
         '42', '1.5000', 'true', 'false', 'null', None]
STRING_RULES = ['HASH', 'MASK-HIDDEN', 'HASH-SKIP-FIRST-2', 'MASK-STRING-SKIP-ENDS-2']


def _rule(field_id='payload', trans_type='HASH', **kwargs):
    return {'tap_stream_name': 'items', 'field_id': field_id, 'type': trans_type, **kwargs}


def _schema():
    return {'type': 'object', 'properties': {
        'id': {'type': ['integer']}, 'payload': copy.deepcopy(JSON_SCHEMA),
        'secret': {'type': ['null', 'string']},
    }}


def _schema_message(schema=None):
    return {'type': 'SCHEMA', 'stream': 'items', 'key_properties': ['id'], 'schema': schema or _schema()}


def _catalog(schema):
    return Catalog.from_dict({'streams': [{
        'stream': 'items', 'tap_stream_id': 'items', 'schema': schema,
        'metadata': [{'breadcrumb': [], 'metadata': {'selected': True}}],
    }]})


def _run(monkeypatch, rules, records, schema=None):
    output = []
    monkeypatch.setattr(singer, 'write_message', output.append)
    transformer = TransformField({'transformations': rules})
    messages = [
        _schema_message(schema),
        *({'type': 'RECORD', 'stream': 'items', 'record': record} for record in records),
        {'type': 'STATE', 'value': {'items': {'position': 9}}},
    ]
    transformer.consume(json.dumps(message) for message in messages)
    return transformer, output


def _expected(value, trans_type):
    if trans_type == 'MASK-HIDDEN':
        return 'hidden'
    if value is None:
        return None
    if trans_type == 'HASH':
        return hashlib.sha256(value.encode('utf-8')).hexdigest()
    if trans_type == 'HASH-SKIP-FIRST-2':
        return value[:2] + hashlib.sha256(value.encode('utf-8')[2:]).hexdigest()
    return '*' * len(value) if len(value) <= 4 else value[:2] + '*' * (len(value) - 4) + value[-2:]


@pytest.mark.parametrize('trans_type', STRING_RULES)
def test_import_accepts_legacy_string_rules_without_mutating_catalog(trans_type):
    catalog = _catalog(_schema())
    before = catalog.to_dict()
    TransformField({'transformations': [_rule(trans_type=trans_type)]}).validate(catalog)
    assert catalog.to_dict() == before


@pytest.mark.parametrize('trans_type', STRING_RULES)
def test_runtime_marks_text_requirement_and_transforms_all_serialized_roots(monkeypatch, trans_type):
    transformer, output = _run(monkeypatch, [_rule(trans_type=trans_type)], [
        {'id': index, 'payload': value} for index, value in enumerate(ROOTS)
    ])
    output_property = output[0].schema['properties']['payload']
    assert output_property == {**JSON_SCHEMA, 'format': 'mariadb-json-text'}
    assert Schema.from_dict(output[0].schema).to_dict()['properties']['payload'] == output_property
    assert transformer.stream_meta['items'].schema == _schema()
    assert [message.record['payload'] for message in output[1:-1]] == [
        _expected(value, trans_type) for value in ROOTS
    ]
    assert output[-1].value == {'items': {'position': 9}}


@pytest.mark.parametrize('trans_type', STRING_RULES)
def test_conditional_rules_retain_nonmatching_bytes_and_omit_absent_columns(monkeypatch, trans_type):
    raw = ROOTS[0]
    records = [{'id': 1, 'payload': raw}, {'id': 2, 'payload': raw}, {'id': 1}]
    _, output = _run(monkeypatch, [_rule(trans_type=trans_type, when=[{'column': 'id', 'equals': 1}])], records)
    assert output[0].schema['properties']['payload']['format'] == 'mariadb-json-text'
    assert [message.record for message in output[1:-1]] == [
        {'id': 1, 'payload': _expected(raw, trans_type)}, {'id': 2, 'payload': raw}, {'id': 1},
    ]


@pytest.mark.parametrize('condition', [
    {'column': 'payload', 'regex_match': 'private'}, {'column': 'payload', 'equals': ROOTS[0]},
    {'column': 'payload', 'equals': None},
])
def test_conditions_mark_only_fields_needing_text_and_preserve_null_semantics(monkeypatch, condition):
    rule = _rule('secret', 'SET-NULL', when=[condition])
    catalog = _catalog(_schema())
    before = catalog.to_dict()
    TransformField({'transformations': [rule]}).validate(catalog)
    assert catalog.to_dict() == before
    _, output = _run(monkeypatch, [rule], [
        {'id': 1, 'payload': ROOTS[0], 'secret': 'private'},
        {'id': 2, 'payload': 'null', 'secret': 'private'},
        {'id': 3, 'payload': None, 'secret': 'private'}, {'id': 4, 'secret': 'private'},
    ])
    null_condition = condition.get('equals', 'not-null') is None
    expected_format = 'mariadb-json' if null_condition else 'mariadb-json-text'
    assert output[0].schema['properties']['payload']['format'] == expected_format
    assert [message.record['secret'] for message in output[1:-1]] == (
        ['private', 'private', None, 'private'] if null_condition else [None, 'private', 'private', 'private']
    )


@pytest.mark.parametrize('trans_type', ['HASH', 'HASH-SKIP-FIRST-2', 'MASK-STRING-SKIP-ENDS-2', 'MASK-DATE'])
def test_nullable_plain_target_with_mariadb_json_condition_keeps_null(monkeypatch, trans_type):
    schema = _schema()
    field = 'secret'
    if trans_type == 'MASK-DATE':
        field = 'date_created'
        schema['properties'][field] = {'type': ['null', 'string'], 'format': 'date'}
    _, output = _run(monkeypatch, [_rule(field, trans_type, when=[{'column': 'payload', 'equals': None}])], [
        {'id': 1, 'payload': None, field: None},
    ], schema=schema)
    assert output[1].record[field] is None
    assert output[-1].value == {'items': {'position': 9}}


def test_nullable_nested_target_with_mariadb_json_condition_keeps_null(monkeypatch):
    schema = _schema()
    schema['properties']['details'] = {'type': ['null', 'object']}
    _, output = _run(monkeypatch, [_rule('details', 'HASH', field_paths=['secret'], when=[
        {'column': 'payload', 'equals': None},
    ])], [
        {'id': 1, 'payload': None, 'details': {'secret': None}},
        {'id': 2, 'payload': None, 'details': {'secret': 'private'}},
    ], schema=schema)
    assert [message.record['details']['secret'] for message in output[1:-1]] == [
        None, hashlib.sha256(b'private').hexdigest(),
    ]
    assert output[-1].value == {'items': {'position': 9}}


def test_nullable_plain_regex_condition_with_mariadb_json_target_is_a_nonmatch(monkeypatch):
    raw = ROOTS[0]
    _, output = _run(monkeypatch, [_rule('payload', 'SET-NULL', when=[
        {'column': 'secret', 'regex_match': 'private'},
    ])], [
        {'id': 1, 'payload': raw, 'secret': None},
        {'id': 2, 'payload': raw, 'secret': 'private'},
    ])
    assert [message.record['payload'] for message in output[1:-1]] == [raw, None]
    assert output[-1].value == {'items': {'position': 9}}


def test_invalid_regex_still_fails_closed_for_nullable_plain_condition(monkeypatch):
    with pytest.raises(InvalidTransformationException):
        _run(monkeypatch, [_rule('payload', 'SET-NULL', when=[
            {'column': 'secret', 'regex_match': '[private'},
        ])], [{'id': 1, 'payload': ROOTS[0], 'secret': None}])


def test_top_level_set_null_keeps_variant_contract(monkeypatch):
    _, output = _run(monkeypatch, [_rule(trans_type='SET-NULL')], [{'id': 1, 'payload': ROOTS[0]}])
    assert output[0].schema == _schema()
    assert output[1].record['payload'] is None


@pytest.mark.parametrize('source', ['catalog', 'schema'])
@pytest.mark.parametrize('rule', [
    _rule(trans_type='MASK-NUMBER'), _rule(trans_type='MASK-DATE'),
    _rule(field_paths=[]), _rule(field_paths=['private']),
    _rule(trans_type='SET-NULL', field_paths=['private']),
    _rule('secret', 'SET-NULL', when=[{'column': 'payload', 'field_path': 'private', 'equals': 'private'}]),
])
def test_unsupported_rules_fail_before_schema_or_record_emission(monkeypatch, rule, source):
    output = []
    monkeypatch.setattr(singer, 'write_message', output.append)
    transformer = TransformField({'transformations': [rule]})
    with pytest.raises(InvalidTransformationException, match='MariaDB JSON field `payload`'):
        if source == 'catalog':
            transformer.validate(_catalog(_schema()))
        else:
            transformer.handle_line(json.dumps(_schema_message()))
    assert output == []


@pytest.mark.parametrize('trans_type', STRING_RULES)
@pytest.mark.parametrize('value', [{'private': 'secret'}, ['secret'], 42, False])
def test_unexpected_transport_fails_without_records_state_or_private_error(monkeypatch, value, trans_type):
    output = []
    monkeypatch.setattr(singer, 'write_message', output.append)
    transformer = TransformField({'transformations': [_rule(trans_type=trans_type)]})
    messages = [_schema_message(), {'type': 'RECORD', 'stream': 'items', 'record': {'payload': value}},
                {'type': 'STATE', 'value': {'position': 9}}]
    with pytest.raises(InvalidTransformationException) as error:
        transformer.consume(json.dumps(message) for message in messages)
    assert 'secret' not in str(error.value)
    assert [message.asdict()['type'] for message in output] == ['SCHEMA']


@pytest.mark.parametrize('when', [
    [{'column': 'payload', 'regex_match': '[private'}], [{'column': 'id', 'regex_match': '[private'}],
])
def test_condition_errors_cannot_return_original_private_value(monkeypatch, when):
    output = []
    monkeypatch.setattr(singer, 'write_message', output.append)
    transformer = TransformField({'transformations': [_rule(trans_type='HASH', when=when)]})
    with pytest.raises(InvalidTransformationException) as error:
        transformer.consume(json.dumps(message) for message in [
            _schema_message(), {'type': 'RECORD', 'stream': 'items', 'record': {'id': 1, 'payload': ROOTS[0]}},
            {'type': 'STATE', 'value': {'position': 9}},
        ])
    assert 'private' not in str(error.value)
    assert [message.asdict()['type'] for message in output] == ['SCHEMA']


def test_condition_transport_failure_is_closed_even_when_transformed_field_is_plain_string(monkeypatch):
    with pytest.raises(InvalidTransformationException):
        _run(monkeypatch, [_rule('secret', 'HASH', when=[{'column': 'payload', 'regex_match': 'private'}])], [
            {'id': 1, 'payload': {'private': 'raw'}, 'secret': 'private'},
        ])


def test_plain_string_failure_and_ordinary_json_validation_remain_unchanged():
    assert transform.do_transform({'secret': 42}, 'secret', 'HASH') == 42
    schema = _schema()
    schema['properties']['payload'] = {'type': ['null', 'object']}
    with pytest.raises(InvalidTransformationException):
        TransformField({'transformations': [_rule()]}).validate(_catalog(schema))
