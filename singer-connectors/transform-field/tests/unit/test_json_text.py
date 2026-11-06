"""Serialized JSON text fields, such as PostgreSQL numeric[] on decimal routes, only allow null-safe rules."""

import json
from unittest.mock import patch

import pytest
import singer
from singer import Catalog, Schema

import transform_field
from transform_field import TransformField, transform
from transform_field.errors import InvalidTransformationException

DECIMAL_ARRAY_REF = {'$ref': '#/definitions/sdc_recursive_decimal_100_38_array'}
JSON_TEXT_SCHEMA = {'type': ['null', 'string', 'array'], 'format': 'singer.json', 'items': DECIMAL_ARRAY_REF}
ARRAY_SCHEMA = {'type': ['null', 'array'], 'items': DECIMAL_ARRAY_REF}
DEFINITIONS = {'sdc_recursive_decimal_100_38_array': {
    'type': ['null', 'number', 'array'], 'multipleOf': 1e-38, 'exclusiveMaximum': True, 'maximum': 10 ** 62,
    'exclusiveMinimum': True, 'minimum': -10 ** 62, 'items': DECIMAL_ARRAY_REF,
}}
JSON_TEXT = '[1,1.10,12345678901234567890.123,0.00000012,"NaN","Infinity","-Infinity",null]'
JSON_TEXT_VALUES = [JSON_TEXT, '[[1,2],[3,4]]', '[]', None]
UNSUPPORTED_CONDITIONS = [
    {'column': 'amounts', 'field_path': '0', 'equals': 1},
    {'column': 'amounts', 'field_path': '0', 'regex_match': '1'},
    {'column': 'amounts', 'regex_match': 'NaN'},
    {'column': 'amounts', 'equals': JSON_TEXT},
    {'column': 'amounts', 'equals': 0},
]
VALUE_TRANSFORMATIONS = [
    'HASH', 'HASH-SKIP-FIRST-2', 'MASK-HIDDEN', 'MASK-STRING-SKIP-ENDS-2', 'MASK-DATE', 'MASK-NUMBER',
]
CONDITION_ERROR = (
    'Conditions on JSON text field `amounts` in stream `items` cannot use '
    '`field_path`, `regex_match` or a non-null `equals`'
)
FIELD_PATHS_ERROR = 'Only `SET-NULL` can use `field_paths` on JSON text field `amounts` in stream `items`'


def _stream_schema(amounts_schema):
    return {'type': 'object', 'definitions': DEFINITIONS, 'properties': {
        'id': {'type': ['integer']}, 'amounts': amounts_schema, 'secret': {'type': ['null', 'string']},
    }}


def _schema_message(amounts_schema):
    return {'type': 'SCHEMA', 'stream': 'items', 'key_properties': ['id'], 'schema': _stream_schema(amounts_schema)}


def _transformer(field_id='secret', trans_type='SET-NULL', when=None, field_paths=None):
    transformation = {'tap_stream_name': 'items', 'field_id': field_id, 'type': trans_type, 'when': when}
    if field_paths is not None:
        transformation['field_paths'] = field_paths
    return TransformField({'transformations': [transformation]})


def _validate(transformer, amounts_schema, source):
    """Validate as `--validate` does with catalog Schema objects, or as a running pipeline does on SCHEMA."""
    if source == 'catalog':
        schemas = {'items': Schema.from_dict(_stream_schema(amounts_schema))}
        with patch('transform_field.utils.get_stream_schemas', return_value=schemas):
            transformer.validate(Catalog.from_dict({'streams': []}))
    else:
        with patch.object(singer, 'write_message'):
            transformer.handle_line(json.dumps(_schema_message(amounts_schema)))


def _run(monkeypatch, transformer, amounts_schema, records):
    output = []
    monkeypatch.setattr(singer, 'write_message', output.append)
    messages = [_schema_message(amounts_schema)]
    messages.extend({'type': 'RECORD', 'stream': 'items', 'record': record} for record in records)
    transformer.consume(json.dumps(message) for message in messages)
    return output


@pytest.mark.parametrize('source', ['catalog', 'schema_message'])
@pytest.mark.parametrize('condition', UNSUPPORTED_CONDITIONS)
def test_conditions_inspecting_json_text_are_rejected_naming_only_stream_and_field(condition, source):
    with pytest.raises(InvalidTransformationException) as error:
        _validate(_transformer(when=[{'column': 'id', 'equals': 1}, condition]), JSON_TEXT_SCHEMA, source)

    assert str(error.value) == CONDITION_ERROR


@pytest.mark.parametrize('source', ['catalog', 'schema_message'])
def test_null_equals_and_undeclared_condition_columns_still_validate(source):
    _validate(_transformer(when=[{'column': 'amounts', 'equals': None}]), JSON_TEXT_SCHEMA, source)
    _validate(_transformer(when=[{'column': 'undeclared', 'equals': 'x'}]), JSON_TEXT_SCHEMA, source)
    _validate(_transformer(when=[{'column': 'secret', 'regex_match': 'NaN'}]), JSON_TEXT_SCHEMA, source)


@pytest.mark.parametrize('source', ['catalog', 'schema_message'])
@pytest.mark.parametrize('field_paths', [['0'], []])
@pytest.mark.parametrize('trans_type', VALUE_TRANSFORMATIONS)
def test_field_paths_on_json_text_require_set_null(trans_type, field_paths, source):
    with pytest.raises(InvalidTransformationException) as error:
        _validate(_transformer('amounts', trans_type, field_paths=field_paths), JSON_TEXT_SCHEMA, source)

    assert str(error.value) == FIELD_PATHS_ERROR


@pytest.mark.parametrize('source', ['catalog', 'schema_message'])
@pytest.mark.parametrize('trans_type', VALUE_TRANSFORMATIONS)
def test_json_text_keeps_value_transformations_invalid_without_field_paths(trans_type, source):
    with pytest.raises(InvalidTransformationException, match='field `amounts` in stream `items`'):
        _validate(_transformer('amounts', trans_type), JSON_TEXT_SCHEMA, source)


@pytest.mark.parametrize('field_paths', [None, ['0']])
def test_set_null_replaces_the_whole_json_text(monkeypatch, field_paths):
    output = _run(monkeypatch, _transformer('amounts', field_paths=field_paths), JSON_TEXT_SCHEMA, [
        {'id': index, 'amounts': value, 'secret': 'private'} for index, value in enumerate(JSON_TEXT_VALUES)
    ])

    assert [message.record['amounts'] for message in output[1:]] == [None] * len(JSON_TEXT_VALUES)


@pytest.mark.parametrize(('when', 'expected_secrets'), [
    (None, [None, None, None, None]),
    ([{'column': 'id', 'equals': 1}], ['private', None, 'private', 'private']),
    ([{'column': 'amounts', 'equals': None}], ['private', 'private', 'private', None]),
])
def test_json_text_passes_unchanged_through_other_transformations_and_record_validation(
        monkeypatch, when, expected_secrets):
    monkeypatch.setattr(transform_field, 'VALIDATE_RECORDS', True)
    output = _run(monkeypatch, _transformer(when=when), JSON_TEXT_SCHEMA, [
        {'id': index, 'amounts': value, 'secret': 'private'} for index, value in enumerate(JSON_TEXT_VALUES)
    ])

    assert output[0].schema['properties']['amounts'] == JSON_TEXT_SCHEMA
    assert [message.record['amounts'] for message in output[1:]] == JSON_TEXT_VALUES
    assert [message.record['secret'] for message in output[1:]] == expected_secrets


@pytest.mark.parametrize('condition', UNSUPPORTED_CONDITIONS)
def test_runtime_never_meets_conditions_inspecting_json_text(condition):
    when = [{'column': 'id', 'equals': 1}, condition]
    schema = _stream_schema(JSON_TEXT_SCHEMA)
    record = {'id': 1, 'amounts': JSON_TEXT, 'secret': 'private'}

    assert not transform.is_transform_required(record, when, schema)
    assert transform.do_transform(record, 'secret', 'SET-NULL', when, schema=schema) == 'private'


@pytest.mark.parametrize('condition', [
    {'column': 'amounts', 'regex_match': 'NaN'}, {'column': 'amounts', 'equals': JSON_TEXT},
])
def test_the_same_conditions_still_match_plain_string_fields(condition):
    schema = _stream_schema({'type': ['null', 'string']})
    assert transform.is_transform_required({'id': 1, 'amounts': JSON_TEXT}, [condition], schema)


def test_runtime_null_equals_on_json_text_matches_only_null_like_unmarked_arrays():
    when = [{'column': 'id', 'equals': 1}, {'column': 'amounts', 'equals': None}]
    for record in ({'id': 1, 'amounts': None}, {'id': 1, 'amounts': '[]'}, {'id': 2, 'amounts': None}):
        assert transform.is_transform_required(record, when, _stream_schema(JSON_TEXT_SCHEMA)) == (
            transform.is_transform_required(record, when, _stream_schema(ARRAY_SCHEMA))
        ) == (record['id'] == 1 and record['amounts'] is None)


@pytest.mark.parametrize('source', ['catalog', 'schema_message'])
def test_unmarked_arrays_keep_existing_validation(source):
    _validate(_transformer('amounts', 'HASH', field_paths=['0']), ARRAY_SCHEMA, source)
    for condition in UNSUPPORTED_CONDITIONS:
        _validate(_transformer(when=[condition]), ARRAY_SCHEMA, source)


def test_unmarked_array_conditions_keep_existing_runtime_behaviour():
    schema = _stream_schema(ARRAY_SCHEMA)
    record = {'id': 1, 'amounts': [1, 1.1], 'secret': 'private'}

    assert transform.is_transform_required(record, [{'column': 'amounts', 'equals': [1, 1.1]}], schema)
    assert transform.is_transform_required(record, [{'column': 'amounts', 'field_path': '1', 'equals': 1.1}], schema)
    assert transform.do_transform(record, 'secret', 'SET-NULL', [{'column': 'amounts', 'regex_match': '1'}],
                                  schema=schema) == 'private'


@pytest.mark.parametrize(('schema', 'expected'), [
    (JSON_TEXT_SCHEMA, True), (Schema.from_dict(JSON_TEXT_SCHEMA), True), (ARRAY_SCHEMA, False),
    ({'type': ['null', 'string'], 'format': 'singer.decimal'}, False), (None, False), ('singer.json', False),
])
def test_json_text_schema_detection_tolerates_catalog_objects_and_non_dictionaries(schema, expected):
    assert transform.is_json_text_schema(schema) is expected
