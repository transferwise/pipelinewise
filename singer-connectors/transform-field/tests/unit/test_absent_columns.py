"""Records can omit condition columns, as LOG_BASED PATCH updates and deletes do; no equality matches them."""

import json

import pytest
import singer
from singer.decimal_support import decimal_schema

from transform_field import TransformField, transform

STRING_SCHEMA = {'type': ['null', 'string']}
NOTES_SCHEMAS = {
    'string': STRING_SCHEMA,
    'decimal': decimal_schema(38, 18),
    'float_tokens': {'type': ['null', 'number', 'string']},
    'json_text': {'type': ['null', 'string', 'array'], 'format': 'singer.json'},
    'mariadb_json': {'type': ['null', 'object', 'array', 'string', 'number', 'boolean'], 'format': 'mariadb-json'},
}


def _schema(notes_schema=None):
    """Return a stream schema as tap-postgres and tap-yugabyte announce sparse LOG_BASED records."""
    properties = {'id': {'type': ['integer']}, 'kind': STRING_SCHEMA, 'secret': STRING_SCHEMA}
    if notes_schema is not None:
        properties['notes'] = notes_schema
    return {'type': 'object', 'properties': properties, 'x-pipelinewise-record-update-mode': 'PATCH'}


SCHEMAS = {**{name: _schema(notes) for name, notes in NOTES_SCHEMAS.items()}, 'undeclared': _schema(), 'none': None}
SPARSE_RECORD = {'id': 1, 'kind': 'card', 'secret': 'private'}


@pytest.mark.parametrize('schema', SCHEMAS.values(), ids=SCHEMAS.keys())
@pytest.mark.parametrize('value', ['', None, 0, False, 'card'])
def test_equals_is_not_met_when_the_record_omits_the_column(value, schema):
    assert transform.is_transform_required(SPARSE_RECORD, [{'column': 'notes', 'equals': value}], schema) is False


@pytest.mark.parametrize('condition', [
    {'column': 'notes', 'field_path': 'text', 'equals': ''},
    {'column': 'notes', 'field_path': '/', 'equals': ''},
    {'column': 'notes', 'equals': '', 'regex_match': '^$'},
])
def test_field_path_and_mixed_equality_conditions_are_not_met_for_an_omitted_column(condition):
    assert transform.is_transform_required(SPARSE_RECORD, [condition], _schema(STRING_SCHEMA)) is False


@pytest.mark.parametrize(('notes', 'value', 'expected'), [
    ('', '', True), (None, None, True), (None, '', False), ('', None, False),
])
def test_present_empty_strings_and_nulls_still_meet_only_their_own_equality(notes, value, expected):
    record = {**SPARSE_RECORD, 'notes': notes}
    assert transform.is_transform_required(record, [{'column': 'notes', 'equals': value}], _schema(STRING_SCHEMA)) \
        is expected


@pytest.mark.parametrize(('when', 'notes'), [
    ([{'column': 'id', 'equals': 1}, {'column': 'notes', 'equals': ''}], ''),
    ([{'column': 'notes', 'equals': ''}, {'column': 'id', 'equals': 1}], ''),
    ([{'column': 'kind', 'regex_match': 'card'}, {'column': 'notes', 'equals': None}], None),
    ([{'column': 'notes', 'equals': None}, {'column': 'kind', 'regex_match': 'card'}], None),
])
def test_an_omitted_equality_column_fails_a_rule_whose_other_conditions_match(when, notes):
    schema = _schema(STRING_SCHEMA)
    assert transform.is_transform_required(SPARSE_RECORD, when, schema) is False
    assert transform.is_transform_required({**SPARSE_RECORD, 'notes': notes}, when, schema) is True


@pytest.mark.parametrize('condition', [
    {'column': 'doc', 'field_path': 'notes', 'equals': ''},
    {'column': 'doc', 'field_path': 'notes', 'regex_match': '^$'},
])
def test_field_path_conditions_still_inspect_only_present_paths(condition):
    assert transform.is_transform_required({'doc': {'notes': ''}}, [condition])
    assert not transform.is_transform_required({'doc': {'notes': 'kept'}}, [condition])
    assert not transform.is_transform_required({'doc': {'other': ''}}, [condition])
    assert not transform.is_transform_required({'id': 1}, [condition])


@pytest.mark.parametrize(('pattern', 'expected'), [('^$', True), ('.*', True), ('card', False)])
def test_regex_conditions_still_search_an_omitted_column_as_an_empty_string(pattern, expected):
    """Kept on purpose: treating the column as unmatched would stop such existing rules masking sparse records."""
    when = [{'column': 'notes', 'regex_match': pattern}]
    assert transform.is_transform_required(SPARSE_RECORD, when, _schema(STRING_SCHEMA)) is expected


PATCH_RECORDS = [
    {'id': 1, 'notes': '', 'secret': 'private'},
    {'id': 2, 'notes': None, 'secret': 'private'},
    {'id': 3, 'notes': 'kept', 'secret': 'private'},
    # An update that changes only the secret omits the unchanged notes column.
    {'id': 4, 'secret': 'private'},
]


@pytest.mark.parametrize(('when', 'cleared_ids'), [
    ([{'column': 'notes', 'equals': ''}], {1}),
    ([{'column': 'notes', 'equals': None}], {2}),
    ([{'column': 'id', 'equals': 4}, {'column': 'notes', 'equals': ''}], set()),
])
def test_set_null_keeps_the_field_of_records_that_omit_the_equality_column(monkeypatch, when, cleared_ids):
    output = []
    monkeypatch.setattr(singer, 'write_message', output.append)
    transformer = TransformField({'transformations': [{
        'tap_stream_name': 'items', 'field_id': 'secret', 'type': 'SET-NULL', 'when': when,
    }]})
    messages = [
        {'type': 'SCHEMA', 'stream': 'items', 'key_properties': ['id'], 'schema': _schema(STRING_SCHEMA)},
        *({'type': 'RECORD', 'stream': 'items', 'record': record} for record in PATCH_RECORDS),
    ]

    transformer.consume(json.dumps(message) for message in messages)

    assert output[0].schema == _schema(STRING_SCHEMA)
    assert [message.record for message in output[1:]] == [
        {**record, 'secret': None if record['id'] in cleared_ids else 'private'} for record in PATCH_RECORDS
    ]
