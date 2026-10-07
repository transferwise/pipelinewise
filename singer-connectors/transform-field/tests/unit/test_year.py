"""MySQL YEAR values retain integer transport through numeric masking."""

import json

import pytest
import singer

from transform_field import TransformField
from transform_field.errors import InvalidTransformationException


@pytest.mark.parametrize('year', [2024, 0, None])
def test_mask_number_accepts_year_schema_and_keeps_integer_transport(monkeypatch, year):
    output = []
    monkeypatch.setattr(singer, 'write_message', output.append)
    transformer = TransformField({'transformations': [{
        'tap_stream_name': 'items', 'field_id': 'year', 'type': 'MASK-NUMBER',
    }]})
    schema = {'properties': {'year': {
        'type': ['null', 'integer'], 'format': 'singer.year', 'minimum': 0, 'maximum': 2155,
    }}}
    messages = [
        {'type': 'SCHEMA', 'stream': 'items', 'key_properties': [], 'schema': schema},
        {'type': 'RECORD', 'stream': 'items', 'record': {'year': year}},
        {'type': 'STATE', 'value': {'position': 1}},
    ]

    transformer.consume(json.dumps(message) for message in messages)

    assert output[0].schema == schema
    assert output[1].record == {'year': 0}
    assert output[2].value == {'position': 1}


@pytest.mark.parametrize('field_type', ['string', 'number'])
def test_year_marker_does_not_allow_masking_other_formatted_types(field_type):
    transformer = TransformField({'transformations': [{
        'tap_stream_name': 'items', 'field_id': 'year', 'type': 'MASK-NUMBER',
    }]})
    message = {'type': 'SCHEMA', 'stream': 'items', 'key_properties': [], 'schema': {'properties': {
        'year': {'type': ['null', field_type], 'format': 'singer.year'},
    }}}

    with pytest.raises(InvalidTransformationException, match='non-numeric field'):
        transformer.handle_line(json.dumps(message))
