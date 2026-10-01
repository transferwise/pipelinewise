"""Regression coverage for durable Singer checkpoints with per-stream flushing."""

import copy
import io
import json
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

import pytest

import target_postgres
from target_postgres.db_sync import DbSync


def schema_message(stream):
    """Build a minimal keyed stream schema."""
    return {
        'type': 'SCHEMA',
        'stream': stream,
        'key_properties': ['id'],
        'schema': {
            'properties': {
                'id': {'type': ['string']},
                'payload': {'type': ['null', 'string']},
            },
        },
    }


def mock_db_sync(_config, schema):
    """Mock database access while retaining real buffered-row identity."""
    db = Mock(record_update_mode=None, data_flattening_max_level=0)
    db.stream_schema_message = schema
    db.flatten_schema = schema['schema']['properties']
    db.record_primary_key_string.side_effect = (
        lambda record: DbSync.record_primary_key_string(db, record)
    )
    return db


@pytest.mark.parametrize('initial_state', [
    None,
    {'bookmarks': {'db-slow': {'lsn': 4}, 'db-fast': {'lsn': 4}}},
])
def test_filtered_flush_never_acknowledges_an_unloaded_stream(initial_state):
    """A shared LSN cannot advance while another stream still has buffered rows."""
    messages = [schema_message(stream) for stream in ['db-slow', 'db-fast']]
    if initial_state is not None:
        messages.append({'type': 'STATE', 'value': initial_state})
    messages.extend([
        {'type': 'RECORD', 'stream': 'db-slow', 'record': {'id': '1'}},
        {'type': 'RECORD', 'stream': 'db-fast', 'record': {'id': '1'}},
        {
            'type': 'STATE',
            'value': {'bookmarks': {
                'db-slow': {'lsn': 10},
                'db-fast': {'lsn': 10},
            }},
        },
        {'type': 'RECORD', 'stream': 'db-fast', 'record': {'id': '2'}},
    ])

    def interrupted_input():
        yield from map(json.dumps, messages)
        raise RuntimeError('source interrupted')

    output = io.StringIO()
    with patch('target_postgres.DbSync', side_effect=mock_db_sync), \
            patch('target_postgres.flush_records') as load, redirect_stdout(output), \
            pytest.raises(RuntimeError, match='source interrupted'):
        target_postgres.persist_lines(
            {
                'parallelism': 1,
                'batch_size_rows': 2,
                'flush_all_streams': False,
            },
            interrupted_input(),
        )

    assert [entry.args[0] for entry in load.call_args_list] == ['db-fast']
    emitted = [json.loads(line) for line in output.getvalue().splitlines()]
    if initial_state is None:
        assert emitted == []
    else:
        expected = copy.deepcopy(initial_state)
        expected['bookmarks']['db-fast']['lsn'] = 10
        assert emitted == [expected]
