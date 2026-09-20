"""Regression coverage for row identity and durable Singer checkpoints."""

import copy
import io
import json
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

import pytest

import target_snowflake
from target_snowflake.db_sync import DbSync


def schema_message(stream, keys):
    """Build a schema with string keys and a payload."""
    return {
        'type': 'SCHEMA', 'stream': stream, 'key_properties': keys,
        'schema': {'properties': {name: {'type': ['string']} for name in [*keys, 'payload']}},
    }


def mock_db_sync(config, schema, *args):
    """Mock Snowflake access while using the actual buffered-row identity."""
    db = Mock(record_update_mode=None, data_flattening_max_level=0)
    db.stream_schema_message = schema
    db.flatten_schema = schema['schema']['properties']
    db.record_primary_key_string.side_effect = lambda record: DbSync.record_primary_key_string(db, record)
    return db


@pytest.mark.parametrize('first,second', [
    (('a,b', 'c'), ('a', 'b,c')),
    (('', ','), (',', '')),
    (('a"', 'b\\c'), ('a', '"b\\c')),
])
def test_composite_keys_preserve_distinct_rows_in_one_batch(first, second):
    messages = [schema_message('db-table', ['key1', 'key2'])]
    for keys, payload in [(first, 'first'), (second, 'second'), (first, 'updated')]:
        messages.append({
            'type': 'RECORD', 'stream': 'db-table',
            'record': {'key1': keys[0], 'key2': keys[1], 'payload': payload},
        })
    with patch('target_snowflake.DbSync', side_effect=mock_db_sync), \
            patch('target_snowflake.flush_records') as load:
        target_snowflake.persist_lines({'parallelism': 1}, map(json.dumps, messages))

    records = list(load.call_args.args[1].values())
    assert len(records) == 2
    assert {(record['key1'], record['key2'], record['payload']) for record in records} == {
        (*first, 'updated'), (*second, 'second'),
    }


def test_empty_string_primary_key_coalesces_repeated_updates():
    messages = [schema_message('db-table', ['id'])]
    for payload in ['first', 'updated']:
        messages.append({'type': 'RECORD', 'stream': 'db-table', 'record': {'id': '', 'payload': payload}})
    with patch('target_snowflake.DbSync', side_effect=mock_db_sync), \
            patch('target_snowflake.flush_records') as load:
        target_snowflake.persist_lines({'parallelism': 1}, map(json.dumps, messages))

    records = list(load.call_args.args[1].values())
    assert len(records) == 1
    assert records[0]['payload'] == 'updated'


@pytest.mark.parametrize('initial_state', [None, {'bookmarks': {'db-slow': {'log_pos': 4}, 'db-fast': {'log_pos': 4}}}])
def test_filtered_flush_never_acknowledges_an_unloaded_stream(initial_state):
    messages = [schema_message(stream, ['id']) for stream in ['db-slow', 'db-fast']]
    if initial_state is not None:
        messages.append({'type': 'STATE', 'value': initial_state})
    messages.extend([
        {'type': 'RECORD', 'stream': 'db-slow', 'record': {'id': '1'}},
        {'type': 'RECORD', 'stream': 'db-fast', 'record': {'id': '1'}},
        {'type': 'STATE', 'value': {'bookmarks': {'db-slow': {'log_pos': 10}, 'db-fast': {'log_pos': 10}}}},
        {'type': 'RECORD', 'stream': 'db-fast', 'record': {'id': '2'}},
    ])

    def interrupted_input():
        yield from map(json.dumps, messages)
        raise RuntimeError('source interrupted')

    output = io.StringIO()
    with patch('target_snowflake.DbSync', side_effect=mock_db_sync), \
            patch('target_snowflake.flush_records') as load, redirect_stdout(output), \
            pytest.raises(RuntimeError, match='source interrupted'):
        target_snowflake.persist_lines(
            {'parallelism': 1, 'batch_size_rows': 2, 'flush_all_streams': False}, interrupted_input(),
        )

    assert [entry.args[0] for entry in load.call_args_list] == ['db-fast']
    emitted = [json.loads(line) for line in output.getvalue().splitlines()]
    if initial_state is None:
        assert emitted == []
    else:
        expected = copy.deepcopy(initial_state)
        expected['bookmarks']['db-fast']['log_pos'] = 10
        assert emitted == [expected]


def test_filtered_flush_acknowledges_latest_state_after_all_streams_are_durable():
    state = {'bookmarks': {'db-table': {'log_pos': 10}}, 'currently_syncing': None}
    messages = [
        schema_message('db-table', ['id']),
        {'type': 'RECORD', 'stream': 'db-table', 'record': {'id': '1'}},
        {'type': 'STATE', 'value': state},
        {'type': 'RECORD', 'stream': 'db-table', 'record': {'id': '2'}},
    ]
    output = io.StringIO()
    with patch('target_snowflake.DbSync', side_effect=mock_db_sync), \
            patch('target_snowflake.flush_records'), redirect_stdout(output):
        target_snowflake.persist_lines(
            {'parallelism': 1, 'batch_size_rows': 2, 'flush_all_streams': False}, map(json.dumps, messages),
        )

    assert [json.loads(line) for line in output.getvalue().splitlines()] == [state, state]


def test_first_durable_checkpoint_uses_latest_state_not_first_buffered_state():
    def state(position):
        return {'bookmarks': {stream: {'log_pos': position} for stream in ['db-fast', 'db-slow']}}

    def record(stream, key):
        return {'type': 'RECORD', 'stream': stream, 'record': {'id': key}}

    messages = [schema_message(stream, ['id']) for stream in ['db-fast', 'db-slow']]
    messages.extend([
        record('db-fast', '1'),
        {'type': 'STATE', 'value': state(4)},
        record('db-fast', '2'),
        {'type': 'STATE', 'value': state(10)},
        record('db-fast', '3'),
        record('db-slow', '1'),
        record('db-fast', '4'),
        record('db-fast', '5'),
        {'type': 'STATE', 'value': state(20)},
        record('db-fast', '6'),
    ])

    def interrupted_input():
        yield from map(json.dumps, messages)
        raise RuntimeError('source interrupted')

    output = io.StringIO()
    with patch('target_snowflake.DbSync', side_effect=mock_db_sync), \
            patch('target_snowflake.flush_records') as load, redirect_stdout(output), \
            pytest.raises(RuntimeError, match='source interrupted'):
        target_snowflake.persist_lines(
            {'parallelism': 1, 'batch_size_rows': 3, 'flush_all_streams': False}, interrupted_input(),
        )

    # The first flush drains all buffers; the second must retain the slow stream's safe baseline.
    expected = state(10)
    expected['bookmarks']['db-fast']['log_pos'] = 20
    assert [entry.args[0] for entry in load.call_args_list] == ['db-fast', 'db-fast']
    assert [json.loads(line) for line in output.getvalue().splitlines()] == [state(10), expected]


@pytest.mark.parametrize('failure_stage', ['load', 'delete'])
def test_parallel_flush_failure_never_acknowledges_pending_state(failure_stage):
    state = {'bookmarks': {'db-success': {'log_pos': 10}, 'db-failing': {'log_pos': 10}}}
    messages = [schema_message(stream, ['id']) for stream in ['db-success', 'db-failing']]
    messages.extend([
        {'type': 'RECORD', 'stream': 'db-success', 'record': {'id': '1'}},
        {'type': 'RECORD', 'stream': 'db-failing', 'record': {'id': '1'}},
        {'type': 'STATE', 'value': state},
    ])

    def create_db(config, schema, *args):
        db = mock_db_sync(config, schema, *args)
        if schema['stream'] == 'db-failing' and failure_stage == 'delete':
            db.delete_rows.side_effect = RuntimeError('batch failed')
        return db

    def load(stream, *args):
        if stream == 'db-failing' and failure_stage == 'load':
            raise RuntimeError('batch failed')

    output = io.StringIO()
    with patch('target_snowflake.DbSync', side_effect=create_db), \
            patch('target_snowflake.flush_records', side_effect=load), redirect_stdout(output), \
            pytest.raises(RuntimeError, match='batch failed'):
        target_snowflake.persist_lines({'parallelism': 2, 'flush_all_streams': True}, map(json.dumps, messages))

    assert output.getvalue() == ''
