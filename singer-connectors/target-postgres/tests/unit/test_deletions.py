"""Delete-event processing and acknowledgement guarantees."""

import json
from unittest.mock import MagicMock, call, patch

import pytest

import target_postgres
from target_postgres.db_sync import DbSync, validate_config


@pytest.mark.parametrize('metadata_config', [{}, {'add_metadata_columns': False}, {'add_metadata_columns': True}])
def test_delete_is_processed_before_state(metadata_config):
    schema = {
        'type': 'SCHEMA',
        'stream': 'public-items',
        'schema': {'type': 'object', 'properties': {'id': {'type': ['integer']}}},
        'key_properties': ['id'],
    }
    deleted_at = '2026-09-17T10:00:00Z'
    record = {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, '_sdc_deleted_at': deleted_at}}
    state = {'bookmarks': {'public-items': {'lsn': 123}}}
    lines = [json.dumps(message) for message in [schema, record, {'type': 'STATE', 'value': state}]]
    operations = MagicMock()

    with patch('target_postgres.DbSync') as db_sync, \
            patch('target_postgres.flush_records') as load, \
            patch('target_postgres.emit_state') as emit:
        instance = db_sync.return_value
        instance.record_primary_key_string.return_value = '1'
        operations.attach_mock(load, 'load')
        operations.attach_mock(instance.create_indices, 'index')
        operations.attach_mock(instance.delete_rows, 'delete')
        operations.attach_mock(emit, 'state')

        target_postgres.persist_lines({'parallelism': 1, **metadata_config}, lines)

    generated_schema = db_sync.call_args.args[1]['schema']['properties']
    assert '_sdc_deleted_at' in generated_schema
    loaded_record = load.call_args.args[1]['1']
    assert loaded_record['_sdc_deleted_at'] == deleted_at
    assert '_sdc_batched_at' in loaded_record
    assert [operation[0] for operation in operations.mock_calls] == ['load', 'index', 'delete', 'state']
    assert operations.mock_calls[-1] == call.state(state)


def test_delete_failure_does_not_acknowledge_state_or_clear_batch():
    stream = 'public-items'
    records = {stream: {'1': {'id': 1, '_sdc_deleted_at': '2026-09-17T10:00:00Z'}}}
    counts = {stream: 1}
    sync = MagicMock()
    sync.delete_rows.side_effect = RuntimeError('delete failed')
    state = {'bookmarks': {stream: {'lsn': 123}}}
    previous_state = {'bookmarks': {stream: {'lsn': 100}}}

    with patch('target_postgres.flush_records'), patch('target_postgres.emit_state') as emit:
        with pytest.raises(RuntimeError, match='delete failed'):
            target_postgres.flush_streams(
                records, counts, {stream: sync}, {'parallelism': 1}, state, previous_state,
            )

    assert counts[stream] == 1
    assert records[stream]['1']['id'] == 1
    assert previous_state['bookmarks'][stream]['lsn'] == 100
    emit.assert_not_called()


def test_delete_failure_in_persist_lines_does_not_emit_state():
    messages = [
        {'type': 'SCHEMA', 'stream': 'public-items', 'key_properties': ['id'],
         'schema': {'type': 'object', 'properties': {'id': {'type': ['integer']}}}},
        {'type': 'RECORD', 'stream': 'public-items', 'record': {'id': 1, '_sdc_deleted_at': 'deleted'}},
        {'type': 'STATE', 'value': {'bookmarks': {'public-items': {'lsn': 123}}}},
    ]
    with patch('target_postgres.DbSync') as db_sync, \
            patch('target_postgres.flush_records'), patch('target_postgres.emit_state') as emit:
        db_sync.return_value.delete_rows.side_effect = RuntimeError('delete failed')
        with pytest.raises(RuntimeError, match='delete failed'):
            target_postgres.persist_lines({'parallelism': 1}, [json.dumps(message) for message in messages])
    emit.assert_not_called()


@pytest.fixture
def deletion_sync():
    config = {
        'host': 'localhost', 'port': 5432, 'user': 'user', 'password': 'test-only',
        'dbname': 'test', 'default_target_schema': 'test',
    }
    schema = {
        'stream': 'public-items', 'key_properties': ['id'],
        'schema': {'type': 'object', 'properties': {
            'id': {'type': ['integer']}, '_sdc_deleted_at': {'type': ['null', 'string']},
        }},
    }
    assert validate_config(config) == []
    return DbSync(config, schema)


def test_deletion_index_is_always_enabled(deletion_sync):
    assert deletion_sync.indices == ['_sdc_deleted_at']


@pytest.mark.parametrize('deleted_rows', [0, 10_000_000])
def test_delete_counts_rows_without_fetching(deletion_sync, deleted_rows):
    with patch.object(deletion_sync, 'open_connection') as connect, \
            patch.object(deletion_sync, 'logger') as logger:
        connection = connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.rowcount = deleted_rows

        deletion_sync.delete_rows('public-items')

    cursor.execute.assert_called_once_with('DELETE FROM test."items" WHERE _sdc_deleted_at IS NOT NULL')
    cursor.fetchall.assert_not_called()
    cursor.fetchmany.assert_not_called()
    cursor.fetchone.assert_not_called()
    connection.cursor.return_value.__exit__.assert_called_once_with(None, None, None)
    connect.return_value.__exit__.assert_called_once_with(None, None, None)
    assert logger.info.call_args == call('DELETE %s', deleted_rows)


@pytest.mark.parametrize('failure_stage', ['execute', 'commit'])
def test_delete_failure_propagates_without_logging_success(deletion_sync, failure_stage):
    error = RuntimeError(f'{failure_stage} failed')
    with patch.object(deletion_sync, 'open_connection') as connect, \
            patch.object(deletion_sync, 'logger') as logger:
        connection = connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.rowcount = 1
        if failure_stage == 'execute':
            cursor.execute.side_effect = error
        else:
            connect.return_value.__exit__.side_effect = error

        with pytest.raises(RuntimeError) as exc:
            deletion_sync.delete_rows('public-items')

    assert exc.value is error
    if failure_stage == 'execute':
        assert connect.return_value.__exit__.call_args.args[:2] == (RuntimeError, error)
        assert connection.cursor.return_value.__exit__.call_args.args[:2] == (RuntimeError, error)
    else:
        connect.return_value.__exit__.assert_called_once_with(None, None, None)
    cursor.fetchall.assert_not_called()
    assert not any(log.args[0] == 'DELETE %s' for log in logger.info.call_args_list)
