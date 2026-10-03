"""Protect migration prerequisites and commit-safe backlog checkpoints."""

import copy
import json
from unittest.mock import MagicMock, patch

import pytest

from tap_postgres import db
from tap_postgres.sync_strategies import logical_replication as replication
from tests.unit.test_pgoutput_lifecycle import WalMessage, _ReplicationConnection, _config, _state, _stream


@pytest.mark.parametrize('permission', [None, (False,), (None,)])
def test_missing_boundary_permission_stops_before_publication_or_slot_work(permission):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = permission
    with patch.object(db, 'open_connection', return_value=connection), \
            patch.object(replication, '_validate_replication_slot_candidates') as slots, \
            pytest.raises(RuntimeError, match='requires EXECUTE.*pg_logical_emit_message'):
        replication.prepare_publication(_config(), [_stream()])
    slots.assert_not_called()
    assert cursor.execute.call_count == 1
    assert 'has_function_privilege' in cursor.execute.call_args.args[0]


def test_denied_snapshot_permission_closes_connection_without_writing_a_boundary():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (False,)
    with patch.object(db, 'open_connection', return_value=connection), \
            pytest.raises(RuntimeError, match='requires EXECUTE'):
        db.capture_snapshot_boundary(_config())
    assert cursor.execute.call_count == 1
    connection.commit.assert_not_called()
    connection.close.assert_called_once()


@pytest.mark.parametrize('replay_lsn', [None, '0/FFFF'])
def test_nonrecovering_secondary_is_rejected_even_with_an_advanced_lsn(replay_lsn):
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (False, replay_lsn)
    with patch.object(replication.singer, 'write_message') as write, \
            pytest.raises(replication.ReplicationSlotMigrationError, match='secondary is not in recovery'):
        replication.wait_for_replica_replay(_config(use_secondary=True), 100, connection=connection)
    write.assert_not_called()
    connection.close.assert_not_called()


@pytest.mark.parametrize('trigger', ['rows', 'time'])
@pytest.mark.parametrize('overlap', [False, True])
def test_backlog_checkpoint_waits_for_commit_after_rows_or_time(trigger, overlap):
    state = _state(lsn=100)
    emitted = []
    now = [0.0]
    with patch.object(replication.time, 'monotonic', side_effect=lambda: now[0]), \
            patch.object(replication, 'UPDATE_BOOKMARK_PERIOD', 3), \
            patch.object(replication.singer, 'write_message',
                         side_effect=lambda msg: emitted.append(copy.deepcopy(msg))):
        checkpoint = replication._PgoutputCheckpoint(state, [_stream()], overlap)
        for _ in range(3 if trigger == 'rows' else 1):
            checkpoint.cadence.observe({'_record_emitted': True})
        if trigger == 'time':
            now[0] = replication.CHECKPOINT_INTERVAL_SECONDS
        assert not emitted
        assert state['bookmarks']['public-payments']['lsn'] == 100

        commit_lsn = 90 if overlap else 150
        assert not checkpoint.record_commit({'end_lsn': commit_lsn}, 1000, False)
        assert emitted[-1].value['bookmarks']['public-payments']['lsn'] == max(100, commit_lsn)
        assert checkpoint.cadence.rows_since_state == 0
        assert not checkpoint.cadence.due()


def test_unselected_rows_do_not_trigger_row_checkpoints_and_post_boundary_commits_still_checkpoint():
    with patch.object(replication, 'UPDATE_BOOKMARK_PERIOD', 1), \
            patch.object(replication.singer, 'write_message') as write:
        checkpoint = replication._PgoutputCheckpoint(_state(), [_stream()], False)
        checkpoint.cadence.observe({'action': 'I'})
        assert not checkpoint.record_commit({'end_lsn': 110}, 150, False)
        write.assert_not_called()
        assert not checkpoint.record_commit({'end_lsn': 160}, 150, False)
        assert write.call_args.args[0].value['bookmarks']['public-payments']['lsn'] == 160


@pytest.mark.parametrize('trigger', ['rows', 'time'])
def test_legacy_backlog_checkpoint_is_emitted_only_after_the_large_transaction_commits(trigger):
    now = [0.0]
    actions = ['B', 'I', 'I', 'I', 'C', 'B', 'C']
    messages = [WalMessage(json.dumps({'action': action}), 101 + index) for index, action in enumerate(actions)]
    connection = _ReplicationConnection(messages)
    emitted = []

    def consume(_streams, state, _msg, _time, _config, *, message_payload):
        if message_payload['action'] == 'I':
            message_payload['_record_emitted'] = True
            assert len(emitted) == 1
            if trigger == 'time':
                now[0] = replication.CHECKPOINT_INTERVAL_SECONDS
        return state

    with patch.object(db, 'open_connection', return_value=connection), \
            patch.object(replication, 'emit_wal_progress_message', return_value=106), \
            patch.object(replication, 'consume_message', side_effect=consume), \
            patch.object(replication.time, 'monotonic', side_effect=lambda: now[0]), \
            patch.object(replication, 'UPDATE_BOOKMARK_PERIOD', 3 if trigger == 'rows' else 100), \
            patch.object(replication.singer, 'write_message',
                         side_effect=lambda message: emitted.append(copy.deepcopy(message.value))):
        replication._bridge_wal2json_slot(
            _config(), [_stream()], _state(), 'state.json',
            replication.PreparedPublication('ppw_slot_orders', wal2json_tables=[('public', 'payments')]),
            'pipelinewise_source_orders', 'ppw_slot_orders', 100, 100, None,
        )
    assert [state['bookmarks']['public-payments']['lsn'] for state in emitted] == [100, 105, 107]
    assert emitted[0][replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'] == 'bridge_pending'
    assert emitted[-1][replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'] == 'bridge'
    assert all(call.kwargs['flush_lsn'] == 0 for call in connection.cursor_instance.send_feedback.call_args_list)
