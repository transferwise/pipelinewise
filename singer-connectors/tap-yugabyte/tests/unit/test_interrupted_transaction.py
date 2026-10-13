"""Checkpoint recovery when a HYBRID_TIME transaction is interrupted."""
import copy
import json
import unittest
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tap_yugabyte.sync_strategies import logical_replication


class TestInterruptedTransaction(unittest.TestCase):
    """Exercise real row consumption with a mocked replication connection."""

    def _run_interrupted_sync(self, stop, completed_transaction=False, *, finish_first=False,
                              progress_message=False, acknowledged_lsn=None, missing_commit=False,
                              expected_flush_lsn=None, commit_lsn_offset=0, check_feedback_boundary=False):
        stream = {
            'tap_stream_id': 'public-items',
            'stream': 'items',
            'table_name': 'items',
            'schema': {'properties': {'id': {'type': ['integer']}}},
            'metadata': [
                {'breadcrumb': [], 'metadata': {'schema-name': 'public'}},
                {'breadcrumb': ['properties', 'id'],
                 'metadata': {'sql-datatype': 'integer', 'inclusion': 'automatic'}},
            ],
        }
        state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
        config = {
            'max_run_seconds': 60,
            'logical_poll_total_seconds': 120 if stop == 'max_run_seconds' else 1,
            'break_at_end_lsn': False,
        }
        message = namedtuple('Message', ['payload', 'data_start'])

        def transaction(lsn, row_id, committed):
            messages = [
                message(json.dumps({'action': 'B'}), lsn),
                message(json.dumps({'action': 'I', 'schema': 'public', 'table': 'items',
                                    'columns': [{'name': 'id', 'value': row_id}]}), lsn),
            ]
            if committed:
                messages.append(message(json.dumps({'action': 'C'}), lsn + commit_lsn_offset))
            return messages

        messages = transaction(200, 1, not missing_commit) if completed_transaction else []
        messages.extend(transaction(300 if completed_transaction else 200, 2, finish_first))
        if progress_message:
            messages.extend([
                message(json.dumps({'action': 'B'}), 300),
                message(json.dumps({'action': 'M', 'transactional': True,
                                    'prefix': logical_replication.WAL_PROGRESS_MESSAGE_PREFIX,
                                    'content': 'test-progress'}), 300),
                message(json.dumps({'action': 'C'}), 300),
            ])
        pending = iter(messages)
        emitted = []
        now = datetime(2026, 9, 28, tzinfo=timezone.utc)
        disconnect = ConnectionError('replication connection lost before COMMIT')

        with TemporaryDirectory() as directory, \
                patch.object(logical_replication.yb_db, 'open_connection') as connect, \
                patch.object(logical_replication, 'locate_replication_slot', return_value='test_slot'), \
                patch.object(logical_replication.utils, 'now', return_value=now), \
                patch.object(logical_replication.datetime, 'datetime') as clock, \
                patch.object(logical_replication.singer, 'write_message',
                             side_effect=lambda msg: emitted.append(copy.deepcopy(msg.asdict()))):
            state_file = None
            if acknowledged_lsn is not None:
                state_file = str(Path(directory) / 'state.json')
                acknowledged_state = copy.deepcopy(state)
                acknowledged_state['bookmarks']['public-items']['lsn'] = acknowledged_lsn
                Path(state_file).write_text(json.dumps(acknowledged_state), encoding='utf-8')
            clock.now.return_value = now
            cursor = connect.return_value.cursor.return_value

            def read_message():
                if check_feedback_boundary:
                    for call in cursor.send_feedback.call_args_list:
                        self.assertLessEqual(call.kwargs['flush_lsn'], 100)
                    clock.now.return_value += timedelta(seconds=logical_replication.FEEDBACK_POLL_INTERVAL)
                msg = next(pending)
                payload = json.loads(msg.payload)
                stop_here = (
                    payload.get('action') == 'C'
                    and msg.data_start == (300 if progress_message else 200 + commit_lsn_offset)
                    if finish_first else
                    payload.get('action') == 'I' and payload['columns'][0]['value'] == 2
                )
                if stop_here:
                    if stop == 'max_run_seconds':
                        clock.now.return_value = now + timedelta(seconds=61)
                    elif stop == 'idle_timeout':
                        clock.now.return_value = now + timedelta(seconds=2)
                    else:
                        cursor.read_message.side_effect = disconnect
                return msg

            cursor.read_message.side_effect = read_message
            if stop == 'disconnect':
                with self.assertRaises(ConnectionError) as caught:
                    logical_replication.sync_tables(
                        config, [stream], state, 1000, state_file,
                        wal_progress_content='test-progress' if progress_message else None)
                self.assertIs(caught.exception, disconnect)
            else:
                returned_state = logical_replication.sync_tables(
                    config, [stream], state, 1000, state_file,
                    wal_progress_content='test-progress' if progress_message else None)
                self.assertIs(returned_state, state)

        records = [msg for msg in emitted if msg['type'] == 'RECORD']
        self.assertEqual([1, 2] if completed_transaction else [2], [msg['record']['id'] for msg in records])
        checkpoints = [msg['value'] for msg in emitted if msg['type'] == 'STATE']
        self.assertTrue(checkpoints)
        expected_lsn = 300 if progress_message else 200 + commit_lsn_offset if (
            completed_transaction and not missing_commit) or finish_first else 100
        expected_lsn = max(expected_lsn, acknowledged_lsn or 100)
        feedback = cursor.send_feedback.call_args_list
        self.assertTrue(feedback)
        flush_limit = expected_flush_lsn if expected_flush_lsn is not None else acknowledged_lsn or 100
        for call in feedback:
            self.assertLessEqual(call.kwargs['flush_lsn'], flush_limit)
        if acknowledged_lsn is not None:
            self.assertEqual(flush_limit, feedback[-1].kwargs['flush_lsn'])
        for checkpoint in checkpoints:
            self.assertEqual(expected_lsn, checkpoint['bookmarks']['public-items']['lsn'])
        self.assertEqual(expected_lsn, state['bookmarks']['public-items']['lsn'])

    def test_disconnect_during_first_transaction_retains_initial_checkpoint(self):
        self._run_interrupted_sync('disconnect')

    def test_run_limit_during_first_transaction_retains_initial_checkpoint(self):
        self._run_interrupted_sync('max_run_seconds')

    def test_idle_timeout_during_first_transaction_retains_initial_checkpoint(self):
        self._run_interrupted_sync('idle_timeout')

    def test_disconnect_during_second_transaction_retains_completed_checkpoint(self):
        for commit_lsn_offset in (0, 1):
            with self.subTest(commit_lsn_offset=commit_lsn_offset):
                self._run_interrupted_sync('disconnect', completed_transaction=True,
                                           commit_lsn_offset=commit_lsn_offset)

    def test_completed_first_transaction_without_later_message_checkpoints(self):
        for commit_lsn_offset in (0, 1):
            with self.subTest(commit_lsn_offset=commit_lsn_offset):
                self._run_interrupted_sync('max_run_seconds', finish_first=True,
                                           commit_lsn_offset=commit_lsn_offset)

    def test_row_before_commit_does_not_flush_the_acknowledged_commit_lsn(self):
        self._run_interrupted_sync('max_run_seconds', acknowledged_lsn=201,
                                   expected_flush_lsn=100, commit_lsn_offset=1, check_feedback_boundary=True)

    def test_commit_flushes_acknowledged_lsn_without_a_later_message(self):
        self._run_interrupted_sync('max_run_seconds', finish_first=True, acknowledged_lsn=201,
                                   expected_flush_lsn=201, commit_lsn_offset=1, check_feedback_boundary=True)

    def test_wal_progress_commit_retains_completed_checkpoint(self):
        self._run_interrupted_sync('max_run_seconds', finish_first=True, progress_message=True)

    def test_first_transaction_preserves_newer_target_acknowledgement(self):
        self._run_interrupted_sync('max_run_seconds', acknowledged_lsn=150)

    def test_missing_commit_before_second_transaction_retains_initial_checkpoint(self):
        self._run_interrupted_sync('disconnect', completed_transaction=True, missing_commit=True)

    def test_missing_commit_cannot_be_flushed_after_a_higher_lsn_arrives(self):
        self._run_interrupted_sync('max_run_seconds', completed_transaction=True, missing_commit=True,
                                   acknowledged_lsn=200, expected_flush_lsn=100)
