"""Replay a real HYBRID_TIME transaction after interrupting row consumption."""
import copy
import json
import unittest
import uuid
from unittest.mock import patch

from tap_yugabyte.sync_strategies import logical_replication

from tests.integration.test_full_table import _discover_stream
from tests.utils import (
    create_replication_slot,
    drop_replication_slot,
    drop_table,
    ensure_test_table,
    get_test_connection,
    get_test_connection_config,
)


class _ObservedCursor:
    """Forward replication traffic, interrupting only after delivered row messages."""

    def __init__(self, cursor, interrupt_after):
        self.cursor = cursor
        self.interrupt_after = interrupt_after
        self.rows_read = 0
        self.messages = []
        self.feedback = []

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def read_message(self):
        if self.interrupt_after is not None and self.rows_read == self.interrupt_after:
            raise ConnectionError('test interruption before transaction commit')
        message = self.cursor.read_message()
        if message is not None:
            payload = json.loads(message.payload)
            action = payload.get('action')
            self.messages.append((action, message.data_start))
            if action == 'I':
                self.rows_read += 1
        return message

    def send_feedback(self, **kwargs):
        self.feedback.append(kwargs.copy())
        return self.cursor.send_feedback(**kwargs)


class _ObservedConnection:
    """Keep the actual replication connection behind an observable cursor."""

    def __init__(self, connection, interrupt_after):
        self.connection = connection
        self.observed_cursor = _ObservedCursor(connection.cursor(), interrupt_after)

    def cursor(self):
        return self.observed_cursor

    def close(self):
        self.connection.close()


class TestInterruptedTransactionReplay(unittest.TestCase):
    """Require complete replay and commit-driven checkpoints on a live server."""

    def _run_sync(self, config, stream, state, interrupt_after=None):
        emitted = []
        connections = []
        open_connection = logical_replication.yb_db.open_connection

        def observe_connection(conn_info, *args, **kwargs):
            connection = open_connection(conn_info, *args, **kwargs)
            if kwargs.get('logical_replication'):
                connection = _ObservedConnection(connection, interrupt_after)
                connections.append(connection)
            return connection

        try:
            with patch.object(logical_replication.yb_db, 'open_connection', side_effect=observe_connection), \
                    patch.object(logical_replication.singer, 'write_message',
                                 side_effect=lambda msg: emitted.append(copy.deepcopy(msg.asdict()))):
                if interrupt_after is not None:
                    with self.assertRaisesRegex(ConnectionError, 'test interruption before transaction commit'):
                        logical_replication.sync_tables(config, [stream], state, 2 ** 64 - 1, None)
                else:
                    returned_state = logical_replication.sync_tables(config, [stream], state, 2 ** 64 - 1, None)
                    self.assertIs(returned_state, state)
        finally:
            for connection in connections:
                connection.close()

        self.assertEqual(1, len(connections))
        return emitted, connections[0].observed_cursor

    def test_interrupted_rows_replay_before_checkpoint_advances_to_commit(self):
        table = f'interrupted_{uuid.uuid4().hex[:10]}'
        config = get_test_connection_config()
        config.update(tap_id=table, break_at_end_lsn=False, max_run_seconds=60, logical_poll_total_seconds=10)
        ensure_test_table({'name': table, 'columns': [{'name': 'id', 'type': 'integer', 'primary_key': True}]})
        self.addCleanup(drop_table, table)
        stream = _discover_stream(config, table)
        stream_id = stream['tap_stream_id']
        create_replication_slot(tap_id=table)
        self.addCleanup(drop_replication_slot, tap_id=table)

        with get_test_connection() as connection, connection.cursor() as cursor:
            cursor.execute('SELECT yb_get_current_hybrid_time_lsn()')
            initial_lsn = cursor.fetchone()[0]
            cursor.execute(f'INSERT INTO {table} SELECT generate_series(1, 1000)')

        state = {'bookmarks': {stream_id: {'lsn': initial_lsn, 'version': 1}}}
        interrupted, first_cursor = self._run_sync(config, stream, state, interrupt_after=10)
        first_records = [message['record']['id'] for message in interrupted if message['type'] == 'RECORD']
        self.assertEqual(10, len(first_records))
        self.assertNotIn('C', [action for action, _ in first_cursor.messages])
        self.assertTrue(first_cursor.feedback)
        self.assertTrue(all(call['flush_lsn'] <= initial_lsn for call in first_cursor.feedback))
        checkpoints = [message['value'] for message in interrupted if message['type'] == 'STATE']
        self.assertTrue(checkpoints)
        self.assertTrue(all(checkpoint['bookmarks'][stream_id]['lsn'] == initial_lsn for checkpoint in checkpoints))
        self.assertEqual(initial_lsn, state['bookmarks'][stream_id]['lsn'])

        replay_state = copy.deepcopy(checkpoints[-1])
        replayed, second_cursor = self._run_sync(config, stream, replay_state)
        replayed_ids = [message['record']['id'] for message in replayed if message['type'] == 'RECORD']
        self.assertEqual(set(range(1, 1001)), set(replayed_ids))
        self.assertTrue(set(first_records).issubset(replayed_ids))
        row_lsns = {lsn for action, lsn in second_cursor.messages if action in {'B', 'I'}}
        commit_lsns = [lsn for action, lsn in second_cursor.messages if action == 'C']
        self.assertEqual(1, len(row_lsns))
        self.assertEqual([next(iter(row_lsns)) + 1], commit_lsns)
        self.assertGreater(commit_lsns[0], initial_lsn)
        replay_checkpoints = [message['value'] for message in replayed if message['type'] == 'STATE']
        self.assertTrue(replay_checkpoints)
        self.assertTrue(all(checkpoint['bookmarks'][stream_id]['lsn'] == commit_lsns[0]
                            for checkpoint in replay_checkpoints))
        self.assertEqual(commit_lsns[0], replay_state['bookmarks'][stream_id]['lsn'])
        self.assertTrue(all(call['flush_lsn'] <= initial_lsn for call in second_cursor.feedback))
