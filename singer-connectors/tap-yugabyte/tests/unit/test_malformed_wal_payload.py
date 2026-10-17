"""Malformed WAL messages must not advance a transaction checkpoint."""
import copy
import json
import traceback
import unittest
from collections import namedtuple
from unittest.mock import patch

from tap_yugabyte.sync_strategies import logical_replication


Message = namedtuple('Message', 'payload data_start')


class TestMalformedWALPayload(unittest.TestCase):
    def setUp(self):
        self.streams = [self._stream('public-items', 'items')]
        self.config = {
            'tap_id': 'test_tap',
            'max_run_seconds': 60,
            'break_at_end_lsn': False,
            'logical_poll_total_seconds': 120,
        }

    @staticmethod
    def _stream(stream_id, table):
        return {
            'tap_stream_id': stream_id,
            'stream': table,
            'table_name': table,
            'schema': {'properties': {'id': {'type': ['integer']}}},
            'metadata': [
                {'breadcrumb': [], 'metadata': {'schema-name': 'public'}},
                {'breadcrumb': ['properties', 'id'],
                 'metadata': {'sql-datatype': 'integer', 'inclusion': 'automatic'}},
            ],
        }

    @staticmethod
    def _message(action, lsn, **fields):
        return Message(json.dumps({'action': action, **fields}), lsn)

    @staticmethod
    def _row(lsn, row_id):
        return TestMalformedWALPayload._message(
            'I', lsn, schema='public', table='items',
            columns=[{'name': 'id', 'value': row_id}],
        )

    def _run_sync(self, state, messages, expected_read_count, *, close_error=False):
        emitted = []
        with patch.object(logical_replication, 'locate_replication_slot', return_value='test_slot'), \
                patch.object(logical_replication.sync_common, 'send_schema_message'), \
                patch.object(logical_replication.yb_db, 'open_connection') as connect, \
                patch.object(logical_replication.singer, 'write_message',
                             side_effect=lambda msg: emitted.append(copy.deepcopy(msg.asdict()))), \
                patch.object(logical_replication, 'UPDATE_BOOKMARK_PERIOD', 1):
            cursor = connect.return_value.cursor.return_value
            cursor.read_message.side_effect = [*messages, AssertionError('read after corrupt WAL')]
            if close_error:
                cursor.close.side_effect = RuntimeError('close failure')
            with self.assertRaises(logical_replication.MalformedWALPayloadError) as caught:
                logical_replication.sync_tables(self.config, self.streams, state, 1000, None)

        self.assertEqual(expected_read_count, cursor.read_message.call_count)
        self.assertTrue(cursor.close.called)
        self.assertTrue(connect.return_value.close.called)
        self.assertTrue(cursor.send_feedback.call_args_list)
        self.assertTrue(all(call.kwargs['flush_lsn'] <= 100 for call in cursor.send_feedback.call_args_list))
        return caught.exception, emitted

    def test_direct_consume_rejects_malformed_json_without_changing_state(self):
        state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
        before = copy.deepcopy(state)

        with self.assertRaises(logical_replication.MalformedWALPayloadError) as caught:
            logical_replication.consume_message(
                self.streams, state, Message('{bad-json', 200), None, self.config)

        self.assertEqual(before, state)
        self.assertIn('LSN 200', str(caught.exception))
        self.assertIn('test_tap', str(caught.exception))
        self.assertNotIn('{bad-json', str(caught.exception))
        self.assertIsNotNone(caught.exception.__cause__)
        self.assertEqual('', caught.exception.__cause__.doc)

    def test_direct_consume_rejects_invalid_row_shapes_with_context(self):
        column = {'name': 'id', 'value': 1}
        invalid_rows = [
            {'action': 'I', 'table': 'items', 'columns': [column]},
            {'action': 'I', 'schema': 'public', 'columns': [column]},
            {'action': 'I', 'schema': '', 'table': 'items', 'columns': [column]},
            {'action': 'I', 'schema': 'public', 'table': 7, 'columns': [column]},
            {'action': 'I', 'schema': 'public', 'table': 'items'},
            {'action': 'I', 'schema': 'public', 'table': 'items', 'columns': []},
            {'action': 'U', 'schema': 'public', 'table': 'items', 'columns': {}},
            {'action': 'U', 'schema': 'public', 'table': 'items', 'columns': [None]},
            {'action': 'U', 'schema': 'public', 'table': 'items', 'columns': [{'value': 1}]},
            {'action': 'U', 'schema': 'public', 'table': 'items', 'columns': [{'name': '', 'value': 1}]},
            {'action': 'U', 'schema': 'public', 'table': 'items', 'columns': [{'name': 'id'}]},
            {'action': 'D', 'schema': 'public', 'table': 'items'},
            {'action': 'D', 'schema': 'public', 'table': 'items', 'identity': []},
            {'action': 'D', 'schema': 'public', 'table': 'items', 'identity': [None]},
            {'action': 'D', 'schema': 'public', 'table': 'items', 'identity': [{'name': 'id'}]},
        ]
        for payload in invalid_rows:
            with self.subTest(payload=payload):
                state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
                before = copy.deepcopy(state)
                message = Message(json.dumps(payload), 200)
                with patch.object(logical_replication.singer, 'write_message') as write_message, \
                        self.assertRaises(logical_replication.MalformedWALPayloadError) as caught:
                    logical_replication.consume_message(
                        self.streams, state, message, None, self.config, slot='test_slot')
                self.assertEqual(before, state)
                write_message.assert_not_called()
                for context in ('LSN 200', 'test_slot', 'test_tap'):
                    self.assertIn(context, str(caught.exception))
                self.assertNotIn(json.dumps(payload), str(caught.exception))

    def test_null_column_value_is_valid(self):
        state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
        message = self._message(
            'I', 200, schema='public', table='items', columns=[{'name': 'id', 'value': None}])
        with patch.object(logical_replication.singer, 'write_message') as write_message:
            result = logical_replication.consume_message(
                self.streams, state, message, None, self.config, slot='test_slot')
        self.assertIs(result, state)
        self.assertIsNone(write_message.call_args.args[0].record['id'])

    def test_empty_row_fields_fail_before_commit_after_an_emitted_row(self):
        invalid_rows = [
            self._message('I', 200, schema='public', table='items', columns=[]),
            self._message('U', 200, schema='public', table='items', columns=[]),
            self._message('D', 200, schema='public', table='items', identity=[]),
        ]
        for invalid_row in invalid_rows:
            with self.subTest(action=json.loads(invalid_row.payload)['action']):
                state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
                messages = [self._message('B', 200), self._row(200, 1), invalid_row,
                            self._message('C', 200), self._message('B', 300)]
                error, emitted = self._run_sync(state, messages, expected_read_count=3)
                self.assertIn('LSN 200', str(error))
                self.assertIn('test_slot', str(error))
                self.assertEqual([1], [msg['record']['id'] for msg in emitted if msg['type'] == 'RECORD'])
                self.assertEqual([100], [msg['value']['bookmarks']['public-items']['lsn']
                                         for msg in emitted if msg['type'] == 'STATE'])

    def test_invalid_row_on_unselected_stream_fails_before_commit(self):
        state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
        bad_row = self._message('I', 200, schema='public', table='unselected', columns=[])
        error, emitted = self._run_sync(
            state, [self._message('B', 200), bad_row, self._message('C', 200)], expected_read_count=2)
        self.assertIn('LSN 200', str(error))
        self.assertEqual(100, emitted[-1]['value']['bookmarks']['public-items']['lsn'])

    def test_first_transaction_corruption_does_not_checkpoint_or_consume_later_messages(self):
        state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
        messages = [self._message('B', 200), self._row(200, 1), Message('{bad-json', 200),
                    self._message('C', 200), self._message('B', 300)]

        error, emitted = self._run_sync(state, messages, expected_read_count=3)

        self.assertIn('test_slot', str(error))
        self.assertEqual([1], [message['record']['id'] for message in emitted if message['type'] == 'RECORD'])
        self.assertEqual([100], [message['value']['bookmarks']['public-items']['lsn']
                                 for message in emitted if message['type'] == 'STATE'])
        self.assertEqual(100, state['bookmarks']['public-items']['lsn'])

    def test_corruption_after_completed_transaction_keeps_only_earlier_checkpoint(self):
        state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
        messages = [self._message('B', 200), self._row(200, 1), self._message('C', 200),
                    self._message('B', 300), self._row(300, 2), Message('{bad-json', 300),
                    self._message('C', 300)]

        _, emitted = self._run_sync(state, messages, expected_read_count=6)

        self.assertEqual([1, 2], [message['record']['id'] for message in emitted if message['type'] == 'RECORD'])
        self.assertEqual([200, 200], [message['value']['bookmarks']['public-items']['lsn']
                                      for message in emitted if message['type'] == 'STATE'])

    def test_unequal_starting_stream_checkpoints_are_retained(self):
        self.streams.append(self._stream('public-other', 'other'))
        state = {'bookmarks': {'public-items': {'lsn': 150, 'version': 1},
                               'public-other': {'lsn': 100, 'version': 1}}}

        _, emitted = self._run_sync(state, [self._message('B', 200), Message('{bad-json', 200)], 2)

        self.assertEqual(150, emitted[-1]['value']['bookmarks']['public-items']['lsn'])
        self.assertEqual(100, emitted[-1]['value']['bookmarks']['public-other']['lsn'])

    def test_cleanup_failure_does_not_mask_malformed_payload(self):
        state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
        error, emitted = self._run_sync(
            state, [self._message('B', 200), Message('{bad-json', 200)], 2,
            close_error=True,
        )
        self.assertIn('LSN 200', str(error))
        self.assertEqual(100, emitted[-1]['value']['bookmarks']['public-items']['lsn'])

    def test_invalid_utf8_and_decoded_shapes_fail_before_commit(self):
        invalid_payloads = [b'\x80secret', 'null', '[]', '42', '{}', '{"action":null}',
                            '{"action":[]}', '{"action":"unknown"}']
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
                error, emitted = self._run_sync(
                    state, [self._message('B', 200), Message(payload, 200), self._message('C', 200)], 2)
                self.assertEqual(100, emitted[-1]['value']['bookmarks']['public-items']['lsn'])
                self.assertNotIn('secret', ''.join(traceback.format_exception(error)))

    def test_supported_control_messages_leave_direct_checkpoint_unchanged(self):
        state = {'bookmarks': {'public-items': {'lsn': 100, 'version': 1}}}
        for action in ('B', 'C', 'M', 'T'):
            with self.subTest(action=action):
                result = logical_replication.consume_message(
                    self.streams, state, self._message(action, 200), None, self.config)
                self.assertIs(state, result)
                self.assertEqual(100, state['bookmarks']['public-items']['lsn'])
