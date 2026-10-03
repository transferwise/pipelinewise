import copy
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from transform_field import TransformField, TransformFieldException


class TestPatchPassthrough(unittest.TestCase):
    """Exercise sparse PostgreSQL records through Singer parsing and serialization."""

    def setUp(self):
        self.schema = {
            'type': 'SCHEMA',
            'stream': 'public-accounts',
            'schema': {
                'type': 'object',
                'x-pipelinewise-record-update-mode': 'PATCH',
                'properties': {
                    'id': {'type': ['integer']},
                    'email': {'type': ['null', 'string']},
                    'profile': {'type': ['null', 'object']},
                    'classification': {'type': ['null', 'string']},
                    '_sdc_deleted_at': {'type': ['null', 'string'], 'format': 'date-time'},
                },
            },
            'key_properties': ['id'],
        }
        self.config = {'transformations': [
            {'tap_stream_name': 'public-accounts', 'field_id': 'email', 'type': 'SET-NULL'},
        ]}
        self.state = {
            'type': 'STATE',
            'value': {
                'bookmarks': {'public-accounts': {'lsn': 300}},
                '_pipelinewise_pgoutput_migration': {
                    'version': 1,
                    'phase': 'retire',
                    'source_slot': 'pipelinewise_source_accounts',
                    'destination_slot': 'ppw_slot_accounts',
                    'slot_lsn': 100,
                    'bridge_lsn': 200,
                    'retire_lsn': 300,
                },
            },
        }

    @staticmethod
    def record(values):
        return {
            'type': 'RECORD',
            'stream': 'public-accounts',
            'record': values,
            'version': 1,
            'time_extracted': '2026-10-01T12:00:00.000000Z',
        }

    def consume(self, messages, config=None):
        reader = (json.dumps(message) + '\n' for message in messages)
        with io.TextIOWrapper(io.BytesIO(), encoding='utf-8') as output:
            with redirect_stdout(output):
                TransformField(self.config if config is None else config).consume(reader)
            output.seek(0)
            return [json.loads(line) for line in output.read().splitlines()]

    def test_patch_schema_and_record_metadata_survive_serialization(self):
        self.schema['bookmark_properties'] = ['id']
        record = self.record({'id': 1})

        output = self.consume([self.schema, record], config={'transformations': []})

        self.assertEqual(self.schema, output[0])
        self.assertEqual(record, output[1])

    def test_transform_preserves_omitted_columns_and_explicit_null(self):
        records = [
            self.record({'id': 1}),
            self.record({'id': 2, 'email': None, 'profile': None}),
            self.record({'id': 3, 'email': 'private@example.com', 'profile': {'name': 'Ada'}}),
        ]

        output = self.consume([self.schema, *records])

        self.assertEqual({'id': 1}, output[1]['record'])
        self.assertEqual({'id': 2, 'email': None, 'profile': None}, output[2]['record'])
        self.assertEqual({'id': 3, 'email': None, 'profile': {'name': 'Ada'}}, output[3]['record'])

    def test_sparse_delete_precedes_unchanged_migration_checkpoint(self):
        update = self.record({'id': 1, 'email': 'private@example.com'})
        deletion = self.record({'id': 1, '_sdc_deleted_at': '2026-10-01T12:00:01Z'})

        output = self.consume([self.schema, update, deletion, self.state])

        self.assertEqual(['SCHEMA', 'RECORD', 'RECORD', 'STATE'], [message['type'] for message in output])
        self.assertEqual({'id': 1, 'email': None}, output[1]['record'])
        self.assertEqual(deletion, output[2])
        self.assertEqual(self.state, output[3])

    def test_schema_change_flushes_previous_records_and_checkpoint_first(self):
        changed_schema = copy.deepcopy(self.schema)
        changed_schema['schema']['properties']['new_column'] = {'type': ['null', 'string']}
        first_record = self.record({'id': 1})
        second_record = self.record({'id': 2, 'new_column': None})
        second_state = copy.deepcopy(self.state)
        second_state['value']['bookmarks']['public-accounts']['lsn'] = 400

        output = self.consume([
            self.schema, first_record, self.state, changed_schema, second_record, second_state,
        ])

        self.assertEqual([
            self.schema, first_record, self.state, changed_schema, second_record, second_state,
        ], output)

    def test_batch_flush_cannot_acknowledge_a_sparse_record_before_output(self):
        first_record = self.record({'id': 1})
        second_record = self.record({'id': 2, 'profile': None})
        second_state = copy.deepcopy(self.state)
        second_state['value']['bookmarks']['public-accounts']['lsn'] = 400

        with patch('transform_field.DEFAULT_MAX_BATCH_RECORDS', 2):
            output = self.consume([self.schema, first_record, self.state, second_record, second_state])

        self.assertEqual([self.schema, first_record, second_record, self.state, second_state], output)

    def conditional_config(self, column='classification'):
        return {'transformations': [{
            'tap_stream_name': 'public-accounts', 'field_id': 'email', 'type': 'SET-NULL',
            'when': [{'column': column, 'regex_match': '^private'}],
        }]}

    def test_incomplete_conditional_patch_emits_neither_raw_record_nor_later_state(self):
        for values in ({'id': 1, 'email': 'sensitive@example.com'}, {'id': 1, 'classification': 'private'}):
            with self.subTest(values=values):
                messages = [self.schema, self.record(values), self.state]
                reader = (json.dumps(message) + '\n' for message in messages)
                with io.TextIOWrapper(io.BytesIO(), encoding='utf-8') as output:
                    with redirect_stdout(output), self.assertRaisesRegex(
                            TransformFieldException, 'Cannot safely apply conditional transformation') as error:
                        TransformField(self.conditional_config()).consume(reader)
                    output.seek(0)
                    emitted = [json.loads(line) for line in output.read().splitlines()]
                self.assertFalse(any(message['type'] in ('RECORD', 'STATE') for message in emitted))
                self.assertNotIn('sensitive@example.com', str(error.exception))

    def test_present_conditions_keep_configured_masking_and_null_semantics(self):
        records = [
            self.record({'id': 1, 'email': 'sensitive@example.com', 'classification': 'private'}),
            self.record({'id': 2, 'email': 'public@example.com', 'classification': 'public'}),
            self.record({'id': 3, 'email': 'null@example.com', 'classification': None}),
            self.record({'id': 4}),
        ]
        output = self.consume([self.schema, *records, self.state], self.conditional_config())
        self.assertIsNone(output[1]['record']['email'])
        self.assertEqual('public@example.com', output[2]['record']['email'])
        self.assertEqual('null@example.com', output[3]['record']['email'])
        self.assertEqual({'id': 4}, output[4]['record'])
        self.assertEqual(self.state, output[-1])

    def test_sparse_delete_does_not_require_a_missing_transformed_value(self):
        deletion = self.record({'id': 1, '_sdc_deleted_at': '2026-10-01T12:00:01Z'})
        output = self.consume([self.schema, deletion, self.state], self.conditional_config(column='id'))
        self.assertEqual([self.schema, deletion, self.state], output)

    def test_delete_with_sensitive_value_still_requires_its_condition(self):
        deletion = self.record({
            'id': 1, 'email': 'sensitive@example.com', '_sdc_deleted_at': '2026-10-01T12:00:01Z',
        })
        with self.assertRaisesRegex(TransformFieldException, 'missing fields'):
            self.consume([self.schema, deletion, self.state], self.conditional_config())

    def test_non_patch_conditional_behavior_is_unchanged(self):
        del self.schema['schema']['x-pipelinewise-record-update-mode']
        record = self.record({'id': 1, 'email': 'public@example.com'})
        self.assertEqual([self.schema, record], self.consume([self.schema, record], self.conditional_config()))
