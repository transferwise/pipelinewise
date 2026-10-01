import copy
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from transform_field import TransformField


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
                    'destination_slot': 'pipelinewise_accounts',
                    'copy_lsn': 100,
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
