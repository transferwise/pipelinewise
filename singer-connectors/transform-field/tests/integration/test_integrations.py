import subprocess
import unittest
import os
import sys
import json
import tempfile

from transform_field import TransformField, TransformFieldException, InvalidTransformationException


class Base(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        sys.stdout = self._stdout = tempfile.NamedTemporaryFile('w+', delete=True)
        sys.stderr.write(self._stdout.name + ' ')

    def teardown(self):
        self._stdout.close()
        sys.stdout = sys.__stdout__

    @property
    def stdout(self):
        self._stdout.seek(0)
        return self._stdout.read()[:-1]  # Remove trailing \n:w

    def get_tap_input_messages(self, filename):
        lines = []
        with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as tap_stdout:
            for line in tap_stdout.readlines():
                lines.append(line)

        return lines

    def singer_output_to_objects(self, output):
        messages = []
        for message in output.splitlines():
            messages.append(json.loads(message))

        return messages


class TestIntegration(Base):

    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = self.get_tap_input_messages('invalid_messages.json')
        trans_config = {'transformations': []}

        transform_field = TransformField(trans_config)
        with self.assertRaises(TransformFieldException):
            transform_field.consume(tap_lines)

    def test_multiple_singer_json_messages(self):
        """Test a bunch of singer messages with different field transformation types"""
        tap_lines = self.get_tap_input_messages('messages.json')

        # Set transformations on some columns
        trans_config = {'transformations': [
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_1', 'type': 'SET-NULL'},
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_2', 'type': 'HASH'},
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_3', 'type': 'HASH-SKIP-FIRST-2'},
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_4', 'type': 'HASH-SKIP-FIRST-3'},
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_5', 'type': 'MASK-DATE'},
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_6', 'type': 'MASK-NUMBER'},
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_11', 'type': 'SET-NULL',
             'when': [
                 {'column': 'column_7', 'equals': "Dummy row 2"},
                 {'column': 'column_9', 'equals': 200},
                 {'column': 'column_10', 'regex_match': 'sensitive'},
             ]
             },
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_12', 'type': 'MASK-HIDDEN'},
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_13', 'type': 'MASK-STRING-SKIP-ENDS-2'},
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_14', 'type': 'MASK-STRING-SKIP-ENDS-3'}
        ]}

        transform_field = TransformField(trans_config)
        transform_field.consume(tap_lines)

        singer_output_messages = self.singer_output_to_objects(self.stdout)

        # First message is the STATE message
        self.assertEqual(
            singer_output_messages[0],
            {
                'type': 'STATE',
                'value': {'currently_syncing': 'dummy_stream'}
            }
        )

        # Second message is the SCHEMA message
        self.assertEqual(
            singer_output_messages[1],
            {
                'type': 'SCHEMA',
                'stream': 'dummy_stream',
                'schema': {
                    'properties': {
                        'c_pk': {'inclusion': 'automatic', 'minimum': -2147483648, 'maximum': 2147483647,
                                 'type': ['null', 'integer']},
                        'column_1': {'inclusion': 'available', 'maxLength': 16, 'type': ['null', 'string']},
                        'column_2': {'inclusion': 'available', 'maxLength': 16, 'type': ['null', 'string']},
                        'column_3': {'inclusion': 'available', 'maxLength': 16, 'type': ['null', 'string']},
                        'column_4': {'inclusion': 'available', 'maxLength': 16, 'type': ['null', 'string']},
                        'column_5': {'inclusion': 'available', 'format': 'date-time', 'type': ['null', 'string']},
                        'column_6': {'inclusion': 'available', 'type': ['null', 'integer']},
                        'column_7': {'inclusion': 'available', 'maxLength': 16, 'type': ['null', 'string']},
                        'column_8': {'inclusion': 'available', 'format': 'date-time', 'type': ['null', 'string']},
                        'column_9': {'inclusion': 'available', 'type': ['null', 'integer']},
                        'column_10': {'inclusion': 'available', 'maxLength': 64, 'type': ['null', 'string']},
                        'column_11': {'inclusion': 'available', 'maxLength': 64, 'type': ['null', 'string']},
                        'column_12': {'inclusion': 'available', 'maxLength': 64, 'type': ['null', 'string']},
                        'column_13': {'inclusion': 'available', 'maxLength': 16, 'type': ['null', 'string']},
                        'column_14': {'inclusion': 'available', 'maxLength': 16, 'type': ['null', 'string']},
                    },
                    'type': 'object'
                },
                'key_properties': ['c_pk']
            }
        )

        # Third message is a RECORD message with transformed values 
        self.assertEqual(
            singer_output_messages[2],
            {
                'type': 'RECORD',
                'stream': 'dummy_stream',
                'record': {
                    'c_pk': 1,
                    'column_1': None,  # should be SET-NULL transformed
                    'column_2': 'c584d22683f3e523df9a7396e7939c0da16af89976b613adfe4bcd4c9c526f32',
                    # Should be HASH transformed
                    'column_3': 'Ducd571661edac8d47669a60b964c7124b228b69862cd21d548794af41c139a8e3',
                    # Should be HASH-SKIP-2 tranformed
                    'column_4': 'Dum1fe9627d907b0a37a31b270cc0f660a7388eb470a2558e839e0c1f601aedfaa7',
                    # Should be HASH-SKIP-3 tranformed
                    'column_5': '2019-01-01T12:12:45',  # Should be MASK-DATE transformed
                    'column_6': 0,  # Should be MASK-NUMBER transformed
                    'column_7': 'Dummy row 1',  # Should be the originl value - Unknown transformation type
                    'column_8': '2019-12-21T12:12:45',  # Should be the original date-time value
                    'column_9': 100,  # Should be the original number value

                    # Conditional transformation
                    'column_10': 'column_11 is safe to keep',
                    'column_11': 'My name is John',

                    'column_12': 'hidden',

                    # Should be MASK-STRING-SKIP-ENDS-2 transformed
                    'column_13': 'do****me',
                    # Should be MASK-STRING-SKIP-ENDS-3 transformed
                    'column_14': 'dom**kme',
                },
                'version': 1,
                'time_extracted': '2019-01-31T15:51:50.215998Z'
            }
        )

        # Third message is a RECORD message with transformed values 
        self.assertEqual(
            singer_output_messages[3],
            {
                'type': 'RECORD',
                'stream': 'dummy_stream',
                'record': {
                    'c_pk': 2,
                    'column_1': None,  # should be SET-NULL transformed
                    'column_2': '12c7ca803f4ae4044b8c3a6aa7dbaf9fe73a25e12f2258dbf8a832961ac6abab',
                    # Should be HASH tranformed
                    'column_3': 'Du7c2717bbc7489d36cea73c8519c815ce962142a5b32db413abe0bce7f58d943f',
                    # Should be HASH-SKIP-3 tranformed
                    'column_4': 'Dum5b2be872199a84657234144caec9106483a522edd36783c7a12439bcf3853c56',
                    # Should be HASH-SKIP-3 tranformed
                    'column_5': '2019-01-01T13:12:45',  # Should be MASK-DATE transformed
                    'column_6': 0,  # Should be MASK-NUMBER transformed
                    'column_7': 'Dummy row 2',  # Should be the origian value - Unknown transformation type
                    'column_8': '2019-12-21T13:12:45',  # Should be the original date-time value
                    'column_9': 200,  # Should be the original number value

                    # Conditional transformation
                    'column_10': 'column_11 has sensitive data. Needs to transform to NULL',
                    'column_11': None,  # Should be SET-NULL transformed

                    'column_12': 'hidden',

                    # Should be MASK-STRING-SKIP-ENDS-2 transformed
                    'column_13': '***',
                    # Should be MASK-STRING-SKIP-ENDS-3 transformed
                    'column_14': '******',
                },
                'version': 1,
                'time_extracted': '2019-01-31T15:51:50.215998Z'
            }
        )

    def test_messages_with_changing_schema(self):
        """Test a bunch of singer messages where a column in schema message
        changes its type"""
        tap_lines = self.get_tap_input_messages('streams_with_changing_schema.json')

        # Set transformations on some columns
        trans_config = {'transformations': [
            {'tap_stream_name': 'dummy_stream', 'field_id': 'column_2', 'type': 'MASK-NUMBER'},
        ]}

        transform_field = TransformField(trans_config)

        with self.assertRaises(InvalidTransformationException):
            transform_field.consume(tap_lines)

    def test_validate_flag_with_invalid_transformations(self):
        config = '{}/resources/invalid_config.json'.format(os.path.dirname(__file__))
        catalog = '{}/resources/catalog.json'.format(os.path.dirname(__file__))

        result = subprocess.run([
            'transform-field',
            '--validate',
            '--config', config,
            '--catalog', catalog,
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        with self.assertRaises(subprocess.CalledProcessError):
            result.check_returncode()

    def test_validate_flag_with_valid_transformations(self):

        config = '{}/resources/valid_config.json'.format(os.path.dirname(__file__))
        catalog = '{}/resources/catalog.json'.format(os.path.dirname(__file__))

        result = subprocess.run([
            'transform-field',
            '--validate',
            '--config', config,
            '--catalog', catalog,
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        self.assertIsNone(result.check_returncode())

    def test_multiple_singer_json_messages_with_transformation_on_json(self):
        """Test a bunch of singer messages with transformation on json"""
        tap_lines = self.get_tap_input_messages('streams_with_object.json')

        # Set transformations on some columns
        trans_config = {'transformations': [
            {'tap_stream_name': 'my_cool_stream', 'field_id': 'column_1', 'type': 'SET-NULL'},
            {'tap_stream_name': 'my_cool_stream', 'field_id': 'column_2', 'type': 'MASK-HIDDEN'},
            {'tap_stream_name': 'my_cool_stream', 'field_id': 'column_3', 'type': 'MASK-DATE',
             'when': [
                 {'column': 'c_pk', 'equals': 2},
                 {'column': 'column_6', 'field_path': 'key1', 'equals': 'B'}
             ]
             },
            {'tap_stream_name': 'my_cool_stream', 'field_id': 'column_4', 'type': 'MASK-NUMBER',
                'when': [
                    {'column': 'column_4', 'equals': -44},
                ]
             },
            {'tap_stream_name': 'my_cool_stream', 'field_id': 'column_6', 'type': 'SET-NULL',
             'field_paths': ['key2/key2_2']},
        ]}

        transform_field = TransformField(trans_config)
        transform_field.consume(tap_lines)

        records = [msg['record'] for msg in self.singer_output_to_objects(self.stdout) if msg['type'] == 'RECORD']

        self.assertListEqual(records, [
            {
                'c_pk': 1,
                'column_1': None,
                'column_2': 'hidden',
                'column_3': '2019-12-21T12:12:45',
                'column_4': 1234,
                'column_5': '2021-12-21T12:12:45',
                'column_6': {'id': 50, 'key1': 'A', 'key2': {'key2_2': None}},
            },
            {
                'c_pk': 2,
                'column_1': None,
                'column_2': 'hidden',
                'column_3': '2019-01-01T13:12:45',
                'column_4': 4,
                'column_5': '2021-12-21T13:12:45',
                'column_6': {'id': 51, 'key1': 'B', 'key2': {'key2_1': 'ds'}},
            },
            {
                'c_pk': 3,
                'column_1': None,
                'column_2': 'hidden',
                'column_3': '2019-12-21T14:12:45',
                'column_4': 15,
                'column_5': '2021-12-21T14:12:45',
                'column_6': {'id': 52, 'key1': 'C', 'key2': {'key2_1': 'xv43dgf', 'key2_2': None}},
            },
            {
                'c_pk': 4,
                'column_1': None,
                'column_2': 'hidden',
                'column_3': '2019-12-21T15:12:45',
                'column_4': 1000,
                'column_5': '2021-12-21T15:12:45',
                'column_6': {'id': 53, 'key1': 'D', 'key2': {'key2_1': '43xvf', 'key2_2': None}},
            },
            {
                'c_pk': 5,
                'column_1': None,
                'column_2': 'hidden',
                'column_3': '2019-12-21T16:12:45',
                'column_4': 0,
                'column_5': '2021-12-21T16:12:45',
                'column_6': {'id': 54, 'key1': 'E', 'key2': {'key2_1': 'trter', 'key2_3': False}},
            },
        ])
