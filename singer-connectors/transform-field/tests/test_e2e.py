import unittest
import os
import sys
import json
import tempfile

from nose.tools import assert_raises 
from transform_field import TransformField, TransformFieldException


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
        return self._stdout.read()[:-1] # Remove trailing \n:w


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


class TestEndToEnd(Base):

    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = self.get_tap_input_messages('invalid-json.json')
        trans_config = {'transformations': []}

        transform_field = TransformField(trans_config)
        with assert_raises(TransformFieldException):
            transform_field.consume(tap_lines)


    def test_multiple_singer_json_messages(self):
        """Test a bunch of singer messages with different field transformation types"""
        tap_lines = self.get_tap_input_messages('messages.json')

        # Set transformations on some columns
        trans_config = {'transformations': [
          { 'tap_stream_name': 'dummy_stream', 'field_id': 'column_1', 'type': 'SET-NULL' },
          { 'tap_stream_name': 'dummy_stream', 'field_id': 'column_2', 'type': 'HASH' },
          { 'tap_stream_name': 'dummy_stream', 'field_id': 'column_3', 'type': 'HASH-SKIP-FIRST-2' },
          { 'tap_stream_name': 'dummy_stream', 'field_id': 'column_4', 'type': 'HASH-SKIP-FIRST-3' },
          { 'tap_stream_name': 'dummy_stream', 'field_id': 'column_5', 'type': 'MASK-DATE' },
          { 'tap_stream_name': 'dummy_stream', 'field_id': 'column_6', 'type': 'MASK-NUMBER' },
          { 'tap_stream_name': 'dummy_stream', 'field_id': 'column_7', 'type': 'NOT-EXISTING-TRANSFORMATION-TYPE' },
          { 'tap_stream_name': 'dummy_stream', 'field_id': 'column_11', 'type': 'SET-NULL',
              'when': [
                {'column': 'column_7', 'equals': "Dummy row 2" },
                {'column': 'column_9', 'equals': 200 },
                {'column': 'column_10', 'regex_match': 'sensitive' },
              ]
          }
        ]}

        transform_field = TransformField(trans_config)
        transform_field.consume(tap_lines)

        singer_output_messages = self.singer_output_to_objects(self.stdout)

        # First message is the STATE message
        self.assertEquals(
            singer_output_messages[0],
            {
              'type': 'STATE',
              'value': {'currently_syncing': 'dummy_stream'}
            }
        )

        # Second message is the SCHEMA message
        self.assertEquals(
            singer_output_messages[1],
            {
              'type': 'SCHEMA',
              'stream': 'dummy_stream',
              'schema': {
                'properties': {
                  'c_pk': {'inclusion': 'automatic', 'minimum': -2147483648, 'maximum': 2147483647, 'type': ['null', 'integer']},
                  'column_1': {'inclusion':'available', 'maxLength': 16, 'type': ['null', 'string']},
                  'column_2': {'inclusion':'available', 'maxLength': 16, 'type': ['null', 'string']},
                  'column_3': {'inclusion':'available', 'maxLength': 16, 'type': ['null', 'string']},
                  'column_4': {'inclusion':'available', 'maxLength': 16, 'type': ['null', 'string']},
                  'column_5': {'inclusion':'available', 'format':'date-time', 'type': ['null', 'string']},
                  'column_6': {'inclusion':'available', 'type': ['null', 'integer']},
                  'column_7': {'inclusion':'available', 'maxLength': 16, 'type': ['null', 'string']},
                  'column_8': {'inclusion':'available', 'format':'date-time', 'type': ['null', 'string']},
                  'column_9': {'inclusion':'available', 'type': ['null', 'integer']},
                  'column_10': {'inclusion':'available', 'maxLength': 64, 'type': ['null', 'string']},
                  'column_11': {'inclusion':'available', 'maxLength': 64, 'type': ['null', 'string']},
                },
                'type': 'object'
              },
              'key_properties': ['c_pk']
            }
        )

        # Third message is a RECORD message with transformed values 
        self.assertEquals(
            singer_output_messages[2],
            {
              'type': 'RECORD',
              'stream': 'dummy_stream',
              'record': {
                'c_pk': 1,
                'column_1': None,                   # should be SET-NULL transformed
                'column_2': 'c584d22683f3e523',     # Should be HASH transformed
                'column_3': 'Ducd571661edac8d',     # Should be HASH-SKIP-2 tranformed
                'column_4': 'Dum1fe9627d907b0',     # Should be HASH-SKIP-3 tranformed
                'column_5': '2019-01-01T12:12:45',  # Should be MASK-DATE transformed
                'column_6': 0,                      # Should be MASK-NUMBER transformed
                'column_7': 'Dummy row 1',          # Should be the originl value - Unknown transformation type
                'column_8': '2019-12-21T12:12:45',  # Should be the original date-time value
                'column_9': 100,                    # Should be the original number value

                # Conditional transformation
                'column_10': 'column_11 is safe to keep',
                'column_11': 'My name is John',
              },
              'version': 1,
              'time_extracted': '2019-01-31T15:51:50.215998Z'
            }
        )

        # Third message is a RECORD message with transformed values 
        self.assertEquals(
            singer_output_messages[3],
            {
              'type': 'RECORD',
              'stream': 'dummy_stream',
              'record': {
                'c_pk': 2,
                'column_1': None,                   # should be SET-NULL transformed
                'column_2': '12c7ca803f4ae404',     # Should be HASH tranformed
                'column_3': 'Du7c2717bbc7489d',     # Should be HASH-SKIP-3 tranformed
                'column_4': 'Dum5b2be872199a8',     # Should be HASH-SKIP-3 tranformed
                'column_5': '2019-01-01T13:12:45',  # Should be MASK-DATE transformed
                'column_6': 0,                      # Should be MASK-NUMBER transformed
                'column_7': 'Dummy row 2',          # Should be the origian value - Unknown transformation type
                'column_8': '2019-12-21T13:12:45',  # Should be the original date-time value
                'column_9': 200,                    # Should be the original number value

                # Conditional transformation
                'column_10': 'column_11 has sensitive data. Needs to transform to NULL',
                'column_11': None,                  # Should be SET-NULL transformed
              },
              'version': 1,
              'time_extracted': '2019-01-31T15:51:50.215998Z'
            }
        )
