import contextlib
import io
import json
import unittest

from unittest.mock import patch, Mock

import pytest
from botocore.client import BaseClient

from target_s3_csv import emit_state, persist_messages


class TestMain(unittest.TestCase):

    def setUp(self) -> None:
        self.config = {
            's3_bucket': 'my-awesome-bucket',
        }

    def test_emit_state_with_None_does_nothing(self):
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            emit_state(None)
            self.assertEqual('', f.getvalue())

    def test_emit_state_with_dictionary_makes_dump(self):
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            emit_state({'a': 1, 'b': 2, 'c': 'lool'})
            self.assertEqual('{"a": 1, "b": 2, "c": "lool"}\n', f.getvalue())

    @patch('target_s3_csv.open')
    @patch('target_s3_csv.csv')
    @patch('target_s3_csv.s3')
    @patch('target_s3_csv.os')
    def test_persist_messages(self, os, s3, csv, open):
        messages = [
            json.dumps({"type": "SCHEMA", "stream": "my_stream",
                        "schema": {
                            "properties": {
                                "id": {"type": "integer"},
                                "name": {"type": ["string", "null"]},
                                "age": {"type": ["integer", "null"]},
                            },
                        },
                        "key_properties": ["id"],
                        "metadata": {}
            }),
            json.dumps({"type": "STATE", "stream": "my_stream", "value": {"bookmarks": {"my_stream": 1}}}),
            json.dumps({"type": "RECORD", "stream": "my_stream", "record": {"id": 1, "name": "Steve", "age": 10}}),
            json.dumps({"type": "RECORD", "stream": "my_stream", "record": {"id": 2, "name": "Peter", "age": 33}}),
            json.dumps({"type": "RECORD", "stream": "my_stream", "record": {"id": 3, "name": "Pete", "age": 25}}),
            json.dumps({"type": "RECORD", "stream": "my_stream", "record": {"id": 4, "name": "John", "age": 40}}),
        ]

        s3_client = Mock(spec_set=BaseClient)

        state = persist_messages(messages, self.config, s3_client)

        self.assertDictEqual({"bookmarks": {"my_stream": 1}}, state)
        s3.upload_files.assert_called_once()
