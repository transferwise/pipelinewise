import unittest
import target_snowflake
import json

from nose.tools import assert_raises 


try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils


class TestLoad(unittest.TestCase):


    @classmethod
    def setUp(self):
        self.conn = test_utils.get_test_config()


    def test_invalid_json(self):
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with assert_raises(json.decoder.JSONDecodeError):
            target_snowflake.persist_lines(self.conn, tap_lines)
