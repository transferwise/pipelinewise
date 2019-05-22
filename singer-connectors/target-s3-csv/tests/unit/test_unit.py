import unittest
from nose.tools import assert_raises

import target_s3_csv


class TestUnit(unittest.TestCase):
    """
    Unit Tests
    """
    @classmethod
    def setUp(self):
        self.config = {}


    def test_config_validation(self):
        """Test configuration validator"""
        validator = target_s3_csv.utils.validate_config
        empty_config = {}
        minimal_config = {
            'aws_access_key_id':        "dummy-value",
            'aws_secret_access_key':    "dummy-value",
            's3_bucket':                "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors >= 0)
        self.assertGreater(len(validator(empty_config)),  0)

        # Minimal configuratino should pass - (nr_of_errors == 0)
        self.assertEqual(len(validator(minimal_config)), 0)

