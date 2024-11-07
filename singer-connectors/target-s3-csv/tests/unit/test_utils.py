import unittest
from unittest.mock import patch

from target_s3_csv import utils


class TestUtils(unittest.TestCase):
    """
    Unit Tests for utils module
    """

    def test_config_validation(self):
        """Test configuration validator"""
        empty_config = {}
        minimal_config = {
            'aws_access_key_id': "dummy-value",
            'aws_secret_access_key': "dummy-value",
            's3_bucket': "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors >= 0)
        self.assertGreater(len(utils.validate_config(empty_config)), 0)

        # Minimal configuration should pass - (nr_of_errors == 0)
        self.assertEqual(len(utils.validate_config(minimal_config)), 0)

    def test_naming_convention_replaces_tokens(self):
        """Test that the naming_convention tokens are replaced"""
        message = {
            'stream': 'the_stream'
        }
        timestamp = 'fake_timestamp'
        s3_key = utils.get_target_key(message,
                                      timestamp=timestamp,
                                      naming_convention='test_{stream}_{timestamp}_test.csv')

        self.assertEqual('test_the_stream_fake_timestamp_test.csv', s3_key)

    def test_naming_convention_has_reasonable_default(self):
        """Test the default value of the naming convention"""
        message = {
            'stream': 'the_stream'
        }
        s3_key = utils.get_target_key(message)

        # default is "{stream}-{timestamp}.csv"
        self.assertTrue(s3_key.startswith('the_stream'))
        self.assertTrue(s3_key.endswith('.csv'))

    def test_naming_convention_honors_prefix(self):
        """Test that if the prefix is set in the config, that it is used in the s3 key"""
        message = {
            'stream': 'the_stream'
        }
        s3_key = utils.get_target_key(message, prefix='the_prefix__',
                                      naming_convention='folder1/test_{stream}_test.csv')

        self.assertEqual('folder1/the_prefix__test_the_stream_test.csv', s3_key)
