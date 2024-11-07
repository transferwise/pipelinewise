import os
import unittest
import botocore.exceptions
import boto3

from tap_s3_csv import s3

from .utils import get_test_config


class TestS3Connection(unittest.TestCase):
    """
    Integration Tests
    """
    maxDiff = None

    def setUp(self):
        self.config = get_test_config()

    def assertListFiles(self):
        s3_client = boto3.client('s3', endpoint_url=self.config.get('aws_endpoint_url'))
        files = list(s3_client.list_objects_v2(Bucket=self.config['bucket']))
        self.assertTrue(isinstance(files, list))

    def test_credentials(self):
        """Test connecting to S3 with credentials explicitly defined in config"""
        s3.setup_aws_client(self.config)
        self.assertListFiles()

    def test_credentials_aws_env_vars(self):
        """Test connecting to S3 with credentials defined in AWS environment variables
        rather than explicitly provided access keys"""

        # Save original config to restore later
        orig_config = self.config.copy()
        try:
            # Move aws access key and secret from config into environment variables
            os.environ['AWS_ACCESS_KEY_ID'] = self.config['aws_access_key_id']
            os.environ['AWS_SECRET_ACCESS_KEY'] = self.config['aws_secret_access_key']
            del self.config['aws_access_key_id']
            del self.config['aws_secret_access_key']

            # Create a new S3 client using env vars
            s3.setup_aws_client(self.config)
            self.assertListFiles()

        # Restore the original state to not confuse other tests
        finally:
            del os.environ['AWS_ACCESS_KEY_ID']
            del os.environ['AWS_SECRET_ACCESS_KEY']
            self.config = orig_config.copy()

    def test_profile_based_auth(self):
        """Test AWS profile based authentication rather than access keys"""
        # Save original config to restore later
        orig_config = self.config.copy()

        try:
            # Remove access keys from config and add profile name
            del self.config['aws_access_key_id']
            del self.config['aws_secret_access_key']
            self.config['aws_profile'] = 'fake-profile'

            # Create a new S3 client using env vars
            with self.assertRaises(botocore.exceptions.ProfileNotFound):
                s3.setup_aws_client(self.config)

        # Restore the original state to not confuse other tests
        finally:
            self.config = orig_config.copy()

    def test_profile_based_auth_aws_env_vars(self):
        """Test AWS profile based authentication using AWS environment variables"""

        # Save original config to restore later
        orig_config = self.config.copy()
        try:
            # Remove access keys from config and add profile name
            del self.config['aws_access_key_id']
            del self.config['aws_secret_access_key']

            # Profile name defined as env var and config is empty
            os.environ['AWS_PROFILE'] = 'fake_profile'

            # Create a new S3 client using env vars
            with self.assertRaises(botocore.exceptions.ProfileNotFound):
                s3.setup_aws_client(self.config)

        # Restore the original state to not confuse other tests
        finally:
            del os.environ['AWS_PROFILE']
            self.config = orig_config.copy()


if __name__ == '__main__':
    unittest.main()
