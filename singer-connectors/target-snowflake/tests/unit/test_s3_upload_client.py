import os
import unittest
from unittest.mock import patch, MagicMock

from target_snowflake.upload_clients.s3_upload_client import S3UploadClient


class TestS3UploadClient(unittest.TestCase):

    def _get_base_config(self):
        return {
            's3_bucket': 'test-bucket',
            's3_key_prefix': 'snowflake-imports/',
        }

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_upload_without_encryption(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = self._get_base_config()
        client = S3UploadClient(config)
        client.upload_file('/tmp/testfile.csv', 'my_stream', temp_dir='/tmp')

        mock_s3.upload_file.assert_called_once()
        call_kwargs = mock_s3.upload_file.call_args
        assert call_kwargs[1]['ExtraArgs'] is None or 'ServerSideEncryption' not in call_kwargs[1].get('ExtraArgs', {})

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_upload_with_sse_kms(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = self._get_base_config()
        config['s3_server_side_encryption_kms_key_id'] = 'arn:aws:kms:eu-central-1:123456789:key/test-key-id'
        client = S3UploadClient(config)
        client.upload_file('/tmp/testfile.csv', 'my_stream', temp_dir='/tmp')

        mock_s3.upload_file.assert_called_once()
        extra_args = mock_s3.upload_file.call_args[1]['ExtraArgs']
        assert extra_args['ServerSideEncryption'] == 'aws:kms'
        assert extra_args['SSEKMSKeyId'] == 'arn:aws:kms:eu-central-1:123456789:key/test-key-id'

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_upload_with_sse_kms_and_acl(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = self._get_base_config()
        config['s3_server_side_encryption_kms_key_id'] = 'test-key-id'
        config['s3_acl'] = 'bucket-owner-full-control'
        client = S3UploadClient(config)
        client.upload_file('/tmp/testfile.csv', 'my_stream', temp_dir='/tmp')

        mock_s3.upload_file.assert_called_once()
        extra_args = mock_s3.upload_file.call_args[1]['ExtraArgs']
        assert extra_args['ServerSideEncryption'] == 'aws:kms'
        assert extra_args['SSEKMSKeyId'] == 'test-key-id'
        assert extra_args['ACL'] == 'bucket-owner-full-control'

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_upload_with_client_side_encryption_ignores_sse_kms(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = self._get_base_config()
        config['client_side_encryption_master_key'] = 'dGVzdC1tYXN0ZXIta2V5LWJhc2U2NA=='
        config['s3_server_side_encryption_kms_key_id'] = 'test-key-id'
        client = S3UploadClient(config)

        with patch('target_snowflake.upload_clients.s3_upload_client.SnowflakeEncryptionUtil') as mock_enc:
            mock_enc.encrypt_file.return_value = (
                MagicMock(key='testkey', iv='testiv'),
                '/tmp/encrypted_file'
            )
            with patch('os.remove'):
                client.upload_file('/tmp/testfile.csv', 'my_stream', temp_dir='/tmp')

        mock_s3.upload_file.assert_called_once()
        extra_args = mock_s3.upload_file.call_args[1]['ExtraArgs']
        assert 'ServerSideEncryption' not in extra_args
        assert 'Metadata' in extra_args

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_instance_profile_auth_when_no_keys(self, mock_session_cls):
        config = self._get_base_config()
        S3UploadClient(config)

        mock_session_cls.assert_called_once_with(profile_name=None)


if __name__ == '__main__':
    unittest.main()
