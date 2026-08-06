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
        config['encryption_type'] = 'KMS'
        config['encryption_key'] = 'arn:aws:kms:eu-central-1:123456789:key/test-key-id'
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
        config['encryption_type'] = 'KMS'
        config['encryption_key'] = 'test-key-id'
        config['s3_acl'] = 'bucket-owner-full-control'
        client = S3UploadClient(config)
        client.upload_file('/tmp/testfile.csv', 'my_stream', temp_dir='/tmp')

        mock_s3.upload_file.assert_called_once()
        extra_args = mock_s3.upload_file.call_args[1]['ExtraArgs']
        assert extra_args['ServerSideEncryption'] == 'aws:kms'
        assert extra_args['SSEKMSKeyId'] == 'test-key-id'
        assert extra_args['ACL'] == 'bucket-owner-full-control'

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_upload_with_client_side_encryption_only(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = self._get_base_config()
        config['client_side_encryption_master_key'] = 'dGVzdC1tYXN0ZXIta2V5LWJhc2U2NA=='
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
    def test_sse_kms_takes_precedence_over_client_side_encryption(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = self._get_base_config()
        config['client_side_encryption_master_key'] = 'dGVzdC1tYXN0ZXIta2V5LWJhc2U2NA=='
        config['encryption_type'] = 'KMS'
        config['encryption_key'] = 'test-key-id'
        client = S3UploadClient(config)

        with patch('target_snowflake.upload_clients.s3_upload_client.SnowflakeEncryptionUtil') as mock_enc:
            client.upload_file('/tmp/testfile.csv', 'my_stream', temp_dir='/tmp')

        # The file must be uploaded as-is, not client side encrypted
        mock_enc.encrypt_file.assert_not_called()
        mock_s3.upload_file.assert_called_once()
        assert mock_s3.upload_file.call_args[0][0] == '/tmp/testfile.csv'
        extra_args = mock_s3.upload_file.call_args[1]['ExtraArgs']
        assert extra_args['ServerSideEncryption'] == 'aws:kms'
        assert extra_args['SSEKMSKeyId'] == 'test-key-id'
        assert 'Metadata' not in extra_args

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_copy_object_with_sse_kms(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {'Metadata': {'existing': 'value'}}
        mock_session_cls.return_value.client.return_value = mock_s3

        config = self._get_base_config()
        config['encryption_type'] = 'KMS'
        config['encryption_key'] = 'test-key-id'
        client = S3UploadClient(config)
        client.copy_object('test-bucket/source_key', 'archive-bucket', 'archive_key', {'tap': 'my_tap'})

        mock_s3.copy_object.assert_called_once()
        call_kwargs = mock_s3.copy_object.call_args[1]
        assert call_kwargs['ServerSideEncryption'] == 'aws:kms'
        assert call_kwargs['SSEKMSKeyId'] == 'test-key-id'
        assert call_kwargs['Metadata'] == {'existing': 'value', 'tap': 'my_tap'}

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_copy_object_without_sse_kms(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {'Metadata': {}}
        mock_session_cls.return_value.client.return_value = mock_s3

        client = S3UploadClient(self._get_base_config())
        client.copy_object('test-bucket/source_key', 'archive-bucket', 'archive_key', {'tap': 'my_tap'})

        mock_s3.copy_object.assert_called_once()
        call_kwargs = mock_s3.copy_object.call_args[1]
        assert 'ServerSideEncryption' not in call_kwargs
        assert 'SSEKMSKeyId' not in call_kwargs

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_upload_with_kms_and_no_key_uses_bucket_default(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = self._get_base_config()
        config['encryption_type'] = 'KMS'
        client = S3UploadClient(config)
        client.upload_file('/tmp/testfile.csv', 'my_stream', temp_dir='/tmp')

        extra_args = mock_s3.upload_file.call_args[1]['ExtraArgs']
        assert extra_args['ServerSideEncryption'] == 'aws:kms'
        assert 'SSEKMSKeyId' not in extra_args

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_upload_with_encryption_type_none(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = self._get_base_config()
        config['encryption_type'] = 'none'
        client = S3UploadClient(config)
        client.upload_file('/tmp/testfile.csv', 'my_stream', temp_dir='/tmp')

        assert mock_s3.upload_file.call_args[1]['ExtraArgs'] is None

    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_unsupported_encryption_type_raises(self, mock_session_cls):
        mock_session_cls.return_value.client.return_value = MagicMock()

        config = self._get_base_config()
        config['encryption_type'] = 'AES256'
        client = S3UploadClient(config)

        with self.assertRaises(NotImplementedError):
            client.upload_file('/tmp/testfile.csv', 'my_stream', temp_dir='/tmp')

    @patch.dict(os.environ, {}, clear=True)
    @patch('target_snowflake.upload_clients.s3_upload_client.boto3.session.Session')
    def test_instance_profile_auth_when_no_keys(self, mock_session_cls):
        config = self._get_base_config()
        S3UploadClient(config)

        mock_session_cls.assert_called_once_with(profile_name=None)


if __name__ == '__main__':
    unittest.main()
