import os
import tempfile
import unittest
from unittest.mock import patch, Mock, call

from botocore.client import BaseClient

from target_s3_csv import s3


class TestS3(unittest.TestCase):

    @patch("target_s3_csv.s3.boto3.session.Session.client")
    def test_create_client(self, mock_client):
        """Test that if an endpoint_url is provided in the config, that it is used in client request"""
        config = {
            'aws_access_key_id': 'foo',
            'aws_secret_access_key': 'bar',
            'aws_endpoint_url': 'other_url'
        }
        s3.create_client(config)
        mock_client.assert_called_with('s3', endpoint_url='other_url')

    def test_upload_files_with_no_compression_nor_encryption(self):
        file1 = tempfile.NamedTemporaryFile(suffix='.csv')
        file2 = tempfile.NamedTemporaryFile(suffix='.csv')
        file3 = tempfile.NamedTemporaryFile(suffix='.csv')

        filenames = [
            {'filename': file1.name, 'target_key': 'folder1/file.csv'},
            {'filename': file2.name, 'target_key': 'folder2/file.csv'},
            {'filename': file3.name, 'target_key': 'folder3/file.csv'},
        ]

        s3_client = Mock(**{
            'upload_file.return_value': None
        })

        s3.upload_files(
            filenames,
            s3_client,
            'my_bucket',
            None,
            None,
            None
        )

        # make sure the uploading to s3 has been called once for each file
        s3_client.upload_file.assert_has_calls(
            [
                call(file1.name, 'my_bucket', 'folder1/file.csv', ExtraArgs=None),
                call(file2.name, 'my_bucket', 'folder2/file.csv', ExtraArgs=None),
                call(file3.name, 'my_bucket', 'folder3/file.csv', ExtraArgs=None),
            ]
        )

        # make sure that the upload_files function removed the files
        self.assertFalse(os.path.exists(file1.name))
        self.assertFalse(os.path.exists(file2.name))
        self.assertFalse(os.path.exists(file3.name))

    def test_upload_files_with_compression_and_no_encryption(self):
        file1 = tempfile.NamedTemporaryFile(suffix='.csv')
        file2 = tempfile.NamedTemporaryFile(suffix='.csv')
        file3 = tempfile.NamedTemporaryFile(suffix='.csv')

        filenames = [
            {'filename': file1.name, 'target_key': 'folder1/file.csv'},
            {'filename': file2.name, 'target_key': 'folder2/file.csv'},
            {'filename': file3.name, 'target_key': 'folder3/file.csv'},
        ]

        s3_client = Mock(**{
            'upload_file.return_value': None
        })

        s3.upload_files(
            filenames,
            s3_client,
            'my_bucket',
            'gzip',
            None,
            None
        )

        # make sure the uploading to s3 has been called once for each file
        s3_client.upload_file.assert_has_calls(
            [
                call(f'{file1.name}.gz', 'my_bucket', 'folder1/file.csv.gz', ExtraArgs=None),
                call(f'{file2.name}.gz', 'my_bucket', 'folder2/file.csv.gz', ExtraArgs=None),
                call(f'{file3.name}.gz', 'my_bucket', 'folder3/file.csv.gz', ExtraArgs=None),
            ]
        )

        # make sure that the upload_files function removed the files
        self.assertFalse(os.path.exists(file1.name))
        self.assertFalse(os.path.exists(file2.name))
        self.assertFalse(os.path.exists(file3.name))

    def test_upload_files_with_no_compression_and_with_encryption(self):
        file1 = tempfile.NamedTemporaryFile(suffix='.csv')
        file2 = tempfile.NamedTemporaryFile(suffix='.csv')
        file3 = tempfile.NamedTemporaryFile(suffix='.csv')

        filenames = [
            {'filename': file1.name, 'target_key': 'folder1/file.csv'},
            {'filename': file2.name, 'target_key': 'folder2/file.csv'},
            {'filename': file3.name, 'target_key': 'folder3/file.csv'},
        ]

        s3_client = Mock(**{
            'upload_file.return_value': None
        })

        s3.upload_files(
            filenames,
            s3_client,
            'my_bucket',
            None,
            'kms',
            None
        )

        # make sure the uploading to s3 has been called once for each file
        s3_client.upload_file.assert_has_calls(
            [
                call(f'{file1.name}', 'my_bucket', 'folder1/file.csv', ExtraArgs={'ServerSideEncryption': 'aws:kms'}),
                call(f'{file2.name}', 'my_bucket', 'folder2/file.csv', ExtraArgs={'ServerSideEncryption': 'aws:kms'}),
                call(f'{file3.name}', 'my_bucket', 'folder3/file.csv', ExtraArgs={'ServerSideEncryption': 'aws:kms'}),
            ]
        )

        # make sure that the upload_files function removed the files
        self.assertFalse(os.path.exists(file1.name))
        self.assertFalse(os.path.exists(file2.name))
        self.assertFalse(os.path.exists(file3.name))
