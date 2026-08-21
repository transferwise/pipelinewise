import unittest
from unittest.mock import MagicMock, patch

from target_snowflake.upload_clients.s3_upload_client import S3UploadClient


class TestS3UploadClient(unittest.TestCase):
    """Test S3 upload client behavior."""

    def create_client(self, config):
        """Create an S3 upload client with a mocked boto3 client."""
        with patch("boto3.session.Session") as session:
            s3_client = MagicMock()
            session.return_value.client.return_value = s3_client
            upload_client = S3UploadClient(config)

        return upload_client, s3_client

    def test_copy_object_without_acl(self):
        """Do not pass an ACL argument unless one is configured."""
        upload_client, s3_client = self.create_client({})
        s3_client.head_object.return_value = {"Metadata": {"x-amz-key": "key"}}

        upload_client.copy_object("source-bucket/source-key", "target-bucket", "target-key", {"x-amz-iv": "iv"})

        s3_client.copy_object.assert_called_once_with(
            CopySource="source-bucket/source-key",
            Bucket="target-bucket",
            Key="target-key",
            Metadata={"x-amz-key": "key", "x-amz-iv": "iv"},
            MetadataDirective="REPLACE",
        )

    def test_copy_object_with_s3_acl(self):
        """Pass the configured ACL through to S3 copy_object."""
        upload_client, s3_client = self.create_client({"s3_acl": "bucket-owner-full-control"})
        s3_client.head_object.return_value = {"Metadata": {"x-amz-key": "key"}}

        upload_client.copy_object("source-bucket/source-key", "target-bucket", "target-key", {"x-amz-iv": "iv"})

        s3_client.copy_object.assert_called_once_with(
            CopySource="source-bucket/source-key",
            Bucket="target-bucket",
            Key="target-key",
            Metadata={"x-amz-key": "key", "x-amz-iv": "iv"},
            MetadataDirective="REPLACE",
            ACL="bucket-owner-full-control",
        )
