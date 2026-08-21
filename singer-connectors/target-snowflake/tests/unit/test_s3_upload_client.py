import unittest
from unittest.mock import MagicMock

from target_snowflake.upload_clients.s3_upload_client import S3UploadClient


class TestS3UploadClient(unittest.TestCase):
    def make_client(self, config):
        client = S3UploadClient.__new__(S3UploadClient)
        client.connection_config = config
        client.logger = MagicMock()
        client.s3_client = MagicMock()
        client.s3_client.head_object.return_value = {"Metadata": {"existing": "metadata"}}
        return client

    def test_copy_object_uses_configured_acl(self):
        client = self.make_client({"s3_acl": "bucket-owner-full-control"})

        client.copy_object("source-bucket/source-key", "target-bucket", "target-key", {"new": "metadata"})

        client.s3_client.copy_object.assert_called_once_with(
            ACL="bucket-owner-full-control",
            Bucket="target-bucket",
            CopySource="source-bucket/source-key",
            Key="target-key",
            Metadata={"existing": "metadata", "new": "metadata"},
            MetadataDirective="REPLACE",
        )

    def test_copy_object_omits_acl_by_default(self):
        client = self.make_client({})

        client.copy_object("source-bucket/source-key", "target-bucket", "target-key", {"new": "metadata"})

        self.assertNotIn("ACL", client.s3_client.copy_object.call_args.kwargs)
