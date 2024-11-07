"""
S3 Upload Client
"""
import os
import boto3
import datetime

from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.storage_client import SnowflakeFileEncryptionMaterial

from .base_upload_client import BaseUploadClient


class S3UploadClient(BaseUploadClient):
    """S3 Upload Client class"""

    def __init__(self, connection_config):
        super().__init__(connection_config)
        self.s3_client = self._create_s3_client()

    def _create_s3_client(self, config=None):
        if not config:
            config = self.connection_config

        # Get the required parameters from config file and/or environment variables
        aws_profile = config.get('aws_profile') or os.environ.get('AWS_PROFILE')
        aws_access_key_id = config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_session_token = config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token
            )
        # AWS Profile based authentication
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)

        # Create the s3 client
        return aws_session.client('s3',
                                  region_name=config.get('s3_region_name'),
                                  endpoint_url=config.get('s3_endpoint_url'))

    def upload_file(self, file, stream, temp_dir=None):
        """Upload file to an external snowflake stage on s3"""
        # Generating key in S3 bucket
        bucket = self.connection_config['s3_bucket']
        s3_acl = self.connection_config.get('s3_acl')
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")

        s3_key = f"{s3_key_prefix}pipelinewise_{stream}_{timestamp}_{os.path.basename(file)}"
        self.logger.info('Target S3 bucket: %s, local file: %s, S3 key: %s', bucket, file, s3_key)

        # Encrypt csv if client side encryption enabled
        master_key = self.connection_config.get('client_side_encryption_master_key', '')
        if master_key != '':
            # Encrypt the file
            encryption_material = SnowflakeFileEncryptionMaterial(
                query_stage_master_key=master_key,
                query_id='',
                smk_id=0
            )
            encryption_metadata, encrypted_file = SnowflakeEncryptionUtil.encrypt_file(
                encryption_material,
                file,
                tmp_dir=temp_dir
            )

            # Upload to s3
            extra_args = {'ACL': s3_acl} if s3_acl else {}

            # Send key and iv in the metadata, that will be required to decrypt and upload the encrypted file
            extra_args['Metadata'] = {
                'x-amz-key': encryption_metadata.key,
                'x-amz-iv': encryption_metadata.iv
            }
            self.s3_client.upload_file(encrypted_file, bucket, s3_key, ExtraArgs=extra_args)

            # Remove the uploaded encrypted file
            os.remove(encrypted_file)

        # Upload to S3 without encrypting
        else:
            extra_args = {'ACL': s3_acl} if s3_acl else None
            self.s3_client.upload_file(file, bucket, s3_key, ExtraArgs=extra_args)

        return s3_key

    def delete_object(self, stream: str, key: str) -> None:
        """Delete object from an external snowflake stage on S3"""
        self.logger.info('Deleting %s from external snowflake stage on S3', key)
        bucket = self.connection_config['s3_bucket']
        self.s3_client.delete_object(Bucket=bucket, Key=key)

    def copy_object(self, copy_source: str, target_bucket: str, target_key: str, target_metadata: dict) -> None:
        """Copy object to another location on S3"""
        self.logger.info('Copying %s to %s/%s', copy_source, target_bucket, target_key)
        source_bucket, source_key = copy_source.split("/", 1)
        metadata = self.s3_client.head_object(Bucket=source_bucket, Key=source_key).get('Metadata', {})
        metadata.update(target_metadata)
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy_object
        self.s3_client.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=target_key,
                                   Metadata=metadata, MetadataDirective="REPLACE")
