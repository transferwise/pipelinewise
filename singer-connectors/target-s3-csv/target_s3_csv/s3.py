#!/usr/bin/env python3
import gzip
import os
import shutil
import backoff
import boto3
import singer

from typing import Optional, Tuple, List, Dict, Iterator
from botocore.client import BaseClient
from botocore.exceptions import ClientError

LOGGER = singer.get_logger('target_s3_csv')


def retry_pattern():
    return backoff.on_exception(backoff.expo,
                                ClientError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


@retry_pattern()
def create_client(config):
    LOGGER.info("Attempting to create AWS session")

    # Get the required parameters from config file and/or environment variables
    aws_access_key_id = config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_session_token = config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')
    aws_profile = config.get('aws_profile') or os.environ.get('AWS_PROFILE')
    aws_endpoint_url = config.get('aws_endpoint_url')

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
    if aws_endpoint_url:
        s3 = aws_session.client('s3', endpoint_url=aws_endpoint_url)
    else:
        s3 = aws_session.client('s3')
    return s3


# pylint: disable=too-many-arguments
@retry_pattern()
def upload_file(filename, s3_client, bucket, s3_key,
                encryption_type=None, encryption_key=None):

    if encryption_type is None or encryption_type.lower() == "none":
        # No encryption config (defaults to settings on the bucket):
        encryption_desc = ""
        encryption_args = None
    else:
        if encryption_type.lower() == "kms":
            encryption_args = {"ServerSideEncryption": "aws:kms"}
            if encryption_key:
                encryption_desc = (
                    " using KMS encryption key ID '{}'"
                    .format(encryption_key)
                )
                encryption_args["SSEKMSKeyId"] = encryption_key
            else:
                encryption_desc = " using default KMS encryption"
        else:
            raise NotImplementedError(
                "Encryption type '{}' is not supported. "
                "Expected: 'none' or 'KMS'"
                .format(encryption_type)
            )
    LOGGER.info(
        "Uploading {} to bucket {} at {}{}"
        .format(filename, bucket, s3_key, encryption_desc)
    )
    s3_client.upload_file(filename, bucket, s3_key, ExtraArgs=encryption_args)


def upload_files(filenames: Iterator[Dict],
                 s3_client: BaseClient,
                 s3_bucket: str,
                 compression: Optional[str],
                 encryption_type: Optional[str],
                 encryption_key: Optional[str]):
    """
    Uploads given local files to s3
    Compress if necessary
    """
    for file in filenames:
        filename, target_key = file['filename'], file['target_key']
        compressed_file = None

        if compression is not None and compression.lower() != "none":
            if compression == "gzip":
                compressed_file = f"{filename}.gz"
                target_key = f'{target_key}.gz'

                with open(filename, 'rb') as f_in:
                    with gzip.open(compressed_file, 'wb') as f_out:
                        LOGGER.info(f"Compressing file as '%s'", compressed_file)
                        shutil.copyfileobj(f_in, f_out)

            else:
                raise NotImplementedError(
                    "Compression type '{}' is not supported. Expected: 'none' or 'gzip'".format(compression)
                )

        upload_file(compressed_file or filename,
                    s3_client,
                    s3_bucket,
                    target_key,
                    encryption_type=encryption_type,
                    encryption_key=encryption_key
                    )

        # Remove the local file(s)
        if os.path.exists(filename):
            os.remove(filename)
            if compressed_file:
                os.remove(compressed_file)
