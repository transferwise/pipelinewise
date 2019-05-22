#!/usr/bin/env python3

import os
import backoff
import boto3
import singer

from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    CredentialResolver,
    DeferredRefreshableCredentials,
    JSONFileCache
)
from botocore.exceptions import ClientError
from botocore.session import Session

LOGGER = singer.get_logger()


def retry_pattern():
    return backoff.on_exception(backoff.expo,
                                ClientError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


@retry_pattern()
def setup_aws_client(config):
    aws_access_key_id = config['aws_access_key_id']
    aws_secret_access_key = config['aws_secret_access_key']

    LOGGER.info("Attempting to create AWS session")
    boto3.setup_default_session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)

@retry_pattern()
def upload_file(filename, bucket, key_prefix):
    s3_client = boto3.client('s3')
    s3_key = "{}{}".format(key_prefix, os.path.basename(filename))

    LOGGER.info("Uploading {} to bucket {} at {}".format(filename, bucket, s3_key))
    s3_client.upload_file(filename, bucket, s3_key)

