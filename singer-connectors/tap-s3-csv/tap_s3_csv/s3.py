"""
Modules containing all AWS S3 related features
"""
from __future__ import division

import os
import itertools
import more_itertools
import re
import backoff
import boto3

from botocore.exceptions import ClientError
from singer_encodings.csv import get_row_iterator, SDC_EXTRA_COLUMN  # pylint:disable=no-name-in-module
from singer import get_logger, utils
from typing import Dict, Generator, Optional, Iterator, List


LOGGER = get_logger('tap_s3_csv')

SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"


def retry_pattern():
    """
    Retry decorator to retry failed functions
    :return:
    """
    return backoff.on_exception(backoff.expo,
                                ClientError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    """
    For logging attempts to connect with Amazon
    :param details:
    :return:
    """
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


@retry_pattern()
def setup_aws_client(config: Dict) -> None:
    """
    Initialize a default AWS session
    :param config: connection config
    """
    LOGGER.info("Attempting to create AWS session")

    # Get the required parameters from config file and/or environment variables
    aws_access_key_id = config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_session_token = config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')
    aws_profile = config.get('aws_profile') or os.environ.get('AWS_PROFILE')

    # AWS credentials based authentication
    if aws_access_key_id and aws_secret_access_key:
        boto3.setup_default_session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )
    # AWS Profile based authentication
    else:
        boto3.setup_default_session(profile_name=aws_profile)


def get_sampled_schema_for_table(config: Dict, table_spec: Dict) -> Dict:
    """
    Detects json schema using a sample of table/stream data
    :param config: Tap config
    :param table_spec: tables specs
    :return: detected schema
    """
    LOGGER.info('Sampling records to determine table schema.')

    modified_since = utils.strptime_with_tz(config['start_date'])
    s3_files_gen = get_input_files_for_table(config, table_spec, modified_since)

    samples = list(sample_files(config, table_spec, s3_files_gen))

    if not samples:
        return {}

    metadata_schema = {
        SDC_SOURCE_BUCKET_COLUMN: {'type': 'string'},
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        SDC_EXTRA_COLUMN: {'type': 'array', 'items': {'type': 'string'}},
    }

    data_schema = generate_schema(samples, table_spec)

    return {
        'type': 'object',
        'properties': merge_dicts(data_schema, metadata_schema)
    }


def generate_schema(samples: List[Dict], table_spec: Dict) -> Dict:
    """
    Build json schema, all columns in the headers would be string.
    with format date-time if date_overrides has been configured for the table.

    :param samples: List of dictionaries containing samples data from csv file(s)
    :param table_spec: table/stream specs given in the tap definition
    :return: json schema dictionary representing  the table
    """
    schema = {}
    date_overrides = set(table_spec.get('date_overrides', []))

    for sample in samples:
        for header in sample.keys():
            schema[header] = {'type': ['null', 'string']}

            if header in date_overrides:
                schema[header]['format'] = 'date-time'

    return schema


def merge_dicts(first: Dict, second: Dict) -> Dict:
    """
    Merged two given dictionaries
    :param first: first dictionary
    :param second: second dictionary
    :return: merged dictionary
    """
    to_return = first.copy()

    for key in second:
        if key in first:
            if isinstance(first[key], dict) and isinstance(second[key], dict):
                to_return[key] = merge_dicts(first[key], second[key])
            else:
                to_return[key] = second[key]

        else:
            to_return[key] = second[key]

    return to_return


def sample_file(config: Dict, table_spec: Dict, s3_path: str, sample_rate: int) -> Generator:
    """
    Get a sample data from the given S3 file
    :param config:
    :param table_spec:
    :param s3_path:
    :param sample_rate:
    :return: generator containing the samples as dictionaries
    """
    file_handle = get_file_handle(config, s3_path)
    # _raw_stream seems like the wrong way to access this..
    iterator = get_row_iterator(file_handle._raw_stream, table_spec)  # pylint:disable=protected-access

    current_row = 0

    sampled_row_count = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            if row.get(SDC_EXTRA_COLUMN):
                row.pop(SDC_EXTRA_COLUMN)
            sampled_row_count += 1
            if (sampled_row_count % 200) == 0:
                LOGGER.info("Sampled %s rows from %s",
                            sampled_row_count, s3_path)
            yield row

        current_row += 1

    LOGGER.info("Sampled %s rows from %s",
                sampled_row_count,
                s3_path)


# pylint: disable=too-many-arguments
def sample_files(config: Dict, table_spec: Dict, s3_files: Generator,
                 sample_rate: int = 5, max_records: int = 1000, max_files: int = 5) -> Generator:
    """
    Get samples from all files
    :param config:
    :param table_spec:
    :param s3_files:
    :param sample_rate:
    :param max_records:
    :param max_files:
    :returns: Generator containing all samples as dicts
    """
    LOGGER.info("Sampling files (max files: %s)", max_files)
    for s3_file in more_itertools.tail(max_files, s3_files):
        LOGGER.info('Sampling %s (max records: %s, sample rate: %s)',
                    s3_file['key'],
                    max_records,
                    sample_rate)
        yield from itertools.islice(sample_file(config, table_spec, s3_file['key'], sample_rate), max_records)


def get_input_files_for_table(config: Dict, table_spec: Dict, modified_since: str = None) -> Generator:
    """
    Gets all files that match the search pattern in table specs and were modified after modified since
    :param config: tap config
    :param table_spec: table specs
    :param modified_since: string date
    :returns: generator containing all the found files
    """
    bucket = config['bucket']

    prefix = table_spec.get('search_prefix')
    pattern = table_spec['search_pattern']
    try:
        matcher = re.compile(pattern)
    except re.error as err:
        raise ValueError(
            (f"search_pattern for table `{table_spec['table_name']}` is not a valid regular "
             "expression. See https://docs.python.org/3.5/library/re.html#regular-expression-syntax"),
            pattern) from err

    LOGGER.info('Checking bucket "%s" for keys matching "%s"', bucket, pattern)
    LOGGER.info('Skipping files which have a LastModified value older than %s', modified_since)

    matched_files_count = 0
    unmatched_files_count = 0
    max_files_before_log = 30000
    for s3_object in sorted(list_files_in_bucket(bucket, prefix, aws_endpoint_url=config.get('aws_endpoint_url')),
                            key=lambda item: item['LastModified'], reverse=False):
        key = s3_object['Key']
        last_modified = s3_object['LastModified']

        if s3_object['Size'] == 0:
            LOGGER.info('Skipping matched file "%s" as it is empty', key)
            unmatched_files_count += 1
            continue

        if matcher.search(key):
            matched_files_count += 1
            if modified_since is None or modified_since < last_modified:
                LOGGER.info('Will download key "%s" as it was last modified %s',
                            key,
                            last_modified)
                yield {'key': key, 'last_modified': last_modified}
        else:
            unmatched_files_count += 1

        if (unmatched_files_count + matched_files_count) % max_files_before_log == 0:
            # Are we skipping greater than 50% of the files?
            if (unmatched_files_count / (matched_files_count + unmatched_files_count)) > 0.5:
                LOGGER.warning(("Found %s matching files and %s non-matching files. "
                                "You should consider adding a `search_prefix` to the config "
                                "or removing non-matching files from the bucket."),
                               matched_files_count, unmatched_files_count)
            else:
                LOGGER.info("Found %s matching files and %s non-matching files",
                            matched_files_count, unmatched_files_count)

    if matched_files_count == 0:
        if prefix:
            raise Exception(
                f'No files found in bucket "{bucket}" that matches prefix "{prefix}" and pattern "{pattern}"'
            )

        raise Exception(f'No files found in bucket "{bucket}" that matches pattern "{pattern}"')


@retry_pattern()
def list_files_in_bucket(bucket: str, search_prefix: str = None, aws_endpoint_url: Optional[str] = None) -> Generator:
    """
    Gets all files in the given S3 bucket that match the search prefix
    :param bucket: S3 bucket name
    :param search_prefix: search pattern
    :param aws_endpoint_url: optional aws url
    :returns: generator containing all found files
    """
    # override default endpoint for non aws s3 services
    if aws_endpoint_url is not None:
        s3_client = boto3.client('s3', endpoint_url=aws_endpoint_url)
    else:
        s3_client = boto3.client('s3')

    s3_object_count = 0

    max_results = 1000
    args = {
        'Bucket': bucket,
        'MaxKeys': max_results,
    }

    if search_prefix is not None:
        args['Prefix'] = search_prefix

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(**args)
    filtered_s3_objects = page_iterator.search("Contents[?StorageClass=='STANDARD']")

    for s3_obj in filtered_s3_objects:
        s3_object_count += 1
        yield s3_obj

    if s3_object_count > 0:
        LOGGER.info("Found %s files.", s3_object_count)
    else:
        LOGGER.warning('Found no files for bucket "%s" that match prefix "%s"', bucket, search_prefix)


@retry_pattern()
def get_file_handle(config: Dict, s3_path: str) -> Iterator:
    """
    Get a iterator of file located in the s3 path
    :param config: tap config
    :param s3_path: file path in S3
    :return: file Body iterator
    """
    bucket = config['bucket']
    aws_endpoint_url = config.get('aws_endpoint_url')

    # override default endpoint for non aws s3 services
    if aws_endpoint_url is not None:
        s3_client = boto3.resource('s3', endpoint_url=aws_endpoint_url)
    else:
        s3_client = boto3.resource('s3')

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    return s3_object.get()['Body']
