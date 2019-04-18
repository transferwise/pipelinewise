import itertools
import re
import backoff
import boto3
import singer

from botocore.exceptions import ClientError
from singer_encodings import csv
from tap_s3_csv import conversion

LOGGER = singer.get_logger()

SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"


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


def get_sampled_schema_for_table(config, table_spec):
    LOGGER.info('Sampling records to determine table schema.')

    s3_files_gen = get_input_files_for_table(config)

    samples = [sample for sample in sample_files(config, table_spec, s3_files_gen)]

    if not samples:
        return {}

    metadata_schema = {
        SDC_SOURCE_BUCKET_COLUMN: {'type': 'string'},
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        csv.SDC_EXTRA_COLUMN: {'type': 'array', 'items': {'type': 'string'}},
    }

    data_schema = conversion.generate_schema(samples, table_spec)

    return {
        'type': 'object',
        'properties': merge_dicts(data_schema, metadata_schema)
    }


def merge_dicts(first, second):
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


def sample_file(config, table_spec, s3_path, sample_rate):
    file_handle = get_file_handle(config, s3_path)
    iterator = csv.get_row_iterator(file_handle._raw_stream, table_spec) #pylint:disable=protected-access

    current_row = 0

    sampled_row_count = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            if row.get(csv.SDC_EXTRA_COLUMN):
                row.pop(csv.SDC_EXTRA_COLUMN)
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
def sample_files(config, table_spec, s3_files,
                 sample_rate=5, max_records=1000, max_files=5):
    LOGGER.info("Sampling files (max files: %s)", max_files)
    for s3_file in itertools.islice(s3_files, max_files):
        LOGGER.info('Sampling %s (max records: %s, sample rate: %s)',
                    s3_file['key'],
                    max_records,
                    sample_rate)
        yield from itertools.islice(sample_file(config, table_spec, s3_file['key'], sample_rate), max_records)


def get_input_files_for_table(config, modified_since=None):
    # TODO! rename this to be more accurate

    bucket = config['bucket']
    to_return = []

    pattern = config['file_extension']
    try:
        matcher = re.compile(pattern)
    except re.error as e:
        raise ValueError(
            ("File extension `{}` for bucket `{}` is not a valid regular "
             "expression. See "
             "https://docs.python.org/3.5/library/re.html#regular-expression-syntax").format(config['file_extension'], config['bucket']),
            pattern) from e

    LOGGER.info(
        'Checking bucket "%s" for keys matching "%s"', bucket, pattern)

    matched_files_count = 0
    unmatched_files_count = 0
    max_files_before_log = 30000
    for s3_object in list_files_in_bucket(bucket, config.get('search_prefix')):
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
                yield {'key': key, 'last_modified': last_modified, 'key_properties': ''}
                # TODO! fix key_properties, get it from config
        else:
            unmatched_files_count += 1

        if (unmatched_files_count + matched_files_count) % max_files_before_log == 0:
            # Are we skipping greater than 50% of the files?
            if 0.5 < (unmatched_files_count / (matched_files_count + unmatched_files_count)):
                LOGGER.warn(("Found %s matching files and %s non-matching files. "
                             "You should consider adding a `search_prefix` to the config "
                             "or removing non-matching files from the bucket."),
                            matched_files_count, unmatched_files_count)
            else:
                LOGGER.info("Found %s matching files and %s non-matching files",
                            matched_files_count, unmatched_files_count)

    if 0 == matched_files_count:
        raise Exception("No files found matching pattern {}".format(pattern))


@retry_pattern()
def list_files_in_bucket(bucket, search_prefix=None):
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
    pages = 0
    for page in paginator.paginate(**args):
        pages += 1
        LOGGER.debug("On page %s", pages)
        s3_object_count += len(page['Contents'])
        yield from page['Contents']

    if 0 < s3_object_count:
        LOGGER.info("Found %s files.", s3_object_count)
    else:
        LOGGER.warning('Found no files for bucket "%s" that match prefix "%s"', bucket, search_prefix)


@retry_pattern()
def get_file_handle(config, s3_path):
    bucket = config['bucket']
    s3_client = boto3.resource('s3')

    s3_bucket = s3_client.Bucket(bucket)
    s3_object = s3_bucket.Object(s3_path)
    return s3_object.get()['Body']
