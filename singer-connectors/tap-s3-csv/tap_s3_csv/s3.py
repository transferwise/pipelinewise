import re

import boto3
from tap_s3_csv.logger import LOGGER as logger

import tap_s3_csv.format_handler


def sample_file(config, table_spec, s3_path, sample_rate, max_records):
    logger.info('Sampling {} ({} records, every {}th record).'
                .format(s3_path, max_records, sample_rate))

    samples = []

    iterator = tap_s3_csv.format_handler.get_row_iterator(
        config, table_spec, s3_path)

    current_row = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            samples.append(row)

        current_row += 1

        if len(samples) >= max_records:
            break

    logger.info('Sampled {} records.'.format(len(samples)))

    return samples


def sample_files(config, table_spec, s3_files,
                 sample_rate=10, max_records=1000, max_files=5):
    to_return = []

    files_so_far = 0

    for s3_file in s3_files:
        to_return += sample_file(config, table_spec, s3_file['key'],
                                 sample_rate, max_records)

        files_so_far += 1

        if files_so_far >= max_files:
            break

    return to_return


def get_input_files_for_table(config, table_spec, modified_since=None):
    bucket = config['bucket']

    to_return = []
    pattern = table_spec['pattern']
    matcher = re.compile(pattern)

    logger.debug(
        'Checking bucket "{}" for keys matching "{}"'
        .format(bucket, pattern))

    s3_objects = list_files_in_bucket(
        config, bucket, table_spec.get('search_prefix'))

    for s3_object in s3_objects:
        key = s3_object['Key']
        last_modified = s3_object['LastModified']

        logger.debug('Last modified: {}'.format(last_modified))

        if(matcher.search(key) and
           (modified_since is None or modified_since < last_modified)):
            logger.debug('Will download key "{}"'.format(key))
            to_return.append({'key': key, 'last_modified': last_modified})
        else:
            logger.debug('Will not download key "{}"'.format(key))

    to_return = sorted(to_return, key=lambda item: item['last_modified'])

    return to_return


def list_files_in_bucket(config, bucket, search_prefix=None):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=config['aws_access_key_id'],
        aws_secret_access_key=config['aws_secret_access_key'])

    s3_objects = []

    max_results = 1000
    args = {
        'Bucket': bucket,
        'MaxKeys': max_results,
    }

    if search_prefix is not None:
        args['Prefix'] = search_prefix

    result = s3_client.list_objects_v2(**args)

    s3_objects += result['Contents']
    next_continuation_token = result.get('NextContinuationToken')

    while next_continuation_token is not None:
        logger.debug('Continuing pagination with token "{}".'
                     .format(next_continuation_token))

        continuation_args = args.copy()
        continuation_args['ContinuationToken'] = next_continuation_token

        result = s3_client.list_objects_v2(**continuation_args)

        s3_objects += result['Contents']
        next_continuation_token = result.get('NextContinuationToken')

    logger.info("Found {} files.".format(len(s3_objects)))

    return s3_objects
