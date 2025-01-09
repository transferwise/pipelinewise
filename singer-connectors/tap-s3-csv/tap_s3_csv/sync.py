"""
Syncing related functions
"""

import sys
import csv
from typing import Dict

from singer import metadata, Transformer, utils, get_bookmark, write_bookmark, write_state, write_record, get_logger
from singer_encodings.csv import get_row_iterator # pylint:disable=no-name-in-module

from tap_s3_csv import s3

LOGGER = get_logger('tap_s3_csv')


def sync_stream(config: Dict, state: Dict, table_spec: Dict, stream: Dict) -> int:
    """
    Sync the stream
    :param config: Connection and stream config
    :param state: current state
    :param table_spec: table specs
    :param stream: stream
    :return: count of streamed records
    """
    table_name = table_spec['table_name']
    modified_since = utils.strptime_with_tz(get_bookmark(state, table_name, 'modified_since') or
                                            config['start_date'])

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    s3_files = s3.get_input_files_for_table(
        config, table_spec, modified_since)

    records_streamed = 0

    # We sort here so that tracking the modified_since bookmark makes
    # sense. This means that we can't sync s3 buckets that are larger than
    # we can sort in memory which is suboptimal. If we could bookmark
    # based on anything else then we could just sync files as we see them.
    for s3_file in sorted(s3_files, key=lambda item: item['last_modified']):
        records_streamed += sync_table_file(
            config, s3_file['key'], table_spec, stream)

        state = write_bookmark(state, table_name, 'modified_since', s3_file['last_modified'].isoformat())
        write_state(state)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed


def sync_table_file(config: Dict, s3_path: str, table_spec: Dict, stream: Dict) -> int:
    """
    Sync a given csv found file
    :param config: tap configuration
    :param s3_path: file path given by S3
    :param table_spec: tables specs
    :param stream: Stream data
    :return: number of streamed records
    """
    LOGGER.info('Syncing file "%s".', s3_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    s3_file_handle = s3.get_file_handle(config, s3_path)
    # We observed data who's field size exceeded the default maximum of
    # 131072. We believe the primary consequence of the following setting
    # is that a malformed, wide CSV would potentially parse into a single
    # large field rather than giving this error, but we also think the
    # chances of that are very small and at any rate the source data would
    # need to be fixed. The other consequence of this could be larger
    # memory consumption but that's acceptable as well.
    csv.field_size_limit(sys.maxsize)
    iterator = get_row_iterator(s3_file_handle._raw_stream, table_spec)  # pylint:disable=protected-access

    records_synced = 0

    for row in iterator:
        time_extracted = utils.now()

        custom_columns = {
            s3.SDC_SOURCE_BUCKET_COLUMN: bucket,
            s3.SDC_SOURCE_FILE_COLUMN: s3_path,

            # index zero, +1 for header row
            s3.SDC_SOURCE_LINENO_COLUMN: records_synced + 2
        }
        rec = {**row, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

        write_record(table_name, to_write, time_extracted=time_extracted)
        records_synced += 1

    return records_synced
