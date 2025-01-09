#!/usr/bin/env python3
import copy
import time
import pymongo
import singer

from typing import Optional, Dict
from pymongo.collection import Collection
from singer import utils

from tap_mongodb.sync_strategies import common

LOGGER = singer.get_logger('tap_mongodb')


def get_max_id_value(collection: Collection) -> Optional[str]:
    """
    Finds and returns the maximum ID in the collection if exists None otherwise
    Args:
        collection: MongoDB Collection instance

    Returns: Max ID or None

    """
    row = collection.find_one(sort=[("_id", pymongo.DESCENDING)])
    if row:
        return row['_id']

    LOGGER.info("No max id found for collection: collection is likely empty")
    return None


def sync_collection(collection: Collection, stream: Dict, state: Dict) -> None:
    """
    Sync collection records incrementally
    Args:
        collection: MongoDB collection instance
        stream: dictionary of all stream details
        state: the tap state
    """
    LOGGER.info('Starting full table sync for %s', stream['tap_stream_id'])

    # before writing the table version to state, check if we had one to begin with
    first_run = singer.get_bookmark(state, stream['tap_stream_id'], 'version') is None

    # last run was interrupted if there is a last_id_fetched bookmark
    # pick a new table version if last run wasn't interrupted
    if singer.get_bookmark(state, stream['tap_stream_id'], 'last_id_fetched') is not None:
        stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    else:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    activate_version_message = singer.ActivateVersionMessage(
        stream=common.calculate_destination_stream_name(stream),
        version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if first_run:
        singer.write_message(activate_version_message)

    if singer.get_bookmark(state, stream['tap_stream_id'], 'max_id_value'):
        # There is a bookmark
        max_id_value = common.string_to_class(singer.get_bookmark(state, stream['tap_stream_id'], 'max_id_value'),
                                              singer.get_bookmark(state, stream['tap_stream_id'], 'max_id_type'))
    else:
        max_id_value = get_max_id_value(collection)

    last_id_fetched = singer.get_bookmark(state,
                                          stream['tap_stream_id'],
                                          'last_id_fetched')

    if max_id_value:
        # Write the bookmark if max_id_value is defined
        state = singer.write_bookmark(state,
                                      stream['tap_stream_id'],
                                      'max_id_value',
                                      common.class_to_string(max_id_value,
                                                             max_id_value.__class__.__name__))
        state = singer.write_bookmark(state,
                                      stream['tap_stream_id'],
                                      'max_id_type',
                                      max_id_value.__class__.__name__)

    find_filter = {'$lte': max_id_value}
    if last_id_fetched:
        find_filter['$gte'] = common.string_to_class(last_id_fetched, singer.get_bookmark(state,
                                                                                          stream['tap_stream_id'],
                                                                                          'last_id_fetched_type'))

    LOGGER.info('Querying %s with: %s', stream['tap_stream_id'], dict(find=find_filter))

    with collection.find({'_id': find_filter},
                         sort=[("_id", pymongo.ASCENDING)]) as cursor:
        rows_saved = 0
        start_time = time.time()

        for row in cursor:
            rows_saved += 1

            singer.write_message(common.row_to_singer_record(stream=stream,
                                                             row=row,
                                                             time_extracted=utils.now(),
                                                             time_deleted=None,
                                                             version=stream_version))

            state = singer.write_bookmark(state,
                                          stream['tap_stream_id'],
                                          'last_id_fetched',
                                          common.class_to_string(row['_id'], row['_id'].__class__.__name__))
            state = singer.write_bookmark(state,
                                          stream['tap_stream_id'],
                                          'last_id_fetched_type',
                                          row['_id'].__class__.__name__)

            if rows_saved % common.UPDATE_BOOKMARK_PERIOD == 0:
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        common.COUNTS[stream['tap_stream_id']] += rows_saved
        common.TIMES[stream['tap_stream_id']] += time.time() - start_time

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, stream['tap_stream_id'], 'max_id_value')
    singer.clear_bookmark(state, stream['tap_stream_id'], 'max_id_type')
    singer.clear_bookmark(state, stream['tap_stream_id'], 'last_id_fetched')
    singer.clear_bookmark(state, stream['tap_stream_id'], 'last_id_fetched_type')

    singer.write_bookmark(state,
                          stream['tap_stream_id'],
                          'initial_full_table_complete',
                          True)

    singer.write_message(activate_version_message)

    LOGGER.info('Syncd %s records for %s', rows_saved, stream['tap_stream_id'])
