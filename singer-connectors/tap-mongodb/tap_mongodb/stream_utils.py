#!/usr/bin/env python3
"""
List of helper functions for streams only
"""

from typing import Dict, Optional, List, Tuple

import singer
from singer import metadata, write_message, SchemaMessage

from tap_mongodb.sync_strategies.common import calculate_destination_stream_name


def get_replication_method_from_stream(stream: Dict) -> Optional[str]:
    """
    Search for the stream replication method
    Args:
        stream: stream dictionary

    Returns: replication method if defined, None otherwise

    """
    md_map = metadata.to_map(stream['metadata'])
    return metadata.get(md_map, (), 'replication-method')


def is_log_based_stream(stream: Dict) -> bool:
    """
    checks if stream uses log based replication method
    Returns: True if LOG_BASED, False otherwise
    """
    return get_replication_method_from_stream(stream) == 'LOG_BASED'


def write_schema_message(stream: Dict):
    """
    Creates and writes a stream schema message to stdout
    Args:
        stream: stream catalog
    """
    write_message(SchemaMessage(
        stream=calculate_destination_stream_name(stream),
        schema=stream['schema'],
        key_properties=['_id']))


def is_stream_selected(stream: Dict) -> bool:
    """
    Checks the stream's metadata to see if stream is selected for sync
    Args:
        stream: stream dictionary

    Returns: True if selected, False otherwise

    """
    mdata = metadata.to_map(stream['metadata'])
    is_selected = metadata.get(mdata, (), 'selected')

    return is_selected is True


def streams_list_to_dict(streams: List[Dict]) -> Dict[str, Dict]:
    """
    converts the streams list to dictionary of streams where the keys are the tap stream ids
    Args:
        streams: stream list

    Returns: dictionary od streams

    """
    return {stream['tap_stream_id']: stream for stream in streams}


def filter_streams_by_replication_method(streams_to_sync: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Divides the list of streams into two lists: one of streams that use log based and the other that use
    traditional replication method, i.e either Full table or Incremental
    Args:
        streams_to_sync: List of streams selected to sync
    Returns: Tuple of two lists, first is log based streams and the second is list of traditional streams

    """
    log_based_streams = []
    non_log_based_streams = []

    for stream in streams_to_sync:
        if is_log_based_stream(stream):
            log_based_streams.append(stream)
        else:
            non_log_based_streams.append(stream)

    return log_based_streams, non_log_based_streams


def get_streams_to_sync(streams: List[Dict], state: Dict) -> List:
    """
    Filter the streams list to return only those selected for sync
    Args:
        streams: list of all discovered streams
        state: streams state

    Returns: list of selected streams, ordered from streams without state to those with state

    """
    # get selected streams
    selected_streams = [stream for stream in streams if is_stream_selected(stream)]

    # prioritize streams that have not been processed
    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        if state.get('bookmarks', {}).get(stream['tap_stream_id']):
            streams_with_state.append(stream)
        else:
            streams_without_state.append(stream)

    ordered_streams = streams_without_state + streams_with_state

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)

    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s['tap_stream_id'] == currently_syncing,
            ordered_streams))
        non_currently_syncing_streams = list(filter(lambda s: s['tap_stream_id'] != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        streams_to_sync = ordered_streams

    return streams_to_sync
