"""
Tap S3 csv main script
"""

import sys
import ujson
import singer

from typing import Dict
from singer import metadata, get_logger
from tap_s3_csv.discover import discover_streams
from tap_s3_csv import s3
from tap_s3_csv.sync import sync_stream
from tap_s3_csv.config import CONFIG_CONTRACT

LOGGER = get_logger('tap_s3_csv')

REQUIRED_CONFIG_KEYS = ["start_date", "bucket"]


def do_discover(config: Dict) -> None:
    """
    Discovers the source by connecting to the it and collecting information about the given tables/streams,
    it dumps the information to stdout
    :param config: connection and streams information
    :return: nothing
    """
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    ujson.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(meta_data: Dict) -> bool:
    """
    Detects whether the stream is selected to be synced
    :param meta_data: stream metadata
    :return: True if selected, False otherwise
    """
    return meta_data.get((), {}).get('selected', False)


def do_sync(config: Dict, catalog: Dict, state: Dict) -> None:
    """
    Syncs every selected stream in the catalog and updates the state
    :param config: connection and streams information
    :param catalog: Streams catalog
    :param state: current state
    :return: Nothing
    """
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        table_spec = next(s for s in config['tables'] if s['table_name'] == stream_name)
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, table_spec, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')


@singer.utils.handle_top_exception(LOGGER)
def main() -> None:
    """
    Main function
    :return: None
    """
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    # Reassign the config tables to the validated object
    config['tables'] = CONFIG_CONTRACT(config.get('tables', {}))

    try:
        for _ in s3.list_files_in_bucket(config['bucket']):
            break
        LOGGER.warning("I have direct access to the bucket without assuming the configured role.")
    except Exception:
        s3.setup_aws_client(config)

    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(config, args.properties, args.state)


if __name__ == '__main__':
    main()
