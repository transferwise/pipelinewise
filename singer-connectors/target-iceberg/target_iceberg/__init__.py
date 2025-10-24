#!/usr/bin/env python3

import argparse
import io
import json
import logging
import sys
from typing import Dict, List

import singer
from joblib import Parallel, delayed

from target_iceberg.db_sync import DbSync

LOGGER = singer.get_logger('target_iceberg')

DEFAULT_BATCH_SIZE_ROWS = 100000
DEFAULT_PARALLELISM = 0  # Auto-detect
DEFAULT_MAX_PARALLELISM = 16


def add_metadata_columns_to_schema(schema_message: Dict) -> Dict:
    """Add metadata columns to the schema if not present"""
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = {
        'type': ['null', 'string'],
        'format': 'date-time'
    }
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = {
        'type': ['null', 'string'],
        'format': 'date-time'
    }
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = {
        'type': ['null', 'string'],
        'format': 'date-time'
    }

    return extended_schema_message


def emit_state(state: Dict) -> None:
    """Emit state to stdout"""
    if state is not None:
        line = json.dumps(state)
        LOGGER.info('Emitting state %s', line)
        sys.stdout.write(f'{line}\n')
        sys.stdout.flush()


def persist_lines(config: Dict, lines: List[str]) -> None:
    """Main loop to process Singer messages"""
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    # Get config parameters
    add_metadata_columns = config.get('add_metadata_columns', True)
    batch_size_rows = config.get('batch_size_rows', DEFAULT_BATCH_SIZE_ROWS)

    # Create DbSync instance
    db_sync = DbSync(config)

    # Process messages
    for line in lines:
        try:
            message = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error('Unable to parse message: %s', line)
            continue

        if isinstance(message, singer.RecordMessage):
            # Process record
            if message.stream not in schemas:
                raise Exception(
                    f'A record for stream {message.stream} was encountered '
                    'before a corresponding schema'
                )

            # Add metadata to record
            if add_metadata_columns:
                message.record['_sdc_extracted_at'] = message.time_extracted
                message.record['_sdc_batched_at'] = singer.utils.now()
                message.record['_sdc_deleted_at'] = message.record.get('_sdc_deleted_at')

            db_sync.process_record(
                message.stream,
                message.record,
                message.version
            )

            state = None

        elif isinstance(message, singer.SchemaMessage):
            # Process schema
            stream = message.stream
            validators[stream] = singer.get_validator(stream, message.schema)

            if add_metadata_columns:
                message = add_metadata_columns_to_schema(message.asdict())
                message = singer.SchemaMessage.from_dict(message)

            schemas[stream] = message.schema
            key_properties[stream] = message.key_properties

            # Create or update table
            db_sync.create_schema_if_not_exists(
                message.stream,
                message.schema,
                message.key_properties
            )

        elif isinstance(message, singer.StateMessage):
            # Process state
            LOGGER.debug('Setting state to %s', message.value)
            state = message.value

        elif isinstance(message, singer.ActivateVersionMessage):
            # Flush current stream
            pass

        else:
            LOGGER.warning('Unknown message type %s', message)

    # Flush any remaining records
    db_sync.flush_all_streams()

    # Emit final state
    emit_state(state)


def main() -> None:
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    args = parser.parse_args()

    # Load config
    if args.config:
        with open(args.config, encoding='utf-8') as config_file:
            config = json.load(config_file)
    else:
        config = {}

    # Setup logging
    logging.basicConfig(level=logging.INFO)

    LOGGER.info('Target Iceberg started')

    # Read from stdin
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    # Process messages
    persist_lines(config, input_messages)

    LOGGER.info('Target Iceberg finished')


if __name__ == '__main__':
    main()
