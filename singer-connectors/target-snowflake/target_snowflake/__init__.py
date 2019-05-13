#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import time
import collections
from tempfile import NamedTemporaryFile
from decimal import Decimal
from joblib import Parallel, delayed, parallel_backend
import tempfile

import pkg_resources
from jsonschema import ValidationError, Draft4Validator, FormatChecker
import singer
from target_snowflake.db_sync import DbSync

logger = singer.get_logger()

def float_to_decimal(value):
    '''Walk the given data structure and turn all instances of float into
    double.'''
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value

def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = { 'type': ['null', 'string'], 'format': 'date-time' }
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = { 'type': ['null', 'string'] }
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = { 'type': ['null', 'string'], 'format': 'date-time' }
    extended_schema_message['schema']['properties']['_sdc_primary_key'] = {'type': ['null', 'string'] }
    extended_schema_message['schema']['properties']['_sdc_received_at'] = { 'type': ['null', 'string'], 'format': 'date-time' }
    extended_schema_message['schema']['properties']['_sdc_sequence'] = {'type': ['integer'] }
    extended_schema_message['schema']['properties']['_sdc_table_version'] = {'type': ['null', 'string'] }

    return extended_schema_message

def add_metadata_values_to_record(record_message, stream_to_sync):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    """
    extended_record = record_message['record']
    extended_record['_sdc_batched_at'] = datetime.now().isoformat()
    extended_record['_sdc_deleted_at'] = record_message.get('record', {}).get('_sdc_deleted_at')
    extended_record['_sdc_extracted_at'] = record_message.get('time_extracted')
    extended_record['_sdc_primary_key'] = stream_to_sync.stream_schema_message['key_properties']
    extended_record['_sdc_received_at'] = datetime.now().isoformat()
    extended_record['_sdc_sequence'] = int(round(time.time() * 1000))
    extended_record['_sdc_table_version'] = record_message.get('version')

    return extended_record

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def get_schema_names_from_config(config):
    default_target_schema = config.get('default_target_schema')
    schema_mapping = config.get('schema_mapping', {})
    schema_names = []

    if default_target_schema:
        schema_names.append(default_target_schema)

    if schema_mapping:
        for source_schema, target in schema_mapping.items():
            schema_names.append(target.get('target_schema'))

    return schema_names

# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    records_to_load = {}
    csv_files_to_load = {}
    row_count = {}
    stream_to_sync = {}
    batch_size_rows = config['batch_size_rows'] if 'batch_size_rows' in config else 100000
    table_columns_cache = None

    # Cache the available schemas, tables and columns from snowflake if not disabled in config
    # The cache will be used later use to avoid lot of small queries hitting snowflake
    if not ('disable_table_cache' in config and config['disable_table_cache'] == True):
        logger.info("Caching available catalog objects in snowflake...")
        filter_schemas = get_schema_names_from_config(config)
        table_columns_cache = DbSync(config).get_table_columns(filter_schemas=filter_schemas)

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            stream = o['stream']

            # Validate record
            try:
                validators[stream].validate(float_to_decimal(o['record']))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    logger.error("Data validation failed and cannot load to destination. RECORD: {}\n'multipleOf' validations that allows long precisions are not supported (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema."
                    .format(o['record']))
                    raise ex

            primary_key_string = stream_to_sync[stream].record_primary_key_string(o['record'])
            if not primary_key_string:
                primary_key_string = 'RID-{}'.format(row_count[stream])

            if stream not in records_to_load:
                records_to_load[stream] = {}

            if config.get('add_metadata_columns') or config.get('hard_delete'):
                records_to_load[stream][primary_key_string] = add_metadata_values_to_record(o, stream_to_sync[stream])
            else:
                records_to_load[stream][primary_key_string] = o['record']

            row_count[stream] = len(records_to_load[stream])

            if row_count[stream] >= batch_size_rows:
                flush_records(stream, records_to_load[stream], row_count[stream], stream_to_sync[stream])
                row_count[stream] = 0
                records_to_load[stream] = {}

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']

            schemas[stream] = o
            schema = float_to_decimal(o['schema'])
            validators[stream] = Draft4Validator(schema, format_checker=FormatChecker())

            # flush records from previous stream SCHEMA
            if row_count.get(stream, 0) > 0:
                flush_records(stream, records_to_load[stream], row_count[stream], stream_to_sync[stream])

            # key_properties key must be available in the SCHEMA message.
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")

            # Log based and Incremental replications on tables with no Primary Key
            # cause duplicates when merging UPDATE events.
            # Stop loading data by default if no Primary Key.
            #
            # If you want to load tables with no Primary Key:
            #  1) Set ` 'primary_key_required': false ` in the target-snowflake config.json
            #  or
            #  2) Use fastsync [postgres-to-snowflake, mysql-to-snowflake, etc.]
            if config.get('primary_key_required', True) and len(o['key_properties']) == 0:
                logger.critical("Primary key is set to mandatory but not defined in the [{}] stream".format(stream))
                raise Exception("key_properties field is required")

            key_properties[stream] = o['key_properties']

            if config.get('add_metadata_columns') or config.get('hard_delete'):
                stream_to_sync[stream] = DbSync(config, add_metadata_columns_to_schema(o))
            else:
                stream_to_sync[stream] = DbSync(config, o)

            stream_to_sync[stream].create_schema_if_not_exists(table_columns_cache)
            stream_to_sync[stream].sync_table(table_columns_cache)
            row_count[stream] = 0
            csv_files_to_load[stream] = NamedTemporaryFile(mode='w+b')
        elif t == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))


    # Single-host, thread-based parallelism
    with parallel_backend('threading', n_jobs=-1):
        Parallel()(delayed(load_stream_batch)(
            stream=stream,
            records_to_load=records_to_load[stream],
            row_count=row_count[stream],
            db_sync=stream_to_sync[stream],
            delete_rows=config.get('hard_delete')
        ) for (stream) in records_to_load.keys())

    return state


def load_stream_batch(stream, records_to_load, row_count, db_sync, delete_rows=False):
    #Load into snowflake
    if row_count > 0:
        flush_records(stream, records_to_load, row_count, db_sync)

    # Delete soft-deleted, flagged rows - where _sdc_deleted at is not null
    if delete_rows:
        db_sync.delete_rows(stream)


def flush_records(stream, records_to_load, row_count, db_sync):
    csv_fd, csv_file = tempfile.mkstemp()
    with open(csv_fd, 'w+b') as f:
        for record in records_to_load.values():
            csv_line = db_sync.record_to_csv_line(record)
            f.write(bytes(csv_line + '\n', 'UTF-8'))

    s3_key = db_sync.put_to_stage(csv_file, stream, row_count)
    db_sync.load_csv(s3_key, row_count)
    os.remove(csv_file)
    db_sync.delete_from_stage(s3_key)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()