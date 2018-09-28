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
import collections
from tempfile import TemporaryFile

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer
from target_postgres.db_sync import DbSync

logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    csv_files_to_load = {}
    row_count = {}
    stream_to_sync = {}
    primary_key_exists = {}
    batch_size = config['batch_size'] if 'batch_size' in config else 100000

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

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
            validators[stream].validate(o['record'])

            sync = stream_to_sync[stream]

            primary_key_string = sync.record_primary_key_string(o['record'])
            if stream not in primary_key_exists:
                primary_key_exists[stream] = {}
            if primary_key_string and primary_key_string in primary_key_exists[stream]:
                flush_records(o, csv_files_to_load, row_count, primary_key_exists, sync)

            csv_line = sync.record_to_csv_line(o['record'])
            csv_files_to_load[o['stream']].write(bytes(csv_line + '\n', 'UTF-8'))
            row_count[o['stream']] += 1
            if primary_key_string:
                primary_key_exists[stream][primary_key_string] = True

            if row_count[o['stream']] >= batch_size:
                flush_records(o, csv_files_to_load, row_count, primary_key_exists, sync)

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
            stream_to_sync[stream] = DbSync(config, o)
            stream_to_sync[stream].create_schema_if_not_exists()
            stream_to_sync[stream].sync_table()
            row_count[stream] = 0
            csv_files_to_load[stream] = TemporaryFile(mode='w+b')
        elif t == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    for (stream_name, count) in row_count.items():
        if count > 0:
            stream_to_sync[stream_name].load_csv(csv_files_to_load[stream_name], count)

    return state


def flush_records(o, csv_files_to_load, row_count, primary_key_exists, sync):
    stream = o['stream']
    sync.load_csv(csv_files_to_load[stream], row_count[stream])
    row_count[stream] = 0
    primary_key_exists[stream] = {}
    csv_files_to_load[stream] = TemporaryFile(mode='w+b')


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