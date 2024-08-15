#!/usr/bin/env python3

import argparse
import csv
import gzip
import io
import json
import os
import shutil
import sys
import tempfile
import singer

from datetime import datetime
from jsonschema import Draft7Validator, FormatChecker

from target_s3_csv import s3
from target_s3_csv import utils

logger = singer.get_logger('target_s3_csv')


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def persist_messages(messages, config, s3_client):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    delimiter = config.get('delimiter', ',')
    quotechar = config.get('quotechar', '"')

    # Use the system specific temp directory if no custom temp_dir provided
    temp_dir = os.path.expanduser(config.get('temp_dir', tempfile.gettempdir()))

    # Create temp_dir if not exists
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)

    # dictionary to hold csv filename per stream
    filenames = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD':
            stream_name = o['stream']

            if stream_name not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(stream_name))

            # Validate record
            try:
                validators[stream_name].validate(utils.float_to_decimal(o['record']))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    logger.error("Data validation failed and cannot load to destination. \n"
                                 "'multipleOf' validations that allows long precisions are not supported"
                                 " (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema.")
                    raise ex

            record_to_load = o['record']
            if config.get('add_metadata_columns'):
                record_to_load = utils.add_metadata_values_to_record(o, {})
            else:
                record_to_load = utils.remove_metadata_values_from_record(o)

            if stream_name not in filenames:
                filename = os.path.expanduser(os.path.join(temp_dir, stream_name + '-' + now + '.csv'))

                filenames[stream_name] = {
                    'filename': filename,
                    'target_key': utils.get_target_key(message=o,
                                                       prefix=config.get('s3_key_prefix', ''),
                                                       timestamp=now,
                                                       naming_convention=config.get('naming_convention'))

                }
            else:
                filename = filenames[stream_name]['filename']

            file_is_empty = (not os.path.isfile(filename)) or os.stat(filename).st_size == 0

            flattened_record = utils.flatten_record(record_to_load)

            if stream_name not in headers and not file_is_empty:
                with open(filename, 'r') as csvfile:
                    reader = csv.reader(csvfile,
                                        delimiter=delimiter,
                                        quotechar=quotechar)
                    first_line = next(reader)
                    headers[stream_name] = first_line if first_line else flattened_record.keys()
            else:
                headers[stream_name] = flattened_record.keys()

            with open(filename, 'a') as csvfile:
                writer = csv.DictWriter(csvfile,
                                        headers[stream_name],
                                        extrasaction='ignore',
                                        delimiter=delimiter,
                                        quotechar=quotechar)
                if file_is_empty:
                    writer.writeheader()

                writer.writerow(flattened_record)

        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']

        elif message_type == 'SCHEMA':
            stream_name = o['stream']
            schemas[stream_name] = o['schema']

            if config.get('add_metadata_columns'):
                schemas[stream_name] = utils.add_metadata_columns_to_schema(o)

            schema = utils.float_to_decimal(o['schema'])
            validators[stream_name] = Draft7Validator(schema, format_checker=FormatChecker())
            key_properties[stream_name] = o['key_properties']
        elif message_type == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')
        else:
            logger.warning("Unknown message type {} in message {}".format(o['type'], o))

    # Upload created CSV files to S3
    s3.upload_files(iter(filenames.values()), s3_client, config['s3_bucket'], config.get("compression"),
                    config.get('encryption_type'), config.get('encryption_key'))

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    config_errors = utils.validate_config(config)
    if len(config_errors) > 0:
        logger.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
        sys.exit(1)

    s3_client = s3.create_client(config)

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(input_messages, config, s3_client)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
