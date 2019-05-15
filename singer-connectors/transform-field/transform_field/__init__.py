#!/usr/bin/env python3

import argparse
import io
import sys
import json
import time

from collections import namedtuple
from decimal import Decimal
from jsonschema import ValidationError, Draft4Validator, FormatChecker
import singer
from singer import utils

import transform_field.transform

from transform_field.timings import Timings

LOGGER = singer.get_logger()
TIMINGS = Timings(LOGGER)
DEFAULT_MAX_BATCH_BYTES = 4000000
DEFAULT_MAX_BATCH_RECORDS = 20000
DEFAULT_BATCH_DELAY_SECONDS = 300.0

StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])
TransMeta = namedtuple('TransMeta', ['field_id', 'type', 'when'])

REQUIRED_CONFIG_KEYS = [
    "transformations"
]

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

class TransformFieldException(Exception):
    '''A known exception for which we don't need to pring a stack trace'''
    pass

class TransformField(object):
    def __init__(self, trans_config):
        self.trans_config = trans_config
        self.messages = []
        self.buffer_size_bytes = 0
        self.state = None

        # TODO: Make it configurable
        self.max_batch_bytes = DEFAULT_MAX_BATCH_BYTES
        self.max_batch_records = DEFAULT_MAX_BATCH_RECORDS

        # Minimum frequency to send a batch, used with self.time_last_batch_sent
        self.batch_delay_seconds = DEFAULT_BATCH_DELAY_SECONDS

        # Time that the last batch was sent
        self.time_last_batch_sent = time.time()

        # Mapping from stream name to {'schema': ..., 'key_names': ..., 'bookmark_names': ... }
        self.stream_meta = {}

        # Writer that we write state records to
        self.state_writer = sys.stdout

        # Mapping from transformation stream to {'stream': [ 'field_id': ..., 'type': ... ] ... }
        self.trans_meta = {}
        for trans in trans_config["transformations"]:
            # Naming differences in stream ids:
            #  1. properties.json and transformation_json using 'tap_stream_id'
            #  2. taps send in the 'stream' key in singer messages
            stream = trans["tap_stream_name"]
            if stream not in self.trans_meta:
                self.trans_meta[stream] = []
            
            self.trans_meta[stream].append(TransMeta(
                trans["field_id"],
                trans["type"],
                trans.get('when')
            ))

    def flush(self):
        '''Give batch to handlers to process'''

        if self.messages:
            stream = self.messages[0].stream
            stream_meta = self.stream_meta[stream]

            # Transform columns
            messages = self.messages
            schema = float_to_decimal(stream_meta.schema)
            key_properties = stream_meta.key_properties
            validator = Draft4Validator(schema, format_checker=FormatChecker())
            trans_meta = []
            if stream in self.trans_meta:
                trans_meta = self.trans_meta[stream]

            for i, message in enumerate(messages):
                if isinstance(message, singer.RecordMessage):

                    # Do transformation on every column where it is required
                    for trans in trans_meta:

                        if trans.field_id in message.record:
                            transformed = transform.do_transform(message.record, trans.field_id, trans.type, trans.when)

                            # Truncate to transformed value to the max allowed length if required
                            if transformed is not None:
                                max_length = False
                                if trans.field_id in schema['properties']:
                                    if 'maxLength' in schema['properties'][trans.field_id]:
                                        max_length = schema['properties'][trans.field_id]['maxLength']

                                if max_length:
                                    message.record[trans.field_id] = transformed[:max_length]
                                else:
                                    message.record[trans.field_id] = transformed
                            else:
                                message.record[trans.field_id] = transformed

                    # Validate the transformed columns
                    data = float_to_decimal(message.record)
                    try:
                        validator.validate(data)
                        if key_properties:
                            for k in key_properties:
                                if k not in data:
                                    raise TransformFieldException(
                                        'Message {} is missing key property {}'.format(
                                            i, k))

                        # Write the transformed message
                        singer.write_message(message)

                    except Exception as e:
                        if type(e).__name__ == "InvalidOperation":
                            raise TransformFieldException(
                                "Record does not pass schema validation. RECORD: {}\n'multipleOf' validations that allows long precisions are not supported (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema.\n{}"
                                .format(message.record, e)
                            )
                        else:
                            raise TransformFieldException(
                                "Record does not pass schema validation. RECORD: {}\n{}".format(message.record, e))

            LOGGER.debug("Batch is valid with {} messages".format(len(messages)))

            # Update stats
            self.time_last_batch_sent = time.time()
            self.messages = []
            self.buffer_size_bytes = 0

        if self.state:
            singer.write_message(singer.StateMessage(self.state))
            self.state = None

        TIMINGS.log_timings()

    def handle_line(self, line):
        '''Takes a raw line from stdin and transforms it'''
        try :
            message = singer.parse_message(line)

            if not message:
                raise TransformFieldException('Unknown message type')
        except Exception as exc:
            raise TransformFieldException('Failed to process incoming message: {}\n{}'.format(line, exc))
        
        LOGGER.debug(message)

        # If we got a Schema, set the schema and key properties for this
        # stream. Flush the batch, if there is one, in case the schema is
        # different
        if isinstance(message, singer.SchemaMessage):
            self.flush()
        
            self.stream_meta[message.stream] = StreamMeta(
                message.schema,
                message.key_properties,
                message.bookmark_properties)

            # Write the transformed message
            singer.write_message(message)

        elif isinstance(message, (singer.RecordMessage, singer.ActivateVersionMessage)):
            if self.messages and (
                    message.stream != self.messages[0].stream or
                    message.version != self.messages[0].version):
                self.flush()
            self.messages.append(message)
            self.buffer_size_bytes += len(line)

            num_bytes = self.buffer_size_bytes
            num_messages = len(self.messages)
            num_seconds = time.time() - self.time_last_batch_sent

            enough_bytes = num_bytes >= self.max_batch_bytes
            enough_messages = num_messages >= self.max_batch_records
            enough_time = num_seconds >= self.batch_delay_seconds
            if enough_bytes or enough_messages or enough_time:
                LOGGER.debug('Flushing %d bytes, %d messages, after %.2f seconds', num_bytes, num_messages, num_seconds)
                self.flush()

        elif isinstance(message, singer.StateMessage):
            self.state = message.value

    def consume(self, reader):
        '''Consume all the lines from the queue, flushing when done.'''
        for line in reader:
            self.handle_line(line)
        self.flush()

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    trans_config = {'transformations': args.config['transformations']}

    reader = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    TransformField(trans_config).consume(reader)
    LOGGER.info("Exiting normally")

def main():
    '''Main entry point'''
    try:
        main_impl()

    except TransformFieldException as exc:
        for line in str(exc).splitlines():
            LOGGER.critical(line)
        sys.exit(1)
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == '__main__':
    main()