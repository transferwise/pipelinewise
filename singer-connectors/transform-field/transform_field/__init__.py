#!/usr/bin/env python3

import argparse
import io
import sys

from collections import namedtuple
from decimal import Decimal
from jsonschema.validators import Draft4Validator
import singer
from singer import utils

import transform_field.transform

from transform_field.timings import Timings

LOGGER = singer.get_logger()
TIMINGS = Timings(LOGGER)
StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])
TransMeta = namedtuple('TransMeta', ['field_id', 'type'])

REQUIRED_CONFIG_KEYS = [
    "transformations"
]

class TransformFieldException(Exception):
    '''A known exception for which we don't need to pring a stack trace'''
    pass

class TransformField(object):
    def __init__(self, trans_config):
        self.trans_config = trans_config
        self.messages = []

        # Mapping from stream name to {'schema': ..., 'key_names': ..., 'bookmark_names': ... }
        self.stream_meta = {}

        # Mapping from transformation stream to {'stream': [ 'field_id': ..., 'type': ... ] ... }
        self.trans_meta = {}
        for trans in trans_config["transformations"]:
            stream = trans["stream"]
            if stream not in self.trans_meta:
                self.trans_meta[stream] = []
            
            self.trans_meta[stream].append(TransMeta(
                trans["fieldId"],
                trans["type"]
            ))

    def validate_messages(self, stream_meta):
        '''Validate messages in batch'''
        if len(self.messages) > 0:
            key_properties = stream_meta.key_properties
            schema = stream_meta.schema
            validator = Draft4Validator(schema)

            for i, message in enumerate(self.messages):
                if isinstance(message, singer.RecordMessage):
                    data = message.record
                    try:
                        validator.validate(data)
                        if key_properties:
                            for k in key_properties:
                                if k not in data:
                                    raise TransformFieldException(
                                        'Message {} is missing key property {}'.format(
                                            i, k))
                    
                    except Exception as e:
                        raise TransformFieldException(
                            'Record does not pass schema validation {}',format(e))

    def flush(self):
        '''Give batch to handlers to process'''

        for message in self.messages:
            if isinstance(message, singer.RecordMessage):
                stream_meta = self.stream_meta[self.messages[0].stream]
                self.validate_messages(stream_meta)
    
            singer.write_message(message)

        self.messages = []

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
            
            self.messages.append(message)
        elif isinstance(message, singer.StateMessage):
            self.messages.append(message)
        elif isinstance(message, singer.RecordMessage):
            trans_meta = []
            if message.stream in self.trans_meta:
                trans_meta = self.trans_meta[message.stream]
            
            # Do transformation on every column where it is required
            for trans in trans_meta:
                if trans.field_id in message.record:
                    orig_value = message.record[trans.field_id]
                    message.record[trans.field_id] = transform.do_transform(orig_value, trans.type)

            self.messages.append(message)
        else:
            self.messages.append(message)

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