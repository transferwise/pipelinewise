#!/usr/bin/env python3

import argparse
import os
import io
import sys
import json
import time
from datetime import datetime
import collections

from collections import namedtuple
from jsonschema.validators import Draft4Validator
import singer


from target_postgres.timings import Timings
from target_postgres.console_handler import ConsoleHandler
from target_postgres.logging_handler import LoggingHandler

LOGGER = singer.get_logger()
TIMINGS = Timings(LOGGER)
StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])

DEFAULT_MAX_BATCH_BYTES = 4000000
DEFAULT_MAX_BATCH_RECORDS = 20000

class TargetPostgresException(Exception):
    '''A known exception for which we don't need to pring a stack trace'''
    pass

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

class TargetPostgres(object):
    def __init__(self,
                handlers,
                state_writer,
                max_batch_bytes,
                max_batch_records,
                batch_delay_seconds):
        self.messages = []
        self.buffer_size_bytes = 0
        self.state = None

        # Mapping from stream name to {'schema': ..., 'key_names': ..., 'bookmark_names': ... }
        self.stream_meta = {}

        # Instance of PostgresHandler
        self.handlers = handlers

        # Writer that we write state records to
        self.state_writer = state_writer
        
        # Batch size limits. Stored as properties here so we can easily
        # change for testing.
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records

        # Minimum frequency to send a batch, used with self.time_last_batch_sent
        self.batch_delay_seconds = batch_delay_seconds

        # Time that the last batch was sent
        self.time_last_batch_sent = time.time()

    def validate_messages(self, stream_meta):
        '''Validate messages in batch'''
        if len(self.messages) > 0:
            key_properties = stream_meta.key_properties
            schema = stream_meta.schema
            validator = Draft4Validator(float_to_decimal(schema))

            for i, message in enumerate(self.messages):
                if isinstance(message, singer.RecordMessage):
                    data = float_to_decimal(message.record)
                    try:
                        validator.validate(data)
                        if key_properties:
                            for k in key_properties:
                                if k not in data:
                                    raise TargetPostgresException(
                                        'Message {} is missing key property {}'.format(
                                            i, k))
                    
                    except Exception as e:
                        raise TargetPostgresException(
                            'Record does not pass schema validation {}',format(e))
            
            LOGGER.info('Batch is valid')

    def flush(self):
        '''Give batch to handlers to process'''

        if self.messages:
            if self.messages[0].stream not in self.stream_meta:
                raise TargetConsoleException("A record for stream {} was encountered before a corresponding schema".format(self.messages[0].stream))
            
            stream_meta = self.stream_meta[self.messages[0].stream]
            with TIMINGS.mode('validating'):
                self.validate_messages(stream_meta)

            for handler in self.handlers:
                handler.handle_batch(self.messages,
                                     stream_meta.schema,
                                     stream_meta.key_properties,
                                     stream_meta.bookmark_properties)
            self.time_last_batch_sent = time.time()
            self.messages = []
            self.buffer_size_bytes = 0
        
        if self.state:
            line = json.dumps(self.state)
            self.state_writer.write("{}\n".format(line))
            self.state_writer.flush()
            self.state = None
            TIMINGS.log_timings()

    def handle_line(self, line):
        '''Takes a raw line from stdin and handles it, updating state and possibly
        flushing the batch to the Gate and the state to the output
        stream'''
        try :
            message = singer.parse_message(line)

            if not message:
                raise TargetPostgresException('Unknown message type')
        except Exception as exc:
            raise TargetPostgresException('Failed to process incoming message: {}\n{}'.format(line, exc))
        
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
        
        elif isinstance(message, singer.StateMessage):
            self.state = message.value
        else:
            self.messages.append(message)
            self.buffer_size_bytes += len(line)

            num_bytes = self.buffer_size_bytes
            num_messages = len(self.messages)
            num_seconds = time.time() - self.time_last_batch_sent

            enough_bytes = num_bytes >= self.max_batch_bytes
            enough_messages = num_messages >= self.max_batch_records
            enough_time = num_seconds >= self.batch_delay_seconds
            if enough_bytes or enough_messages or enough_time:
                LOGGER.debug('Flushing %d bytes, %d messages, after %.2f seconds',
                              num_bytes, num_messages, num_seconds)
                self.flush()

    def consume(self, reader):
        '''Consume all the lines from the queue, flushing when done.'''
        for line in reader:
            self.handle_line(line)
        self.flush()

"""
def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))

def persist_lines(lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']

        elif t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            schema = schemas[o['stream']]
            validators[o['stream']].validate(o['record'])
            
            flattened_record = flatten(o['record'])
            LOGGERlogger.info('== TARGET_CONSOLE: Captured RECORD in stream {} ==: {}'.format(o['stream'], o['record']))
            LOGGER.info(line)
        
        elif t == 'STATE':
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']

        else:
            LOGGER.info('== TARGET_CONSOLE: Ignored message type [{}]: ==: {}'.format(t, line))

    return state
"""

def main_impl():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', type=argparse.FileType('r'))
    parser.add_argument('-o', '--output-file', help='Save requests to this output file', type=argparse.FileType('w'))
    parser.add_argument('-p', '--print', help='Print validated messages', action='store_true')
    parser.add_argument('-v', '--verbose', help='Produce debug-level logging', action='store_true')
    parser.add_argument('-q', '--quiet', help='Suppress info-level logging', action='store_true')
    parser.add_argument('--max-batch-records', type=int, default=DEFAULT_MAX_BATCH_RECORDS)
    parser.add_argument('--max-batch-bytes', type=int, default=DEFAULT_MAX_BATCH_BYTES)
    parser.add_argument('--batch-delay-seconds', type=float, default=300.0)
    args = parser.parse_args()

    if args.verbose:
        LOGGER.setLevel('DEBUG')
    elif args.quiet:
        LOGGER.setLevel('WARNING')
    
    handlers = []
    #if not args.config:
    #    parser.error("config file required")

    if args.output_file:
        handlers.append(LoggingHandler(args.output_file,
                                       args.max_batch_bytes,
                                       args.max_batch_records))
    
    if args.print:    
        handlers.append(ConsoleHandler(args.max_batch_bytes,
                                       args.max_batch_records))

    reader = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    TargetPostgres(handlers,
                   sys.stdout,
                   args.max_batch_bytes,
                   args.max_batch_records,
                   args.batch_delay_seconds).consume(reader)
    LOGGER.info("Exiting normally")

def main():
    '''Main entry pint'''
    try:
        main_impl()

    except TargetPostgresException as exc:
        for line in str(exc).splitlines():
            LOGGER.critical(line)
        sys.exit(1)
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == '__main__':
    main()