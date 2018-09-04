#!/usr/bin/env python3

import singer

LOGGER = singer.get_logger()

class ConsoleHandler(object):
    '''Write messsages to console'''
    def __init__(self, max_batch_bytes, max_batch_records):
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records

    def handle_batch(self, messages, schema, key_names, bookmark_names=None):
        LOGGER.info('== CONSOLE HANDLER == SETTINGS: max_batch_bytes: {} max_batch_records: {}'.format(self.max_batch_bytes, self.max_batch_records))
        LOGGER.info('== CONSOLE HANDLER == SCHEMA: {}'.format(schema))
        for m in messages:
            LOGGER.info('== CONSOLE HANDLER == MESSAGE: {}'.format(m))
