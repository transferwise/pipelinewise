#!/usr/bin/env python3

import singer

LOGGER = singer.get_logger()

class LoggingHandler(object):
    '''Logs records to a local output file'''
    def __init__(self, output_file, max_batch_bytes, max_batch_records):
        self.output_file = output_file
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records
    
    def handle_batch(self, messages, schema, key_names, bookmark_names=None):
        LOGGER.info("== LOGGING HANDLER == Saving batch with %d messages for table %s to %s",
            len(messages), messages[0].stream, self.output_file.name)
#        for i, body in enumerate(serialize(messages,
#                                          schema,
#                                          key_names,
#                                          bookmark_names)):
#            LOGGER.debug("Request body %d is %d bytes", i, len(body))
#        LOGGER.debug("Request message is %d bytes",)
        self.output_file.write('{}'.format(messages))
        self.output_file.write('\n')

