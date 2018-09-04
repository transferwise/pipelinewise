#!/usr/bin/env python3

import time

from contextlib import contextmanager

class Timings(object):
    '''Gathers timing information for the three main steps of the Tap.'''
    def __init__(self, logger):
        self.LOGGER = logger
        self.last_time = time.time()
        self.timings = {
            'validating': 0.0,
            'posting': 0.0,
            None: 0.0
        }
    
    @contextmanager
    def mode(self, mode):
        '''We wrap the big steps of the Tap in this context manager to accumulate
        timing info.'''

        start = time.time()
        yield
        end = time.time()
        self.timings[None] += start - self.last_time
        self.timings[mode] += end - start
        self.last_time = end
    
    def log_timings(self):
        '''We call this with every flush to print out the accumulated timings'''
        self.LOGGER.debug('Timings: unspecified: %.3f; validating: %.3f; posting: %.3f;',
                          self.timings[None],
                          self.timings['validating'],
                          self.timings['posting'])
