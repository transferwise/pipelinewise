'''Utilities for logging and parsing metrics.

A Tap should use this library to log structured messages about the read
operations it makes.

Counter is a general-purpose class that allows you to increment a
"counter"-type metric. You initialize a Counter as a context manager, with
a metric name and a dictionary of tags. The Counter will periodically emit
a metric that indicates the amount by which the counter was incremented
since the last time it reported. For example, to increment a record count
for records from a "users" endpoint, you could do:

    with Counter('record_count', {'endpoint': 'users'}) as counter:
        for record in my_records:
            # Do stuff...
            counter.increment()

Timer is class that allows you to track the timing of operations. Like
Counter, you initialize it as a context manager, with a metric name and a
dictionary of tags. When the context exits, the timer will emit a single
metric that indicates how long it took in seconds. The metric will
automatically include a tag called "status" that is set to "failed" if an
Exception was raised, or "succeeded" otherwise.

    with Timer('http_request_duration', {'endpoint': 'users'}):
        # Make a request, do some things

In order to encourage consistent metric and tag names, this module
provides several functions for creating Counters and Timers for very
commonly used metrics.

  * record_counter - Increments a 'record_count' metric to track number of
    records fetched from a source. Provides an "endpoint" tag.

  * http_request_timer - Emits an 'http_request_duration' metric to time
    HTTP requests. Provides "endpoint" tag.

  * job_timer - Emits a 'job_duration' metric to track time of
    asynchronous jobs. Provides "job_type" tag.

'''

import orjson
import re
import time
from collections import namedtuple
from singer.logger import get_logger

DEFAULT_LOG_INTERVAL = 60


class Status:
    """Constants for status codes"""
    succeeded = 'succeeded'
    failed = 'failed'


class Metric:
    """Constants for metric names"""

    record_count = 'record_count'
    job_duration = 'job_duration'
    http_request_duration = 'http_request_duration'


class Tag:
    """Constants for commonly used tags"""

    endpoint = 'endpoint'
    job_type = 'job_type'
    http_status_code = 'http_status_code'
    status = 'status'


Point = namedtuple('Point', ['metric_type', 'metric', 'value', 'tags'])


def log(logger, point):
    """Log a single data point."""
    result = {
        'type': point.metric_type,
        'metric': point.metric,
        'value': point.value,
        'tags': point.tags
    }
    logger.info('METRIC: %s', orjson.dumps(result))


class Counter():
    '''Increments a counter metric.

    When you use Counter as a context manager, it will automatically emit
    points for a "counter" metric periodically and also when the context
    exits. The only thing you need to do is initialize the Counter and
    then call increment().

    with singer.metrics.Counter('record_count', {'endpoint': 'users'}) as counter:
       for user in get_users(...):
           # Print out the user
           counter.increment()

    This would print a metric like this:

    {
      "type":   "counter",
      "metric": "record_count",
      "value":   12345,
      "tags": {
        "endpoint": "users",
      }
    }

    '''

    def __init__(self, metric, tags=None, log_interval=DEFAULT_LOG_INTERVAL):
        self.metric = metric
        self.value = 0
        self.tags = tags if tags else {}
        self.log_interval = log_interval
        self.logger = get_logger()
        self.last_log_time = time.time()

    def __enter__(self):
        self.last_log_time = time.time()
        return self

    def increment(self, amount=1):
        """Increments value by the specified amount."""
        self.value += amount
        if self._ready_to_log():
            self._pop()

    def _pop(self):
        log(self.logger, Point('counter', self.metric, self.value, self.tags))
        self.value = 0
        self.last_log_time = time.time()

    def __exit__(self, exc_type, exc_value, traceback):
        self._pop()

    def _ready_to_log(self):
        return time.time() - self.last_log_time > self.log_interval


class Timer():
    '''Produces metrics about the duration of operations.

    You use a Timer as a context manager wrapping around some operation.
    When the context exits, the Timer emits a metric that indicates how
    long (in seconds) the operation took.

    It will automatically include a "status" tag that is "failed" if the
    context exits with an Exception or "success" if it exits cleanly. You
    can override this by setting timer.status within the context.

    with singer.metrics.Timer('request_duration', {'endpoint': 'users'}):
       # Do some stuff

    This produces a metric like this:

    {
      "type": "timer",
      "metric": "request_duration",
      "value": 1.23,
      "tags": {
        "endpoint": "users",
        "status": "success"
      }
    },

    '''
    def __init__(self, metric, tags):
        self.metric = metric
        self.tags = tags if tags else {}
        self.logger = get_logger()
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def elapsed(self):
        """Return elapsed time"""
        return time.time() - self.start_time

    def __exit__(self, exc_type, exc_value, traceback):
        if Tag.status not in self.tags:
            if exc_type is None:
                self.tags[Tag.status] = Status.succeeded
            else:
                self.tags[Tag.status] = Status.failed
        log(self.logger, Point('timer', self.metric, self.elapsed(), self.tags))


def record_counter(endpoint=None, log_interval=DEFAULT_LOG_INTERVAL):
    '''Use for counting records retrieved from the source.

    with singer.metrics.record_counter(endpoint="users") as counter:
         for record in my_records:
             # Do something with the record
             counter.increment()
    '''
    tags = {}
    if endpoint:
        tags[Tag.endpoint] = endpoint
    return Counter(Metric.record_count, tags, log_interval=log_interval)


def http_request_timer(endpoint):
    '''Use for timing HTTP requests to an endpoint

    with singer.metrics.http_request_timer("users") as timer:
        # Make a request
    '''
    tags = {}
    if endpoint:
        tags[Tag.endpoint] = endpoint
    return Timer(Metric.http_request_duration, tags)


def job_timer(job_type=None):
    '''Use for timing asynchronous jobs

    with singer.metrics.job_timer(job_type="users") as timer:
         # Make a request
    '''
    tags = {}
    if job_type:
        tags[Tag.job_type] = job_type
    return Timer(Metric.job_duration, tags)


def parse(line):
    """Parse a Point from a log line and return it, or None if no data point."""
    match = re.match(r'^INFO METRIC: (.*)$', line)
    if match:
        json_str = match.group(1)
        try:
            raw = orjson.loads(json_str)
            return Point(
                metric_type=raw.get('type'),
                metric=raw.get('metric'),
                value=raw.get('value'),
                tags=raw.get('tags'))
        except Exception as exc:
            get_logger().warning('Error parsing metric: %s', exc)
    return None
