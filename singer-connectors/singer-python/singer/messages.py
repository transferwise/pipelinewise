import sys

import pytz
import orjson
import ciso8601
import decimal

import singer.utils as u
from .logger import get_logger
LOGGER = get_logger()


class Message():
    """Base class for messages."""

    def asdict(self):
        raise RuntimeError('Not implemented')

    def __eq__(self, other):
        return isinstance(other, Message) and self.asdict() == other.asdict()

    def __repr__(self):
        pairs = [f'{k}={v}' for k, v in self.asdict().items()]
        attrstr = ', '.join(pairs)
        return f'{self.__class__.__name__}({attrstr})'

    def __str__(self):
        return str(self.asdict())


class RecordMessage(Message):
    """RECORD message.

    The RECORD message has these fields:

      * stream (string) - The name of the stream the record belongs to.
      * record (dict) - The raw data for the record
      * version (optional, int) - For versioned streams, the version
        number. Note that this feature is experimental and most Taps and
        Targets should not need to use versioned streams.

    msg = singer.RecordMessage(
        stream='users',
        record={'id': 1, 'name': 'Mary'})

    """

    def __init__(self, stream, record, version=None, time_extracted=None):
        self.stream = stream
        self.record = record
        self.version = version
        self.time_extracted = time_extracted
        if time_extracted and not time_extracted.tzinfo:
            raise ValueError("'time_extracted' must be either None " +
                             'or an aware datetime (with a time zone)')

    def asdict(self):
        result = {
            'type': 'RECORD',
            'stream': self.stream,
            'record': self.record,
        }
        if self.version is not None:
            result['version'] = self.version
        if self.time_extracted:
            as_utc = self.time_extracted.astimezone(pytz.utc)
            result['time_extracted'] = u.strftime(as_utc)
        return result

    def __str__(self):
        return str(self.asdict())


class SchemaMessage(Message):
    """SCHEMA message.

    The SCHEMA message has these fields:

      * stream (string) - The name of the stream this schema describes.
      * schema (dict) - The JSON schema.
      * key_properties (list of strings) - List of primary key properties.

    msg = singer.SchemaMessage(
        stream='users',
        schema={'type': 'object',
                'properties': {
                    'id': {'type': 'integer'},
                    'name': {'type': 'string'}
                }
               },
        key_properties=['id'])

    """
    def __init__(self, stream, schema, key_properties, bookmark_properties=None):
        self.stream = stream
        self.schema = schema
        self.key_properties = key_properties

        if isinstance(bookmark_properties, (str, bytes)):
            bookmark_properties = [bookmark_properties]
        if bookmark_properties and not isinstance(bookmark_properties, list):
            raise TypeError('bookmark_properties must be a string or list of strings')

        self.bookmark_properties = bookmark_properties

    def asdict(self):
        result = {
            'type': 'SCHEMA',
            'stream': self.stream,
            'schema': self.schema,
            'key_properties': self.key_properties
        }
        if self.bookmark_properties:
            result['bookmark_properties'] = self.bookmark_properties
        return result


class StateMessage(Message):
    """STATE message.

    The STATE message has one field:

      * value (dict) - The value of the state.

    msg = singer.StateMessage(
        value={'users': '2017-06-19T00:00:00'})

    """
    def __init__(self, value):
        self.value = value

    def asdict(self):
        return {
            'type': 'STATE',
            'value': self.value
        }


class ActivateVersionMessage(Message):
    '''ACTIVATE_VERSION message (EXPERIMENTAL).

    The ACTIVATE_VERSION messages has these fields:

      * stream - The name of the stream.
      * version - The version number to activate.

    This is a signal to the Target that it should delete all previously
    seen data and replace it with all the RECORDs it has seen where the
    record's version matches this version number.

    Note that this feature is experimental. Most Taps and Targets should
    not need to use the "version" field of "RECORD" messages or the
    "ACTIVATE_VERSION" message at all.

    msg = singer.ActivateVersionMessage(
        stream='users',
        version=2)

    '''
    def __init__(self, stream, version):
        self.stream = stream
        self.version = version

    def asdict(self):
        return {
            'type': 'ACTIVATE_VERSION',
            'stream': self.stream,
            'version': self.version
        }


class BatchMessage(Message):
    """ BATCH message (EXPERIMENTAL).

    The BATCH message has these fields:

      * stream (string) - The name of the stream.
      * filepath (string) - The location of a batch file. e.g. '/tmp/users001.jsonl'.
      * format (string, optional) - An indication of serialization format.
            If none is provided, 'jsonl' will be assumed. e.g. 'csv'.
      * compression (string, optional) - An indication of file compression format. e.g. 'gzip'.
      * batch_size (int, optional) - Number of records in this batch. e.g. 100000.
      * time_extracted (datetime, optional) - TZ-aware datetime with batch extraction time.

    If file_properties are not provided, uncompressed jsonl files are assumed.

    A BATCH record points to a collection of messages (from a single stream) serialized to disk,
    and is implemented for performance reasons. Most Taps and Targets should not need to use
    BATCH messages at all.

    msg = singer.BatchMessage(
        stream='users',
        filepath='/tmp/users0001.jsonl'
    )

    """

    def __init__(
        self, stream, filepath, file_format=None, compression=None,
        batch_size=None, time_extracted=None
    ):
        self.stream = stream
        self.filepath = filepath
        self.format = file_format or 'jsonl'
        self.compression = compression
        self.batch_size = batch_size
        self.time_extracted = time_extracted
        if time_extracted and not time_extracted.tzinfo:
            raise ValueError("'time_extracted' must be either None " +
                             'or an aware datetime (with a time zone)')

    def asdict(self):
        result = {
            'type': 'BATCH',
            'stream': self.stream,
            'filepath': self.filepath,
            'format': self.format
        }
        if self.compression is not None:
            result['compression'] = self.compression
        if self.batch_size is not None:
            result['batch_size'] = self.batch_size
        if self.time_extracted:
            as_utc = self.time_extracted.astimezone(pytz.utc)
            result['time_extracted'] = u.strftime(as_utc)
        return result


def _required_key(msg, k):
    if k not in msg:
        raise ValueError(f"Message is missing required key '{k}': {msg}")

    return msg[k]


def parse_message(msg):
    """Parse a message string into a Message object."""

    # We are not using Decimals for parsing here.
    # We recognize that exposes data to potentially
    # lossy conversions.  However, this will affect
    # very few data points and we have chosen to
    # leave conversion as is for now.
    obj = orjson.loads(msg)
    msg_type = _required_key(obj, 'type')

    if msg_type == 'RECORD':
        time_extracted = obj.get('time_extracted')
        if time_extracted:
            try:
                time_extracted = ciso8601.parse_datetime(time_extracted)
            except Exception:
                LOGGER.warning('unable to parse time_extracted with ciso8601 library')
                time_extracted = None

            # time_extracted = dateutil.parser.parse(time_extracted)
        return RecordMessage(stream=_required_key(obj, 'stream'),
                             record=_required_key(obj, 'record'),
                             version=obj.get('version'),
                             time_extracted=time_extracted)

    if msg_type == 'SCHEMA':
        return SchemaMessage(stream=_required_key(obj, 'stream'),
                             schema=_required_key(obj, 'schema'),
                             key_properties=_required_key(obj, 'key_properties'),
                             bookmark_properties=obj.get('bookmark_properties'))

    if msg_type == 'STATE':
        return StateMessage(value=_required_key(obj, 'value'))

    if msg_type == 'ACTIVATE_VERSION':
        return ActivateVersionMessage(stream=_required_key(obj, 'stream'),
                                      version=_required_key(obj, 'version'))

    if msg_type == 'BATCH':
        time_extracted = obj.get('time_extracted')
        if time_extracted:
            try:
                time_extracted = ciso8601.parse_datetime(time_extracted)
            except Exception:
                LOGGER.warning('Unable to parse time_extracted with ciso8601 library')
                time_extracted = None

        return BatchMessage(
            stream=_required_key(obj, 'stream'),
            filepath=_required_key(obj, 'filepath'),
            file_format=_required_key(obj, 'format'),
            compression=obj.get('compression'),
            batch_size=obj.get('batch_size'),
            time_extracted=time_extracted
        )

    return None


def handler_for_decimal_object(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


def format_message(message, option=0):
    return orjson.dumps(message.asdict(), option=option, default=handler_for_decimal_object)


def write_message(message):
    sys.stdout.buffer.write(format_message(message, option=orjson.OPT_APPEND_NEWLINE))
    sys.stdout.buffer.flush()


def write_record(stream_name, record, stream_alias=None, time_extracted=None):
    """Write a single record for the given stream.

    write_record("users", {"id": 2, "email": "mike@stitchdata.com"})
    """
    write_message(RecordMessage(stream=(stream_alias or stream_name),
                                record=record,
                                time_extracted=time_extracted))


def write_records(stream_name, records):
    """Write a list of records for the given stream.

    chris = {"id": 1, "email": "chris@stitchdata.com"}
    mike = {"id": 2, "email": "mike@stitchdata.com"}
    write_records("users", [chris, mike])
    """
    for record in records:
        write_record(stream_name, record)


def write_schema(stream_name, schema, key_properties, bookmark_properties=None, stream_alias=None):
    """Write a schema message.

    stream = 'test'
    schema = {'properties': {'id': {'type': 'integer'}, 'email': {'type': 'string'}}}  # nopep8
    key_properties = ['id']
    write_schema(stream, schema, key_properties)
    """
    if isinstance(key_properties, (str, bytes)):
        key_properties = [key_properties]
    if not isinstance(key_properties, list):
        raise TypeError('key_properties must be a string or list of strings')

    write_message(
        SchemaMessage(
            stream=(stream_alias or stream_name),
            schema=schema,
            key_properties=key_properties,
            bookmark_properties=bookmark_properties))


def write_state(value):
    """Write a state message.

    write_state({'last_updated_at': '2017-02-14T09:21:00'})
    """
    write_message(StateMessage(value=value))


def write_version(stream_name, version):
    """Write an activate version message.

    stream = 'test'
    version = int(time.time())
    write_version(stream, version)
    """
    write_message(ActivateVersionMessage(stream_name, version))


def write_batch(
    stream_name, filepath, file_format=None,
    compression=None, batch_size=None, time_extracted=None
):
    """Write a batch message.

    stream = 'users'
    filepath = '/tmp/users0001.jsonl'
    file_format = 'jsonl'
    compression = None
    batch_size = 100000
    """
    write_message(
        BatchMessage(
            stream=stream_name,
            filepath=filepath,
            file_format=file_format,
            compression=compression,
            batch_size=batch_size,
            time_extracted=time_extracted
        )
    )
