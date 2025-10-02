#!/usr/bin/env python3
import base64
import datetime
import time
import uuid
import bson
import singer
import pytz
import tzlocal

from typing import Dict, Any, Optional
from bson import objectid, timestamp, datetime as bson_datetime
from singer import utils, metadata
from terminaltables import AsciiTable

from tap_mongodb.errors import MongoInvalidDateTimeException, SyncException, UnsupportedKeyTypeException

SDC_DELETED_AT = "_sdc_deleted_at"
INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME = False
UPDATE_BOOKMARK_PERIOD = 1000
COUNTS = {}
TIMES = {}
SCHEMA_COUNT = {}
SCHEMA_TIMES = {}


def calculate_destination_stream_name(stream: Dict) -> str:
    """
    Builds the right stream name to be written in singer messages
    Args:
        stream: stream dictionary

    Returns: str holding the stream name
    """
    s_md = metadata.to_map(stream['metadata'])
    if INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME:
        return f"{s_md.get((), {}).get('database-name')}-{stream['stream']}"

    return stream['stream']


def get_stream_version(tap_stream_id: str, state: Dict) -> int:
    """
    Get the stream version by either extracting it from the state or generating a new one
    Args:
        tap_stream_id: stream ID to get version for
        state: state dictionary to extract version from if exists

    Returns: version as an integer

    """
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def class_to_string(key_value: Any, key_type: str) -> str:
    """
    Converts specific types to string equivalent
    The supported types are: datetime, bson Timestamp, bytes, int, Int64, float, ObjectId, str and UUID
    Args:
        key_value: The value to convert to string
        key_type: the value type

    Returns: string equivalent of key value
    Raises: UnsupportedKeyTypeException if key_type is not supported
    """
    if key_type == 'datetime':
        if key_value.tzinfo is None:
            timezone = tzlocal.get_localzone()
            local_datetime = datetime.datetime.fromtimestamp(key_value.timestamp(), tz=timezone)
            utc_datetime = local_datetime.astimezone(pytz.UTC)
        else:
            utc_datetime = key_value.astimezone(pytz.UTC)

        return utils.strftime(utc_datetime)

    if key_type == 'Timestamp':
        return f'{key_value.time}.{key_value.inc}'

    if key_type == 'bytes':
        return base64.b64encode(key_value).decode('utf-8')

    if key_type in ['int', 'Int64', 'float', 'ObjectId', 'str', 'UUID']:
        return str(key_value)

    raise UnsupportedKeyTypeException(f"{key_type} is not a supported key type")


def string_to_class(str_value: str, type_value: str) -> Any:
    """
    Converts the string value into the given type if supported.
    The supported types are: UUID, datetime, int, Int64, float, ObjectId, Timestamp, bytes, str
    Args:
        str_value: the string value to convert
        type_value: the value type

    Returns: converted string value
    Raises: UnsupportedKeyTypeException if key is unsupported
    """

    conversion = {
        'UUID': lambda val: bson.Binary.from_uuid(uuid.UUID(val)),
        'datetime': singer.utils.strptime_with_tz,
        'int': int,
        'Int64': bson.int64.Int64,
        'float': str,
        'ObjectId': objectid.ObjectId,
        'Timestamp': lambda val: (lambda split_value=val.split('.'):
                                  bson.timestamp.Timestamp(int(split_value[0]), int(split_value[1])))(),
        'bytes': lambda val: base64.b64decode(val.encode()),
        'str': str,
    }

    if type_value in conversion:
        return conversion[type_value](str_value)

    raise UnsupportedKeyTypeException(f"{type_value} is not a supported key type")


def safe_transform_datetime(value: datetime.datetime, path):
    """
    Safely transform datetime from local tz to UTC if applicable
    Args:
        value: datetime value to transform
        path:

    Returns: utc datetime as string

    """
    timezone = tzlocal.get_localzone()
    try:
        local_datetime = datetime.datetime.fromtimestamp(value.timestamp(), tz=timezone)
        utc_datetime = local_datetime.astimezone(pytz.UTC)
    except Exception as ex:
        if str(ex) == "year is out of range" and value.year == 0:
            # NB: Since datetimes are persisted as strings, it doesn't
            # make sense to blow up on invalid Python datetimes (e.g.,
            # year=0). In this case we're formatting it as a string and
            # passing it along down the pipeline.
            return f"{value.year:04d}-{value.month:02d}-{value.day:02d}T{value.hour:02d}:{value.minute:02d}:" \
                   f"{value.second:02d}.{value.microsecond:06d}Z"
        raise MongoInvalidDateTimeException(f"Found invalid datetime at [{'.'.join(map(str, path))}]: {value}") from ex
    return utils.strftime(utc_datetime)


def transform_value(value: Any, path) -> Any:
    """
    transform values to json friendly ones
    Args:
        value: value to transform
        path:

    Returns: transformed value

    """
    conversion = {
        list: lambda val, pat: list(map(lambda v: transform_value(v[1], pat + [v[0]]), enumerate(val))),
        dict: lambda val, pat: {k: transform_value(v, pat + [k]) for k, v in val.items()},
        uuid.UUID: lambda val, _: class_to_string(val, 'UUID'),
        objectid.ObjectId: lambda val, _: class_to_string(val, 'ObjectId'),
        bson_datetime.datetime: safe_transform_datetime,
        timestamp.Timestamp: lambda val, _: utils.strftime(val.as_datetime()),
        bson.int64.Int64: lambda val, _: class_to_string(val, 'Int64'),
        bytes: lambda val, _: class_to_string(val, 'bytes'),
        datetime.datetime: lambda val, _: class_to_string(val, 'datetime'),
        bson.decimal128.Decimal128: lambda val, _: val.to_decimal(),
        bson.regex.Regex: lambda val, _: dict(pattern=val.pattern, flags=val.flags),
        bson.binary.Binary: lambda val, _: class_to_string(val, 'bytes'),
        bson.code.Code: lambda val, _: dict(value=str(val), scope=str(val.scope)) if val.scope else str(val),
        bson.dbref.DBRef: lambda val, _: dict(id=str(val.id), collection=val.collection, database=val.database),
    }

    if isinstance(value, tuple(conversion.keys())):
        return conversion[type(value)](value, path)

    return value


def row_to_singer_record(stream: Dict,
                         row: Dict,
                         time_extracted: datetime.datetime,
                         time_deleted: Optional[datetime.datetime],
                         version: Optional[int] = None,
                         ) -> singer.RecordMessage:
    """
    Transforms row to singer record message
    Args:
        time_deleted: Datetime when row got deleted
        stream: stream details
        row: DB row
        version: stream version
        time_extracted: Datetime when row was extracted

    Returns: Singer RecordMessage instance

    """

    if version is None:
        version = int(time.time() * 1000)

    try:
        row_to_persist = {k: transform_value(v, [k]) for k, v in row.items()
                          if not isinstance(v, (bson.min_key.MinKey, bson.max_key.MaxKey))}
    except MongoInvalidDateTimeException as ex:
        raise SyncException(
            f"Error syncing collection {stream['tap_stream_id']}, object ID {row['_id']} - {ex}") from ex

    row_to_persist = {
        '_id': row_to_persist['_id'],
        'document': row_to_persist,
        SDC_DELETED_AT: utils.strftime(time_deleted) if time_deleted else None
    }

    return singer.RecordMessage(
        stream=calculate_destination_stream_name(stream),
        record=row_to_persist,
        version=version,
        time_extracted=time_extracted)


def get_sync_summary(catalog)->str:
    """
    Builds a summary of sync for all streams
    Args:
        catalog: dictionary with all the streams details

    Returns: summary table as string

    """
    headers = [['database',
                'collection',
                'replication method',
                'total records',
                'write speed',
                'total time',
                'schemas written',
                'schema build duration',
                'percent building schemas']]

    rows = []
    for stream_id, stream_count in COUNTS.items():
        stream = [x for x in catalog['streams'] if x['tap_stream_id'] == stream_id][0]
        collection_name = stream.get("table_name")
        md_map = metadata.to_map(stream['metadata'])
        db_name = metadata.get(md_map, (), 'database-name')
        replication_method = metadata.get(md_map, (), 'replication-method')

        stream_time = TIMES[stream_id]
        schemas_written = SCHEMA_COUNT[stream_id]
        schema_duration = SCHEMA_TIMES[stream_id]

        if stream_time == 0:
            stream_time = 0.000001

        rows.append(
            [
                db_name,
                collection_name,
                replication_method,
                f'{stream_count} records',
                f'{stream_count / float(stream_time):.1f} records/second',
                f'{stream_time:.5f} seconds',
                f'{schemas_written} schemas',
                f'{schema_duration:.5f} seconds',
                f'{100 * schema_duration / float(stream_time):.2f}%'
            ]
        )

    data = headers + rows
    table = AsciiTable(data, title='Sync Summary')

    return '\n\n' + table.table
