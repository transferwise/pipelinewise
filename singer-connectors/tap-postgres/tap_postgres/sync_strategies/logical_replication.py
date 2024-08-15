import datetime
import pytz
import decimal
import psycopg2
import copy
import json
import re
import singer
import warnings

from select import select
from psycopg2 import sql
from singer import metadata, utils, get_bookmark
from dateutil.parser import parse, UnknownTimezoneWarning, ParserError
from functools import reduce

import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common
from tap_postgres.stream_utils import refresh_streams_schema

LOGGER = singer.get_logger('tap_postgres')

UPDATE_BOOKMARK_PERIOD = 10000
FALLBACK_DATETIME = '9999-12-31T23:59:59.999+00:00'
FALLBACK_DATE = '9999-12-31T00:00:00+00:00'


class ReplicationSlotNotFoundError(Exception):
    """Custom exception when replication slot not found"""


class UnsupportedPayloadKindError(Exception):
    """Custom exception when waljson payload is not insert, update nor delete"""


# pylint: disable=invalid-name,missing-function-docstring,too-many-branches,too-many-statements,too-many-arguments
def get_pg_version(conn_info):
    with post_db.open_connection(conn_info, False, True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT setting::int AS version FROM pg_settings WHERE name='server_version_num'")
            version = cur.fetchone()[0]
    LOGGER.debug('Detected PostgreSQL version: %s', version)
    return version


def lsn_to_int(lsn):
    """Convert pg_lsn to int"""

    if not lsn:
        return None

    file, index = lsn.split('/')
    lsni = (int(file, 16) << 32) + int(index, 16)
    return lsni


def int_to_lsn(lsni):
    """Convert int to pg_lsn"""

    if not lsni:
        return None

    # Convert the integer to binary
    lsnb = f'{lsni:b}'

    # file is the binary before the 32nd character, converted to hex
    if len(lsnb) > 32:
        file = (format(int(lsnb[:-32], 2), 'x')).upper()
    else:
        file = '0'

    # index is the binary from the 32nd character, converted to hex
    index = (format(int(lsnb[-32:], 2), 'x')).upper()
    # Formatting
    lsn = f"{file}/{index}"
    return lsn


# pylint: disable=chained-comparison
def fetch_current_lsn(conn_config):
    version = get_pg_version(conn_config)
    # Make sure PostgreSQL version is 9.4 or higher
    # Do not allow minor versions with PostgreSQL BUG #15114
    if (version >= 110000) and (version < 110002):
        raise Exception('PostgreSQL upgrade required to minor version 11.2')
    if (version >= 100000) and (version < 100007):
        raise Exception('PostgreSQL upgrade required to minor version 10.7')
    if (version >= 90600) and (version < 90612):
        raise Exception('PostgreSQL upgrade required to minor version 9.6.12')
    if (version >= 90500) and (version < 90516):
        raise Exception('PostgreSQL upgrade required to minor version 9.5.16')
    if (version >= 90400) and (version < 90421):
        raise Exception('PostgreSQL upgrade required to minor version 9.4.21')
    if version < 90400:
        raise Exception('Logical replication not supported before PostgreSQL 9.4')

    with post_db.open_connection(conn_config, False, True) as conn:
        with conn.cursor() as cur:
            # Use version specific lsn command
            if version >= 100000:
                cur.execute("SELECT pg_current_wal_lsn() AS current_lsn")
            else:
                cur.execute("SELECT pg_current_xlog_location() AS current_lsn")

            current_lsn = cur.fetchone()[0]
            return lsn_to_int(current_lsn)


def add_automatic_properties(stream, debug_lsn: bool = False):
    stream['schema']['properties']['_sdc_deleted_at'] = {'type': ['null', 'string'], 'format': 'date-time'}

    if debug_lsn:
        LOGGER.debug('debug_lsn is ON')
        stream['schema']['properties']['_sdc_lsn'] = {'type': ['null', 'string']}
    else:
        LOGGER.debug('debug_lsn is OFF')

    return stream


def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        raise Exception(f"version not found for log miner {tap_stream_id}")

    return stream_version


def tuples_to_map(accum, t):
    accum[t[0]] = t[1]
    return accum


def create_hstore_elem_query(elem):
    return sql.SQL("SELECT hstore_to_array({})").format(sql.Literal(elem))


def create_hstore_elem(conn_info, elem):
    with post_db.open_connection(conn_info, False, True) as conn:
        with conn.cursor() as cur:
            query = create_hstore_elem_query(elem)
            cur.execute(query)
            res = cur.fetchone()[0]
            hstore_elem = reduce(tuples_to_map, [res[i:i + 2] for i in range(0, len(res), 2)], {})
            return hstore_elem


def create_array_elem(elem, sql_datatype, conn_info):
    if elem is None:
        return None

    with post_db.open_connection(conn_info, False, True) as conn:
        with conn.cursor() as cur:
            if sql_datatype == 'bit[]':
                cast_datatype = 'boolean[]'
            elif sql_datatype == 'boolean[]':
                cast_datatype = 'boolean[]'
            elif sql_datatype == 'character varying[]':
                cast_datatype = 'character varying[]'
            elif sql_datatype == 'cidr[]':
                cast_datatype = 'cidr[]'
            elif sql_datatype == 'citext[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'date[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'double precision[]':
                cast_datatype = 'double precision[]'
            elif sql_datatype == 'hstore[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'integer[]':
                cast_datatype = 'integer[]'
            elif sql_datatype == 'inet[]':
                cast_datatype = 'inet[]'
            elif sql_datatype == 'json[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'jsonb[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'macaddr[]':
                cast_datatype = 'macaddr[]'
            elif sql_datatype == 'money[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'numeric[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'real[]':
                cast_datatype = 'real[]'
            elif sql_datatype == 'smallint[]':
                cast_datatype = 'smallint[]'
            elif sql_datatype == 'text[]':
                cast_datatype = 'text[]'
            elif sql_datatype in ('time without time zone[]', 'time with time zone[]'):
                cast_datatype = 'text[]'
            elif sql_datatype in ('timestamp with time zone[]', 'timestamp without time zone[]'):
                cast_datatype = 'text[]'
            elif sql_datatype == 'uuid[]':
                cast_datatype = 'text[]'

            else:
                # custom datatypes like enums
                cast_datatype = 'text[]'

            sql_stmt = f"""SELECT $stitch_quote${elem}$stitch_quote$::{cast_datatype}"""
            cur.execute(sql_stmt)
            res = cur.fetchone()[0]
            return res


# pylint: disable=too-many-branches,too-many-nested-blocks,too-many-return-statements
def selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info):
    sql_datatype = og_sql_datatype.replace('[]', '')

    if elem is None:
        return elem

    if sql_datatype == 'money':
        return elem

    if sql_datatype in ['json', 'jsonb']:
        return json.loads(elem)

    if sql_datatype == 'timestamp without time zone':
        if isinstance(elem, datetime.datetime):
            # we don't want a datetime like datetime(9999, 12, 31, 23, 59, 59, 999999) to be returned
            # compare the date in UTC tz to the max allowed
            if elem > datetime.datetime(9999, 12, 31, 23, 59, 59, 999000):
                return FALLBACK_DATETIME

            return elem.isoformat() + '+00:00'

        with warnings.catch_warnings():
            # we need to catch and handle this warning
            # github.com/
            #           dateutil/dateutil/blob/c496b4f872b50e8845c0f46b585a1e3830ed3648/dateutil/parser/_parser.py#L1213
            # otherwise ad date like this '0001-12-31 23:40:28 BC' would be parsed as
            # '0001-12-31T23:40:28+00:00' instead of using the fallback date
            warnings.filterwarnings('error')

            # parsing dates with era is not possible at moment
            # github.com/dateutil/dateutil/blob/c496b4f872b50e8845c0f46b585a1e3830ed3648/dateutil/parser/_parser.py#L297
            try:
                parsed = parse(elem)

                # compare the date in UTC tz to the max allowed
                if parsed > datetime.datetime(9999, 12, 31, 23, 59, 59, 999000):
                    return FALLBACK_DATETIME

                return parsed.isoformat() + '+00:00'
            except (ParserError, UnknownTimezoneWarning):
                return FALLBACK_DATETIME

    if sql_datatype == 'timestamp with time zone':
        if isinstance(elem, datetime.datetime):
            try:
                # compare the date in UTC tz to the max allowed
                utc_datetime = elem.astimezone(pytz.UTC).replace(tzinfo=None)
                if utc_datetime > datetime.datetime(9999, 12, 31, 23, 59, 59, 999000):
                    return FALLBACK_DATETIME

                return elem.isoformat()
            except OverflowError:
                return FALLBACK_DATETIME

        with warnings.catch_warnings():
            # we need to catch and handle this warning
            # github.com/
            #           dateutil/dateutil/blob/c496b4f872b50e8845c0f46b585a1e3830ed3648/dateutil/parser/_parser.py#L1213
            # otherwise ad date like this '0001-12-31 23:40:28 BC' would be parsed as
            # '0001-12-31T23:40:28+00:00' instead of using the fallback date
            warnings.filterwarnings('error')

            # parsing dates with era is not possible at moment
            # github.com/dateutil/dateutil/blob/c496b4f872b50e8845c0f46b585a1e3830ed3648/dateutil/parser/_parser.py#L297
            try:
                parsed = parse(elem)

                # compare the date in UTC tz to the max allowed
                if parsed.astimezone(pytz.UTC).replace(tzinfo=None) > \
                        datetime.datetime(9999, 12, 31, 23, 59, 59, 999000):
                    return FALLBACK_DATETIME

                return parsed.isoformat()

            except (ParserError, UnknownTimezoneWarning, OverflowError):
                return FALLBACK_DATETIME

    if sql_datatype == 'date':
        if isinstance(elem, datetime.date):
            # logical replication gives us dates as strings UNLESS they from an array
            return elem.isoformat() + 'T00:00:00+00:00'
        try:
            return parse(elem).isoformat() + "+00:00"
        except ValueError as e:
            match = re.match(r'year (\d+) is out of range', str(e))
            if match and int(match.group(1)) > 9999:
                LOGGER.warning('datetimes cannot handle years past 9999, returning %s for %s',
                               FALLBACK_DATE, elem)
                return FALLBACK_DATE
            raise
    if sql_datatype == 'time with time zone':
        # time with time zone values will be converted to UTC and time zone dropped
        # Replace hour=24 with hour=0
        if elem.startswith('24'):
            elem = elem.replace('24', '00', 1)
        # convert to UTC
        elem = elem + '00'
        elem_obj = datetime.datetime.strptime(elem, '%H:%M:%S%z')
        if elem_obj.utcoffset() != datetime.timedelta(seconds=0):
            LOGGER.warning('time with time zone values are converted to UTC: %s', og_sql_datatype)
        elem_obj = elem_obj.astimezone(pytz.utc)
        # drop time zone
        elem = elem_obj.strftime('%H:%M:%S')
        return parse(elem).isoformat().split('T')[1]
    if sql_datatype == 'time without time zone':
        # Replace hour=24 with hour=0
        if elem.startswith('24'):
            elem = elem.replace('24', '00', 1)
        return parse(elem).isoformat().split('T')[1]
    if sql_datatype == 'bit':
        # for arrays, elem will == True
        # for ordinary bits, elem will == '1'
        return elem == '1' or elem is True
    if sql_datatype == 'boolean':
        return elem
    if sql_datatype == 'hstore':
        return create_hstore_elem(conn_info, elem)
    if 'numeric' in sql_datatype:
        return decimal.Decimal(elem)
    if isinstance(elem, int):
        return elem
    if isinstance(elem, float):
        return elem
    if isinstance(elem, str):
        return elem

    raise Exception(f"do not know how to marshall value of type {type(elem)}")


def selected_array_to_singer_value(elem, sql_datatype, conn_info):
    if isinstance(elem, list):
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype, conn_info), elem))

    return selected_value_to_singer_value_impl(elem, sql_datatype, conn_info)


def selected_value_to_singer_value(elem, sql_datatype, conn_info):
    # are we dealing with an array?
    if sql_datatype.find('[]') > 0:
        cleaned_elem = create_array_elem(elem, sql_datatype, conn_info)
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype, conn_info),
                        (cleaned_elem or [])))

    return selected_value_to_singer_value_impl(elem, sql_datatype, conn_info)


def row_to_singer_message(stream, row, version, columns, time_extracted, md_map, conn_info):
    row_to_persist = ()
    md_map[('properties', '_sdc_deleted_at')] = {'sql-datatype': 'timestamp with time zone'}
    md_map[('properties', '_sdc_lsn')] = {'sql-datatype': "character varying"}

    for idx, elem in enumerate(row):
        sql_datatype = md_map.get(('properties', columns[idx])).get('sql-datatype')

        if not sql_datatype:
            LOGGER.info("No sql-datatype found for stream %s: %s", stream, columns[idx])
            raise Exception(f"Unable to find sql-datatype for stream {stream}")

        cleaned_elem = selected_value_to_singer_value(elem, sql_datatype, conn_info)
        row_to_persist += (cleaned_elem,)

    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=post_db.calculate_destination_stream_name(stream, md_map),
        record=rec,
        version=version,
        time_extracted=time_extracted)


# pylint: disable=unused-argument,too-many-locals
def consume_message(streams, state, msg, time_extracted, conn_info):
    try:
        payload = json.loads(msg.payload)
    except Exception:
        return state

    lsn = msg.data_start

    streams_lookup = {s['tap_stream_id']: s for s in streams}

    tap_stream_id = post_db.compute_tap_stream_id(payload['schema'], payload['table'])
    if streams_lookup.get(tap_stream_id) is None:
        return state

    target_stream = streams_lookup[tap_stream_id]

    # Example of Insert payload:
    # {
    #   "action":"I",
    #   "schema":"public",
    #   "table":"awesome_table",
    #   "columns":[
    #       {"name":"a","type":"integer","value":1},
    #       {"name":"b","type":"character varying(30)","value":"Backup"}
    #    ]
    # }

    # Example of Delete payload:
    # {
    #   "action":"D",
    #   "schema":"public",
    #   "table":"awesome_table",
    #   "identity":[
    #       {"name":"a","type":"integer","value":1},
    #       {"name":"c","type":"timestamp without time zone","value":"2019-12-29 04:58:34.806671"}
    #   ]
    # }

    # Action Types:
    # I = Insert
    # U = Update
    # D = Delete
    # B = Begin Transaction
    # C = Commit Transaction
    # M = Message
    # T = Truncate
    action = payload['action']

    if action not in {'I', 'U', 'D'}:
        raise UnsupportedPayloadKindError(f"unrecognized replication operation: {action}")

    # Get the additional fields in payload that are not in schema properties:
    # only inserts and updates have the list of columns that can be used to detect any different in columns
    diff = set()
    if action in {'I', 'U'}:
        diff = {column['name'] for column in payload['columns']}.\
            difference(target_stream['schema']['properties'].keys())

    # if there is new columns in the payload that are not in the schema properties then refresh the stream schema
    if diff:
        LOGGER.info('Detected new columns "%s", refreshing schema of stream %s', diff, target_stream['stream'])
        # encountered a column that is not in the schema
        # refresh the stream schema and metadata by running discovery
        refresh_streams_schema(conn_info, [target_stream])

        # add the automatic properties back to the stream
        add_automatic_properties(target_stream, conn_info.get('debug_lsn', False))

        # publish new schema
        sync_common.send_schema_message(target_stream, ['lsn'])

    stream_version = get_stream_version(target_stream['tap_stream_id'], state)
    stream_md_map = metadata.to_map(target_stream['metadata'])

    desired_columns = {c for c in target_stream['schema']['properties'].keys() if sync_common.should_sync_column(
        stream_md_map, c)}

    col_names = []
    col_vals = []

    if action in {'I', 'U'}:
        for col in payload['columns']:
            if col['name'] in desired_columns:
                col_names.append(col['name'])
                col_vals.append(col['value'])

        col_names.append('_sdc_deleted_at')
        col_vals.append(None)

    elif action == 'D':
        for column in payload['identity']:
            if column['name'] in set(desired_columns):
                col_names.append(column['name'])
                col_vals.append(column['value'])

        col_names.append('_sdc_deleted_at')
        col_vals.append(singer.utils.strftime(time_extracted))

    if conn_info.get('debug_lsn'):
        col_names.append('_sdc_lsn')
        col_vals.append(str(lsn))

    record_message = row_to_singer_message(target_stream,
                                           col_vals,
                                           stream_version,
                                           col_names,
                                           time_extracted,
                                           stream_md_map,
                                           conn_info)

    singer.write_message(record_message)
    state = singer.write_bookmark(state, target_stream['tap_stream_id'], 'lsn', lsn)

    return state


def generate_replication_slot_name(dbname, tap_id=None, prefix='pipelinewise'):
    """Generate replication slot name with

    :param str dbname: Database name that will be part of the replication slot name
    :param str tap_id: Optional. If provided then it will be appended to the end of the slot name
    :param str prefix: Optional. Defaults to 'pipelinewise'
    :return: well formatted lowercased replication slot name
    :rtype: str
    """
    # Add tap_id to the end of the slot name if provided
    if tap_id:
        tap_id = f'_{tap_id}'
    # Convert None to empty string
    else:
        tap_id = ''

    slot_name = f'{prefix}_{dbname}{tap_id}'.lower()

    # Replace invalid characters to ensure replication slot name is in accordance with Postgres spec
    return re.sub('[^a-z0-9_]', '_', slot_name)


def locate_replication_slot_by_cur(cursor, dbname, tap_id=None):
    slot_name_v15 = generate_replication_slot_name(dbname)
    slot_name_v16 = generate_replication_slot_name(dbname, tap_id)

    # Backward compatibility: try to locate existing v15 slot first. PPW <= 0.15.0
    cursor.execute(f"SELECT * FROM pg_replication_slots WHERE slot_name = '{slot_name_v15}'")
    if len(cursor.fetchall()) == 1:
        LOGGER.info('Using pg_replication_slot %s', slot_name_v15)
        return slot_name_v15

    # v15 style replication slot not found, try to locate v16
    cursor.execute(f"SELECT * FROM pg_replication_slots WHERE slot_name = '{slot_name_v16}'")
    if len(cursor.fetchall()) == 1:
        LOGGER.info('Using pg_replication_slot %s', slot_name_v16)
        return slot_name_v16

    raise ReplicationSlotNotFoundError(f'Unable to find replication slot {slot_name_v16}')


def locate_replication_slot(conn_info):
    with post_db.open_connection(conn_info, False, True) as conn:
        with conn.cursor() as cur:
            return locate_replication_slot_by_cur(cur, conn_info['dbname'], conn_info['tap_id'])


# pylint: disable=anomalous-backslash-in-string
def streams_to_wal2json_tables(streams):
    """Converts a list of singer stream dictionaries to wal2json plugin compatible string list.
    The output is compatible with the 'filter-tables' and 'add-tables' option of wal2json plugin.

    Special characters (space, single quote, comma, period, asterisk) must be escaped with backslash.
    Schema and table are case-sensitive. Table "public"."Foo bar" should be specified as "public.Foo\ bar".
    Documentation in wal2json plugin: https://github.com/eulerto/wal2json/blob/master/README.md#parameters

    :param streams: List of singer stream dictionaries
    :return: tables(str): comma separated and escaped list of tables, compatible for wal2json plugin
    :rtype: str
    """
    def escape_spec_chars(string):
        escaped = string
        wal2json_special_chars = " ',.*"
        for ch in wal2json_special_chars:
            escaped = escaped.replace(ch, f'\\{ch}')
        return escaped

    tables = []
    for s in streams:
        schema_name = escape_spec_chars(s['metadata'][0]['metadata']['schema-name'])
        table_name = escape_spec_chars(s['table_name'])

        tables.append(f'{schema_name}.{table_name}')

    return ','.join(tables)


def sync_tables(conn_info, logical_streams, state, end_lsn, state_file):
    state_comitted = state
    lsn_comitted = min([get_bookmark(state_comitted, s['tap_stream_id'], 'lsn') for s in logical_streams])
    start_lsn = lsn_comitted
    lsn_to_flush = None
    time_extracted = utils.now()
    slot = locate_replication_slot(conn_info)
    lsn_last_processed = None
    lsn_currently_processing = None
    lsn_processed_count = 0
    start_run_timestamp = datetime.datetime.utcnow()
    max_run_seconds = conn_info['max_run_seconds']
    break_at_end_lsn = conn_info['break_at_end_lsn']
    logical_poll_total_seconds = conn_info['logical_poll_total_seconds'] or 10800  # 3 hours
    poll_interval = 10

    for s in logical_streams:
        sync_common.send_schema_message(s, ['lsn'])

    version = get_pg_version(conn_info)

    # Create replication connection and cursor
    conn = post_db.open_connection(conn_info, True, True)
    cur = conn.cursor()

    # Set session wal_sender_timeout for PG12 and above
    if version >= 120000:
        wal_sender_timeout = 10800000  # 10800000ms = 3 hours
        LOGGER.info('Set session wal_sender_timeout = %i milliseconds', wal_sender_timeout)
        cur.execute(f"SET SESSION wal_sender_timeout = {wal_sender_timeout}")

    try:
        LOGGER.info('Request wal streaming from %s to %s (slot %s)',
                    int_to_lsn(start_lsn),
                    int_to_lsn(end_lsn),
                    slot)
        # psycopg2 2.8.4 will send a keep-alive message to postgres every status_interval
        cur.start_replication(slot_name=slot,
                              decode=True,
                              start_lsn=start_lsn,
                              status_interval=poll_interval,
                              options={
                                  'format-version': 2,
                                  'include-transaction': False,
                                  'include-timestamp': True,
                                  'include-types': False,
                                  'actions': 'insert,update,delete',
                                  'add-tables': streams_to_wal2json_tables(logical_streams)
                              })

    except psycopg2.ProgrammingError as ex:
        raise Exception(f"Unable to start replication with logical replication (slot {ex})") from ex

    lsn_received_timestamp = datetime.datetime.utcnow()
    poll_timestamp = datetime.datetime.utcnow()

    try:
        while True:
            # Disconnect when no data received for logical_poll_total_seconds
            # needs to be long enough to wait for the largest single wal payload to avoid unplanned timeouts
            poll_duration = (datetime.datetime.utcnow() - lsn_received_timestamp).total_seconds()
            if poll_duration > logical_poll_total_seconds:
                LOGGER.info('Breaking - %i seconds of polling with no data', poll_duration)
                break

            if datetime.datetime.utcnow() >= (start_run_timestamp + datetime.timedelta(seconds=max_run_seconds)):
                LOGGER.info('Breaking - reached max_run_seconds of %i', max_run_seconds)
                break

            try:
                msg = cur.read_message()
            except Exception as e:
                LOGGER.error(e)
                raise

            if msg:
                if (break_at_end_lsn) and (msg.data_start > end_lsn):
                    LOGGER.info('Breaking - latest wal message %s is past end_lsn %s',
                                int_to_lsn(msg.data_start),
                                int_to_lsn(end_lsn))
                    break

                state = consume_message(logical_streams, state, msg, time_extracted, conn_info)

                # When using wal2json with write-in-chunks, multiple messages can have the same lsn
                # This is to ensure we only flush to lsn that has completed entirely
                if lsn_currently_processing is None:
                    lsn_currently_processing = msg.data_start
                    LOGGER.info('First wal message received is %s', int_to_lsn(lsn_currently_processing))

                    # Flush Postgres wal up to lsn comitted in previous run, or first lsn received in this run
                    lsn_to_flush = lsn_comitted
                    if lsn_currently_processing < lsn_to_flush:
                        lsn_to_flush = lsn_currently_processing
                    LOGGER.info('Confirming write up to %s, flush to %s',
                                int_to_lsn(lsn_to_flush),
                                int_to_lsn(lsn_to_flush))
                    cur.send_feedback(write_lsn=lsn_to_flush, flush_lsn=lsn_to_flush, reply=True, force=True)

                elif int(msg.data_start) > lsn_currently_processing:
                    lsn_last_processed = lsn_currently_processing
                    lsn_currently_processing = msg.data_start
                    lsn_received_timestamp = datetime.datetime.utcnow()
                    lsn_processed_count = lsn_processed_count + 1
                    if lsn_processed_count >= UPDATE_BOOKMARK_PERIOD:
                        LOGGER.debug('Updating bookmarks for all streams to lsn = %s (%s)',
                                    lsn_last_processed,
                                    int_to_lsn(lsn_last_processed))
                        for s in logical_streams:
                            state = singer.write_bookmark(state, s['tap_stream_id'], 'lsn', lsn_last_processed)
                        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
                        lsn_processed_count = 0
            else:
                try:
                    # Wait for a second unless a message arrives
                    select([cur], [], [], 1)
                except InterruptedError:
                    pass

            # Every poll_interval, update latest comitted lsn position from the state_file
            if datetime.datetime.utcnow() >= (poll_timestamp + datetime.timedelta(seconds=poll_interval)):
                if lsn_currently_processing is None:
                    LOGGER.info('Waiting for first wal message')
                else:
                    LOGGER.info('Lastest wal message received was %s', int_to_lsn(lsn_last_processed))
                    try:
                        with open(state_file, mode="r", encoding="utf-8") as fh:
                            state_comitted = json.load(fh)
                    except Exception:
                        LOGGER.debug('Unable to open and parse %s', state_file)
                    finally:
                        lsn_comitted = min(
                            [get_bookmark(state_comitted, s['tap_stream_id'], 'lsn') for s in logical_streams])
                        if (lsn_currently_processing > lsn_comitted) and (lsn_comitted > lsn_to_flush):
                            lsn_to_flush = lsn_comitted
                            LOGGER.info('Confirming write up to %s, flush to %s',
                                        int_to_lsn(lsn_to_flush),
                                        int_to_lsn(lsn_to_flush))
                            cur.send_feedback(write_lsn=lsn_to_flush, flush_lsn=lsn_to_flush, reply=True, force=True)

                poll_timestamp = datetime.datetime.utcnow()

        # Close replication connection and cursor
        cur.close()
        conn.close()
    finally:
        if lsn_last_processed:
            if lsn_comitted > lsn_last_processed:
                lsn_last_processed = lsn_comitted
                LOGGER.info('Current lsn_last_processed %s is older than lsn_comitted %s',
                            int_to_lsn(lsn_last_processed),
                            int_to_lsn(lsn_comitted))

            LOGGER.info('Updating bookmarks for all streams to lsn = %s (%s)',
                        lsn_last_processed,
                        int_to_lsn(lsn_last_processed))

            for s in logical_streams:
                state = singer.write_bookmark(state, s['tap_stream_id'], 'lsn', lsn_last_processed)

        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    return state
