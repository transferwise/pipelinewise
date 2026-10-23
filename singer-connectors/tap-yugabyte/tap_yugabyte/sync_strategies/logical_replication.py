import datetime
import pytz
import decimal
import psycopg2
import copy
import json
import re
import singer
import sys
import warnings

from select import select
from psycopg2 import sql
from singer import metadata, utils, get_bookmark
from singer.decimal_support import decimal_to_string, is_decimal_schema
from dateutil.parser import parse, UnknownTimezoneWarning, ParserError
from functools import reduce

import tap_yugabyte.db as yb_db
import tap_yugabyte.sync_strategies.common as sync_common
from tap_yugabyte.stream_utils import refresh_streams_schema
from tap_yugabyte.discovery_utils import Column, schema_for_column

LOGGER = singer.get_logger('tap_yugabyte')

UPDATE_BOOKMARK_PERIOD = 10000
FEEDBACK_POLL_INTERVAL = 10
FALLBACK_DATETIME = '9999-12-31T23:59:59.999+00:00'
FALLBACK_DATE = '9999-12-31T00:00:00+00:00'
# psycopg2-yugabytedb 2.9.3.5 and 2.9.3.5.post1 replace the configured port with 5433 on replication connections.
REPLICATION_PORT = 5433


class ReplicationSlotNotFoundError(Exception):
    """Custom exception when replication slot not found"""


class IncompatibleReplicationSlotError(Exception):
    """Custom exception when replication slot is not a wal2json HYBRID_TIME slot"""


class UnsupportedReplicationPortError(Exception):
    """Custom exception when LOG_BASED replication is configured on a port the driver cannot use"""


class UnsupportedPayloadKindError(Exception):
    """Custom exception when waljson payload is not insert, update nor delete"""


class MalformedWALPayloadError(ValueError):
    """A WAL payload cannot be safely included in a transaction checkpoint."""


def _validate_wal_payload(message_payload, msg, conn_info, slot=None):
    context = f"LSN {msg.data_start}, slot {slot or 'unknown'}, tap {conn_info.get('tap_id', 'unknown')}"
    if not isinstance(message_payload, dict):
        raise MalformedWALPayloadError(f'Invalid wal2json payload at {context}: expected a JSON object')
    action = message_payload.get('action')
    if not isinstance(action, str) or action not in {'I', 'U', 'D', 'B', 'C', 'M', 'T'}:
        raise MalformedWALPayloadError(f'Invalid wal2json payload at {context}: missing or unsupported action')
    if action in {'I', 'U', 'D'}:
        for field in ('schema', 'table'):
            if not isinstance(message_payload.get(field), str) or not message_payload[field]:
                raise MalformedWALPayloadError(f'Invalid wal2json payload at {context}: missing or invalid {field}')
        row_field = 'identity' if action == 'D' else 'columns'
        columns = message_payload.get(row_field)
        if not isinstance(columns, list) or not columns:
            raise MalformedWALPayloadError(f'Invalid wal2json payload at {context}: missing or empty {row_field}')
        if any(not isinstance(column, dict) or not isinstance(column.get('name'), str)
               or not column['name'] or 'value' not in column for column in columns):
            raise MalformedWALPayloadError(f'Invalid wal2json payload at {context}: invalid {row_field} entry')
    return message_payload


def _decode_wal_payload(msg, conn_info, slot=None):
    context = f"LSN {msg.data_start}, slot {slot or 'unknown'}, tap {conn_info.get('tap_id', 'unknown')}"
    try:
        payload = msg.payload.decode('utf-8') if isinstance(msg.payload, bytes) else msg.payload
        message_payload = parse_wal_payload(payload, conn_info)
    except (TypeError, ValueError) as exc:
        # Preserve the decoding cause without retaining source record bytes in tracebacks.
        if isinstance(exc, UnicodeDecodeError):
            exc.object = b''
        elif isinstance(exc, json.JSONDecodeError):
            exc.doc = ''
        raise MalformedWALPayloadError(f'Invalid wal2json payload at {context}: invalid JSON or UTF-8') from exc
    return _validate_wal_payload(message_payload, msg, conn_info, slot)


def ensure_replication_port(conn_config):
    """Reject LOG_BASED replication on any port other than 5433, which the YugabyteDB driver would ignore."""
    if str(conn_config['port']) != str(REPLICATION_PORT):
        raise UnsupportedReplicationPortError(
            f"LOG_BASED replication requires port {REPLICATION_PORT}, but port {conn_config['port']} is configured. "
            'The current YugabyteDB psycopg2 drivers (psycopg2-yugabytedb-binary 2.9.3.5 and 2.9.3.5.post1) '
            f'always open replication connections on port {REPLICATION_PORT}. Connect on port '
            f'{REPLICATION_PORT}, or use FULL_TABLE or INCREMENTAL for this tap.')


def fetch_current_lsn(conn_config):
    """Return the current HYBRID_TIME LSN boundary, usable as a slot-independent replication end point."""
    with yb_db.open_connection(conn_config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT yb_get_current_hybrid_time_lsn()")
            return cur.fetchone()[0]


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
    """CDC hstore values arrive as text (e.g. '"a"=>"1"'); cast back to a map via YSQL, same as tap-postgres."""
    with yb_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            query = create_hstore_elem_query(elem)
            cur.execute(query)
            res = cur.fetchone()[0]
            hstore_elem = reduce(tuples_to_map, [res[i:i + 2] for i in range(0, len(res), 2)], {})
            return hstore_elem


def create_array_elem(elem, sql_datatype, conn_info):  # noqa: C901
    """CDC array values arrive as PG array-literal text (e.g. '{1,2,3}'); cast back via YSQL, same as tap-postgres."""
    if elem is None:
        return None

    with yb_db.open_connection(conn_info) as conn:
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


def selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info):  # noqa: C901
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

        column_schema = stream.get('schema', {}).get('properties', {}).get(columns[idx], {})
        if is_decimal_schema(column_schema):
            cleaned_elem = decimal_to_string(elem, column_schema)
        else:
            if (conn_info or {}).get('decimal_target') and elem is not None:
                if sql_datatype in {'real', 'double precision'}:
                    elem = float(elem)
                elif sql_datatype in {'smallint', 'integer', 'bigint'}:
                    elem = int(elem)
            cleaned_elem = selected_value_to_singer_value(elem, sql_datatype, conn_info)
        row_to_persist += (cleaned_elem,)

    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=yb_db.calculate_destination_stream_name(stream, md_map),
        record=rec,
        version=version,
        time_extracted=time_extracted)


def parse_wal_payload(payload, conn_info):
    """Preserve decimal tokens before source-column conversion on enabled routes."""
    # Unbounded NUMERIC may exceed Python's integer digit limit on older wal2json.
    options = {'parse_float': decimal.Decimal, 'parse_int': str} if conn_info.get('decimal_target') else {}
    return json.loads(payload, **options)


def changed_decimal_columns(payload, stream):
    """Detect numeric typmod changes carried by wal2json on enabled routes."""
    changed = set()
    properties = stream['schema']['properties']
    for column in payload.get('columns', payload.get('identity', [])):
        sql_type = column.get('type', '')
        schema = properties.get(column['name'], {})
        if sql_type == 'numeric':
            dimensions = {'precision': None, 'scale': None}
        else:
            match = re.fullmatch(r'numeric\((\d+),\s*(-?\d+)\)', sql_type)
            if not match:
                if is_decimal_schema(schema) and sql_type:
                    changed.add(column['name'])
                continue
            dimensions = {'precision': int(match[1]), 'scale': int(match[2])}
        if not is_decimal_schema(schema) or schema['decimal'] != dimensions:
            changed.add(column['name'])
    return changed


def reconcile_wal_decimal_schema(payload, stream, previous_metadata, decimal_target):
    """Use event type metadata when live discovery cannot describe a WAL row."""
    unresolved = changed_decimal_columns(payload, stream)
    if not unresolved:
        return
    LOGGER.warning('Using WAL type metadata for stream %s, columns %s after schema discovery did not converge',
                   stream['tap_stream_id'], ', '.join(sorted(unresolved)))
    md_map = metadata.to_map(stream['metadata'])
    key_properties = md_map.get((), {}).get('table-key-properties', [])
    for column in payload.get('columns', payload.get('identity', [])):
        name = column['name']
        if name not in unresolved:
            continue
        sql_type = column['type']
        numeric = re.fullmatch(r'numeric(?:\((\d+),\s*(-?\d+)\))?', sql_type)
        type_modifier = re.search(r'\((\d+)\)', sql_type)
        sql_type = re.sub(r'\([^)]*\)', '', sql_type)
        precision = int(numeric[1]) if numeric and numeric[1] else {
            'smallint': 16, 'integer': 32, 'bigint': 64,
        }.get(sql_type)
        scale = int(numeric[2]) if numeric and numeric[2] else None
        description = Column(name, name in key_properties, sql_type,
                             int(type_modifier[1]) if type_modifier else None,
                             precision, scale, sql_type.endswith('[]'), False,
                             key_properties.index(name) if name in key_properties else None)
        column_schema = schema_for_column(description, decimal_target)
        stream['schema']['properties'][name] = column_schema or {'type': ['null', 'string']}
        field_metadata = md_map.setdefault(('properties', name), dict(previous_metadata.get(('properties', name), {})))
        field_metadata.setdefault('inclusion', 'available')
        field_metadata['sql-datatype'] = sql_type
    stream['metadata'] = metadata.to_list(md_map)


def refresh_message_schema(stream, payload, conn_info):
    """Refresh a changed stream and reconcile decimal metadata before publication."""
    previous_metadata = metadata.to_map(stream['metadata'])
    refresh_streams_schema(conn_info, [stream])
    if conn_info.get('decimal_target'):
        reconcile_wal_decimal_schema(payload, stream, previous_metadata, conn_info['decimal_target'])
    add_automatic_properties(stream, conn_info.get('debug_lsn', False))
    sync_common.send_schema_message(stream, ['lsn'], record_update_mode=sync_common.PATCH_RECORD_UPDATE_MODE)


def consume_message(streams, state, msg, time_extracted, conn_info, *, message_payload=None, slot=None):
    if message_payload is None:
        message_payload = _decode_wal_payload(msg, conn_info, slot)
    else:
        message_payload = _validate_wal_payload(message_payload, msg, conn_info, slot)

    lsn = msg.data_start

    action = message_payload.get('action')
    # Action Types:
    # I = Insert
    # U = Update
    # D = Delete
    # B = Begin Transaction
    # C = Commit Transaction
    # M = Message
    # T = Truncate

    # Advance the slot LSN for non-row actions without doing any processing
    # This avoids the slot growing when the source has very busy tables that are NOT selected for replication
    if action not in {'I', 'U', 'D'}:
        LOGGER.debug('Skipping non-row wal2json message: action=%s, lsn=%s', action, lsn)
        return state

    streams_lookup = {s['tap_stream_id']: s for s in streams}

    tap_stream_id = yb_db.compute_tap_stream_id(message_payload['schema'], message_payload['table'])
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

    # Get the additional fields in payload that are not in schema properties:
    # only inserts and updates have the list of columns that can be used to detect any different in columns
    diff = set()
    if action in {'I', 'U'}:
        diff = {column['name'] for column in message_payload['columns']}.\
            difference(target_stream['schema']['properties'].keys())
    if conn_info.get('decimal_target'):
        # A DELETE can be the first row event after a numeric typmod change.
        diff.update(changed_decimal_columns(message_payload, target_stream))

    # if there is new columns in the payload that are not in the schema properties then refresh the stream schema
    if diff:
        LOGGER.info('Detected changed columns "%s", refreshing schema of stream %s', diff, target_stream['stream'])
        refresh_message_schema(target_stream, message_payload, conn_info)

    stream_version = get_stream_version(target_stream['tap_stream_id'], state)
    stream_md_map = metadata.to_map(target_stream['metadata'])

    desired_columns = {c for c in target_stream['schema']['properties'].keys() if sync_common.should_sync_column(
        stream_md_map, c)}

    col_names = []
    col_vals = []

    if action in {'I', 'U'}:
        for col in message_payload['columns']:
            if col['name'] in desired_columns:
                col_names.append(col['name'])
                col_vals.append(col['value'])

        col_names.append('_sdc_deleted_at')
        col_vals.append(None)

    elif action == 'D':
        for column in message_payload['identity']:
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
    slot_name = generate_replication_slot_name(dbname, tap_id)

    cursor.execute('SELECT plugin, yb_lsn_type FROM pg_replication_slots WHERE slot_name = %s', (slot_name,))
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise ReplicationSlotNotFoundError(f'Unable to find replication slot {slot_name}')

    plugin, lsn_type = rows[0]
    if (plugin, lsn_type) != ('wal2json', 'HYBRID_TIME'):
        raise IncompatibleReplicationSlotError(
            f'Replication slot {slot_name} uses plugin {plugin} and LSN type {lsn_type}; '
            'tap-yugabyte requires plugin wal2json and LSN type HYBRID_TIME. '
            'Stop replication, drop the slot, and rerun the unfiltered fast_sync to recreate it.')

    LOGGER.info('Using pg_replication_slot %s', slot_name)
    return slot_name


def locate_replication_slot(conn_info):
    with yb_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            return locate_replication_slot_by_cur(cur, conn_info['dbname'], conn_info['tap_id'])


def streams_to_wal2json_tables(streams):
    r"""Converts a list of singer stream dictionaries to wal2json plugin compatible string list.
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


def _minimum_acknowledged_lsn(state, logical_streams):
    """Return the oldest valid target-acknowledged LSN across logical streams."""
    acknowledged_lsns = [
        get_bookmark(state, stream['tap_stream_id'], 'lsn')
        for stream in logical_streams
    ]
    if not acknowledged_lsns or any(
            isinstance(lsn, bool) or not isinstance(lsn, int) or lsn < 0
            for lsn in acknowledged_lsns):
        raise ValueError('State does not contain a valid LSN for every logical stream')
    return min(acknowledged_lsns)


def _read_target_acknowledged_lsn(state_file, logical_streams, previous_safe_lsn):
    """Read a target acknowledgement without accepting invalid or regressing state."""
    try:
        with open(state_file, mode='r', encoding='utf-8') as fh:
            target_state = json.load(fh)
        return max(previous_safe_lsn, _minimum_acknowledged_lsn(target_state, logical_streams))
    except (AttributeError, KeyError, OSError, TypeError, UnicodeError, ValueError):
        LOGGER.debug('Unable to open and parse %s', state_file)
        return previous_safe_lsn


def _write_lsn_state(state, logical_streams, lsn):
    for stream in logical_streams:
        state = singer.write_bookmark(state, stream['tap_stream_id'], 'lsn', lsn)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state


def wal2json_options(logical_streams, conn_info):
    """Build wal2json options; decimal routes need typmods and exact numeric text."""
    decimal_target = conn_info.get('decimal_target')
    options = {
        'format-version': '2',
        'include-transaction': 'true',
        'include-timestamp': 'true',
        'include-types': 'true' if decimal_target else 'false',
        'actions': 'insert,update,delete',
        'add-tables': streams_to_wal2json_tables(logical_streams)
    }
    if decimal_target:
        # YugabyteDB bundles a wal2json that accepts both options, unlike self-managed PostgreSQL.
        options.update({'include-typmod': 'true', 'numeric-data-types-as-string': 'true'})
    return options


def sync_tables(conn_info, logical_streams, state, end_lsn, state_file):  # noqa: C901
    target_acknowledged_lsn = _minimum_acknowledged_lsn(state, logical_streams)
    initial_stream_lsns = {
        stream['tap_stream_id']: get_bookmark(state, stream['tap_stream_id'], 'lsn')
        for stream in logical_streams
    }
    start_lsn = target_acknowledged_lsn
    lsn_to_flush = None
    time_extracted = utils.now()
    slot = locate_replication_slot(conn_info)
    last_committed_lsn = None
    lsn_currently_processing = None
    first_received_lsn = None
    lsn_processed_count = 0
    start_run_timestamp = datetime.datetime.now(datetime.UTC)
    max_run_seconds = conn_info['max_run_seconds']
    break_at_end_lsn = conn_info['break_at_end_lsn']
    logical_poll_total_seconds = conn_info['logical_poll_total_seconds']
    poll_interval = FEEDBACK_POLL_INTERVAL

    for s in logical_streams:
        sync_common.send_schema_message(
            s,
            ['lsn'],
            record_update_mode=sync_common.PATCH_RECORD_UPDATE_MODE)

    # Create replication connection and cursor
    conn = yb_db.open_connection(conn_info, logical_replication=True)
    cur = conn.cursor()

    # YugabyteDB's Postgres layer is fixed at 15.2 across supported versions, so this is
    # always safe (unlike tap-postgres, which version-gates it for pre-PG12 compatibility).
    wal_sender_timeout = 10800000  # 10800000ms = 3 hours
    LOGGER.info('Set session wal_sender_timeout = %i milliseconds', wal_sender_timeout)
    cur.execute(f"SET SESSION wal_sender_timeout = {wal_sender_timeout}")

    try:
        LOGGER.info('Request wal streaming from %s to %s (slot %s)', start_lsn, end_lsn, slot)
        # psycopg2 2.8.4 will send a keep-alive message to postgres every status_interval
        cur.start_replication(slot_name=slot,
                              decode=True,
                              start_lsn=start_lsn,
                              status_interval=poll_interval,
                              options=wal2json_options(logical_streams, conn_info))

    except psycopg2.ProgrammingError as ex:
        raise Exception(f"Unable to start replication with logical replication (slot {ex})") from ex

    lsn_received_timestamp = datetime.datetime.now(datetime.UTC)
    poll_timestamp = datetime.datetime.now(datetime.UTC)

    try:
        while True:
            # YugabyteDB never streams pg_logical_emit_message, so an idle source only ends a run here.
            poll_duration = (datetime.datetime.now(datetime.UTC) - lsn_received_timestamp).total_seconds()
            if logical_poll_total_seconds and poll_duration > logical_poll_total_seconds:
                LOGGER.info('Breaking - %i seconds of polling with no data', poll_duration)
                break

            if datetime.datetime.now(datetime.UTC) >= (start_run_timestamp
                                                      + datetime.timedelta(seconds=max_run_seconds)):
                LOGGER.info('Breaking - reached max_run_seconds of %i', max_run_seconds)
                break

            try:
                msg = cur.read_message()
            except Exception as e:
                LOGGER.error(e)
                raise

            if msg:
                # Rows share their transaction's LSN, so any message, not only a new LSN, proves activity.
                lsn_received_timestamp = datetime.datetime.now(datetime.UTC)
                if (break_at_end_lsn) and (msg.data_start > end_lsn):
                    LOGGER.info('Breaking - latest wal message %s is past end_lsn %s',
                                msg.data_start,
                                end_lsn)
                    break

                message_payload = _decode_wal_payload(msg, conn_info, slot)

                state = consume_message(
                    logical_streams,
                    state,
                    msg,
                    time_extracted,
                    conn_info,
                    message_payload=message_payload,
                    slot=slot,
                )

                if lsn_currently_processing is None:
                    lsn_currently_processing = msg.data_start
                    first_received_lsn = msg.data_start
                    LOGGER.info('First wal message received is %s', lsn_currently_processing)

                    # Flush wal up to the previous target acknowledgement, or the first LSN received.
                    lsn_to_flush = target_acknowledged_lsn
                    if lsn_currently_processing < lsn_to_flush:
                        lsn_to_flush = lsn_currently_processing
                    LOGGER.info('Confirming write up to %s, flush to %s', lsn_to_flush, lsn_to_flush)
                    cur.send_feedback(write_lsn=lsn_to_flush, flush_lsn=lsn_to_flush, reply=True, force=True)

                elif int(msg.data_start) > lsn_currently_processing:
                    lsn_currently_processing = msg.data_start

                if message_payload.get('action') == 'C':
                    last_committed_lsn = msg.data_start
                    lsn_processed_count += 1
                    if lsn_processed_count >= UPDATE_BOOKMARK_PERIOD:
                        LOGGER.debug('Updating bookmarks for all streams to lsn = %s', last_committed_lsn)
                        state = _write_lsn_state(state, logical_streams,
                                                 max(last_committed_lsn, target_acknowledged_lsn))
                        lsn_processed_count = 0
            else:
                try:
                    # Wait for a second unless a message arrives
                    select([cur], [], [], 1)
                except InterruptedError:
                    pass

            # Every poll_interval, update latest committed lsn position from the state_file
            if datetime.datetime.now(datetime.UTC) >= (poll_timestamp + datetime.timedelta(seconds=poll_interval)):
                if lsn_currently_processing is None:
                    LOGGER.info('Waiting for first wal message')
                else:
                    LOGGER.info('Latest committed wal message received was %s', last_committed_lsn)
                    target_acknowledged_lsn = _read_target_acknowledged_lsn(
                        state_file, logical_streams, target_acknowledged_lsn)
                    target_lsn_is_complete = (
                        first_received_lsn > target_acknowledged_lsn
                        or (last_committed_lsn is not None
                            and last_committed_lsn >= target_acknowledged_lsn)
                    )
                    if target_lsn_is_complete and target_acknowledged_lsn > lsn_to_flush:
                        lsn_to_flush = target_acknowledged_lsn
                        LOGGER.info('Confirming write up to %s, flush to %s', lsn_to_flush, lsn_to_flush)
                        cur.send_feedback(write_lsn=lsn_to_flush, flush_lsn=lsn_to_flush, reply=True, force=True)

                poll_timestamp = datetime.datetime.now(datetime.UTC)

    finally:
        active_error = sys.exception()
        close_error = None
        for resource in (cur, conn):
            try:
                resource.close()
            except Exception as exc:  # A close failure must not replace the replication failure.
                LOGGER.warning('Unable to close replication resource: %s', type(exc).__name__)
                if close_error is None:
                    close_error = exc
        # Row bookmarks may include an incomplete transaction; only observed commits can advance them.
        for stream_id, initial_lsn in initial_stream_lsns.items():
            checkpoint_lsn = max(initial_lsn, last_committed_lsn or 0, target_acknowledged_lsn)
            state = singer.write_bookmark(state, stream_id, 'lsn', checkpoint_lsn)
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
        if active_error is None and close_error is not None:
            raise close_error

    return state
