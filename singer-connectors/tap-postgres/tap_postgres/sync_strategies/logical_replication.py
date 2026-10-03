import base64
import binascii
import datetime
import pytz
import decimal
import psycopg2
import copy
import json
import math
import re
import singer
import time
import warnings

from dataclasses import dataclass
from select import select
from typing import NamedTuple
from psycopg2 import sql
from singer import metadata, utils, get_bookmark
from dateutil.parser import parse, UnknownTimezoneWarning, ParserError
from functools import reduce

import tap_postgres.db as post_db
from tap_postgres.pgoutput import PgoutputDecoder, PgoutputProtocolError
import tap_postgres.sync_strategies.common as sync_common
from tap_postgres.stream_utils import refresh_streams_schema

LOGGER = singer.get_logger('tap_postgres')

UPDATE_BOOKMARK_PERIOD = 10000
FEEDBACK_POLL_INTERVAL = 10
FALLBACK_DATETIME = '9999-12-31T23:59:59.999+00:00'
FALLBACK_DATE = '9999-12-31T00:00:00+00:00'
WAL_PROGRESS_MESSAGE_PREFIX = 'pipelinewise'
WAL_PROGRESS_MESSAGE_CONTENT = 'wal_progress'
PUBLICATION_FENCE_COMMENT_PREFIX = 'pipelinewise-publication-fence-v1:'
PGOUTPUT_MIGRATION_STATE_KEY = '_pipelinewise_pgoutput_migration'
PGOUTPUT_MIGRATION_STATE_VERSION = 2


class ReplicationSlotNotFoundError(Exception):
    """Custom exception when replication slot not found"""


class UnsupportedPayloadKindError(Exception):
    """Custom exception when a payload is not insert, update nor delete."""


class ReplicationSlotMigrationError(RuntimeError):
    """Raised when a wal2json slot cannot be migrated safely."""


class PreparedReplicationSlot(str):
    """Canonical pgoutput slot plus an old slot retained during migration."""

    def __new__(cls, name, migration_source=None, source_confirmed_lsn=None,
                confirmed_flush_lsn=None):
        value = super().__new__(cls, name)
        value.migration_source = migration_source
        value.source_confirmed_lsn = source_confirmed_lsn
        value.confirmed_flush_lsn = confirmed_flush_lsn
        return value


class PreparedPublication(str):
    """Publication name plus physical-table mappings used during migration."""

    def __new__(cls, name, wal2json_tables=(), wal2json_aliases=None,
                pgoutput_aliases=None, has_partition_roots=False,
                expected_primary_keys=None):
        value = super().__new__(cls, name)
        value.wal2json_tables = tuple(wal2json_tables)
        value.wal2json_aliases = wal2json_aliases or {}
        value.pgoutput_aliases = pgoutput_aliases or {}
        value.has_partition_roots = has_partition_roots
        value.expected_primary_keys = expected_primary_keys or {}
        return value


def lsn_to_int(lsn):
    """Convert pg_lsn to int"""

    if not lsn:
        return None

    file, index = lsn.split('/')
    lsni = (int(file, 16) << 32) + int(index, 16)
    return lsni


def int_to_lsn(lsni):
    """Convert int to pg_lsn"""

    if lsni is None:
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


def fetch_current_lsn(conn_config):
    return post_db.capture_snapshot_boundary(conn_config)


def emit_wal_progress_message(conn_info):
    """Emit a transactional marker through the portable three-argument API."""
    availability_query = """
        WITH function_check AS (
            SELECT COALESCE(
                pg_catalog.to_regprocedure(
                    'pg_catalog.pg_logical_emit_message(boolean,text,text,boolean)'
                ),
                pg_catalog.to_regprocedure(
                    'pg_catalog.pg_logical_emit_message(boolean,text,text)'
                )
            ) AS function_oid
        )
        SELECT CASE
                   WHEN function_oid IS NULL THEN FALSE
                   ELSE pg_catalog.has_function_privilege(current_user, function_oid, 'EXECUTE')
               END
          FROM function_check
    """

    conn = None
    try:
        conn = post_db.open_connection(conn_info, False, True)
        with conn:
            with conn.cursor() as cur:
                cur.execute(availability_query)
                available = cur.fetchone()
                if not available or available[0] is not True:
                    LOGGER.debug('Logical WAL progress messages are unavailable')
                    return None

                cur.execute(
                    'SELECT pg_catalog.pg_logical_emit_message(TRUE, %s, %s)',
                    (WAL_PROGRESS_MESSAGE_PREFIX, WAL_PROGRESS_MESSAGE_CONTENT)
                )
                emitted_lsn = cur.fetchone()
        marker_lsn = lsn_to_int(emitted_lsn[0]) if emitted_lsn else None
        return marker_lsn if marker_lsn is not None and marker_lsn > 0 else None
    except (
            psycopg2.errors.InsufficientPrivilege,
            psycopg2.errors.UndefinedFunction):
        LOGGER.debug('Logical WAL progress messages are unavailable')
        return None
    except psycopg2.Error as ex:
        LOGGER.warning('Unable to emit a logical WAL progress message; continuing without it: %s', ex)
        return None
    finally:
        if conn is not None:
            conn.close()


def wait_for_replica_replay(conn_info, boundary_lsn, connection=None):
    """Wait until a secondary snapshot can contain everything before its bookmark."""
    if not conn_info.get('use_secondary'):
        return
    timeout_seconds = conn_info.get('publication_fence_timeout_seconds', 300)
    deadline = time.monotonic() + timeout_seconds
    conn = connection if connection is not None else post_db.open_connection(conn_info)
    try:
        with conn.cursor() as cur:
            while True:
                cur.execute('SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn()::text')
                in_recovery, replay_lsn = cur.fetchone()
                if not in_recovery or (lsn_to_int(replay_lsn) or 0) >= boundary_lsn:
                    return
                if time.monotonic() >= deadline:
                    raise ReplicationSlotMigrationError(
                        'Timed out waiting for the secondary to replay the LOG_BASED snapshot '
                        f'boundary {int_to_lsn(boundary_lsn)}; last replay LSN is {replay_lsn}'
                    )
                time.sleep(0.1)
    finally:
        if connection is None:
            conn.close()


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


def create_array_elem(elem, sql_datatype, conn_info):  # noqa: C901
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

            cur.execute(f'SELECT %s::{cast_datatype}', (elem,))
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
        if isinstance(elem, str):
            if elem.lower() in {'t', 'true', '1'}:
                return True
            if elem.lower() in {'f', 'false', '0'}:
                return False
        return elem
    if sql_datatype == 'hstore':
        return create_hstore_elem(conn_info, elem)
    if 'numeric' in sql_datatype:
        numeric_value = decimal.Decimal(elem)
        return numeric_value if numeric_value.is_finite() else None
    if sql_datatype in {'smallint', 'integer', 'bigint'}:
        return int(elem)
    if sql_datatype in {'real', 'double precision'}:
        float_value = float(elem)
        return float_value if math.isfinite(float_value) else None
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


def _validate_pgoutput_relation_identity(message_payload, key_properties):
    """Fail if decoded pgoutput identity differs from the Singer merge key."""
    if not message_payload.get('_pgoutput'):
        return
    relation_keys = {
        column['name']
        for column in message_payload.get('relation_columns', ())
        if column.get('is_key')
    }
    if relation_keys != set(key_properties):
        raise ReplicationSlotMigrationError(
            'The decoded pgoutput replica identity does not match the Singer target merge key: '
            f'{sorted(relation_keys)} != {sorted(key_properties)}. A full resync is required.'
        )


def _old_primary_key_for_changed_update(message_payload, key_properties, desired_columns):
    """Return the old primary key when an UPDATE moves a row to a new target key."""
    identity = message_payload.get('identity')
    if not message_payload.get('_pgoutput') or message_payload.get('action') != 'U' or identity is None:
        return None

    old_values = {column['name']: column['value'] for column in identity}
    new_values = {column['name']: column['value'] for column in message_payload['columns']}
    missing_keys = set(key_properties).difference(old_values).union(
        set(key_properties).difference(new_values))
    if missing_keys:
        raise ReplicationSlotMigrationError(
            'A pgoutput UPDATE did not contain every Singer target merge key column: '
            f'{sorted(missing_keys)}. A full resync is required.'
        )
    if all(old_values[key] == new_values[key] for key in key_properties):
        return None

    historical_columns = {
        column['name'] for column in message_payload.get('relation_columns', [])
    }
    missing_values = (
        set(desired_columns).intersection(historical_columns or desired_columns)
        .difference(new_values)
        .difference({'_sdc_deleted_at', '_sdc_lsn'})
    )
    if missing_values:
        raise ReplicationSlotMigrationError(
            'A primary-key-changing pgoutput UPDATE omitted unchanged selected columns '
            f'{sorted(missing_values)}. The row cannot be moved without losing values; '
            'a full resync is required.'
        )
    return [old_values[key] for key in key_properties]


def _write_old_primary_key_delete(
        message_payload, key_properties, desired_columns, target_stream,
        stream_version, time_extracted, stream_md_map, conn_info, lsn):
    """Delete the old target key immediately before writing a moved row."""
    old_primary_key = _old_primary_key_for_changed_update(
        message_payload, key_properties, desired_columns)
    if old_primary_key is None:
        return

    delete_names = list(key_properties) + ['_sdc_deleted_at']
    delete_values = old_primary_key + [singer.utils.strftime(time_extracted)]
    if conn_info.get('debug_lsn'):
        delete_names.append('_sdc_lsn')
        delete_values.append(str(lsn))
    singer.write_message(row_to_singer_message(
        target_stream,
        delete_values,
        stream_version,
        delete_names,
        time_extracted,
        stream_md_map,
        conn_info,
    ))


def _refresh_changed_relation(stream, payload, conn_info):
    """Refresh changed relation metadata on primary before emitting its next record."""
    new_columns = set()
    if payload.get('action') in {'I', 'U'}:
        relation_columns = payload.get('relation_columns', payload['columns'])
        new_columns = {column['name'] for column in relation_columns}.difference(stream['schema']['properties'])
    if not new_columns and not payload.get('schema_changed'):
        return

    LOGGER.info('Detected relation change%s, refreshing schema of stream %s',
                f' with new columns {new_columns}' if new_columns else '', stream['stream'])
    refresh_streams_schema({**conn_info, 'use_secondary': False}, [stream])
    add_automatic_properties(stream, conn_info.get('debug_lsn', False))
    sync_common.send_schema_message(stream, ['lsn'], record_update_mode=sync_common.PATCH_RECORD_UPDATE_MODE)


def consume_message(streams, state, msg, time_extracted, conn_info, *, message_payload=None):
    """Convert selected row changes, preserving omitted pgoutput values as PATCHes."""
    if message_payload is None:
        try:
            message_payload = json.loads(msg.payload)
        except Exception:
            return state

    lsn = message_payload.get('transaction_lsn') or msg.data_start

    action = message_payload.get('action')
    if action not in {'I', 'U', 'D'}:
        LOGGER.debug('Skipping non-row pgoutput message: action=%s, lsn=%s', action,
                     int_to_lsn(lsn) if isinstance(lsn, int) else lsn)
        return state

    streams_lookup = {s['tap_stream_id']: s for s in streams}

    tap_stream_id = post_db.compute_tap_stream_id(message_payload['schema'], message_payload['table'])
    if streams_lookup.get(tap_stream_id) is None:
        return state

    target_stream = streams_lookup[tap_stream_id]
    original_stream_md_map = metadata.to_map(target_stream['metadata'])
    key_properties = original_stream_md_map.get((), {}).get('table-key-properties', [])
    _validate_pgoutput_relation_identity(message_payload, key_properties)

    _refresh_changed_relation(target_stream, message_payload, conn_info)

    stream_version = get_stream_version(target_stream['tap_stream_id'], state)
    stream_md_map = metadata.to_map(target_stream['metadata'])

    desired_columns = {c for c in target_stream['schema']['properties'].keys() if sync_common.should_sync_column(
        stream_md_map, c)}

    _write_old_primary_key_delete(
        message_payload,
        key_properties,
        desired_columns,
        target_stream,
        stream_version,
        time_extracted,
        stream_md_map,
        conn_info,
        lsn,
    )

    columns = message_payload['identity' if action == 'D' else 'columns']
    selected_columns = [column for column in columns if column['name'] in desired_columns]
    col_names = [column['name'] for column in selected_columns]
    col_vals = [column['value'] for column in selected_columns]
    col_names.append('_sdc_deleted_at')
    col_vals.append(singer.utils.strftime(time_extracted) if action == 'D' else None)

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
    message_payload['_record_emitted'] = True
    if not message_payload.get('_pgoutput'):
        state = singer.write_bookmark(state, target_stream['tap_stream_id'], 'lsn', lsn)

    return state


def _replication_identifier(*parts):
    return re.sub('[^a-z0-9_]', '_', '_'.join(parts).lower())


def validate_tap_id(tap_id):
    if (not isinstance(tap_id, str)
            or not re.fullmatch(r'[a-z0-9_]+', tap_id)
            or len(tap_id) > 50):
        raise ValueError('tap_id must match ^[a-z0-9_]+$ and be at most 50 characters')
    return tap_id


def generate_replication_slot_name(tap_id, prefix='ppw_slot'):
    """Return the canonical tap-scoped pgoutput replication slot name."""
    validate_tap_id(tap_id)
    return _replication_identifier(prefix, tap_id)


def generate_publication_name(tap_id):
    """Return the publication used by one tap in the current database."""
    return generate_replication_slot_name(tap_id)


def legacy_replication_slot_names(dbname, tap_id):
    """Return the database-wide and tap-specific historical slot names."""
    return [
        _replication_identifier('pipelinewise', dbname)[:63],
        _replication_identifier('pipelinewise', dbname, tap_id)[:63],
    ]


def _implicit_historical_slot_is_truncated(dbname, tap_id, previous_tap_id):
    """Return whether an automatically inferred historical name is non-injective."""
    return (
        previous_tap_id is None
        and len(_replication_identifier('pipelinewise', dbname, tap_id)) > 63
    )


def _slot_rows(cursor, names):
    cursor.execute(
        """
        SELECT slot_name, plugin, slot_type, database, active, confirmed_flush_lsn::text
          FROM pg_catalog.pg_replication_slots
         WHERE slot_name = ANY(%s)
        """,
        (names,),
    )
    return {
        row[0]: {
            'plugin': row[1],
            'slot_type': row[2],
            'database': row[3],
            'active': row[4],
            'confirmed_flush_lsn': row[5],
        }
        for row in cursor.fetchall()
    }


def _validate_slot(slot_name, slot, dbname, expected_plugin):
    if slot['slot_type'] != 'logical' or slot['database'] != dbname:
        raise ReplicationSlotMigrationError(
            f'Replication slot {slot_name} must be a logical slot for database {dbname}'
        )
    if slot['plugin'] != expected_plugin:
        raise ReplicationSlotMigrationError(
            f'Replication slot {slot_name} uses {slot["plugin"]}, expected {expected_plugin}'
        )
    if slot['active']:
        raise ReplicationSlotMigrationError(f'Replication slot {slot_name} is already active')


def _validate_replication_slot_candidates(
        cursor, dbname, tap_id, allow_wal2json_migration=True,
        fresh_start=False, previous_tap_id=None, final_log_deselection=False):
    """Read and validate every tap-owned slot candidate without mutating it."""
    destination = generate_replication_slot_name(tap_id)
    legacy_names = legacy_replication_slot_names(dbname, previous_tap_id or tap_id)
    rows = _slot_rows(cursor, list(dict.fromkeys([destination, *legacy_names])))

    database_wide_slot, tap_specific_slot = legacy_names
    ambiguous_legacy = database_wide_slot == tap_specific_slot
    if (ambiguous_legacy and tap_specific_slot in rows and destination not in rows
            and not fresh_start and not final_log_deselection):
        raise ReplicationSlotMigrationError(
            'The historical database-wide and tap-specific slot names collide after '
            'PostgreSQL truncation; their ownership must be resolved before migration'
        )
    destination_row = rows.get(destination)
    if destination_row and destination_row['plugin'] != 'pgoutput':
        if destination in legacy_names and destination_row['plugin'] == 'wal2json':
            raise ReplicationSlotMigrationError(
                f'Legacy wal2json slot {destination} collides with the required pgoutput slot name; '
                'rename the tap or migrate the slot explicitly'
            )
        raise ReplicationSlotMigrationError(
            f'Replication slot {destination} uses {destination_row["plugin"]}, expected pgoutput'
        )
    if destination_row:
        _validate_slot(destination, destination_row, dbname, 'pgoutput')

    migration_source = tap_specific_slot if (
        not ambiguous_legacy and tap_specific_slot != destination and tap_specific_slot in rows
    ) else None

    if migration_source is not None:
        implicit_truncated_source = _implicit_historical_slot_is_truncated(
            dbname, tap_id, previous_tap_id)
        if implicit_truncated_source and (
                destination_row is not None or fresh_start or final_log_deselection):
            LOGGER.warning(
                'Preserving implicitly truncated historical slot %s without claiming it',
                migration_source,
            )
            migration_source = None
        elif implicit_truncated_source:
            raise ReplicationSlotMigrationError(
                f'Historical tap-specific slot {migration_source} has an implicitly truncated '
                'name that may belong to another tap. Verify ownership and set previous_tap_id '
                'explicitly before migration.'
            )
        if migration_source is not None:
            _validate_slot(migration_source, rows[migration_source], dbname, 'wal2json')
        if migration_source is not None and not allow_wal2json_migration and not fresh_start:
            raise ReplicationSlotMigrationError(
                'Automatic wal2json migration is not supported when a selected relation is a '
                'partition root because partitions can change while the bridge is running. '
                'Complete a full resync into a fresh pgoutput slot instead.'
            )

    if (destination_row is None and migration_source is None
            and database_wide_slot in rows and not fresh_start and not final_log_deselection):
        raise ReplicationSlotMigrationError(
            f'Historical database-wide slot {database_wide_slot} may be shared by '
            'multiple taps and cannot be migrated automatically. A DBA must migrate '
            f'it to a dedicated tap-specific slot {tap_specific_slot} before '
            'pgoutput migration.'
        )

    return destination, migration_source, rows


def locate_replication_slot_by_cur(cursor, dbname, tap_id, allow_wal2json_migration=True,
                                   previous_tap_id=None):
    """Resolve or create the canonical pgoutput slot beside legacy history."""
    destination, migration_source, rows = _validate_replication_slot_candidates(
        cursor,
        dbname,
        tap_id,
        allow_wal2json_migration=allow_wal2json_migration,
        previous_tap_id=previous_tap_id,
    )
    destination_row = rows.get(destination)

    if destination_row is None:
        if migration_source is None:
            raise ReplicationSlotNotFoundError(
                f'Unable to find pgoutput slot {destination} or a legacy wal2json slot to migrate'
            )
        cursor.execute(
            'SELECT slot_name, lsn::text FROM pg_catalog.pg_create_logical_replication_slot(%s, %s)',
            (destination, 'pgoutput'),
        )
        created_slot, created_lsn = cursor.fetchone()
        if created_slot != destination:
            raise ReplicationSlotMigrationError(
                f'PostgreSQL created unexpected slot {created_slot} instead of {destination}'
            )
        source_confirmed_lsn = lsn_to_int(
            rows[migration_source]['confirmed_flush_lsn'])
        confirmed_flush_lsn = lsn_to_int(created_lsn)
        LOGGER.info('Created pgoutput replication slot %s at %s beside wal2json slot %s',
                    destination, created_lsn, migration_source)
    else:
        confirmed_flush_lsn = lsn_to_int(
            destination_row['confirmed_flush_lsn'])
        source_confirmed_lsn = (
            lsn_to_int(rows[migration_source]['confirmed_flush_lsn'])
            if migration_source else None
        )

    LOGGER.info('Using pgoutput replication slot %s', destination)
    return PreparedReplicationSlot(
        destination,
        migration_source,
        source_confirmed_lsn,
        confirmed_flush_lsn,
    )


def locate_replication_slot(conn_info, allow_wal2json_migration=True):
    with post_db.open_connection(conn_info, False, True) as conn:
        with conn.cursor() as cur:
            return locate_replication_slot_by_cur(
                cur,
                conn_info['dbname'],
                conn_info['tap_id'],
                allow_wal2json_migration=allow_wal2json_migration,
                previous_tap_id=conn_info.get('previous_tap_id'),
            )


def _wait_for_prepublication_transactions(conn_info):
    """Fence transactions that wrote before the publication change committed."""
    timeout_seconds = conn_info.get('publication_fence_timeout_seconds', 300)
    deadline = time.monotonic() + timeout_seconds
    conn = post_db.open_connection(conn_info, False, True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT lock.virtualxid
                  FROM pg_catalog.pg_locks AS lock
                  JOIN pg_catalog.pg_stat_activity AS activity
                    ON activity.pid = lock.pid
                 WHERE lock.locktype = 'virtualxid'
                   AND lock.mode = 'ExclusiveLock'
                   AND lock.granted
                   AND activity.datname = pg_catalog.current_database()
                   AND activity.backend_xid IS NOT NULL
                   AND lock.pid <> pg_catalog.pg_backend_pid()
                """
            )
            virtual_xids = [row[0] for row in cur.fetchall()]

            while True:
                if virtual_xids:
                    cur.execute(
                        """
                        SELECT virtualxid
                          FROM pg_catalog.pg_locks
                         WHERE locktype = 'virtualxid'
                           AND mode = 'ExclusiveLock'
                           AND granted
                           AND virtualxid = ANY(%s)
                        """,
                        (virtual_xids,),
                    )
                    remaining = [row[0] for row in cur.fetchall()]
                else:
                    remaining = []
                cur.execute(
                    """
                    SELECT gid
                      FROM pg_catalog.pg_prepared_xacts
                     WHERE database = pg_catalog.current_database()
                    """
                )
                prepared = [row[0] for row in cur.fetchall()]
                if prepared:
                    raise ReplicationSlotMigrationError(
                        'Cannot establish the publication fence while prepared transactions '
                        f'exist in database {conn_info["dbname"]}: {prepared}'
                    )
                if not remaining:
                    return
                if time.monotonic() >= deadline:
                    raise ReplicationSlotMigrationError(
                        'Timed out waiting for writing transactions that predate pgoutput publication setup: '
                        f'{remaining}'
                    )
                LOGGER.debug('Waiting for pre-publication writing transactions: %s', remaining)
                time.sleep(0.5)
    finally:
        conn.close()


def _validate_publication_tables(cur, tables, _server_version=None):
    """Validate relations and expand partition roots for the wal2json bridge."""
    schemas = list({schema_name for schema_name, _ in tables})
    names = list({table_name for _, table_name in tables})
    cur.execute(
        """
        SELECT namespace.nspname,
               relation.relname,
               relation.relkind,
               COALESCE(root_namespace.nspname, namespace.nspname),
               COALESCE(root.relname, relation.relname)
          FROM pg_catalog.pg_class AS relation
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          LEFT JOIN pg_catalog.pg_class AS root
            ON root.oid = pg_catalog.pg_partition_root(relation.oid)
          LEFT JOIN pg_catalog.pg_namespace AS root_namespace
            ON root_namespace.oid = root.relnamespace
         WHERE namespace.nspname = ANY(%s)
           AND relation.relname = ANY(%s)
        """,
        (schemas, names),
    )
    requested = set(tables)
    relation_info = {
        (schema_name, table_name): (relation_kind, (root_schema, root_table))
        for schema_name, table_name, relation_kind, root_schema, root_table in cur.fetchall()
        if (schema_name, table_name) in requested
    }
    missing = requested.difference(relation_info)
    if missing:
        raise ReplicationSlotMigrationError(
            f'Publication relations do not exist: {sorted(missing)}'
        )

    unsupported = [table for table, (kind, _) in relation_info.items() if kind not in {'r', 'p'}]
    if unsupported:
        raise ReplicationSlotMigrationError(
            f'LOG_BASED replication supports only persistent and partitioned tables: {unsupported}'
        )

    partitioned = {table for table, (kind, _) in relation_info.items() if kind == 'p'}
    cur.execute(
        """
        WITH RECURSIVE selected_roots AS (
            SELECT relation.oid AS root_oid,
                   namespace.nspname AS root_schema,
                   relation.relname AS root_table
              FROM pg_catalog.pg_class AS relation
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = ANY(%s)
               AND relation.relname = ANY(%s)
               AND relation.relkind IN ('p', 'r')
        ), partition_tree AS (
            SELECT root_oid, root_schema, root_table, root_oid AS relation_oid
              FROM selected_roots
            UNION ALL
            SELECT tree.root_oid,
                   tree.root_schema,
                   tree.root_table,
                   inheritance.inhrelid
              FROM partition_tree AS tree
              JOIN pg_catalog.pg_inherits AS inheritance
                ON inheritance.inhparent = tree.relation_oid
        )
        SELECT tree.root_schema,
               tree.root_table,
               namespace.nspname,
               relation.relname,
               relation.relkind
          FROM partition_tree AS tree
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = tree.relation_oid
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE tree.relation_oid <> tree.root_oid
        """,
        (schemas, names),
    )
    descendants = {}
    leaves = {}
    unsupported_partition_descendants = {}
    inherited_ordinary_tables = set()
    for root_schema, root_table, child_schema, child_table, child_kind in cur.fetchall():
        root = (root_schema, root_table)
        child = (child_schema, child_table)
        if root not in requested:
            continue
        descendants.setdefault(root, set()).add(child)
        root_kind = relation_info[root][0]
        if root_kind == 'r':
            inherited_ordinary_tables.add(root)
        elif child_kind not in {'p', 'r'}:
            unsupported_partition_descendants.setdefault(root, set()).add(
                (child_schema, child_table, child_kind))
        if child_kind == 'r':
            leaves.setdefault(root, set()).add(child)

    if inherited_ordinary_tables:
        raise ReplicationSlotMigrationError(
            'LOG_BASED replication does not support selected ordinary tables with '
            f'inheritance descendants: {sorted(inherited_ordinary_tables)}'
        )
    if unsupported_partition_descendants:
        unsupported = {
            root: sorted(children)
            for root, children in unsupported_partition_descendants.items()
        }
        raise ReplicationSlotMigrationError(
            'LOG_BASED replication supports only persistent and partitioned descendants '
            f'under selected partition roots: {unsupported}'
        )

    duplicate_selections = {
        root: sorted(descendants.get(root, set()).intersection(requested))
        for root in partitioned
        if descendants.get(root, set()).intersection(requested)
    }
    if duplicate_selections:
        raise ReplicationSlotMigrationError(
            'Do not select a partition parent and any descendant together: '
            f'{duplicate_selections}'
        )

    wal2json_tables = []
    aliases = {}
    for table in tables:
        if table in partitioned:
            for leaf in sorted(leaves.get(table, set())):
                if leaf in aliases and aliases[leaf] != table:
                    raise ReplicationSlotMigrationError(
                        f'Partition {leaf} maps to multiple selected parents'
                    )
                aliases[leaf] = table
                wal2json_tables.append(leaf)
        else:
            wal2json_tables.append(table)

    # PostgreSQL 14+ publishes partition roots directly. The leaf expansion is
    # retained only for draining a historical wal2json slot during migration.
    return tables, bool(partitioned), wal2json_tables, aliases, {}


def _reject_selected_generated_columns(cur, logical_streams):
    selected_columns = {}
    for stream in logical_streams:
        stream_metadata = metadata.to_map(stream['metadata'])
        table = (stream_metadata.get(())['schema-name'], stream['table_name'])
        selected_columns[table] = {
            column
            for column in stream['schema']['properties']
            if sync_common.should_sync_column(stream_metadata, column)
        }

    schemas = list({schema for schema, _ in selected_columns})
    tables = list({table for _, table in selected_columns})
    cur.execute(
        """
        SELECT namespace.nspname, relation.relname, attribute.attname
          FROM pg_catalog.pg_attribute AS attribute
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = attribute.attrelid
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = ANY(%s)
           AND relation.relname = ANY(%s)
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
           AND attribute.attgenerated <> ''
        """,
        (schemas, tables),
    )
    generated = [
        (schema, table, column)
        for schema, table, column in cur.fetchall()
        if column in selected_columns.get((schema, table), set())
    ]
    if generated:
        raise ReplicationSlotMigrationError(
            'LOG_BASED replication does not support selected stored generated columns: '
            f'{generated}'
        )


def _validate_replica_identity(cur, physical_tables, expected_primary_keys=None):
    """Require source identity and catalog merge keys to use the same primary key."""
    if not physical_tables:
        return
    schemas = list({schema for schema, _ in physical_tables})
    tables = list({table for _, table in physical_tables})
    cur.execute(
        """
        SELECT namespace.nspname,
               relation.relname,
               relation.relreplident,
               COALESCE((
                   SELECT source_index.indisvalid
                          AND source_index.indisready
                          AND source_index.indislive
                          AND source_index.indimmediate
                     FROM pg_catalog.pg_index AS source_index
                    WHERE source_index.indrelid = relation.oid
                      AND source_index.indisprimary
               ), FALSE),
               COALESCE((
                   SELECT pg_catalog.array_agg(attribute.attname ORDER BY key_column.ordinality)
                     FROM pg_catalog.pg_index AS source_index
                     CROSS JOIN LATERAL pg_catalog.unnest(source_index.indkey)
                         WITH ORDINALITY AS key_column(attnum, ordinality)
                     JOIN pg_catalog.pg_attribute AS attribute
                       ON attribute.attrelid = relation.oid
                      AND attribute.attnum = key_column.attnum
                    WHERE source_index.indrelid = relation.oid
                      AND source_index.indisprimary
                      AND key_column.ordinality <= source_index.indnkeyatts
               ), ARRAY[]::name[])
          FROM pg_catalog.pg_class AS relation
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = ANY(%s)
           AND relation.relname = ANY(%s)
        """,
        (schemas, tables),
    )
    requested = set(physical_tables)
    identities = {
        (schema, table): (replica_identity, has_usable_index, tuple(primary_keys))
        for schema, table, replica_identity, has_usable_index, primary_keys in cur.fetchall()
        if (schema, table) in requested
    }
    invalid = sorted(
        table
        for table in requested
        if table not in identities
        or identities[table][0] != 'd'
        or identities[table][1] is not True
        or (
            expected_primary_keys is not None
            and set(expected_primary_keys.get(table, ())) != set(identities[table][2])
        )
    )
    if invalid:
        raise ReplicationSlotMigrationError(
            'LOG_BASED pgoutput requires REPLICA IDENTITY DEFAULT backed by a valid non-deferrable primary key '
            'so source row identity matches the Singer target merge key. Add a valid primary key '
            'and set REPLICA IDENTITY DEFAULT on: '
            f'{invalid}'
        )


def _validate_replica_identity_tables(
        conn_info, physical_tables, expected_primary_keys=None):
    """Validate replica identity using a short, read-only control connection."""
    with post_db.open_connection(conn_info, False, True) as conn:
        with conn.cursor() as cur:
            _validate_replica_identity(
                cur, physical_tables, expected_primary_keys)


def _encode_publication_fence_comment(state, original_comment, managed_tables=()):
    payload = json.dumps({
        'state': state,
        'original_comment': original_comment,
        'managed_tables': [list(table) for table in sorted(managed_tables)],
    }, separators=(',', ':')).encode()
    return PUBLICATION_FENCE_COMMENT_PREFIX + base64.urlsafe_b64encode(payload).decode()


def _decode_publication_fence_comment(comment):
    if not isinstance(comment, str) or not comment.startswith(PUBLICATION_FENCE_COMMENT_PREFIX):
        return None, comment, set()
    try:
        payload = comment.removeprefix(PUBLICATION_FENCE_COMMENT_PREFIX)
        decoded = json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
        expected_keys = {'state', 'original_comment'}
        if isinstance(decoded, dict) and 'managed_tables' in decoded:
            expected_keys.add('managed_tables')
        managed_tables = decoded.get('managed_tables', []) if isinstance(decoded, dict) else []
        if not isinstance(decoded, dict) \
                or set(decoded) != expected_keys \
                or decoded['state'] not in {'pending', 'ready'} or (
                decoded['original_comment'] is not None
                and not isinstance(decoded['original_comment'], str)) or (
                not isinstance(managed_tables, list)
                or any(
                    not isinstance(table, list)
                    or len(table) != 2
                    or any(not isinstance(part, str) or not part for part in table)
                    for table in managed_tables
                )):
            raise ValueError('invalid original publication comment')
        return decoded['state'], decoded['original_comment'], {
            tuple(table) for table in managed_tables
        }
    except (binascii.Error, TypeError, UnicodeError, ValueError) as ex:
        raise ReplicationSlotMigrationError(
            'The pgoutput publication has an invalid pending transaction-fence comment'
        ) from ex


def _set_publication_comment(cur, publication, comment):
    value = sql.SQL('NULL') if comment is None else sql.Literal(comment)
    cur.execute(sql.SQL('COMMENT ON PUBLICATION {} IS {}').format(
        sql.Identifier(publication), value))


def _raise_frozen_publication_change(publication):
    raise ReplicationSlotMigrationError(
        f'Publication {publication} selection or options cannot change while automatic '
        'wal2json-to-pgoutput migration is in progress. Finish the migration, or revert '
        'the LOG_BASED selection and re-run import_config. An unfiltered whole-tap '
        'FastSync can explicitly reset the migration.'
    )


class _PublicationSettings(NamedTuple):
    """Publication options read from PostgreSQL in catalog query order."""

    all_tables: bool
    insert: bool
    update: bool
    delete: bool
    truncate: bool
    via_root: bool
    comment: str | None


@dataclass
class _PublicationUpdate:
    """Publication metadata to persist before and after the transaction fence."""

    original_comment: str | None
    managed_tables: set
    needs_fence: bool


def _publication_table_list(tables):
    return sql.SQL(', ').join(
        sql.SQL('{}.{}').format(sql.Identifier(schema), sql.Identifier(table))
        for schema, table in tables
    )


def _validate_publication_options(cur, publication, settings, server_version):
    """Reject publication filters and operations the receiver cannot preserve."""
    if not all((settings.insert, settings.update, settings.delete)):
        raise ReplicationSlotMigrationError(
            f'Publication {publication} must publish insert, update, and delete'
        )
    if settings.truncate:
        raise ReplicationSlotMigrationError(
            f'Publication {publication} must not publish truncate'
        )
    if settings.all_tables:
        raise ReplicationSlotMigrationError(
            f'Publication {publication} must list exactly the selected tables'
        )
    if server_version >= 150000:
        cur.execute(
            """
            SELECT 1
              FROM pg_catalog.pg_publication_namespace AS publication_namespace
              JOIN pg_catalog.pg_publication AS publication
                ON publication.oid = publication_namespace.pnpubid
             WHERE publication.pubname = %s
             LIMIT 1
            """,
            (publication,),
        )
        if cur.fetchone() is not None:
            raise ReplicationSlotMigrationError(
                f'Publication {publication} must not publish whole schemas'
            )
        cur.execute(
            """
            SELECT 1
              FROM pg_catalog.pg_publication_rel AS publication_relation
              JOIN pg_catalog.pg_publication AS publication
                ON publication.oid = publication_relation.prpubid
             WHERE publication.pubname = %s
               AND (publication_relation.prattrs IS NOT NULL
                    OR publication_relation.prqual IS NOT NULL)
             LIMIT 1
            """,
            (publication,),
        )
        if cur.fetchone() is not None:
            raise ReplicationSlotMigrationError(
                f'Publication {publication} must not use column lists or row filters'
            )


def _reconcile_existing_publication(
        cur, publication, settings, publication_tables, server_version, *,
        fresh_start, publication_frozen, reconcile, final_log_deselection):
    """Reconcile owned members without claiming untracked DBA tables."""
    publication_fence_state, original_comment, managed_tables = _decode_publication_fence_comment(settings.comment)
    if reconcile and not publication_tables and publication_fence_state is None:
        if publication_frozen:
            _raise_frozen_publication_change(publication)
        return None

    needs_transaction_fence = publication_fence_state != 'ready'
    _validate_publication_options(cur, publication, settings, server_version)
    cur.execute(
        """
        SELECT namespace.nspname, relation.relname
          FROM pg_catalog.pg_publication_rel AS publication_relation
          JOIN pg_catalog.pg_publication AS publication
            ON publication.oid = publication_relation.prpubid
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = publication_relation.prrelid
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE publication.pubname = %s
        """,
        (publication,),
    )
    existing_tables = set(cur.fetchall())
    desired_tables = set(publication_tables)
    added_tables = desired_tables.difference(existing_tables)
    missing_managed = added_tables.intersection(managed_tables)
    if missing_managed and not fresh_start:
        raise ReplicationSlotMigrationError(
            f'Previously managed publication tables disappeared: {sorted(missing_managed)}. '
            'Run an unfiltered whole-tap FastSync before continuing replication.'
        )
    removed_tables = managed_tables.difference(desired_tables)
    explicit_removed_tables = removed_tables.intersection(existing_tables)
    frozen_publication_change = (
            publication_fence_state != 'ready'
            or desired_tables != managed_tables
            or added_tables
            or (reconcile and explicit_removed_tables)
            or not settings.via_root)
    final_retirement = (
        final_log_deselection
        and reconcile
        and publication_fence_state == 'ready'
        and not desired_tables
        and not added_tables
        and settings.via_root
    )
    if publication_frozen and frozen_publication_change and not final_retirement:
        _raise_frozen_publication_change(publication)
    if not settings.via_root:
        cur.execute(sql.SQL(
            'ALTER PUBLICATION {} SET (publish_via_partition_root = true)'
        ).format(sql.Identifier(publication)))
        needs_transaction_fence = True
    if added_tables:
        added_table_list = _publication_table_list(sorted(added_tables))
        cur.execute(
            sql.SQL('ALTER PUBLICATION {} ADD TABLE {}').format(
                sql.Identifier(publication), added_table_list
            )
        )
        needs_transaction_fence = True
    if reconcile:
        if explicit_removed_tables:
            removed_table_list = _publication_table_list(sorted(explicit_removed_tables))
            cur.execute(
                sql.SQL('ALTER PUBLICATION {} DROP TABLE {}').format(
                    sql.Identifier(publication), removed_table_list
                )
            )
            needs_transaction_fence = True
        next_managed_tables = desired_tables
    else:
        next_managed_tables = managed_tables.union(desired_tables)
    if next_managed_tables != managed_tables:
        managed_tables = next_managed_tables
        needs_transaction_fence = True
    return _PublicationUpdate(original_comment, managed_tables, needs_transaction_fence)


def _create_publication(cur, publication, publication_tables, *, publication_frozen, has_slot_history):
    """Create a publication only when no retained pgoutput history depends on it."""
    if publication_frozen:
        _raise_frozen_publication_change(publication)
    if has_slot_history:
        raise ReplicationSlotMigrationError(
            'The pgoutput publication disappeared while its slot retained history. '
            'Run an unfiltered whole-tap FastSync to restore publication continuity.'
        )
    options = "publish = 'insert, update, delete', publish_via_partition_root = true"
    cur.execute(sql.SQL('CREATE PUBLICATION {} FOR TABLE {} WITH ({})').format(
        sql.Identifier(publication), _publication_table_list(publication_tables), sql.SQL(options)))
    return _PublicationUpdate(None, set(publication_tables), True)


def _finish_publication_fence(conn_info, publication, update):
    """Mark publication metadata ready only after earlier writing transactions end."""
    _wait_for_prepublication_transactions(conn_info)
    try:
        with post_db.open_connection(conn_info, False, True) as conn:
            with conn.cursor() as cur:
                _set_publication_comment(
                    cur, publication,
                    _encode_publication_fence_comment('ready', update.original_comment, update.managed_tables),
                )
    except psycopg2.errors.InsufficientPrivilege as ex:
        raise ReplicationSlotMigrationError(
            f'Publication {publication} transaction fence completed, but user '
            f'{conn_info["user"]} cannot clear its pending fence comment'
        ) from ex


def _validate_publication_selection(logical_streams, state, reconcile, final_log_deselection):
    """Require durable removal of logical state before retiring the final selection."""
    if not logical_streams and not reconcile:
        raise ValueError('Cannot create a pgoutput publication without selected tables')
    if final_log_deselection and (logical_streams or not reconcile):
        raise ValueError('Final LOG deselection requires an empty selection and reconciliation')
    if final_log_deselection:
        retirement_state = state or {}
        bookmarks = retirement_state.get('bookmarks', {})
        if (
                PGOUTPUT_MIGRATION_STATE_KEY in retirement_state
                or '_pipelinewise_pgoutput_fresh_start' in retirement_state
                or not isinstance(bookmarks, dict)
                or any(not isinstance(bookmark, dict) or 'lsn' in bookmark
                       for bookmark in bookmarks.values())):
            raise ReplicationSlotMigrationError(
                'Final LOG deselection requires PipelineWise to persist removal of all '
                'logical bookmarks and migration state before publication changes'
            )


def prepare_publication(
        conn_info, logical_streams, state=None, fresh_start=False, reconcile=False,
        final_log_deselection=False):
    """Prepare selected tables and optionally remove stale managed members."""
    _validate_publication_selection(logical_streams, state, reconcile, final_log_deselection)
    publication = generate_publication_name(conn_info['tap_id'])
    tables = []
    seen_tables = set()
    catalog_primary_keys = {}
    for stream in logical_streams:
        md_map = metadata.to_map(stream['metadata'])
        table = (md_map.get(())['schema-name'], stream['table_name'])
        catalog_primary_keys[table] = tuple(
            md_map.get(()).get('table-key-properties', ()))
        if table not in seen_tables:
            seen_tables.add(table)
            tables.append(table)
    publication_tables = []
    publishes_partition_roots = False
    wal2json_tables = []
    wal2json_aliases = {}
    pgoutput_aliases = {}
    expected_primary_keys = {}
    migration_state = (state or {}).get(PGOUTPUT_MIGRATION_STATE_KEY)
    migration_phase = (
        _validate_migration_state(conn_info, migration_state)
        if migration_state is not None and not fresh_start else None
    )
    migration_slots_exist = False
    with post_db.open_connection(conn_info, False, True) as conn:
        with conn.cursor() as cur:
            timeout = str(max(1, int(conn_info.get('publication_fence_timeout_seconds', 300) * 1000)))
            cur.execute("SELECT set_config('lock_timeout', %s, TRUE), "
                        "set_config('statement_timeout', %s, TRUE)", (timeout, timeout))
            if reconcile:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                if cur.fetchone() is None:
                    return PreparedPublication(publication)
            if logical_streams:
                _reject_selected_generated_columns(cur, logical_streams)
                (publication_tables,
                 publishes_partition_roots,
                 wal2json_tables,
                 wal2json_aliases,
                 pgoutput_aliases) = _validate_publication_tables(
                     cur, tables, conn.server_version)
                destination, migration_source, slot_rows = _validate_replication_slot_candidates(
                    cur,
                    conn_info['dbname'],
                    conn_info['tap_id'],
                    allow_wal2json_migration=(
                        not publishes_partition_roots or migration_phase in {
                            'pgoutput_overlap', 'overlap_complete'
                        }),
                    fresh_start=fresh_start,
                    previous_tap_id=conn_info.get('previous_tap_id'),
                )
                migration_slots_exist = migration_source is not None and destination in slot_rows
                # Pgoutput encodes partition changes against the publication root,
                # but source DML uses physical leaves and checks their identities too.
                identity_tables = list(dict.fromkeys([
                    *publication_tables,
                    *wal2json_tables,
                ]))
                expected_primary_keys = dict(catalog_primary_keys)
                expected_primary_keys.update({
                    leaf: catalog_primary_keys[root]
                    for leaf, root in wal2json_aliases.items()
                })
                _validate_replica_identity(
                    cur, identity_tables, expected_primary_keys)
            elif reconcile:
                destination, migration_source, slot_rows = _validate_replication_slot_candidates(
                    cur,
                    conn_info['dbname'],
                    conn_info['tap_id'],
                    fresh_start=fresh_start,
                    previous_tap_id=conn_info.get('previous_tap_id'),
                    final_log_deselection=final_log_deselection,
                )
                migration_slots_exist = migration_source is not None and destination in slot_rows
            publication_frozen = not fresh_start and (
                migration_phase is not None or migration_slots_exist
            )
            cur.execute(
                """
                SELECT puballtables,
                       pubinsert,
                       pubupdate,
                       pubdelete,
                       pubtruncate,
                       pubviaroot,
                       pg_catalog.obj_description(oid, 'pg_publication')
                  FROM pg_catalog.pg_publication
                 WHERE pubname = %s
                """,
                (publication,),
            )
            existing = cur.fetchone()
            try:
                if existing is None:
                    if reconcile:
                        return PreparedPublication(publication)
                    update = _create_publication(
                        cur, publication, publication_tables,
                        publication_frozen=publication_frozen,
                        has_slot_history=not fresh_start and slot_rows.get(destination),
                    )
                else:
                    update = _reconcile_existing_publication(
                        cur, publication, _PublicationSettings(*existing), publication_tables, conn.server_version,
                        fresh_start=fresh_start, publication_frozen=publication_frozen,
                        reconcile=reconcile, final_log_deselection=final_log_deselection,
                    )
                    if update is None:
                        return PreparedPublication(publication)
                if update.needs_fence:
                    _set_publication_comment(
                        cur, publication,
                        _encode_publication_fence_comment('pending', update.original_comment, update.managed_tables),
                    )
            except psycopg2.errors.InsufficientPrivilege as ex:
                raise ReplicationSlotMigrationError(
                    f'Publication {publication} does not match selected tables and user '
                    f'{conn_info["user"]} cannot create or alter it'
                ) from ex
    if update.needs_fence:
        _finish_publication_fence(conn_info, publication, update)
    return PreparedPublication(
        publication,
        wal2json_tables=wal2json_tables,
        wal2json_aliases=wal2json_aliases,
        pgoutput_aliases=pgoutput_aliases,
        has_partition_roots=publishes_partition_roots,
        expected_primary_keys=expected_primary_keys,
    )


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


def streams_to_wal2json_tables(streams, physical_tables=None):
    """Return a wal2json-compatible escaped table filter."""
    def escape(value):
        for character in " ',.*":
            value = value.replace(character, f'\\{character}')
        return value

    tables = list(physical_tables or [
        (metadata.to_map(stream['metadata']).get(())['schema-name'], stream['table_name'])
        for stream in streams
    ])
    return ','.join(f'{escape(schema)}.{escape(table)}' for schema, table in tables)


def _apply_relation_alias(message_payload, relation_aliases):
    relation = (message_payload.get('schema'), message_payload.get('table'))
    alias = relation_aliases.get(relation)
    if alias is not None:
        message_payload['schema'], message_payload['table'] = alias
    return message_payload


def _write_lsn_bookmarks(state, logical_streams, lsn):
    for stream in logical_streams:
        state = singer.write_bookmark(state, stream['tap_stream_id'], 'lsn', lsn)
    return state


def _write_lsn_state(state, logical_streams, lsn):
    state = _write_lsn_bookmarks(state, logical_streams, lsn)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state


def _write_monotonic_lsn_state(state, logical_streams, lsn):
    """Write migration replay state without moving any stream bookmark backwards."""
    for stream in logical_streams:
        stream_id = stream['tap_stream_id']
        current_lsn = get_bookmark(state, stream_id, 'lsn')
        state = singer.write_bookmark(
            state,
            stream_id,
            'lsn',
            max(current_lsn, lsn) if type(current_lsn) is int else lsn,
        )
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state


def _start_wal2json_replication(cur, logical_streams, slot, start_lsn, version, tap_id,
                                physical_tables=None):
    cur.execute('SET SESSION wal_sender_timeout = 10800000')
    try:
        cur.start_replication(
            slot_name=slot,
            decode=True,
            start_lsn=start_lsn,
            status_interval=FEEDBACK_POLL_INTERVAL,
            options={
                'format-version': 2,
                'include-transaction': True,
                'include-timestamp': True,
                'include-types': False,
                'actions': 'insert,update,delete',
                'add-tables': streams_to_wal2json_tables(
                    logical_streams,
                    physical_tables=physical_tables,
                ),
            },
        )
    except psycopg2.ProgrammingError as ex:
        raise ReplicationSlotMigrationError(
            f'Unable to bridge legacy wal2json slot {slot}: {ex}'
        ) from ex


def _prepare_bridge_boundary(conn_info, state, source_slot, destination_slot, slot_lsn):
    """Create or reuse the numeric boundary persisted before reading legacy WAL."""
    migration_state = state.get(PGOUTPUT_MIGRATION_STATE_KEY)
    if migration_state is None:
        boundary_lsn = emit_wal_progress_message(conn_info) or fetch_current_lsn(conn_info)
        state[PGOUTPUT_MIGRATION_STATE_KEY] = {
            'version': PGOUTPUT_MIGRATION_STATE_VERSION,
            'phase': 'bridge_pending',
            'source_slot': source_slot,
            'destination_slot': str(destination_slot),
            'slot_lsn': slot_lsn,
            'boundary_lsn': boundary_lsn,
        }
    else:
        phase = _validate_migration_state(conn_info, migration_state)
        if (
                phase != 'bridge_pending'
                or migration_state['source_slot'] != source_slot
                or migration_state['destination_slot'] != str(destination_slot)
                or migration_state['slot_lsn'] != slot_lsn):
            raise ReplicationSlotMigrationError(
                'Persisted pgoutput bridge boundary does not match the source slots'
            )
        boundary_lsn = migration_state['boundary_lsn']
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return boundary_lsn


def _bridge_wal2json_slot(conn_info, logical_streams, state, state_file, publication, source_slot,  # noqa: C901
                          destination_slot, slot_lsn, start_lsn, time_extracted):
    """Deliver legacy WAL through a complete transaction beyond the fresh slot."""
    boundary_lsn = _prepare_bridge_boundary(conn_info, state, source_slot, destination_slot, slot_lsn)
    conn = post_db.open_connection(conn_info, True, True, replication_plugin='wal2json')
    cur = None
    bridge_lsn = None
    try:
        cur = conn.cursor()
        _start_wal2json_replication(
            cur,
            logical_streams,
            source_slot,
            start_lsn,
            conn.server_version,
            conn_info['tap_id'],
            physical_tables=publication.wal2json_tables,
        )
        cur.send_feedback(
            write_lsn=start_lsn,
            flush_lsn=0,
            reply=True,
            force=True,
        )
        last_complete_lsn = None
        commits_since_state = 0
        last_message_at = datetime.datetime.utcnow()
        started_at = datetime.datetime.utcnow()
        poll_timestamp = datetime.datetime.utcnow()
        feedback_lsn = 0
        while True:
            now = datetime.datetime.utcnow()
            if now >= started_at + datetime.timedelta(seconds=conn_info['max_run_seconds']):
                LOGGER.info('Pausing legacy bridge at max_run_seconds')
                break
            msg = cur.read_message()
            if msg is None:
                idle_seconds = (
                    datetime.datetime.utcnow() - last_message_at
                ).total_seconds()
                if idle_seconds > (conn_info['logical_poll_total_seconds'] or 10800):
                    LOGGER.info('Pausing legacy bridge after idle timeout')
                    break
                try:
                    select([cur], [], [], 1)
                except InterruptedError:
                    pass
            else:
                try:
                    message_payload = json.loads(msg.payload)
                except (TypeError, ValueError) as ex:
                    raise ReplicationSlotMigrationError(
                        f'Legacy wal2json slot {source_slot} returned invalid JSON'
                    ) from ex
                _apply_relation_alias(message_payload, publication.wal2json_aliases)

                state = consume_message(
                    logical_streams,
                    state,
                    msg,
                    time_extracted,
                    conn_info,
                    message_payload=message_payload,
                )
                if message_payload.get('action') == 'C':
                    last_complete_lsn = int(msg.data_start)
                    commits_since_state += 1
                    if last_complete_lsn >= boundary_lsn:
                        bridge_lsn = last_complete_lsn
                        state[PGOUTPUT_MIGRATION_STATE_KEY] = {
                            'version': PGOUTPUT_MIGRATION_STATE_VERSION,
                            'phase': 'bridge',
                            'source_slot': source_slot,
                            'destination_slot': str(destination_slot),
                            'slot_lsn': slot_lsn,
                            'bridge_lsn': bridge_lsn,
                            'boundary_lsn': boundary_lsn,
                        }
                        state = _write_lsn_state(state, logical_streams, bridge_lsn)
                        break
                    if commits_since_state >= UPDATE_BOOKMARK_PERIOD:
                        state = _write_lsn_state(state, logical_streams, last_complete_lsn)
                        commits_since_state = 0
                last_message_at = datetime.datetime.utcnow()

            if datetime.datetime.utcnow() >= (
                    poll_timestamp + datetime.timedelta(seconds=FEEDBACK_POLL_INTERVAL)):
                target_lsn = _read_target_acknowledged_lsn(
                    state_file, logical_streams, feedback_lsn)
                safe_feedback_lsn = min(target_lsn, last_complete_lsn or feedback_lsn)
                if safe_feedback_lsn > feedback_lsn:
                    cur.send_feedback(
                        write_lsn=safe_feedback_lsn,
                        flush_lsn=safe_feedback_lsn,
                        reply=True,
                        force=True,
                    )
                    feedback_lsn = safe_feedback_lsn
                poll_timestamp = datetime.datetime.utcnow()
        if bridge_lsn is None:
            if last_complete_lsn is not None:
                state = _write_lsn_state(state, logical_streams, last_complete_lsn)
            return state
        LOGGER.info(
            'Legacy bridge reached %s; exiting for target persistence before pgoutput activation',
            int_to_lsn(bridge_lsn),
        )
        return state
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            conn.close()


def _start_replication(cur, publication, slot, start_lsn, version):
    wal_sender_timeout = 10800000  # 10800000ms = 3 hours
    LOGGER.info('Set session wal_sender_timeout = %i milliseconds', wal_sender_timeout)
    cur.execute(f"SET SESSION wal_sender_timeout = {wal_sender_timeout}")

    try:
        cur.start_replication(slot_name=slot,
                              decode=False,
                              start_lsn=start_lsn,
                              status_interval=FEEDBACK_POLL_INTERVAL,
                              options={
                                  'proto_version': '1',
                                  'publication_names': publication,
                              })
    except psycopg2.ProgrammingError as ex:
        raise Exception(f"Unable to start replication with logical replication (slot {ex})") from ex


def _decode_message(decoder, msg):
    if isinstance(msg.payload, (bytes, bytearray, memoryview)):
        return decoder.decode(msg.payload)
    try:
        return json.loads(msg.payload)
    except (TypeError, ValueError) as ex:
        raise PgoutputProtocolError('Replication payload is not valid pgoutput data') from ex


def _migration_positions_are_valid(marker, phase):
    """Require each completed migration stage to cover the preceding WAL boundary."""
    slot_lsn = marker.get('slot_lsn')
    boundary_lsn = marker.get('boundary_lsn')
    if type(slot_lsn) is not int or slot_lsn < 0:
        return False
    if type(boundary_lsn) is not int or boundary_lsn < slot_lsn:
        return False
    if phase == 'bridge_pending':
        return True

    bridge_lsn = marker.get('bridge_lsn')
    if type(bridge_lsn) is not int or bridge_lsn <= slot_lsn or bridge_lsn < boundary_lsn:
        return False
    if phase != 'overlap_complete':
        return True

    crossover_lsn = marker.get('crossover_lsn')
    return type(crossover_lsn) is int and crossover_lsn >= bridge_lsn


def _validate_migration_state(conn_info, migration_state):
    required = {'version', 'phase', 'source_slot', 'destination_slot', 'slot_lsn'}
    destination = generate_replication_slot_name(conn_info['tap_id'])
    if _implicit_historical_slot_is_truncated(
            conn_info['dbname'], conn_info['tap_id'], conn_info.get('previous_tap_id')):
        raise ReplicationSlotMigrationError(
            'Persisted pgoutput migration state refers to an implicitly truncated historical '
            'slot name. Verify ownership and set previous_tap_id explicitly before continuing.'
        )
    allowed_sources = {
        legacy_replication_slot_names(
            conn_info['dbname'], conn_info.get('previous_tap_id') or conn_info['tap_id'])[1]
    } - {destination}
    phase = migration_state.get('phase') if isinstance(migration_state, dict) else None
    if (
            not isinstance(migration_state, dict)
            or not required.issubset(migration_state)
            or type(migration_state.get('version')) is not int
            or migration_state['version'] != PGOUTPUT_MIGRATION_STATE_VERSION
            or phase not in {'bridge_pending', 'bridge', 'pgoutput_overlap', 'overlap_complete'}
            or not isinstance(migration_state.get('source_slot'), str)
            or migration_state['source_slot'] not in allowed_sources
            or migration_state.get('destination_slot') != destination
            or not _migration_positions_are_valid(migration_state, phase)
    ):
        raise ReplicationSlotMigrationError(
            f'Invalid {PGOUTPUT_MIGRATION_STATE_KEY} state: {migration_state!r}'
        )
    return phase


def _validate_canonical_slot_position(slot, start_lsn, migration_phase):
    """Refuse a bookmark whose required WAL has already been discarded."""
    canonical_confirmed_lsn = getattr(slot, 'confirmed_flush_lsn', None)
    migration_source = getattr(slot, 'migration_source', None)
    if isinstance(slot, PreparedReplicationSlot):
        if (isinstance(canonical_confirmed_lsn, bool)
                or not isinstance(canonical_confirmed_lsn, int)):
            raise ReplicationSlotMigrationError(
                f'Canonical pgoutput slot {slot} has no valid confirmed flush LSN'
            )
        if (
            start_lsn < canonical_confirmed_lsn
            and migration_phase != 'pgoutput_overlap'
            and migration_source is None
        ):
            raise ReplicationSlotMigrationError(
                f'Target bookmark {int_to_lsn(start_lsn)} predates canonical pgoutput '
                f'slot {slot} confirmed flush LSN '
                f'{int_to_lsn(canonical_confirmed_lsn)}. PostgreSQL cannot replay '
                'discarded WAL; run an unfiltered whole-tap FastSync to reset the '
                'slot and state together.'
            )


def _validate_migration_slot_identity(slot, migration_state, migration_phase):
    """Keep a persisted handoff bound to the same source and destination slots."""
    migration_source = getattr(slot, 'migration_source', None)
    expected_source = migration_state['source_slot']
    source_matches = (
        migration_source == expected_source
        if migration_phase in {'bridge_pending', 'bridge'}
        else migration_source in {None, expected_source}
    )
    if str(slot) != migration_state['destination_slot'] or not source_matches:
        raise ReplicationSlotMigrationError(
            'Persisted pgoutput migration slots do not match the source database: '
            f'{(expected_source, migration_state["destination_slot"])!r} != '
            f'{(migration_source, str(slot))!r}'
        )


def _validate_bridge_slot_position(slot, start_lsn, migration_state, migration_phase):
    """Require retained legacy WAL and the untouched pgoutput starting position."""
    migration_source = getattr(slot, 'migration_source', None)
    source_confirmed_lsn = getattr(slot, 'source_confirmed_lsn', None)
    canonical_confirmed_lsn = getattr(slot, 'confirmed_flush_lsn', None)
    if (isinstance(source_confirmed_lsn, bool)
            or not isinstance(source_confirmed_lsn, int)):
        raise ReplicationSlotMigrationError(
            f'Legacy slot {migration_source} has no valid confirmed flush LSN'
        )
    if start_lsn < source_confirmed_lsn:
        raise ReplicationSlotMigrationError(
            f'Target bookmark {int_to_lsn(start_lsn)} predates legacy slot '
            f'{migration_source} confirmed flush LSN {int_to_lsn(source_confirmed_lsn)}; '
            'a full resync is required to avoid skipping unacknowledged WAL'
        )
    if (
            migration_phase == 'bridge_pending'
            and canonical_confirmed_lsn != migration_state['slot_lsn']):
        raise ReplicationSlotMigrationError(
            f'Canonical pgoutput slot {slot} moved from the persisted bridge start LSN; '
            'a full resync is required to avoid skipping unacknowledged WAL'
        )


@dataclass
class _PgoutputCheckpoint:
    """Track decoded commits separately from target-durable feedback."""

    state: dict
    streams: list
    overlap_replay: bool
    last_complete_lsn: int | None = None
    emitted_lsn: int | None = None
    commits_since_state: int = 0

    def write(self, lsn):
        writer = _write_monotonic_lsn_state if self.overlap_replay else _write_lsn_state
        self.state = writer(self.state, self.streams, lsn)
        self.emitted_lsn = lsn

    def record_commit(self, payload, boundary_lsn, break_at_end_lsn):
        """Checkpoint a complete transaction and report whether this run must stop."""
        commit_lsn = payload.get('end_lsn')
        if commit_lsn is None:
            raise PgoutputProtocolError('pgoutput Commit message has no end LSN')
        self.last_complete_lsn = commit_lsn
        self.commits_since_state += 1

        if commit_lsn >= boundary_lsn:
            if self.overlap_replay:
                self.state[PGOUTPUT_MIGRATION_STATE_KEY].update({
                    'phase': 'overlap_complete',
                    'crossover_lsn': commit_lsn,
                })
            self.write(commit_lsn)
            LOGGER.info('Reached pgoutput commit boundary at %s', int_to_lsn(commit_lsn))
            return self.overlap_replay or break_at_end_lsn

        if self.commits_since_state >= UPDATE_BOOKMARK_PERIOD:
            self.write(commit_lsn)
            self.commits_since_state = 0
        return False

    def finalize(self):
        """Emit the last complete commit, including when a later transaction fails."""
        if self.last_complete_lsn is not None and self.emitted_lsn != self.last_complete_lsn:
            LOGGER.info('Updating bookmarks for all streams to pgoutput LSN %s',
                        int_to_lsn(self.last_complete_lsn))
            self.write(self.last_complete_lsn)
        elif self.last_complete_lsn is None:
            singer.write_message(singer.StateMessage(value=copy.deepcopy(self.state)))


def sync_tables(conn_info, logical_streams, state, end_lsn, state_file):  # noqa: C901
    target_acknowledged_lsn = _minimum_acknowledged_lsn(state, logical_streams)
    start_lsn = target_acknowledged_lsn
    time_extracted = utils.now()
    has_migration_state = PGOUTPUT_MIGRATION_STATE_KEY in state
    migration_state = state.get(PGOUTPUT_MIGRATION_STATE_KEY)
    migration_phase = (
        _validate_migration_state(conn_info, migration_state)
        if has_migration_state else None
    )
    if migration_phase in {'bridge', 'overlap_complete'}:
        raise ReplicationSlotMigrationError(
            f'Persisted pgoutput migration phase {migration_phase!r} must be '
            'processed by PipelineWise before the tap resumes'
        )

    # Publication DDL must commit before the fresh slot is created or old history is bridged. Pgoutput
    # evaluates historical transactions against their historical catalog snapshot.
    publication = prepare_publication(conn_info, logical_streams, state=state)
    slot = locate_replication_slot(
        conn_info,
        allow_wal2json_migration=(
            not publication.has_partition_roots
            or migration_phase in {'pgoutput_overlap', 'overlap_complete'}
        ),
    )
    migration_source = getattr(slot, 'migration_source', None)
    canonical_confirmed_lsn = getattr(slot, 'confirmed_flush_lsn', None)
    _validate_canonical_slot_position(slot, start_lsn, migration_phase)
    start_run_timestamp = datetime.datetime.utcnow()
    max_run_seconds = conn_info['max_run_seconds']
    break_at_end_lsn = conn_info['break_at_end_lsn']
    logical_poll_total_seconds = conn_info['logical_poll_total_seconds'] or 10800

    for stream in logical_streams:
        sync_common.send_schema_message(
            stream,
            ['lsn'],
            record_update_mode=sync_common.PATCH_RECORD_UPDATE_MODE)

    if has_migration_state:
        _validate_migration_slot_identity(slot, migration_state, migration_phase)
    if migration_source and migration_phase not in {'pgoutput_overlap', 'overlap_complete'}:
        _validate_bridge_slot_position(slot, start_lsn, migration_state, migration_phase)
        # Revalidate the physical relations immediately before the historical
        # bridge in case an operator changed an identity after preflight.
        _validate_replica_identity_tables(
            conn_info,
            publication.wal2json_tables,
            {
                table: publication.expected_primary_keys[table]
                for table in publication.wal2json_tables
            },
        )
        return _bridge_wal2json_slot(
            conn_info,
            logical_streams,
            state,
            state_file,
            publication,
            migration_source,
            slot,
            migration_state['slot_lsn'] if migration_phase == 'bridge_pending'
            else canonical_confirmed_lsn,
            start_lsn,
            time_extracted,
        )
    overlap_replay = migration_phase == 'pgoutput_overlap'
    if overlap_replay:
        start_lsn = migration_state['slot_lsn']
        if canonical_confirmed_lsn != start_lsn:
            raise ReplicationSlotMigrationError(
                'The pgoutput overlap position advanced before target acknowledgement; '
                'an unfiltered whole-tap FastSync is required')

    conn = post_db.open_connection(conn_info, True, True)
    cur = None
    finalize_state = False
    checkpoint = _PgoutputCheckpoint(state, logical_streams, overlap_replay)
    feedback_lsn = 0

    try:
        cur = conn.cursor()
        _start_replication(cur, publication, slot, start_lsn, conn.server_version)
        cur.send_feedback(
            write_lsn=start_lsn if overlap_replay else target_acknowledged_lsn,
            flush_lsn=0,
            reply=True,
            force=True,
        )
        boundary_lsn = (
            migration_state['bridge_lsn']
            if overlap_replay
            else emit_wal_progress_message(conn_info) or end_lsn
        )
        LOGGER.info(
            'Request pgoutput streaming after startup LSN %s (slot %s, publication %s)',
            int_to_lsn(end_lsn), slot, publication,
        )

        decoder = PgoutputDecoder()
        lsn_received_timestamp = datetime.datetime.utcnow()
        poll_timestamp = datetime.datetime.utcnow()
        finalize_state = True

        while True:
            if datetime.datetime.utcnow() >= (start_run_timestamp + datetime.timedelta(seconds=max_run_seconds)):
                LOGGER.info('Breaking - reached max_run_seconds of %i', max_run_seconds)
                break

            try:
                msg = cur.read_message()
            except Exception as ex:
                LOGGER.error(ex)
                raise

            if msg:
                message_payload = _decode_message(decoder, msg)
                _apply_relation_alias(message_payload, publication.pgoutput_aliases)
                action = message_payload.get('action')

                checkpoint.state = consume_message(
                    logical_streams,
                    checkpoint.state,
                    msg,
                    time_extracted,
                    conn_info,
                    message_payload=message_payload,
                )
                if action == 'C' and checkpoint.record_commit(message_payload, boundary_lsn, break_at_end_lsn):
                    break
                lsn_received_timestamp = datetime.datetime.utcnow()
            else:
                poll_duration = (
                    datetime.datetime.utcnow() - lsn_received_timestamp
                ).total_seconds()
                if poll_duration > logical_poll_total_seconds:
                    LOGGER.info('Breaking - %i seconds of polling with no data', poll_duration)
                    break
                try:
                    select([cur], [], [], 1)
                except InterruptedError:
                    pass

            if not overlap_replay and datetime.datetime.utcnow() >= (
                    poll_timestamp + datetime.timedelta(seconds=FEEDBACK_POLL_INTERVAL)):
                target_acknowledged_lsn = _read_target_acknowledged_lsn(
                    state_file, logical_streams, feedback_lsn)
                safe_feedback_lsn = min(target_acknowledged_lsn, checkpoint.last_complete_lsn or start_lsn)
                if safe_feedback_lsn > feedback_lsn:
                    LOGGER.info('Confirming pgoutput through target-acknowledged LSN %s',
                                int_to_lsn(safe_feedback_lsn))
                    cur.send_feedback(
                        write_lsn=safe_feedback_lsn,
                        flush_lsn=safe_feedback_lsn,
                        reply=True,
                        force=True,
                    )
                    feedback_lsn = safe_feedback_lsn
                poll_timestamp = datetime.datetime.utcnow()
    finally:
        try:
            if finalize_state:
                checkpoint.finalize()
        finally:
            try:
                if cur is not None:
                    cur.close()
            finally:
                conn.close()

    return checkpoint.state
