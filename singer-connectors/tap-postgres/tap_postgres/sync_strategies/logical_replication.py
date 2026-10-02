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
import uuid
import warnings

from select import select
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
BOUNDARY_MESSAGE_PREFIX = 'pipelinewise'
PUBLICATION_FENCE_COMMENT_PREFIX = 'pipelinewise-publication-fence-v1:'
PGOUTPUT_MIGRATION_STATE_KEY = '_pipelinewise_pgoutput_migration'


class ReplicationSlotNotFoundError(Exception):
    """Custom exception when replication slot not found"""


class UnsupportedPayloadKindError(Exception):
    """Custom exception when a payload is not insert, update nor delete."""


class ReplicationSlotMigrationError(RuntimeError):
    """Raised when a wal2json slot cannot be migrated safely."""


class PreparedReplicationSlot(str):
    """Canonical pgoutput slot plus an old slot retained during migration."""

    def __new__(cls, name, migration_source=None, migration_lsn=None,
                confirmed_flush_lsn=None):
        value = super().__new__(cls, name)
        value.migration_source = migration_source
        value.migration_lsn = migration_lsn
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


def consume_message(streams, state, msg, time_extracted, conn_info, *, message_payload=None):
    if message_payload is None:
        try:
            message_payload = json.loads(msg.payload)
        except Exception:
            return state

    lsn = message_payload.get('transaction_lsn') or msg.data_start

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
        relation_columns = message_payload.get('relation_columns', message_payload['columns'])
        diff = {column['name'] for column in relation_columns}.\
            difference(target_stream['schema']['properties'].keys())

    if diff or message_payload.get('schema_changed'):
        LOGGER.info('Detected relation change%s, refreshing schema of stream %s',
                    f' with new columns {diff}' if diff else '', target_stream['stream'])
        # encountered a column that is not in the schema
        # refresh the stream schema and metadata by running discovery
        refresh_streams_schema({**conn_info, 'use_secondary': False}, [target_stream])

        # add the automatic properties back to the stream
        add_automatic_properties(target_stream, conn_info.get('debug_lsn', False))

        # publish new schema
        sync_common.send_schema_message(
            target_stream,
            ['lsn'],
            record_update_mode=sync_common.PATCH_RECORD_UPDATE_MODE)

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


def generate_replication_slot_name(tap_id, prefix='pipelinewise'):
    """Return the canonical tap-scoped pgoutput replication slot name."""
    validate_tap_id(tap_id)
    return _replication_identifier(prefix, tap_id)


def generate_publication_name(tap_id):
    """Return the publication used by one tap in the current database."""
    validate_tap_id(tap_id)
    return _replication_identifier('pw', 'pub', tap_id)


def legacy_replication_slot_names(dbname, tap_id):
    """Return the database-wide and tap-specific historical slot names."""
    return [
        _replication_identifier('pipelinewise', dbname)[:63],
        _replication_identifier('pipelinewise', dbname, tap_id)[:63],
    ]


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
        fresh_start=False, previous_tap_id=None):
    """Read and validate every tap-owned slot candidate without mutating it."""
    destination = generate_replication_slot_name(tap_id)
    legacy_names = legacy_replication_slot_names(dbname, previous_tap_id or tap_id)
    rows = _slot_rows(cursor, list(dict.fromkeys([destination, *legacy_names])))

    database_wide_slot, tap_specific_slot = legacy_names
    ambiguous_legacy = database_wide_slot == tap_specific_slot
    if ambiguous_legacy and tap_specific_slot in rows and destination not in rows and not fresh_start:
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
        _validate_slot(migration_source, rows[migration_source], dbname, 'wal2json')
        if not allow_wal2json_migration and not fresh_start:
            raise ReplicationSlotMigrationError(
                'Automatic wal2json migration is not supported when a selected relation is a '
                'partition root because partitions can change while the bridge is running. '
                'Complete a full resync into a fresh pgoutput slot instead.'
            )

    if (destination_row is None and migration_source is None
            and database_wide_slot in rows and not fresh_start):
        raise ReplicationSlotMigrationError(
            f'Historical database-wide slot {database_wide_slot} may be shared by '
            'multiple taps and cannot be migrated automatically. A DBA must migrate '
            f'it to a dedicated tap-specific slot {tap_specific_slot} before '
            'pgoutput migration.'
        )

    return destination, migration_source, rows


def locate_replication_slot_by_cur(cursor, dbname, tap_id, allow_wal2json_migration=True,
                                   previous_tap_id=None):
    """Resolve or copy a slot to the canonical pgoutput slot name."""
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
                f'Unable to find pgoutput slot {destination} or a legacy wal2json slot to copy'
            )
        cursor.execute(
            'SELECT slot_name, lsn::text FROM pg_catalog.pg_copy_logical_replication_slot(%s, %s, FALSE, %s)',
            (migration_source, destination, 'pgoutput'),
        )
        copied_slot, copied_lsn = cursor.fetchone()
        if copied_slot != destination:
            raise ReplicationSlotMigrationError(
                f'PostgreSQL copied {migration_source} to unexpected slot {copied_slot}'
            )
        migration_lsn = lsn_to_int(copied_lsn)
        confirmed_flush_lsn = migration_lsn
        LOGGER.info('Copied wal2json replication slot %s to pgoutput slot %s at %s',
                    migration_source, destination, copied_lsn)
    else:
        confirmed_flush_lsn = lsn_to_int(
            destination_row['confirmed_flush_lsn'])
        migration_lsn = (
            lsn_to_int(rows[migration_source]['confirmed_flush_lsn'])
            if migration_source else None
        )

    LOGGER.info('Using pgoutput replication slot %s', destination)
    return PreparedReplicationSlot(
        destination,
        migration_source,
        migration_lsn,
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


def _encode_publication_fence_comment(state, original_comment):
    payload = json.dumps({
        'state': state,
        'original_comment': original_comment,
    }, separators=(',', ':')).encode()
    return PUBLICATION_FENCE_COMMENT_PREFIX + base64.urlsafe_b64encode(payload).decode()


def _decode_publication_fence_comment(comment):
    if not isinstance(comment, str) or not comment.startswith(PUBLICATION_FENCE_COMMENT_PREFIX):
        return None, comment
    try:
        payload = comment.removeprefix(PUBLICATION_FENCE_COMMENT_PREFIX)
        decoded = json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
        if not isinstance(decoded, dict) \
                or set(decoded) != {'state', 'original_comment'} \
                or decoded['state'] not in {'pending', 'ready'} or (
                decoded['original_comment'] is not None
                and not isinstance(decoded['original_comment'], str)):
            raise ValueError('invalid original publication comment')
        return decoded['state'], decoded['original_comment']
    except (binascii.Error, TypeError, UnicodeError, ValueError) as ex:
        raise ReplicationSlotMigrationError(
            'The pgoutput publication has an invalid pending transaction-fence comment'
        ) from ex


def _set_publication_comment(cur, publication, comment):
    value = sql.SQL('NULL') if comment is None else sql.Literal(comment)
    cur.execute(sql.SQL('COMMENT ON PUBLICATION {} IS {}').format(
        sql.Identifier(publication), value))


def prepare_publication(conn_info, logical_streams, state=None, fresh_start=False):  # noqa: C901
    """Add selected tables without removing tables needed by other replication runs."""
    if not logical_streams:
        raise ValueError('Cannot create a pgoutput publication without selected tables')
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
    needs_transaction_fence = False
    publication_changed = False
    original_publication_comment = None
    publication_fence_state = None
    migration_state = (state or {}).get(PGOUTPUT_MIGRATION_STATE_KEY)
    migration_phase = (
        _validate_migration_state(conn_info, migration_state)
        if migration_state is not None and not fresh_start else None
    )
    with post_db.open_connection(conn_info, False, True) as conn:
        with conn.cursor() as cur:
            timeout = str(max(1, int(conn_info.get('publication_fence_timeout_seconds', 300) * 1000)))
            cur.execute("SELECT set_config('lock_timeout', %s, TRUE), "
                        "set_config('statement_timeout', %s, TRUE)", (timeout, timeout))
            _reject_selected_generated_columns(cur, logical_streams)
            (publication_tables,
             publishes_partition_roots,
             wal2json_tables,
             wal2json_aliases,
             pgoutput_aliases) = _validate_publication_tables(
                 cur, tables, conn.server_version)
            _validate_replication_slot_candidates(
                cur,
                conn_info['dbname'],
                conn_info['tap_id'],
                allow_wal2json_migration=(
                    not publishes_partition_roots or migration_phase in {'pgoutput', 'retire'}),
                fresh_start=fresh_start,
                previous_tap_id=conn_info.get('previous_tap_id'),
            )
            # Pgoutput encodes partition changes against the publication root,
            # but PostgreSQL executes source DML on physical leaves and checks
            # their identities too. Validate both before publication DDL can
            # make an UPDATE or DELETE fail at the source.
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
            table_list = sql.SQL(', ').join(
                sql.SQL('{}.{}').format(sql.Identifier(schema_name), sql.Identifier(table_name))
                for schema_name, table_name in publication_tables
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
                    publication_options = "publish = 'insert, update, delete'"
                    publication_options += ', publish_via_partition_root = true'
                    cur.execute(sql.SQL(
                        'CREATE PUBLICATION {} FOR TABLE {} WITH ({})'
                    ).format(
                        sql.Identifier(publication),
                        table_list,
                        sql.SQL(publication_options),
                    ))
                    needs_transaction_fence = True
                    publication_changed = True
                else:
                    (puballtables,
                     pubinsert,
                     pubupdate,
                     pubdelete,
                     pubtruncate,
                     pubviaroot,
                     publication_comment) = existing
                    publication_fence_state, original_publication_comment = (
                        _decode_publication_fence_comment(publication_comment)
                    )
                    needs_transaction_fence = publication_fence_state != 'ready'
                    if not all((pubinsert, pubupdate, pubdelete)):
                        raise ReplicationSlotMigrationError(
                            f'Publication {publication} must publish insert, update, and delete'
                        )
                    if pubtruncate:
                        raise ReplicationSlotMigrationError(
                            f'Publication {publication} must not publish truncate'
                        )
                    if puballtables:
                        raise ReplicationSlotMigrationError(
                            f'Publication {publication} must list exactly the selected tables'
                        )
                    if conn.server_version >= 150000:
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
                    if publishes_partition_roots and not pubviaroot:
                        cur.execute(sql.SQL(
                            'ALTER PUBLICATION {} SET (publish_via_partition_root = true)'
                        ).format(sql.Identifier(publication)))
                        needs_transaction_fence = True
                        publication_changed = True
                    cur.execute(
                        """
                        SELECT schemaname, tablename
                          FROM pg_catalog.pg_publication_tables
                         WHERE pubname = %s
                        """,
                        (publication,),
                    )
                    added_tables = set(publication_tables).difference(cur.fetchall())
                    if added_tables:
                        added_table_list = sql.SQL(', ').join(
                            sql.SQL('{}.{}').format(sql.Identifier(schema_name), sql.Identifier(table_name))
                            for schema_name, table_name in sorted(added_tables)
                        )
                        cur.execute(
                            sql.SQL('ALTER PUBLICATION {} ADD TABLE {}').format(
                                sql.Identifier(publication), added_table_list
                            )
                        )
                        needs_transaction_fence = True
                        publication_changed = True
                if publication_changed or publication_fence_state is None:
                    _set_publication_comment(
                        cur,
                        publication,
                        _encode_publication_fence_comment(
                            'pending', original_publication_comment),
                    )
                    needs_transaction_fence = True
            except psycopg2.errors.InsufficientPrivilege as ex:
                raise ReplicationSlotMigrationError(
                    f'Publication {publication} does not match selected tables and user '
                    f'{conn_info["user"]} cannot create or alter it'
                ) from ex
    if needs_transaction_fence:
        _wait_for_prepublication_transactions(conn_info)
        try:
            with post_db.open_connection(conn_info, False, True) as conn:
                with conn.cursor() as cur:
                    _set_publication_comment(
                        cur,
                        publication,
                        _encode_publication_fence_comment(
                            'ready', original_publication_comment),
                    )
        except psycopg2.errors.InsufficientPrivilege as ex:
            raise ReplicationSlotMigrationError(
                f'Publication {publication} transaction fence completed, but user '
                f'{conn_info["user"]} cannot clear its pending fence comment'
            ) from ex
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


def _boundary_message_prefix(tap_id):
    validate_tap_id(tap_id)
    return f'{BOUNDARY_MESSAGE_PREFIX}_{tap_id}'


def emit_boundary_message(conn_info, token=None):
    """Commit a unique transactional message decoded by pgoutput and wal2json."""
    token = token or uuid.uuid4().hex
    try:
        with post_db.open_connection(conn_info, False, True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT pg_catalog.pg_logical_emit_message(TRUE, %s, %s)::text',
                    (_boundary_message_prefix(conn_info['tap_id']), token),
                )
                emitted_lsn = cur.fetchone()
                if not emitted_lsn or lsn_to_int(emitted_lsn[0]) <= 0:
                    raise ReplicationSlotMigrationError(
                        'PostgreSQL did not return an LSN for the logical boundary message'
                    )
    except (psycopg2.errors.InsufficientPrivilege, psycopg2.errors.UndefinedFunction) as ex:
        raise ReplicationSlotMigrationError(
            'Unable to emit the transactional logical boundary message; grant EXECUTE on '
            'pg_catalog.pg_logical_emit_message(boolean,text,text) to the tap user'
        ) from ex
    return token


def _is_boundary_message(message_payload, tap_id, token):
    content = message_payload.get('content')
    if isinstance(content, (bytes, bytearray, memoryview)):
        content_matches = bytes(content) == token.encode('utf-8')
    else:
        content_matches = content == token
    return (
        message_payload.get('action') == 'M'
        and message_payload.get('transactional') is True
        and message_payload.get('prefix') == _boundary_message_prefix(tap_id)
        and content_matches
    )


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
                'add-msg-prefixes': _boundary_message_prefix(tap_id),
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


def _bridge_wal2json_slot(conn_info, logical_streams, state, state_file, publication, source_slot,  # noqa: C901
                          destination_slot, copy_lsn, start_lsn,
                          time_extracted):
    """Deliver the legacy backlog through a post-publication logical-message commit."""
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
        token = emit_boundary_message(conn_info)
        boundary_transaction = False
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

                if message_payload.get('action') == 'B':
                    boundary_transaction = False
                if _is_boundary_message(message_payload, conn_info['tap_id'], token):
                    boundary_transaction = True
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
                    if boundary_transaction:
                        bridge_lsn = last_complete_lsn
                        state[PGOUTPUT_MIGRATION_STATE_KEY] = {
                            'version': 1,
                            'phase': 'bridge',
                            'source_slot': source_slot,
                            'destination_slot': str(destination_slot),
                            'copy_lsn': copy_lsn,
                            'bridge_lsn': bridge_lsn,
                        }
                        state = _write_lsn_state(state, logical_streams, bridge_lsn)
                        break
                    if commits_since_state >= UPDATE_BOOKMARK_PERIOD:
                        state = _write_lsn_state(state, logical_streams, last_complete_lsn)
                        commits_since_state = 0
                    boundary_transaction = False
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
                                  'messages': 'true',
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


def _validate_migration_state(conn_info, migration_state):
    required = {
        'version', 'phase', 'source_slot', 'destination_slot', 'copy_lsn', 'bridge_lsn',
    }
    destination = generate_replication_slot_name(conn_info['tap_id'])
    allowed_sources = {
        legacy_replication_slot_names(
            conn_info['dbname'], conn_info.get('previous_tap_id') or conn_info['tap_id'])[1]
    } - {destination}
    if (
            not isinstance(migration_state, dict)
            or not required.issubset(migration_state)
            or type(migration_state.get('version')) is not int
            or migration_state['version'] != 1
            or migration_state.get('phase') not in {'bridge', 'pgoutput', 'retire'}
            or not isinstance(migration_state.get('source_slot'), str)
            or migration_state['source_slot'] not in allowed_sources
            or migration_state.get('destination_slot') != destination
            or type(migration_state.get('copy_lsn')) is not int
            or migration_state['copy_lsn'] < 0
            or type(migration_state.get('bridge_lsn')) is not int
            or migration_state['bridge_lsn'] <= migration_state['copy_lsn']
            or (
                migration_state.get('phase') == 'retire'
                and (
                    type(migration_state.get('retire_lsn')) is not int
                    or migration_state['retire_lsn'] <= migration_state['bridge_lsn']
                )
            )):
        raise ReplicationSlotMigrationError(
            f'Invalid {PGOUTPUT_MIGRATION_STATE_KEY} state: {migration_state!r}'
        )
    return migration_state['phase']


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
    if migration_phase == 'bridge':
        raise ReplicationSlotMigrationError(
            'Persisted pgoutput bridge state must be target-acknowledged and promoted '
            'by PipelineWise before the tap resumes'
        )

    # Publication DDL must commit before an old slot is copied or bridged. Pgoutput
    # evaluates historical transactions against their historical catalog snapshot.
    publication = prepare_publication(conn_info, logical_streams, state=state)
    slot = locate_replication_slot(
        conn_info,
        allow_wal2json_migration=(
            not publication.has_partition_roots
            or migration_phase in {'pgoutput', 'retire'}
        ),
    )
    migration_source = getattr(slot, 'migration_source', None)
    migration_lsn = getattr(slot, 'migration_lsn', None)
    canonical_confirmed_lsn = getattr(slot, 'confirmed_flush_lsn', None)
    if isinstance(slot, PreparedReplicationSlot):
        if (isinstance(canonical_confirmed_lsn, bool)
                or not isinstance(canonical_confirmed_lsn, int)):
            raise ReplicationSlotMigrationError(
                f'Canonical pgoutput slot {slot} has no valid confirmed flush LSN'
            )
        if start_lsn < canonical_confirmed_lsn:
            raise ReplicationSlotMigrationError(
                f'Target bookmark {int_to_lsn(start_lsn)} predates canonical pgoutput '
                f'slot {slot} confirmed flush LSN '
                f'{int_to_lsn(canonical_confirmed_lsn)}. PostgreSQL cannot replay '
                'discarded WAL; run an unfiltered whole-tap FastSync to reset the '
                'slot and state together.'
            )
    migration_activation_lsn = None
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
        migration_activation_lsn = migration_state['bridge_lsn']
        expected_source = migration_state['source_slot']
        if str(slot) != migration_state['destination_slot'] or (
                migration_phase != 'retire' and migration_source != expected_source):
            raise ReplicationSlotMigrationError(
                'Persisted pgoutput migration slots do not match the source database: '
                f'{(expected_source, migration_state["destination_slot"])!r} != '
                f'{(migration_source, str(slot))!r}'
            )

    if migration_source and migration_phase not in {'pgoutput', 'retire'}:
        if isinstance(migration_lsn, bool) or not isinstance(migration_lsn, int):
            raise ReplicationSlotMigrationError(
                f'Legacy slot {migration_source} has no valid confirmed flush LSN'
            )
        if start_lsn < migration_lsn:
            raise ReplicationSlotMigrationError(
                f'Target bookmark {int_to_lsn(start_lsn)} predates legacy slot '
                f'{migration_source} confirmed flush LSN {int_to_lsn(migration_lsn)}; '
                'a full resync is required to avoid skipping unacknowledged WAL'
            )
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
            migration_lsn,
            start_lsn,
            time_extracted,
        )
    conn = post_db.open_connection(conn_info, True, True)
    cur = None
    finalize_state = False
    last_complete_lsn = None
    state_emitted_lsn = None
    commits_since_state = 0
    feedback_lsn = 0

    try:
        cur = conn.cursor()
        _start_replication(cur, publication, slot, start_lsn, conn.server_version)
        cur.send_feedback(
            write_lsn=target_acknowledged_lsn,
            flush_lsn=0,
            reply=True,
            force=True,
        )
        boundary_token = emit_boundary_message(conn_info)
        LOGGER.info(
            'Request pgoutput streaming after startup LSN %s (slot %s, publication %s)',
            int_to_lsn(end_lsn), slot, publication,
        )

        decoder = PgoutputDecoder()
        transaction_has_boundary = False
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
                if action == 'B':
                    transaction_has_boundary = False
                if _is_boundary_message(message_payload, conn_info['tap_id'], boundary_token):
                    transaction_has_boundary = True

                state = consume_message(
                    logical_streams,
                    state,
                    msg,
                    time_extracted,
                    conn_info,
                    message_payload=message_payload,
                )
                if action == 'C':
                    commit_lsn = message_payload.get('end_lsn')
                    if commit_lsn is None:
                        raise PgoutputProtocolError('pgoutput Commit message has no end LSN')
                    last_complete_lsn = commit_lsn
                    commits_since_state += 1

                    if transaction_has_boundary:
                        if (migration_source
                                and migration_phase == 'pgoutput'
                                and migration_activation_lsn is not None
                                and commit_lsn > migration_activation_lsn):
                            state[PGOUTPUT_MIGRATION_STATE_KEY].update({
                                'phase': 'retire',
                                'retire_lsn': commit_lsn,
                            })
                            migration_phase = 'retire'
                        state = _write_lsn_state(state, logical_streams, commit_lsn)
                        state_emitted_lsn = commit_lsn
                        LOGGER.info('Reached decoded pgoutput boundary at %s', int_to_lsn(commit_lsn))
                        if break_at_end_lsn:
                            break
                    elif commits_since_state >= UPDATE_BOOKMARK_PERIOD:
                        state = _write_lsn_state(state, logical_streams, commit_lsn)
                        state_emitted_lsn = commit_lsn
                        commits_since_state = 0
                    transaction_has_boundary = False
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

            if datetime.datetime.utcnow() >= (
                    poll_timestamp + datetime.timedelta(seconds=FEEDBACK_POLL_INTERVAL)):
                target_acknowledged_lsn = _read_target_acknowledged_lsn(
                    state_file, logical_streams, feedback_lsn)
                safe_feedback_lsn = min(target_acknowledged_lsn, last_complete_lsn or start_lsn)
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
                if last_complete_lsn is not None and state_emitted_lsn != last_complete_lsn:
                    LOGGER.info('Updating bookmarks for all streams to pgoutput LSN %s',
                                int_to_lsn(last_complete_lsn))
                    state = _write_lsn_state(state, logical_streams, last_complete_lsn)
                elif last_complete_lsn is None:
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
        finally:
            try:
                if cur is not None:
                    cur.close()
            finally:
                conn.close()

    return state
