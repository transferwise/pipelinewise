
import codecs
import copy
import datetime
import json
import random
import re
import socket
import pymysql.err
import pytz
import singer
import tzlocal

from typing import Dict, Set, Union, Optional, Any, Tuple
from plpygis import Geometry
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.constants import FIELD_TYPE
from pymysqlreplication.event import (
    RotateEvent, MariadbGtidEvent, GtidEvent, NotImplementedEvent, QueryEvent, XidEvent, XAPrepareEvent,
)
from pymysqlreplication.gtid import Gtid, GtidSet
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
    TableMapEvent,
)
from singer import utils, Schema, metadata

from tap_mysql import connection
from tap_mysql.connection import connect_with_backoff, make_connection_wrapper, MySQLConnection
from tap_mysql.discover_utils import (
    discover_catalog,
    desired_columns,
    mariadb_json_aliases_enabled,
    should_run_discovery,
)
from tap_mysql.stream_utils import write_schema_message
from tap_mysql.sync_strategies import common

LOGGER = singer.get_logger('tap_mysql')

SDC_DELETED_AT = "_sdc_deleted_at"
UPDATE_BOOKMARK_PERIOD = 1000
BOOKMARK_KEYS = {'log_file', 'log_pos', 'version', 'gtid', 'gtid_complete'}
CHECKPOINT_PROBE_LIMIT = 16
CHECKPOINT_PROBE_MAX_EVENTS = 256
BINLOG_START_POSITION = 4
CHECKPOINT_SCAN_PAGE_SIZE = 10000
CHECKPOINT_SCAN_LOG_INTERVAL = 100000

MYSQL_TIMESTAMP_TYPES = {
    FIELD_TYPE.TIMESTAMP,
    FIELD_TYPE.TIMESTAMP2
}


def binlog_filename_key(filename: str) -> Tuple[str, int]:
    """
    Key function for sorting binlog filenames numerically by their suffix.

    Args:
        filename: Binlog filename

    Returns:
        Tuple of (prefix, numeric_suffix)
    """
    if filename and '.' in filename:
        prefix, suffix = filename.rsplit('.', 1)
        if suffix.isdigit():
            return prefix, int(suffix)
    return filename, 0


def add_automatic_properties(catalog_entry, columns):
    catalog_entry.schema.properties[SDC_DELETED_AT] = Schema(
        type=["null", "string"],
        format="date-time"
    )

    columns.append(SDC_DELETED_AT)

    return columns


def verify_binlog_config(mysql_conn):
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SELECT  @@binlog_format")
            binlog_format = cur.fetchone()[0]

            if binlog_format != 'ROW':
                raise Exception(f"Unable to replicate binlog stream because binlog_format is "
                                f"not set to 'ROW': {binlog_format}.")

            try:
                cur.execute("SELECT  @@binlog_row_image")
                binlog_row_image = cur.fetchone()[0]
            except pymysql.err.InternalError as ex:
                if ex.args[0] == 1193:
                    raise Exception("Unable to replicate binlog stream because binlog_row_image "
                                    "system variable does not exist. MySQL version must be at "
                                    "least 5.6.2 to use binlog replication.") from ex
                raise ex

            if binlog_row_image != 'FULL':
                raise Exception(f"Unable to replicate binlog stream because binlog_row_image is "
                                f"not set to 'FULL': {binlog_row_image}.")

            for variable, unsupported in (
                    ('binlog_row_value_options', {'PARTIAL_JSON'}),
                    ('binlog_transaction_compression', {'ON', '1'}),
                    ('log_bin_compress', {'ON', '1'})):
                try:
                    cur.execute(f'SELECT @@{variable}')
                    value = str(cur.fetchone()[0]).upper()
                except (pymysql.err.OperationalError, pymysql.err.InternalError) as exc:
                    if exc.args[0] == 1193:
                        continue
                    raise
                if unsupported.intersection(value.split(',')):
                    raise ValueError(f'tap-mysql cannot decode {variable}={value}; disable it before replicating.')


def verify_gtid_config(mysql_conn: MySQLConnection):
    """
    Checks if GTID is enabled, raises exception if it's not
    Args:
        mysql_conn: instance of MySQLConnection

    Returns: None if gtid is enabled
    """
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("select @@gtid_mode;")
            binlog_format = cur.fetchone()[0]

            if binlog_format != 'ON':
                raise Exception('Unable to replicate binlog stream because GTID mode is not enabled.')


def fetch_current_log_file_and_pos(mysql_conn):
    result = _fetch_current_binlog_status(mysql_conn)

    return tuple(result[0:2])


def _fetch_current_binlog_status(mysql_conn):
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            try:
                cur.execute("SHOW MASTER STATUS")
            except pymysql.err.ProgrammingError as exc:
                if exc.args[0] != 1064:
                    raise
                cur.execute('SHOW BINARY LOG STATUS')

            result = cur.fetchone()

            if result is None:
                raise Exception("MySQL binary logging is not enabled.")

            return result


def fetch_current_binlog_checkpoint(mysql_conn, engine):
    """Sample file coordinates and the complete GTID history at the same MySQL boundary."""
    status = _fetch_current_binlog_status(mysql_conn)
    log_file, log_pos = status[0:2]
    if engine == connection.MARIADB_ENGINE:
        gtid = _find_gtid_by_binlog_coordinates(mysql_conn, log_file, log_pos)
    else:
        gtid = _normalize_gtid_position(status[4], engine) if len(status) > 4 and status[4] else None
    if not gtid:
        raise ValueError('Cannot rebuild a complete GTID checkpoint from the current binlog endpoint.')
    return log_file, log_pos, gtid


def fetch_current_gtid_pos(
        mysql_conn: MySQLConnection,
        engine: str
) -> str:
    """Capture all executed transactions, including history from previous source servers."""
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:

            if engine != connection.MARIADB_ENGINE:
                cur.execute('select @@GLOBAL.gtid_executed;')
            else:
                cur.execute('select @@gtid_current_pos;')

            result = cur.fetchone()

            if not result or not result[0]:
                raise Exception("GTID is not present on this server!")
            position = _normalize_gtid_position(result[0], engine)
            LOGGER.info('Using GTID %s for state bookmark', position)
            return position


def _normalize_gtid_position(position, engine):
    """Validate the full checkpoint without dropping any source UUID or domain."""
    if engine != connection.MARIADB_ENGINE:
        return str(GtidSet(position.lower()))
    values = [value.strip() for value in position.split(',')]
    if not all(re.fullmatch(r'\d+-\d+-\d+', value) for value in values):
        raise ValueError('Invalid MariaDB GTID position.')
    return ','.join(values)


def json_bytes_to_string(data):
    if isinstance(data, bytes):
        return data.decode()

    if isinstance(data, dict):
        return dict(map(json_bytes_to_string, data.items()))

    if isinstance(data, tuple):
        return tuple(map(json_bytes_to_string, data))

    if isinstance(data, list):
        return list(map(json_bytes_to_string, data))

    return data


def row_to_singer_record(catalog_entry, version, db_column_map, row, time_extracted):  # noqa: C901
    row_to_persist = {}
    for column_name, val in row.items():
        property_type = catalog_entry.schema.properties[column_name].type
        property_format = catalog_entry.schema.properties[column_name].format
        db_column_type = db_column_map.get(column_name)

        if property_format == common.MARIADB_JSON_FORMAT:
            val = common.parse_mariadb_json_alias(val)

        if isinstance(val, datetime.datetime):
            if db_column_type in MYSQL_TIMESTAMP_TYPES:
                # The mysql-replication library creates datetimes from TIMESTAMP columns using fromtimestamp which
                # will use the local timezone thus we must set tzinfo accordingly See:
                # https://github.com/noplay/python-mysql-replication/blob/master/pymysqlreplication/row_event.py#L143
                # -L145
                timezone = tzlocal.get_localzone()
                local_datetime = datetime.datetime.fromtimestamp(val.timestamp(), tz=timezone)
                utc_datetime = local_datetime.astimezone(pytz.UTC)
                row_to_persist[column_name] = utc_datetime.isoformat()
            else:
                row_to_persist[column_name] = val.isoformat() + '+00:00'

        elif isinstance(val, datetime.date):
            row_to_persist[column_name] = val.isoformat() + 'T00:00:00+00:00'

        elif isinstance(val, datetime.timedelta):
            if property_format == 'time':
                row_to_persist[column_name] = common.format_mysql_time(val)
            else:
                timedelta_from_epoch = datetime.datetime.utcfromtimestamp(0) + val
                row_to_persist[column_name] = timedelta_from_epoch.isoformat() + '+00:00'

        elif db_column_type == FIELD_TYPE.JSON:
            row_to_persist[column_name] = json.dumps(json_bytes_to_string(val))

        elif property_format == 'spatial':
            if val:
                geom = Geometry(val)
                row_to_persist[column_name] = json.dumps(geom.geojson)
            else:
                row_to_persist[column_name] = None

        elif property_format == 'date-time' and common.is_invalid_mysql_datetime(val):
            row_to_persist[column_name] = None

        elif isinstance(val, bytes):
            # encode bytes as hex bytes then to utf8 string
            row_to_persist[column_name] = codecs.encode(val, 'hex').decode('utf-8')

        elif property_format != common.MARIADB_JSON_FORMAT and (
                'boolean' in property_type or property_type == 'boolean'):
            if val is None:
                boolean_representation = None
            elif val == 0:
                boolean_representation = False
            elif db_column_type == FIELD_TYPE.BIT:
                boolean_representation = int(val) != 0
            else:
                boolean_representation = True
            row_to_persist[column_name] = boolean_representation

        else:
            row_to_persist[column_name] = val

    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=row_to_persist,
        version=version,
        time_extracted=time_extracted)


def _intersect_gtid_positions(positions, engine):
    """A restart may skip only transactions acknowledged by every selected stream."""
    if engine == connection.MARIADB_ENGINE:
        common_domains = None
        for position in positions:
            domains = {value.split('-')[0]: value for value in position.split(',')}
            if common_domains is None:
                common_domains = domains
            else:
                common_domains = {
                    domain: min(value, domains[domain], key=lambda gtid: int(gtid.split('-')[2]))
                    for domain, value in common_domains.items() if domain in domains
                }
        return ','.join(common_domains.values())

    common_gtids = GtidSet(positions[0].lower()).gtids
    for position in positions[1:]:
        other = {gtid.sid: gtid for gtid in GtidSet(position.lower()).gtids}
        intersection = []
        for gtid in common_gtids:
            intervals = [(max(start, other_start), min(end, other_end))
                         for start, end in gtid.intervals
                         for other_start, other_end in other[gtid.sid].intervals
                         if max(start, other_start) < min(end, other_end)] if gtid.sid in other else []
            if intervals:
                intersection.append(Gtid('', sid=gtid.sid, intervals=intervals))
        common_gtids = intersection
    return str(GtidSet(common_gtids))


def _verify_gtid_bookmarks(bookmarks):
    """Legacy checkpoints may omit earlier transactions or other source UUIDs/domains."""
    missing = sorted(stream for stream, bookmark in bookmarks.items() if not bookmark.get('gtid'))
    legacy = sorted(stream for stream, bookmark in bookmarks.items()
                    if bookmark.get('gtid') and bookmark.get('gtid_complete') is not True)
    issues = []
    if missing and len(missing) != len(bookmarks):
        issues.append('Every selected stream needs a GTID bookmark before GTID replication can resume; '
                      f'missing GTID bookmarks: {", ".join(missing)}.')
    if legacy:
        issues.append('A legacy GTID bookmark cannot prove complete transaction history; '
                      f'affected streams: {", ".join(legacy)}.')
    if issues:
        raise ValueError(' '.join(issues) + ' Perform a full resync of the affected streams before replication.')


def calculate_gtid_bookmark(
        mysql_conn: MySQLConnection,
        binlog_streams_map: Dict[str, Any],
        state: Dict,
        engine: str
) -> str:
    """
    Find the GTID history acknowledged by every selected stream.
    Args:
        mysql_conn: instance of MySqlConnection
        binlog_streams_map: dictionary of selected streams
        state: state dict with bookmarks
        engine: the DB flavor mysql/mariadb

    Returns: Common acknowledged GTID set, or its MariaDB file-position equivalent.
    """
    bookmarks = {stream: state.get('bookmarks', {}).get(stream, {}) for stream in binlog_streams_map}
    positions = [bookmark.get('gtid') for bookmark in bookmarks.values()]
    _verify_gtid_bookmarks(bookmarks)

    min_gtid = _intersect_gtid_positions(positions, engine) if all(positions) and positions else None
    if positions and all(positions) and not min_gtid:
        raise ValueError('Selected streams have no shared GTID checkpoint; resync them before resuming replication.')

    if not min_gtid:

        # Mariadb has a handy sql function BINLOG_GTID_POS to infer the gtid position from given binlog coordinates so
        # we will use that, as for mysql, there is no such thing, the only available option is using the cli utility
        # mysqlbinlog which we deemed as not nice to use, and we don't wanna make it a system requirement of this tap,
        # hence, this functionality of inferring gtid is not implemented for it.

        if engine != connection.MARIADB_ENGINE:
            raise ValueError("Couldn't find any gtid in state bookmarks to resume logical replication; "
                             f'missing GTID bookmarks: {", ".join(sorted(bookmarks))}. '
                             'Perform a full resync of the affected streams before replication.')

        LOGGER.info("Couldn't find a gtid in state, will try to infer one from binlog coordinates if they exist ..")
        log_file, log_pos = calculate_bookmark(mysql_conn, binlog_streams_map, state)

        if not (log_file and log_pos):
            raise Exception("No binlog coordinates in state to infer gtid position! Cannot resume logical replication")

        verify_binlog_checkpoint(mysql_conn, log_file, log_pos, require_transaction_boundary=True)
        min_gtid = _find_gtid_by_binlog_coordinates(mysql_conn, log_file, log_pos)

        if not min_gtid:
            raise Exception("Couldn't infer any gtid from binlog coordinates to resume logical replication")

        LOGGER.info('The inferred GTID is "%s", it will be used to resume replication',
                    min_gtid)
    else:
        LOGGER.info('The earliest bookmarked GTID found in the state is "%s", and will be used to resume replication',
                    min_gtid)

    return min_gtid


def _find_gtid_by_binlog_coordinates(mysql_conn: MySQLConnection, log_file: str, log_pos: int) -> Optional[str]:
    """
    Finds the equivalent gtid position from the given binlog file and pos.
    This only works on MariaDB

    Args:
        mysql_conn: instance of MySQLConnection
        log_file: a binlog file
        log_pos: a position in the log file

    Returns: gtid position
    """
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute(f"select BINLOG_GTID_POS('{log_file}', {log_pos});")
            gtids = cur.fetchone()[0]

    LOGGER.debug('BINLOG_GTID_POS returned gtids: %s', gtids)

    if not gtids:
        return None

    return _normalize_gtid_position(gtids, connection.MARIADB_ENGINE)


def get_min_log_pos_per_log_file(binlog_streams_map, state) -> Dict[str, Dict]:
    min_log_pos_per_file = {}

    for tap_stream_id, bookmark in state.get('bookmarks', {}).items():
        stream = binlog_streams_map.get(tap_stream_id)

        if not stream:
            continue

        log_file = bookmark.get('log_file')
        log_pos = bookmark.get('log_pos')

        if not min_log_pos_per_file.get(log_file):
            min_log_pos_per_file[log_file] = {
                'log_pos': log_pos,
                'streams': [tap_stream_id]
            }

        elif min_log_pos_per_file[log_file]['log_pos'] > log_pos:
            min_log_pos_per_file[log_file]['log_pos'] = log_pos
            min_log_pos_per_file[log_file]['streams'].append(tap_stream_id)

        else:
            min_log_pos_per_file[log_file]['streams'].append(tap_stream_id)

    return min_log_pos_per_file


def calculate_bookmark(mysql_conn, binlog_streams_map, state) -> Tuple[str, int]:
    min_log_pos_per_file = get_min_log_pos_per_log_file(binlog_streams_map, state)

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SHOW BINARY LOGS")

            binary_logs = cur.fetchall()

            if binary_logs:
                server_logs_set = {log[0] for log in binary_logs}
                state_logs_set = set(min_log_pos_per_file.keys())
                expired_logs = state_logs_set.difference(server_logs_set)

                if expired_logs:
                    raise Exception('Unable to replicate binlog stream because the following binary log(s) no longer '
                                    f'exist: {", ".join(expired_logs)}')

                for log_file in sorted(server_logs_set, key=binlog_filename_key):
                    if min_log_pos_per_file.get(log_file):
                        return log_file, min_log_pos_per_file[log_file]['log_pos']

            raise Exception("Unable to replicate binlog stream because no binary logs exist on the server.")


def _binlog_text(value):
    """Decode structural SHOW BINLOG fields while leaving arbitrary event payloads as bytes."""
    return value.decode('ascii') if isinstance(value, bytes) else value


def _binlog_name_matches(value, expected):
    return value == expected or (isinstance(value, bytes) and value == expected.encode('utf-8'))


def verify_binlog_checkpoint(mysql_conn, log_file, log_pos, require_transaction_boundary=False):
    """Require decode context, or a whole-transaction boundary when converting to GTID."""
    safe_events = {'gtid', 'rotate'}
    neutral_events = {'format_desc', 'gtid_list', 'previous_gtids', 'binlog_checkpoint', 'stop'}
    if not require_transaction_boundary:
        safe_events.update({'table_map', 'anonymous_gtid', 'query', 'xid'})
        neutral_events.update({'annotate_rows', 'rows_query'})
    probe_pos = log_pos
    probed_events = 0
    unsafe_event = None
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cursor:
            # Event Info contains original statement bytes and is not guaranteed to match the connection charset.
            cursor.execute('SET character_set_results = binary')
            while probed_events < CHECKPOINT_PROBE_MAX_EVENTS:
                page_size = min(CHECKPOINT_PROBE_LIMIT, CHECKPOINT_PROBE_MAX_EVENTS - probed_events)
                try:
                    cursor.execute(
                        f'SHOW BINLOG EVENTS IN %s FROM %s LIMIT {page_size + 1}', (log_file, probe_pos))
                except (pymysql.err.InternalError, pymysql.err.OperationalError) as exc:
                    if exc.args[0] != 1220:
                        raise
                    raise ValueError(
                        f'Cannot validate binlog bookmark {log_file}:{log_pos}: the server could not decode the event. '
                        'Check the bookmark and source binlog integrity; perform a full resync if the checkpoint '
                        f'cannot be recovered safely. Server error: {exc}') from exc
                events = cursor.fetchall()
                for event in events[:page_size]:
                    probed_events += 1
                    # MariaDB 11.4 leaves End_log_pos zero by default. Pos and event order remain authoritative.
                    event_type = _binlog_text(event[2]).lower()
                    if event_type in safe_events:
                        return
                    if event_type not in neutral_events:
                        unsafe_event = event_type
                        break
                if unsafe_event:
                    break
                if events and len(events) <= page_size:
                    return
                if not events:
                    # An empty result is safe only at the exact current end of that file.
                    cursor.execute('SHOW BINARY LOGS')
                    if any(_binlog_name_matches(row[0], log_file) and row[1] == probe_pos
                           for row in cursor.fetchall()):
                        return
                    break
                next_probe_pos = events[page_size][1]
                if next_probe_pos <= probe_pos:
                    raise InconclusiveBinlogCheckpointError(
                        f'Binlog bookmark validation is inconclusive for {log_file}:{log_pos}: '
                        f'the bounded probe did not advance beyond position {probe_pos}.')
                probe_pos = next_probe_pos
            else:
                raise InconclusiveBinlogCheckpointError(
                    f'Binlog bookmark validation is inconclusive for {log_file}:{log_pos} after inspecting '
                    f'{probed_events} neutral events; no safe or unsafe boundary was observed.')
    message = ('The binlog bookmark is not a safe transaction boundary and may omit its TABLE_MAP; '
               'perform a full resync before resuming replication.')
    if unsafe_event and re.fullmatch(r'(?:write|update|delete)_rows(?:_v\d+)?', unsafe_event):
        raise UnsafeBinlogCheckpointError(message)
    raise ValueError(message)


class UnsafeBinlogCheckpointError(ValueError):
    """A legacy file checkpoint resumes at a row event without its table map."""


class InconclusiveBinlogCheckpointError(ValueError):
    """A bounded checkpoint probe found neither a safe nor an unsafe event."""


def _binlog_info(event):
    value = event[5]
    return value.decode('utf-8', errors='backslashreplace') if isinstance(value, bytes) else value


def _is_row_event_type(event_type):
    return bool(re.fullmatch(r'(?:write|update|delete)_rows(?:_v\d+)?', event_type))


def _observe_binlog_boundary(event, transaction_start, transaction_open, last_boundary):
    event_pos = event[1]
    event_type = _binlog_text(event[2]).lower()
    if event_type in {'gtid', 'anonymous_gtid'}:
        return event_pos, False, last_boundary
    if event_type == 'xid':
        return None, False, event_pos
    if event_type == 'query':
        query = _binlog_info(event).strip().upper()
        if re.fullmatch(r'BEGIN(?:\s+WORK)?', query):
            return transaction_start if transaction_start is not None else event_pos, True, last_boundary
        if re.fullmatch(r'(?:COMMIT|ROLLBACK)(?:\s+WORK)?(?:\s*/\*.*\*/)?', query):
            return None, False, event_pos
        if not transaction_open:
            return None, False, event_pos
    if event_type in {'table_map', 'annotate_rows', 'rows_query'} or _is_row_event_type(event_type):
        transaction_open = True
    return transaction_start, transaction_open, last_boundary


def find_binlog_transaction_start(mysql_conn, log_file, log_pos):
    """Find a proven transaction boundary immediately before an unsafe row checkpoint."""
    scan_pos = BINLOG_START_POSITION
    scanned_events = 0
    next_progress_log = CHECKPOINT_SCAN_LOG_INTERVAL
    transaction_start = None
    transaction_open = False
    last_boundary = None

    LOGGER.warning(
        'Recovering legacy binlog bookmark %s:%s requires scanning that source binlog from its beginning; '
        'large binlogs can take time and increase source load.', log_file, log_pos)

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cursor:
            cursor.execute('SET character_set_results = binary')
            while scan_pos <= log_pos:
                cursor.execute(
                    f'SHOW BINLOG EVENTS IN %s FROM %s LIMIT {CHECKPOINT_SCAN_PAGE_SIZE + 1}',
                    (log_file, scan_pos))
                events = cursor.fetchall()
                if not events:
                    break

                advance_to = None
                for event in events[:CHECKPOINT_SCAN_PAGE_SIZE]:
                    scanned_events += 1
                    event_pos = event[1]
                    event_type = _binlog_text(event[2]).lower()
                    if event_pos == log_pos:
                        replay_pos = transaction_start if transaction_start is not None else last_boundary
                        if _is_row_event_type(event_type) and replay_pos is not None and replay_pos < log_pos:
                            LOGGER.info(
                                'Recovered transaction boundary %s for legacy bookmark %s:%s after scanning %s '
                                'binlog event(s).', replay_pos, log_file, log_pos, scanned_events)
                            return replay_pos
                        break
                    if event_pos > log_pos:
                        break
                    transaction_start, transaction_open, last_boundary = _observe_binlog_boundary(
                        event, transaction_start, transaction_open, last_boundary)
                else:
                    if len(events) > CHECKPOINT_SCAN_PAGE_SIZE:
                        advance_to = events[CHECKPOINT_SCAN_PAGE_SIZE][1]

                if advance_to is None:
                    break
                if advance_to <= scan_pos:
                    break
                scan_pos = advance_to
                if scanned_events >= next_progress_log:
                    LOGGER.info(
                        'Scanned %s binlog event(s) through %s:%s while recovering legacy bookmark %s:%s.',
                        scanned_events, log_file, scan_pos, log_file, log_pos)
                    next_progress_log = (
                        (scanned_events // CHECKPOINT_SCAN_LOG_INTERVAL) + 1
                    ) * CHECKPOINT_SCAN_LOG_INTERVAL

    raise ValueError(
        f'Cannot prove a transaction boundary before legacy binlog bookmark {log_file}:{log_pos}; '
        'perform a full resync before resuming replication.')


def recover_legacy_binlog_checkpoint(mysql_conn, log_file, log_pos):
    """Validate a file checkpoint, rewinding only a proven legacy row boundary."""
    try:
        verify_binlog_checkpoint(mysql_conn, log_file, log_pos)
    except UnsafeBinlogCheckpointError:
        replay_pos = find_binlog_transaction_start(mysql_conn, log_file, log_pos)
        LOGGER.warning(
            'Legacy binlog bookmark %s:%s points inside a row transaction; replaying from transaction boundary %s '
            'while retaining per-stream durable bookmarks.', log_file, log_pos, replay_pos)
        return replay_pos
    return log_pos


def update_bookmarks(
        state: Dict,
        binlog_streams_map: Dict,
        log_file: str,
        log_pos: int,
        gtid: Optional[str]) -> Dict:
    """
    Updates the state bookmarks with the given binlog file & position or GTID
    Args:
        state: state to update
        binlog_streams_map: dictionary of log based streams
        log_file: new binlog file
        log_pos: new binlog pos
        gtid: new gtid pos

    Returns: updated state
    """
    LOGGER.debug('Updating state bookmark to binlog file and pos and GTID: %s, %d, %s', log_file, log_pos, gtid)

    if log_file and not log_pos:
        raise ValueError("binlog_file is present but binlog_pos is null! Please provide a binlog position "
                         "to properly update the state")

    for tap_stream_id in binlog_streams_map.keys():
        previous = state.get('bookmarks', {}).get(tap_stream_id, {})
        previous_file = previous.get('log_file')
        previous_pos = previous.get('log_pos')
        if not (previous_file and previous_pos and _position_at_or_before(
                log_file, log_pos, previous_file, previous_pos)):
            state = singer.write_bookmark(state, tap_stream_id, 'log_file', log_file)
            state = singer.write_bookmark(state, tap_stream_id, 'log_pos', log_pos)

        # update gtid only if it's not null
        if gtid:
            if previous.get('gtid') and previous.get('gtid_complete') is True:
                engine = connection.MYSQL_ENGINE if ':' in gtid else connection.MARIADB_ENGINE
                stream_gtid = merge_gtid_position(previous['gtid'], gtid, engine)
            else:
                stream_gtid = gtid
            state = singer.write_bookmark(state,
                                          tap_stream_id,
                                          'gtid',
                                          stream_gtid)
            state = singer.write_bookmark(state, tap_stream_id, 'gtid_complete', True)

    return state


def get_db_column_types(event):
    return {c.name: c.type for c in event.columns}


def handle_write_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    for row in event.rows:
        filtered_vals = {k: v for k, v in row['values'].items()
                         if k in columns}

        record_message = row_to_singer_record(catalog_entry,
                                              stream_version,
                                              db_column_types,
                                              filtered_vals,
                                              time_extracted)

        singer.write_message(record_message)
        rows_saved += 1

    return rows_saved


def handle_update_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)
    key_properties = common.get_key_properties(catalog_entry)

    for row in event.rows:
        before_values = row.get('before_values', {})
        if any(before_values.get(key) != row['after_values'].get(key)
               for key in key_properties if key in before_values):
            deleted_values = {key: value for key, value in before_values.items() if key in columns}
            deleted_values[SDC_DELETED_AT] = datetime.datetime.fromtimestamp(
                event.timestamp, tz=pytz.UTC).isoformat()
            singer.write_message(row_to_singer_record(
                catalog_entry, stream_version, db_column_types, deleted_values, time_extracted))
            rows_saved += 1

        filtered_vals = {k: v for k, v in row['after_values'].items() if k in columns}

        record_message = row_to_singer_record(catalog_entry,
                                              stream_version,
                                              db_column_types,
                                              filtered_vals,
                                              time_extracted)

        singer.write_message(record_message)

        rows_saved += 1

    return rows_saved


def handle_delete_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    event_ts = datetime.datetime.utcfromtimestamp(event.timestamp) \
        .replace(tzinfo=pytz.UTC).isoformat()

    for row in event.rows:
        vals = row['values']
        vals[SDC_DELETED_AT] = event_ts

        filtered_vals = {k: v for k, v in vals.items()
                         if k in columns}

        record_message = row_to_singer_record(catalog_entry,
                                              stream_version,
                                              db_column_types,
                                              filtered_vals,
                                              time_extracted)

        singer.write_message(record_message)

        rows_saved += 1

    return rows_saved


def generate_streams_map(binlog_streams):
    stream_map = {}

    for catalog_entry in binlog_streams:
        columns = add_automatic_properties(catalog_entry,
                                           list(catalog_entry.schema.properties.keys()))

        stream_map[catalog_entry.tap_stream_id] = {
            'catalog_entry': catalog_entry,
            'desired_columns': columns
        }

    return stream_map


def __get_diff_in_columns_list(
        binlog_event: Union[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        schema_properties: Set[str],
        ignore_columns: Optional[Set[str]] = None) -> Set[str]:
    """
    Compare event's columns to the schema properties and get the difference

    Args:
        binlog_event: Row type binlog event
        schema_properties: stream known and supported schema properties
        ignore_columns: an optional set of binlog event columns to ignore and not include in the diff

    Returns: Difference as a set of column names

    """

    if ignore_columns is None:
        ignore_columns = set()

    # if a column no longer exists, the event will have something like __dropped_col_XY__
    # to refer to this column, we don't want these columns to be included in the difference
    # we also will ignore any column using the given ignore_columns argument.

    # binlog_columns_filtered = filter(
    #     lambda col_name, ignored_cols=ignore_columns:
    #     not bool(re.match(r'__dropped_col_\d+__', col_name) or col_name in ignored_cols),
    #     [col.name for col in binlog_event.columns])

    binlog_columns_filtered = [
        col.name for col in binlog_event.columns
        if col.name and not (
                re.match(r'__dropped_col_\d+__', str(col.name)) or
                col.name in ignore_columns
        )
    ]

    return set(binlog_columns_filtered).difference(schema_properties)


def merge_gtid_position(position, transaction, engine):
    """Retain every acknowledged GTID domain when completing a transaction."""
    if engine == connection.MARIADB_ENGINE:
        domains = {gtid.split('-')[0]: gtid for gtid in position.split(',') if gtid}
        for gtid in transaction.split(','):
            domain = gtid.split('-')[0]
            domains[domain] = max(
                domains.get(domain, gtid), gtid, key=lambda value: int(value.split('-')[2]))
        return ','.join(domains.values())
    executed = GtidSet(position.lower())
    for completed in GtidSet(transaction.lower()).gtids:
        for acknowledged in executed.gtids:
            completed = completed - acknowledged
        if completed.intervals:
            executed = executed + completed
    return str(executed)


def _position_at_or_before(log_file, log_pos, other_file, other_pos):
    """Compare positions only within the same binlog filename namespace."""
    current = binlog_filename_key(log_file)
    other = binlog_filename_key(other_file)
    if current[0] != other[0]:
        return False
    return current < other or (current == other and log_pos <= other_pos)


def _event_already_bookmarked(
        bookmark, log_file, log_pos, transaction, engine, position_is_event_end=True):
    """Do not replay an advanced stream while catching up another stream."""
    if transaction is True:
        raise ValueError('GTID replication encountered a transaction without a GTID marker; perform a full resync.')
    position = bookmark.get('gtid') if bookmark.get('gtid_complete') is True else None
    if position and transaction:
        if engine != connection.MARIADB_ENGINE:
            return Gtid(transaction.lower()) in GtidSet(position.lower())
        domain, _, sequence = transaction.split('-')
        return any(value.split('-')[0] == domain and int(value.split('-')[2]) >= int(sequence)
                   for value in position.split(','))
    if not (bookmark.get('log_file') and bookmark.get('log_pos')):
        return False
    if (not position_is_event_end and log_file == bookmark['log_file']
            and log_pos == bookmark['log_pos']):
        # MariaDB 11.4 may leave an event's End_log_pos zero, so the decoder retains the preceding
        # boundary. Equality does not prove that the current row was already acknowledged.
        return False
    return _position_at_or_before(log_file, log_pos, bookmark['log_file'], bookmark['log_pos'])


def _event_has_end_position(event):
    packet = getattr(event, 'packet', None)
    return packet is None or bool(getattr(packet, 'log_pos', None))


def _pending_gtid(checkpoint):
    return checkpoint.pending if isinstance(checkpoint.pending, str) else None


class _BinlogCheckpoint:
    """Keep restart positions behind transactions whose rows are still arriving."""

    def __init__(self, reader, config):
        self.reader = reader
        self.engine = config['engine']
        self.use_gtid = config['use_gtid']
        self.position = reader.auto_position
        self.pending = None
        self.standalone = False
        self.in_transaction = False

    def observe(self, event):
        """Advance a GTID only after its complete transaction was emitted."""
        if isinstance(event, XAPrepareEvent) or (
                isinstance(event, QueryEvent) and event.query.strip().upper().startswith('XA ')):
            raise ValueError('XA transactions cannot be safely checkpointed by tap-mysql binlog replication.')
        if isinstance(event, (MariadbGtidEvent, GtidEvent)):
            self.pending = event.gtid
            self.standalone = isinstance(event, MariadbGtidEvent) and bool(event.flags & 1)
            # MariaDB's non-standalone GTID replaces the BEGIN query event.
            self.in_transaction = isinstance(event, MariadbGtidEvent) and not self.standalone
            return
        starts_without_gtid = isinstance(event, TableMapEvent) or (
            isinstance(event, QueryEvent) and event.query.strip().upper() == 'BEGIN')
        if starts_without_gtid and self.pending is None:
            if self.use_gtid:
                raise ValueError('GTID replication encountered a transaction without a GTID marker; '
                                 'perform a full resync.')
            self.pending = True
            self.standalone = isinstance(event, TableMapEvent)
        if not self.pending:
            return

        completed = isinstance(event, XidEvent)
        if isinstance(event, QueryEvent):
            query = event.query.strip().upper()
            if query == 'BEGIN':
                self.in_transaction = True
                self.standalone = False
            else:
                completed = query in {'COMMIT', 'ROLLBACK'} or not self.in_transaction
        elif self.standalone and isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
            completed = bool(event.flags & 1)

        if completed:
            self.complete()

    def complete(self):
        """Record a transaction only after its terminating boundary was consumed."""
        if self.use_gtid:
            if self.pending is True:
                raise ValueError('GTID replication encountered a transaction without a GTID marker; '
                                 'perform a full resync.')
            self.position = merge_gtid_position(self.position, self.pending, self.engine)
            self.reader.auto_position = self.position
        self.pending = None
        self.in_transaction = False
        self.standalone = False


def _reject_unsupported_query(event, streams, bookmarks, reader, transaction, engine):
    """TRUNCATE has no row delete images and cannot be applied by the Singer target."""
    tokens = re.findall(r'`(?:``|[^`])*`|"(?:""|[^"])*"|/\*.*?\*/|--(?=\s)[^\r\n]*|\#[^\r\n]*|[\w$]+|[^\s]',
                        event.query, flags=re.DOTALL)
    query = ' '.join(re.sub(r'/\*(?:M)?!\d*\s*(.*?)\*/', r'\1', token, flags=re.DOTALL)
                     for token in tokens if not token.startswith(('--', '#')) and (
                         not token.startswith('/*') or re.match(r'/\*(?:M)?!', token)))
    if not re.match(r'TRUNCATE\b', query, re.IGNORECASE):
        return
    identifier = r'(?:`(?:``|[^`])+`|"(?:""|[^"])+"|[\w$]+)'
    matched = re.match(
        rf'TRUNCATE\s+(?:TABLE\s+)?({identifier})(?:\s*\.\s*({identifier}))?', query, re.IGNORECASE)
    schema = event.schema.decode('utf-8') if isinstance(event.schema, bytes) else event.schema
    if matched:
        first, second = [value[1:-1].replace(value[0] * 2, value[0])
                         if value and value.startswith(('`', '"')) else value
                         for value in matched.groups()]
        database, table = (first, second) if second else (schema, first)
        stream_id = common.generate_tap_stream_id(database, table)
        # Case-equivalent names may address the selected table on lower_case_table_names sources.
        candidates = [stream_id] if stream_id in streams else [
            candidate for candidate in streams if candidate.casefold() == stream_id.casefold()]
        if not any(not _event_already_bookmarked(
                bookmarks.get(candidate, {}), reader.log_file, reader.log_pos, transaction, engine,
                _event_has_end_position(event))
                   for candidate in candidates):
            return
    elif not any(common.get_database_name(entry['catalog_entry']) == schema for entry in streams.values()):
        return
    raise ValueError('tap-mysql cannot replicate TRUNCATE for a selected or case-equivalent table; '
                     'perform a full resync.')


def _run_binlog_sync(  # noqa: C901
        mysql_conn: MySQLConnection,
        reader: BinLogStreamReader,
        binlog_streams_map: Dict,
        state: Dict,
        config: Dict,
        end_log_file: str,
        end_log_pos: int):

    processed_rows_events = 0
    events_skipped = 0

    log_file = None
    log_pos = None
    checkpoint = _BinlogCheckpoint(reader, config)
    gtid_pos = reader.auto_position
    bookmark_log_file = bookmark_log_pos = None
    last_checkpoint_count = 0
    initial_bookmarks = copy.deepcopy(state.get('bookmarks', {}))

    # A set to hold all columns that are detected as we sync but should be ignored cuz they are unsupported types.
    # Saving them here to avoid doing the check if we should ignore a column over and over again
    ignored_columns = {}
    # Exit from the loop when the reader either runs out of streams to return or we reach
    # the end position (which is Master's)
    for binlog_event in reader:

        # get reader current binlog file and position
        log_file = reader.log_file
        log_pos = reader.log_pos

        # Schema filtering hides row endings; a different GTID proves the standalone group finished.
        # A repeated marker can replay an unacknowledged group after a GTID reconnect.
        if (checkpoint.standalone and isinstance(binlog_event, (MariadbGtidEvent, GtidEvent))
                and binlog_event.gtid != checkpoint.pending):
            previous_end = binlog_event.packet.log_pos - binlog_event.packet.event_size
            if _position_at_or_before(log_file, previous_end, end_log_file, end_log_pos):
                checkpoint.complete()
                gtid_pos = checkpoint.position
                bookmark_log_file, bookmark_log_pos = log_file, previous_end

        # The iterator across python-mysql-replication's fetchone method should ultimately terminate
        # upon receiving an EOF packet. There seem to be some cases when a MySQL server will not send
        # one causing binlog replication to hang.
        if (binlog_filename_key(log_file) > binlog_filename_key(end_log_file)) or (
                end_log_file == log_file and log_pos > end_log_pos):
            LOGGER.info('BinLog reader (file: %s, pos:%s) has reached or exceeded end position, exiting!',
                        log_file,
                        log_pos)

            # There are cases when a mass operation (inserts, updates, deletes) starts right after we get the Master
            # binlog file and position above, making the latter behind the stream reader and it causes some data loss
            # in the next run by skipping everything between end_log_file and log_pos
            # so we need to update log_pos back to master's position
            if not checkpoint.use_gtid and checkpoint.pending is None:
                bookmark_log_file = end_log_file
                bookmark_log_pos = end_log_pos

            break

        if isinstance(binlog_event, RotateEvent):
            LOGGER.debug('RotateEvent: log_file=%s, log_pos=%d',
                         binlog_event.next_binlog,
                         binlog_event.position)

        elif isinstance(binlog_event, (MariadbGtidEvent, GtidEvent)):
            LOGGER.debug('%s: gtid=%s',
                         binlog_event.__class__.__name__,
                         binlog_event.gtid)
        elif isinstance(binlog_event, QueryEvent):
            _reject_unsupported_query(
                binlog_event, binlog_streams_map, initial_bookmarks, reader,
                _pending_gtid(checkpoint), config['engine'])
        elif isinstance(binlog_event, NotImplementedEvent):
            if checkpoint.use_gtid and binlog_event.event_type == 34:
                raise ValueError('GTID replication encountered an anonymous transaction; perform a full resync.')
            # MySQL partial rows/transaction payloads and MariaDB compressed query/row events.
            if binlog_event.event_type in {39, 40, 165, 166, 167, 168, 169, 170, 171}:
                raise ValueError('tap-mysql cannot decode partial-JSON or compressed binlog events; '
                                 'disable these source features and resync affected tables.')
        elif isinstance(binlog_event, (XidEvent, TableMapEvent, XAPrepareEvent)):
            pass
        else:
            time_extracted = utils.now()

            tap_stream_id = common.generate_tap_stream_id(binlog_event.schema, binlog_event.table)
            streams_map_entry = binlog_streams_map.get(tap_stream_id, {})
            catalog_entry = streams_map_entry.get('catalog_entry')
            columns = streams_map_entry.get('desired_columns')

            if not catalog_entry or _event_already_bookmarked(
                    initial_bookmarks.get(tap_stream_id, {}), log_file, log_pos,
                    _pending_gtid(checkpoint), config['engine'],
                    _event_has_end_position(binlog_event)):
                events_skipped += 1

                if events_skipped % UPDATE_BOOKMARK_PERIOD == 0:
                    LOGGER.debug("Skipped %s events so far as they were not for selected tables; %s rows extracted",
                                 events_skipped,
                                 processed_rows_events)
            else:
                # Compare event's columns to the schema properties
                diff = __get_diff_in_columns_list(binlog_event,
                                                  catalog_entry.schema.properties.keys(),
                                                  ignored_columns.get(tap_stream_id, set()))

                # If there are additional cols in the event then run discovery if needed and update the catalog
                if diff:

                    LOGGER.info('Stream `%s`: Difference detected between event and schema: %s', tap_stream_id, diff)

                    md_map = metadata.to_map(catalog_entry.metadata)

                    if not should_run_discovery(diff, md_map):
                        LOGGER.info('Stream `%s`: Not running discovery. Ignoring all detected columns in %s',
                                    tap_stream_id,
                                    diff)
                        ignored_columns.setdefault(tap_stream_id, set()).update(diff)

                    else:
                        LOGGER.info('Stream `%s`: Running discovery ... ', tap_stream_id)

                        # run discovery for the current table only
                        discovery_options = (
                            {'detect_json_aliases': True}
                            if mariadb_json_aliases_enabled(config)
                            else {}
                        )
                        new_catalog_entry = discover_catalog(
                            mysql_conn,
                            common.get_database_name(catalog_entry),
                            catalog_entry.table,
                            **discovery_options,
                        ).streams[0]

                        selected = {k for k, v in new_catalog_entry.schema.properties.items()
                                    if common.property_is_selected(new_catalog_entry, k)}

                        # the new catalog has "stream" property = table name, we need to update that to make it the
                        # same as the result of the "resolve_catalog" function
                        new_catalog_entry.stream = tap_stream_id

                        # These are the columns we need to select
                        new_columns = desired_columns(selected, new_catalog_entry.schema)

                        cols = set(new_catalog_entry.schema.properties.keys())

                        # drop unsupported properties from schema
                        for col in cols:
                            if col not in new_columns:
                                new_catalog_entry.schema.properties.pop(col, None)

                        # Add the _sdc_deleted_at col
                        new_columns = add_automatic_properties(new_catalog_entry, list(new_columns))

                        # send the new scheme to target if we have a new schema
                        if new_catalog_entry.schema.properties != catalog_entry.schema.properties:
                            write_schema_message(catalog_entry=new_catalog_entry)
                            catalog_entry = new_catalog_entry

                            # update this dictionary while we're at it
                            binlog_streams_map[tap_stream_id]['catalog_entry'] = new_catalog_entry
                            binlog_streams_map[tap_stream_id]['desired_columns'] = new_columns
                            columns = new_columns

                if isinstance(binlog_event, WriteRowsEvent):
                    processed_rows_events = handle_write_rows_event(binlog_event,
                                                                    catalog_entry,
                                                                    state,
                                                                    columns,
                                                                    processed_rows_events,
                                                                    time_extracted)

                elif isinstance(binlog_event, UpdateRowsEvent):
                    processed_rows_events = handle_update_rows_event(binlog_event,
                                                                     catalog_entry,
                                                                     state,
                                                                     columns,
                                                                     processed_rows_events,
                                                                     time_extracted)

                elif isinstance(binlog_event, DeleteRowsEvent):
                    processed_rows_events = handle_delete_rows_event(binlog_event,
                                                                     catalog_entry,
                                                                     state,
                                                                     columns,
                                                                     processed_rows_events,
                                                                     time_extracted)
                else:
                    LOGGER.debug("Skipping event for table %s.%s as it is not an INSERT, UPDATE, or DELETE",
                                 binlog_event.schema,
                                 binlog_event.table)

        checkpoint.observe(binlog_event)
        gtid_pos = checkpoint.position

        if checkpoint.pending is None:
            bookmark_log_file, bookmark_log_pos = log_file, log_pos

        # Mid-transaction restarts either skip its GTID or miss the earlier TABLE_MAP event.
        event_count = processed_rows_events + events_skipped
        if (event_count - last_checkpoint_count >= UPDATE_BOOKMARK_PERIOD
                and checkpoint.pending is None):
            state = update_bookmarks(state,
                                     binlog_streams_map,
                                     log_file,
                                     log_pos,
                                     gtid_pos
                                     )
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
            last_checkpoint_count = event_count

        if log_file == end_log_file and log_pos == end_log_pos:
            break
    else:
        # EOF must reach the sampled endpoint after consuming hidden rows, not merely the standalone GTID.
        if (checkpoint.standalone and reader.log_file == end_log_file
                and reader.log_pos == end_log_pos and reader.log_pos != log_pos):
            checkpoint.complete()
            gtid_pos = checkpoint.position
            bookmark_log_file, bookmark_log_pos = end_log_file, end_log_pos

    LOGGER.info('Processed %s rows', processed_rows_events)

    # Update singer bookmark at the last time to point it the last processed binlog event
    if bookmark_log_file and bookmark_log_pos:
        state = update_bookmarks(state,
                                 binlog_streams_map,
                                 bookmark_log_file,
                                 bookmark_log_pos,
                                 gtid_pos)

    return bookmark_log_file, bookmark_log_pos


def create_binlog_stream_reader(
        config: Dict,
        log_file: Optional[str],
        log_pos: Optional[int],
        gtid_pos: Optional[str]
) -> BinLogStreamReader:
    """
    Create an instance of BinlogStreamReader with the right config

    Args:
        config: dictionary of the content of tap config.json
        log_file: binlog file name to start replication from (Optional if using gtid)
        log_pos: binlog pos to start replication from (Optional if using gtid)
        gtid_pos: GTID pos to start replication from (Optional if using log_file & pos)

    Returns: Instance of BinlogStreamReader
    """
    if config.get('server_id'):
        server_id = int(config.get('server_id'))
        LOGGER.info("Using provided server_id=%s", server_id)
    else:
        server_id = random.randint(1, 2 ** 32 - 1)  # generate random server id for this slave
        LOGGER.info("Using randomly generated server_id=%s", server_id)

    engine = config['engine']

    kwargs = {
        'connection_settings': {},
        'pymysql_wrapper': make_connection_wrapper(config),
        # The MariaDB-specific dump protocol ignores file/position arguments unless GTID is used.
        'is_mariadb': connection.MARIADB_ENGINE == engine and config['use_gtid'],
        'server_id': server_id,  # slave server ID
        'report_slave': socket.gethostname() or 'pipelinewise',  # this is so this slave appears in SHOW SLAVE HOSTS;
        'only_events': [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent, NotImplementedEvent,
                        GtidEvent, MariadbGtidEvent, XidEvent, TableMapEvent, XAPrepareEvent],
        'fail_on_table_metadata_unavailable': True,
        'filter_non_implemented_events': False,
    }

    # only fetch events pertaining to the schemas in filter db.
    if config.get('filter_dbs'):
        kwargs['only_schemas'] = config['filter_dbs'].split(',')

    if config['use_gtid']:

        if not gtid_pos:
            raise ValueError(f'gtid_pos is empty "{gtid_pos}"! Cannot start logical replication from empty gtid.')

        LOGGER.info("Starting logical replication from GTID '%s' on engine '%s'", gtid_pos, engine)

        # When using GTID, we want to listen in for GTID events and start from given gtid pos
        kwargs['auto_position'] = gtid_pos

    else:
        if not log_file or not log_pos or log_pos < 0:
            raise ValueError(f'log file or pos is empty ("{log_file}", "{log_pos}")! '
                             f'Cannot start logical replication from invalid log file/pos.')

        LOGGER.info("Starting logical replication from binlog file ['%s', %d]", log_file, log_pos)

        # When not using GTID, we want to listen in for rotate events, and start from given log position and file
        kwargs['only_events'].append(RotateEvent)
        kwargs['log_file'] = log_file
        kwargs['log_pos'] = log_pos
        kwargs['resume_stream'] = True

    return BinLogStreamReader(**kwargs)


def sync_binlog_stream(
        mysql_conn: MySQLConnection,
        config: Dict,
        binlog_streams_map: Dict[str, Any],
        state: Dict) -> None:
    """
    Capture the binlog events created between the pos in the state and current Master position and creates Singer
    streams to be flushed to stdout
    Args:
        mysql_conn: mysql connection instance
        config: tap config
        binlog_streams_map: tables to stream using binlog
        state: the current state
    """
    verify_binlog_config(mysql_conn)

    for tap_stream_id in binlog_streams_map:
        common.whitelist_bookmark_keys(BOOKMARK_KEYS, tap_stream_id, state)

    log_file = log_pos = gtid = None
    end_log_file = end_log_pos = complete_endpoint_gtid = None
    runtime_config = config

    if config['use_gtid']:
        bookmarks = {
            stream: state.get('bookmarks', {}).get(stream, {})
            for stream in binlog_streams_map
        }
        legacy_streams = sorted(
            stream for stream, bookmark in bookmarks.items()
            if not bookmark.get('gtid') or bookmark.get('gtid_complete') is not True
        )
        if legacy_streams:
            missing_coordinates = [
                stream for stream in bookmarks
                if not bookmarks[stream].get('log_file') or not bookmarks[stream].get('log_pos')
            ]
            if missing_coordinates:
                raise ValueError(
                    'Legacy GTID bookmarks require retained file/position coordinates for automatic migration; '
                    f'missing coordinates: {", ".join(missing_coordinates)}. State was not changed.')
            log_file, log_pos = calculate_bookmark(mysql_conn, binlog_streams_map, state)
            log_pos = recover_legacy_binlog_checkpoint(mysql_conn, log_file, log_pos)
            end_log_file, end_log_pos, complete_endpoint_gtid = fetch_current_binlog_checkpoint(
                mysql_conn, config['engine'])
            runtime_config = {**config, 'use_gtid': False}
            LOGGER.info(
                'Migrating legacy GTID bookmarks for %s stream(s) through file/position replay.',
                len(legacy_streams))
        else:
            gtid = calculate_gtid_bookmark(mysql_conn, binlog_streams_map, state, config['engine'])
    else:
        log_file, log_pos = calculate_bookmark(mysql_conn, binlog_streams_map, state)
        log_pos = recover_legacy_binlog_checkpoint(mysql_conn, log_file, log_pos)

    reader = None

    try:
        reader = create_binlog_stream_reader(runtime_config, log_file, log_pos, gtid)

        if end_log_file is None:
            end_log_file, end_log_pos = fetch_current_log_file_and_pos(mysql_conn)
        LOGGER.info('Current Master binlog file and pos: %s %s', end_log_file, end_log_pos)
        final_log_file, final_log_pos = _run_binlog_sync(
            mysql_conn, reader, binlog_streams_map, state, runtime_config, end_log_file, end_log_pos)

        if complete_endpoint_gtid:
            started_at_endpoint = (log_file, log_pos) == (end_log_file, end_log_pos)
            if not started_at_endpoint and (final_log_file, final_log_pos) != (end_log_file, end_log_pos):
                raise RuntimeError(
                    'Legacy GTID migration did not reach the sampled binlog endpoint. Durable state was retained; '
                    'the next scheduled run will retry automatically.')
            update_bookmarks(
                state, binlog_streams_map, end_log_file, end_log_pos, complete_endpoint_gtid)

    finally:
        # BinLogStreamReader doesn't implement the `with` methods
        # So, try/finally will close the chain from the top
        if reader:
            reader.close()

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
