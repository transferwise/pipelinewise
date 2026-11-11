"""Bind tap state to the source instance whose log positions it holds."""
import re
from contextlib import closing

from pipelinewise.fastsync.commons.tap_postgres import (
    SLOT_PLUGIN,
    FastSyncTapPostgres,
    int_to_lsn,
    lsn_to_int,
)

from .source_table_sizes import open_source_connection

STATE_KEY = 'source_identity'

POSTGRES_IDENTITY_SQL = (
    'SELECT (SELECT system_identifier FROM pg_control_system())::text, '
    'pg_walfile_name(pg_current_wal_lsn())'
)
POSTGRES_SLOTS_SQL = (
    'SELECT slot_name, database, plugin, slot_type, temporary, active_pid, confirmed_flush_lsn '
    'FROM pg_replication_slots WHERE slot_name IN (%s, %s)'
)
MYSQL_SERVER_UUID_SQL = "SHOW GLOBAL VARIABLES LIKE 'server_uuid'"
LSN_PATTERN = re.compile(r'^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$')


class SourceIdentityError(Exception):
    """The connected source cannot be proven to be the one the tap state belongs to."""


def read_source_identity(tap_type: str, config: dict) -> dict | None:
    """Read the identity of the connected source, or None for tap types without a check.

    PostgreSQL uses system_identifier with the current timeline: a promoted physical
    replica keeps the system_identifier but starts a new timeline, and its log positions
    after the fork describe a different history. MySQL reports server_uuid. MariaDB has
    no server_uuid, so the configured host is used.
    """
    if tap_type == 'tap-postgres':
        with closing(open_source_connection(tap_type, config)) as connection, connection.cursor() as cursor:
            cursor.execute(POSTGRES_IDENTITY_SQL)
            system_identifier, wal_file = cursor.fetchone()
        return {'system_identifier': system_identifier, 'timeline_id': int(wal_file[:8], 16)}

    if tap_type == 'tap-mysql':
        with closing(open_source_connection(tap_type, config)) as connection, connection.cursor() as cursor:
            cursor.execute(MYSQL_SERVER_UUID_SQL)
            row = cursor.fetchone()
        if row and row[1]:
            return {'server_uuid': row[1]}
        return {'host': config['host']}

    return None


def bind_state_to_source(state: dict, identity: dict) -> bool:
    """Check state against the connected source identity, recording it when absent.

    Returns True when the identity was added and the state needs saving. Raises
    SourceIdentityError when the state belongs to another source.
    """
    recorded = state.get(STATE_KEY)
    if recorded is None:
        state[STATE_KEY] = identity
        return True
    if recorded != identity:
        raise SourceIdentityError(
            f'Tap state belongs to source {recorded} but the tap is connected to {identity}. '
            'Its log positions do not apply to this source. After a switchover, run reset_state; '
            'otherwise check the tap host and restore the matching state.'
        )
    return False


def parse_lsn(value: str | None) -> int:
    """Parse an operator-supplied LSN in X/Y form."""
    if value is None or not LSN_PATTERN.match(value):
        raise SourceIdentityError(f'Invalid PostgreSQL LSN {value!r}; expected the X/Y form, e.g. 16/B374D848')
    return lsn_to_int(value)


def verify_postgres_slot_covers_boundary(config: dict, boundary_lsn: int) -> str:
    """Check the tap's slot on the connected source starts at or before the boundary.

    Returns the slot name. A tap state reset to lsn=1 streams from the slot's
    confirmed_flush_lsn, so commits between the boundary and a later slot start
    would never be read.
    """
    dbname = config['dbname']
    legacy_name = FastSyncTapPostgres.generate_replication_slot_name(dbname)
    slot_name = FastSyncTapPostgres.generate_replication_slot_name(dbname, config['tap_id'])
    with closing(open_source_connection('tap-postgres', config)) as connection, connection.cursor() as cursor:
        cursor.execute(POSTGRES_SLOTS_SQL, (legacy_name, slot_name))
        slots = {row[0]: row[1:] for row in cursor.fetchall()}

    name = legacy_name if legacy_name in slots else slot_name
    if name not in slots:
        raise SourceIdentityError(f'Replication slot "{slot_name}" does not exist on the source')

    database, plugin, slot_type, temporary, active_pid, confirmed_flush_lsn = slots[name]
    if slot_type != 'logical' or database != dbname or plugin != SLOT_PLUGIN or temporary:
        raise SourceIdentityError(
            f'Replication slot "{name}" does not belong to this tap: type {slot_type}, database {database}, '
            f'plugin {plugin}, temporary {temporary}'
        )
    if confirmed_flush_lsn is None or active_pid is not None:
        raise SourceIdentityError(
            f'Replication slot "{name}" is not ready: active_pid {active_pid}, '
            f'confirmed_flush_lsn {confirmed_flush_lsn}'
        )
    if lsn_to_int(confirmed_flush_lsn) > boundary_lsn:
        raise SourceIdentityError(
            f'Replication slot "{name}" starts at {confirmed_flush_lsn}, after the boundary '
            f'{int_to_lsn(boundary_lsn)}. Commits in between would be skipped. Recreate the slot '
            'before the boundary or resync the tap.'
        )
    return name
