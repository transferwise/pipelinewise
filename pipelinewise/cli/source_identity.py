"""Bind tap state to the source instance whose log positions it holds."""
import re
from contextlib import closing

from pipelinewise.fastsync.commons.tap_postgres import (
    FastSyncTapPostgres,
    ReplicationSlotVerificationError,
    finished_slot_start,
    int_to_lsn,
    lsn_to_int,
    read_primary_history,
    read_slot,
)

from .source_table_sizes import open_source_connection

STATE_KEY = 'source_identity'

POSTGRES_CURRENT_LSN_SQL = 'SELECT pg_current_wal_lsn()::text'
MYSQL_IDENTITY_SQL = "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('server_uuid', 'server_id')"
LSN_PATTERN = re.compile(r'^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$')


class SourceIdentityError(Exception):
    """The connected source cannot be proven to be the one the tap state belongs to."""


def read_source_identity(tap_type: str, config: dict) -> dict | None:
    """Read the identity of the connected source, or None for tap types without a check.

    PostgreSQL uses the configured host with system_identifier and the current timeline:
    a promoted physical replica keeps the system_identifier but starts a new timeline,
    and its log positions after the fork describe a different history. MySQL reports
    server_uuid. MariaDB has no server_uuid, so the configured host and server_id are used.
    """
    if tap_type == 'tap-postgres':
        with closing(open_source_connection(tap_type, config)) as connection:
            system_identifier, timeline_id = read_primary_history(connection)
        return {'host': config['host'], 'system_identifier': system_identifier, 'timeline_id': timeline_id}

    if tap_type == 'tap-mysql':
        with closing(open_source_connection(tap_type, config)) as connection, connection.cursor() as cursor:
            cursor.execute(MYSQL_IDENTITY_SQL)
            variables = dict(cursor.fetchall())
        if variables.get('server_uuid'):
            return {'server_uuid': variables['server_uuid']}
        if not variables.get('server_id'):
            raise SourceIdentityError('Cannot read server_id from the MariaDB/MySQL source')
        return {'host': config['host'], 'server_id': int(variables['server_id'])}

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
    """Check the tap's slot on the connected primary starts at or before the boundary.

    Returns the slot name. A tap state reset to lsn=1 streams from the slot's
    confirmed_flush_lsn, so commits between the boundary and a later slot start
    would never be read. A boundary ahead of the primary's current WAL position
    cannot be a switchover boundary on it and is refused.
    """
    dbname = config['dbname']
    with closing(open_source_connection('tap-postgres', config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(POSTGRES_CURRENT_LSN_SQL)
            current_lsn = cursor.fetchone()[0]
        name = FastSyncTapPostgres.resolve_slot_name(connection, dbname, config['tap_id'])
        slot = read_slot(connection, name)

    if boundary_lsn > lsn_to_int(current_lsn):
        raise SourceIdentityError(
            f'Boundary {int_to_lsn(boundary_lsn)} is ahead of the current WAL position {current_lsn} '
            'on the connected source. It cannot be the switchover boundary of this primary.'
        )
    if slot is None:
        raise SourceIdentityError(f'Replication slot "{name}" does not exist on the source')
    try:
        start_lsn = finished_slot_start(slot, dbname)
    except ReplicationSlotVerificationError as exc:
        raise SourceIdentityError(str(exc)) from exc
    if start_lsn is None:
        raise SourceIdentityError(
            f'Replication slot "{name}" is not ready: active_pid {slot["active_pid"]}, '
            f'confirmed_flush_lsn {slot["confirmed_flush_lsn"]}'
        )
    if start_lsn > boundary_lsn:
        raise SourceIdentityError(
            f'Replication slot "{name}" starts at {slot["confirmed_flush_lsn"]}, after the boundary '
            f'{int_to_lsn(boundary_lsn)}. Commits in between would be skipped. Recreate the slot '
            'before the boundary or resync the tap.'
        )
    return name
