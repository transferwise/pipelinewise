"""Real-engine checks for source identity, FastSync slot verification and the reset_state boundary."""

import json
import os
import subprocess
import threading
import time
import uuid

import psycopg2
import pytest

from pipelinewise import cli
from pipelinewise.cli import source_identity
from pipelinewise.fastsync.commons import tap_postgres
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres

TAP_ID_PREFIX = 'e2e_identity_'
TAP_POSTGRES_PYTHON = os.environ.get(
    'TAP_POSTGRES_PYTHON', os.path.join(cli.VENV_DIR, 'tap-postgres', 'bin', 'python'),
)
VERIFY_ON_REPLICATION_CONNECTION = """
import json, sys
from tap_postgres import db
from tap_postgres.sync_strategies.logical_replication import SourceIdentityMismatchError, verify_source_identity
conn_info, state = json.loads(sys.argv[1]), json.loads(sys.argv[2])
conn = db.open_connection(conn_info, True, True)
try:
    verify_source_identity(conn.cursor(), conn_info, state)
    print('match')
except SourceIdentityMismatchError:
    print('mismatch')
finally:
    conn.close()
"""


def _pg_config(tap_id: str, **overrides) -> dict:
    config = {
        'host': os.environ['TAP_POSTGRES_HOST'],
        'port': os.environ['TAP_POSTGRES_PORT'],
        'user': os.environ['TAP_POSTGRES_USER'],
        'password': os.environ['TAP_POSTGRES_PASSWORD'],
        'dbname': os.environ['TAP_POSTGRES_DB'],
        'tap_id': tap_id,
    }
    config.update(overrides)
    return config


def _mysql_config(prefix: str) -> dict:
    return {
        'host': os.environ[f'{prefix}_HOST'],
        'port': os.environ[f'{prefix}_PORT'],
        'user': os.environ[f'{prefix}_USER'],
        'password': os.environ[f'{prefix}_PASSWORD'],
    }


def _connect():
    config = _pg_config('admin')
    connection = psycopg2.connect(
        host=config['host'], port=config['port'], user=config['user'],
        password=config['password'], dbname=config['dbname'],
    )
    connection.autocommit = True
    return connection


def _query_one(sql, params=None):
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchone()


@pytest.fixture(name='tap_id')
def fixture_tap_id():
    tap_id = f'{TAP_ID_PREFIX}{uuid.uuid4().hex[:12]}'
    yield tap_id
    slot_prefix = FastSyncTapPostgres.generate_replication_slot_name(os.environ['TAP_POSTGRES_DB'], tap_id)
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots '
                'WHERE slot_name = %s AND active_pid IS NULL',
                (slot_prefix,),
            )
    finally:
        connection.close()


def _bookmark(tap_id, state_path=None, synced_tables=None, **overrides):
    tap = FastSyncTapPostgres(_pg_config(tap_id, **overrides), {})
    tap.state_path = state_path
    tap.synced_tables = synced_tables
    tap.open_connection()
    try:
        return tap.fetch_current_log_pos()
    finally:
        tap.close_connection(silent=True)


def _slot(tap_id):
    slot_name = FastSyncTapPostgres.generate_replication_slot_name(os.environ['TAP_POSTGRES_DB'], tap_id)
    return _query_one(
        'SELECT active_pid, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s', (slot_name,)
    )


def test_postgres_identity_is_the_system_identifier_and_insertion_timeline(tap_id):
    identity = source_identity.read_source_identity('tap-postgres', _pg_config(tap_id))

    system_identifier, timeline_id = _query_one(
        'SELECT system_identifier::text, '
        "('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int FROM pg_control_system()"
    )
    assert identity == {
        'host': os.environ['TAP_POSTGRES_HOST'], 'system_identifier': system_identifier, 'timeline_id': timeline_id,
    }


def test_fastsync_bookmark_is_at_or_after_a_new_and_a_reused_slot_start(tap_id):
    first = _bookmark(tap_id)
    second = _bookmark(tap_id)

    _, confirmed_flush_lsn = _slot(tap_id)
    slot_start = tap_postgres.lsn_to_int(confirmed_flush_lsn)
    assert first['lsn'] >= slot_start
    assert second['lsn'] >= slot_start


def test_second_worker_waits_for_a_slot_still_being_created(tap_id):
    """Slot creation waits for running transactions; the duplicate error arrives before the slot is usable."""
    blocker = _connect()
    blocker.autocommit = False
    results = {}

    def run(name):
        try:
            results[name] = _bookmark(tap_id)
        except Exception as exc:
            results[name] = exc

    try:
        with blocker.cursor() as cursor:
            cursor.execute('CREATE TEMP TABLE e2e_identity_blocker (id int)')
            cursor.execute('INSERT INTO e2e_identity_blocker VALUES (1)')

        creator = threading.Thread(target=run, args=('creator',))
        creator.start()
        deadline = time.monotonic() + 30
        while _slot(tap_id) is None and time.monotonic() < deadline:
            time.sleep(0.2)
        active_pid, confirmed_flush_lsn = _slot(tap_id)
        assert active_pid is not None
        assert confirmed_flush_lsn is None

        waiter = threading.Thread(target=run, args=('waiter',))
        waiter.start()
        time.sleep(3)
        assert waiter.is_alive()
    finally:
        blocker.commit()
        blocker.close()

    creator.join(60)
    waiter.join(60)
    assert not creator.is_alive()
    assert not waiter.is_alive()
    assert set(results) == {'creator', 'waiter'}
    _, confirmed_flush_lsn = _slot(tap_id)
    slot_start = tap_postgres.lsn_to_int(confirmed_flush_lsn)
    assert all(isinstance(result, dict) and result['lsn'] >= slot_start for result in results.values()), results


def test_tap_postgres_replication_connection_matches_the_recorded_identity(tap_id):
    """IDENTIFY_SYSTEM on the streaming connection must agree with the identity the CLI records."""
    config = _pg_config(tap_id)
    identity = source_identity.read_source_identity('tap-postgres', config)
    conn_info = dict(config, use_secondary=False)

    def verify(recorded):
        result = subprocess.run(
            [TAP_POSTGRES_PYTHON, '-I', '-c', VERIFY_ON_REPLICATION_CONNECTION,
             json.dumps(conn_info), json.dumps({'bookmarks': {}, 'source_identity': recorded})],
            capture_output=True, text=True, timeout=60, check=True,
        )
        return result.stdout.strip()

    assert verify(identity) == 'match'
    assert verify(dict(identity, timeline_id=identity['timeline_id'] + 1)) == 'mismatch'
    assert verify(dict(identity, system_identifier='1')) == 'mismatch'


def test_missing_slot_is_not_created_behind_unsynced_log_positions(tap_id, tmp_path):
    """After reset_state, a dropped slot must not be recreated by a FastSync of other tables."""
    _bookmark(tap_id)
    slot_name = FastSyncTapPostgres.generate_replication_slot_name(os.environ['TAP_POSTGRES_DB'], tap_id)
    _query_one('SELECT pg_drop_replication_slot(%s)', (slot_name,))
    state_path = tmp_path / 'state.json'
    state_path.write_text(json.dumps({'bookmarks': {'public-other': {'lsn': 1}}}))

    with pytest.raises(tap_postgres.ReplicationSlotVerificationError, match='outside this sync: public-other'):
        _bookmark(tap_id, state_path=str(state_path), synced_tables=['public.synced'])

    assert _slot(tap_id) is None
    assert _bookmark(tap_id, state_path=str(state_path), synced_tables=['public.synced', 'public.other'])['lsn']
    assert _slot(tap_id) is not None


def test_existing_slot_with_another_plugin_is_refused(tap_id):
    slot_name = FastSyncTapPostgres.generate_replication_slot_name(os.environ['TAP_POSTGRES_DB'], tap_id)
    _query_one("SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", (slot_name,))

    with pytest.raises(tap_postgres.ReplicationSlotVerificationError, match='does not belong to this tap'):
        _bookmark(tap_id)


def test_replica_host_that_is_a_primary_is_refused(tap_id):
    """pg_last_wal_replay_lsn() is NULL outside recovery, so a primary cannot supply a replica bookmark."""
    with pytest.raises(tap_postgres.ReplicationSlotVerificationError, match='not a streaming standby'):
        _bookmark(tap_id, replica_host=os.environ['TAP_POSTGRES_HOST'])


def test_reset_state_boundary_must_lie_between_the_slot_start_and_the_current_position(tap_id):
    _bookmark(tap_id)
    _, confirmed_flush_lsn = _slot(tap_id)
    slot_start = tap_postgres.lsn_to_int(confirmed_flush_lsn)
    current = tap_postgres.lsn_to_int(_query_one('SELECT pg_current_wal_lsn()::text')[0])
    config = _pg_config(tap_id)

    assert source_identity.verify_postgres_slot_covers_boundary(config, slot_start)
    with pytest.raises(source_identity.SourceIdentityError, match='after the boundary'):
        source_identity.verify_postgres_slot_covers_boundary(config, slot_start - 1)
    with pytest.raises(source_identity.SourceIdentityError, match='ahead of the current WAL position'):
        source_identity.verify_postgres_slot_covers_boundary(config, current + (1 << 32))


def test_mariadb_primary_and_replica_have_distinct_identities():
    """MariaDB has no server_uuid, so server_id is what tells two servers behind one host name apart."""
    primary = source_identity.read_source_identity('tap-mysql', _mysql_config('TAP_MYSQL'))
    replica = source_identity.read_source_identity('tap-mysql', _mysql_config('TAP_MYSQL_REPLICA'))

    assert set(primary) == {'host', 'server_id'}
    assert set(replica) == {'host', 'server_id'}
    assert primary['server_id'] != replica['server_id']


def test_mysql_identity_is_the_server_uuid():
    identity = source_identity.read_source_identity('tap-mysql', _mysql_config('TAP_ORACLE_MYSQL'))

    assert set(identity) == {'server_uuid'}
    assert len(identity['server_uuid']) == 36
