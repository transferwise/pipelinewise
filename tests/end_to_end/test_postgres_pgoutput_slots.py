"""PostgreSQL pgoutput boundary and wal2json migration E2E coverage."""

import base64
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import psycopg2
import pytest

from .helpers.env import E2EEnv


TAP_ID = 'postgres_pgoutput_migration'
TARGET_ID = 'postgres_pgoutput_migration_dwh'
SOURCE_SCHEMA = 'ppw_e2e_pgoutput_source'
TARGET_SCHEMA = 'ppw_e2e_pgoutput_target'
TABLE_NAME = 'migration_records'
STREAM_ID = f'{SOURCE_SCHEMA}-{TABLE_NAME}'
PARTITIONED_TABLE_NAME = 'partitioned_records'
PARTITIONED_STREAM_ID = f'{SOURCE_SCHEMA}-{PARTITIONED_TABLE_NAME}'
TEMPLATE_DIR = Path(__file__).parent / 'postgres_pgoutput_test_project'
MIGRATION_STATE_KEY = '_pipelinewise_pgoutput_migration'
PUBLICATION_NAME = f'ppw_slot_{TAP_ID}'
PUBLICATION_FENCE_COMMENT_PREFIX = 'pipelinewise-publication-fence-v1:'


@dataclass(frozen=True)
class MigrationCase:
    """Isolated PostgreSQL migration resources owned by one E2E test."""

    e2e: E2EEnv
    project_dir: Path
    config_dir: Path
    command_env: dict
    state_path: Path
    wal2json_slot: str
    pgoutput_slot: str

    @property
    def run_command(self):
        """Return the configured tap invocation."""
        return ['pipelinewise', 'run_tap', '--tap', TAP_ID, '--target', TARGET_ID]


def _start(command, env):
    """Start a PipelineWise command in an independently terminable process group."""
    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        start_new_session=True,
    )


def _wait(process, command, timeout=120):
    """Wait for a PipelineWise command with bounded output capture."""
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        stdout, stderr = _stop_process(process)
        raise AssertionError(
            f'Command timed out: {command}\nstdout:\n{stdout}\nstderr:\n{stderr}'
        ) from exc
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def _run(command, env, timeout=120):
    """Run a PipelineWise command with bounded output capture."""
    return _wait(_start(command, env), command, timeout)


def _stop_process(process, timeout=20):
    """Terminate a timed-out PipelineWise command and its connector processes."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        return process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        return process.communicate(timeout=10)


def _run_success(command, env, timeout=120):
    """Run a PipelineWise command and include complete output on failure."""
    process = _run(command, env, timeout)
    assert process.returncode == 0, (
        f'Command failed with return code {process.returncode}: {command}\n'
        f'stdout:\n{process.stdout}\nstderr:\n{process.stderr}'
    )
    return process.stdout


def _slot_name(*parts):
    """Build the exact normalized slot name used by PipelineWise."""
    return re.sub('[^a-z0-9_]', '_', '_'.join(('pipelinewise', *parts)).lower())[:63]


def _slot_status(e2e, slot_name):
    """Return the slot identity and durable boundary, if it exists."""
    rows = e2e.run_query_tap_postgres(
        """
        SELECT plugin,
               database,
               active,
               (confirmed_flush_lsn - '0/0'::pg_lsn)::bigint
          FROM pg_replication_slots
         WHERE slot_name = %s
        """,
        (slot_name,),
    )
    if not rows:
        return None
    plugin, database, active, confirmed_flush_lsn = rows[0]
    return {
        'plugin': plugin,
        'database': database,
        'active': active,
        'confirmed_flush_lsn': int(confirmed_flush_lsn),
    }


def _drop_slot(e2e, slot_name):
    """Drop one inactive test-owned replication slot if it exists."""
    status = _slot_status(e2e, slot_name)
    if status is None:
        return
    assert not status['active'], f'Refusing to drop active test slot {slot_name}'
    e2e.run_query_tap_postgres(
        'SELECT pg_drop_replication_slot(%s)',
        (slot_name,),
    )


def _lsn_to_int(lsn):
    """Convert either a PostgreSQL textual LSN or an integer bookmark."""
    if isinstance(lsn, int):
        return lsn
    upper, lower = str(lsn).split('/')
    return (int(upper, 16) << 32) + int(lower, 16)


def _read_state_lsn(state_path):
    """Return this test stream's persisted integer LSN."""
    state = _read_state(state_path)
    return _lsn_to_int(state['bookmarks'][STREAM_ID]['lsn'])


def _read_state(state_path):
    """Return one complete persisted Singer state."""
    with state_path.open(encoding='utf-8') as state_file:
        return json.load(state_file)


def _source_target_rows(e2e):
    """Return compact fingerprints of business columns in primary-key order."""
    source_rows = e2e.run_query_tap_postgres(
        f'SELECT id, status, octet_length(payload), md5(payload) '
        f'FROM {SOURCE_SCHEMA}.{TABLE_NAME} ORDER BY id'
    )
    target_rows = e2e.run_query_target_postgres(
        f'SELECT id, status, octet_length(payload), md5(payload) '
        f'FROM {TARGET_SCHEMA}.{TABLE_NAME} ORDER BY id'
    )
    return source_rows, target_rows


def _partition_source_target_rows(e2e):
    """Return partition-root rows in primary-key order on each side."""
    source_rows = e2e.run_query_tap_postgres(
        f'SELECT id, bucket, status FROM {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME} '
        'ORDER BY id, bucket'
    )
    target_rows = e2e.run_query_target_postgres(
        f'SELECT id, bucket, status FROM {TARGET_SCHEMA}.{PARTITIONED_TABLE_NAME} '
        'ORDER BY id, bucket'
    )
    return source_rows, target_rows


def _select_logical_table(project_dir, table_name):
    """Persist another logical table in the generated test project."""
    tap_yaml = project_dir / 'tap_postgres_pgoutput_to_pg.yml'
    with tap_yaml.open('a', encoding='utf-8') as config_file:
        config_file.write(
            f'      - table_name: "{table_name}"\n'
            '        replication_method: "LOG_BASED"\n'
        )


def _connect_source(e2e):
    """Open a source connection for a transaction that spans publication setup."""
    return psycopg2.connect(
        host=e2e.get_conn_env_var('TAP_POSTGRES', 'HOST'),
        port=e2e.get_conn_env_var('TAP_POSTGRES', 'PORT'),
        user=e2e.get_conn_env_var('TAP_POSTGRES', 'USER'),
        password=e2e.get_conn_env_var('TAP_POSTGRES', 'PASSWORD'),
        database=e2e.get_conn_env_var('TAP_POSTGRES', 'DB'),
    )


def _wait_for_publication_fence(e2e, process, command, timeout=20):
    """Wait until durable publication metadata records an active transaction fence."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            result = _wait(process, command)
            raise AssertionError(
                'FastSync exited before reaching the publication transaction fence\n'
                f'stdout:\n{result.stdout}\nstderr:\n{result.stderr}'
            )
        comment_rows = e2e.run_query_tap_postgres(
            "SELECT pg_catalog.obj_description(publication.oid, 'pg_publication') "
            'FROM pg_catalog.pg_publication AS publication WHERE pubname = %s',
            (PUBLICATION_NAME,),
        )
        if (
            comment_rows
            and comment_rows[0][0] is not None
            and comment_rows[0][0].startswith(PUBLICATION_FENCE_COMMENT_PREFIX)
            and _decode_publication_fence_comment(comment_rows[0][0]) == {
                'state': 'pending',
                'original_comment': None,
                'managed_tables': [[SOURCE_SCHEMA, TABLE_NAME]],
            }
        ):
            assert process.poll() is None
            return
        time.sleep(0.1)
    raise AssertionError('FastSync did not reach the publication transaction fence in time')


def _drop_publication(e2e):
    """Drop the test-owned publication if it exists."""
    e2e.run_query_tap_postgres(f'DROP PUBLICATION IF EXISTS {PUBLICATION_NAME}')


def _publication_comment(e2e):
    """Return the test publication's persisted comment."""
    return e2e.run_query_tap_postgres(
        "SELECT pg_catalog.obj_description(publication.oid, 'pg_publication') "
        'FROM pg_catalog.pg_publication AS publication WHERE pubname = %s',
        (PUBLICATION_NAME,),
    )[0][0]


def _decode_publication_fence_comment(comment):
    """Decode the persisted publication-fence metadata contract."""
    assert comment.startswith(PUBLICATION_FENCE_COMMENT_PREFIX)
    payload = comment.removeprefix(PUBLICATION_FENCE_COMMENT_PREFIX)
    return json.loads(base64.urlsafe_b64decode(payload.encode()).decode())


def _publication_fence_metadata(e2e):
    """Return decoded crash-persistent publication-fence metadata."""
    return _decode_publication_fence_comment(_publication_comment(e2e))


def _publication_tables(e2e):
    """Return the exact relations published for this tap."""
    return set(e2e.run_query_tap_postgres(
        'SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables '
        'WHERE pubname = %s',
        (PUBLICATION_NAME,),
    ))


def _simulate_historical_tap_identity(config_dir, old_tap_id, lsn):
    """Retain a complete generated configuration as it existed before this upgrade."""
    old_dir = config_dir / TARGET_ID / old_tap_id
    (config_dir / TARGET_ID / TAP_ID).rename(old_dir)
    old_config_path = old_dir / 'config.json'
    old_config = _read_state(old_config_path)
    old_config['tap_id'] = old_tap_id
    old_config_path.write_text(json.dumps(old_config), encoding='utf-8')
    state_path = old_dir / 'state.json'
    state = _read_state(state_path)
    assert MIGRATION_STATE_KEY not in state
    state['bookmarks'][STREAM_ID]['lsn'] = lsn
    state_path.write_text(json.dumps(state), encoding='utf-8')
    root_config_path = config_dir / 'config.json'
    root_config = _read_state(root_config_path)
    for target in root_config['targets']:
        for tap in target['taps']:
            if tap['id'] == TAP_ID:
                tap['id'] = old_tap_id
    root_config_path.write_text(json.dumps(root_config), encoding='utf-8')
    return state_path


@contextmanager
def _simple_migration_case(tmp_path, *, break_at_end_lsn=True):
    """Prepare one filtered FastSync beside a dedicated historical slot."""
    project_dir = tmp_path / 'project'
    shutil.copytree(TEMPLATE_DIR, project_dir)
    e2e = E2EEnv(project_dir)
    tap_yaml = project_dir / 'tap_postgres_pgoutput_to_pg.yml'
    if not break_at_end_lsn:
        tap_config = tap_yaml.read_text(encoding='utf-8')
        tap_config = tap_config.replace(
            'logical_poll_total_seconds: 10', 'logical_poll_total_seconds: 45'
        ).replace(
            'break_at_end_lsn: true', 'break_at_end_lsn: false'
        ).replace(
            'max_run_seconds: 20', 'max_run_seconds: 60'
        )
        tap_yaml.write_text(tap_config, encoding='utf-8')

    config_dir = tmp_path / 'pipelinewise-config'
    config_dir.mkdir()
    command_env = {**os.environ, 'PIPELINEWISE_CONFIG_DIRECTORY': str(config_dir)}
    database = e2e.get_conn_env_var('TAP_POSTGRES', 'DB')
    wal2json_slot = _slot_name(database, TAP_ID)
    legacy_wal2json_slot = _slot_name(database)
    pgoutput_slot = f'ppw_slot_{TAP_ID}'
    state_path = config_dir / TARGET_ID / TAP_ID / 'state.json'
    migration_case = MigrationCase(
        e2e=e2e,
        project_dir=project_dir,
        config_dir=config_dir,
        command_env=command_env,
        state_path=state_path,
        wal2json_slot=wal2json_slot,
        pgoutput_slot=pgoutput_slot,
    )
    try:
        _drop_slot(e2e, pgoutput_slot)
        _drop_slot(e2e, wal2json_slot)
        _drop_publication(e2e)
        assert _slot_status(e2e, legacy_wal2json_slot) is None
        e2e.run_query_tap_postgres(
            f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE; '
            f'CREATE SCHEMA {SOURCE_SCHEMA}; '
            f'CREATE TABLE {SOURCE_SCHEMA}.{TABLE_NAME} '
            '(id integer PRIMARY KEY, status text NOT NULL, payload text NOT NULL); '
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (1, 'initial', 'payload-1')"
        )
        e2e.run_query_target_postgres(
            f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE'
        )
        e2e.run_query_tap_postgres(
            'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
            (wal2json_slot, 'wal2json'),
        )
        _run_success(
            ['pipelinewise', 'validate', '--dir', str(project_dir)], command_env
        )
        _run_success(
            ['pipelinewise', 'import_config', '--dir', str(project_dir)], command_env
        )
        _run_success(
            [
                'pipelinewise',
                'fast_sync',
                '--tap',
                TAP_ID,
                '--target',
                TARGET_ID,
                '--tables',
                f'{SOURCE_SCHEMA}.{TABLE_NAME}',
            ],
            command_env,
        )
        assert _source_target_rows(e2e)[0] == _source_target_rows(e2e)[1]
        assert _slot_status(e2e, wal2json_slot)['plugin'] == 'wal2json'
        assert _slot_status(e2e, pgoutput_slot)['plugin'] == 'pgoutput'
        yield migration_case
    finally:
        _drop_slot(e2e, pgoutput_slot)
        _drop_slot(e2e, wal2json_slot)
        _drop_publication(e2e)
        e2e.run_query_tap_postgres(
            f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE'
        )
        e2e.run_query_target_postgres(
            f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE'
        )


def _assert_promoted_overlap(migration_case):
    """Require the durable promotion state and immediate wal2json retirement."""
    marker = _read_state(migration_case.state_path)[MIGRATION_STATE_KEY]
    assert marker['phase'] == 'pgoutput_overlap'
    assert marker['source_slot'] == migration_case.wal2json_slot
    assert marker['destination_slot'] == migration_case.pgoutput_slot
    assert _slot_status(migration_case.e2e, migration_case.wal2json_slot) is None
    pgoutput_slot = _slot_status(migration_case.e2e, migration_case.pgoutput_slot)
    assert pgoutput_slot['confirmed_flush_lsn'] == marker['slot_lsn']
    return marker


def _finish_overlap(migration_case):
    """Replay the shared pgoutput boundary and require normal durable state."""
    _run_success(migration_case.run_command, migration_case.command_env, timeout=40)
    assert MIGRATION_STATE_KEY not in _read_state(migration_case.state_path)
    assert _slot_status(migration_case.e2e, migration_case.wal2json_slot) is None


def test_idle_migration_completes_without_filling_target_batch(tmp_path):
    """A message-only boundary retires wal2json when no source row changed."""
    with _simple_migration_case(tmp_path) as migration_case:
        assert len(_source_target_rows(migration_case.e2e)[0]) == 1
        _run_success(migration_case.run_command, migration_case.command_env)
        _assert_promoted_overlap(migration_case)
        _finish_overlap(migration_case)
        assert _source_target_rows(migration_case.e2e)[0] == _source_target_rows(
            migration_case.e2e
        )[1]


def test_bridge_target_failure_retains_wal2json_for_retry(tmp_path):
    """A failed bridge write cannot promote pgoutput or retire wal2json."""
    with _simple_migration_case(tmp_path) as migration_case:
        initial_target_rows = _source_target_rows(migration_case.e2e)[1]
        original_pgoutput_lsn = _slot_status(
            migration_case.e2e, migration_case.pgoutput_slot
        )['confirmed_flush_lsn']
        original_wal2json_lsn = _slot_status(
            migration_case.e2e, migration_case.wal2json_slot
        )['confirmed_flush_lsn']
        migration_case.e2e.run_query_tap_postgres(
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} "
            "VALUES (2, 'must retry', 'payload-2')"
        )
        migration_case.e2e.run_query_target_postgres(
            f'CREATE FUNCTION {TARGET_SCHEMA}.reject_bridge_write() '
            'RETURNS trigger LANGUAGE plpgsql AS $$ '
            "BEGIN RAISE EXCEPTION 'deliberate bridge target failure'; END $$; "
            f'CREATE TRIGGER reject_bridge_write BEFORE INSERT OR UPDATE '
            f'ON {TARGET_SCHEMA}.{TABLE_NAME} FOR EACH ROW '
            f'EXECUTE FUNCTION {TARGET_SCHEMA}.reject_bridge_write()'
        )
        try:
            failed_run = _run(migration_case.run_command, migration_case.command_env)
        finally:
            migration_case.e2e.run_query_target_postgres(
                f'DROP TRIGGER IF EXISTS reject_bridge_write '
                f'ON {TARGET_SCHEMA}.{TABLE_NAME}; '
                f'DROP FUNCTION IF EXISTS {TARGET_SCHEMA}.reject_bridge_write()'
            )

        assert failed_run.returncode != 0
        assert MIGRATION_STATE_KEY not in _read_state(migration_case.state_path)
        wal2json_slot = _slot_status(
            migration_case.e2e, migration_case.wal2json_slot
        )
        assert wal2json_slot['confirmed_flush_lsn'] == original_wal2json_lsn
        pgoutput_slot = _slot_status(migration_case.e2e, migration_case.pgoutput_slot)
        assert pgoutput_slot['confirmed_flush_lsn'] == original_pgoutput_lsn
        assert _source_target_rows(migration_case.e2e)[1] == initial_target_rows

        _run_success(migration_case.run_command, migration_case.command_env)
        _assert_promoted_overlap(migration_case)
        _finish_overlap(migration_case)
        assert _source_target_rows(migration_case.e2e)[0] == _source_target_rows(
            migration_case.e2e
        )[1]


def test_unfinished_migration_can_be_replaced_by_whole_tap_fastsync(tmp_path):
    """An explicit whole-tap reset removes migration debt before new snapshots."""
    with _simple_migration_case(tmp_path) as migration_case:
        _run_success(migration_case.run_command, migration_case.command_env)
        previous_marker = _assert_promoted_overlap(migration_case)
        previous_slot_lsn = previous_marker['slot_lsn']

        _run_success(
            ['pipelinewise', 'fast_sync', '--tap', TAP_ID, '--target', TARGET_ID],
            migration_case.command_env,
        )

        reset_state = _read_state(migration_case.state_path)
        assert MIGRATION_STATE_KEY not in reset_state
        assert '_pipelinewise_pgoutput_fresh_start' not in reset_state
        assert _slot_status(migration_case.e2e, migration_case.wal2json_slot) is None
        reset_slot = _slot_status(migration_case.e2e, migration_case.pgoutput_slot)
        assert reset_slot['plugin'] == 'pgoutput'
        assert reset_slot['confirmed_flush_lsn'] > previous_slot_lsn

        migration_case.e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'after reset' WHERE id = 1"
        )
        _run_success(migration_case.run_command, migration_case.command_env)
        assert _source_target_rows(migration_case.e2e)[0] == _source_target_rows(
            migration_case.e2e
        )[1]


def test_continuous_source_traffic_does_not_delay_migration_boundary(tmp_path):
    """Migration boundaries stop both decoders even when ordinary runs do not."""
    with _simple_migration_case(tmp_path, break_at_end_lsn=False) as migration_case:
        stop_writes = threading.Event()
        writer_errors = []

        def write_source_rows():
            connection = _connect_source(migration_case.e2e)
            connection.autocommit = True
            next_id = 2
            try:
                with connection.cursor() as cursor:
                    while not stop_writes.is_set():
                        cursor.execute(
                            f'INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} '
                            '(id, status, payload) VALUES (%s, %s, %s)',
                            (next_id, f'continuous-{next_id}', f'payload-{next_id}'),
                        )
                        next_id += 1
                        time.sleep(0.05)
            except Exception as exc:
                writer_errors.append(exc)
            finally:
                connection.close()

        writer = threading.Thread(target=write_source_rows, daemon=True)
        writer.start()
        try:
            _run_success(
                migration_case.run_command, migration_case.command_env, timeout=40
            )
            _assert_promoted_overlap(migration_case)
            assert writer.is_alive()
            _finish_overlap(migration_case)
            assert writer.is_alive()
        finally:
            stop_writes.set()
            writer.join(timeout=10)

        assert not writer.is_alive()
        assert writer_errors == []
        _run_success(migration_case.run_command, migration_case.command_env, timeout=70)
        assert _source_target_rows(migration_case.e2e)[0] == _source_target_rows(
            migration_case.e2e
        )[1]


def test_fenced_migration_rejects_config_change_and_survives_ddl(tmp_path):
    """Selection stays frozen while old relation WAL replays across source DDL."""
    added_table = 'migration_added_records'
    with _simple_migration_case(tmp_path) as migration_case:
        _run_success(migration_case.run_command, migration_case.command_env)
        _assert_promoted_overlap(migration_case)

        migration_case.e2e.run_query_tap_postgres(
            f'ALTER TABLE {SOURCE_SCHEMA}.{TABLE_NAME} '
            "ADD COLUMN detail text NOT NULL DEFAULT 'added-during-overlap'; "
            f'CREATE TABLE {SOURCE_SCHEMA}.{added_table} '
            '(id integer PRIMARY KEY, status text NOT NULL); '
            f"INSERT INTO {SOURCE_SCHEMA}.{added_table} VALUES (1, 'new selection')"
        )
        _select_logical_table(migration_case.project_dir, added_table)
        failed_import = _run(
            [
                'pipelinewise',
                'import_config',
                '--dir',
                str(migration_case.project_dir),
            ],
            migration_case.command_env,
        )
        assert failed_import.returncode != 0
        assert 'selection or options cannot change' in (
            failed_import.stdout + failed_import.stderr
        )
        assert _publication_tables(migration_case.e2e) == {
            (SOURCE_SCHEMA, TABLE_NAME)
        }

        _finish_overlap(migration_case)
        _run_success(
            [
                'pipelinewise',
                'import_config',
                '--dir',
                str(migration_case.project_dir),
            ],
            migration_case.command_env,
        )
        assert _publication_tables(migration_case.e2e) == {
            (SOURCE_SCHEMA, TABLE_NAME),
            (SOURCE_SCHEMA, added_table),
        }
        _run_success(
            [
                'pipelinewise',
                'fast_sync',
                '--tap',
                TAP_ID,
                '--target',
                TARGET_ID,
                '--tables',
                f'{SOURCE_SCHEMA}.{added_table}',
            ],
            migration_case.command_env,
        )
        migration_case.e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET detail = 'decoded after overlap' WHERE id = 1"
        )
        _run_success(migration_case.run_command, migration_case.command_env)
        assert migration_case.e2e.run_query_target_postgres(
            f'SELECT id, detail FROM {TARGET_SCHEMA}.{TABLE_NAME} ORDER BY id'
        ) == [(1, 'decoded after overlap')]
        assert migration_case.e2e.run_query_target_postgres(
            f'SELECT id, status FROM {TARGET_SCHEMA}.{added_table} ORDER BY id'
        ) == [(1, 'new selection')]


def test_renamed_legacy_tap_preserves_checkpoint_and_retires_truncated_slot(tmp_path):
    """Import a valid replacement ID without losing its existing target or WAL history."""
    project_dir = tmp_path / 'project'
    shutil.copytree(TEMPLATE_DIR, project_dir)
    e2e = E2EEnv(project_dir)
    config_dir = tmp_path / 'pipelinewise-config'
    config_dir.mkdir()
    command_env = {**os.environ, 'PIPELINEWISE_CONFIG_DIRECTORY': str(config_dir)}
    old_tap_id = 'Historical-Postgres-' + 'x' * 45
    database = e2e.get_conn_env_var('TAP_POSTGRES', 'DB')
    old_slot = _slot_name(database, old_tap_id)
    new_slot = f'ppw_slot_{TAP_ID}'
    run_command = ['pipelinewise', 'run_tap', '--tap', TAP_ID, '--target', TARGET_ID]
    state_path = config_dir / TARGET_ID / TAP_ID / 'state.json'
    try:
        _drop_slot(e2e, new_slot)
        _drop_slot(e2e, old_slot)
        _drop_publication(e2e)
        e2e.run_query_tap_postgres(
            f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE; CREATE SCHEMA {SOURCE_SCHEMA}; '
            f'CREATE TABLE {SOURCE_SCHEMA}.{TABLE_NAME} '
            '(id integer PRIMARY KEY, status text NOT NULL, payload text NOT NULL); '
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (1, 'existing target row', 'payload')"
        )
        e2e.run_query_target_postgres(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')
        _run_success(['pipelinewise', 'import_config', '--dir', str(project_dir)], command_env)
        _run_success(
            ['pipelinewise', 'fast_sync', '--tap', TAP_ID, '--target', TARGET_ID,
             '--tables', f'{SOURCE_SCHEMA}.{TABLE_NAME}'],
            command_env,
        )
        assert _source_target_rows(e2e)[0] == _source_target_rows(e2e)[1]
        _drop_slot(e2e, new_slot)
        _drop_publication(e2e)
        e2e.run_query_tap_postgres(
            'SELECT * FROM pg_create_logical_replication_slot(%s, %s)', (old_slot, 'wal2json'),
        )
        assert len(old_slot) == 63
        old_boundary = _slot_status(e2e, old_slot)['confirmed_flush_lsn']
        old_state_path = _simulate_historical_tap_identity(config_dir, old_tap_id, old_boundary)
        old_state = _read_state(old_state_path)
        e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'committed before rename' WHERE id = 1; "
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (2, 'new row before rename', 'payload2')"
        )
        with (project_dir / 'tap_postgres_pgoutput_to_pg.yml').open('a', encoding='utf-8') as tap_yaml:
            tap_yaml.write(f'\nprevious_tap_id: "{old_tap_id}"\n')
        _run_success(['pipelinewise', 'import_config', '--dir', str(project_dir)], command_env)
        assert _read_state(state_path) == old_state
        assert _read_state(old_state_path) == old_state
        assert _slot_status(e2e, old_slot)['confirmed_flush_lsn'] == old_boundary
        assert _slot_status(e2e, new_slot) is None

        _run_success(run_command, command_env)
        bridge_state = _read_state(state_path)
        bridge_marker = bridge_state[MIGRATION_STATE_KEY]
        assert bridge_marker['phase'] == 'pgoutput_overlap'
        assert bridge_marker['source_slot'] == old_slot
        assert bridge_marker['destination_slot'] == new_slot
        assert _slot_status(e2e, old_slot) is None
        assert _source_target_rows(e2e)[0] == _source_target_rows(e2e)[1]
        _run_success(run_command, command_env)
        assert _slot_status(e2e, old_slot) is None
        assert MIGRATION_STATE_KEY not in _read_state(state_path)
        assert _source_target_rows(e2e)[0] == _source_target_rows(e2e)[1]

        e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'pgoutput after rename' WHERE id = 2"
        )
        _run_success(run_command, command_env)
        assert _source_target_rows(e2e)[0] == _source_target_rows(e2e)[1]
        final_state = _read_state(state_path)
        _run_success(['pipelinewise', 'import_config', '--dir', str(project_dir)], command_env)
        assert _read_state(state_path) == final_state
        assert _read_state(old_state_path) == old_state
    finally:
        _drop_slot(e2e, new_slot)
        _drop_slot(e2e, old_slot)
        _drop_publication(e2e)
        e2e.run_query_tap_postgres(f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE')
        e2e.run_query_target_postgres(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')


def test_import_removes_only_deselected_managed_publication_tables(tmp_path):
    """Persisted selection removes owned members while filtered runs preserve peers."""
    project_dir = tmp_path / 'project'
    shutil.copytree(TEMPLATE_DIR, project_dir)
    e2e = E2EEnv(project_dir)
    config_dir = tmp_path / 'pipelinewise-config'
    config_dir.mkdir()
    command_env = {**os.environ, 'PIPELINEWISE_CONFIG_DIRECTORY': str(config_dir)}
    retained_table = 'retained_records'
    unmanaged_table = 'dba_managed_records'
    slot_name = f'ppw_slot_{TAP_ID}'
    wal2json_slot = _slot_name(e2e.get_conn_env_var('TAP_POSTGRES', 'DB'), TAP_ID)
    import_command = ['pipelinewise', 'import_config', '--dir', str(project_dir)]
    run_command = ['pipelinewise', 'run_tap', '--tap', TAP_ID, '--target', TARGET_ID]
    state_path = config_dir / TARGET_ID / TAP_ID / 'state.json'
    try:
        _drop_slot(e2e, slot_name)
        _drop_slot(e2e, wal2json_slot)
        _drop_publication(e2e)
        e2e.run_query_tap_postgres(
            f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE; CREATE SCHEMA {SOURCE_SCHEMA}'
        )
        for table_name in (TABLE_NAME, retained_table, unmanaged_table):
            e2e.run_query_tap_postgres(
                f'CREATE TABLE {SOURCE_SCHEMA}.{table_name} '
                '(id integer PRIMARY KEY, status text, payload text); '
                f"INSERT INTO {SOURCE_SCHEMA}.{table_name} VALUES (1, 'source', 'payload')"
            )
        e2e.run_query_target_postgres(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')
        _select_logical_table(project_dir, retained_table)
        _run_success(import_command, command_env)
        filtered_sync = [
            'pipelinewise', 'fast_sync', '--tap', TAP_ID, '--target', TARGET_ID,
            '--tables', f'{SOURCE_SCHEMA}.{TABLE_NAME}',
        ]
        _run_success(filtered_sync, command_env)
        expected_managed = {(SOURCE_SCHEMA, TABLE_NAME), (SOURCE_SCHEMA, retained_table)}
        assert _publication_tables(e2e) == expected_managed
        e2e.run_query_tap_postgres(
            f'ALTER PUBLICATION {PUBLICATION_NAME} ADD TABLE {SOURCE_SCHEMA}.{unmanaged_table}'
        )
        _run_success(filtered_sync, command_env)
        assert _publication_tables(e2e) == expected_managed | {(SOURCE_SCHEMA, unmanaged_table)}
        original_lsn = _slot_status(e2e, slot_name)['confirmed_flush_lsn']
        assert STREAM_ID in _read_state(state_path)['bookmarks']
        tap_yaml = project_dir / 'tap_postgres_pgoutput_to_pg.yml'
        tap_yaml.write_text(
            tap_yaml.read_text(encoding='utf-8').replace(
                f'      - table_name: "{TABLE_NAME}"\n        replication_method: "LOG_BASED"\n', ''),
            encoding='utf-8',
        )
        _run_success(import_command, command_env)
        assert _publication_tables(e2e) == {
            (SOURCE_SCHEMA, retained_table), (SOURCE_SCHEMA, unmanaged_table),
        }
        assert _publication_fence_metadata(e2e)['managed_tables'] == [[SOURCE_SCHEMA, retained_table]]
        assert STREAM_ID not in _read_state(state_path)['bookmarks']
        e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'changed while deselected' WHERE id = 1; "
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (2, 'inserted while deselected', 'payload2')"
        )
        _select_logical_table(project_dir, TABLE_NAME)
        _run_success(import_command, command_env)
        assert _slot_status(e2e, slot_name)['confirmed_flush_lsn'] == original_lsn
        assert STREAM_ID not in _read_state(state_path)['bookmarks']
        _run_success(run_command, command_env)
        assert _source_target_rows(e2e)[0] == _source_target_rows(e2e)[1]
        assert _source_target_rows(e2e)[1][0][1] == 'changed while deselected'
        e2e.run_query_tap_postgres(
            'SELECT * FROM pg_create_logical_replication_slot(%s, %s)', (wal2json_slot, 'wal2json'),
        )
        tap_yaml.write_text(
            tap_yaml.read_text(encoding='utf-8').replace('"LOG_BASED"', '"FULL_TABLE"'),
            encoding='utf-8',
        )
        _run_success(import_command, command_env)
        assert _publication_tables(e2e) == {(SOURCE_SCHEMA, unmanaged_table)}
        assert _publication_fence_metadata(e2e)['managed_tables'] == []
        assert _slot_status(e2e, slot_name) is None
        assert _slot_status(e2e, wal2json_slot) is None
        assert not any('lsn' in bookmark for bookmark in _read_state(state_path)['bookmarks'].values())
        e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'changed with no slot' WHERE id = 1"
        )
        tap_yaml.write_text(
            tap_yaml.read_text(encoding='utf-8').replace('"FULL_TABLE"', '"LOG_BASED"'),
            encoding='utf-8',
        )
        _run_success(import_command, command_env)
        assert _slot_status(e2e, slot_name) is None
        _run_success(run_command, command_env)
        assert _slot_status(e2e, slot_name)['plugin'] == 'pgoutput'
        assert _source_target_rows(e2e)[0] == _source_target_rows(e2e)[1]
        assert _source_target_rows(e2e)[1][0][1] == 'changed with no slot'
    finally:
        _drop_slot(e2e, slot_name)
        _drop_slot(e2e, wal2json_slot)
        _drop_publication(e2e)
        e2e.run_query_tap_postgres(f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE')
        e2e.run_query_target_postgres(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')


@pytest.mark.parametrize('fresh_reset', [False, True], ids=['automatic-migration', 'explicit-reset'])
def test_wal2json_slot_is_retired_after_pgoutput_target_checkpoint(tmp_path, fresh_reset):
    """Migrate safely, retire after durability, and checkpoint a logical message."""
    project_dir = tmp_path / 'project'
    shutil.copytree(TEMPLATE_DIR, project_dir)
    e2e = E2EEnv(project_dir)

    config_dir = tmp_path / 'pipelinewise-config'
    config_dir.mkdir()
    command_env = os.environ.copy()
    command_env['PIPELINEWISE_CONFIG_DIRECTORY'] = str(config_dir)
    state_path = config_dir / TARGET_ID / TAP_ID / 'state.json'

    database = e2e.get_conn_env_var('TAP_POSTGRES', 'DB')
    legacy_wal2json_slot = _slot_name(database)
    wal2json_slot = _slot_name(database, TAP_ID)
    pgoutput_slot = f'ppw_slot_{TAP_ID}'
    fast_sync_process = None
    long_transaction = None

    try:
        _drop_slot(e2e, pgoutput_slot)
        _drop_slot(e2e, wal2json_slot)
        _drop_publication(e2e)
        server_version = e2e.run_query_tap_postgres(
            "SELECT pg_catalog.current_setting('server_version_num')::integer"
        )[0][0]
        assert server_version >= 140000
        assert _slot_status(e2e, legacy_wal2json_slot) is None, (
            f'Shared legacy slot {legacy_wal2json_slot} must be migrated '
            'outside this isolated E2E before it can run'
        )
        e2e.run_query_tap_postgres(
            f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE; '
            f'CREATE SCHEMA {SOURCE_SCHEMA}; '
            f'CREATE TABLE {SOURCE_SCHEMA}.{TABLE_NAME} '
            '(id integer PRIMARY KEY, status text NOT NULL, payload text NOT NULL); '
            f'ALTER TABLE {SOURCE_SCHEMA}.{TABLE_NAME} '
            'ALTER COLUMN payload SET STORAGE EXTERNAL; '
            f'INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} '
            "SELECT 1, 'original', string_agg(md5(part::text), '' ORDER BY part) "
            'FROM generate_series(1, 1024) AS generated(part)'
        )
        e2e.run_query_target_postgres(
            f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE'
        )
        e2e.run_query_tap_postgres(
            'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
            (wal2json_slot, 'wal2json'),
        )
        original_slot = _slot_status(e2e, wal2json_slot)
        assert original_slot is not None
        assert original_slot['plugin'] == 'wal2json'

        _run_success(['pipelinewise', 'validate', '--dir', str(project_dir)], command_env)
        _run_success(
            ['pipelinewise', 'import_config', '--dir', str(project_dir)],
            command_env,
        )
        _drop_publication(e2e)
        long_transaction = _connect_source(e2e)
        with long_transaction.cursor() as cursor:
            cursor.execute(
                f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} "
                "SET status = 'committed across publication setup' WHERE id = 1"
            )
        fast_sync_command = [
            'pipelinewise',
            'fast_sync',
            '--tap',
            TAP_ID,
            '--target',
            TARGET_ID,
        ]
        if not fresh_reset:
            fast_sync_command.extend(['--tables', f'{SOURCE_SCHEMA}.{TABLE_NAME}'])
        fast_sync_process = _start(fast_sync_command, command_env)
        _wait_for_publication_fence(
            e2e,
            fast_sync_process,
            fast_sync_command,
        )
        long_transaction.commit()
        long_transaction.close()
        long_transaction = None
        fast_sync_result = _wait(fast_sync_process, fast_sync_command)
        fast_sync_process = None
        assert fast_sync_result.returncode == 0, (
            f'FastSync failed with return code {fast_sync_result.returncode}\n'
            f'stdout:\n{fast_sync_result.stdout}\nstderr:\n{fast_sync_result.stderr}'
        )
        assert _publication_fence_metadata(e2e) == {
            'state': 'ready',
            'original_comment': None,
            'managed_tables': [[SOURCE_SCHEMA, TABLE_NAME]],
        }
        assert _publication_tables(e2e) == {(SOURCE_SCHEMA, TABLE_NAME)}

        old_slot = _slot_status(e2e, wal2json_slot)
        new_slot = _slot_status(e2e, pgoutput_slot)
        assert new_slot is not None
        assert new_slot['plugin'] == 'pgoutput'
        assert new_slot['database'] == database
        assert new_slot['confirmed_flush_lsn'] > original_slot['confirmed_flush_lsn']
        if fresh_reset:
            assert old_slot is None
        else:
            assert old_slot is not None
            assert old_slot['plugin'] == 'wal2json'
            assert old_slot['confirmed_flush_lsn'] == original_slot['confirmed_flush_lsn']
        source_rows, target_rows = _source_target_rows(e2e)
        assert source_rows == target_rows
        assert source_rows[0][1:3] == ('committed across publication setup', 32768)
        original_payload_fingerprint = source_rows[0][2:]

        if not fresh_reset:
            _run_success(
                ['pipelinewise', 'run_tap', '--tap', TAP_ID, '--target', TARGET_ID],
                command_env,
            )

        bridge_state = _read_state(state_path)
        bridge_marker = bridge_state.get(MIGRATION_STATE_KEY)
        assert '_pipelinewise_pgoutput_fresh_start' not in bridge_state
        bridge_lsn = _read_state_lsn(state_path)
        bridged_old_slot = _slot_status(e2e, wal2json_slot)
        bridged_new_slot = _slot_status(e2e, pgoutput_slot)
        assert bridged_new_slot is not None
        assert bridged_new_slot['plugin'] == 'pgoutput'
        if fresh_reset:
            assert bridge_marker is None
            assert bridged_old_slot is None
        else:
            assert bridge_marker['version'] == 2
            assert bridge_marker['phase'] == 'pgoutput_overlap'
            assert bridge_marker['source_slot'] == wal2json_slot
            assert bridge_marker['destination_slot'] == pgoutput_slot
            assert bridge_marker['slot_lsn'] == new_slot['confirmed_flush_lsn']
            assert bridge_marker['bridge_lsn'] > bridge_marker['slot_lsn']
            assert len(bridge_marker['boundary_token']) == 32
            assert bridge_lsn == bridge_marker['bridge_lsn']
            assert bridged_old_slot is None
            assert bridged_new_slot['confirmed_flush_lsn'] == bridge_marker['slot_lsn']
        source_rows, target_rows = _source_target_rows(e2e)
        assert source_rows == target_rows

        target_config_path = config_dir / TARGET_ID / 'config.json'
        original_target_config = target_config_path.read_text(encoding='utf-8')
        broken_target_config = json.loads(original_target_config)
        broken_target_config['password'] = 'deliberately-invalid-e2e-password'
        target_config_path.write_text(json.dumps(broken_target_config), encoding='utf-8')
        try:
            failed_run = _run(
                [
                    'pipelinewise',
                    'run_tap',
                    '--tap',
                    TAP_ID,
                    '--target',
                    TARGET_ID,
                ],
                command_env,
            )
        finally:
            target_config_path.write_text(original_target_config, encoding='utf-8')
        assert failed_run.returncode != 0, (
            'Invalid target credentials should reject the pgoutput boundary\n'
            f'stdout:\n{failed_run.stdout}\nstderr:\n{failed_run.stderr}'
        )
        failed_slot = _slot_status(e2e, pgoutput_slot)
        assert failed_slot is not None
        assert failed_slot['plugin'] == 'pgoutput'
        failed_old_slot = _slot_status(e2e, wal2json_slot)
        assert failed_old_slot is None
        failed_state = _read_state(state_path)
        assert failed_state.get(MIGRATION_STATE_KEY) == bridge_marker
        assert _read_state_lsn(state_path) == bridge_lsn
        assert _source_target_rows(e2e)[1] == target_rows

        _run_success(
            [
                'pipelinewise',
                'run_tap',
                '--tap',
                TAP_ID,
                '--target',
                TARGET_ID,
            ],
            command_env,
        )

        final_slot = _slot_status(e2e, pgoutput_slot)
        assert final_slot is not None
        assert final_slot['plugin'] == 'pgoutput'
        assert not final_slot['active']
        if not fresh_reset:
            assert final_slot['confirmed_flush_lsn'] > bridge_marker['slot_lsn']
        assert _slot_status(e2e, wal2json_slot) is None
        final_state = _read_state(state_path)
        assert MIGRATION_STATE_KEY not in final_state
        idle_retirement_lsn = _read_state_lsn(state_path)
        assert idle_retirement_lsn >= bridge_lsn
        source_rows, target_rows = _source_target_rows(e2e)
        assert target_rows == source_rows

        e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'pgoutput update' WHERE id = 1"
        )

        _run_success(
            [
                'pipelinewise',
                'run_tap',
                '--tap',
                TAP_ID,
                '--target',
                TARGET_ID,
            ],
            command_env,
        )
        record_lsn = _read_state_lsn(state_path)
        assert record_lsn > idle_retirement_lsn
        boundary_slot = _slot_status(e2e, pgoutput_slot)
        assert boundary_slot is not None
        assert boundary_slot['plugin'] == 'pgoutput'
        assert boundary_slot['confirmed_flush_lsn'] >= record_lsn
        assert _slot_status(e2e, wal2json_slot) is None
        source_rows, target_rows = _source_target_rows(e2e)
        assert target_rows == source_rows
        assert target_rows[0][1] == 'pgoutput update'
        assert target_rows[0][2:] == original_payload_fingerprint

        e2e.run_query_tap_postgres(
            f'CREATE TABLE {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME} '
            '(id integer NOT NULL, bucket integer NOT NULL, status text NOT NULL, '
            'PRIMARY KEY (id, bucket)) PARTITION BY RANGE (bucket); '
            f'CREATE TABLE {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME}_p0 '
            f'PARTITION OF {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME} '
            'FOR VALUES FROM (0) TO (10); '
            f'INSERT INTO {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME} '
            "VALUES (1, 1, 'initial')"
        )
        _select_logical_table(project_dir, PARTITIONED_TABLE_NAME)
        _run_success(
            ['pipelinewise', 'import_config', '--dir', str(project_dir)],
            command_env,
        )
        _run_success(
            [
                'pipelinewise',
                'fast_sync',
                '--tap',
                TAP_ID,
                '--target',
                TARGET_ID,
                '--tables',
                f'{SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME}',
            ],
            command_env,
        )
        partition_source_rows, partition_target_rows = _partition_source_target_rows(e2e)
        assert partition_source_rows == partition_target_rows == [(1, 1, 'initial')]

        e2e.run_query_tap_postgres(
            f'CREATE TABLE {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME}_p1 '
            f'(LIKE {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME} INCLUDING ALL); '
            f'ALTER TABLE {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME} '
            f'ATTACH PARTITION {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME}_p1 '
            'FOR VALUES FROM (10) TO (20); '
            f'UPDATE {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME} '
            "SET status = 'updated' WHERE id = 1 AND bucket = 1; "
            f'INSERT INTO {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME} '
            "VALUES (2, 12, 'attached-empty-leaf')"
        )
        _run_success(
            ['pipelinewise', 'run_tap', '--tap', TAP_ID, '--target', TARGET_ID],
            command_env,
        )
        partition_source_rows, partition_target_rows = _partition_source_target_rows(e2e)
        assert partition_source_rows == partition_target_rows == [
            (1, 1, 'updated'),
            (2, 12, 'attached-empty-leaf'),
        ]

        e2e.run_query_tap_postgres(
            f'DELETE FROM {SOURCE_SCHEMA}.{PARTITIONED_TABLE_NAME} '
            'WHERE id = 1 AND bucket = 1'
        )
        _run_success(
            ['pipelinewise', 'run_tap', '--tap', TAP_ID, '--target', TARGET_ID],
            command_env,
        )
        partition_source_rows, partition_target_rows = _partition_source_target_rows(e2e)
        assert partition_source_rows == partition_target_rows == [(2, 12, 'attached-empty-leaf')]
        assert _publication_fence_metadata(e2e) == {
            'state': 'ready',
            'original_comment': None,
            'managed_tables': [[SOURCE_SCHEMA, TABLE_NAME], [SOURCE_SCHEMA, PARTITIONED_TABLE_NAME]],
        }
        assert _publication_tables(e2e) == {
            (SOURCE_SCHEMA, TABLE_NAME),
            (SOURCE_SCHEMA, PARTITIONED_TABLE_NAME),
        }
        if fresh_reset:
            target_config_path.write_text(json.dumps(broken_target_config), encoding='utf-8')
            try:
                failed_reset = _run(fast_sync_command, command_env)
                assert failed_reset.returncode != 0
                pending_state = _read_state(state_path)
                assert pending_state['_pipelinewise_pgoutput_fresh_start'] == {
                    'version': 1, 'wal2json_slot': None, 'destination_slot': pgoutput_slot,
                }
                rejected_resume = _run(
                    ['pipelinewise', 'run_tap', '--tap', TAP_ID, '--target', TARGET_ID], command_env,
                )
                assert rejected_resume.returncode != 0
                assert 'whole-tap fast_sync' in rejected_resume.stderr + rejected_resume.stdout
                assert _read_state(state_path) == pending_state
            finally:
                target_config_path.write_text(original_target_config, encoding='utf-8')
            _run_success(fast_sync_command, command_env)
            assert '_pipelinewise_pgoutput_fresh_start' not in _read_state(state_path)
            _run_success(
                ['pipelinewise', 'run_tap', '--tap', TAP_ID, '--target', TARGET_ID], command_env,
            )
            assert _source_target_rows(e2e)[0] == _source_target_rows(e2e)[1]
            assert _partition_source_target_rows(e2e)[0] == _partition_source_target_rows(e2e)[1]
    finally:
        test_error = sys.exception()
        cleanup_errors = []
        if fast_sync_process is not None and fast_sync_process.poll() is None:
            _stop_process(fast_sync_process)
        if long_transaction is not None:
            try:
                long_transaction.rollback()
            finally:
                long_transaction.close()
        for description, operation in (
            ('drop pgoutput slot', lambda: _drop_slot(e2e, pgoutput_slot)),
            ('drop wal2json slot', lambda: _drop_slot(e2e, wal2json_slot)),
            ('drop publication', lambda: _drop_publication(e2e)),
            (
                'drop source schema',
                lambda: e2e.run_query_tap_postgres(
                    f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE'
                ),
            ),
            (
                'drop target schema',
                lambda: e2e.run_query_target_postgres(
                    f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE'
                ),
            ),
        ):
            try:
                operation()
            except Exception as exc:  # Cleanup continues so every resource is attempted.
                cleanup_errors.append((description, exc))

        if cleanup_errors:
            details = '; '.join(
                f'{description}: {error!r}'
                for description, error in cleanup_errors
            )
            cleanup_failure = AssertionError(f'Test cleanup failed: {details}')
            if test_error is None:
                raise cleanup_failure from cleanup_errors[0][1]
            test_error.add_note(str(cleanup_failure))
