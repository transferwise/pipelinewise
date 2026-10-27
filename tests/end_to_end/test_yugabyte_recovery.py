"""YugabyteDB LOG_BASED repair, concurrent-write and interruption recovery against real databases.

The scenarios share one tap so they share one replication slot: YugabyteDB keeps a slot "active" for minutes after
its consumer disconnects, so dropping and recreating it per test would dominate the run time.
"""

import json
import os
import shutil
import subprocess
import sys
import threading

from argparse import Namespace
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

import psycopg2
import pytest

from pipelinewise.fastsync import yugabyte_to_postgres
from pipelinewise.fastsync.commons.tap_yugabyte import FastSyncTapYugabyte

from .helpers.env import E2EEnv
from .helpers.pipeline_process import (
    buffered_bytes,
    find_descendant,
    find_running_log,
    processes_stopped,
    read_state,
    run_cleanups,
    run_success,
    stop_process,
    terminate_surviving_processes,
    wait_for,
)
from .helpers.yugabyte_recovery import (
    SLOT_FEEDBACK_LSN_OFFSET,
    KeyAllocator,
    acknowledge_source_changes,
    assert_export_predates,
    assert_slot_behind_state,
    commit_each,
    drop_slot,
    latest_committed_lsn,
    mixed_changes,
    slot_name,
    slot_status,
)


TAP_ID = 'yugabyte_recovery'
TARGET_ID = 'yugabyte_recovery_dwh'
SOURCE_SCHEMA = 'ppw_e2e_yb_recovery_source'
TARGET_SCHEMA = 'ppw_e2e_yb_recovery_target'
REPAIRED_TABLE = 'repaired_records'
BUFFERED_TABLE = 'buffered_records'
TABLES = (REPAIRED_TABLE, BUFFERED_TABLE)
INITIAL_ROWS = 20000
BUFFERED_RECORD_COUNT = 12050
PAYLOAD_REPETITIONS = 4
MIN_BUFFERED_BYTES = 1024 * 1024
TEMPLATE_DIR = Path(__file__).parent / 'yugabyte_recovery_test_project'


@dataclass
class RecoveryProject:
    """One imported tap whose replication slot all scenarios share."""

    e2e: E2EEnv
    project_dir: Path
    config_dir: Path
    temp_dir: Path
    command_env: dict
    slot_name: str
    keys: KeyAllocator = field(default_factory=KeyAllocator)

    @property
    def tap_dir(self):
        return self.config_dir / TARGET_ID / TAP_ID

    @property
    def state_path(self):
        return self.tap_dir / 'state.json'

    def pipelinewise(self, *arguments, timeout=300):
        return run_success(['pipelinewise', *arguments], self.command_env, timeout=timeout)

    def run_tap(self):
        return self.pipelinewise('run_tap', '--tap', TAP_ID, '--target', TARGET_ID)

    def fast_sync(self, *tables):
        selection = ('--tables', ','.join(f'{SOURCE_SCHEMA}.{table}' for table in tables)) if tables else ()
        return self.pipelinewise('fast_sync', '--tap', TAP_ID, '--target', TARGET_ID, *selection)

    def tap_config(self):
        return json.loads((self.tap_dir / 'config.json').read_text(encoding='utf-8'))

    def fastsync_args(self, table):
        """Build the arguments the CLI passes to the YugabyteDB-to-PostgreSQL FullSync worker."""
        target = json.loads((self.config_dir / TARGET_ID / 'config.json').read_text(encoding='utf-8'))
        target.update(json.loads((self.tap_dir / 'inheritable_config.json').read_text(encoding='utf-8')))
        return Namespace(
            tap=self.tap_config(),
            properties=json.loads((self.tap_dir / 'properties.json').read_text(encoding='utf-8')),
            target=target,
            transform={},
            state=str(self.state_path),
            temp_dir=str(self.temp_dir),
            tables=[f'{SOURCE_SCHEMA}.{table}'],
        )


def _connect_yugabyte(e2e):
    connection = psycopg2.connect(
        host=e2e.get_conn_env_var('TAP_YUGABYTE', 'HOST'),
        port=e2e.get_conn_env_var('TAP_YUGABYTE', 'PORT'),
        user=e2e.get_conn_env_var('TAP_YUGABYTE', 'USER'),
        password=e2e.get_conn_env_var('TAP_YUGABYTE', 'PASSWORD'),
        database=e2e.get_conn_env_var('TAP_YUGABYTE', 'DB'),
    )
    connection.autocommit = True
    return connection


def _connect_target(e2e):
    return psycopg2.connect(
        host=e2e.get_conn_env_var('TARGET_POSTGRES', 'HOST'),
        port=e2e.get_conn_env_var('TARGET_POSTGRES', 'PORT'),
        user=e2e.get_conn_env_var('TARGET_POSTGRES', 'USER'),
        password=e2e.get_conn_env_var('TARGET_POSTGRES', 'PASSWORD'),
        database=e2e.get_conn_env_var('TARGET_POSTGRES', 'DB'),
    )


def _bookmark_lsns(state):
    return {table: state['bookmarks'][f'{SOURCE_SCHEMA}-{table}']['lsn'] for table in TABLES}


def _assert_slot_behind_target_state(project):
    state_lsns = _bookmark_lsns(read_state(project.state_path)).values()
    return assert_slot_behind_state(project.e2e, project.slot_name, state_lsns)


def _rows(e2e, table):
    """Return complete rows from both databases, plus target rows still carrying a delete marker."""
    select_rows = f'SELECT id, payload, version FROM {{schema}}.{table} ORDER BY id'
    source_rows = e2e.run_query_tap_yugabyte(select_rows.format(schema=SOURCE_SCHEMA))
    target_rows = e2e.run_query_target_postgres(select_rows.format(schema=TARGET_SCHEMA))
    marked_rows = e2e.run_query_target_postgres(
        f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.{table} WHERE _sdc_deleted_at IS NOT NULL'
    )[0][0]
    return source_rows, target_rows, marked_rows


def _assert_target_matches_source(e2e, table):
    source_rows, target_rows, marked_rows = _rows(e2e, table)
    assert source_rows, f'{table} has no source rows to compare'
    missing = sorted(set(source_rows) - set(target_rows))[:5]
    unexpected = sorted(set(target_rows) - set(source_rows))[:5]
    assert target_rows == source_rows, (
        f'{table} differs: {len(source_rows)} source rows, {len(target_rows)} target rows; '
        f'missing {missing}, unexpected {unexpected}'
    )
    assert marked_rows == 0
    return source_rows


def _seed_mixed_changes(project, table):
    """Commit and acknowledge the rows the changes act on, so a slot-pinned export holds their old shape."""
    changes = mixed_changes(f'{SOURCE_SCHEMA}.{table}', project.keys)
    project.e2e.run_query_tap_yugabyte(*changes.seed)
    acknowledge_source_changes(project.e2e, f'{SOURCE_SCHEMA}.{REPAIRED_TABLE}', project.run_tap)
    return changes


def _assert_export_predates(project, table, changes):
    exported_keys = {row[0] for row in _rows(project.e2e, table)[1]}
    assert_export_predates(exported_keys, changes)
    assert -1 not in exported_keys


def _damage_target(project, table):
    """Leave a stale row and remove a live one so only a real repair can restore the target."""
    project.e2e.run_query_target_postgres(
        f'INSERT INTO {TARGET_SCHEMA}.{table} (id, payload, version) VALUES (-1, \'stale target row\', 0); '
        f'DELETE FROM {TARGET_SCHEMA}.{table} WHERE id = (SELECT MIN(id) FROM {TARGET_SCHEMA}.{table} WHERE id > 0)'
    )


class _SourceWriter(threading.Thread):
    """Commit a steady mix of inserts, updates, deletes and key changes until stopped."""

    def __init__(self, e2e, table, keys):
        super().__init__(daemon=True)
        self.e2e = e2e
        self.table = table
        self.keys = iter(keys)
        self.stop_event = threading.Event()
        self.commits = 0
        self.error = None

    def run(self):
        try:
            connection = _connect_yugabyte(self.e2e)
            with connection, connection.cursor() as cursor:
                while not self.stop_event.is_set():
                    key, moved_to = next(self.keys), next(self.keys)
                    existing = (self.commits % (INITIAL_ROWS - 10)) + 1
                    for statement, params in (
                        (f'INSERT INTO {SOURCE_SCHEMA}.{self.table} (id, payload, version) VALUES (%s, %s, 0)',
                         (key, f'concurrent {key}')),
                        (f'UPDATE {SOURCE_SCHEMA}.{self.table} SET version = version + 1 WHERE id = %s',
                         (existing,)),
                        (f'UPDATE {SOURCE_SCHEMA}.{self.table} SET id = %s WHERE id = %s', (moved_to, key)),
                        (f'DELETE FROM {SOURCE_SCHEMA}.{self.table} WHERE id = %s', (existing + 1,)),
                    ):
                        cursor.execute(statement, params)
                        self.commits += 1
            connection.close()
        except Exception as exc:  # Surfaced by the test after join so a writer failure is never silent.
            self.error = exc

    def stop(self):
        self.stop_event.set()
        self.join(timeout=60)
        assert not self.is_alive(), 'source writer did not stop'
        if self.error is not None:
            raise self.error


@pytest.fixture(scope='module')
def project(tmp_path_factory):
    """Import an isolated tap, then drop its slot and schemas once every scenario has run."""
    root = tmp_path_factory.mktemp('yugabyte_recovery')
    project_dir = root / 'project'
    shutil.copytree(TEMPLATE_DIR, project_dir)
    e2e = E2EEnv(project_dir)
    config_dir = root / 'pipelinewise-config'
    config_dir.mkdir()
    temp_dir = root / 'exports'
    temp_dir.mkdir()
    command_env = os.environ.copy()
    command_env['PIPELINEWISE_CONFIG_DIRECTORY'] = str(config_dir)
    recovery = RecoveryProject(e2e, project_dir, config_dir, temp_dir, command_env, slot_name(e2e, TAP_ID))

    drop_slot(e2e, TAP_ID)
    e2e.run_query_tap_yugabyte(f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE')
    e2e.run_query_tap_yugabyte(f'CREATE SCHEMA {SOURCE_SCHEMA}')
    for table in TABLES:
        e2e.run_query_tap_yugabyte(
            f'CREATE TABLE {SOURCE_SCHEMA}.{table} '
            '(id integer PRIMARY KEY, payload text NOT NULL, version integer NOT NULL)'
        )
        # target-postgres overwrites columns a CHANGE-identity update omits; identity is fixed when the slot is made.
        e2e.run_query_tap_yugabyte(f'ALTER TABLE {SOURCE_SCHEMA}.{table} REPLICA IDENTITY DEFAULT')
    e2e.run_query_tap_yugabyte(
        f'INSERT INTO {SOURCE_SCHEMA}.{REPAIRED_TABLE} (id, payload, version) '
        f'SELECT k, repeat(md5(k::text), {PAYLOAD_REPETITIONS}), 0 FROM generate_series(1, {INITIAL_ROWS}) AS k'
    )
    e2e.run_query_target_postgres(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')
    recovery.pipelinewise('validate', '--dir', str(project_dir))
    recovery.pipelinewise('import_config', '--dir', str(project_dir))
    try:
        yield recovery
    finally:
        run_cleanups(sys.exception(), [
            ('drop replication slot', lambda: drop_slot(e2e, TAP_ID)),
            ('drop source schema',
             lambda: e2e.run_query_tap_yugabyte(f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE')),
            ('drop target schema',
             lambda: e2e.run_query_target_postgres(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')),
        ])


def test_initial_fullsync_replays_concurrent_writes(project):
    """Writes committed while the slot is created and the snapshot exported reach the target exactly once."""
    writer = _SourceWriter(project.e2e, REPAIRED_TABLE, project.keys.take(10 ** 6))
    writer.start()
    try:
        wait_for('the source writer to commit', lambda: writer.commits >= 20)
        project.fast_sync()
        commits_during_fullsync = writer.commits
    finally:
        writer.stop()

    slot = slot_status(project.e2e, project.slot_name)
    bookmarks = _bookmark_lsns(read_state(project.state_path))
    assert slot is not None
    assert set(bookmarks.values()) == {slot['restart_commit_ht']}
    source_rows, target_rows, _ = _rows(project.e2e, REPAIRED_TABLE)
    assert target_rows != source_rows, 'the writer committed nothing after the snapshot boundary'

    project.run_tap()

    _assert_target_matches_source(project.e2e, REPAIRED_TABLE)
    _assert_slot_behind_target_state(project)
    assert writer.commits > commits_during_fullsync > 20


def test_cdc_removes_the_old_row_when_a_primary_key_changes(project):
    """YugabyteDB streams a key change as a delete of the old key plus an insert of the new one."""
    changes = _seed_mixed_changes(project, REPAIRED_TABLE)
    commit_each(project.e2e, changes.statements)

    project.run_tap()

    _assert_target_matches_source(project.e2e, REPAIRED_TABLE)
    _assert_slot_behind_target_state(project)


def test_filtered_resync_repairs_a_damaged_target(project):
    """A historical export at the slot's restart point plus replay converges to current source data."""
    changes = _seed_mixed_changes(project, REPAIRED_TABLE)
    acknowledged_slot = _assert_slot_behind_target_state(project)
    commit_each(project.e2e, changes.statements)
    _damage_target(project, REPAIRED_TABLE)

    project.fast_sync(REPAIRED_TABLE)

    slot = slot_status(project.e2e, project.slot_name)
    assert _bookmark_lsns(read_state(project.state_path))[REPAIRED_TABLE] == slot['restart_commit_ht']
    assert slot['confirmed_flush_lsn'] == acknowledged_slot['confirmed_flush_lsn']
    _assert_export_predates(project, REPAIRED_TABLE, changes)

    project.run_tap()

    _assert_target_matches_source(project.e2e, REPAIRED_TABLE)
    _assert_slot_behind_target_state(project)


def test_writes_after_the_resync_boundary_are_replayed_not_exported(project):
    """Commits between fixing the slot boundary and exporting must come from replay alone."""
    changes = _seed_mixed_changes(project, REPAIRED_TABLE)
    real_copy_table = FastSyncTapYugabyte.copy_table

    def copy_after_concurrent_commits(source, *args, **kwargs):
        commit_each(project.e2e, changes.statements)
        return real_copy_table(source, *args, **kwargs)

    with patch.object(FastSyncTapYugabyte, 'copy_table', copy_after_concurrent_commits):
        result = yugabyte_to_postgres.sync_table(
            f'{SOURCE_SCHEMA}.{REPAIRED_TABLE}', project.fastsync_args(REPAIRED_TABLE)
        )
    assert result is True
    _assert_export_predates(project, REPAIRED_TABLE, changes)

    project.run_tap()

    _assert_target_matches_source(project.e2e, REPAIRED_TABLE)
    _assert_slot_behind_target_state(project)


def test_interrupted_filtered_resync_keeps_the_previous_bookmark(project):
    """A FullSync that dies after swapping the table but before saving state resumes from the old bookmark."""
    changes = _seed_mixed_changes(project, REPAIRED_TABLE)
    state_before = read_state(project.state_path)
    commit_each(project.e2e, changes.statements)
    _damage_target(project, REPAIRED_TABLE)

    interruption = RuntimeError('FullSync stopped before saving state')
    with patch.object(yugabyte_to_postgres.utils, 'save_state_file', side_effect=interruption):
        result = yugabyte_to_postgres.sync_table(
            f'{SOURCE_SCHEMA}.{REPAIRED_TABLE}', project.fastsync_args(REPAIRED_TABLE)
        )
    assert result == f'{SOURCE_SCHEMA}.{REPAIRED_TABLE}: {interruption}'
    assert read_state(project.state_path) == state_before
    _assert_export_predates(project, REPAIRED_TABLE, changes)

    project.run_tap()

    _assert_target_matches_source(project.e2e, REPAIRED_TABLE)
    _assert_slot_behind_target_state(project)


def test_buffered_changes_survive_interruption_and_restart(project):
    """Changes consumed into mbuffer but never written by the target replay after the pipeline is killed."""
    project.run_tap()
    baseline_state = read_state(project.state_path)
    baseline_lsn = min(_bookmark_lsns(baseline_state).values())
    log_dir = project.tap_dir / 'log'
    supervisor = None
    lock_connection = None
    captured_processes = []

    try:
        keys = project.keys.take(BUFFERED_RECORD_COUNT)
        connection = _connect_yugabyte(project.e2e)
        with connection, connection.cursor() as cursor:
            for key in keys:
                cursor.execute(
                    f'INSERT INTO {SOURCE_SCHEMA}.{BUFFERED_TABLE} (id, payload, version) '
                    'VALUES (%s, repeat(md5(%s::text), %s), 0)',
                    (key, key, PAYLOAD_REPETITIONS),
                )
        connection.close()

        lock_connection = _connect_target(project.e2e)
        with lock_connection.cursor() as lock_cursor:
            lock_cursor.execute(f'LOCK TABLE {TARGET_SCHEMA}.{BUFFERED_TABLE} IN ACCESS EXCLUSIVE MODE')

        supervisor = subprocess.Popen(
            ['pipelinewise', 'run_tap', '--tap', TAP_ID, '--target', TARGET_ID],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=project.command_env,
            start_new_session=True,
        )
        tap_process = wait_for('tap-yugabyte child process', lambda: find_descendant(supervisor, 'tap-yugabyte'))
        mbuffer_process = wait_for('mbuffer child process', lambda: find_descendant(supervisor, 'mbuffer'))
        target_process = wait_for(
            'target-postgres child process', lambda: find_descendant(supervisor, 'target-postgres')
        )
        captured_processes = [tap_process, mbuffer_process, target_process]
        singer_log = wait_for('the running Singer log', lambda: find_running_log(log_dir))
        consumed_lsn = wait_for(
            'tap-yugabyte to report consumed changes ahead of target state',
            lambda: lsn if (lsn := latest_committed_lsn(singer_log)) > baseline_lsn else None,
            timeout=60,
        )
        backlog = wait_for(
            'at least 1 MB retained in mbuffer',
            lambda: buffered_bytes(mbuffer_process, MIN_BUFFERED_BYTES),
            timeout=60,
        )

        slot_before_interruption = slot_status(project.e2e, project.slot_name)
        assert read_state(project.state_path) == baseline_state
        assert slot_before_interruption['confirmed_flush_lsn'] <= baseline_lsn - SLOT_FEEDBACK_LSN_OFFSET

        interrupted_stdout, interrupted_stderr = stop_process(supervisor)
        assert supervisor.returncode != 0, (
            'Interrupted pipeline exited successfully\n'
            f'stdout:\n{interrupted_stdout}\nstderr:\n{interrupted_stderr}'
        )
        wait_for('all Singer pipeline children to stop', lambda: processes_stopped(captured_processes))
        supervisor = None

        assert read_state(project.state_path) == baseline_state
        slot_after_interruption = slot_status(project.e2e, project.slot_name)
        assert slot_after_interruption['confirmed_flush_lsn'] <= baseline_lsn - SLOT_FEEDBACK_LSN_OFFSET
        lock_connection.rollback()
        lock_connection.close()
        lock_connection = None
        assert project.e2e.run_query_target_postgres(
            f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.{BUFFERED_TABLE}'
        )[0][0] == 0

        project.run_tap()

        target_rows = _assert_target_matches_source(project.e2e, BUFFERED_TABLE)
        assert [row[0] for row in target_rows] == list(keys)
        _assert_slot_behind_target_state(project)
        # The tap acknowledges a target state in the run after it was written.
        project.run_tap()
        final_slot = _assert_slot_behind_target_state(project)
        assert final_slot['confirmed_flush_lsn'] > slot_after_interruption['confirmed_flush_lsn']
        print(json.dumps({
            'baseline_state_lsn': baseline_lsn,
            'buffered_bytes': backlog,
            'consumed_lsn_before_interruption': consumed_lsn,
            'final_slot_confirmed_flush_lsn': final_slot['confirmed_flush_lsn'],
            'final_state_lsns': _bookmark_lsns(read_state(project.state_path)),
        }, sort_keys=True))
    finally:
        cleanups = []
        if supervisor is not None:
            cleanups.append(('stop PipelineWise supervisor', lambda: stop_process(supervisor)))
        cleanups.append((
            'terminate captured connector processes',
            lambda: terminate_surviving_processes(captured_processes),
        ))
        if lock_connection is not None:
            cleanups.append(('rollback target lock', lock_connection.rollback))
            cleanups.append(('close target lock connection', lock_connection.close))
        run_cleanups(sys.exception(), cleanups)
