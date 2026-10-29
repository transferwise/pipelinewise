"""Deterministic PostgreSQL LOG_BASED recovery with an interrupted stream buffer."""

import json
import os
import re
import shutil
import subprocess
import sys

from pathlib import Path

import psycopg2

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


TAP_ID = 'postgres_stream_buffer_recovery'
TARGET_ID = 'postgres_stream_buffer_recovery_dwh'
SOURCE_SCHEMA = 'ppw_e2e_stream_buffer_source'
TARGET_SCHEMA = 'ppw_e2e_stream_buffer_target'
TABLE_NAME = 'buffered_records'
STREAM_ID = f'{SOURCE_SCHEMA}-{TABLE_NAME}'
RECORD_COUNT = 12050
PAYLOAD_REPETITIONS = 4
MIN_BUFFERED_BYTES = 1024 * 1024
TEMPLATE_DIR = Path(__file__).parent / 'postgres_stream_buffer_test_project'
LATEST_WAL_MESSAGE = re.compile(
    r'La(?:s)?test wal message received was ([0-9A-F]+/[0-9A-F]+)',
    re.IGNORECASE,
)


def _connect_postgres(e2e, connector):
    """Open a PostgreSQL connection using the E2E connector environment."""
    return psycopg2.connect(
        host=e2e.get_conn_env_var(connector, 'HOST'),
        port=e2e.get_conn_env_var(connector, 'PORT'),
        user=e2e.get_conn_env_var(connector, 'USER'),
        password=e2e.get_conn_env_var(connector, 'PASSWORD'),
        database=e2e.get_conn_env_var(connector, 'DB'),
    )


def _lsn_to_int(lsn):
    """Convert either a PostgreSQL textual LSN or an integer bookmark."""
    if isinstance(lsn, int):
        return lsn
    upper, lower = str(lsn).split('/')
    return (int(upper, 16) << 32) + int(lower, 16)


def _slot_name(e2e):
    """Return the tap-specific PostgreSQL replication slot name."""
    database = e2e.get_conn_env_var('TAP_POSTGRES', 'DB')
    return re.sub('[^a-z0-9_]', '_', f'pipelinewise_{database}_{TAP_ID}'.lower())


def _slot_status(e2e, slot_name):
    """Return the slot boundary and sender high-water mark, if the slot exists."""
    rows = e2e.run_query_tap_postgres(
        """
        SELECT slot.active,
               slot.active_pid,
               (slot.confirmed_flush_lsn - '0/0'::pg_lsn)::bigint,
               CASE WHEN sender.sent_lsn IS NULL THEN NULL
                    ELSE (sender.sent_lsn - '0/0'::pg_lsn)::bigint
               END
          FROM pg_replication_slots slot
          LEFT JOIN pg_stat_replication sender ON sender.pid = slot.active_pid
         WHERE slot.slot_name = %s
        """,
        (slot_name,),
    )
    if not rows:
        return None
    active, active_pid, confirmed_flush_lsn, sent_lsn = rows[0]
    return {
        'active': active,
        'active_pid': active_pid,
        'confirmed_flush_lsn': int(confirmed_flush_lsn),
        'sent_lsn': int(sent_lsn) if sent_lsn is not None else None,
    }


def _current_source_lsn(e2e):
    """Return PostgreSQL's current WAL insert position as an integer."""
    return int(e2e.run_query_tap_postgres(
        "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint"
    )[0][0])


def _target_table_lock_waiters(e2e):
    """Return blocked lock requests for the dedicated target table."""
    return int(e2e.run_query_target_postgres(
        f"""
        SELECT COUNT(*)
          FROM pg_locks waiting
          JOIN pg_class relation ON relation.oid = waiting.relation
          JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
         WHERE NOT waiting.granted
           AND namespace.nspname = '{TARGET_SCHEMA}'
           AND relation.relname = '{TABLE_NAME}'
        """
    )[0][0])


def _drop_slot(e2e, slot_name):
    """Drop the dedicated inactive replication slot if it exists."""
    status = _slot_status(e2e, slot_name)
    if status is None:
        return
    assert not status['active'], (
        f'Refusing to drop active test slot {slot_name} owned by pid '
        f'{status["active_pid"]}'
    )
    e2e.run_query_tap_postgres(
        'SELECT pg_drop_replication_slot(%s)', (slot_name,)
    )


def _latest_consumed_lsn(log_path, minimum_reports=1):
    """Read WAL only after enough complete feedback polling cycles."""
    matches = LATEST_WAL_MESSAGE.findall(
        log_path.read_text(encoding='utf-8')
    )
    if len(matches) < minimum_reports:
        return None
    return max(map(_lsn_to_int, matches)) if matches else None


def _insert_source_records(e2e):
    """Create distinct commits and an observable in-buffer backlog."""
    connection = _connect_postgres(e2e, 'TAP_POSTGRES')
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            for record_id in range(1, RECORD_COUNT + 1):
                cursor.execute(
                    f'INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} (id, payload) '
                    'VALUES (%s, repeat(md5(%s::text), %s))',
                    (record_id, record_id, PAYLOAD_REPETITIONS),
                )
            cursor.execute(
                "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint"
            )
            return int(cursor.fetchone()[0])
    finally:
        connection.close()


def _source_target_rows(e2e):
    """Return stable primary keys and payload checksums from both databases."""
    select_rows = (
        f'SELECT id, md5(payload) FROM {{schema}}.{TABLE_NAME} ORDER BY id'
    )
    source_rows = e2e.run_query_tap_postgres(
        select_rows.format(schema=SOURCE_SCHEMA)
    )
    target_rows = e2e.run_query_target_postgres(
        select_rows.format(schema=TARGET_SCHEMA)
    )
    return source_rows, target_rows


def test_postgres_buffer_recovers_after_stop(tmp_path):
    """Replay WAL that was consumed into mbuffer but never target-acknowledged."""
    project_dir = tmp_path / 'project'
    shutil.copytree(TEMPLATE_DIR, project_dir)
    e2e = E2EEnv(project_dir)

    config_dir = tmp_path / 'pipelinewise-config'
    config_dir.mkdir()
    command_env = os.environ.copy()
    command_env['PIPELINEWISE_CONFIG_DIRECTORY'] = str(config_dir)

    state_path = config_dir / TARGET_ID / TAP_ID / 'state.json'
    pid_path = config_dir / TARGET_ID / TAP_ID / 'pipelinewise.pid'
    log_dir = config_dir / TARGET_ID / TAP_ID / 'log'
    slot_name = _slot_name(e2e)
    supervisor = None
    lock_connection = None
    captured_processes = []

    try:
        _drop_slot(e2e, slot_name)
        e2e.run_query_tap_postgres(
            f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE; '
            f'CREATE SCHEMA {SOURCE_SCHEMA}; '
            f'CREATE TABLE {SOURCE_SCHEMA}.{TABLE_NAME} '
            '(id integer PRIMARY KEY, payload text NOT NULL)'
        )
        e2e.run_query_target_postgres(
            f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE'
        )

        run_success(['pipelinewise', 'validate', '--dir', str(project_dir)], command_env)
        run_success(
            ['pipelinewise', 'import_config', '--dir', str(project_dir)],
            command_env,
        )
        run_success(
            [
                'pipelinewise',
                'fast_sync',
                '--tap',
                TAP_ID,
                '--target',
                TARGET_ID,
            ],
            command_env,
        )

        baseline_state = read_state(state_path)
        baseline_lsn = _lsn_to_int(
            baseline_state['bookmarks'][STREAM_ID]['lsn']
        )
        initial_slot = _slot_status(e2e, slot_name)
        assert initial_slot is not None
        assert initial_slot['confirmed_flush_lsn'] <= baseline_lsn
        assert e2e.run_query_target_postgres(
            f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TABLE_NAME}'
        )[0][0] == 0

        source_high_water_lsn = _insert_source_records(e2e)
        assert source_high_water_lsn > baseline_lsn

        lock_connection = _connect_postgres(e2e, 'TARGET_POSTGRES')
        with lock_connection.cursor() as lock_cursor:
            lock_cursor.execute(
                f'LOCK TABLE {TARGET_SCHEMA}.{TABLE_NAME} IN ACCESS EXCLUSIVE MODE'
            )

        supervisor = subprocess.Popen(
            [
                'pipelinewise',
                'run_tap',
                '--tap',
                TAP_ID,
                '--target',
                TARGET_ID,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=command_env,
            start_new_session=True,
        )

        wait_for('the production pidfile', pid_path.is_file)
        assert int(pid_path.read_text(encoding='utf-8')) == supervisor.pid

        tap_process = wait_for(
            'tap-postgres child process',
            lambda: find_descendant(supervisor, 'tap-postgres'),
        )
        mbuffer_process = wait_for(
            'mbuffer child process',
            lambda: find_descendant(supervisor, 'mbuffer'),
        )
        target_process = wait_for(
            'target-postgres child process',
            lambda: find_descendant(supervisor, 'target-postgres'),
        )
        captured_processes = [tap_process, mbuffer_process, target_process]
        waiting_lock_count = wait_for(
            'target-postgres to wait on the controlled target table lock',
            lambda: _target_table_lock_waiters(e2e),
        )

        singer_log = wait_for(
            'the running Singer log', lambda: find_running_log(log_dir)
        )
        consumed_lsn = wait_for(
            'tap-postgres to report consumed WAL ahead of target state',
            lambda: (
                lsn
                if (
                    lsn := _latest_consumed_lsn(
                        singer_log, minimum_reports=2)
                ) is not None
                and lsn > baseline_lsn
                else None
            ),
            timeout=40,
        )
        backlog = wait_for(
            'at least 1 MB retained in mbuffer',
            lambda: buffered_bytes(mbuffer_process, MIN_BUFFERED_BYTES),
        )

        state_before_interruption = read_state(state_path)
        slot_before_interruption = _slot_status(e2e, slot_name)
        source_lsn_before_interruption = _current_source_lsn(e2e)
        assert baseline_lsn < consumed_lsn <= source_lsn_before_interruption
        assert backlog >= MIN_BUFFERED_BYTES
        assert waiting_lock_count >= 1
        assert state_before_interruption == baseline_state
        assert slot_before_interruption is not None
        assert slot_before_interruption['active']
        assert slot_before_interruption['sent_lsn'] is not None
        assert slot_before_interruption['sent_lsn'] >= consumed_lsn
        assert slot_before_interruption['confirmed_flush_lsn'] <= baseline_lsn

        interrupted_stdout, interrupted_stderr = stop_process(supervisor)
        assert supervisor.returncode != 0, (
            'Interrupted pipeline exited successfully\n'
            f'stdout:\n{interrupted_stdout}\nstderr:\n{interrupted_stderr}'
        )
        wait_for(
            'all Singer pipeline children to stop',
            lambda: processes_stopped(captured_processes),
        )
        supervisor = None

        terminated_log = Path(f'{str(singer_log).removesuffix(".running")}.terminated')
        assert terminated_log.is_file()
        assert read_state(state_path) == baseline_state
        slot_after_interruption = wait_for(
            'the replication slot to become inactive',
            lambda: (
                status
                if (status := _slot_status(e2e, slot_name)) and not status['active']
                else None
            ),
        )
        assert slot_after_interruption['confirmed_flush_lsn'] <= baseline_lsn

        lock_connection.rollback()
        lock_connection.close()
        lock_connection = None
        assert e2e.run_query_target_postgres(
            f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TABLE_NAME}'
        )[0][0] == 0

        restart_stdout = run_success(
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
        assert len(
            re.findall(
                r'Writing output into .+\.singer\.log$',
                restart_stdout,
                re.MULTILINE,
            )
        ) == 1
        assert not re.search(
            r'Writing output into .+\.fastsync\.log$',
            restart_stdout,
            re.MULTILINE,
        )

        source_rows, target_rows = _source_target_rows(e2e)
        assert [row[0] for row in source_rows] == list(range(1, RECORD_COUNT + 1))
        assert target_rows == source_rows
        final_state = read_state(state_path)
        final_state_lsn = _lsn_to_int(final_state['bookmarks'][STREAM_ID]['lsn'])
        final_slot = _slot_status(e2e, slot_name)
        final_source_lsn = _current_source_lsn(e2e)
        assert final_slot is not None
        assert not final_slot['active']
        assert baseline_lsn < final_slot['confirmed_flush_lsn'] <= final_state_lsn
        assert final_state_lsn <= final_source_lsn
        print(json.dumps({
            'baseline_state_lsn': baseline_lsn,
            'buffered_bytes': backlog,
            'consumed_lsn_before_interruption': consumed_lsn,
            'final_source_lsn': final_source_lsn,
            'final_state_lsn': final_state_lsn,
            'final_target_rows': len(target_rows),
            'slot_confirmed_flush_lsn_after_restart': (
                final_slot['confirmed_flush_lsn']
            ),
            'slot_confirmed_flush_lsn_before_interruption': (
                slot_before_interruption['confirmed_flush_lsn']
            ),
            'source_high_water_lsn': source_high_water_lsn,
            'target_table_lock_waiters': waiting_lock_count,
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

        def cleanup_slot():
            slot_status = _slot_status(e2e, slot_name)
            if slot_status and slot_status['active']:
                wait_for(
                    'the test replication slot to become inactive during cleanup',
                    lambda: (
                        status
                        if (status := _slot_status(e2e, slot_name))
                        and not status['active']
                        else None
                    ),
                )
            _drop_slot(e2e, slot_name)

        cleanups.extend([
            ('drop replication slot', cleanup_slot),
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
        ])
        run_cleanups(sys.exception(), cleanups)
