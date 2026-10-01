"""PostgreSQL pgoutput boundary and wal2json migration E2E coverage."""

import base64
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time

from pathlib import Path

import psycopg2

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
PUBLICATION_NAME = f'pw_pub_{TAP_ID}'
PUBLICATION_FENCE_COMMENT_PREFIX = 'pipelinewise-publication-fence-v1:'


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
    return re.sub('[^a-z0-9_]', '_', '_'.join(('pipelinewise', *parts)).lower())


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


def _select_partition_root(project_dir):
    """Add the partition root after wal2json migration has completed."""
    tap_yaml = project_dir / 'tap_postgres_pgoutput_to_pg.yml'
    with tap_yaml.open('a', encoding='utf-8') as config_file:
        config_file.write(
            f'      - table_name: "{PARTITIONED_TABLE_NAME}"\n'
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


def test_wal2json_slot_is_retired_after_pgoutput_target_checkpoint(tmp_path):
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
    pgoutput_slot = f'pipelinewise_{TAP_ID}'
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
        }
        assert _publication_tables(e2e) == {(SOURCE_SCHEMA, TABLE_NAME)}

        copied_old_slot = _slot_status(e2e, wal2json_slot)
        copied_new_slot = _slot_status(e2e, pgoutput_slot)
        assert copied_old_slot is not None
        assert copied_new_slot is not None
        assert copied_old_slot['plugin'] == 'wal2json'
        assert copied_new_slot['plugin'] == 'pgoutput'
        assert copied_new_slot['database'] == database
        assert copied_new_slot['confirmed_flush_lsn'] == copied_old_slot['confirmed_flush_lsn']
        source_rows, target_rows = _source_target_rows(e2e)
        assert source_rows == target_rows
        assert source_rows[0][1:3] == ('committed across publication setup', 32768)
        original_payload_fingerprint = source_rows[0][2:]

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

        bridge_state = _read_state(state_path)
        bridge_marker = bridge_state[MIGRATION_STATE_KEY]
        assert bridge_marker['phase'] == 'pgoutput'
        assert bridge_marker['source_slot'] == wal2json_slot
        assert bridge_marker['destination_slot'] == pgoutput_slot
        assert bridge_marker['bridge_lsn'] > bridge_marker['copy_lsn']
        bridge_lsn = _read_state_lsn(state_path)
        assert bridge_lsn == bridge_marker['bridge_lsn']
        bridged_old_slot = _slot_status(e2e, wal2json_slot)
        bridged_new_slot = _slot_status(e2e, pgoutput_slot)
        assert bridged_old_slot is not None
        assert bridged_old_slot['plugin'] == 'wal2json'
        assert bridged_new_slot is not None
        assert bridged_new_slot['plugin'] == 'pgoutput'
        assert bridged_old_slot['confirmed_flush_lsn'] >= bridge_lsn
        assert bridged_new_slot['confirmed_flush_lsn'] >= bridge_lsn
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
        assert failed_old_slot is not None
        assert failed_old_slot['plugin'] == 'wal2json'
        failed_state = _read_state(state_path)
        assert failed_state[MIGRATION_STATE_KEY] == bridge_marker
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
        assert final_slot['confirmed_flush_lsn'] > bridge_lsn
        assert _slot_status(e2e, wal2json_slot) is None
        final_state = _read_state(state_path)
        assert MIGRATION_STATE_KEY not in final_state
        idle_retirement_lsn = _read_state_lsn(state_path)
        assert idle_retirement_lsn > bridge_lsn
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
        _select_partition_root(project_dir)
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
        }
        assert _publication_tables(e2e) == {
            (SOURCE_SCHEMA, TABLE_NAME),
            (SOURCE_SCHEMA, PARTITIONED_TABLE_NAME),
        }
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
