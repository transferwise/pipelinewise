"""Snowflake Native acknowledgement and de-duplication during pgoutput migration."""

import os
import shutil

from pathlib import Path

import pytest

from tests.end_to_end.helpers.env import E2EEnv
from tests.end_to_end.test_postgres_pgoutput_slots import (
    MIGRATION_STATE_KEY,
    _drop_slot,
    _read_state,
    _run_success,
    _slot_name,
    _slot_status,
)


TAP_ID = 'postgres_pgoutput_migration_sf'
TARGET_ID = 'postgres_pgoutput_migration_sf_dwh'
SOURCE_SCHEMA = 'ppw_e2e_pgoutput_sf_source'
TARGET_SCHEMA_PREFIX = 'ppw_e2e_pgoutput_sf_target'
TABLE_NAME = 'migration_records'
TEMPLATE_DIR = (
    Path(__file__).parents[2] / 'postgres_pgoutput_snowflake_test_project'
)


def _drop_publication(e2e, publication):
    e2e.run_query_tap_postgres(f'DROP PUBLICATION IF EXISTS {publication}')


def _source_target_rows(e2e, target_schema):
    source_rows = e2e.run_query_tap_postgres(
        f'SELECT id, status, payload FROM {SOURCE_SCHEMA}.{TABLE_NAME} ORDER BY id'
    )
    target_rows = e2e.run_query_target_snowflake(
        f'SELECT "ID", "STATUS", "PAYLOAD" '
        f'FROM "{target_schema}"."{TABLE_NAME.upper()}" ORDER BY "ID"'
    )
    return source_rows, target_rows


def test_native_snowflake_deduplicates_pgoutput_migration_overlap(tmp_path):
    """Snowflake makes the bridge durable and merges the repeated pgoutput rows."""
    project_dir = tmp_path / 'project'
    shutil.copytree(TEMPLATE_DIR, project_dir)
    e2e = E2EEnv(project_dir)
    if not e2e.env['TARGET_SNOWFLAKE']['is_configured']:
        pytest.skip('TARGET_SNOWFLAKE is not configured')

    config_dir = tmp_path / 'pipelinewise-config'
    config_dir.mkdir()
    command_env = {**os.environ, 'PIPELINEWISE_CONFIG_DIRECTORY': str(config_dir)}
    database = e2e.get_conn_env_var('TAP_POSTGRES', 'DB')
    wal2json_slot = _slot_name(database, TAP_ID)
    pgoutput_slot = f'ppw_slot_{TAP_ID}'
    publication = pgoutput_slot
    target_schema = f'{TARGET_SCHEMA_PREFIX}{e2e.sf_schema_postfix}'.upper()
    state_path = config_dir / TARGET_ID / TAP_ID / 'state.json'
    run_command = [
        'pipelinewise',
        'run_tap',
        '--tap',
        TAP_ID,
        '--target',
        TARGET_ID,
    ]
    try:
        _drop_slot(e2e, pgoutput_slot)
        _drop_slot(e2e, wal2json_slot)
        _drop_publication(e2e, publication)
        e2e.run_query_tap_postgres(
            f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE; '
            f'CREATE SCHEMA {SOURCE_SCHEMA}; '
            f'CREATE TABLE {SOURCE_SCHEMA}.{TABLE_NAME} '
            '(id integer PRIMARY KEY, status text NOT NULL, payload text NOT NULL); '
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (1, 'initial', 'payload-1')"
        )
        e2e.run_query_target_snowflake(
            f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE'
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
        assert _source_target_rows(e2e, target_schema)[0] == _source_target_rows(
            e2e, target_schema
        )[1]

        e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'delivered twice' WHERE id = 1; "
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (2, 'overlap row', 'payload-2')"
        )

        _run_success(run_command, command_env)
        marker = _read_state(state_path)[MIGRATION_STATE_KEY]
        assert marker['phase'] == 'pgoutput_overlap'
        assert _slot_status(e2e, wal2json_slot) is None
        assert _slot_status(e2e, pgoutput_slot)['confirmed_flush_lsn'] == marker['slot_lsn']
        assert _source_target_rows(e2e, target_schema)[0] == _source_target_rows(
            e2e, target_schema
        )[1]

        e2e.run_query_tap_postgres(
            f'UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = status WHERE id = 1'
        )
        _run_success(run_command, command_env)
        assert MIGRATION_STATE_KEY not in _read_state(state_path)
        source_rows, target_rows = _source_target_rows(e2e, target_schema)
        assert target_rows == source_rows == [
            (1, 'delivered twice', 'payload-1'),
            (2, 'overlap row', 'payload-2'),
        ]
        e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'after overlap' WHERE id = 1; "
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (3, 'native pgoutput', 'payload-3')"
        )
        _run_success(run_command, command_env)
        source_rows, target_rows = _source_target_rows(e2e, target_schema)
        assert target_rows == source_rows == [
            (1, 'after overlap', 'payload-1'),
            (2, 'overlap row', 'payload-2'),
            (3, 'native pgoutput', 'payload-3'),
        ]
    finally:
        _drop_slot(e2e, pgoutput_slot)
        _drop_slot(e2e, wal2json_slot)
        _drop_publication(e2e, publication)
        e2e.run_query_tap_postgres(
            f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE'
        )
        if e2e.env['TARGET_SNOWFLAKE']['is_configured']:
            e2e.run_query_target_snowflake(
                f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE'
            )
