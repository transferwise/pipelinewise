"""Unbounded PartialSync repair of a YugabyteDB LOG_BASED table on Snowflake, verified against exact contents."""

import json
import os
import shutil
import sys

from pathlib import Path

import pytest

from tests.end_to_end.helpers.env import E2EEnv
from tests.end_to_end.helpers.pipeline_process import read_state, run_cleanups, run_success
from tests.end_to_end.helpers.yugabyte_recovery import (
    KeyAllocator,
    acknowledge_source_changes,
    assert_export_predates,
    assert_slot_behind_state,
    commit_each,
    drop_slot,
    mixed_changes,
    slot_name,
    slot_status,
)


TAP_ID = 'yugabyte_recovery_sf'
TARGET_ID = 'yugabyte_recovery_sf_dwh'
SOURCE_SCHEMA = 'ppw_e2e_yb_recovery_sf_source'
TABLE = 'repaired_records'
SOURCE_TABLE = f'{SOURCE_SCHEMA}.{TABLE}'
STREAM_ID = f'{SOURCE_SCHEMA}-{TABLE}'
INITIAL_ROWS = 1000
TEMPLATE_DIR = Path(__file__).parent / 'recovery_test_project'


@pytest.fixture
def project(tmp_path):
    """Import an isolated tap whose table keeps YugabyteDB's default CHANGE replica identity."""
    project_dir = tmp_path / 'project'
    shutil.copytree(TEMPLATE_DIR, project_dir)
    e2e = E2EEnv(project_dir)
    if not e2e.env['TARGET_SNOWFLAKE']['is_configured']:
        pytest.fail('TARGET_SNOWFLAKE is not configured')
    config_dir = tmp_path / 'pipelinewise-config'
    config_dir.mkdir()
    command_env = os.environ.copy()
    command_env['PIPELINEWISE_CONFIG_DIRECTORY'] = str(config_dir)
    target_schema = f'ppw_e2e_yb_recovery_sf{e2e.sf_schema_postfix}'.upper()

    def pipelinewise(*arguments):
        return run_success(['pipelinewise', *arguments], command_env, timeout=600)

    drop_slot(e2e, TAP_ID)
    e2e.run_query_tap_yugabyte(f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE')
    e2e.run_query_tap_yugabyte(f'CREATE SCHEMA {SOURCE_SCHEMA}')
    e2e.run_query_tap_yugabyte(
        f'CREATE TABLE {SOURCE_TABLE} (id integer PRIMARY KEY, payload text NOT NULL, version integer NOT NULL)'
    )
    e2e.run_query_tap_yugabyte(
        f'INSERT INTO {SOURCE_TABLE} (id, payload, version) '
        f'SELECT k, md5(k::text), 0 FROM generate_series(1, {INITIAL_ROWS}) AS k'
    )
    pipelinewise('validate', '--dir', str(project_dir))
    pipelinewise('import_config', '--dir', str(project_dir))
    try:
        yield {
            'e2e': e2e,
            'pipelinewise': pipelinewise,
            'state_path': config_dir / TARGET_ID / TAP_ID / 'state.json',
            'slot_name': slot_name(e2e, TAP_ID),
            'target_table': f'"{target_schema}"."{TABLE.upper()}"',
        }
    finally:
        run_cleanups(sys.exception(), [
            ('drop replication slot', lambda: drop_slot(e2e, TAP_ID)),
            ('drop source schema',
             lambda: e2e.run_query_tap_yugabyte(f'DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE')),
            ('drop target schema',
             lambda: e2e.run_query_target_snowflake(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')),
        ])


def _target_rows(project):
    rows = project['e2e'].run_query_target_snowflake(
        f'SELECT "ID", "PAYLOAD", "VERSION" FROM {project["target_table"]} ORDER BY "ID"'
    )
    return [(int(row_id), payload, int(version)) for row_id, payload, version in rows]


def _assert_target_matches_source(project):
    e2e = project['e2e']
    source_rows = e2e.run_query_tap_yugabyte(f'SELECT id, payload, version FROM {SOURCE_TABLE} ORDER BY id')
    target_rows = _target_rows(project)
    assert target_rows == source_rows, (
        f'{len(source_rows)} source rows, {len(target_rows)} target rows; '
        f'missing {sorted(set(source_rows) - set(target_rows))[:5]}, '
        f'unexpected {sorted(set(target_rows) - set(source_rows))[:5]}'
    )
    assert e2e.run_query_target_snowflake(
        f'SELECT COUNT(*) FROM {project["target_table"]} WHERE "_SDC_DELETED_AT" IS NOT NULL'
    )[0][0] == 0


def test_unbounded_partial_sync_repairs_a_log_based_table(project):
    """A slot-pinned PartialSync export plus replay converges, keeping columns a CHANGE update omits."""
    e2e = project['e2e']

    def run_tap():
        project['pipelinewise']('run_tap', '--tap', TAP_ID, '--target', TARGET_ID)

    def state_lsn():
        return read_state(project['state_path'])['bookmarks'][STREAM_ID]['lsn']

    project['pipelinewise']('fast_sync', '--tap', TAP_ID, '--target', TARGET_ID)
    changes = mixed_changes(SOURCE_TABLE, KeyAllocator())
    e2e.run_query_tap_yugabyte(*changes.seed)
    acknowledge_source_changes(e2e, SOURCE_TABLE, run_tap)
    acknowledged_slot = assert_slot_behind_state(e2e, project['slot_name'], [state_lsn()])

    commit_each(e2e, changes.statements)
    e2e.run_query_target_snowflake(
        f"INSERT INTO {project['target_table']} (\"ID\", \"PAYLOAD\", \"VERSION\") VALUES (-1, 'stale target row', 0)"
    )
    e2e.run_query_target_snowflake(f'DELETE FROM {project["target_table"]} WHERE "ID" = 2')

    project['pipelinewise'](
        'partial_sync_table', '--tap', TAP_ID, '--target', TARGET_ID,
        '--table', SOURCE_TABLE, '--column', 'id', '--start_value', '-100',
    )

    slot = slot_status(e2e, project['slot_name'])
    assert state_lsn() == slot['restart_commit_ht']
    assert slot['confirmed_flush_lsn'] == acknowledged_slot['confirmed_flush_lsn']
    exported_keys = {row[0] for row in _target_rows(project)}
    assert_export_predates(exported_keys, changes)
    assert -1 not in exported_keys
    assert 2 in exported_keys

    run_tap()

    _assert_target_matches_source(project)
    final_slot = assert_slot_behind_state(e2e, project['slot_name'], [state_lsn()])
    print(json.dumps({
        'acknowledged_before_resync': acknowledged_slot['confirmed_flush_lsn'],
        'final_slot_confirmed_flush_lsn': final_slot['confirmed_flush_lsn'],
        'final_state_lsn': state_lsn(),
        'resync_bookmark': slot['restart_commit_ht'],
    }, sort_keys=True))
