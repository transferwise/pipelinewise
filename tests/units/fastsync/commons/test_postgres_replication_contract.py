"""Keep the separately installed Singer and FastSync replication contracts aligned."""

import ast
import re
from pathlib import Path

import pytest

from pipelinewise.fastsync.commons import tap_postgres as fastsync


ROOT = Path(__file__).resolve().parents[4]
TAP = ROOT / 'singer-connectors/tap-postgres/tap_postgres'


def _singer_helpers():
    # Load only dependency-free protocol helpers, without importing another runtime's packages.
    tree = ast.parse((TAP / 'sync_strategies/logical_replication.py').read_text())
    names = {'_replication_identifier', 'validate_tap_id', 'generate_replication_slot_name',
             'legacy_replication_slot_names', '_implicit_historical_slot_is_truncated',
             '_validate_migration_state'}
    module = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)
                              and node.name in names], type_ignores=[])
    namespace = {'re': re, 'ReplicationSlotMigrationError': RuntimeError,
                 'PGOUTPUT_MIGRATION_STATE_KEY': fastsync.PGOUTPUT_MIGRATION_STATE_KEY,
                 'PGOUTPUT_MIGRATION_STATE_VERSION':
                     fastsync.PGOUTPUT_MIGRATION_STATE_VERSION}
    exec(compile(module, '<Singer protocol helpers>', 'exec'), namespace)
    return namespace


@pytest.mark.parametrize('dbname,old_id,new_id', [
    ('orders', 'Old-id', 'new_id'), ('Mixed.DB', 'old_tap', 't' * 50),
    ('d' * 48, 't' * 40, 'new'), ('d' * 63, 'Old-id', 'new'),
])
def test_canonical_and_truncated_historical_slot_names_match(dbname, old_id, new_id):
    singer = _singer_helpers()
    destination, shared, dedicated = fastsync.FastSyncTapPostgres._replication_slot_names(dbname, new_id, old_id)
    assert destination == singer['generate_replication_slot_name'](new_id)
    assert [shared, dedicated] == singer['legacy_replication_slot_names'](dbname, old_id)


@pytest.mark.parametrize(
    'phase', ['bridge_pending', 'bridge', 'pgoutput_overlap', 'overlap_complete']
)
def test_persisted_migration_marker_is_accepted_by_both_runtimes(phase):
    config = {'dbname': 'orders', 'tap_id': 'new_id', 'previous_tap_id': 'Old-id'}
    marker = {'version': 2, 'phase': phase, 'source_slot': 'pipelinewise_orders_old_id',
              'destination_slot': 'ppw_slot_new_id', 'slot_lsn': 100,
              'boundary_token': 'a' * 32}
    if phase != 'bridge_pending':
        marker['bridge_lsn'] = 200
    if phase == 'overlap_complete':
        marker['crossover_lsn'] = 300
    assert fastsync.FastSyncTapPostgres.validate_migration_state_marker(config, marker)[0] == phase
    assert _singer_helpers()['_validate_migration_state'](config, marker) == phase


def test_safe_minor_release_warning_thresholds_match():
    tree = ast.parse((TAP / 'db.py').read_text())
    declaration = next(node.value for node in tree.body if isinstance(node, ast.Assign)
                       and any(isinstance(name, ast.Name) and name.id == 'MIN_SAFE_POSTGRES_VERSIONS'
                               for name in node.targets))
    assert ast.literal_eval(declaration) == fastsync.MIN_SAFE_POSTGRES_VERSIONS
