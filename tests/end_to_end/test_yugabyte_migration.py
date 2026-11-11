"""Cut a tap over from tap-postgres to tap-yugabyte on a PostgreSQL target without a resync."""

from pathlib import Path

import pytest

from .helpers.yugabyte_migration import (
    MIXED_TABLES,
    PostgresTarget,
    migration_scenarios,
    run_cutover,
)


TEMPLATE_DIR = Path(__file__).parent / 'yugabyte_migration_test_project'


@pytest.fixture(scope='module')
def make_scenario(tmp_path_factory):
    with migration_scenarios(tmp_path_factory, TEMPLATE_DIR, PostgresTarget) as (_, make):
        yield make


def test_cutover_with_slot_created_early_replays_overlap(make_scenario):
    """A slot created while YugabyteDB is still kept in sync replays changes the target already has."""
    run_cutover(make_scenario('ybm_pg_early'), slot_created_early=True)


def test_cutover_after_freeze_with_mixed_replication_methods(make_scenario):
    """The slot is created once PostgreSQL is drained; INCREMENTAL and FULL_TABLE tables move with the tap."""
    run_cutover(make_scenario('ybm_pg_mixed', MIXED_TABLES), slot_created_early=False)
