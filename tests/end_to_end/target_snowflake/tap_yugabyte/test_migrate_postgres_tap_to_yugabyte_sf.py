"""Cut a tap over from tap-postgres to tap-yugabyte on Snowflake without a resync.

The YugabyteDB table keeps its default CHANGE replica identity, as production tables do: Snowflake keeps the columns
an update omits.
"""

from pathlib import Path

import pytest

from tests.end_to_end.helpers.yugabyte_migration import (
    MIXED_TABLES_WITHOUT_FULL_TABLE,
    SnowflakeTarget,
    migration_scenarios,
    run_cutover,
)


TEMPLATE_DIR = Path(__file__).parent / 'migration_test_project'


@pytest.fixture(scope='module')
def make_scenario(tmp_path_factory):
    with migration_scenarios(tmp_path_factory, TEMPLATE_DIR, SnowflakeTarget) as (e2e, make):
        if not e2e.env['TARGET_SNOWFLAKE']['is_configured']:
            pytest.fail('TARGET_SNOWFLAKE is not configured')
        yield make


def test_cutover_with_slot_created_early_replays_overlap(make_scenario):
    """A slot created while YugabyteDB is still kept in sync replays changes the target already has."""
    run_cutover(make_scenario('ybm_sf_early'), slot_created_early=True)


def test_cutover_after_freeze_with_mixed_replication_methods(make_scenario):
    """The slot is created once PostgreSQL is drained; INCREMENTAL tables move with the tap."""
    run_cutover(make_scenario('ybm_sf_mixed', MIXED_TABLES_WITHOUT_FULL_TABLE), slot_created_early=False)
