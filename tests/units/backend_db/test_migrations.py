import importlib
from unittest.mock import patch


# pylint: disable=missing-function-docstring


MIGRATION = importlib.import_module(
    "pipelinewise.backend_db.migrations.versions.002_rename_watermark_columns"
)

UPGRADE_STATE_COLUMN_RENAMES = [
    "ALTER TABLE public.dd_watermark_state "
    "RENAME COLUMN coverage_start TO verified_start",
    "ALTER TABLE public.dd_watermark_state "
    "RENAME COLUMN verified_through TO verified_end",
    "ALTER TABLE public.dd_watermark_state "
    "RENAME COLUMN max_observed_end TO furthest_observed_end",
    "ALTER TABLE public.dd_watermark_state "
    "RENAME COLUMN coverage_status TO verified_status",
    "ALTER TABLE public.dd_watermark_state "
    "RENAME COLUMN evaluated_run_id TO last_evaluated_run_id",
]

UPGRADE_EVENT_COLUMN_RENAMES = [
    "ALTER TABLE public.dd_watermark_events "
    "RENAME COLUMN coverage_start TO verified_start",
    "ALTER TABLE public.dd_watermark_events "
    "RENAME COLUMN previous_verified_through TO previous_verified_end",
    "ALTER TABLE public.dd_watermark_events "
    "RENAME COLUMN verified_through TO verified_end",
    "ALTER TABLE public.dd_watermark_events "
    "RENAME COLUMN max_observed_end TO furthest_observed_end",
    "ALTER TABLE public.dd_watermark_events "
    "RENAME COLUMN coverage_status TO verified_status",
]


def _executed_sql(operation):
    return [call.args[0] for call in operation.call_args_list]


def test_watermark_upgrade_renames_columns_and_constraints():
    with patch.object(MIGRATION.op, "execute") as execute:
        MIGRATION.upgrade()

    statements = _executed_sql(execute)
    assert statements[:5] == UPGRADE_STATE_COLUMN_RENAMES
    assert statements[5:10] == UPGRADE_EVENT_COLUMN_RENAMES
    assert statements[10:12] == [
        "ALTER TABLE public.dd_watermark_state "
        "RENAME CONSTRAINT dd_watermark_state_coverage_status_check "
        "TO dd_watermark_state_verified_status_check",
        "ALTER TABLE public.dd_watermark_state "
        "RENAME CONSTRAINT dd_watermark_state_evaluated_run_id_fkey "
        "TO dd_watermark_state_last_evaluated_run_id_fkey",
    ]
    assert statements[12] == (
        "ALTER TABLE public.dd_watermark_events "
        "RENAME CONSTRAINT dd_watermark_events_coverage_status_check "
        "TO dd_watermark_events_verified_status_check"
    )
    assert "verified interval" in statements[13]
    assert "verified interval" in statements[14]
    assert "furthest observed" in statements[14]


def test_watermark_downgrade_reverses_every_rename():
    with patch.object(MIGRATION.op, "execute") as execute:
        MIGRATION.downgrade()

    statements = _executed_sql(execute)
    assert statements[0] == (
        "ALTER TABLE public.dd_watermark_events "
        "RENAME CONSTRAINT dd_watermark_events_verified_status_check "
        "TO dd_watermark_events_coverage_status_check"
    )
    assert statements[1:3] == [
        "ALTER TABLE public.dd_watermark_state "
        "RENAME CONSTRAINT dd_watermark_state_last_evaluated_run_id_fkey "
        "TO dd_watermark_state_evaluated_run_id_fkey",
        "ALTER TABLE public.dd_watermark_state "
        "RENAME CONSTRAINT dd_watermark_state_verified_status_check "
        "TO dd_watermark_state_coverage_status_check",
    ]
    assert statements[3:8] == [
        "ALTER TABLE public.dd_watermark_events "
        "RENAME COLUMN verified_status TO coverage_status",
        "ALTER TABLE public.dd_watermark_events "
        "RENAME COLUMN furthest_observed_end TO max_observed_end",
        "ALTER TABLE public.dd_watermark_events "
        "RENAME COLUMN verified_end TO verified_through",
        "ALTER TABLE public.dd_watermark_events "
        "RENAME COLUMN previous_verified_end TO previous_verified_through",
        "ALTER TABLE public.dd_watermark_events "
        "RENAME COLUMN verified_start TO coverage_start",
    ]
    assert statements[8:13] == [
        "ALTER TABLE public.dd_watermark_state "
        "RENAME COLUMN last_evaluated_run_id TO evaluated_run_id",
        "ALTER TABLE public.dd_watermark_state "
        "RENAME COLUMN verified_status TO coverage_status",
        "ALTER TABLE public.dd_watermark_state "
        "RENAME COLUMN furthest_observed_end TO max_observed_end",
        "ALTER TABLE public.dd_watermark_state "
        "RENAME COLUMN verified_end TO verified_through",
        "ALTER TABLE public.dd_watermark_state "
        "RENAME COLUMN verified_start TO coverage_start",
    ]
    assert "verified-through" in statements[13]
    assert "verified-through" in statements[14]
