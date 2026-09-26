import importlib
from unittest.mock import patch

import pytest


MIGRATION = importlib.import_module(
    "pipelinewise.backend_db.migrations.versions.002_rename_watermark_columns"
)
HISTORICAL_MIGRATION = importlib.import_module(
    "pipelinewise.backend_db.migrations.versions.003_unresolved_historical_windows"
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


def test_historical_window_upgrade_requires_resolved_pass_and_fail_windows():
    with patch.object(HISTORICAL_MIGRATION.op, "execute") as execute:
        HISTORICAL_MIGRATION.upgrade()

    statements = _executed_sql(execute)
    assert HISTORICAL_MIGRATION.revision == "003"
    assert HISTORICAL_MIGRATION.down_revision == "002"
    assert (
        "ALTER TABLE public.dd_run_attempts ADD CONSTRAINT dd_run_attempts_status_check "
        "CHECK (status IN ('RUNNING', 'PASS', 'FAIL', 'ERROR', 'DEFERRED'))"
    ) in statements
    for table, unresolved_statuses in (
        ("dd_run_attempts", "'RUNNING', 'ERROR', 'DEFERRED'"),
        ("dd_run_slot_state", "'ERROR'"),
    ):
        assert f"ALTER TABLE public.{table} ALTER COLUMN window_start DROP NOT NULL" in statements
        assert (
            f"ALTER TABLE public.{table} ADD CONSTRAINT ck_{table}_resolved_window "
            f"CHECK (window_start IS NOT NULL OR status IN ({unresolved_statuses}))"
        ) in statements
        assert any(statement.startswith(f"COMMENT ON COLUMN public.{table}.window_start") for statement in statements)
    assert not any("DROP CONSTRAINT dd_run_slot_state_status_check" in statement for statement in statements)
    assert not any("DROP CONSTRAINT" in statement and "status_check" not in statement for statement in statements)


def test_historical_window_upgrade_pairs_nullable_verified_bounds_with_blocker():
    with patch.object(HISTORICAL_MIGRATION.op, "execute") as execute:
        HISTORICAL_MIGRATION.upgrade()

    statements = _executed_sql(execute)
    for table in ("dd_watermark_state", "dd_watermark_events"):
        assert (
            f"ALTER TABLE public.{table} "
            "ALTER COLUMN verified_start DROP NOT NULL, ALTER COLUMN verified_end DROP NOT NULL"
        ) in statements
        assert (
            f"ALTER TABLE public.{table} ADD CONSTRAINT ck_{table}_verified_bounds "
            "CHECK ((verified_start IS NOT NULL AND verified_end IS NOT NULL) OR "
            "(verified_start IS NULL AND verified_end IS NULL AND "
            "verified_status = 'BLOCKED' AND blocking_run_id IS NOT NULL))"
        ) in statements
        for column in ("verified_start", "verified_end"):
            assert any(statement.startswith(f"COMMENT ON COLUMN public.{table}.{column}") for statement in statements)
    assert not any(statement.lstrip().startswith(("UPDATE ", "DELETE ")) for statement in statements)


def test_historical_window_downgrade_checks_all_unrepresentable_history_before_changes():
    with patch.object(HISTORICAL_MIGRATION.op, "execute") as execute:
        HISTORICAL_MIGRATION.downgrade()

    statements = _executed_sql(execute)
    guard = " ".join(statements[0].split())
    assert "dd_run_attempts WHERE window_start IS NULL OR status = 'DEFERRED'" in guard
    assert "dd_run_slot_state WHERE window_start IS NULL" in guard
    for table in ("dd_watermark_state", "dd_watermark_events"):
        assert f"{table} WHERE verified_start IS NULL OR verified_end IS NULL" in guard
    assert "RAISE EXCEPTION 'Cannot downgrade data-diff to revision 002" in guard
    assert "do not discard audit history" in guard
    assert not any(statement.lstrip().startswith(("UPDATE ", "DELETE ")) for statement in statements)


def test_historical_window_downgrade_stops_without_mutation_when_history_blocks_it():
    with patch.object(HISTORICAL_MIGRATION.op, "execute", side_effect=RuntimeError("unresolved history")) as execute:
        with pytest.raises(RuntimeError, match="unresolved history"):
            HISTORICAL_MIGRATION.downgrade()

    execute.assert_called_once()
    assert execute.call_args.args[0].lstrip().startswith("DO $$ BEGIN")


def test_historical_window_downgrade_restores_revision_002_constraints_and_comments():
    with patch.object(HISTORICAL_MIGRATION.op, "execute") as execute:
        HISTORICAL_MIGRATION.downgrade()

    statements = _executed_sql(execute)
    for table in ("dd_run_attempts", "dd_run_slot_state"):
        assert f"ALTER TABLE public.{table} DROP CONSTRAINT ck_{table}_resolved_window" in statements
        assert f"ALTER TABLE public.{table} ALTER COLUMN window_start SET NOT NULL" in statements
        assert f"COMMENT ON COLUMN public.{table}.window_start IS NULL" in statements
    for table in ("dd_watermark_state", "dd_watermark_events"):
        assert f"ALTER TABLE public.{table} DROP CONSTRAINT ck_{table}_verified_bounds" in statements
        assert (
            f"ALTER TABLE public.{table} "
            "ALTER COLUMN verified_start SET NOT NULL, ALTER COLUMN verified_end SET NOT NULL"
        ) in statements
        for column in ("verified_start", "verified_end"):
            assert f"COMMENT ON COLUMN public.{table}.{column} IS NULL" in statements
    assert (
        "ALTER TABLE public.dd_run_attempts ADD CONSTRAINT dd_run_attempts_status_check "
        "CHECK (status IN ('RUNNING', 'PASS', 'FAIL', 'ERROR'))"
    ) in statements
    assert "COMMENT ON COLUMN public.dd_run_attempts.status IS NULL" in statements
