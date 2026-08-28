from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

from pipelinewise.data_diff.config import CheckDefinition
from pipelinewise.data_diff.repository import DataDiffRepository

# pylint: disable=missing-class-docstring,missing-function-docstring,invalid-name
# pylint: disable=protected-access


def _definition(config_hash_seed="one"):
    # The seed varies frequency to produce different config hashes.
    return CheckDefinition(
        full_check_name="target/tap/public/payments",
        target_id="target",
        tap_id="tap",
        source_type="tap-postgres",
        target_type="target-snowflake",
        source_database="source",
        target_database="target",
        source_schema="public",
        source_table="payments",
        target_schema="PUBLIC",
        target_table="PAYMENTS",
        source_key_column="id",
        target_key_column="ID",
        source_timestamp_column="updated_at",
        target_timestamp_column="UPDATED_AT",
        source_compare_columns=("status",),
        target_compare_columns=("STATUS",),
        checks=("row_count",),
        frequency=f"0 */{config_hash_seed} * * *",
        window_start_seconds=3600,
        window_end_seconds=0,
        statement_timeout_seconds=300,
    )


class ScriptedCursor:
    def __init__(self, current):
        self.current = current
        self.last_sql = ""
        self.executions = []

    def execute(self, sql, params=None):
        self.last_sql = " ".join(sql.split())
        self.executions.append((self.last_sql, params))

    def fetchall(self):
        if "dd_checks" in self.last_sql:
            return self.current
        return []

    def fetchone(self):
        if "MAX(revision)" in self.last_sql:
            return {"revision": 1}
        raise AssertionError(f"Unexpected fetchone for {self.last_sql}")


def _repository_with_cursor(cursor):
    repository = DataDiffRepository(Mock())
    repository.ensure_schema = Mock()

    @contextmanager
    def use_cursor():
        yield cursor

    repository.cursor = use_cursor
    return repository


def test_new_definition_is_inserted_as_revision_one():
    cursor = ScriptedCursor([])
    repository = _repository_with_cursor(cursor)

    stats = repository.sync_definitions([_definition()])

    assert stats == {"created": 1, "unchanged": 0, "superseded": 0, "deactivated": 0}
    assert any("INSERT INTO public.dd_checks" in sql for sql, _ in cursor.executions)


def test_same_hash_is_idempotent():
    definition = _definition()
    current = [{
        "check_id": uuid4(),
        "full_check_name": definition.full_check_name,
        "config_hash": definition.config_hash,
        "tap_id": definition.tap_id,
    }]
    cursor = ScriptedCursor(current)
    repository = _repository_with_cursor(cursor)

    stats = repository.sync_definitions([definition])

    assert stats["unchanged"] == 1
    assert not any("INSERT INTO public.dd_checks" in sql for sql, _ in cursor.executions)


def test_changed_definition_supersedes_active_revision():
    definition = _definition()
    current = [{
        "check_id": uuid4(),
        "full_check_name": definition.full_check_name,
        "config_hash": "stale" + "0" * 59,
        "tap_id": definition.tap_id,
    }]
    cursor = ScriptedCursor(current)
    repository = _repository_with_cursor(cursor)

    stats = repository.sync_definitions([definition])

    assert stats["superseded"] == 1
    assert stats["created"] == 1
    supersede = next(
        (sql, params)
        for sql, params in cursor.executions
        if "SET current = FALSE" in sql
    )
    assert supersede[1][1] == current[0]["check_id"]
    # dd_checks is keyed by check_id; dd_check_id only exists on dd_runs.
    assert "WHERE check_id = %s" in supersede[0]
    assert "dd_check_id" not in supersede[0]


def test_partial_scope_only_deactivates_selected_tap():
    current = [
        {"check_id": uuid4(), "full_check_name": "one", "config_hash": "a" * 64, "tap_id": "selected"},
        {"check_id": uuid4(), "full_check_name": "two", "config_hash": "b" * 64, "tap_id": "untouched"},
    ]
    cursor = ScriptedCursor(current)
    repository = _repository_with_cursor(cursor)

    stats = repository.sync_definitions([], selected_taps=["selected"])

    assert stats["deactivated"] == 1
    updates = [
        (sql, params)
        for sql, params in cursor.executions
        if "SET current = FALSE" in sql
    ]
    assert updates[0][1][1] == current[0]["check_id"]
    # dd_checks is keyed by check_id; dd_check_id only exists on dd_runs.
    assert "WHERE check_id = %s" in updates[0][0]
    assert "dd_check_id" not in updates[0][0]


def test_list_checks_exposes_compare_columns_from_version_snapshot():
    row = {
        "check_id": uuid4(),
        "full_check_name": "target/tap/public/payments/check",
        "canonical_config": {
            "source_compare_columns": ["status", "amount"],
            "target_compare_columns": ["STATUS", "AMOUNT"],
        },
    }
    cursor = ScriptedCursor([row])
    repository = _repository_with_cursor(cursor)

    checks = repository.list_checks()

    assert checks[0]["source_compare_columns"] == ["status", "amount"]
    assert checks[0]["target_compare_columns"] == ["STATUS", "AMOUNT"]
    assert "LEFT JOIN public.dd_coverage_state coverage" in cursor.last_sql
    assert "coverage.updated_at AS verified_at" in cursor.last_sql
    assert "dd_current_coverage" not in cursor.last_sql


def test_schema_migration_never_drops_shared_schema():
    """Alembic migrations must not contain DROP SCHEMA in their upgrade path."""
    versions_dir = (
        Path(__file__).parents[3]
        / "pipelinewise"
        / "backend_db"
        / "migrations"
        / "versions"
    )
    migration_files = list(versions_dir.glob("*.py"))
    assert len(migration_files) > 0, "No migration files found"

    all_upgrade_content = []
    for path in migration_files:
        content = path.read_text(encoding="utf-8")
        # Extract only the upgrade() function content (rough heuristic)
        if 'def upgrade' in content:
            upgrade_section = content.split('def upgrade')[1].split('def downgrade')[0]
            all_upgrade_content.append(upgrade_section.upper())

    combined = "\n".join(all_upgrade_content)
    assert "DROP SCHEMA" not in combined
    assert "TIMESTAMPTZ" in combined
    assert "DD_COVERAGE_EVENTS" in combined


def test_ensure_schema_calls_alembic_migrate():
    database = Mock()
    repository = DataDiffRepository(database)

    repository.ensure_schema()

    database.migrate.assert_called_once()


def test_remediation_attempt_reuses_failed_window_and_links_original_run():
    cursor = Mock()
    cursor.fetchall.return_value = [{"status": "FAIL", "max_attempt": 1}]
    repository = _repository_with_cursor(cursor)
    original_id = uuid4()
    check_id = uuid4()
    scheduled_for = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    original = {
        "run_id": original_id,
        "dd_check_id": check_id,
        "scheduled_for": scheduled_for,
        "window_start": datetime(2026, 7, 22, 6, tzinfo=timezone.utc),
        "window_end": datetime(2026, 7, 22, 7, tzinfo=timezone.utc),
        "status": "FAIL",
        "rerun_of_run_id": None,
    }

    run = repository.start_remediation_run(original, "AP-1234")

    assert run["attempt"] == 2
    assert run["trigger"] == "REMEDIATION"
    assert run["rerun_of_run_id"] == original_id
    insert_params = next(
        call_args.args[1] for call_args in cursor.execute.call_args_list
        if "INSERT INTO public.dd_runs"
        in " ".join(call_args.args[0].split())
    )
    assert insert_params[1] == check_id
    assert insert_params[2] == scheduled_for
    assert insert_params[3] == original["window_start"]
    assert insert_params[4] == original["window_end"]
    assert insert_params[6] == original_id
    assert insert_params[7] == "AP-1234"


def test_finish_run_updates_coverage_in_same_transaction():
    cursor = Mock()
    repository = _repository_with_cursor(cursor)
    repository._record_coverage_event = Mock()
    run_id = uuid4()

    repository.finish_run(
        run_id,
        "PASS",
        [{"check_type": "row_count", "status": "PASS", "source_value": 1}],
    )

    repository._record_coverage_event.assert_called_once()
    assert repository._record_coverage_event.call_args.args[0] is cursor
    assert repository._record_coverage_event.call_args.args[1] == run_id


def _coverage_state(start, end, *, status="CONTIGUOUS", blocking_run_id=None):
    return {
        "coverage_start": start,
        "verified_through": end,
        "max_observed_end": end,
        "coverage_status": status,
        "blocking_run_id": blocking_run_id,
        "reason": "previous coverage state",
        "state_version": 4,
    }


def _terminal_attempt(start, end, *, status="PASS", attempt=1):
    return {
        "run_id": uuid4(),
        "check_id": uuid4(),
        "scheduled_for": end,
        "window_start": start,
        "window_end": end,
        "attempt": attempt,
        "status": status,
        "checks": ["row_count"],
    }


def test_new_latest_slot_advances_coverage_without_history_scan():
    cursor = Mock()
    start = datetime(2026, 7, 22, 10, tzinfo=timezone.utc)
    previous_end = start + timedelta(hours=1)
    attempt = _terminal_attempt(previous_end, previous_end + timedelta(hours=1))
    cursor.fetchone.return_value = attempt
    previous = _coverage_state(start, previous_end)

    with patch.object(
        DataDiffRepository, "_coverage_state_for_update", return_value=previous
    ), patch.object(
        DataDiffRepository, "_effective_attempt_for_update", return_value=None
    ), patch.object(
        DataDiffRepository, "_has_later_effective_attempt", return_value=False
    ), patch.object(
        DataDiffRepository, "_upsert_effective_attempt", return_value=True
    ), patch.object(
        DataDiffRepository, "_recalculate_effective_coverage"
    ) as recalculate, patch.object(
        DataDiffRepository, "_upsert_coverage_state"
    ) as upsert_state, patch.object(
        DataDiffRepository, "_insert_coverage_event"
    ):
        DataDiffRepository._record_coverage_event(cursor, attempt["run_id"], attempt["window_end"])

    recalculate.assert_not_called()
    coverage = upsert_state.call_args.args[2]
    assert coverage["verified_through"] == attempt["window_end"]
    assert coverage["coverage_status"] == "CONTIGUOUS"


def test_replacement_attempt_recalculates_from_effective_slots():
    cursor = Mock()
    start = datetime(2026, 7, 22, 10, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    attempt = _terminal_attempt(start, end, status="FAIL", attempt=2)
    cursor.fetchone.return_value = attempt
    previous = _coverage_state(start, end)
    recalculated = {
        **previous,
        "verified_through": start,
        "coverage_status": "BLOCKED",
        "blocking_run_id": attempt["run_id"],
        "reason": "replacement failed",
    }

    with patch.object(
        DataDiffRepository, "_coverage_state_for_update", return_value=previous
    ), patch.object(
        DataDiffRepository,
        "_effective_attempt_for_update",
        return_value={"run_id": uuid4(), "attempt": 1},
    ), patch.object(
        DataDiffRepository, "_has_later_effective_attempt", return_value=False
    ), patch.object(
        DataDiffRepository, "_upsert_effective_attempt", return_value=True
    ), patch.object(
        DataDiffRepository,
        "_recalculate_effective_coverage",
        return_value=recalculated,
    ) as recalculate, patch.object(
        DataDiffRepository, "_upsert_coverage_state"
    ), patch.object(
        DataDiffRepository, "_insert_coverage_event"
    ):
        DataDiffRepository._record_coverage_event(cursor, attempt["run_id"], end)

    recalculate.assert_called_once_with(
        cursor,
        attempt["check_id"],
        data_checks_enabled=True,
    )


def test_out_of_order_new_slot_recalculates_from_effective_slots():
    cursor = Mock()
    start = datetime(2026, 7, 22, 10, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    attempt = _terminal_attempt(start, end)
    cursor.fetchone.return_value = attempt
    previous = _coverage_state(start, end + timedelta(hours=2))

    with patch.object(
        DataDiffRepository, "_coverage_state_for_update", return_value=previous
    ), patch.object(
        DataDiffRepository, "_effective_attempt_for_update", return_value=None
    ), patch.object(
        DataDiffRepository, "_has_later_effective_attempt", return_value=True
    ), patch.object(
        DataDiffRepository, "_upsert_effective_attempt", return_value=True
    ), patch.object(
        DataDiffRepository,
        "_recalculate_effective_coverage",
        return_value=previous,
    ) as recalculate, patch.object(
        DataDiffRepository, "_upsert_coverage_state"
    ), patch.object(
        DataDiffRepository, "_insert_coverage_event"
    ):
        DataDiffRepository._record_coverage_event(cursor, attempt["run_id"], end)

    recalculate.assert_called_once_with(
        cursor,
        attempt["check_id"],
        data_checks_enabled=True,
    )


def test_exceptional_recalculation_reads_effective_slots_not_run_history():
    cursor = Mock()
    start = datetime(2026, 7, 22, 10, tzinfo=timezone.utc)
    cursor.fetchall.return_value = [
        {
            "run_id": uuid4(),
            "scheduled_for": start + timedelta(hours=1),
            "window_start": start,
            "window_end": start + timedelta(hours=1),
            "attempt": 2,
            "status": "PASS",
        }
    ]

    coverage = DataDiffRepository._recalculate_effective_coverage(
        cursor,
        uuid4(),
        data_checks_enabled=True,
    )

    sql = " ".join(cursor.execute.call_args.args[0].split())
    assert "FROM public.dd_effective_attempts" in sql
    assert "FROM public.dd_runs" not in sql
    assert coverage["coverage_status"] == "CONTIGUOUS"


def test_repository_builds_the_shared_database_from_backend_config():
    config = {
        "host": "backend",
        "port": 5432,
        "user": "pipelinewise",
        "password": "secret",
        "dbname": "pipelinewise",
    }
    with patch(
        "pipelinewise.data_diff.repository.BackendDatabase.from_config"
    ) as database_from_config:
        repository = DataDiffRepository.from_backend_config(config)

    database_from_config.assert_called_once_with(
        config,
        application_name="pipelinewise-data-diff",
    )
    assert repository.database is database_from_config.return_value
