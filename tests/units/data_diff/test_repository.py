from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from pipelinewise.data_diff.config import CheckDefinition
from pipelinewise.data_diff.repository import DataDiffRepository, RunLeaseLostError


def _definition(config_hash_seed="one", *, tap_id="tap", source_table="payments"):
    # The seed varies frequency to produce different config hashes.
    return CheckDefinition(
        full_check_name=f"target/{tap_id}/public/{source_table}",
        target_id="target",
        tap_id=tap_id,
        source_type="tap-postgres",
        target_type="target-snowflake",
        source_database="source",
        target_database="target",
        source_schema="public",
        source_table=source_table,
        target_schema="PUBLIC",
        target_table=source_table.upper(),
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
        if "dd_check_definitions" in self.last_sql:
            return self.current
        return []

    def fetchone(self):
        if "MAX(revision)" in self.last_sql:
            return {"revision": 1}
        if 'COUNT(*) AS historical_scans_pending' in self.last_sql:
            return {'historical_scans_pending': 0}
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

    assert stats == {
        "created": 1, "unchanged": 0, "superseded": 0, "deactivated": 0, 'historical_scans_pending': 0,
    }
    assert any("INSERT INTO public.dd_check_definitions" in sql for sql, _ in cursor.executions)


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
    assert not any("INSERT INTO public.dd_check_definitions" in sql for sql, _ in cursor.executions)


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
        if "SET is_current = FALSE" in sql
    )
    assert supersede[1][1] == current[0]["check_id"]
    assert "WHERE check_id = %s" in supersede[0]


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
        if "SET is_current = FALSE" in sql
    ]
    assert updates[0][1][1] == current[0]["check_id"]
    assert "WHERE check_id = %s" in updates[0][0]


def test_full_scope_preserves_failed_tap_and_deactivates_deleted_tap():
    failed_definition = _definition(tap_id="failed", source_table="new")
    successful_definition = _definition(tap_id="successful", source_table="new")
    failed_check_id = uuid4()
    deleted_check_id = uuid4()
    current = [
        {
            "check_id": failed_check_id,
            "full_check_name": "target/failed/public/old",
            "config_hash": "a" * 64,
            "tap_id": "failed",
        },
        {
            "check_id": deleted_check_id,
            "full_check_name": "target/deleted/public/old",
            "config_hash": "b" * 64,
            "tap_id": "deleted",
        },
    ]
    cursor = ScriptedCursor(current)
    repository = _repository_with_cursor(cursor)

    stats = repository.sync_definitions(
        [failed_definition, successful_definition],
        selected_taps=["*"],
        excluded_taps=["failed"],
    )

    assert stats == {
        "created": 1,
        "unchanged": 0,
        "superseded": 0,
        "deactivated": 1,
        'historical_scans_pending': 0,
    }
    assert not any(
        params and failed_check_id in params
        for _sql, params in cursor.executions
    )
    assert any(
        params and deleted_check_id in params
        for _sql, params in cursor.executions
    )


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
    assert checks[0]["initial_full_scan"] is True
    assert "LEFT JOIN public.dd_watermark_state coverage" in cursor.last_sql
    assert "coverage.verified_start" in cursor.last_sql
    assert "coverage.verified_end" in cursor.last_sql
    assert "coverage.furthest_observed_end" in cursor.last_sql
    assert "coverage.verified_status" in cursor.last_sql
    assert "coverage.last_evaluated_run_id" in cursor.last_sql
    assert "coverage.updated_at AS verified_at" in cursor.last_sql
    assert "dd_current_coverage" not in cursor.last_sql
    assert 'AS historical_scan_pending' in cursor.last_sql
    assert "history.status != 'DEFERRED'" in cursor.last_sql
    assert "history.trigger_type != 'REMEDIATION'" in cursor.last_sql


def test_get_check_version_restores_initial_full_scan_from_snapshot():
    cursor = Mock()
    check_id = uuid4()
    cursor.fetchone.return_value = {
        "check_id": check_id,
        "canonical_config": {"initial_full_scan": False},
    }
    repository = _repository_with_cursor(cursor)

    assert repository.get_check_version(check_id)["initial_full_scan"] is False


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
    assert "DD_WATERMARK_EVENTS" in combined


def test_ensure_schema_calls_alembic_migrate():
    database = Mock()
    repository = DataDiffRepository(database)

    repository.ensure_schema()

    database.migrate.assert_called_once()


@pytest.mark.parametrize('status', ['FAIL', 'ERROR'])
def test_remediation_attempt_reuses_failed_window_and_links_original_run(status):
    cursor = Mock()
    cursor.fetchall.return_value = [{"status": status, "max_attempt": 1}]
    repository = _repository_with_cursor(cursor)
    original_id = uuid4()
    check_id = uuid4()
    scheduled_for = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    original = {
        "run_id": original_id,
        "check_id": check_id,
        "scheduled_for": scheduled_for,
        "window_start": datetime(2026, 7, 22, 6, tzinfo=timezone.utc),
        "window_end": datetime(2026, 7, 22, 7, tzinfo=timezone.utc),
        "status": status,
        "rerun_of_run_id": None,
    }

    run = repository.start_remediation_run(original, "AP-1234")

    assert run["attempt"] == 2
    assert run["trigger_type"] == "REMEDIATION"
    assert run["rerun_of_run_id"] == original_id
    insert_params = next(
        call_args.args[1] for call_args in cursor.execute.call_args_list
        if "INSERT INTO public.dd_run_attempts"
        in " ".join(call_args.args[0].split())
    )
    assert insert_params[1] == check_id
    assert insert_params[2] == scheduled_for
    assert insert_params[3] == original["window_start"]
    assert insert_params[4] == original["window_end"]
    assert insert_params[6] == original_id
    assert insert_params[7] == "AP-1234"


def _start_run_insert_params(cursor):
    return next(
        call_args.args[1] for call_args in cursor.execute.call_args_list
        if "INSERT INTO public.dd_run_attempts"
        in " ".join(call_args.args[0].split())
    )


def test_first_scheduled_attempt_has_unresolved_historical_start():
    cursor = Mock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = {"has_previous_run": False, "has_pending_historical_run": False}
    repository = _repository_with_cursor(cursor)
    check_id = uuid4()
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    normal_start = slot - timedelta(hours=1)

    run = repository.start_run({"check_id": check_id, "checks": ["row_count"]}, slot, normal_start, slot)

    assert run["window_start"] is None
    assert run["window_end"] == slot
    assert run["trigger_type"] == "SCHEDULED"
    assert _start_run_insert_params(cursor)[3:5] == (None, slot)
    assert any(
        "WHERE check_id = %s AND trigger_type != 'REMEDIATION' AND status != 'DEFERRED'"
        in " ".join(call.args[0].split())
        for call in cursor.execute.call_args_list
    )


def test_first_scheduled_attempt_uses_bounded_window_when_full_scan_disabled():
    cursor = Mock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = {"has_previous_run": False, "has_pending_historical_run": False}
    repository = _repository_with_cursor(cursor)
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    normal_start = slot - timedelta(hours=1)

    run = repository.start_run(
        {"check_id": uuid4(), "initial_full_scan": False},
        slot, normal_start, slot,
    )

    assert run["window_start"] == normal_start
    assert _start_run_insert_params(cursor)[3:5] == (normal_start, slot)


def test_metadata_only_first_attempt_keeps_rolling_window():
    cursor = Mock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = {'has_previous_run': False, 'has_pending_historical_run': False}
    repository = _repository_with_cursor(cursor)
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    start = slot - timedelta(hours=1)

    run = repository.start_run(
        {'check_id': uuid4(), 'checks': ['schema_compatibility']}, slot, start, slot,
    )

    assert run['window_start'] == start
    assert _start_run_insert_params(cursor)[3:5] == (start, slot)


def test_later_scheduled_attempt_keeps_bounded_window():
    cursor = Mock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = {"has_previous_run": True, "has_pending_historical_run": False}
    repository = _repository_with_cursor(cursor)
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    normal_start = slot - timedelta(hours=1)

    run = repository.start_run({"check_id": uuid4()}, slot, normal_start, slot)

    assert run["window_start"] == normal_start
    assert _start_run_insert_params(cursor)[3:5] == (normal_start, slot)


@pytest.mark.parametrize('force', [False, True])
@pytest.mark.parametrize('has_previous_run', [False, True])
def test_new_slot_waits_for_unresolved_historical_attempt(force, has_previous_run):
    cursor = Mock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = {
        'has_previous_run': has_previous_run,
        'has_pending_historical_run': True,
    }
    repository = _repository_with_cursor(cursor)
    check = {'check_id': uuid4(), 'checks': ['row_count']}
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)

    skipped = repository.start_run(check, slot, slot - timedelta(hours=1), slot, force=force)
    assert skipped['status'] == 'SKIPPED'
    assert skipped['slot_status'] == 'RUNNING'
    assert 'Historical baseline discovery' in skipped['error']
    assert skipped['window_start'] is None
    assert skipped['window_end'] is None

    assert not any('INSERT INTO public.dd_run_attempts' in call.args[0] for call in cursor.execute.call_args_list)
    sql, params = cursor.execute.call_args.args
    pending_query = ' '.join(sql.split()).split('AS has_previous_run,')[1]
    assert "WHERE check_id = %s AND status = 'RUNNING' AND window_start IS NULL" in pending_query
    assert 'trigger_type' not in pending_query
    assert params == (check['check_id'], check['check_id'])


def test_waiting_slot_still_discovers_history_after_earlier_attempt_is_deferred():
    cursor = Mock()
    cursor.fetchall.return_value = []
    cursor.fetchone.side_effect = [
        {'has_previous_run': True, 'has_pending_historical_run': True},
        {'has_previous_run': False, 'has_pending_historical_run': False},
    ]
    repository = _repository_with_cursor(cursor)
    check = {'check_id': uuid4(), 'checks': ['row_count']}
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    start = slot - timedelta(hours=1)

    assert repository.start_run(check, slot, start, slot)['status'] == 'SKIPPED'
    run = repository.start_run(check, slot, start, slot)

    assert run['window_start'] is None
    assert run['window_end'] == slot
    assert _start_run_insert_params(cursor)[3:5] == (None, slot)


@pytest.mark.parametrize('status', ['PASS', 'FAIL', 'ERROR', 'DEFERRED'])
def test_forced_same_slot_rerun_reuses_recorded_historical_bounds(status):
    cursor = Mock()
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    historical_end = slot - timedelta(minutes=5)
    cursor.fetchall.return_value = [{
        "status": status,
        "max_attempt": 1,
        "window_start": None,
        "window_end": historical_end,
    }]
    repository = _repository_with_cursor(cursor)

    run = repository.start_run(
        {"check_id": uuid4()}, slot, slot - timedelta(hours=1), slot, force=True,
    )

    assert run["attempt"] == 2
    assert run["trigger_type"] == "MANUAL"
    assert run["window_start"] is None
    assert run["window_end"] == historical_end
    assert _start_run_insert_params(cursor)[3:5] == (None, historical_end)
    assert not any("AS has_previous_run" in call.args[0] for call in cursor.execute.call_args_list)


def test_forced_rerun_reuses_latest_resolved_bounds_after_unresolved_error():
    cursor = Mock()
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    resolved_start = slot - timedelta(days=7)
    cursor.fetchall.return_value = [
        {'status': 'FAIL', 'max_attempt': 2, 'window_start': resolved_start, 'window_end': slot},
        {'status': 'ERROR', 'max_attempt': 1, 'window_start': None, 'window_end': slot},
    ]
    repository = _repository_with_cursor(cursor)

    run = repository.start_run({'check_id': uuid4()}, slot, slot - timedelta(hours=1), slot, force=True)

    assert run['attempt'] == 3
    assert run['window_start'] == resolved_start
    assert _start_run_insert_params(cursor)[3:5] == (resolved_start, slot)
    assert 'ORDER BY attempt DESC' in cursor.execute.call_args_list[1].args[0]


def test_remediation_of_unresolved_error_reuses_subsequently_resolved_window():
    cursor = Mock()
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    resolved_start = slot - timedelta(days=7)
    cursor.fetchall.return_value = [
        {'status': 'FAIL', 'max_attempt': 2, 'window_start': resolved_start, 'window_end': slot},
        {'status': 'ERROR', 'max_attempt': 1, 'window_start': None, 'window_end': slot},
    ]
    repository = _repository_with_cursor(cursor)
    original = {
        'check_id': uuid4(), 'run_id': uuid4(), 'scheduled_for': slot, 'status': 'ERROR',
        'window_start': None, 'window_end': slot,
    }

    run = repository.start_remediation_run(original, 'repair-reference')

    assert run['attempt'] == 3
    assert run['window_start'] == resolved_start
    assert run['window_end'] == slot
    assert _start_run_insert_params(cursor)[3:5] == (resolved_start, slot)
    assert 'ORDER BY attempt DESC' in cursor.execute.call_args_list[1].args[0]


@pytest.mark.parametrize('window_start', [None, datetime.min.replace(tzinfo=timezone.utc)])
def test_remediation_preserves_unresolved_or_year_one_original_boundary(window_start):
    cursor = Mock()
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    original = {
        'check_id': uuid4(), 'run_id': uuid4(), 'scheduled_for': slot, 'status': 'ERROR',
        'window_start': window_start, 'window_end': slot,
    }
    cursor.fetchall.return_value = [{**original, 'max_attempt': 1}]
    repository = _repository_with_cursor(cursor)

    run = repository.start_remediation_run(original, 'repair-reference')

    assert run['window_start'] == window_start
    assert run['window_end'] == slot
    assert _start_run_insert_params(cursor)[3:5] == (window_start, slot)


@pytest.mark.parametrize('status', ['PASS', 'FAIL', 'ERROR', 'DEFERRED'])
def test_completed_attempt_is_not_retried_within_its_cron_slot(status):
    cursor = Mock()
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    cursor.fetchall.return_value = [{
        "status": status,
        "max_attempt": 1,
        "window_start": None,
        "window_end": slot,
    }]
    repository = _repository_with_cursor(cursor)

    skipped = repository.start_run(
        {"check_id": uuid4()}, slot, slot - timedelta(hours=1), slot,
    )
    assert skipped['status'] == 'SKIPPED'
    assert skipped['slot_status'] == status
    assert skipped['error'] == 'This cron slot has already been attempted'
    assert skipped['window_start'] is None
    assert skipped['window_end'] == slot
    assert not any(
        "INSERT INTO public.dd_run_attempts" in call.args[0]
        for call in cursor.execute.call_args_list
    )


@pytest.mark.parametrize('force', [False, True])
def test_running_attempt_cannot_be_restarted(force):
    cursor = Mock()
    cursor.fetchall.return_value = [{'status': 'RUNNING', 'max_attempt': 1}]
    repository = _repository_with_cursor(cursor)
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)

    skipped = repository.start_run(
        {'check_id': uuid4()}, slot, slot - timedelta(hours=1), slot, force=force,
    )
    assert skipped['status'] == 'SKIPPED'
    assert skipped['slot_status'] == 'RUNNING'
    assert 'already running' in skipped['error']
    assert not any('INSERT INTO public.dd_run_attempts' in call.args[0] for call in cursor.execute.call_args_list)


@pytest.mark.parametrize('status', ['FAIL', 'ERROR'])
@pytest.mark.parametrize('deferred', [False, True])
def test_cron_retry_preserves_failed_window_even_after_a_deferred_attempt(status, deferred):
    cursor = Mock()
    current = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    slot = current - timedelta(hours=1)
    start = None if deferred else slot - timedelta(days=7)
    failed = {
        'status': status, 'max_attempt': 1, 'window_start': start, 'window_end': slot,
        'attempted_at': current - timedelta(minutes=45),
    }
    cursor.fetchall.return_value = ([{**failed, 'status': 'DEFERRED', 'max_attempt': 2}] if deferred else []) + [failed]
    repository = _repository_with_cursor(cursor)

    run = repository.start_run(
        {'check_id': uuid4()}, slot, current - timedelta(hours=1), current, retry_before=current,
    )

    assert run['attempt'] == (3 if deferred else 2)
    assert run['trigger_type'] == 'RETRY'
    assert (run['window_start'], run['window_end']) == (start, slot)
    assert _start_run_insert_params(cursor)[2:5] == (slot, start, slot)


@pytest.mark.parametrize('status', ['PASS', 'RUNNING', 'DEFERRED', 'FAIL', 'ERROR'])
@pytest.mark.parametrize('same_interval', [False, True])
def test_cron_retry_rechecks_status_and_attempt_time_under_lock(status, same_interval):
    cursor = Mock()
    current = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    slot = current - timedelta(hours=1)
    cursor.fetchall.return_value = [{
        'status': status, 'max_attempt': 1, 'window_start': slot - timedelta(hours=1), 'window_end': slot,
        'attempted_at': current if same_interval else current - timedelta(minutes=1),
    }]
    repository = _repository_with_cursor(cursor)
    check_id = uuid4()

    run = repository.start_run({'check_id': check_id}, slot, slot, slot, retry_before=current)

    assert (run.get('status') != 'SKIPPED') == (status in ('FAIL', 'ERROR') and not same_interval)
    assert cursor.execute.call_args_list[0].args == (
        'SELECT pg_advisory_xact_lock(hashtext(%s))', (str(check_id),),
    )
    assert 'GREATEST(started_at, finished_at)' in cursor.execute.call_args_list[1].args[0]


def test_cron_retry_does_not_repeat_the_current_failed_slot():
    cursor = Mock()
    current = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    cursor.fetchall.return_value = [{
        'status': 'FAIL', 'max_attempt': 1, 'attempted_at': current - timedelta(minutes=1),
    }]
    repository = _repository_with_cursor(cursor)

    skipped = repository.start_run({'check_id': uuid4()}, current, current, current, retry_before=current)
    assert skipped['status'] == 'SKIPPED'
    assert skipped['slot_status'] == 'FAIL'
    assert 'no longer eligible' in skipped['error']


def test_cron_retry_uses_latest_effective_status_after_manual_repair():
    cursor = Mock()
    current = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    cursor.fetchall.return_value = [
        {'status': 'PASS', 'max_attempt': 2, 'attempted_at': current - timedelta(minutes=2)},
        {'status': 'ERROR', 'max_attempt': 1, 'attempted_at': current - timedelta(minutes=30)},
    ]
    repository = _repository_with_cursor(cursor)
    slot = current - timedelta(hours=1)

    skipped = repository.start_run({'check_id': uuid4()}, slot, slot, slot, retry_before=current)
    assert skipped['status'] == 'SKIPPED'
    assert skipped['slot_status'] == 'PASS'


def test_retry_candidates_are_bounded_and_prioritize_the_least_recent_attempt():
    cursor = Mock()
    failed = {'run_id': uuid4(), 'status': 'FAIL'}
    cursor.fetchall.return_value = [failed]
    repository = _repository_with_cursor(cursor)
    current = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    check_id = uuid4()

    assert repository.list_retryable_runs(check_id, current, limit=24) == [failed]

    sql, params = cursor.execute.call_args.args
    assert "slots.status IN ('FAIL', 'ERROR')" in sql
    assert 'GREATEST(started_at, finished_at)' in sql
    assert 'NOT attempts.is_running' in sql
    assert 'ORDER BY attempts.attempted_at, slots.scheduled_for' in sql
    assert params == (check_id, current, current, 24)


def test_scheduled_and_remediation_attempts_lock_same_check_before_reading_attempts():
    cursor = Mock()
    slot = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    check_id = uuid4()
    original = {
        'check_id': check_id,
        'run_id': uuid4(),
        'status': 'ERROR',
        'scheduled_for': slot,
        'window_start': slot - timedelta(hours=1),
        'window_end': slot,
    }
    cursor.fetchall.return_value = [{**original, 'max_attempt': 1}]
    repository = _repository_with_cursor(cursor)

    repository.start_run({'check_id': check_id}, slot, original['window_start'], slot, force=True)
    scheduled_lock = cursor.execute.call_args_list[0]
    cursor.reset_mock()
    repository.start_remediation_run(original, 'repair-reference')

    assert cursor.execute.call_args_list[0] == scheduled_lock
    assert scheduled_lock.args == ('SELECT pg_advisory_xact_lock(hashtext(%s))', (str(check_id),))
    assert 'ORDER BY attempt DESC' in cursor.execute.call_args_list[1].args[0]


def test_remediation_cannot_overlap_running_attempt():
    cursor = Mock()
    cursor.fetchall.return_value = [{'status': 'RUNNING', 'max_attempt': 2}]
    repository = _repository_with_cursor(cursor)
    original = {
        'check_id': uuid4(),
        'run_id': uuid4(),
        'status': 'ERROR',
        'scheduled_for': datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
    }

    with pytest.raises(ValueError, match='already running'):
        repository.start_remediation_run(original, 'repair-reference')
    assert not any('INSERT INTO public.dd_run_attempts' in call.args[0] for call in cursor.execute.call_args_list)


def test_historical_start_is_persisted_only_for_its_unresolved_running_attempt():
    cursor = Mock()
    run_id = uuid4()
    window_start = datetime(2026, 7, 20, tzinfo=timezone.utc)
    cursor.fetchone.return_value = {'run_id': run_id}
    repository = _repository_with_cursor(cursor)

    repository.set_run_window_start(run_id, window_start)

    sql, params = cursor.execute.call_args.args
    assert ' '.join(sql.split()) == (
        'UPDATE public.dd_run_attempts SET window_start = %s '
        "WHERE run_id = %s AND status = 'RUNNING' AND window_start IS NULL RETURNING run_id"
    )
    assert params == (window_start, run_id)


def test_historical_start_cannot_overwrite_a_resolved_running_attempt():
    cursor = Mock()
    cursor.fetchone.side_effect = [None, {'status': 'RUNNING'}]
    repository = _repository_with_cursor(cursor)

    with pytest.raises(ValueError, match='Only an unresolved running historical attempt'):
        repository.set_run_window_start(uuid4(), datetime(2026, 7, 20, tzinfo=timezone.utc))


@pytest.mark.parametrize('status', ['PASS', 'FAIL', 'ERROR', 'DEFERRED'])
def test_historical_start_rejects_a_lost_lease(status):
    cursor = Mock()
    cursor.fetchone.side_effect = [None, {'status': status}]
    repository = _repository_with_cursor(cursor)

    with pytest.raises(RunLeaseLostError) as error:
        repository.set_run_window_start(uuid4(), datetime(2026, 7, 20, tzinfo=timezone.utc))

    assert error.value.status == status


@pytest.mark.parametrize('status', ['PASS', 'FAIL', 'ERROR', 'DEFERRED'])
def test_finish_run_rejects_a_lost_lease_before_writing_results_or_coverage(status):
    cursor = Mock()
    cursor.fetchone.side_effect = [None, {'status': status}]
    repository = _repository_with_cursor(cursor)
    repository._record_watermark_transition = Mock()

    with pytest.raises(RunLeaseLostError) as error:
        repository.finish_run(uuid4(), 'PASS', [{'check_type': 'row_count', 'status': 'PASS'}])

    assert error.value.status == status
    repository._record_watermark_transition.assert_not_called()
    assert not any('INSERT' in call.args[0] for call in cursor.execute.call_args_list)


def test_finish_run_updates_coverage_in_same_transaction():
    cursor = Mock()
    repository = _repository_with_cursor(cursor)
    repository._record_watermark_transition = Mock()
    run_id = uuid4()

    repository.finish_run(
        run_id,
        "PASS",
        [{"check_type": "row_count", "status": "PASS", "source_value": 1}],
    )

    repository._record_watermark_transition.assert_called_once()
    assert repository._record_watermark_transition.call_args.args[0] is cursor
    assert repository._record_watermark_transition.call_args.args[1] == run_id
    claim = ' '.join(cursor.execute.call_args_list[0].args[0].split())
    assert "WHERE run_id = %s AND status = 'RUNNING' RETURNING run_id" in claim
    assert 'INSERT INTO public.dd_run_results' in cursor.execute.call_args_list[1].args[0]


def test_deferred_run_is_recorded_without_replacing_any_watermark_blocker():
    cursor = Mock()
    repository = _repository_with_cursor(cursor)
    repository._record_watermark_transition = Mock()
    run_id = uuid4()

    repository.finish_run(run_id, 'DEFERRED', [], error='Neither side has settled history')

    repository._record_watermark_transition.assert_not_called()
    sql, params = cursor.execute.call_args.args
    assert 'UPDATE public.dd_run_attempts' in sql
    assert params[0] == 'DEFERRED'
    assert params[-1] == run_id


def _coverage_state(start, end, *, status="CONTIGUOUS", blocking_run_id=None):
    return {
        "verified_start": start,
        "verified_end": end,
        "furthest_observed_end": end,
        "verified_status": status,
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
        DataDiffRepository, "_watermark_state_for_update", return_value=previous
    ), patch.object(
        DataDiffRepository, "_run_slot_state_for_update", return_value=None
    ), patch.object(
        DataDiffRepository, "_has_later_run_slot", return_value=False
    ), patch.object(
        DataDiffRepository, "_upsert_run_slot_state", return_value=True
    ), patch.object(
        DataDiffRepository, "_recalculate_watermark"
    ) as recalculate, patch.object(
        DataDiffRepository, "_upsert_watermark_state"
    ) as upsert_state, patch.object(
        DataDiffRepository, "_insert_watermark_event"
    ):
        DataDiffRepository._record_watermark_transition(
            cursor, attempt["run_id"], attempt["window_end"]
        )

    recalculate.assert_not_called()
    coverage = upsert_state.call_args.args[2]
    assert coverage["verified_end"] == attempt["window_end"]
    assert coverage["verified_status"] == "CONTIGUOUS"


def test_new_slot_after_unresolved_error_recalculates_and_preserves_blocker():
    cursor = Mock()
    end = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    unresolved = _terminal_attempt(None, end, status='ERROR')
    attempt = _terminal_attempt(end, end + timedelta(hours=1))
    attempt['check_id'] = unresolved['check_id']
    cursor.fetchone.return_value = attempt
    cursor.fetchall.return_value = [unresolved, attempt]
    previous = _coverage_state(None, None, status='BLOCKED', blocking_run_id=unresolved['run_id'])
    previous['furthest_observed_end'] = end

    with patch.object(
        DataDiffRepository, '_watermark_state_for_update', return_value=previous,
    ), patch.object(
        DataDiffRepository, '_run_slot_state_for_update', return_value=None,
    ), patch.object(
        DataDiffRepository, '_has_later_run_slot', return_value=False,
    ), patch.object(
        DataDiffRepository, '_upsert_run_slot_state', return_value=True,
    ), patch.object(
        DataDiffRepository, '_upsert_watermark_state',
    ) as upsert_state, patch.object(
        DataDiffRepository, '_insert_watermark_event',
    ) as insert_event:
        DataDiffRepository._record_watermark_transition(cursor, attempt['run_id'], attempt['window_end'])

    coverage = upsert_state.call_args.args[2]
    assert coverage['verified_start'] is None
    assert coverage['verified_end'] is None
    assert coverage['furthest_observed_end'] == attempt['window_end']
    assert coverage['blocking_run_id'] == unresolved['run_id']
    assert insert_event.call_args.args[4] == 'BLOCK'
    assert 'FROM public.dd_run_slot_state' in cursor.execute.call_args.args[0]


def test_error_before_historical_resolution_records_null_coverage_and_blocker():
    cursor = Mock()
    end = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    attempt = _terminal_attempt(None, end, status='ERROR')
    cursor.fetchone.return_value = attempt
    cursor.fetchall.return_value = [attempt]

    with patch.object(
        DataDiffRepository, '_watermark_state_for_update', return_value=None,
    ), patch.object(
        DataDiffRepository, '_run_slot_state_for_update', return_value=None,
    ), patch.object(
        DataDiffRepository, '_has_later_run_slot', return_value=False,
    ), patch.object(
        DataDiffRepository, '_upsert_run_slot_state', return_value=True,
    ), patch.object(
        DataDiffRepository, '_upsert_watermark_state',
    ) as upsert_state, patch.object(
        DataDiffRepository, '_insert_watermark_event',
    ) as insert_event:
        DataDiffRepository._record_watermark_transition(cursor, attempt['run_id'], end)

    coverage = upsert_state.call_args.args[2]
    assert coverage['verified_start'] is None
    assert coverage['verified_end'] is None
    assert coverage['furthest_observed_end'] == end
    assert coverage['verified_status'] == 'BLOCKED'
    assert coverage['blocking_run_id'] == attempt['run_id']
    assert insert_event.call_args.args[3] == coverage
    assert insert_event.call_args.args[4] == 'INITIALIZE'


def test_replacement_attempt_recalculates_from_effective_slots():
    cursor = Mock()
    start = datetime(2026, 7, 22, 10, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    attempt = _terminal_attempt(start, end, status="FAIL", attempt=2)
    cursor.fetchone.return_value = attempt
    previous = _coverage_state(start, end)
    recalculated = {
        **previous,
        "verified_end": start,
        "verified_status": "BLOCKED",
        "blocking_run_id": attempt["run_id"],
        "reason": "replacement failed",
    }

    with patch.object(
        DataDiffRepository, "_watermark_state_for_update", return_value=previous
    ), patch.object(
        DataDiffRepository,
        "_run_slot_state_for_update",
        return_value={"run_id": uuid4(), "attempt": 1},
    ), patch.object(
        DataDiffRepository, "_has_later_run_slot", return_value=False
    ), patch.object(
        DataDiffRepository, "_upsert_run_slot_state", return_value=True
    ), patch.object(
        DataDiffRepository,
        "_recalculate_watermark",
        return_value=recalculated,
    ) as recalculate, patch.object(
        DataDiffRepository, "_upsert_watermark_state"
    ), patch.object(
        DataDiffRepository, "_insert_watermark_event"
    ):
        DataDiffRepository._record_watermark_transition(
            cursor, attempt["run_id"], end
        )

    recalculate.assert_called_once_with(
        cursor,
        attempt["check_id"],
        data_checks_enabled=True,
    )


@pytest.mark.parametrize("status", ["PASS", "FAIL", "ERROR"])
def test_rolling_slot_before_historical_start_recalculates_coverage(status):
    cursor = Mock()
    end = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    historical_start = end - timedelta(minutes=30)
    baseline = _terminal_attempt(historical_start, end)
    next_end = end + timedelta(hours=1)
    rolling_start = next_end - timedelta(days=1)
    attempt = _terminal_attempt(rolling_start, next_end, status=status)
    attempt["check_id"] = baseline["check_id"]
    cursor.fetchone.return_value = attempt
    cursor.fetchall.return_value = [baseline, attempt]
    previous = _coverage_state(historical_start, end)

    with patch.object(
        DataDiffRepository, "_watermark_state_for_update", return_value=previous
    ), patch.object(
        DataDiffRepository, "_run_slot_state_for_update", return_value=None
    ), patch.object(
        DataDiffRepository, "_has_later_run_slot", return_value=False
    ), patch.object(
        DataDiffRepository, "_upsert_run_slot_state", return_value=True
    ), patch.object(
        DataDiffRepository, "_upsert_watermark_state"
    ) as upsert_state, patch.object(
        DataDiffRepository, "_insert_watermark_event"
    ):
        DataDiffRepository._record_watermark_transition(
            cursor, attempt["run_id"], next_end
        )

    coverage = upsert_state.call_args.args[2]
    assert coverage["verified_start"] == rolling_start
    assert coverage["verified_end"] == (next_end if status == "PASS" else rolling_start)
    assert coverage["verified_status"] == ("CONTIGUOUS" if status == "PASS" else "BLOCKED")
    assert coverage["blocking_run_id"] == (None if status == "PASS" else attempt["run_id"])
    assert "FROM public.dd_run_slot_state" in cursor.execute.call_args.args[0]


def test_out_of_order_new_slot_recalculates_from_effective_slots():
    cursor = Mock()
    start = datetime(2026, 7, 22, 10, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    attempt = _terminal_attempt(start, end)
    cursor.fetchone.return_value = attempt
    previous = _coverage_state(start, end + timedelta(hours=2))

    with patch.object(
        DataDiffRepository, "_watermark_state_for_update", return_value=previous
    ), patch.object(
        DataDiffRepository, "_run_slot_state_for_update", return_value=None
    ), patch.object(
        DataDiffRepository, "_has_later_run_slot", return_value=True
    ), patch.object(
        DataDiffRepository, "_upsert_run_slot_state", return_value=True
    ), patch.object(
        DataDiffRepository,
        "_recalculate_watermark",
        return_value=previous,
    ) as recalculate, patch.object(
        DataDiffRepository, "_upsert_watermark_state"
    ), patch.object(
        DataDiffRepository, "_insert_watermark_event"
    ):
        DataDiffRepository._record_watermark_transition(
            cursor, attempt["run_id"], end
        )

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

    coverage = DataDiffRepository._recalculate_watermark(
        cursor,
        uuid4(),
        data_checks_enabled=True,
    )

    sql = " ".join(cursor.execute.call_args.args[0].split())
    assert "FROM public.dd_run_slot_state" in sql
    assert "FROM public.dd_run_attempts" not in sql
    assert coverage["verified_status"] == "CONTIGUOUS"


def test_current_watermark_uses_renamed_state_columns():
    cursor = Mock()
    start = datetime(2026, 7, 22, 10, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    definition = {"check_id": uuid4(), "run_id": uuid4()}
    coverage = _coverage_state(start, end)

    DataDiffRepository._upsert_watermark_state(
        cursor,
        definition,
        coverage,
        "ADVANCE",
        5,
        end,
    )

    sql = " ".join(cursor.execute.call_args.args[0].split())
    assert "verified_start, verified_end, furthest_observed_end" in sql
    assert "verified_status, blocking_run_id, last_evaluated_run_id" in sql
    assert "coverage_start" not in sql
    assert "verified_through" not in sql
    assert "max_observed_end" not in sql
    assert "coverage_status" not in sql
    assert "evaluated_run_id" not in sql.replace("last_evaluated_run_id", "")


def test_watermark_event_uses_consistent_verified_column_names():
    cursor = Mock()
    start = datetime(2026, 7, 22, 10, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    definition = {"check_id": uuid4(), "run_id": uuid4()}
    previous = _coverage_state(start, start)
    coverage = _coverage_state(start, end)

    DataDiffRepository._insert_watermark_event(
        cursor,
        definition,
        previous,
        coverage,
        "ADVANCE",
        end,
    )

    sql, params = cursor.execute.call_args.args
    sql = " ".join(sql.split())
    assert "verified_start, previous_verified_end, verified_end" in sql
    assert "furthest_observed_end, verified_status" in sql
    assert "coverage_start" not in sql
    assert "verified_through" not in sql
    assert "max_observed_end" not in sql
    assert "coverage_status" not in sql
    assert params[4:9] == (
        coverage["verified_start"],
        previous["verified_end"],
        coverage["verified_end"],
        coverage["furthest_observed_end"],
        coverage["verified_status"],
    )


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
