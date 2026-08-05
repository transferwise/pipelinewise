import signal
import subprocess
import sys

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest

from pipelinewise.data_diff.runner import (
    _stale_run_age,
    check_window,
    due_slots,
    rerun_failed_check,
    run_due_checks,
    scheduled_slot,
)


def _check():
    return {
        "check_id": uuid4(),
        "full_check_name": "target/tap/public/payments",
        "enabled": True,
        "target_id": "target",
        "tap_id": "tap",
        "source_database": "source",
        "target_database": "target",
        "checks": ["row_count"],
        "frequency": "0 * * * *",
        "window_start_seconds": 3600,
        "window_end_seconds": 0,
    }


class FakeBackend:
    def __init__(self, check, start=True, latest=None):
        self.check = check
        self.start = start
        self.finished = []
        self.preflights = []
        self.latest = latest
        self.expired = []

    def list_checks(self, **_filters):
        return [self.check]

    def expire_stale_running_attempts(self, check_id, stale_before):
        self.expired.append((check_id, stale_before))
        return 0

    def start_run(self, *_args, **_kwargs):
        return {"run_id": uuid4(), "attempt": 1, "trigger": "SCHEDULED"} if self.start else None

    def latest_scheduled_for(self, _check_id):
        return self.latest

    def record_preflight(self, _check_id, preflight):
        self.preflights.append(preflight)
        return uuid4()

    def finish_run(self, *args, **kwargs):
        self.finished.append((args, kwargs))


def _connection_configs(_check):
    return {"dbname": "source"}, {"dbname": "target"}


def test_slot_and_window_are_utc_half_open_boundaries():
    now = datetime(2026, 7, 22, 13, 47, 59, tzinfo=timezone.utc)
    check = _check()

    assert scheduled_slot(now, "0 * * * *") == datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    assert check_window(check, now) == (
        datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
        datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
        datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
    )


PASS_PREFLIGHT = {
    "status": "PASS",
    "query_fingerprint": "a" * 64,
    "index_metadata": [],
    "findings": [],
    "table_rows": 42,
    "row_limit": 100_000,
    "has_leading_index": True,
}


def _fake_run_check(preflight, results, status):
    """Stand in for run_check, honouring its on_preflight contract."""

    def run(*_args, on_preflight=None, **_kwargs):
        if on_preflight is not None:
            on_preflight(preflight)
        return preflight, results, status

    return run


@patch("pipelinewise.data_diff.runner.run_check")
def test_run_persists_preflight_and_results(mock_run):
    mock_run.side_effect = _fake_run_check(PASS_PREFLIGHT, [{"check_type": "row_count", "status": "PASS"}], "PASS")
    backend = FakeBackend(_check())

    summaries = run_due_checks(
        backend,
        _connection_configs,
        now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )

    assert summaries[0]["status"] == "PASS"
    assert backend.preflights[0]["status"] == "PASS"
    assert backend.finished[0][0][1] == "PASS"


def test_completed_slot_is_reported_as_skipped():
    summaries = run_due_checks(
        FakeBackend(_check(), start=False),
        _connection_configs,
        now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )
    assert summaries[0]["status"] == "SKIPPED"


def test_due_slots_backfills_oldest_missing_windows_in_order():
    slots = due_slots(
        _check(),
        datetime(2026, 7, 22, 13, 15, tzinfo=timezone.utc),
        datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
    )

    assert slots == [
        datetime(2026, 7, 22, 11, tzinfo=timezone.utc),
        datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
        datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
    ]


@patch("pipelinewise.data_diff.runner.run_check")
def test_remediation_reuses_exact_failed_definition_and_window(mock_run):
    mock_run.side_effect = _fake_run_check(
        {**PASS_PREFLIGHT, "query_fingerprint": "b" * 64},
        [{"check_type": "row_count", "status": "PASS"}],
        "PASS",
    )
    check = _check()
    original = {
        "run_id": uuid4(),
        "dd_check_id": check["check_id"],
        "scheduled_for": datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
        "window_start": datetime(2026, 7, 22, 6, tzinfo=timezone.utc),
        "window_end": datetime(2026, 7, 22, 7, tzinfo=timezone.utc),
        "status": "FAIL",
    }
    backend = FakeBackend(check)
    backend.get_run = lambda _run_id: original
    backend.get_check_version = lambda _version_id: check
    backend.start_remediation_run = lambda _original, _reference: {
        "run_id": uuid4(),
        "attempt": 2,
        "trigger": "REMEDIATION",
    }

    summary = rerun_failed_check(
        backend,
        _connection_configs,
        original["run_id"],
        "AP-1234",
    )

    assert summary["status"] == "PASS"
    assert summary["attempt"] == 2
    assert summary["window_start"] == original["window_start"]
    assert summary["window_end"] == original["window_end"]
    (call_args, call_kwargs) = mock_run.call_args
    assert call_args == (
        check,
        {"dbname": "source"},
        {"dbname": "target"},
        original["window_start"],
        original["window_end"],
    )
    assert callable(call_kwargs["on_preflight"])


class MultiCheckBackend(FakeBackend):
    """Serve several definitions so one broken check can be isolated."""

    def __init__(self, checks):
        super().__init__(checks[0], start=False)
        self.checks = checks

    def list_checks(self, **_filters):
        return self.checks


def test_an_unschedulable_check_does_not_abort_the_batch():
    # A bad cron fails before a run row exists, so it cannot be recorded against
    # one. It must not stop later checks, or their failures go unreported.
    first, broken, last = _check(), _check(), _check()
    first["full_check_name"] = "target/tap/public/first"
    broken["full_check_name"] = "target/tap/public/broken"
    broken["frequency"] = "1h"
    last["full_check_name"] = "target/tap/public/last"

    summaries = run_due_checks(
        MultiCheckBackend([first, broken, last]),
        _connection_configs,
        now=datetime(2026, 7, 30, 12, tzinfo=timezone.utc),
    )

    by_name = {summary["check"]["full_check_name"]: summary["status"] for summary in summaries}
    assert by_name["target/tap/public/first"] == "SKIPPED"
    assert by_name["target/tap/public/last"] == "SKIPPED"
    assert by_name["target/tap/public/broken"] == "ERROR"

    failure = next(s for s in summaries if s["status"] == "ERROR")
    assert failure["window_start"] is None
    assert "columns" in failure["error"]


@patch("pipelinewise.data_diff.runner.run_check")
def test_an_interrupted_run_is_recorded_as_terminal_before_propagating(mock_run):
    # KeyboardInterrupt and SystemExit are BaseException, so a plain
    # "except Exception" never sees them and the row would stay RUNNING forever.
    mock_run.side_effect = KeyboardInterrupt()
    check = _check()
    backend = FakeBackend(check)

    with pytest.raises(KeyboardInterrupt):
        run_due_checks(
            backend,
            _connection_configs,
            now=datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
        )

    (args, kwargs) = backend.finished[0]
    assert args[1] == "ERROR"
    assert "KeyboardInterrupt" in kwargs["error"]


@patch("pipelinewise.data_diff.runner.run_check")
def test_a_terminating_signal_is_also_recorded_before_exit(mock_run):
    mock_run.side_effect = SystemExit(1)
    backend = FakeBackend(_check())

    with pytest.raises(SystemExit):
        run_due_checks(
            backend,
            _connection_configs,
            now=datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
        )

    assert backend.finished[0][0][1] == "ERROR"


def test_abandoned_attempts_are_expired_before_the_latest_slot_is_read():
    # A worker killed outright cannot mark its own row terminal, and a slot holding
    # RUNNING refuses every later attempt, including --force.
    check = _check()
    backend = FakeBackend(check, start=False)
    now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)

    run_due_checks(backend, _connection_configs, now=now)

    assert len(backend.expired) == 1
    check_id, stale_before = backend.expired[0]
    assert check_id == check["check_id"]
    # At least the grace period back, so a live run is never expired.
    assert stale_before <= now - timedelta(minutes=30)


def test_stale_age_scales_with_the_checks_own_query_budget():
    # statement_timeout has no upper bound, so a generous one must not have its
    # live runs expired.
    quick = _stale_run_age({"statement_timeout_seconds": 60})
    generous = _stale_run_age({"statement_timeout_seconds": 3600})

    assert quick == timedelta(minutes=34)
    assert generous > quick
    assert _stale_run_age({}) == timedelta(minutes=30)


SIGTERM_SCRIPT = """
import os, signal, sys, time
sys.path.insert(0, {repo!r})
from pipelinewise.data_diff.runner import execute_started_run
from datetime import datetime, timezone

RECORDED = {{}}

class Backend:
    def record_preflight(self, _check_id, _preflight):
        return "preflight-id"
    def finish_run(self, run_id, status, _results, preflight_id=None, error=None):
        with open({outfile!r}, "w") as handle:
            handle.write(f"{{status}}|{{error}}")

def loader(_check):
    # Signal ourselves while the check is 'running', as a scheduler kill would.
    os.kill(os.getpid(), signal.SIGTERM)
    time.sleep(10)
    return {{}}, {{}}

execute_started_run(
    Backend(), loader,
    {{"check_id": "c", "full_check_name": "t/p/s/tbl", "statement_timeout_seconds": 60}},
    {{"run_id": "r", "attempt": 1, "trigger": "SCHEDULED"}},
    datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
    datetime(2026, 7, 22, 11, tzinfo=timezone.utc),
    datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
)
"""


def test_a_real_sigterm_marks_the_run_terminal_before_the_process_dies(tmp_path):
    """A genuine signal, not an injected SystemExit.

    Default SIGTERM kills the interpreter without raising, so only an installed
    handler can mark the attempt terminal. Injecting SystemExit would pass even
    with no handler at all, which is why this runs a real subprocess.
    """
    outfile = tmp_path / "finished.txt"
    repo = str(Path(__file__).resolve().parents[3])
    script = SIGTERM_SCRIPT.format(repo=repo, outfile=str(outfile))

    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    # Killed by SIGTERM, exactly as the sender intended.
    assert completed.returncode == -signal.SIGTERM, completed.stderr
    # And the attempt was recorded terminal first, so its slot stays retryable.
    assert outfile.exists(), f"finish_run never ran: {completed.stderr}"
    status, error = outfile.read_text().split("|", 1)
    assert status == "ERROR"
    assert "SIGTERM" in error


def test_the_sweep_is_not_scoped_to_a_slot_or_trigger():
    """It must reach historical and remediation attempts.

    A RUNNING row makes its own slot look observed, so the scheduler advances past
    it. Scoping the sweep to due slots would therefore never reach the row that
    caused the skip, and remediation attempts are swept by nothing else.
    """
    check = _check()
    backend = FakeBackend(check, start=False)

    run_due_checks(
        backend,
        _connection_configs,
        now=datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
    )

    # One call per check, taking only the check and a cutoff: no slot, no trigger.
    assert len(backend.expired) == 1
    assert len(backend.expired[0]) == 2


def test_the_sweep_runs_before_the_scheduler_reads_the_latest_slot():
    order = []
    check = _check()

    class OrderedBackend(FakeBackend):
        def expire_stale_running_attempts(self, check_id, stale_before):
            order.append("sweep")
            return 1

        def latest_scheduled_for(self, check_id):
            order.append("latest")

    run_due_checks(
        OrderedBackend(check, start=False),
        _connection_configs,
        now=datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
    )

    assert order == ["sweep", "latest"]
