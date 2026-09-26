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
from pipelinewise.data_diff.engine import HistoricalWindowNotReady
from pipelinewise.data_diff.repository import RunLeaseLostError


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
        self.window_starts = []

    def list_checks(self, **_filters):
        return [self.check]

    def expire_stale_running_attempts(self, check_id, stale_before):
        self.expired.append((check_id, stale_before))
        return 0

    def start_run(self, *_args, **_kwargs):
        return {
            "run_id": uuid4(),
            "attempt": 1,
            "trigger_type": "SCHEDULED",
        } if self.start else {
            'status': 'SKIPPED', 'slot_status': 'PASS', 'error': 'This cron slot has already been attempted',
            'window_start': None, 'window_end': None,
        }

    def latest_scheduled_for(self, _check_id):
        return self.latest

    def list_retryable_runs(self, _check_id, _retry_before, *, limit):
        return []

    def record_preflight(self, _check_id, preflight):
        self.preflights.append(preflight)
        return uuid4()

    def finish_run(self, *args, **kwargs):
        self.finished.append((args, kwargs))

    def set_run_window_start(self, run_id, start):
        self.window_starts.append((run_id, start))


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
    mock_run.side_effect = _fake_run_check(
        PASS_PREFLIGHT, [{"check_type": "row_count", "status": "PASS"}], "PASS"
    )
    backend = FakeBackend(_check())

    summaries = run_due_checks(
        backend,
        _connection_configs,
        now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )

    assert summaries[0]["status"] == "PASS"
    assert backend.preflights[0]["status"] == "PASS"
    assert backend.finished[0][0][1] == "PASS"


@patch("pipelinewise.data_diff.runner.run_check")
def test_runner_uses_the_window_persisted_by_start_run(mock_run):
    mock_run.side_effect = _fake_run_check(
        {**PASS_PREFLIGHT, "status": "BLOCKED", "findings": ["No timestamp index"]}, [], None,
    )

    class HistoricalBackend(FakeBackend):
        def start_run(self, *_args, **_kwargs):
            return {
                **super().start_run(*_args, **_kwargs),
                "window_start": None,
                "window_end": datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
            }

    summary = run_due_checks(
        HistoricalBackend(_check()), _connection_configs,
        now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )[0]

    assert summary["window_start"] is None
    assert summary["window_end"] == datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    assert mock_run.call_args.args[3:5] == (
        None, summary["window_end"],
    )


@pytest.mark.parametrize('failure', ['connection', 'preflight', 'minimum', 'unsupported'])
@patch('pipelinewise.data_diff.runner.run_check')
def test_historical_failure_before_resolution_keeps_unknown_start(mock_run, failure):
    class HistoricalBackend(FakeBackend):
        def start_run(self, *_args, **_kwargs):
            return {**super().start_run(*_args, **_kwargs), 'window_start': None}

    def load(check):
        if failure == 'connection':
            raise RuntimeError('Source connection failed')
        return _connection_configs(check)

    def execute(*_args, on_preflight, **_kwargs):
        if failure == 'preflight':
            preflight = {**PASS_PREFLIGHT, 'status': 'BLOCKED', 'findings': ['No timestamp index']}
            on_preflight(preflight)
            return preflight, [], None
        on_preflight(PASS_PREFLIGHT)
        if failure == 'minimum':
            raise RuntimeError('Minimum timestamp query timed out')
        return PASS_PREFLIGHT, [{'check_type': 'row_checksum', 'status': 'ERROR',
                                 'error': 'Unsupported comparison type'}], 'ERROR'

    mock_run.side_effect = execute
    backend = HistoricalBackend(_check())
    summary, = run_due_checks(
        backend, load, now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )

    assert summary['status'] == 'ERROR'
    assert summary['window_start'] is None
    assert summary['error']
    assert backend.window_starts == []
    assert backend.finished[0][0][1] == 'ERROR'


@patch('pipelinewise.data_diff.runner.run_check')
def test_both_empty_historical_run_is_deferred_without_coverage(mock_run):
    class HistoricalBackend(FakeBackend):
        def start_run(self, *_args, **_kwargs):
            return {**super().start_run(*_args, **_kwargs), 'window_start': None}

    def execute(*_args, on_preflight, **_kwargs):
        on_preflight(PASS_PREFLIGHT)
        raise HistoricalWindowNotReady('Neither source nor target has settled timestamps')

    mock_run.side_effect = execute
    backend = HistoricalBackend(_check())
    summary, = run_due_checks(
        backend, _connection_configs, now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )

    assert summary['status'] == 'DEFERRED'
    assert summary['window_start'] is None
    assert backend.window_starts == []
    assert backend.finished[0][0][1:3] == ('DEFERRED', [])
    assert backend.finished[0][1]['error'] == summary['error']


@pytest.mark.parametrize("query_fails", [False, True])
@patch("pipelinewise.data_diff.runner.run_check")
def test_resolved_historical_start_is_saved_and_reported_even_when_query_fails(mock_run, query_fails):
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    backend = FakeBackend(_check())

    def execute(*_args, on_preflight, on_window_start):
        on_preflight(PASS_PREFLIGHT)
        on_window_start(start)
        assert backend.window_starts[0][1] == start
        if query_fails:
            raise RuntimeError("Statement timed out")
        return PASS_PREFLIGHT, [{"check_type": "row_count", "status": "PASS"}], "PASS"

    mock_run.side_effect = execute
    summary, = run_due_checks(
        backend, _connection_configs, now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )

    assert summary["status"] == ("ERROR" if query_fails else "PASS")
    assert summary["window_start"] == start
    assert backend.window_starts == [(summary["run_id"], start)]


@patch("pipelinewise.data_diff.runner.run_check")
def test_run_summary_includes_result_errors_without_metric_values(mock_run):
    reason = (
        "row_checksum column 'status' has incompatible source and target types "
        "(missing, missing)"
    )
    results = [
        {
            "check_type": "row_count", "status": "FAIL",
            "source_value": 100, "target_value": 99, "error": None,
        },
        {"check_type": "row_checksum", "status": "ERROR", "error": reason},
    ]
    mock_run.side_effect = _fake_run_check(PASS_PREFLIGHT, results, "ERROR")
    backend = FakeBackend(_check())

    summary = run_due_checks(
        backend,
        _connection_configs,
        now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )[0]

    assert summary["error"] == f"row_count FAIL; {reason}"
    assert summary["results"] == results
    assert backend.finished[0][0][1:3] == ("ERROR", results)


@patch("pipelinewise.data_diff.runner.run_check")
def test_run_summary_includes_blocked_preflight_reason(mock_run):
    preflight = {
        **PASS_PREFLIGHT,
        "status": "BLOCKED",
        "findings": ["No usable source timestamp index"],
    }
    mock_run.side_effect = _fake_run_check(preflight, [], None)
    backend = FakeBackend(_check())

    summary = run_due_checks(
        backend,
        _connection_configs,
        now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )[0]

    assert summary["status"] == "ERROR"
    assert summary["error"] == "No usable source timestamp index"
    assert backend.finished[0][1]["error"] == summary["error"]


def test_completed_slot_is_reported_as_skipped():
    summaries = run_due_checks(
        FakeBackend(_check(), start=False),
        _connection_configs,
        now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc),
    )
    assert summaries[0]["status"] == "SKIPPED"
    assert summaries[0]["window_start"] is None
    assert summaries[0]["window_end"] is None
    assert summaries[0]['slot_status'] == 'PASS'
    assert summaries[0]['error'] == 'This cron slot has already been attempted'


@pytest.mark.parametrize('retry_status', ['PASS', 'FAIL', 'ERROR'])
@patch('pipelinewise.data_diff.runner.run_check')
def test_failed_window_retry_and_new_window_both_run(mock_run, retry_status):
    current = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    original = {
        'scheduled_for': current - timedelta(hours=1),
        'window_start': current - timedelta(days=7),
        'window_end': current - timedelta(hours=1),
    }

    class RetryBackend(FakeBackend):
        def list_retryable_runs(self, check_id, retry_before, *, limit):
            assert check_id == self.check['check_id']
            assert retry_before == current
            assert limit == 24
            return [original]

        def start_run(self, check, slot, start, end, **kwargs):
            run = super().start_run()
            if kwargs.get('retry_before') is not None:
                assert slot == original['scheduled_for']
                return {**run, 'attempt': 2, 'trigger_type': 'RETRY', 'window_start': start, 'window_end': end}
            return run

    mock_run.side_effect = [
        (PASS_PREFLIGHT, [{'check_type': 'row_count', 'status': retry_status}], retry_status),
        (PASS_PREFLIGHT, [{'check_type': 'row_count', 'status': 'PASS'}], 'PASS'),
    ]
    backend = RetryBackend(_check(), latest=original['scheduled_for'])
    summaries = run_due_checks(backend, _connection_configs, now=current + timedelta(minutes=1))

    assert [summary['status'] for summary in summaries] == [retry_status, 'PASS']
    assert [summary['trigger_type'] for summary in summaries] == ['RETRY', 'SCHEDULED']
    assert mock_run.call_args_list[0].args[3:5] == (original['window_start'], original['window_end'])
    assert mock_run.call_args_list[1].args[3:5] == (current - timedelta(hours=1), current)
    assert len(backend.finished) == 2


@patch('pipelinewise.data_diff.runner.run_check')
def test_retry_candidate_resolved_by_another_worker_is_skipped(mock_run):
    current = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)

    class ResolvedBackend(FakeBackend):
        def list_retryable_runs(self, _check_id, _retry_before, *, limit):
            return [{'scheduled_for': current - timedelta(hours=1), 'window_start': None, 'window_end': current}]

    summaries = run_due_checks(
        ResolvedBackend(_check(), start=False, latest=current), _connection_configs,
        now=current + timedelta(minutes=1),
    )

    assert [summary['status'] for summary in summaries] == ['SKIPPED', 'SKIPPED']
    assert all(summary['slot_status'] == 'PASS' for summary in summaries)
    mock_run.assert_not_called()


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


@pytest.mark.parametrize("previously_resolved", [False, True])
@patch("pipelinewise.data_diff.runner.run_check")
def test_remediation_reuses_exact_failed_definition_and_window(mock_run, previously_resolved):
    mock_run.side_effect = _fake_run_check(
        {**PASS_PREFLIGHT, "query_fingerprint": "b" * 64},
        [{"check_type": "row_count", "status": "PASS"}],
        "PASS",
    )
    check = _check()
    original = {
        "run_id": uuid4(),
        "check_id": check["check_id"],
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
        "trigger_type": "REMEDIATION",
        **({"window_start": original["window_start"] + timedelta(minutes=10)} if previously_resolved else {}),
    }

    summary = rerun_failed_check(
        backend,
        _connection_configs,
        original["run_id"],
        "AP-1234",
    )

    assert summary["status"] == "PASS"
    assert summary["attempt"] == 2
    expected_start = original["window_start"]
    if previously_resolved:
        expected_start += timedelta(minutes=10)
    assert summary["window_start"] == expected_start
    assert summary["window_end"] == original["window_end"]
    (call_args, call_kwargs) = mock_run.call_args
    assert call_args == (
        check,
        {"dbname": "source"},
        {"dbname": "target"},
        expected_start,
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

    by_name = {
        summary["check"]["full_check_name"]: summary["status"] for summary in summaries
    }
    assert by_name["target/tap/public/first"] == "SKIPPED"
    assert by_name["target/tap/public/last"] == "SKIPPED"
    assert by_name["target/tap/public/broken"] == "ERROR"

    failure = next(s for s in summaries if s["status"] == "ERROR")
    assert failure["window_start"] is None
    assert "columns" in failure["error"]


@pytest.mark.parametrize(
    'operation', ['expire_stale_running_attempts', 'latest_scheduled_for', 'start_run', 'finish_run'],
)
@patch('pipelinewise.data_diff.runner.run_check')
def test_repository_failure_is_reported_and_later_checks_continue(mock_run, operation):
    first, broken, last = _check(), _check(), _check()
    backend = FakeBackend(first)
    backend.list_checks = lambda **_filters: [first, broken, last]
    original = getattr(backend, operation)
    calls = []

    def fail_middle(*args, **kwargs):
        calls.append(args)
        if len(calls) == 2 or (operation == 'finish_run' and len(calls) == 3):
            raise RuntimeError('Backend write failed')
        return original(*args, **kwargs)

    setattr(backend, operation, fail_middle)
    mock_run.side_effect = _fake_run_check(PASS_PREFLIGHT, [{'check_type': 'row_count', 'status': 'PASS'}], 'PASS')
    summaries = run_due_checks(backend, _connection_configs, now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc))

    assert [summary['check']['check_id'] for summary in summaries] == [
        first['check_id'], broken['check_id'], last['check_id'],
    ]
    assert [summary['status'] for summary in summaries] == ['PASS', 'ERROR', 'PASS']
    assert summaries[1]['error'] == 'Backend write failed'


@patch('pipelinewise.data_diff.runner.run_check')
def test_retry_failure_preserves_completed_summaries_and_later_checks(mock_run):
    current = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    first, last = _check(), _check()
    backend = FakeBackend(first, latest=current)
    backend.list_checks = lambda **_filters: [first, last]
    backend.list_retryable_runs = lambda check_id, *_args, **_kwargs: [
        {'scheduled_for': current - timedelta(hours=hours), 'window_start': None, 'window_end': current}
        for hours in (2, 1)
    ] if check_id == first['check_id'] else []
    original_start = backend.start_run

    def start(check, slot, start, end, **kwargs):
        if check['check_id'] == first['check_id'] and slot == current - timedelta(hours=1):
            raise RuntimeError('Could not start second retry')
        return {**original_start(), 'window_start': start, 'window_end': end}

    backend.start_run = start
    mock_run.side_effect = _fake_run_check(PASS_PREFLIGHT, [{'check_type': 'row_count', 'status': 'FAIL'}], 'FAIL')
    summaries = run_due_checks(backend, _connection_configs, now=current + timedelta(minutes=1))

    assert [summary['status'] for summary in summaries] == ['FAIL', 'ERROR', 'FAIL']
    assert summaries[0]['scheduled_for'] == current - timedelta(hours=2)
    assert summaries[1]['error'] == 'Could not start second retry'
    assert summaries[2]['check']['check_id'] == last['check_id']


@pytest.mark.parametrize('outcome', ['PASS', 'ERROR', 'DEFERRED'])
@patch('pipelinewise.data_diff.runner.run_check')
def test_lease_loss_discards_late_outcome_without_finishing_again(mock_run, outcome):
    backend = FakeBackend(_check())
    calls = []

    def finish(run_id, *_args, **_kwargs):
        calls.append(run_id)
        raise RunLeaseLostError(run_id, 'ERROR')

    backend.finish_run = finish
    if outcome == 'DEFERRED':
        mock_run.side_effect = HistoricalWindowNotReady('Both sides empty')
    elif outcome == 'ERROR':
        mock_run.side_effect = RuntimeError('Source unavailable')
    else:
        mock_run.side_effect = _fake_run_check(PASS_PREFLIGHT, [{'check_type': 'row_count', 'status': 'PASS'}], 'PASS')
    summary, = run_due_checks(backend, _connection_configs, now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc))

    assert summary['status'] == 'SKIPPED'
    assert summary['slot_status'] == 'ERROR'
    assert 'Late results were discarded' in summary['error']
    assert calls == [summary['run_id']]


@pytest.mark.parametrize('interruption', [KeyboardInterrupt, SystemExit])
@patch('pipelinewise.data_diff.runner.run_check')
def test_lease_loss_does_not_swallow_interruption(mock_run, interruption):
    backend = FakeBackend(_check())

    def finish(run_id, *_args, **_kwargs):
        raise RunLeaseLostError(run_id, 'ERROR')

    backend.finish_run = finish
    mock_run.side_effect = interruption()

    with pytest.raises(interruption):
        run_due_checks(backend, _connection_configs, now=datetime(2026, 7, 22, 13, 1, tzinfo=timezone.utc))


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
    {{"run_id": "r", "attempt": 1, "trigger_type": "SCHEDULED"}},
    datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
    None,
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
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=60,
    )

    # Killed by SIGTERM, exactly as the sender intended.
    assert completed.returncode == -signal.SIGTERM, completed.stderr
    # Record the terminal outcome before exit so coverage can be remediated.
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
        backend, _connection_configs,
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
            return None

    run_due_checks(
        OrderedBackend(check, start=False), _connection_configs,
        now=datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
    )

    assert order == ["sweep", "latest"]
