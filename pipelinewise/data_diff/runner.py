"""Scheduling and orchestration for independent persisted data-diff checks."""

import hashlib
import logging
import os
import signal

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable

from croniter import croniter

from .engine import run_check


LOGGER = logging.getLogger(__name__)


MAX_BACKFILL_WINDOWS = 24
# How far past its query budget a RUNNING attempt counts as abandoned. Derived per
# check, because statement_timeout has no upper bound.
STALE_RUN_GRACE = timedelta(minutes=30)
STALE_RUN_QUERY_BUDGET_MULTIPLIER = 4


def _stale_run_age(check: dict) -> timedelta:
    """Return how old a RUNNING attempt must be before it counts as abandoned.

    Derived from the check's own statement_timeout rather than fixed, because that
    timeout has no upper bound: a generous one must not have its live runs expired.
    """
    budget = int(check.get("statement_timeout_seconds") or 0)
    return STALE_RUN_GRACE + timedelta(
        seconds=budget * STALE_RUN_QUERY_BUDGET_MULTIPLIER
    )


def scheduled_slot(now: datetime, cron_expr: str) -> datetime:
    """Return the most recent completed cron fire time at or before ``now``."""
    cron = croniter(cron_expr, now)
    return cron.get_prev(datetime).replace(tzinfo=timezone.utc)


def check_window(check: dict, now: datetime) -> tuple:
    """Return ``(scheduled_for, start, end)`` for a half-open UTC window."""
    scheduled_for = scheduled_slot(now, check["frequency"])
    return window_for_slot(check, scheduled_for)


def window_for_slot(check: dict, scheduled_for: datetime) -> tuple:
    """Return the comparison window for an already aligned UTC slot."""
    window_start = scheduled_for - timedelta(seconds=int(check["window_start_seconds"]))
    window_end = scheduled_for - timedelta(seconds=int(check["window_end_seconds"]))
    return scheduled_for, window_start, window_end


def due_slots(check: dict, now: datetime, latest_scheduled_for: datetime = None) -> list:
    """Return the oldest unobserved slots, capped to protect source databases."""
    current = scheduled_slot(now, check["frequency"])
    if latest_scheduled_for is None or latest_scheduled_for >= current:
        return [current]
    slots = []
    cron = croniter(check["frequency"], latest_scheduled_for)
    while len(slots) < MAX_BACKFILL_WINDOWS:
        slot = cron.get_next(datetime).replace(tzinfo=timezone.utc)
        if slot > current:
            break
        slots.append(slot)
    return slots


ConnectionConfigLoader = Callable[[dict], tuple]


def validate_connection_configs(
    check: dict,
    source_config: dict,
    target_config: dict,
) -> tuple:
    """Reject runtime connections that differ from the persisted definition."""
    if source_config.get("dbname") != check["source_database"]:
        raise ValueError(
            f"Persisted source database '{check['source_database']}' does not match "
            f"the imported connection for tap '{check['tap_id']}'"
        )
    if target_config.get("dbname") != check["target_database"]:
        raise ValueError(
            f"Persisted target database '{check['target_database']}' does not match "
            f"the imported connection for target '{check['target_id']}'"
        )
    return source_config, target_config


def _matches_check_filter(check: dict, check_filter: str) -> bool:
    if not check_filter:
        return True
    return check_filter in {
        str(check["check_id"]),
        check["full_check_name"],
    }


def execute_started_run(
    backend,
    connection_config_loader: ConnectionConfigLoader,
    check: dict,
    run: dict,
    scheduled_for: datetime,
    window_start: datetime,
    window_end: datetime,
) -> dict:
    """Execute and persist one already-created scheduled or remediation attempt."""
    recorded = {"preflight_id": None}

    def _record(preflight):
        # Called by run_check before either aggregate executes.
        recorded["preflight_id"] = backend.record_preflight(
            check["check_id"], preflight
        )

    with _terminating_signals_finish_run(backend, check, run, recorded):
        return _execute_and_persist(
            backend, connection_config_loader, check, run,
            scheduled_for, window_start, window_end,
            recorded=recorded, on_preflight=_record,
        )


# pylint: disable=too-many-arguments,too-many-locals
def _execute_and_persist(
    backend,
    connection_config_loader: ConnectionConfigLoader,
    check: dict,
    run: dict,
    scheduled_for: datetime,
    window_start: datetime,
    window_end: datetime,
    *,
    recorded: dict,
    on_preflight,
) -> dict:
    """Run the check and persist its outcome, converting failures to ERROR."""
    try:
        source_config, target_config = validate_connection_configs(
            check,
            *connection_config_loader(check),
        )
        preflight, results, status = run_check(
            check, source_config, target_config, window_start, window_end,
            on_preflight=on_preflight,
        )
        preflight_id = recorded["preflight_id"]
        if preflight["status"] != "PASS":
            error = preflight.get("error") or "; ".join(preflight.get("findings", []))
            backend.finish_run(
                run["run_id"], "ERROR", [], preflight_id=preflight_id, error=error
            )
            status = "ERROR"
        else:
            backend.finish_run(
                run["run_id"], status, results, preflight_id=preflight_id
            )
        return {
            "check": check,
            "run_id": run["run_id"],
            "status": status,
            "scheduled_for": scheduled_for,
            "window_start": window_start,
            "window_end": window_end,
            "results": results,
            "preflight": preflight,
            "attempt": run["attempt"],
            "trigger": run["trigger"],
        }
    except (KeyboardInterrupt, SystemExit) as exc:
        # Only for interpreter-raised cases, e.g. Ctrl-C with no handler installed.
        # Real signals are converted to a terminal ERROR by the handler above.
        _finish_failed_run(
            backend, check, run, recorded["preflight_id"],
            f"Interrupted by {type(exc).__name__} before the check completed",
        )
        raise
    except Exception as exc:
        _finish_failed_run(backend, check, run, recorded["preflight_id"], str(exc))
        return {
            "check": check,
            "run_id": run["run_id"],
            "status": "ERROR",
            "scheduled_for": scheduled_for,
            "window_start": window_start,
            "window_end": window_end,
            "error": str(exc),
            "attempt": run["attempt"],
            "trigger": run["trigger"],
        }


@contextmanager
def _terminating_signals_finish_run(backend, check: dict, run: dict, recorded: dict):
    """Mark the in-flight run ERROR when a terminating signal arrives.

    Default SIGTERM kills the interpreter outright without raising, so an
    ``except`` clause never sees it and the attempt would stay RUNNING forever —
    a state no later attempt replaces. Handlers are restored on exit, and after
    recording we re-raise the signal through the original disposition so the
    process still dies exactly as the sender intended.
    """
    previous = {}

    def _handle(signum, _frame):
        try:
            _finish_failed_run(
                backend, check, run, recorded["preflight_id"],
                f"Terminated by {signal.Signals(signum).name} "
                "before the check completed",
            )
        finally:
            signal.signal(signum, previous.get(signum, signal.SIG_DFL))
            os.kill(os.getpid(), signum)

    installed = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            previous[signum] = signal.signal(signum, _handle)
            installed.append(signum)
        except ValueError:
            # Only the main thread may install handlers; a worker thread runs
            # without them and relies on the stale-run sweep instead.
            pass
    try:
        yield
    finally:
        for signum in installed:
            signal.signal(signum, previous[signum])


def _finish_failed_run(backend, check: dict, run: dict, preflight_id, error: str):
    """Record a terminal ERROR so the slot is retryable and coverage sees it.

    A placeholder preflight is written only when no real one was recorded, which
    now means the failure happened before the preflight was even decided. Once a
    real preflight exists its id is reused, so a later failure such as a statement
    timeout can no longer overwrite genuine evidence with a synthetic record.
    """
    if preflight_id is None:
        preflight_id = backend.record_preflight(
            check["check_id"],
            {
                "status": "ERROR",
                "query_fingerprint": hashlib.sha256(b"preflight-not-built").hexdigest(),
                "index_metadata": [],
                "findings": [],
                "error": error,
                "table_rows": None,
                "row_limit": None,
                "has_leading_index": None,
            },
        )
    backend.finish_run(
        run["run_id"], "ERROR", [], preflight_id=preflight_id, error=error
    )


# pylint: disable=too-many-locals
def run_due_checks(
    backend,
    connection_config_loader: ConnectionConfigLoader,
    *,
    now: datetime = None,
    target_id: str = None,
    tap_id: str = None,
    check_filter: str = None,
    force: bool = False,
) -> list:
    """Run current definitions serially and backfill bounded missed slots."""
    now = now or datetime.now(timezone.utc)
    checks = backend.list_checks(target_id=target_id, tap_id=tap_id)
    summaries = []
    for check in checks:
        if not _matches_check_filter(check, check_filter):
            continue
        # Before the latest slot is read, not per due slot: a RUNNING row counts as
        # observed, so leaving it would advance the scheduler past its own slot.
        expired = backend.expire_stale_running_attempts(
            check["check_id"], now - _stale_run_age(check)
        )
        if expired:
            LOGGER.warning(
                "Expired %s abandoned run(s) for %s",
                expired,
                check["full_check_name"],
            )
        latest = backend.latest_scheduled_for(check["check_id"])
        try:
            slots = due_slots(check, now, latest)
        except Exception as exc:  # pylint: disable=broad-except
            # No run row exists yet, so this cannot be recorded against one. Report
            # and continue: one broken definition must not stop the other checks.
            LOGGER.error(
                "Cannot schedule data-diff check %s: %s",
                check["full_check_name"],
                exc,
            )
            summaries.append(
                {
                    "check": check,
                    "status": "ERROR",
                    "scheduled_for": None,
                    "window_start": None,
                    "window_end": None,
                    "error": str(exc),
                }
            )
            continue
        for scheduled_for in slots:
            _, window_start, window_end = window_for_slot(check, scheduled_for)
            run = backend.start_run(
                check, scheduled_for, window_start, window_end, force=force
            )
            if run is None:
                summaries.append(
                    {
                        "check": check,
                        "status": "SKIPPED",
                        "scheduled_for": scheduled_for,
                        "window_start": window_start,
                        "window_end": window_end,
                    }
                )
                continue
            summaries.append(
                execute_started_run(
                    backend, connection_config_loader, check, run,
                    scheduled_for, window_start, window_end,
                )
            )
    return summaries


def rerun_failed_check(
    backend,
    connection_config_loader: ConnectionConfigLoader,
    run_id,
    remediation_reference: str,
) -> dict:
    """Re-execute the exact definition revision and window of a failed run."""
    original = backend.get_run(run_id)
    check = backend.get_check_version(original["dd_check_id"])
    run = backend.start_remediation_run(original, remediation_reference)
    return execute_started_run(
        backend, connection_config_loader, check, run,
        original["scheduled_for"], original["window_start"], original["window_end"],
    )
