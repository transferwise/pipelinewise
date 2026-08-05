"""Derive contiguous timestamp coverage from immutable check attempts."""

TERMINAL_STATUSES = {"PASS", "FAIL", "ERROR"}


def _effective_attempts(runs: list) -> list:
    """Return the highest terminal attempt for each scheduled definition slot."""
    latest = {}
    for run in runs:
        if run["status"] not in TERMINAL_STATUSES:
            continue
        slot = run["scheduled_for"]
        current = latest.get(slot)
        if current is None or int(run["attempt"]) > int(current["attempt"]):
            latest[slot] = run
    return sorted(latest.values(), key=lambda item: (item["window_start"], item["window_end"]))


def calculate_coverage(runs: list, *, data_checks_enabled: bool = True) -> dict:
    """Calculate the conservative contiguous interval covered by effective PASS runs."""
    effective = _effective_attempts(runs)
    if not effective:
        return {}

    coverage_start = min(run["window_start"] for run in effective)
    max_observed_end = max(run["window_end"] for run in effective)
    cursor = coverage_start
    blocking_runs = sorted(
        (run for run in effective if run["status"] != "PASS"),
        key=lambda item: (item["window_start"], item["window_end"]),
    )
    barrier = blocking_runs[0]["window_start"] if blocking_runs else None

    if data_checks_enabled:
        passing = sorted(
            (run for run in effective if run["status"] == "PASS"),
            key=lambda item: (item["window_start"], item["window_end"]),
        )
        for run in passing:
            if run["window_start"] > cursor:
                break
            if run["window_end"] > cursor:
                cursor = run["window_end"]
            if barrier is not None and cursor >= barrier:
                cursor = barrier
                break

    blocking_run = None
    if barrier is not None and barrier <= cursor:
        blocking_run = blocking_runs[0]
    status = "CONTIGUOUS" if cursor >= max_observed_end else "BLOCKED"
    if not data_checks_enabled:
        status = "BLOCKED"

    if not data_checks_enabled:
        reason = "Metadata-only checks cannot advance table coverage"
    elif blocking_run:
        reason = f"Run {blocking_run['run_id']} has status {blocking_run['status']} at the coverage boundary"
    elif status == "BLOCKED":
        reason = "No successful run covers the next timestamp interval"
    else:
        reason = "All observed timestamp intervals are covered by successful runs"

    return {
        "coverage_start": coverage_start,
        "verified_through": cursor,
        "max_observed_end": max_observed_end,
        "coverage_status": status,
        "blocking_run_id": blocking_run["run_id"] if blocking_run else None,
        "reason": reason,
    }


def advance_coverage(previous: dict, run: dict, *, data_checks_enabled: bool = True) -> dict:
    """Apply one newly appended effective slot to materialized coverage state.

    Definition revisions have fixed window offsets, so scheduled order is also
    window-start order. Replacements and out-of-order slots use ``calculate_coverage``.
    """
    if not previous:
        return calculate_coverage([run], data_checks_enabled=data_checks_enabled)

    coverage_start = previous["coverage_start"]
    verified_through = previous["verified_through"]
    max_observed_end = max(previous["max_observed_end"], run["window_end"])
    blocking_run_id = previous.get("blocking_run_id")

    if not data_checks_enabled:
        return {
            "coverage_start": coverage_start,
            "verified_through": verified_through,
            "max_observed_end": max_observed_end,
            "coverage_status": "BLOCKED",
            "blocking_run_id": None,
            "reason": "Metadata-only checks cannot advance table coverage",
        }

    if previous["coverage_status"] == "BLOCKED":
        return {
            "coverage_start": coverage_start,
            "verified_through": verified_through,
            "max_observed_end": max_observed_end,
            "coverage_status": "BLOCKED",
            "blocking_run_id": blocking_run_id,
            "reason": previous["reason"],
        }

    if run["status"] == "PASS":
        if run["window_start"] <= verified_through:
            verified_through = max(verified_through, run["window_end"])
    elif run["window_start"] <= verified_through:
        verified_through = run["window_start"]
        blocking_run_id = run["run_id"]

    coverage_status = "CONTIGUOUS" if verified_through >= max_observed_end else "BLOCKED"
    if blocking_run_id:
        reason = f"Run {blocking_run_id} has status {run['status']} at the coverage boundary"
    elif coverage_status == "BLOCKED":
        reason = "No successful run covers the next timestamp interval"
    else:
        reason = "All observed timestamp intervals are covered by successful runs"

    return {
        "coverage_start": coverage_start,
        "verified_through": verified_through,
        "max_observed_end": max_observed_end,
        "coverage_status": coverage_status,
        "blocking_run_id": blocking_run_id,
        "reason": reason,
    }


def coverage_event_type(previous: dict, current: dict) -> str:
    """Describe how a newly evaluated run changed the coverage watermark."""
    if not previous:
        return "INITIALIZE"
    old_value = previous["verified_through"]
    new_value = current["verified_through"]
    if new_value > old_value:
        return "ADVANCE"
    if new_value < old_value:
        return "INVALIDATE"
    if current["coverage_status"] == "BLOCKED":
        return "BLOCK"
    return "CONFIRM"
