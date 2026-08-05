from datetime import datetime, timedelta, timezone
from itertools import product
from uuid import uuid4

from pipelinewise.data_diff.coverage import (
    advance_coverage,
    calculate_coverage,
    coverage_event_type,
)

# pylint: disable=missing-function-docstring,invalid-name


def _instant(hour):
    return datetime(2026, 7, 22, hour, tzinfo=timezone.utc)


def _run(start, end, status, *, slot=None, attempt=1, run_id=None):
    return {
        "run_id": run_id or uuid4(),
        "scheduled_for": _instant(slot if slot is not None else end),
        "window_start": _instant(start),
        "window_end": _instant(end),
        "attempt": attempt,
        "status": status,
    }


def test_failed_interval_blocks_later_successful_coverage():
    failed = _run(11, 12, "FAIL")
    coverage = calculate_coverage([
        _run(10, 11, "PASS"),
        failed,
        _run(12, 13, "PASS"),
    ])

    assert coverage["coverage_start"] == _instant(10)
    assert coverage["verified_through"] == _instant(11)
    assert coverage["coverage_status"] == "BLOCKED"
    assert coverage["blocking_run_id"] == failed["run_id"]


def test_successful_remediation_fills_gap_and_advances_over_later_passes():
    original_id = uuid4()
    coverage = calculate_coverage([
        _run(10, 11, "PASS"),
        _run(11, 12, "FAIL", run_id=original_id),
        _run(11, 12, "PASS", attempt=2),
        _run(12, 13, "PASS"),
    ])

    assert coverage["verified_through"] == _instant(13)
    assert coverage["coverage_status"] == "CONTIGUOUS"
    assert coverage["blocking_run_id"] is None


def test_later_failed_revalidation_invalidates_previous_coverage():
    previous = {
        "verified_through": _instant(12),
        "coverage_status": "CONTIGUOUS",
    }
    current = calculate_coverage([
        _run(10, 11, "PASS"),
        _run(11, 12, "PASS", attempt=1),
        _run(11, 12, "FAIL", attempt=2),
    ])

    assert current["verified_through"] == _instant(11)
    assert coverage_event_type(previous, current) == "INVALIDATE"


def test_metadata_only_definition_never_advances_coverage():
    coverage = calculate_coverage(
        [_run(10, 11, "PASS")], data_checks_enabled=False
    )

    assert coverage["verified_through"] == _instant(10)
    assert coverage["coverage_status"] == "BLOCKED"
    assert "Metadata-only" in coverage["reason"]


def test_missing_interval_is_reported_as_blocked_without_failure_run():
    coverage = calculate_coverage([
        _run(10, 11, "PASS"),
        _run(12, 13, "PASS"),
    ])

    assert coverage["verified_through"] == _instant(11)
    assert coverage["blocking_run_id"] is None
    assert "next timestamp interval" in coverage["reason"]


def _schedule(cadence_hours, start_offset, end_offset, slots=4):
    """Build passing runs the way a tap fires them: one window per cron slot.

    Uses timedeltas rather than _instant so schedules may span more than a day.
    """
    base = datetime(2026, 7, 22, tzinfo=timezone.utc)
    runs = []
    for index in range(slots):
        fire = base + timedelta(hours=cadence_hours * index)
        runs.append({
            "run_id": uuid4(),
            "scheduled_for": fire,
            "window_start": fire - timedelta(hours=start_offset),
            "window_end": fire - timedelta(hours=end_offset),
            "attempt": 1,
            "status": "PASS",
        })
    return runs


def test_window_narrower_than_cadence_blocks_coverage():
    # A 3h window fired every 6h leaves 3h that no check ever examines. This is
    # what the shipped defaults did before they were widened.
    coverage = calculate_coverage(_schedule(6, 15, 12))

    assert coverage["coverage_status"] == "BLOCKED"


def test_window_wider_than_cadence_keeps_coverage_contiguous():
    # The shipped defaults: 12h window on a 6h cadence, so windows overlap and a
    # skipped slot cannot open a gap.
    coverage = calculate_coverage(_schedule(6, 15, 3))

    assert coverage["coverage_status"] == "CONTIGUOUS"


def test_overlapping_windows_survive_a_skipped_slot():
    runs = _schedule(6, 15, 3)
    del runs[1]

    assert calculate_coverage(runs)["coverage_status"] == "CONTIGUOUS"


def test_incremental_coverage_matches_full_calculation_for_appended_slots():
    runs = [
        _run(8, 10, "PASS"),
        _run(9, 11, "PASS"),
        _run(10, 12, "FAIL"),
        _run(11, 13, "PASS"),
    ]
    incremental = None

    for index, run in enumerate(runs, start=1):
        incremental = advance_coverage(incremental, run)
        assert incremental == calculate_coverage(runs[:index])


def test_incremental_coverage_matches_full_calculation_across_a_gap():
    runs = [
        _run(8, 9, "PASS"),
        _run(10, 11, "PASS"),
        _run(11, 12, "PASS"),
    ]
    incremental = None

    for index, run in enumerate(runs, start=1):
        incremental = advance_coverage(incremental, run)
        assert incremental == calculate_coverage(runs[:index])


def test_incremental_metadata_coverage_matches_full_calculation():
    runs = [_run(8, 10, "PASS"), _run(9, 11, "PASS")]
    incremental = None

    for index, run in enumerate(runs, start=1):
        incremental = advance_coverage(
            incremental,
            run,
            data_checks_enabled=False,
        )
        assert incremental == calculate_coverage(
            runs[:index],
            data_checks_enabled=False,
        )


def test_incremental_coverage_matches_full_for_window_and_status_combinations():
    for window_width, cadence in ((1, 1), (1, 2), (2, 1), (3, 2)):
        for statuses in product(("PASS", "FAIL", "ERROR"), repeat=4):
            runs = [
                _run(
                    8 + (index * cadence),
                    8 + (index * cadence) + window_width,
                    status,
                )
                for index, status in enumerate(statuses)
            ]
            incremental = None

            for index, run in enumerate(runs, start=1):
                incremental = advance_coverage(incremental, run)
                assert incremental == calculate_coverage(runs[:index])
