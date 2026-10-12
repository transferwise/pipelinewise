"""Readable, credential-free summaries for data-diff alerts."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from math import isfinite

from .coverage import FAILED_STATUSES


COUNT_CHECKS = ('row_count', 'distinct_key_count')
MAX_ERROR_LENGTH = 2000


def _utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(sep=' ').removesuffix('+00:00')


def _single_line(value) -> str:
    return str(value).replace('\r', '\\r').replace('\n', '\\n')


def _count(value):
    # Adapters canonicalize counts to strings; keep their integer precision.
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        return None
    try:
        count = Decimal(value)
        if count.is_finite() and count >= 0 and count == count.to_integral_value():
            return int(count)
    except (InvalidOperation, ValueError):
        pass
    return None


def _count_rows(results: list) -> list:
    rows = []
    for result in results:
        if result['check_type'] not in COUNT_CHECKS:
            continue
        source = _count(result.get('source_value'))
        target = _count(result.get('target_value'))
        if source is not None and target is not None:
            rows.append((result['check_type'], source, target, target - source))
    return rows


def _headline(summary: dict, rows: list) -> str:
    if summary['status'] == 'ERROR':
        if (summary.get('preflight') or {}).get('status') == 'BLOCKED':
            return 'source index missing'
        return 'check could not complete'
    failed_checks = {
        result['check_type'] for result in (summary.get('results') or [])
        if result['status'] == 'FAIL'
    }
    for name, _source, _target, delta in rows:
        if name == 'row_count' and name in failed_checks and delta:
            direction = 'fewer' if delta < 0 else 'more'
            unit = 'row' if abs(delta) == 1 else 'rows'
            return f'target has {abs(delta)} {direction} {unit}'
    return 'data mismatch'


def _count_table(rows: list) -> str:
    headers = ('Check', 'Source', 'Target', 'Difference')
    widths = [max(len(str(row[index])) for row in [headers, *rows]) for index in range(4)]
    lines = [
        f'{name:<{widths[0]}}  {source:>{widths[1]}}  {target:>{widths[2]}}  {delta:>{widths[3]}}'
        for name, source, target, delta in [headers, *rows]
    ]
    return '\n'.join([*lines, '', 'Difference = target - source'])


def _query_seconds(results: list):
    timings = []
    for side in ('source', 'target'):
        # Each metric repeats the duration of the same aggregate query.
        seconds = next((
            result.get(f'{side}_query_seconds') for result in results
            if isinstance(result.get(f'{side}_query_seconds'), (int, float, Decimal))
            and not isinstance(result[f'{side}_query_seconds'], bool)
            and isfinite(result[f'{side}_query_seconds'])
            and result[f'{side}_query_seconds'] >= 0
        ), None)
        timings.append(seconds)
    if all(seconds is None for seconds in timings):
        return None
    return ', '.join(
        f'{side}={seconds:.2f}' if seconds is not None else f'{side}=not available'
        for side, seconds in zip(('source', 'target'), timings)
    )


def _table_name(check: dict, side: str):
    parts = [check.get(f'{side}_{part}') for part in ('database', 'schema', 'table')]
    return '.'.join(part for part in parts if part) or None


def _check_name(check: dict) -> str:
    if all(check.get(key) for key in ('tap_id', 'source_schema', 'source_table')):
        name = f"{check['tap_id']}/{check['source_schema']}.{check['source_table']}"
    else:
        name = check['full_check_name']
    return _single_line(name)


def _metadata(summary: dict, now: datetime) -> list:
    check = summary['check']
    fields = [
        ('alert_utc_time', _utc_timestamp(now.replace(microsecond=0))),
        ('tap_id', check.get('tap_id')),
        ('target_id', check.get('target_id')),
    ]
    if summary.get('run_id'):
        fields.append(('run_id', str(summary['run_id'])))
    if summary.get('attempt') is not None:
        attempt = str(summary['attempt'])
        if summary.get('trigger_type'):
            attempt += f" ({summary['trigger_type']})"
        fields.append(('attempt', attempt))
    if summary.get('scheduled_for'):
        fields.append(('scheduled_utc', _utc_timestamp(summary['scheduled_for'])))
    fields.extend((
        ('source_table', _table_name(check, 'source')),
        ('target_table', _table_name(check, 'target')),
    ))
    column = check.get('source_timestamp_column')
    target_column = check.get('target_timestamp_column')
    if column and target_column and column != target_column:
        column += f' (target: {target_column})'
    fields.append(('timestamp_column', column))
    if summary.get('window_start') is not None:
        fields.append(('window_start_utc', f"{_utc_timestamp(summary['window_start'])} (inclusive)"))
    elif summary.get('window_end') is not None:
        fields.append(('window_start_utc', 'unresolved (initial scan)'))
    if summary.get('window_end') is not None:
        fields.append(('window_end_utc', f"{_utc_timestamp(summary['window_end'])} (exclusive)"))
    fields.append(('query_seconds', _query_seconds(summary.get('results') or [])))
    return [(name, value) for name, value in fields if value is not None]


def _next_action(summary: dict) -> str:
    if (summary.get('preflight') or {}).get('status') == 'BLOCKED':
        column = summary['check'].get('source_timestamp_column', 'the timestamp column')
        action = f'Add a usable source index starting with {column}.'
    elif summary['status'] == 'FAIL':
        action = 'Check replication lag and investigate the mismatch.'
    else:
        action = 'Resolve the reported error.'
    instructions = [action]
    if not summary.get('run_id'):
        instructions.append(
            'Run the check again after resolving the problem.'
            if summary['check'].get('is_current') is False
            else 'Scheduled runs will attempt this check again.'
        )
    elif summary['check'].get('is_current') is False:
        instructions.append(
            'This definition is inactive. To verify this window, use '
            '`rerun_data_diff_check` with `--run-id` and `--remediation-ref`.'
        )
    else:
        instructions.extend((
            'Failed windows retry automatically on scheduled runs.',
            'To verify sooner, use `rerun_data_diff_check` with `--run-id` and `--remediation-ref`.',
        ))
    return '\n'.join(f'- {instruction}' for instruction in instructions)


def _error_detail(summary: dict):
    if not summary.get('error'):
        return None
    reason = str(summary['error'])
    preflight = summary.get('preflight') or {}
    findings = preflight.get('findings') or []
    if preflight.get('status') == 'BLOCKED' and findings and reason == '; '.join(findings):
        # The primary finding is diagnostic; Next action already supplies the index advice.
        return findings[0]
    return reason


def _failure_totals(failures: list) -> tuple[int, list]:
    windows = set()
    for failure in failures:
        if failure.get('scheduled_for') is not None:
            windows.add(('slot', _utc_timestamp(failure['scheduled_for'])))
        elif failure.get('window_start') is not None or failure.get('window_end') is not None:
            windows.add(tuple(
                _utc_timestamp(failure[bound]) if failure.get(bound) is not None else None
                for bound in ('window_start', 'window_end')
            ))
    statuses = [failure['status'] for failure in failures]
    fields = []
    if windows:
        fields.append(('failed_windows', len(windows)))
    fields.extend((
        ('failed_attempts', len(failures)),
        ('failure_statuses', ', '.join(
            f'{status}={statuses.count(status)}' for status in ('ERROR', 'FAIL') if status in statuses
        )),
        ('shown_window', 'representative failure'),
    ))
    return len(windows), fields


def format_data_diff_alert(summary: dict, *, failures: list = None, now: datetime = None) -> tuple[str, str, str]:
    """Return totals and representative details for a check's failure alert."""
    results = summary.get('results') or []
    rows = _count_rows(results)
    check = summary['check']
    name = _check_name(check)
    description = _headline(summary, rows)
    fields = _metadata(summary, now or datetime.now(timezone.utc))
    if failures and len(failures) > 1:
        windows, totals = _failure_totals(failures)
        fields[1:1] = totals
        count = windows or len(failures)
        unit = ('window' if windows else 'attempt') + ('s' if count != 1 else '')
        description = f'{count} failed {unit}; {description}'
    message = f"{summary['status']} data-diff {name} — {description}"
    failed_checks = [result['check_type'] for result in results if result['status'] in FAILED_STATUSES]
    if failed_checks:
        fields.append(('failed_checks', ', '.join(failed_checks)))
    details = '\n'.join(
        f'{name:<18}: {_single_line(value)}'
        for name, value in fields
    )
    reason = _error_detail(summary)
    if reason:
        if len(reason) > MAX_ERROR_LENGTH:
            reason = reason[:MAX_ERROR_LENGTH] + '\n[error truncated]'
        details += f'\n{"error_detail":<18}: {reason}'
    if rows:
        details += '\n\n' + _count_table(rows)
    return message, details, _next_action(summary)


def format_data_diff_index_warning(check: dict, preflight: dict, *, now: datetime = None) -> tuple[str, str, str]:
    """Describe an unindexed table approaching the data-check row limit."""
    row_limit = f"{preflight['row_limit']:,}"
    column = _single_line(check['source_timestamp_column'])
    fields = [
        ('alert_utc_time', _utc_timestamp((now or datetime.now(timezone.utc)).replace(microsecond=0))),
        ('tap_id', check.get('tap_id')),
        ('target_id', check.get('target_id')),
        ('source_table', _table_name(check, 'source')),
        ('timestamp_column', column),
        ('source_rows', f"{preflight['table_rows']:,}"),
        ('index_required_at', f'{row_limit} rows'),
        ('index_status', 'No qualifying source index'),
        ('data_diff_status', 'Checks can still run'),
    ]
    message = f'WARNING data-diff {_check_name(check)} — approaching source index requirement'
    details = '\n'.join(
        f'{name:<18}: {_single_line(value)}' for name, value in fields if value is not None
    )
    next_action = '\n'.join((
        f'- Arrange a usable source index starting with {column} before the table reaches {row_limit} rows.',
        f'- At {row_limit} rows or more, data checks will be blocked until that index exists.',
    ))
    return message, details, next_action
