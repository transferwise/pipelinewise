from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pipelinewise.data_diff.alerts import format_data_diff_alert, format_data_diff_index_warning


NOW = datetime(2026, 10, 9, 9, 30, 5, tzinfo=timezone.utc)
RUN_ID = '2bd3e725-38fc-48c1-b565-b4f20e5bc7dd'


def _count_result(check_type='row_count', source='100000', target='99980', **overrides):
    return {
        'check_type': check_type,
        'status': 'FAIL',
        'source_value': source,
        'target_value': target,
        'source_query_seconds': 2.4,
        'target_query_seconds': 1.8,
        **overrides,
    }


def _summary(**overrides):
    return {
        'check': {
            'full_check_name': 'snowflake/payments/public/transfers',
            'target_id': 'snowflake',
            'tap_id': 'payments',
            'source_database': 'payments',
            'source_schema': 'public',
            'source_table': 'transfers',
            'target_database': 'ANALYTICS',
            'target_schema': 'PAYMENTS',
            'target_table': 'TRANSFERS',
            'source_timestamp_column': 'updated_at',
            'is_current': True,
        },
        'status': 'FAIL',
        'run_id': RUN_ID,
        'attempt': 3,
        'trigger_type': 'RETRY',
        'scheduled_for': datetime(2026, 10, 9, 9, tzinfo=timezone.utc),
        'window_start': datetime(2026, 10, 9, 8, tzinfo=timezone.utc),
        'window_end': datetime(2026, 10, 9, 9, tzinfo=timezone.utc),
        'results': [_count_result(), _count_result('distinct_key_count')],
        'error': 'row_count FAIL; distinct_key_count FAIL',
        **overrides,
    }


def _fields(details):
    return {
        label.strip(): value.strip()
        for label, separator, value in (line.partition(':') for line in details.splitlines())
        if separator
    }


def _metric_cells(details, check_type):
    return next(line.split() for line in details.splitlines() if line.startswith(check_type))


def test_mismatch_alert_contains_operational_context_and_canonical_counts():
    message, details, next_action = format_data_diff_alert(_summary(), now=NOW)

    assert message == 'FAIL data-diff payments/public.transfers — target has 20 fewer rows'
    assert _fields(details) == {
        'alert_utc_time': '2026-10-09 09:30:05',
        'tap_id': 'payments',
        'run_id': RUN_ID,
        'attempt': '3 (RETRY)',
        'scheduled_utc': '2026-10-09 09:00:00',
        'target_id': 'snowflake',
        'source_table': 'payments.public.transfers',
        'target_table': 'ANALYTICS.PAYMENTS.TRANSFERS',
        'timestamp_column': 'updated_at',
        'window_start_utc': '2026-10-09 08:00:00 (inclusive)',
        'window_end_utc': '2026-10-09 09:00:00 (exclusive)',
        'query_seconds': 'source=2.40, target=1.80',
        'failed_checks': 'row_count, distinct_key_count',
        'error_detail': 'row_count FAIL; distinct_key_count FAIL',
    }
    assert _metric_cells(details, 'row_count') == ['row_count', '100000', '99980', '-20']
    assert _metric_cells(details, 'distinct_key_count') == ['distinct_key_count', '100000', '99980', '-20']
    assert 'Difference = target - source' in details
    assert details.count('query_seconds') == 1
    assert 'lag' in next_action.lower()
    assert 'rerun' in next_action.lower()
    assert 'retry automatically' in next_action
    assert '--run-id' in next_action
    assert '--remediation-ref' in next_action
    assert '```' not in details


@pytest.mark.parametrize('source,target', [
    ('100000000000000000000000000000000000000000', '99999999999999999999999999999999999999998'),
    (100000000000000000000000000000000000000000, 99999999999999999999999999999999999999998),
    (Decimal('100000000000000000000000000000000000000000'),
     Decimal('99999999999999999999999999999999999999998')),
])
def test_large_counts_and_differences_are_exact(source, target):
    message, details, _ = format_data_diff_alert(
        _summary(results=[_count_result(source=source, target=target)]), now=NOW,
    )

    assert message.endswith('target has 2 fewer rows')
    assert _metric_cells(details, 'row_count') == ['row_count', str(int(source)), str(int(target)), '-2']


@pytest.mark.parametrize('source,target,description', [
    ('0', '1', 'target has 1 more row'),
    ('0', '20', 'target has 20 more rows'),
    ('1', '0', 'target has 1 fewer row'),
])
def test_heading_states_mismatch_direction_and_singular_rows(source, target, description):
    message, _, _ = format_data_diff_alert(
        _summary(results=[_count_result(source=source, target=target)]), now=NOW,
    )

    assert message.endswith(description)


@pytest.mark.parametrize('invalid', [
    None, True, False, -1, 1.5, 'not-a-count', '-1', '1.5', 'NaN', 'Infinity', Decimal('1.5'), Decimal('NaN'),
])
@pytest.mark.parametrize('side', ['source_value', 'target_value'])
def test_invalid_counts_do_not_produce_a_misleading_difference(invalid, side):
    result = _count_result(**{side: invalid})

    message, details, _ = format_data_diff_alert(_summary(results=[result], error=None), now=NOW)

    assert message.endswith('data mismatch')
    assert not any(line.startswith('row_count') for line in details.splitlines())
    assert 'Difference = target - source' not in details


def test_count_table_can_include_passed_metrics_without_claiming_a_row_mismatch():
    message, details, _ = format_data_diff_alert(
        _summary(results=[
            _count_result(source='100000', target='100000', status='PASS'),
            _count_result('distinct_key_count'),
        ]), now=NOW,
    )

    assert message.endswith('data mismatch')
    assert _metric_cells(details, 'row_count') == ['row_count', '100000', '100000', '0']
    assert _metric_cells(details, 'distinct_key_count')[-1] == '-20'


def test_alerts_never_disclose_non_allowlisted_aggregate_or_schema_values():
    sensitive_results = [
        _count_result(check_type, source=f'source-secret-{check_type}', target=f'target-secret-{check_type}')
        for check_type in ('min_key', 'max_key', 'row_checksum', 'schema_compatibility', 'null_key_count')
    ]

    rendered = '\n'.join(format_data_diff_alert(
        _summary(results=[_count_result(), *sensitive_results], error=None), now=NOW,
    ))

    assert 'row_count' in rendered
    assert 'source-secret' not in rendered
    assert 'target-secret' not in rendered


def test_offset_timestamps_are_normalized_to_utc():
    offset = timezone(timedelta(hours=2))
    summary = _summary(
        scheduled_for=datetime(2026, 10, 9, 11, tzinfo=offset),
        window_start=datetime(2026, 10, 9, 10, tzinfo=offset),
        window_end=datetime(2026, 10, 9, 11, tzinfo=offset),
    )

    _, details, _ = format_data_diff_alert(
        summary, now=datetime(2026, 10, 9, 11, 30, 5, 123456, tzinfo=offset),
    )

    fields = _fields(details)
    assert fields['alert_utc_time'] == '2026-10-09 09:30:05'
    assert fields['scheduled_utc'] == '2026-10-09 09:00:00'
    assert fields['window_start_utc'] == '2026-10-09 08:00:00 (inclusive)'
    assert fields['window_end_utc'] == '2026-10-09 09:00:00 (exclusive)'


def test_historical_window_preserves_meaningful_timestamp_fractions():
    summary = _summary(
        window_start=datetime(2026, 10, 9, 8, 0, 0, 123456, tzinfo=timezone.utc),
        window_end=datetime(2026, 10, 9, 9, 0, 0, 500000, tzinfo=timezone.utc),
    )

    _, details, _ = format_data_diff_alert(summary, now=NOW)

    fields = _fields(details)
    assert fields['window_start_utc'] == '2026-10-09 08:00:00.123456 (inclusive)'
    assert fields['window_end_utc'] == '2026-10-09 09:00:00.500000 (exclusive)'


def test_blocked_initial_scan_identifies_missing_index_and_preserves_cutoff():
    summary = _summary(
        status='ERROR', window_start=None, results=[],
        preflight={'status': 'BLOCKED', 'has_leading_index': False},
        error="No usable source index starts with timestamp column 'updated_at'",
    )

    message, details, next_action = format_data_diff_alert(summary, now=NOW)

    assert message.endswith('source index missing')
    fields = _fields(details)
    assert fields['window_start_utc'] == 'unresolved (initial scan)'
    assert fields['window_end_utc'] == '2026-10-09 09:00:00 (exclusive)'
    assert fields['error_detail'] == summary['error']
    assert next_action.splitlines() == [
        '- Add a usable source index starting with updated_at.',
        '- Failed windows retry automatically on scheduled runs.',
        '- To verify sooner, use `rerun_data_diff_check` with `--run-id` and `--remediation-ref`.',
    ]


@pytest.mark.parametrize('diagnostic', [
    "No usable source index starts with timestamp column 'updated_at'",
    "No usable source index starts with timestamp column 'updated_at'; ignored as unusable: idx_partial",
])
def test_blocked_alert_shows_index_advice_once_and_preserves_diagnostics(diagnostic):
    findings = [
        diagnostic,
        'Add a source index beginning with the timestamp column; PipelineWise will not create it automatically',
    ]
    error = '; '.join(findings)
    summary = _summary(
        status='ERROR', results=[],
        preflight={'status': 'BLOCKED', 'findings': findings}, error=error,
    )

    message, details, next_action = format_data_diff_alert(summary, now=NOW)

    fields = _fields(details)
    assert message.endswith('source index missing')
    assert fields['tap_id'] == 'payments'
    assert fields['error_detail'] == diagnostic
    assert 'Add a source index' not in details
    assert 'PipelineWise will not create it' not in details
    assert next_action.count('Add a usable source index starting with updated_at.') == 1
    assert summary['error'] == error
    assert summary['preflight']['findings'] == findings


@pytest.mark.parametrize('preflight_status', ['BLOCKED', 'ERROR', 'PASS'])
def test_alert_retains_error_details_that_are_not_repeated_index_advice(preflight_status):
    findings = ['Source diagnostic', 'Further diagnostic']
    error = 'Additional source diagnostic' if preflight_status == 'BLOCKED' else '; '.join(findings)
    summary = _summary(
        status='ERROR', results=[],
        preflight={'status': preflight_status, 'findings': findings}, error=error,
    )

    _, details, _ = format_data_diff_alert(summary, now=NOW)

    assert _fields(details)['error_detail'] == error


def test_generic_execution_error_has_a_distinct_heading_and_recovery_action():
    message, details, next_action = format_data_diff_alert(
        _summary(status='ERROR', results=[], error='Target statement timeout'), now=NOW,
    )

    assert message.endswith('check could not complete')
    assert _fields(details)['error_detail'] == 'Target statement timeout'
    assert 'resolve' in next_action.lower()
    assert 'rerun' in next_action.lower()
    assert 'retry automatically' in next_action
    assert '--run-id' in next_action
    assert '--remediation-ref' in next_action


def test_alerts_before_run_creation_omit_unavailable_metadata():
    message, details, next_action = format_data_diff_alert({
        'check': {'tap_id': 'payments', 'source_schema': 'public', 'source_table': 'transfers'},
        'status': 'ERROR',
        'error': 'Unable to schedule check',
    }, now=NOW)

    assert message.startswith('ERROR data-diff payments/public.transfers')
    fields = _fields(details)
    assert fields['alert_utc_time'] == '2026-10-09 09:30:05'
    assert fields['tap_id'] == 'payments'
    assert fields['source_table'] == 'public.transfers'
    assert fields['error_detail'] == 'Unable to schedule check'
    missing_fields = {'run_id', 'attempt', 'scheduled_utc', 'target_table', 'query_seconds', 'window_start_utc'}
    assert not missing_fields & fields.keys()
    assert 'None' not in '\n'.join((message, details, next_action))
    assert 'scheduled runs' in next_action.lower()
    assert '--run-id' not in next_action
    assert '--remediation-ref' not in next_action


@pytest.mark.parametrize('trigger_type', ['SCHEDULED', 'RETRY', 'REMEDIATION'])
def test_active_checks_explain_automatic_retry_and_complete_manual_rerun_options(trigger_type):
    _, _, next_action = format_data_diff_alert(_summary(trigger_type=trigger_type), now=NOW)

    assert 'Failed windows retry automatically on scheduled runs.' in next_action
    assert 'To verify sooner' in next_action
    assert 'rerun_data_diff_check' in next_action
    assert '--run-id' in next_action
    assert '--remediation-ref' in next_action


@pytest.mark.parametrize('trigger_type', ['SCHEDULED', 'RETRY', 'REMEDIATION'])
def test_inactive_definition_does_not_promise_automatic_retry(trigger_type):
    summary = _summary(trigger_type=trigger_type)
    summary['check']['is_current'] = False

    _, _, next_action = format_data_diff_alert(summary, now=NOW)

    assert 'retry automatically' not in next_action
    assert 'To verify sooner' not in next_action
    assert 'rerun_data_diff_check' in next_action
    assert '--run-id' in next_action
    assert '--remediation-ref' in next_action


def test_single_failure_group_preserves_the_existing_alert_layout():
    summary = _summary()

    grouped = format_data_diff_alert(summary, failures=[summary], now=NOW)

    assert grouped == format_data_diff_alert(summary, now=NOW)
    assert not {'failed_windows', 'failed_attempts', 'failure_statuses', 'shown_window'} & _fields(grouped[1]).keys()


def test_grouped_alert_counts_slots_and_attempts_without_summing_representative_results():
    first = _summary(status='ERROR', results=[], error='Source query failed')
    second = _summary(attempt=4, window_start=NOW - timedelta(days=3))
    representative = _summary(
        status='ERROR', attempt=1, trigger_type='SCHEDULED',
        scheduled_for=NOW + timedelta(hours=1),
        window_start=NOW, window_end=NOW + timedelta(hours=1),
        results=[_count_result(source='90', target='75')],
        error='Source query failed after row count',
    )

    message, details, _ = format_data_diff_alert(
        representative, failures=[first, second, representative], now=NOW,
    )

    assert '2 failed windows' in message
    fields = _fields(details)
    assert fields['failed_windows'] == '2'
    assert fields['failed_attempts'] == '3'
    assert fields['failure_statuses'] == 'ERROR=2, FAIL=1'
    assert fields['shown_window'] == 'representative failure'
    assert fields['attempt'] == '1 (SCHEDULED)'
    assert fields['error_detail'] == representative['error']
    assert _metric_cells(details, 'row_count') == ['row_count', '90', '75', '-15']
    assert not any(line.startswith('distinct_key_count') for line in details.splitlines())
    assert '100000' not in details
    assert fields['query_seconds'] == 'source=2.40, target=1.80'


def test_grouped_alert_counts_window_bounds_when_scheduled_slots_are_unavailable():
    first = _summary(scheduled_for=None)
    repeated = _summary(scheduled_for=None, attempt=4)
    next_window = _summary(
        scheduled_for=None,
        window_start=first['window_end'], window_end=first['window_end'] + timedelta(hours=1),
    )

    message, details, _ = format_data_diff_alert(
        next_window, failures=[first, repeated, next_window], now=NOW,
    )

    assert '2 failed windows' in message
    assert _fields(details)['failed_windows'] == '2'
    assert _fields(details)['failed_attempts'] == '3'
    assert _fields(details)['failure_statuses'] == 'FAIL=3'


def test_grouped_initial_scan_has_a_known_window_from_its_end_alone():
    summary = _summary(status='ERROR', scheduled_for=None, window_start=None, results=[])
    repeated = {**summary, 'attempt': 4}

    message, details, _ = format_data_diff_alert(summary, failures=[summary, repeated], now=NOW)

    assert '1 failed window' in message
    fields = _fields(details)
    assert fields['failed_windows'] == '1'
    assert fields['failed_attempts'] == '2'
    assert fields['window_start_utc'] == 'unresolved (initial scan)'
    assert fields['window_end_utc'] == '2026-10-09 09:00:00 (exclusive)'


def test_grouped_failures_before_scheduling_do_not_invent_window_counts():
    summary = _summary(
        status='ERROR', run_id=None, scheduled_for=None, window_start=None, window_end=None,
        results=[], error='Unable to schedule check',
    )
    repeated = {**summary, 'error': 'Backend connection unavailable'}

    message, details, next_action = format_data_diff_alert(repeated, failures=[summary, repeated], now=NOW)

    fields = _fields(details)
    assert 'failed_windows' not in fields
    assert 'failed window' not in message
    assert fields['failed_attempts'] == '2'
    assert fields['failure_statuses'] == 'ERROR=2'
    assert fields['shown_window'] == 'representative failure'
    assert fields['error_detail'] == repeated['error']
    assert 'scheduled runs' in next_action.lower()
    assert '--run-id' not in next_action
    assert '--remediation-ref' not in next_action


def test_grouped_alert_only_counts_known_windows_without_assigning_unknown_failures_to_one():
    known = _summary()
    unknown = _summary(
        status='ERROR', run_id=None, scheduled_for=None, window_start=None, window_end=None, results=[],
    )

    message, details, _ = format_data_diff_alert(known, failures=[known, unknown], now=NOW)

    assert '1 failed window' in message
    assert _fields(details)['failed_windows'] == '1'
    assert _fields(details)['failed_attempts'] == '2'


def test_multiline_error_is_preserved_but_oversized_errors_are_bounded():
    error = 'Connection failed\nServer diagnostic: ' + 'x' * 4000

    _, details, _ = format_data_diff_alert(_summary(status='ERROR', results=[], error=error), now=NOW)

    assert 'Connection failed\nServer diagnostic:' in details
    assert 'truncat' in details.lower()
    assert error[:2000] in details
    assert error[:2001] not in details
    assert len(details) < 3000


@pytest.mark.parametrize('table_rows', [50000, 75000, 99999])
def test_index_warning_identifies_table_and_exact_count_without_claiming_failure(table_rows):
    check = _summary()['check']
    preflight = {'status': 'PASS', 'table_rows': table_rows, 'row_limit': 100000, 'index_warning': True}

    message, details, next_action = format_data_diff_index_warning(check, preflight, now=NOW)

    assert message == 'WARNING data-diff payments/public.transfers — approaching source index requirement'
    assert _fields(details) == {
        'alert_utc_time': '2026-10-09 09:30:05',
        'tap_id': 'payments',
        'target_id': 'snowflake',
        'source_table': 'payments.public.transfers',
        'timestamp_column': 'updated_at',
        'source_rows': f'{table_rows:,}',
        'index_required_at': '100,000 rows',
        'index_status': 'No qualifying source index',
        'data_diff_status': 'Checks can still run',
    }
    assert next_action.splitlines() == [
        '- Arrange a usable source index starting with updated_at before the table reaches 100,000 rows.',
        '- At 100,000 rows or more, data checks will be blocked until that index exists.',
    ]
    assert 'ERROR' not in message
    assert 'run_id' not in details
    assert 'rerun' not in next_action
    assert preflight['status'] == 'PASS'


def test_index_warning_normalizes_time_and_omits_unavailable_context():
    check = {
        'full_check_name': 'public/transfers', 'source_table': 'transfers', 'source_timestamp_column': 'updated_at',
    }
    message, details, _ = format_data_diff_index_warning(
        check, {'table_rows': 75000, 'row_limit': 100000},
        now=datetime(2026, 10, 9, 11, 30, 5, 999999, tzinfo=timezone(timedelta(hours=2))),
    )

    assert message.startswith('WARNING data-diff public/transfers')
    assert _fields(details)['alert_utc_time'] == '2026-10-09 09:30:05'
    assert _fields(details)['source_table'] == 'transfers'
    assert 'tap_id' not in _fields(details)
    assert 'target_id' not in _fields(details)
    assert 'None' not in details


def test_index_warning_uses_only_allowed_metadata_and_keeps_it_on_single_lines():
    check = {
        **_summary()['check'],
        'tap_id': 'payments\nextra_line',
        'source_timestamp_column': 'updated_at\r\nextra_column',
        'password': 'password-secret',
        'username': 'username-secret',
    }
    preflight = {'table_rows': 75000, 'row_limit': 100000, 'findings': ['unrelated-secret']}

    message, details, next_action = format_data_diff_index_warning(check, preflight, now=NOW)

    rendered = '\n'.join((message, details, next_action))
    assert 'payments\\nextra_line' in message
    assert _fields(details)['timestamp_column'] == 'updated_at\\r\\nextra_column'
    assert next_action.count('\n') == 1
    assert 'password-secret' not in rendered
    assert 'username-secret' not in rendered
    assert 'unrelated-secret' not in rendered
