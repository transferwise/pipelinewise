import json

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs


class RepositoryContext:
    def __init__(self, checks=None, sync_error=None, historical_scans_pending=0):
        self.checks = checks or []
        self.sync_error = sync_error
        self.historical_scans_pending = historical_scans_pending
        self.filters = None
        self.synced = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def list_checks(self, **filters):
        self.filters = filters
        return self.checks

    def sync_definitions(self, definitions, *, selected_taps, excluded_taps=None):
        self.synced = (definitions, selected_taps, excluded_taps)
        if self.sync_error:
            raise self.sync_error
        return {
            'created': len(definitions),
            'historical_scans_pending': self.historical_scans_pending,
        }


def _pipelinewise(**args):
    instance = object.__new__(PipelineWise)
    instance.args = CliArgs(**args)
    instance.config = {"backend_db": {"host": "backend"}}
    instance.config_dir = "/config"
    instance.alert_sender = Mock()
    instance.logger = Mock()
    return instance


def _stored_check():
    return {
        "check_id": uuid4(),
        "revision": 2,
        "is_current": True,
        "target_id": "target",
        "tap_id": "tap",
        "source_schema": "public",
        "source_table": "payments",
        "checks": ["row_count"],
        "source_key_column": "id",
        "source_timestamp_column": "updated_at",
        "source_compare_columns": [],
        "frequency": "0 * * * *",
        "window_start_seconds": 3600,
        "window_end_seconds": 0,
        "full_check_name": "target/tap/public/payments",
        'initial_full_scan': True,
        'historical_scan_pending': True,
        "verified_status": None,
        'verified_start': None,
        "verified_end": None,
    }


def _summary(status="PASS", *, check=None, **overrides):
    instant = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    return {
        "check": check if check is not None else _stored_check(),
        "status": status,
        "window_start": instant,
        "window_end": instant,
        "run_id": uuid4(),
        "attempt": 2,
        **overrides,
    }


def test_list_checks_reads_backend_and_supports_json(capsys):
    repository = RepositoryContext([_stored_check()])
    pipelinewise = _pipelinewise(
        target="target",
        tap="tap",
        output_format="json",
        include_versioned=True,
    )

    with patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=repository,
    ):
        pipelinewise.list_data_diff_checks()

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["full_check_name"] == "target/tap/public/payments"
    assert "verified_status" in payload[0]
    assert "verified_end" in payload[0]
    assert payload[0]['initial_full_scan'] is True
    assert payload[0]['historical_scan_pending'] is True
    assert payload[0]['verified_start'] is None
    assert "coverage_status" not in payload[0]
    assert "verified_through" not in payload[0]
    assert repository.filters == {
        "target_id": "target",
        "tap_id": "tap",
        "include_versioned": True,
    }


def test_list_checks_uses_verified_state_names_in_table_output(capsys):
    check = _stored_check()
    check['historical_scan_pending'] = False
    check["verified_status"] = "CONTIGUOUS"
    check['verified_start'] = datetime(2026, 7, 1, tzinfo=timezone.utc)
    check["verified_end"] = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    repository = RepositoryContext([check])
    pipelinewise = _pipelinewise(
        target="target",
        tap="tap",
        output_format="table",
        include_versioned=False,
    )

    with patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=repository,
    ):
        pipelinewise.list_data_diff_checks()

    output = capsys.readouterr().out
    assert "Verified status" in output
    assert 'Full scan' in output
    assert 'Initial scan pending' in output
    assert 'Verified start' in output
    assert "Verified end" in output
    assert "CONTIGUOUS" in output
    assert "2026-07-22T13:00:00+00:00" in output
    assert '2026-07-01T00:00:00+00:00' in output


@pytest.mark.parametrize('initial_full_scan,pending', [(True, True), (True, False), (False, False)])
def test_list_checks_distinguishes_full_scan_setting_from_pending_work(initial_full_scan, pending):
    check = {**_stored_check(), 'initial_full_scan': initial_full_scan, 'historical_scan_pending': pending}
    pipelinewise = _pipelinewise(output_format='table', include_versioned=False)

    with patch(
        'pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config',
        return_value=RepositoryContext([check]),
    ), patch('pipelinewise.cli.pipelinewise.tabulate', return_value='checks') as format_table:
        pipelinewise.list_data_diff_checks()

    cells = dict(zip(format_table.call_args.kwargs['headers'], format_table.call_args.args[0][0]))
    assert cells['Full scan'] == ('yes' if initial_full_scan else 'no')
    assert cells['Initial scan pending'] == ('yes' if pending else 'no')
    assert cells['Verified start'] == ''


def test_run_checks_prints_utc_window_and_returns_on_pass(capsys):
    pipelinewise = _pipelinewise()
    with patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=RepositoryContext(),
    ), patch(
        "pipelinewise.cli.pipelinewise.run_due_checks",
        return_value=[_summary()],
    ):
        pipelinewise.run_data_diff_checks()

    assert "target/tap/public/payments" in capsys.readouterr().out
    pipelinewise.alert_sender.send_to_all_handlers.assert_not_called()


def test_run_checks_alerts_and_exits_nonzero_on_mismatch():
    pipelinewise = _pipelinewise()
    with patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=RepositoryContext(),
    ), patch(
        "pipelinewise.cli.pipelinewise.run_due_checks",
        return_value=[_summary("FAIL")],
    ), pytest.raises(SystemExit) as exc:
        pipelinewise.run_data_diff_checks()

    assert exc.value.code == 1
    pipelinewise.alert_sender.send_to_all_handlers.assert_called_once()


def test_run_checks_prints_failure_reason(capsys):
    reason = (
        "row_checksum column 'status' has incompatible source and target types "
        "(missing, missing)"
    )
    summary = {**_summary("ERROR"), "error": reason}
    pipelinewise = _pipelinewise()
    with patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=RepositoryContext(),
    ), patch(
        "pipelinewise.cli.pipelinewise.run_due_checks",
        return_value=[summary],
    ), pytest.raises(SystemExit) as exc:
        pipelinewise.run_data_diff_checks()

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert "Reason" in output
    assert reason in output


@pytest.mark.parametrize('known_window', [False, True])
def test_skipped_check_prints_slot_status_and_reason(capsys, known_window):
    summary = {
        **_summary('SKIPPED'),
        'slot_status': 'RUNNING',
        'error': 'An attempt for this slot is already running.',
    }
    if not known_window:
        summary['window_start'] = None
        summary['window_end'] = None
        summary['run_id'] = None

    PipelineWise._print_data_diff_summaries([summary])

    output = capsys.readouterr().out
    assert 'SKIPPED' in output
    assert 'RUNNING' in output
    assert summary['error'] in output
    assert ('2026-07-22T13:00:00+00:00' in output) == known_window
    assert 'None' not in output


def test_remediation_command_reports_linked_attempt(capsys):
    original_run_id = uuid4()
    summary = _summary()
    pipelinewise = _pipelinewise(
        run_id=str(original_run_id),
        remediation_ref="AP-1234",
    )

    with patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=RepositoryContext(),
    ), patch(
        "pipelinewise.cli.pipelinewise.rerun_failed_check",
        return_value=summary,
    ) as rerun:
        pipelinewise.rerun_data_diff_check()

    output = capsys.readouterr().out
    assert str(original_run_id) in output
    assert str(summary["run_id"]) in output
    assert "AP-1234" in output
    rerun.assert_called_once()


@pytest.mark.parametrize('unresolved', [False, True])
def test_remediation_command_prints_failure_reason(capsys, unresolved):
    reason = "row_checksum column 'status' has incompatible source and target types (missing, missing)"
    pipelinewise = _pipelinewise(run_id=str(uuid4()), remediation_ref="AP-1234")
    summary = {**_summary("ERROR"), "error": reason}
    if unresolved:
        summary['window_start'] = None

    with patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=RepositoryContext(),
    ), patch(
        "pipelinewise.cli.pipelinewise.rerun_failed_check",
        return_value=summary,
    ), pytest.raises(SystemExit) as exc:
        pipelinewise.rerun_data_diff_check()

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert "Reason" in output
    assert reason in output


@pytest.mark.parametrize('historical_scans_pending', [0, 3])
def test_import_persists_definitions_only_after_successful_discovery(historical_scans_pending):
    definition = Mock()
    imported = Mock()
    imported.global_config = {"backend_db": {"host": "backend"}}
    imported.targets = {"target": {"taps": [{"id": "tap"}]}}
    imported.get_data_diff_definitions.return_value = [definition]
    repository = RepositoryContext(historical_scans_pending=historical_scans_pending)
    pipelinewise = _pipelinewise(taps="*")
    pipelinewise.logger = Mock()
    pipelinewise.config = {}
    pipelinewise._discover_tap = Mock(return_value=None)
    pipelinewise.load_config = Mock()
    pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

    with patch(
        "pipelinewise.cli.pipelinewise.Config.from_yamls",
        return_value=imported,
    ), patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=repository,
    ):
        pipelinewise.import_project()

    assert repository.synced == ([definition], ["*"], set())
    summary = next(
        call for call in pipelinewise.logger.info.call_args_list
        if 'IMPORTING YAML CONFIGS FINISHED' in call.args[0]
    )
    assert 'Initial data-diff scans pending' in summary.args[0]
    assert summary.args[6] == historical_scans_pending


def test_import_excludes_definitions_after_discovery_failure():
    definition = Mock(tap_id="tap")
    imported = Mock()
    imported.global_config = {"backend_db": {"host": "backend"}}
    imported.targets = {"target": {"taps": [{"id": "tap"}]}}
    imported.get_data_diff_definitions.return_value = [definition]
    repository = RepositoryContext()
    pipelinewise = _pipelinewise(taps="*")
    pipelinewise.logger = Mock()
    pipelinewise.config = {}
    pipelinewise._discover_tap = Mock(return_value="discovery failed")
    pipelinewise.load_config = Mock()
    pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

    with patch(
        "pipelinewise.cli.pipelinewise.Config.from_yamls",
        return_value=imported,
    ), patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=repository,
    ), pytest.raises(SystemExit):
        pipelinewise.import_project()

    assert repository.synced == ([definition], ["*"], {"tap"})


def test_import_without_backend_does_not_report_zero_pending_scans():
    imported = Mock()
    imported.global_config = {}
    imported.targets = {'target': {'taps': [{'id': 'tap'}]}}
    imported.get_data_diff_definitions.return_value = []
    pipelinewise = _pipelinewise(taps='*')
    pipelinewise.config = {}
    pipelinewise._discover_tap = Mock(return_value=None)
    pipelinewise.load_config = Mock()
    pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

    with patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=imported):
        pipelinewise.import_project()

    summary = next(
        call for call in pipelinewise.logger.info.call_args_list
        if 'IMPORTING YAML CONFIGS FINISHED' in call.args[0]
    )
    assert summary.args[6] == 'not configured'


def test_import_persists_successful_tap_definitions_after_partial_failure():
    successful_definition = Mock(tap_id="successful")
    failed_definition = Mock(tap_id="failed")
    imported = Mock()
    imported.global_config = {"backend_db": {"host": "backend"}}
    imported.targets = {
        "target": {"taps": [{"id": "successful"}, {"id": "failed"}]}
    }
    imported.get_data_diff_definitions.return_value = [
        successful_definition,
        failed_definition,
    ]
    repository = RepositoryContext()
    pipelinewise = _pipelinewise(taps="*")
    pipelinewise.logger = Mock()
    pipelinewise.config = {}
    pipelinewise._discover_tap = Mock(
        side_effect=lambda tap, **_kwargs: (
            "discovery failed" if tap["id"] == "failed" else None
        )
    )
    pipelinewise.load_config = Mock()
    pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

    with patch(
        "pipelinewise.cli.pipelinewise.Config.from_yamls",
        return_value=imported,
    ), patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=repository,
    ), pytest.raises(SystemExit) as exc:
        pipelinewise.import_project()

    assert exc.value.code == 1
    assert repository.synced == (
        [successful_definition, failed_definition],
        ["*"],
        {"failed"},
    )
    pipelinewise.logger.error.assert_called_once_with(
        "Tap discovery failed: %s",
        "discovery failed",
    )


def test_import_deactivates_removed_definition_for_successful_tap_after_partial_failure():
    failed_definition = Mock(tap_id="failed")
    imported = Mock()
    imported.global_config = {"backend_db": {"host": "backend"}}
    imported.targets = {
        "target": {"taps": [{"id": "successful"}, {"id": "failed"}]}
    }
    imported.get_data_diff_definitions.return_value = [failed_definition]
    repository = RepositoryContext()
    pipelinewise = _pipelinewise(taps="*")
    pipelinewise.logger = Mock()
    pipelinewise.config = {}
    pipelinewise._discover_tap = Mock(
        side_effect=lambda tap, **_kwargs: (
            "discovery failed" if tap["id"] == "failed" else None
        )
    )
    pipelinewise.load_config = Mock()
    pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

    with patch(
        "pipelinewise.cli.pipelinewise.Config.from_yamls",
        return_value=imported,
    ), patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=repository,
    ), pytest.raises(SystemExit) as exc:
        pipelinewise.import_project()

    assert exc.value.code == 1
    assert repository.synced == ([failed_definition], ["*"], {"failed"})


def test_import_reconciles_an_explicitly_selected_tap_missing_from_yaml():
    definition = Mock(tap_id="found")
    imported = Mock()
    imported.global_config = {"backend_db": {"host": "backend"}}
    imported.targets = {"target": {"taps": [{"id": "found"}]}}
    imported.get_data_diff_definitions.return_value = [definition]
    repository = RepositoryContext()
    pipelinewise = _pipelinewise(taps="found,missing")
    pipelinewise.logger = Mock()
    pipelinewise.config = {}
    pipelinewise._discover_tap = Mock(return_value=None)
    pipelinewise.load_config = Mock()
    pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

    with patch(
        "pipelinewise.cli.pipelinewise.Config.from_yamls",
        return_value=imported,
    ), patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=repository,
    ), pytest.raises(SystemExit) as exc:
        pipelinewise.import_project()

    assert exc.value.code == 1
    assert repository.synced == (
        [definition],
        ["found", "missing"],
        set(),
    )
    pipelinewise.logger.error.assert_called_once_with(
        "Tap not found in project YAML: %s",
        "missing",
    )


def test_import_rejects_incomplete_parallel_discovery_results():
    imported = Mock()
    imported.global_config = {}
    imported.targets = {
        "target": {"taps": [{"id": "first"}, {"id": "second"}]}
    }
    imported.get_data_diff_definitions.return_value = []
    pipelinewise = _pipelinewise(taps="*")

    with patch(
        "pipelinewise.cli.pipelinewise.Config.from_yamls",
        return_value=imported,
    ), patch("pipelinewise.cli.pipelinewise.Parallel") as parallel, pytest.raises(
        ValueError, match="shorter"
    ):
        parallel.return_value.return_value = [None]
        pipelinewise.import_project()


def test_import_reports_backend_sync_failure_after_partial_discovery():
    successful_definition = Mock(tap_id="successful")
    failed_definition = Mock(tap_id="failed")
    imported = Mock()
    imported.global_config = {"backend_db": {"host": "backend"}}
    imported.targets = {
        "target": {"taps": [{"id": "successful"}, {"id": "failed"}]}
    }
    imported.get_data_diff_definitions.return_value = [
        successful_definition,
        failed_definition,
    ]
    backend_error = RuntimeError("backend unavailable")
    repository = RepositoryContext(sync_error=backend_error)
    pipelinewise = _pipelinewise(taps="*")
    pipelinewise.config = {}
    pipelinewise._discover_tap = Mock(
        side_effect=lambda tap, **_kwargs: (
            "discovery failed" if tap["id"] == "failed" else None
        )
    )
    pipelinewise.load_config = Mock()
    pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

    with patch(
        "pipelinewise.cli.pipelinewise.Config.from_yamls",
        return_value=imported,
    ), patch(
        "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
        return_value=repository,
    ), pytest.raises(SystemExit) as exc:
        pipelinewise.import_project()

    assert exc.value.code == 1
    assert repository.synced == (
        [successful_definition, failed_definition],
        ["*"],
        {"failed"},
    )
    pipelinewise.logger.error.assert_called_once_with(
        "Tap discovery failed: %s",
        "discovery failed",
    )
    pipelinewise.logger.exception.assert_called_once_with(
        "Failed to reconcile data-diff definitions: %s",
        backend_error,
    )
    summary = next(
        call
        for call in pipelinewise.logger.info.call_args_list
        if "IMPORTING YAML CONFIGS FINISHED" in call.args[0]
    )
    assert summary.args[1:6] == (1, 2, 1, 0, "['discovery failed']")
    assert summary.args[6] == 'unavailable'


def _alerting_pipelinewise(taps):
    pipelinewise = _pipelinewise(target="target", tap="tap")
    pipelinewise.config = {"targets": [{"id": "target", "taps": taps}]}
    return pipelinewise


def test_separate_check_definitions_alert_to_the_owning_tap_channel():
    pipelinewise = _alerting_pipelinewise(
        [{"id": "tap", "send_alert": True, "slack_alert_channel": "#tap-owner"}]
    )
    failures = [_summary("FAIL"), _summary("ERROR")]

    returned = pipelinewise._alert_data_diff_failures(
        [_summary("PASS"), _summary("SKIPPED"), _summary("DEFERRED")] + failures
    )

    assert [summary["status"] for summary in returned] == ["FAIL", "ERROR"]
    assert pipelinewise.alert_sender.send_to_all_handlers.call_count == 2

    for call, summary in zip(
        pipelinewise.alert_sender.send_to_all_handlers.call_args_list, failures
    ):
        assert call.kwargs["tap_slack_channel"] == "#tap-owner"
        assert 'tap/public.payments' in call.kwargs['message']
        assert str(summary['run_id']) in call.kwargs['details']
        assert summary['window_start'].strftime('%Y-%m-%d %H:%M:%S') in call.kwargs['details']
        assert call.kwargs['data_diff'] is True
        assert call.kwargs['level'] == 'error'


def test_backlogged_check_sends_one_alert_and_retains_every_failed_attempt():
    pipelinewise = _alerting_pipelinewise(
        [{'id': 'tap', 'send_alert': True, 'slack_alert_channel': '#tap-owner'}]
    )
    check = _stored_check()
    instant = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    failures = [
        _summary(
            'ERROR', check=check,
            scheduled_for=instant + timedelta(hours=offset),
            window_start=instant + timedelta(hours=offset - 1),
            window_end=instant + timedelta(hours=offset),
            trigger_type='RETRY' if offset < 24 else 'SCHEDULED',
            preflight={'status': 'BLOCKED', 'has_leading_index': False},
            error="No usable source index starts with timestamp column 'updated_at'",
        )
        for offset in range(25)
    ]

    returned = pipelinewise._alert_data_diff_failures([
        _summary('PASS', check=check), *failures,
        _summary('SKIPPED', check=check), _summary('DEFERRED', check=check),
    ])

    assert len(returned) == 25
    assert all(actual is expected for actual, expected in zip(returned, failures))
    pipelinewise.alert_sender.send_to_all_handlers.assert_called_once()
    kwargs = pipelinewise.alert_sender.send_to_all_handlers.call_args.kwargs
    assert kwargs['tap_slack_channel'] == '#tap-owner'
    assert '25 failed windows' in kwargs['message']
    fields = dict(
        (label.strip(), value.strip())
        for label, separator, value in (line.partition(':') for line in kwargs['details'].splitlines())
        if separator
    )
    assert fields['failed_windows'] == '25'
    assert fields['failed_attempts'] == '25'
    assert fields['failure_statuses'] == 'ERROR=25'
    assert fields['shown_window'] == 'representative failure'
    assert fields['run_id'] == str(failures[-1]['run_id'])
    assert fields['attempt'] == '2 (SCHEDULED)'


@pytest.mark.parametrize('statuses,representative_index', [
    (['FAIL', 'FAIL'], 1),
    (['ERROR', 'FAIL'], 0),
    (['ERROR', 'FAIL', 'ERROR', 'FAIL'], 2),
])
def test_grouped_alert_prefers_latest_execution_error_over_mismatch(statuses, representative_index):
    pipelinewise = _alerting_pipelinewise([{'id': 'tap'}])
    check = _stored_check()
    failures = [
        _summary(status, check=check, error=f'failure-{offset}')
        for offset, status in enumerate(statuses)
    ]

    returned = pipelinewise._alert_data_diff_failures(failures)

    assert returned == failures
    pipelinewise.alert_sender.send_to_all_handlers.assert_called_once()
    kwargs = pipelinewise.alert_sender.send_to_all_handlers.call_args.kwargs
    representative = failures[representative_index]
    assert kwargs['message'].startswith(representative['status'])
    assert str(representative['run_id']) in kwargs['details']
    assert representative['error'] in kwargs['details']
    assert all(
        str(summary['run_id']) not in kwargs['details']
        for summary in failures if summary is not representative
    )


@pytest.mark.parametrize('identity_change', ['check_id', 'tap_id', 'target_id'])
def test_check_or_owning_tap_or_target_boundaries_are_not_merged(identity_change):
    pipelinewise = _alerting_pipelinewise([{'id': 'tap'}, {'id': 'other-tap'}])
    first = _stored_check()
    changed_identity = {'check_id': uuid4(), 'tap_id': 'other-tap', 'target_id': 'other-target'}
    second = {**first, identity_change: changed_identity[identity_change]}
    if identity_change == 'target_id':
        pipelinewise.config['targets'].append({'id': second['target_id'], 'taps': [{'id': 'tap'}]})
    summaries = [_summary('ERROR', check=first), _summary('ERROR', check=second)]

    assert pipelinewise._alert_data_diff_failures(summaries) == summaries

    assert pipelinewise.alert_sender.send_to_all_handlers.call_count == 2


def test_different_definition_revisions_keep_separate_alerts_for_the_same_table():
    pipelinewise = _alerting_pipelinewise([{'id': 'tap'}])
    current = _stored_check()
    inactive = {**current, 'check_id': uuid4(), 'revision': 1, 'is_current': False}
    failures = [_summary('ERROR', check=inactive), _summary('FAIL', check=current)]

    assert pipelinewise._alert_data_diff_failures(failures) == failures

    assert pipelinewise.alert_sender.send_to_all_handlers.call_count == 2
    calls = pipelinewise.alert_sender.send_to_all_handlers.call_args_list
    assert str(failures[0]['run_id']) in calls[0].kwargs['details']
    assert str(failures[1]['run_id']) in calls[1].kwargs['details']


def test_checks_without_stored_ids_are_grouped_by_full_name():
    pipelinewise = _alerting_pipelinewise([{'id': 'tap'}])
    first = _stored_check()
    first.pop('check_id')
    second = {**first, 'source_table': 'other', 'full_check_name': 'target/tap/public/other'}
    failures = [
        _summary('ERROR', check=first), _summary('ERROR', check={**first}),
        _summary('FAIL', check=second),
    ]

    assert pipelinewise._alert_data_diff_failures(failures) == failures

    assert pipelinewise.alert_sender.send_to_all_handlers.call_count == 2
    calls = pipelinewise.alert_sender.send_to_all_handlers.call_args_list
    assert str(failures[1]['run_id']) in calls[0].kwargs['details']
    assert str(failures[2]['run_id']) in calls[1].kwargs['details']


def test_alert_limit_resets_between_invocations():
    pipelinewise = _alerting_pipelinewise([{'id': 'tap'}])
    check = _stored_check()
    failures = [_summary('ERROR', check=check), _summary('ERROR', check=check)]

    assert pipelinewise._alert_data_diff_failures(failures) == failures
    assert pipelinewise._alert_data_diff_failures(failures) == failures

    assert pipelinewise.alert_sender.send_to_all_handlers.call_count == 2


def test_grouped_failures_on_muted_taps_remain_returned_without_alerts():
    pipelinewise = _alerting_pipelinewise([{'id': 'tap', 'send_alert': False}])
    check = _stored_check()
    failures = [_summary('FAIL', check=check), _summary('ERROR', check=check)]

    assert pipelinewise._alert_data_diff_failures(failures) == failures

    pipelinewise.alert_sender.send_to_all_handlers.assert_not_called()


def test_grouped_failures_for_missing_taps_still_send_one_default_alert():
    pipelinewise = _alerting_pipelinewise([{'id': 'another-tap'}])
    check = _stored_check()
    failures = [_summary('FAIL', check=check), _summary('ERROR', check=check)]

    assert pipelinewise._alert_data_diff_failures(failures) == failures

    pipelinewise.alert_sender.send_to_all_handlers.assert_called_once()
    assert pipelinewise.alert_sender.send_to_all_handlers.call_args.kwargs['tap_slack_channel'] is None
    pipelinewise.logger.warning.assert_called_once()


def test_failed_check_alert_includes_failure_reason():
    reason = "row_checksum column 'status' has incompatible source and target types (missing, missing)"
    pipelinewise = _alerting_pipelinewise([{"id": "tap"}])

    pipelinewise._alert_data_diff_failures([{**_summary("ERROR"), "error": reason}])

    details = pipelinewise.alert_sender.send_to_all_handlers.call_args.kwargs['details']
    assert reason in details


def test_send_alert_disabled_on_the_tap_silences_its_checks():
    pipelinewise = _alerting_pipelinewise([{"id": "tap", "send_alert": False}])

    returned = pipelinewise._alert_data_diff_failures([_summary("FAIL")])

    assert len(returned) == 1
    pipelinewise.alert_sender.send_to_all_handlers.assert_not_called()


def test_missing_tap_still_alerts_without_a_custom_channel():
    pipelinewise = _alerting_pipelinewise([{"id": "another-tap"}])

    pipelinewise._alert_data_diff_failures([_summary("FAIL")])

    call = pipelinewise.alert_sender.send_to_all_handlers.call_args
    assert call.kwargs["tap_slack_channel"] is None
    pipelinewise.logger.warning.assert_called_once()


@pytest.mark.parametrize('status', ['PASS', 'DEFERRED'])
def test_passing_or_deferred_summaries_alert_nobody(status):
    pipelinewise = _alerting_pipelinewise([{"id": "tap"}])

    assert pipelinewise._alert_data_diff_failures([_summary(status)]) == []
    pipelinewise.alert_sender.send_to_all_handlers.assert_not_called()
