import json

from datetime import datetime, timezone
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


def _summary(status="PASS"):
    instant = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    return {
        "check": _stored_check(),
        "status": status,
        "window_start": instant,
        "window_end": instant,
        "run_id": uuid4(),
        "attempt": 2,
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


def test_each_failed_window_alerts_to_the_owning_tap_channel():
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
        assert summary["check"]["full_check_name"] in call.kwargs["message"]
        assert str(summary["run_id"]) in call.kwargs["message"]
        assert summary["window_start"].isoformat() in call.kwargs["message"]


def test_failed_check_alert_includes_failure_reason():
    reason = "row_checksum column 'status' has incompatible source and target types (missing, missing)"
    pipelinewise = _alerting_pipelinewise([{"id": "tap"}])

    pipelinewise._alert_data_diff_failures([{**_summary("ERROR"), "error": reason}])

    message = pipelinewise.alert_sender.send_to_all_handlers.call_args.kwargs["message"]
    assert f"reason  {reason}" in message


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
