import json

from datetime import datetime, timezone
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs


class RepositoryContext:
    def __init__(self, checks=None):
        self.checks = checks or []
        self.filters = None
        self.synced = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def list_checks(self, **filters):
        self.filters = filters
        return self.checks

    def sync_definitions(self, definitions, *, selected_taps):
        self.synced = (definitions, selected_taps)
        return {"created": len(definitions)}


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
        "id": uuid4(),
        "revision": 2,
        "current": True,
        "enabled": True,
        "target_id": "target",
        "tap_id": "tap",
        "source_schema": "public",
        "source_table": "payments",
        "checks": ["row_count"],
        "source_key_column": "id",
        "source_timestamp_column": "updated_at",
        "source_compare_columns": [],
        "frequency_seconds": 3600,
        "duration_seconds": 3600,
        "settling_delay_seconds": 21600,
        "name": "payments-check",
        "full_check_name": "target/tap/public/payments/payments-check",
        "coverage_status": None,
        "verified_through": None,
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
    assert payload[0]["name"] == "payments-check"
    assert repository.filters == {
        "target_id": "target",
        "tap_id": "tap",
        "include_versioned": True,
    }


def test_run_checks_prints_utc_window_and_returns_on_pass(capsys):
    pipelinewise = _pipelinewise()
    with (
        patch(
            "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
            return_value=RepositoryContext(),
        ),
        patch(
            "pipelinewise.cli.pipelinewise.run_due_checks",
            return_value=[_summary()],
        ),
    ):
        pipelinewise.run_data_diff_checks()

    assert "payments-check" in capsys.readouterr().out
    pipelinewise.alert_sender.send_to_all_handlers.assert_not_called()


def test_run_checks_alerts_and_exits_nonzero_on_mismatch():
    pipelinewise = _pipelinewise()
    with (
        patch(
            "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
            return_value=RepositoryContext(),
        ),
        patch(
            "pipelinewise.cli.pipelinewise.run_due_checks",
            return_value=[_summary("FAIL")],
        ),
        pytest.raises(SystemExit) as exc,
    ):
        pipelinewise.run_data_diff_checks()

    assert exc.value.code == 1
    pipelinewise.alert_sender.send_to_all_handlers.assert_called_once()


def test_remediation_command_reports_linked_attempt(capsys):
    original_run_id = uuid4()
    summary = _summary()
    pipelinewise = _pipelinewise(
        run_id=str(original_run_id),
        remediation_ref="AP-1234",
    )

    with (
        patch(
            "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
            return_value=RepositoryContext(),
        ),
        patch(
            "pipelinewise.cli.pipelinewise.rerun_failed_check",
            return_value=summary,
        ) as rerun,
    ):
        pipelinewise.rerun_data_diff_check()

    output = capsys.readouterr().out
    assert str(original_run_id) in output
    assert str(summary["run_id"]) in output
    assert "AP-1234" in output
    rerun.assert_called_once()


def test_import_persists_definitions_only_after_successful_discovery():
    definition = Mock()
    imported = Mock()
    imported.global_config = {"backend_db": {"host": "backend"}}
    imported.targets = {"target": {"taps": [{"id": "tap"}]}}
    imported.get_data_diff_definitions.return_value = [definition]
    repository = RepositoryContext()
    pipelinewise = _pipelinewise(taps="*")
    pipelinewise.logger = Mock()
    pipelinewise.config = {}
    pipelinewise._discover_tap = Mock(return_value=None)
    pipelinewise.load_config = Mock()
    pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

    with (
        patch(
            "pipelinewise.cli.pipelinewise.Config.from_yamls",
            return_value=imported,
        ),
        patch(
            "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
            return_value=repository,
        ),
    ):
        pipelinewise.import_project()

    assert repository.synced == ([definition], ["*"])


def test_import_does_not_activate_definitions_after_discovery_failure():
    imported = Mock()
    imported.global_config = {"backend_db": {"host": "backend"}}
    imported.targets = {"target": {"taps": [{"id": "tap"}]}}
    imported.get_data_diff_definitions.return_value = [Mock()]
    repository = RepositoryContext()
    pipelinewise = _pipelinewise(taps="*")
    pipelinewise.logger = Mock()
    pipelinewise.config = {}
    pipelinewise._discover_tap = Mock(return_value="discovery failed")
    pipelinewise.load_config = Mock()
    pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

    with (
        patch(
            "pipelinewise.cli.pipelinewise.Config.from_yamls",
            return_value=imported,
        ),
        patch(
            "pipelinewise.cli.pipelinewise.DataDiffRepository.from_backend_config",
            return_value=repository,
        ),
        pytest.raises(SystemExit),
    ):
        pipelinewise.import_project()

    assert repository.synced is None


def _alerting_pipelinewise(taps):
    pipelinewise = _pipelinewise(target="target", tap="tap")
    pipelinewise.config = {"targets": [{"id": "target", "taps": taps}]}
    return pipelinewise


def test_each_failed_window_alerts_to_the_owning_tap_channel():
    pipelinewise = _alerting_pipelinewise([{"id": "tap", "send_alert": True, "slack_alert_channel": "#tap-owner"}])
    failures = [_summary("FAIL"), _summary("ERROR")]

    returned = pipelinewise._alert_data_diff_failures([_summary("PASS"), _summary("SKIPPED")] + failures)

    assert [summary["status"] for summary in returned] == ["FAIL", "ERROR"]
    assert pipelinewise.alert_sender.send_to_all_handlers.call_count == 2

    for call, summary in zip(pipelinewise.alert_sender.send_to_all_handlers.call_args_list, failures):
        assert call.kwargs["tap_slack_channel"] == "#tap-owner"
        assert summary["check"]["full_check_name"] in call.kwargs["message"]
        assert str(summary["run_id"]) in call.kwargs["message"]
        assert summary["window_start"].isoformat() in call.kwargs["message"]


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


def test_passing_summaries_alert_nobody():
    pipelinewise = _alerting_pipelinewise([{"id": "tap"}])

    assert pipelinewise._alert_data_diff_failures([_summary("PASS")]) == []
    pipelinewise.alert_sender.send_to_all_handlers.assert_not_called()
