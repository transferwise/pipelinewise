"""E2E diagnostics must expose command failures that occur before workers start."""

from unittest.mock import patch

import pytest

from tests.end_to_end.helpers import assertions


@pytest.mark.parametrize('assert_success', [
    assertions.assert_run_tap_success, assertions.assert_resync_tables_success,
])
def test_preflight_error_is_reported(assert_success, capsys):
    """A pre-worker rejection must expose the CLI error even without engine logs."""
    with patch.object(
        assertions.tasks, 'run_command', return_value=[1, 'preflight output', 'slot name is too long'],
    ), patch.object(assertions.tasks, 'assert_run_tap_log_engines') as check_engines:
        with pytest.raises(AssertionError):
            assert_success('postgres_to_sf', 'snowflake', sync_engines=('fastsync',))

    check_engines.assert_not_called()
    output = capsys.readouterr().out
    assert 'preflight output' in output
    assert 'slot name is too long' in output
    assert 'FAILED LOGS: <none found>' in output
