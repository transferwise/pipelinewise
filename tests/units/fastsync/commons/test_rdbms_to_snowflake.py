"""Tests for the shared RDBMS-to-Snowflake FullSync lifecycle."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pipelinewise.fastsync.commons import rdbms_to_snowflake


def test_cleanup_failure_is_logged():
    """Local cleanup debt must not replace the FullSync result."""
    cleanup_error = PermissionError('denied')
    run = SimpleNamespace(
        file_parts=['/tmp/export.csv.gz.part0'],
        logger=MagicMock(),
    )

    with patch.object(
        rdbms_to_snowflake.os,
        'remove',
        side_effect=cleanup_error,
    ):
        rdbms_to_snowflake._cleanup_full_export(run)  # pylint: disable=protected-access

    run.logger.warning.assert_called_once_with(
        'Failed to remove local FastSync export %s: %s',
        '/tmp/export.csv.gz.part0',
        cleanup_error,
    )
