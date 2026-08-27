"""Tests for the format-aware FastSync capability policy."""
import pytest

from pipelinewise.cli import fastsync_capabilities as capability_policy
from pipelinewise.cli import pipelinewise as pipelinewise_module
from pipelinewise.cli.constants import ConnectorType
from pipelinewise.cli.fastsync_capabilities import (
    FASTSYNC_PAIRS,
    ICEBERG_FASTSYNC_PAIRS,
    PARTIAL_SYNC_PAIRS,
    FastSyncCapabilities,
    resolve_fastsync_capabilities,
)
from pipelinewise.cli.config import Config


@pytest.mark.parametrize(
    ('table_format', 'tap_type', 'target_type', 'expected'),
    [
        (
            None,
            'tap-mysql',
            'target-snowflake',
            FastSyncCapabilities(full_sync=True, partial_sync=True),
        ),
        (
            'native',
            'tap-mysql',
            'target-postgres',
            FastSyncCapabilities(full_sync=True),
        ),
        (
            'native',
            'tap-postgres',
            'target-snowflake',
            FastSyncCapabilities(full_sync=True, partial_sync=True),
        ),
        (
            'native',
            'tap-postgres',
            'target-postgres',
            FastSyncCapabilities(full_sync=True),
        ),
        (
            'native',
            'tap-mongodb',
            'target-snowflake',
            FastSyncCapabilities(full_sync=True),
        ),
        (
            'native',
            'tap-mongodb',
            'target-postgres',
            FastSyncCapabilities(full_sync=True),
        ),
        (
            'iceberg',
            'tap-mysql',
            'target-snowflake',
            FastSyncCapabilities(full_sync=True, partial_sync=True),
        ),
        (
            'iceberg',
            'tap-postgres',
            'target-snowflake',
            FastSyncCapabilities(full_sync=True, partial_sync=True),
        ),
        (
            'iceberg',
            'tap-mongodb',
            'target-snowflake',
            FastSyncCapabilities(),
        ),
        (
            'iceberg',
            'tap-salesforce',
            'target-snowflake',
            FastSyncCapabilities(),
        ),
        (
            'iceberg',
            'tap-mysql',
            'target-postgres',
            FastSyncCapabilities(),
        ),
    ],
)
def test_resolve_fastsync_capabilities(
    table_format, tap_type, target_type, expected
):
    """The matrix preserves native routes and gates Iceberg by source and target."""
    assert (
        resolve_fastsync_capabilities(tap_type, target_type, table_format)
        == expected
    )


def test_unknown_format_is_rejected():
    """An invalid format cannot silently fall back to native execution."""
    with pytest.raises(ValueError, match='Unsupported target table format'):
        resolve_fastsync_capabilities(
            'tap-mysql', 'target-snowflake', 'future-format'
        )


@pytest.mark.parametrize(
    ('operation', 'expected'),
    [
        (None, True),
        ('full_sync', True),
        ('partial_sync', False),
    ],
)
def test_capabilities_support_operation(operation, expected):
    """Operation lookup is owned by the immutable capability value."""
    capabilities = FastSyncCapabilities(full_sync=True)

    assert capabilities.supports(operation) is expected


def test_unknown_operation_is_rejected():
    """Unknown operation names cannot silently reuse another capability."""
    with pytest.raises(ValueError, match='Unsupported FastSync operation'):
        FastSyncCapabilities().supports('future_sync')


def test_capability_registry_is_immutable():
    """Runtime code cannot alter the process-wide routing policy."""
    key = (
        'native',
        ConnectorType.TAP_MYSQL,
        ConnectorType.TARGET_SNOWFLAKE,
    )

    with pytest.raises(TypeError):
        capability_policy._FASTSYNC_CAPABILITIES[key] = FastSyncCapabilities()  # pylint: disable=protected-access


def test_legacy_pair_views_are_immutable():
    """Legacy imports retain their values without becoming policy authorities."""
    assert FASTSYNC_PAIRS == {
        ConnectorType.TAP_MYSQL: frozenset(
            {ConnectorType.TARGET_SNOWFLAKE, ConnectorType.TARGET_POSTGRES}
        ),
        ConnectorType.TAP_POSTGRES: frozenset(
            {ConnectorType.TARGET_SNOWFLAKE, ConnectorType.TARGET_POSTGRES}
        ),
        ConnectorType.TAP_MONGODB: frozenset(
            {ConnectorType.TARGET_SNOWFLAKE, ConnectorType.TARGET_POSTGRES}
        ),
    }
    assert ICEBERG_FASTSYNC_PAIRS == {
        ConnectorType.TAP_MYSQL: frozenset({ConnectorType.TARGET_SNOWFLAKE}),
        ConnectorType.TAP_POSTGRES: frozenset(
            {ConnectorType.TARGET_SNOWFLAKE}
        ),
    }
    assert PARTIAL_SYNC_PAIRS == {
        ConnectorType.TAP_MYSQL: frozenset({ConnectorType.TARGET_SNOWFLAKE}),
        ConnectorType.TAP_POSTGRES: frozenset(
            {ConnectorType.TARGET_SNOWFLAKE}
        ),
    }
    assert pipelinewise_module.FASTSYNC_PAIRS is FASTSYNC_PAIRS
    assert pipelinewise_module.ICEBERG_FASTSYNC_PAIRS is ICEBERG_FASTSYNC_PAIRS
    assert pipelinewise_module.PARTIAL_SYNC_PAIRS is PARTIAL_SYNC_PAIRS

    with pytest.raises(TypeError):
        FASTSYNC_PAIRS[ConnectorType.TAP_MYSQL] = frozenset()
    with pytest.raises(AttributeError):
        FASTSYNC_PAIRS[ConnectorType.TAP_MYSQL].add(
            ConnectorType.TARGET_SNOWFLAKE
        )


def test_singer_iceberg_keeps_flattening():
    """A Singer-only Iceberg route may retain nested source structures."""
    tap = {
        'id': 'test_tap',
        'type': 'tap-mongodb',
        'hard_delete': True,
        'target_table_format': 'iceberg',
        'iceberg_version': 3,
        'data_flattening_max_level': 3,
    }
    target = {'id': 'test_target', 'type': 'target-snowflake'}

    Config.validate_target_table_format(tap, target)
