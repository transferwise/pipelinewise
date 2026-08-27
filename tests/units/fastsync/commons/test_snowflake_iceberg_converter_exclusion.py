"""Cross-workflow recovery exclusion tests for manual Iceberg conversion."""

from argparse import Namespace
from pathlib import Path

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_routes
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    TARGET_ATTEMPT_ABORTING,
    TARGET_ATTEMPT_ACTIVE,
    TARGET_ATTEMPT_COMPLETED,
    TARGET_ATTEMPT_RESERVED,
    IcebergRecoveryStore,
    IcebergTargetAttemptPointer,
    SnowflakeObjectName,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    NativeToIcebergConversionError,
    SnowflakeNativeToIcebergConverter,
)
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import (
    FakeSnowflake,
)


def _pointer(target, state):
    return IcebergTargetAttemptPointer(
        target=target,
        stream_fingerprint='a' * 64,
        recovery_fingerprint='b' * 64,
        kind='full',
        state=state,
    )


@pytest.mark.parametrize(
    'state',
    (
        TARGET_ATTEMPT_ABORTING,
        TARGET_ATTEMPT_ACTIVE,
        TARGET_ATTEMPT_COMPLETED,
        TARGET_ATTEMPT_RESERVED,
    ),
)
def test_fastsync_pointer_blocks_converter(tmp_path, state):
    """Every live FastSync pointer leaves its own workflow in control."""
    target = SnowflakeObjectName.parse('DATABASE.SCHEMA.TABLE')
    store = IcebergRecoveryStore(str(tmp_path), target)
    store.save_fastsync_target_pointer(_pointer(target, state))
    Path(store.path).write_text('{invalid manual manifest', encoding='utf-8')
    pointer_before = Path(store.fastsync_target_pointer_path).read_bytes()
    manifest_before = Path(store.path).read_bytes()
    snowflake = FakeSnowflake()

    with pytest.raises(
        NativeToIcebergConversionError,
        match='FastSync Iceberg attempt is pending recovery',
    ):
        SnowflakeNativeToIcebergConverter(snowflake, str(tmp_path)).convert(
            'database.schema.table'
        )

    assert snowflake.queries == []
    assert Path(store.fastsync_target_pointer_path).read_bytes() == pointer_before
    assert Path(store.path).read_bytes() == manifest_before


def test_paths_share_recovery_store(tmp_path):
    """FastSync and manual conversion resolve the same target-scoped store."""
    target_dir = tmp_path / 'target'
    target = SnowflakeObjectName.parse('DATABASE.SCHEMA.TABLE')
    publisher = snowflake_iceberg_routes.create_publisher(
        FakeSnowflake(),
        Namespace(
            state=str(target_dir / 'tap' / 'state.json'),
            temp_dir=str(tmp_path / 'tmp'),
        ),
    )
    publisher.recovery_store(target).save_fastsync_target_pointer(
        _pointer(target, TARGET_ATTEMPT_ACTIVE)
    )
    snowflake = FakeSnowflake()

    with pytest.raises(
        NativeToIcebergConversionError,
        match='FastSync Iceberg attempt is pending recovery',
    ):
        SnowflakeNativeToIcebergConverter(snowflake, str(target_dir)).convert(
            'database.schema.table'
        )

    assert publisher.runtime_dir == str(target_dir)
    assert snowflake.queries == []


def test_other_target_pointer_is_ignored(tmp_path):
    """The target-keyed sidecar excludes only its exact target table."""
    other_target = SnowflakeObjectName.parse('DATABASE.SCHEMA.OTHER_TABLE')
    other_store = IcebergRecoveryStore(str(tmp_path), other_target)
    other_store.save_fastsync_target_pointer(
        _pointer(other_target, TARGET_ATTEMPT_ACTIVE)
    )
    pointer_before = Path(other_store.fastsync_target_pointer_path).read_bytes()
    snowflake = FakeSnowflake()

    SnowflakeNativeToIcebergConverter(snowflake, str(tmp_path)).convert(
        'database.schema.table'
    )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert Path(other_store.fastsync_target_pointer_path).read_bytes() == pointer_before
