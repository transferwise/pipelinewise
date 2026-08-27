"""Crash-ordering tests for Snowflake Iceberg recovery coordination."""

from unittest.mock import patch

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    PHASE_STAGED,
    PHASE_SUBMITTED,
    TARGET_ATTEMPT_ABORTING,
    RecoveryManifestError,
    SnowflakeIcebergPublisher,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    RECOVERY_IDENTITY,
    FakeSnowflake,
    make_attempt,
    persist_attempt,
)


def test_abort_recovers_pre_manifest_delete(tmp_path, spec):
    """An abort marker makes a pre-deletion crash safely recoverable."""
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    attempt = make_attempt(spec, phase=PHASE_STAGED)
    persist_attempt(publisher, attempt)
    target_store = publisher.recovery_store(spec.name)
    stream_store = publisher.recovery_store(spec.name, RECOVERY_IDENTITY)

    with patch.object(
        stream_store,
        'delete',
        side_effect=RuntimeError('manifest delete interrupted'),
    ), pytest.raises(RuntimeError, match='manifest delete interrupted'):
        publisher.abort(attempt)

    assert (
        target_store.load_fastsync_target_pointer().state
        == TARGET_ATTEMPT_ABORTING
    )
    assert stream_store.load().attempt_id == attempt.attempt_id
    assert publisher.load_attempt(
        spec,
        expected_kind='full',
        recovery_identity=RECOVERY_IDENTITY,
    ) is None
    assert stream_store.load() is None
    assert target_store.load_fastsync_target_pointer() is None


def test_abort_recovers_after_manifest_delete(
    tmp_path,
    spec,
):
    """An abort marker is removable after its stream manifest is gone."""
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    attempt = make_attempt(spec, phase=PHASE_STAGED)
    persist_attempt(publisher, attempt)
    target_store = publisher.recovery_store(spec.name)
    stream_store = publisher.recovery_store(spec.name, RECOVERY_IDENTITY)

    with patch.object(
        target_store,
        'delete_fastsync_target_pointer',
        side_effect=RuntimeError('pointer delete interrupted'),
    ), pytest.raises(RuntimeError, match='pointer delete interrupted'):
        publisher.abort(attempt)

    assert (
        target_store.load_fastsync_target_pointer().state
        == TARGET_ATTEMPT_ABORTING
    )
    assert stream_store.load() is None
    assert publisher.load_attempt(
        spec,
        expected_kind='full',
        recovery_identity=RECOVERY_IDENTITY,
    ) is None
    assert target_store.load_fastsync_target_pointer() is None


def test_abort_rejects_submitted_manifest(tmp_path, spec):
    """An abort marker cannot discard an attempt that reached submission."""
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    attempt = make_attempt(spec, phase=PHASE_SUBMITTED)
    persist_attempt(
        publisher,
        attempt,
        pointer_state=TARGET_ATTEMPT_ABORTING,
    )

    with pytest.raises(
        RecoveryManifestError,
        match='Aborting.*unsafe stream manifest',
    ):
        publisher.load_attempt(
            spec,
            expected_kind='full',
            recovery_identity=RECOVERY_IDENTITY,
        )

    assert publisher.recovery_store(
        spec.name,
        RECOVERY_IDENTITY,
    ).load() is not None
    assert (
        publisher.recovery_store(spec.name).load_fastsync_target_pointer().state
        == TARGET_ATTEMPT_ABORTING
    )
