"""Tests for actionable Snowflake Iceberg recovery failures."""

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_routes as routes
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    AmbiguousPublicationError,
    PHASE_FINALIZED,
    PHASE_PUBLISHED,
    PUBLICATION_PARTIAL_BOOTSTRAP_CTAS,
    QueryHistoryLookupError,
    QueryHistoryVisibilityTimeoutError,
    IcebergPublicationAttempt,
    RecoveryManifestError,
    SnowflakeIcebergPublisher,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    RetryableQueryHistoryRecoveryError,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    FakeSnowflake,
    make_attempt,
    v3_snapshot,
)


@pytest.mark.parametrize(
    'error',
    (
        QueryHistoryLookupError('attempt-1', 0.25, 1),
        QueryHistoryVisibilityTimeoutError(
            'attempt-1', 3.0, 3, ('running',)
        ),
    ),
)
def test_retryable_error_is_actionable(error):
    """Expected ambiguity is concise and tells the operator how to recover."""
    logger = Mock()

    result = routes.publication_failure_result(logger, 'orders', error)

    assert isinstance(error, RetryableQueryHistoryRecoveryError)
    assert isinstance(error, AmbiguousPublicationError)
    assert error.retryable is True
    assert 'Publication status remains ambiguous' in result
    assert 'recovery manifest and staging table were preserved' in result
    assert 'retry the same FastSync command unchanged' in result
    logger.error.assert_called_once_with('%s: %s', 'orders', error)
    logger.exception.assert_not_called()


def test_unexpected_failure_logs_traceback():
    """Unexpected failures continue through the traceback-bearing log path."""
    logger = Mock()
    error = RuntimeError('unexpected failure')

    result = routes.publication_failure_result(logger, 'orders', error)

    assert result == 'orders: unexpected failure'
    logger.exception.assert_called_once_with(error)
    logger.error.assert_not_called()


def test_mismatch_reports_aggregate_evidence(tmp_path, spec):
    """Mismatch errors distinguish count and content evidence safely."""
    snowflake = FakeSnowflake([[
        {'ROW_COUNT': 6, 'ROW_FINGERPRINT': 'actual-hash'},
    ]])
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
    publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
    attempt = make_attempt(
        spec,
        phase=PHASE_PUBLISHED,
        kind='partial',
        method=PUBLICATION_PARTIAL_BOOTSTRAP_CTAS,
    )
    attempt.expected_row_count = 7
    attempt.expected_row_fingerprint = 'expected-hash'

    with pytest.raises(RecoveryManifestError) as exc_info:
        publisher._verify_published(attempt, spec)  # pylint: disable=protected-access

    assert str(exc_info.value) == (
        'Published Iceberg target contents do not match staging: '
        'expected_row_count=7, actual_row_count=6, '
        'expected_row_fingerprint=expected-hash, '
        'actual_row_fingerprint=actual-hash'
    )


@pytest.mark.parametrize(
    'finalization',
    (
        {'s3_cleanup': False},
        {'s3_cleanup': 'false'},
        {'s3_cleanup': 1},
        {'unknown_action': True},
    ),
)
def test_manifest_rejects_invalid_progress(spec, finalization):
    """Recovery never treats malformed finalization state as completed."""
    manifest = make_attempt(spec, phase=PHASE_PUBLISHED).as_dict()
    manifest['finalization'] = finalization

    with pytest.raises(
        RecoveryManifestError,
        match='finalization state is invalid',
    ):
        IcebergPublicationAttempt.from_dict(manifest)


def test_finalization_rejects_unknown_action(tmp_path, spec):
    """Only registered finalization actions can become durable."""
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    attempt = make_attempt(spec, phase=PHASE_PUBLISHED)

    with pytest.raises(
        RecoveryManifestError,
        match='Unsupported Iceberg finalization action',
    ):
        publisher.mark_finalized(attempt, ['unknown_action'])

    assert attempt.phase == PHASE_PUBLISHED
    assert attempt.finalization == {}


def test_rejects_inapplicable_finalization(tmp_path, spec):
    """A non-replacement attempt cannot record metadata restoration progress."""
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    attempt = make_attempt(spec, phase=PHASE_PUBLISHED)

    with pytest.raises(
        RecoveryManifestError,
        match='finalization actions are inconsistent with phase published',
    ):
        publisher.record_finalization_action(
            attempt,
            routes.FINALIZATION_METADATA,
        )

    assert attempt.finalization == {}


def test_finalization_requires_all_actions(tmp_path, spec):
    """A production attempt cannot finalize with only a subset of its actions."""
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    attempt = make_attempt(spec, phase=PHASE_PUBLISHED)
    attempt.finalization = {
        routes.FINALIZATION_GRANTS: True,
        routes.FINALIZATION_S3_CLEANUP: True,
    }

    with pytest.raises(
        RecoveryManifestError,
        match='finalization actions are incomplete',
    ):
        publisher.mark_finalized(attempt)

    assert attempt.phase == PHASE_PUBLISHED


def test_replacement_requires_metadata(tmp_path, spec):
    """Replacement metadata restoration is part of the exact action contract."""
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    attempt = make_attempt(
        spec,
        phase=PHASE_PUBLISHED,
        context={'replacement_metadata': {}},
    )
    attempt.finalization = {
        routes.FINALIZATION_GRANTS: True,
        routes.FINALIZATION_S3_CLEANUP: True,
        routes.FINALIZATION_STAGING_CLEANUP: True,
    }

    with pytest.raises(
        RecoveryManifestError,
        match='missing=metadata',
    ):
        publisher.mark_finalized(attempt)

    assert attempt.phase == PHASE_PUBLISHED


def test_finalized_rejects_missing_actions(tmp_path, spec):
    """Malformed terminal progress cannot reconcile or remove recovery state."""
    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    attempt = make_attempt(spec, phase=PHASE_FINALIZED)
    attempt.finalization = {
        routes.FINALIZATION_GRANTS: True,
        routes.FINALIZATION_S3_CLEANUP: True,
        routes.FINALIZATION_STAGING_CLEANUP: True,
    }
    serialized = attempt.as_dict()
    serialized['finalization'].pop(routes.FINALIZATION_STAGING_CLEANUP)
    attempt.finalization.pop(routes.FINALIZATION_STAGING_CLEANUP)

    with pytest.raises(
        RecoveryManifestError,
        match='finalization actions are incomplete',
    ):
        IcebergPublicationAttempt.from_dict(serialized)
    with pytest.raises(
        RecoveryManifestError,
        match='finalization actions are incomplete',
    ):
        publisher.reconcile(attempt)
    with pytest.raises(
        RecoveryManifestError,
        match='finalization actions are incomplete',
    ):
        publisher.complete_state_handoff(attempt)


def test_truthy_progress_does_not_skip_action():
    """Only an exact durable true value skips a finalization action."""
    publisher = Mock()
    snowflake = Mock()
    target_config = {'s3_bucket': 'bucket'}
    attempt = SimpleNamespace(
        s3_keys=[],
        staging_table='PW_STAGE_123',
        manifest_payload=SimpleNamespace(replacement_metadata=None),
        finalization={routes.FINALIZATION_GRANTS: 'false'},
    )

    with patch.object(
        routes.utils,
        'retry_snowflake_table_grants',
    ) as grants, patch.object(routes.utils, 'delete_s3_objects'):
        routes.finalize_attempt(
            publisher,
            snowflake,
            target_config,
            'SCHEMA',
            'source.table',
            attempt,
            'test cleanup',
        )

    grants.assert_called_once_with(
        snowflake,
        target_config,
        'SCHEMA',
        'source.table',
    )
    assert publisher.record_finalization_action.call_args_list[0] == call(
        attempt,
        routes.FINALIZATION_GRANTS,
    )
