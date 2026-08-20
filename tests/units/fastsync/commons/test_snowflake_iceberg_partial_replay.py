"""Tests for safe replay of an ambiguous Snowflake Iceberg Partial MERGE."""

from unittest.mock import MagicMock

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_SUBMITTED,
    PUBLICATION_PARTIAL_MERGE,
    RECOVERY_PUBLISH,
    SnowflakeIcebergPublisher,
    StagingPrimaryKeyError,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    RECOVERY_IDENTITY,
    FakeSnowflake,
    make_attempt,
    persist_attempt,
    v3_snapshot,
)


def test_submitted_bad_keys_stay_ambiguous(tmp_path, spec):
    """Invalid keys cannot erase an already-submitted transaction boundary."""
    snowflake = FakeSnowflake([[{'HAS_NULL_KEY': 0, 'HAS_DUPLICATE_KEY': 1}]])
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
    publisher.inspect_table = MagicMock()
    attempt = make_attempt(
        spec,
        phase=PHASE_SUBMITTED,
        kind='partial',
        method=PUBLICATION_PARTIAL_MERGE,
        snapshot=v3_snapshot(spec),
    )
    attempt.query_id = 'old-query-id'
    attempt.update_manifest_payload({
        'publication_query_hash': 'a' * 64,
        'publication_query_type': 'MERGE',
    })
    old_attempt_id = attempt.attempt_id
    old_payload = attempt.manifest_payload
    expected_evidence = (
        attempt.expected_row_count,
        attempt.expected_row_fingerprint,
    )
    persist_attempt(publisher, attempt)

    with pytest.raises(
        StagingPrimaryKeyError,
        match='may already have committed.*manual recovery is required',
    ):
        publisher.publish_partial_sync(attempt, spec)

    recovered = publisher.load_attempt(
        spec,
        expected_kind='partial',
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert recovered.phase == PHASE_SUBMITTED
    assert (
        recovered.expected_row_count,
        recovered.expected_row_fingerprint,
    ) == expected_evidence
    assert recovered.attempt_id == old_attempt_id
    assert recovered.query_id == 'old-query-id'
    assert recovered.manifest_payload == old_payload
    assert snowflake.transactions == []
    publisher.inspect_table.assert_not_called()


def test_partial_merge_rearms_before_replay(tmp_path, spec):
    """An ambiguous Partial MERGE is durably rearmed under a fresh tag."""
    snowflake = FakeSnowflake([
        [{'HAS_NULL_KEY': 0, 'HAS_DUPLICATE_KEY': 0}],
        [{'ROW_COUNT': 0, 'ROW_FINGERPRINT': 'fixture-hash'}],
    ])
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
    publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
    attempt = make_attempt(
        spec,
        phase=PHASE_SUBMITTED,
        kind='partial',
        method=PUBLICATION_PARTIAL_MERGE,
        snapshot=v3_snapshot(spec),
    )
    attempt.query_id = 'old-query-id'
    attempt.update_manifest_payload({
        'publication_query_hash': 'a' * 64,
        'publication_query_type': 'MERGE',
        'publication_submitted_at': 1_700_000_000.0,
    })
    old_attempt_id = attempt.attempt_id
    old_submitted_at = attempt.manifest_payload.publication_submitted_at
    persist_attempt(publisher, attempt)

    durable_at_plan = {}
    original_plan = publisher.publication_service.plan_partial_sync

    def plan_after_rearm(rearmed_attempt, current_spec):
        durable = publisher.recovery_store(
            spec.name,
            RECOVERY_IDENTITY,
        ).load()
        durable_at_plan.update({
            'phase': durable.phase,
            'attempt_id': durable.attempt_id,
            'query_id': durable.query_id,
            'query_hash': durable.manifest_payload.publication_query_hash,
            'query_type': durable.manifest_payload.publication_query_type,
            'submitted_at': durable.manifest_payload.publication_submitted_at,
        })
        return original_plan(rearmed_attempt, current_spec)

    publisher.publication_service.plan_partial_sync = MagicMock(
        side_effect=plan_after_rearm
    )

    assert publisher.reconcile(attempt, spec).action == RECOVERY_PUBLISH
    plan = publisher.publish_partial_sync(attempt, spec)

    assert durable_at_plan == {
        'phase': PHASE_STAGED,
        'attempt_id': attempt.attempt_id,
        'query_id': None,
        'query_hash': None,
        'query_type': None,
        'submitted_at': None,
    }
    assert attempt.attempt_id != old_attempt_id
    assert plan.query_tag['attempt_id'] == attempt.attempt_id
    assert snowflake.transactions[0][1]['attempt_id'] == attempt.attempt_id
    assert attempt.phase == PHASE_PUBLISHED
    assert attempt.query_id is None
    assert attempt.manifest_payload.publication_query_hash is None
    assert attempt.manifest_payload.publication_query_type is None
    assert attempt.manifest_payload.publication_submitted_at != old_submitted_at
    recovered = publisher.load_attempt(
        spec,
        expected_kind='partial',
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert recovered.phase == PHASE_PUBLISHED
    assert recovered.attempt_id == attempt.attempt_id
