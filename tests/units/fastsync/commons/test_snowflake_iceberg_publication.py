"""Tests for Snowflake Iceberg publication and reconciliation."""

import json
from unittest.mock import MagicMock, call, patch

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    AmbiguousPublicationError,
    PHASE_FINALIZED,
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_STAGING_CREATED,
    PHASE_SUBMITTED,
    PUBLICATION_INSERT_OVERWRITE,
    PUBLICATION_MISSING_CTAS,
    PUBLICATION_PARTIAL_BOOTSTRAP_CTAS,
    PUBLICATION_PARTIAL_MERGE,
    PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
    PUBLICATION_REPLACEMENT_CTAS,
    QueryHistoryLookupError,
    QueryHistoryVisibilityTimeoutError,
    RECOVERY_FINALIZE,
    RECOVERY_PUBLISH,
    RECOVERY_RESTART_STAGING,
    IcebergTableSpec,
    RecoveryManifestError,
    SnowflakeIcebergPublisher,
    SnowflakeQueryAdapter,
    SnowflakeTableMetadata,
    StagingPrimaryKeyError,
    TARGET_ATTEMPT_COMPLETED,
    TableCompatibilityError,
    TableFormatDiscoveryError,
    _sql_hash,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    RECOVERY_IDENTITY,
    FakeClock,
    FakeSnowflake,
    make_attempt,
    missing_snapshot,
    persist_attempt,
    v3_snapshot,
)


class TestPublication:
    """Validate publication and recovery behavior."""

    def test_committed_replacement_recovery_ignores_removed_column_comments(
        self,
        tmp_path,
        spec,
    ):
        """Committed replacement recovery verifies and restores only destination comments."""
        statement = 'CREATE OR REPLACE ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS"'
        history = [
            {
                "QUERY_ID": "query-id",
                "QUERY_TEXT": statement,
                "QUERY_TYPE": "CREATE_TABLE_AS_SELECT",
                "EXECUTION_STATUS": "SUCCESS",
            }
        ]
        expected_metadata = SnowflakeTableMetadata(
            column_comments=(
                ("ID", "id comment"),
                ("UPDATED AT", "updated comment"),
            ),
            owner="PIPELINEWISE_ROLE",
        )
        post_commit_columns = [
            {
                "COLUMN_NAME": "UPDATED AT",
                "COMMENT": "updated comment",
                "COLUMN_DEFAULT": None,
                "IS_IDENTITY": "NO",
            },
            {
                "COLUMN_NAME": "ID",
                "COMMENT": "id comment",
                "COLUMN_DEFAULT": None,
                "IS_IDENTITY": "NO",
            },
        ]
        snowflake = FakeSnowflake(
            [
                history,
                [{"ROW_COUNT": 0, "ROW_FINGERPRINT": "fixture-hash"}],
                [],
                [],
                [],
                [],
                [],
                post_commit_columns,
                [
                    {
                        "name": spec.name.table,
                        "cluster_by": "",
                        "owner": "PIPELINEWISE_ROLE",
                        "owner_role_type": "ROLE",
                    }
                ],
                [{"CURRENT_ROLE": "PIPELINEWISE_ROLE"}],
                [],
                [],
            ]
        )
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
        attempt = make_attempt(
            spec,
            phase=PHASE_SUBMITTED,
            method=PUBLICATION_REPLACEMENT_CTAS,
            context={
                "publication_query_hash": _sql_hash(statement),
                "publication_query_type": "CREATE_TABLE_AS_SELECT",
                "replacement_metadata": expected_metadata.as_dict(),
            },
        )
        persist_attempt(publisher, attempt)

        outcome = publisher.reconcile(attempt, spec)
        publisher.restore_metadata(attempt)

        assert outcome.action == RECOVERY_FINALIZE
        assert attempt.phase == PHASE_PUBLISHED
        restored_comments = [
            query
            for query, _, query_tag in snowflake.queries
            if query_tag and query_tag.get("phase") == "restore_metadata"
        ]
        assert restored_comments == [
            'ALTER ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS" '
            'ALTER COLUMN "ID" COMMENT \'id comment\'',
            'ALTER ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS" '
            'ALTER COLUMN "UPDATED AT" COMMENT \'updated comment\'',
        ]
        assert all("LEGACY_PAYLOAD" not in query for query in restored_comments)

    def test_publish_persists_submitted_before_an_ambiguous_driver_error(self, tmp_path, spec):
        """Publish persists submitted before an ambiguous driver error."""
        snowflake = FakeSnowflake([RuntimeError("connection lost")])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        attempt = make_attempt(spec)
        persist_attempt(publisher, attempt)

        with pytest.raises(RuntimeError, match="connection lost"):
            publisher.publish_full_sync(attempt, spec)

        persisted = publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        )
        assert persisted.phase == PHASE_SUBMITTED
        assert isinstance(persisted.context["publication_submitted_at"], float)
        assert snowflake.queries[0][2] == attempt.query_tag

    def test_publish_rechecks_target_before_mutation(self, tmp_path, spec):
        """Publish rechecks target before mutation."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(side_effect=[missing_snapshot(), v3_snapshot(spec)])
        attempt = make_attempt(spec)

        with pytest.raises(RecoveryManifestError, match="target changed"):
            publisher.publish_full_sync(attempt, spec)

        assert publisher.snowflake.queries == []
        assert attempt.phase == PHASE_STAGED

    def test_successful_full_publication_verifies_count_and_advances_phase(self, tmp_path, spec):
        """Successful full publication verifies count and advances phase."""
        snowflake = FakeSnowflake(responses=[None, [{"ROW_COUNT": 7, "ROW_FINGERPRINT": "staged-hash"}]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(side_effect=[missing_snapshot(), missing_snapshot(), v3_snapshot(spec)])
        attempt = make_attempt(spec)
        attempt.expected_row_count = 7
        attempt.expected_row_fingerprint = "staged-hash"
        persist_attempt(publisher, attempt)

        plan = publisher.publish_full_sync(attempt, spec)

        assert plan.method == PUBLICATION_MISSING_CTAS
        assert attempt.phase == PHASE_PUBLISHED
        assert (
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            ).phase
            == PHASE_PUBLISHED
        )

    @pytest.mark.parametrize(
        'error',
        (
            TableCompatibilityError('copy-on-write value is AUTO'),
            TableFormatDiscoveryError('copy-on-write metadata is missing'),
        ),
    )
    def test_post_publication_table_contract_failure_uses_recovery_taxonomy(
        self,
        tmp_path,
        spec,
        error,
    ):
        """A committed CTAS remains recoverable when table settings cannot be proven."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(side_effect=error)
        attempt = make_attempt(spec, phase=PHASE_SUBMITTED)

        with pytest.raises(
            RecoveryManifestError,
            match='Published Iceberg target does not satisfy',
        ):
            publisher._verify_published(  # pylint: disable=protected-access
                attempt,
                spec,
            )

        assert publisher.snowflake.queries == []

    @pytest.mark.parametrize(
        "method",
        (PUBLICATION_PARTIAL_BOOTSTRAP_CTAS, PUBLICATION_PARTIAL_REPLACEMENT_CTAS),
    )
    def test_partial_ctas_publication_verifies_staging_count(self, tmp_path, spec, method):
        """Partial ctas publication verifies staging count."""
        snowflake = FakeSnowflake([[{"ROW_COUNT": 6, "ROW_FINGERPRINT": "wrong-hash"}]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
        attempt = make_attempt(spec, phase=PHASE_PUBLISHED, kind="partial", method=method)
        attempt.expected_row_count = 7
        attempt.expected_row_fingerprint = "staged-hash"

        with pytest.raises(RecoveryManifestError, match="contents"):
            publisher._verify_published(attempt, spec)  # pylint: disable=protected-access

    def test_published_contents_reject_equal_count_with_wrong_fingerprint(self, tmp_path, spec):
        """Equal row counts cannot hide a different published row set."""
        snowflake = FakeSnowflake([[{"ROW_COUNT": 7, "ROW_FINGERPRINT": "wrong-hash"}]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
        attempt = make_attempt(spec, phase=PHASE_PUBLISHED)
        attempt.expected_row_count = 7
        attempt.expected_row_fingerprint = "staged-hash"

        with pytest.raises(RecoveryManifestError, match="contents do not match"):
            publisher._verify_published(attempt, spec)  # pylint: disable=protected-access

        assert len(snowflake.queries) == 1

    def test_partial_merge_publication_compares_only_the_persisted_range(self, tmp_path, spec):
        """Partial merge publication compares only the persisted range."""
        snowflake = FakeSnowflake([[{"ROW_COUNT": 7, "ROW_FINGERPRINT": "staged-hash"}]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
        attempt = make_attempt(
            spec,
            phase=PHASE_PUBLISHED,
            kind="partial",
            method=PUBLICATION_PARTIAL_MERGE,
            context={"where_clause_sql": ' WHERE "ID" BETWEEN 2 AND 8'},
        )
        attempt.expected_row_count = 7
        attempt.expected_row_fingerprint = "staged-hash"

        publisher._verify_published(attempt, spec)  # pylint: disable=protected-access

        assert 'WHERE "ID" BETWEEN 2 AND 8' in snowflake.queries[0][0]

    def test_staging_evidence_hashes_the_canonical_projection(self, tmp_path, spec):
        """Staging evidence hashes the canonical projection."""
        snowflake = FakeSnowflake([[{"ROW_COUNT": 3, "ROW_FINGERPRINT": -12345}]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_STAGING_CREATED)

        evidence = publisher.staging_evidence(attempt, spec, loaded_row_count=3)

        assert evidence == (3, "-12345")
        query, _, query_tag = snowflake.queries[0]
        assert f"FROM {spec.name.with_table(attempt.staging_table).quoted}" in query
        assert spec.projection in query
        assert query_tag["phase"] == "staging_evidence"

    def test_staging_evidence_rejects_copy_count_mismatch(self, tmp_path, spec):
        """Staging evidence rejects copy count mismatch."""
        snowflake = FakeSnowflake([[{"ROW_COUNT": 2, "ROW_FINGERPRINT": 12345}]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_STAGING_CREATED)

        with pytest.raises(RecoveryManifestError, match="completed COPY"):
            publisher.staging_evidence(attempt, spec, loaded_row_count=3)

    @pytest.mark.parametrize(
        ("primary_keys", "integrity_evidence", "message"),
        (
            (("ID",), {"HAS_NULL_KEY": 0, "HAS_DUPLICATE_KEY": 1}, "duplicate groups"),
            (
                ("ID", "UPDATED AT"),
                {"HAS_NULL_KEY": 0, "HAS_DUPLICATE_KEY": 1},
                "duplicate groups",
            ),
            (
                ("ID", "UPDATED AT"),
                {"HAS_NULL_KEY": 1, "HAS_DUPLICATE_KEY": 0},
                "NULL components",
            ),
        ),
    )
    def test_partial_staging_evidence_rejects_invalid_transformed_primary_keys(
        self,
        tmp_path,
        spec,
        primary_keys,
        integrity_evidence,
        message,
    ):
        """Transformed staging keys are complete and unique before STAGED."""
        partial_spec = IcebergTableSpec(
            spec.name,
            spec.columns,
            primary_keys,
        )
        snowflake = FakeSnowflake([[integrity_evidence]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        attempt = make_attempt(
            partial_spec,
            phase=PHASE_STAGING_CREATED,
            kind="partial",
            method=PUBLICATION_PARTIAL_MERGE,
        )
        intended_state = dict(attempt.intended_state)

        with pytest.raises(StagingPrimaryKeyError, match=message) as exc_info:
            publisher.staging_evidence(attempt, partial_spec, loaded_row_count=2)

        assert attempt.phase == PHASE_STAGING_CREATED
        assert attempt.intended_state == intended_state
        assert snowflake.transactions == []
        assert len(snowflake.queries) == 1
        query, _, query_tag = snowflake.queries[0]
        grouped_keys = ', '.join(
            f'"PW_PROJECTED_STAGE"."{key}"' for key in primary_keys
        )
        assert f"GROUP BY {grouped_keys}" in query
        assert "PW_PROJECTED_STAGE" in query
        assert query_tag["phase"] == "staging_key_validation"
        assert spec.name.quoted in str(exc_info.value)
        assert all(f'"{key}"' in str(exc_info.value) for key in primary_keys)

    def test_valid_partial_staging_keys_are_checked_before_content_evidence(self, tmp_path, spec):
        """Valid transformed keys permit canonical staging evidence."""
        snowflake = FakeSnowflake(
            [
                [{"HAS_NULL_KEY": 0, "HAS_DUPLICATE_KEY": 0}],
                [{"ROW_COUNT": 2, "ROW_FINGERPRINT": "staged-hash"}],
            ]
        )
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        attempt = make_attempt(
            spec,
            phase=PHASE_STAGING_CREATED,
            kind="partial",
            method=PUBLICATION_PARTIAL_MERGE,
        )

        evidence = publisher.staging_evidence(attempt, spec, loaded_row_count=2)

        assert evidence == (2, "staged-hash")
        assert snowflake.queries[0][2]["phase"] == "staging_key_validation"
        assert snowflake.queries[1][2]["phase"] == "staging_evidence"

    def test_partial_staging_key_validation_requires_one_evidence_row(self, tmp_path, spec):
        """Missing key-integrity evidence fails closed before content evidence."""
        snowflake = FakeSnowflake([[]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        attempt = make_attempt(
            spec,
            phase=PHASE_STAGING_CREATED,
            kind="partial",
            method=PUBLICATION_PARTIAL_MERGE,
        )

        with pytest.raises(RecoveryManifestError, match="primary-key integrity evidence"):
            publisher.staging_evidence(attempt, spec, loaded_row_count=0)

        assert attempt.phase == PHASE_STAGING_CREATED
        assert len(snowflake.queries) == 1

    def test_recovered_staged_attempt_rejects_duplicate_keys_before_publication(self, tmp_path, spec):
        """A recovered STAGED manifest cannot bypass the key-integrity preflight."""
        snowflake = FakeSnowflake([[{"HAS_NULL_KEY": 0, "HAS_DUPLICATE_KEY": 1}]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock()
        attempt = make_attempt(
            spec,
            phase=PHASE_STAGED,
            kind="partial",
            method=PUBLICATION_PARTIAL_MERGE,
            snapshot=v3_snapshot(spec),
        )
        intended_state = dict(attempt.intended_state)
        persist_attempt(publisher, attempt)

        with pytest.raises(StagingPrimaryKeyError, match="duplicate groups"):
            publisher.publish_partial_sync(attempt, spec)

        recovered = publisher.load_attempt(
            spec,
            expected_kind="partial",
            recovery_identity=RECOVERY_IDENTITY,
        )
        assert recovered.phase == PHASE_STAGING_CREATED
        assert recovered.expected_row_count is None
        assert recovered.expected_row_fingerprint is None
        assert recovered.intended_state == intended_state
        assert publisher.reconcile(recovered, spec).action == RECOVERY_RESTART_STAGING
        assert snowflake.transactions == []
        publisher.inspect_table.assert_not_called()

class TestQueryHistoryRecovery:
    """Validate publication recovery and bounded query-history polling."""

    def test_submitted_full_sync_recovers_one_successful_exact_tag(self, tmp_path, spec):
        """Submitted full sync recovers one successful exact tag."""
        history = [
            {
                "QUERY_ID": "query-id",
                "QUERY_TEXT": "INSERT OVERWRITE INTO ...",
                "QUERY_TYPE": "INSERT",
                "EXECUTION_STATUS": "SUCCESS",
            }
        ]
        snowflake = FakeSnowflake(
            [
                history,
                [{"ROW_COUNT": 0, "ROW_FINGERPRINT": "fixture-hash"}],
            ]
        )
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
        statement = 'INSERT OVERWRITE INTO "TEST_DB"."TEST_SCHEMA"."ORDERS"'
        history[0]["QUERY_TEXT"] = statement
        attempt = make_attempt(
            spec,
            phase=PHASE_SUBMITTED,
            method=PUBLICATION_INSERT_OVERWRITE,
            snapshot=v3_snapshot(spec),
            context={
                "publication_query_hash": _sql_hash(statement),
                "publication_query_type": "INSERT",
            },
        )
        persist_attempt(publisher, attempt)

        outcome = publisher.reconcile(attempt, spec)

        assert outcome.action == RECOVERY_FINALIZE
        assert attempt.query_id == "query-id"
        assert attempt.phase == PHASE_PUBLISHED
        assert snowflake.queries[0][1] == {
            "query_tag": json.dumps(attempt.query_tag, sort_keys=True, separators=(",", ":")),
            "submitted_at": 1_700_000_000.0,
        }
        assert "QUERY_HISTORY_BY_USER" in snowflake.queries[0][0]
        assert (
            "END_TIME_RANGE_START => DATEADD('minute', -5, "
            "TO_TIMESTAMP_LTZ(%(submitted_at)s))"
        ) in snowflake.queries[0][0]
        assert (
            "END_TIME_RANGE_END => LEAST(CURRENT_TIMESTAMP(), "
            "DATEADD('hour', 24, TO_TIMESTAMP_LTZ(%(submitted_at)s)))"
        ) in snowflake.queries[0][0]
        assert "RESULT_LIMIT => 10000" in snowflake.queries[0][0]
        assert snowflake.query_timeouts == [30.0]

    def test_failed_submitted_query_is_safe_to_republish(self, tmp_path, spec):
        """Failed submitted query is safe to republish."""
        statement = 'CREATE ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS"'
        history = [
            {
                "QUERY_ID": "query-id",
                "QUERY_TEXT": statement,
                "QUERY_TYPE": "CREATE_TABLE_AS_SELECT",
                "EXECUTION_STATUS": "FAILED_WITH_ERROR",
            }
        ]
        publisher = SnowflakeIcebergPublisher(FakeSnowflake([history]), str(tmp_path))
        attempt = make_attempt(
            spec,
            phase=PHASE_SUBMITTED,
            context={
                "publication_query_hash": _sql_hash(statement),
                "publication_query_type": "CREATE_TABLE_AS_SELECT",
            },
        )
        persist_attempt(publisher, attempt)
        failed_attempt_id = attempt.attempt_id

        outcome = publisher.reconcile(attempt, spec)

        assert outcome.action == RECOVERY_PUBLISH
        assert attempt.phase == PHASE_STAGED
        assert attempt.attempt_id != failed_attempt_id
        assert "publication_query_hash" not in attempt.context
        assert "publication_query_type" not in attempt.context
        assert "publication_submitted_at" not in attempt.context

    def test_failed_submitted_query_with_unknown_type_is_safe_to_republish(self, tmp_path, spec):
        """Failed submitted query with unknown type is safe to republish."""
        statement = 'CREATE ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS"'
        history = [
            {
                "QUERY_ID": "query-id",
                "QUERY_TEXT": statement,
                "QUERY_TYPE": "UNKNOWN",
                "EXECUTION_STATUS": "FAILED_WITH_ERROR",
            }
        ]
        publisher = SnowflakeIcebergPublisher(FakeSnowflake([history]), str(tmp_path))
        attempt = make_attempt(
            spec,
            phase=PHASE_SUBMITTED,
            context={
                "publication_query_hash": _sql_hash(statement),
                "publication_query_type": "CREATE_TABLE_AS_SELECT",
            },
        )
        persist_attempt(publisher, attempt)

        outcome = publisher.reconcile(attempt, spec)

        assert outcome.action == RECOVERY_PUBLISH
        assert attempt.phase == PHASE_STAGED

    def test_failed_query_rotates_tag_so_lost_retry_can_reconcile(self, tmp_path, spec):
        """Failed query rotates tag so lost retry can reconcile."""
        statement = 'CREATE ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS"'
        failed_history = [
            {
                "QUERY_ID": "failed-id",
                "QUERY_TEXT": statement,
                "QUERY_TYPE": "CREATE_TABLE_AS_SELECT",
                "EXECUTION_STATUS": "FAILED_WITH_ERROR",
            }
        ]
        successful_history = [
            {
                "QUERY_ID": "success-id",
                "QUERY_TEXT": statement,
                "QUERY_TYPE": "CREATE_TABLE_AS_SELECT",
                "EXECUTION_STATUS": "SUCCESS",
            }
        ]
        snowflake = FakeSnowflake(
            [
                failed_history,
                successful_history,
                [{"ROW_COUNT": 0, "ROW_FINGERPRINT": "fixture-hash"}],
            ]
        )
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
        attempt = make_attempt(
            spec,
            phase=PHASE_SUBMITTED,
            context={
                "publication_query_hash": _sql_hash(statement),
                "publication_query_type": "CREATE_TABLE_AS_SELECT",
            },
        )
        persist_attempt(publisher, attempt)
        first_tag = json.dumps(attempt.query_tag, sort_keys=True, separators=(",", ":"))

        assert publisher.reconcile(attempt, spec).action == RECOVERY_PUBLISH
        second_tag = json.dumps(attempt.query_tag, sort_keys=True, separators=(",", ":"))
        assert second_tag != first_tag

        attempt.update_manifest_payload(
            {
                "publication_query_hash": _sql_hash(statement),
                "publication_query_type": "CREATE_TABLE_AS_SELECT",
                "publication_submitted_at": 1_700_000_001.0,
            }
        )
        attempt.phase = PHASE_SUBMITTED
        publisher._save_active_attempt(attempt)  # pylint: disable=protected-access

        assert publisher.reconcile(attempt, spec).action == RECOVERY_FINALIZE
        assert snowflake.queries[0][1] == {
            "query_tag": first_tag,
            "submitted_at": 1_700_000_000.0,
        }
        assert snowflake.queries[1][1] == {
            "query_tag": second_tag,
            "submitted_at": 1_700_000_001.0,
        }

    def test_delayed_query_history_success_before_deadline(self, tmp_path, spec):
        """Non-terminal history is retried until the exact query succeeds."""
        statement = 'INSERT OVERWRITE INTO "TEST_DB"."TEST_SCHEMA"."ORDERS"'
        history = {
            "QUERY_ID": "query-id",
            "QUERY_TEXT": statement,
            "QUERY_TYPE": "INSERT",
            "EXECUTION_STATUS": "SUCCESS",
        }
        snowflake = FakeSnowflake(
            [
                [],
                [],
                [history],
                [{"ROW_COUNT": 0, "ROW_FINGERPRINT": "fixture-hash"}],
            ]
        )
        publisher = SnowflakeIcebergPublisher(
            snowflake,
            str(tmp_path),
            history_poll_interval_seconds=1.0,
            history_poll_timeout_seconds=10.0,
        )
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
        attempt = make_attempt(
            spec,
            phase=PHASE_SUBMITTED,
            method=PUBLICATION_INSERT_OVERWRITE,
            context={
                "publication_query_hash": _sql_hash(statement),
                "publication_query_type": "INSERT",
            },
        )
        persist_attempt(publisher, attempt)
        clock = FakeClock()

        with (
            patch(
                "pipelinewise.fastsync.commons.snowflake_iceberg_publication.time.monotonic",
                side_effect=clock.monotonic,
            ),
            patch(
                "pipelinewise.fastsync.commons.snowflake_iceberg_publication.time.sleep",
                side_effect=clock.sleep,
            ),
        ):
            outcome = publisher.reconcile(attempt, spec)

        assert outcome.action == RECOVERY_FINALIZE
        assert clock.sleeps == [1.0, 1.0]
        assert snowflake.query_timeouts == [10.0, 9.0, 8.0]

    def test_query_history_deadline_exhaustion_preserves_submission(self, tmp_path, spec):
        """Visibility timeout retains the exact submitted attempt for retry."""
        snowflake = FakeSnowflake(
            [
                [],
                [],
                [],
            ]
        )
        publisher = SnowflakeIcebergPublisher(
            snowflake,
            str(tmp_path),
            history_poll_interval_seconds=1.0,
            history_poll_timeout_seconds=2.5,
            history_lookup_timeout_seconds=5.0,
        )
        attempt = make_attempt(spec, phase=PHASE_SUBMITTED)
        persist_attempt(publisher, attempt)
        clock = FakeClock()

        with (
            patch(
                "pipelinewise.fastsync.commons.snowflake_iceberg_publication.time.monotonic",
                side_effect=clock.monotonic,
            ),
            patch(
                "pipelinewise.fastsync.commons.snowflake_iceberg_publication.time.sleep",
                side_effect=clock.sleep,
            ),
            pytest.raises(QueryHistoryVisibilityTimeoutError) as error,
        ):
            publisher.reconcile(attempt, spec)

        assert error.value.retryable is True
        assert error.value.poll_count == 3
        assert error.value.elapsed_seconds == 2.5
        assert error.value.last_statuses == ()
        assert clock.sleeps == [1.0, 1.0, 0.5]
        assert snowflake.query_timeouts == [2.5, 1.5, 0.5]
        persisted = publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        )
        assert persisted.phase == PHASE_SUBMITTED
        assert persisted.attempt_id == attempt.attempt_id
        assert persisted.context == attempt.context

    def test_query_history_lookup_failure_has_recovery_taxonomy(self, tmp_path, spec):
        """Lookup failures are typed and retain the submitted attempt."""
        snowflake = FakeSnowflake([RuntimeError("lookup failed")])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_SUBMITTED)
        persist_attempt(publisher, attempt)

        with pytest.raises(QueryHistoryLookupError, match="lookup 1") as error:
            publisher.reconcile(attempt, spec)

        assert error.value.retryable is True
        assert isinstance(error.value.__cause__, RuntimeError)
        assert attempt.phase == PHASE_SUBMITTED
        assert publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        ).phase == PHASE_SUBMITTED

    @pytest.mark.parametrize(
        ("query_text", "query_type", "message"),
        (
            ("SELECT 1", "INSERT", "text does not match"),
            (
                'INSERT OVERWRITE INTO "TEST_DB"."TEST_SCHEMA"."ORDERS"',
                "UPDATE",
                "type does not match",
            ),
        ),
    )
    def test_successful_history_with_conflicting_evidence_fails_closed(
        self,
        tmp_path,
        spec,
        query_text,
        query_type,
        message,
    ):
        """A terminal success cannot bypass exact text and type evidence."""
        statement = 'INSERT OVERWRITE INTO "TEST_DB"."TEST_SCHEMA"."ORDERS"'
        history = [{
            "QUERY_ID": "query-id",
            "QUERY_TEXT": query_text,
            "QUERY_TYPE": query_type,
            "EXECUTION_STATUS": "SUCCESS",
        }]
        publisher = SnowflakeIcebergPublisher(FakeSnowflake([history]), str(tmp_path))
        attempt = make_attempt(
            spec,
            phase=PHASE_SUBMITTED,
            method=PUBLICATION_INSERT_OVERWRITE,
            context={
                "publication_query_hash": _sql_hash(statement),
                "publication_query_type": "INSERT",
            },
        )
        persist_attempt(publisher, attempt)

        with pytest.raises(AmbiguousPublicationError, match=message):
            publisher.reconcile(attempt, spec)

        assert attempt.phase == PHASE_SUBMITTED

    def test_multiple_terminal_query_history_rows_are_ambiguous(self, tmp_path, spec):
        """More than one terminal row violates the exact-attempt contract."""
        history = [
            {"QUERY_ID": "one", "EXECUTION_STATUS": "SUCCESS"},
            {"QUERY_ID": "two", "EXECUTION_STATUS": "SUCCESS"},
        ]
        publisher = SnowflakeIcebergPublisher(FakeSnowflake([history]), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_SUBMITTED)

        with pytest.raises(AmbiguousPublicationError, match="Expected one terminal"):
            publisher.reconcile(attempt, spec)

    def test_partial_submitted_attempt_replays_without_history_lookup(self, tmp_path, spec):
        """Partial submitted attempt replays without history lookup."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(
            spec,
            phase=PHASE_SUBMITTED,
            kind="partial",
            method=PUBLICATION_PARTIAL_MERGE,
        )

        outcome = publisher.reconcile(attempt, spec)

        assert outcome.action == RECOVERY_PUBLISH
        assert publisher.snowflake.queries == []

    def test_partial_ctas_submitted_attempt_reconciles_query_history(self, tmp_path, spec):
        """Partial ctas submitted attempt reconciles query history."""
        statement = 'CREATE ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS"'
        history = [
            {
                "QUERY_ID": "query-id",
                "QUERY_TEXT": statement,
                "QUERY_TYPE": "CREATE_TABLE_AS_SELECT",
                "EXECUTION_STATUS": "SUCCESS",
            }
        ]
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake(
                [
                    history,
                    [{"ROW_COUNT": 0, "ROW_FINGERPRINT": "fixture-hash"}],
                ]
            ),
            str(tmp_path),
        )
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
        attempt = make_attempt(
            spec,
            phase=PHASE_SUBMITTED,
            kind="partial",
            method=PUBLICATION_PARTIAL_BOOTSTRAP_CTAS,
            context={
                "where_clause_sql": ' WHERE "ID" >= 1',
                "end_is_unbounded": True,
                "delete_mode": "hard",
                "publication_query_hash": _sql_hash(statement),
                "publication_query_type": "CREATE_TABLE_AS_SELECT",
            },
        )
        persist_attempt(publisher, attempt)

        outcome = publisher.reconcile(attempt, spec)

        assert outcome.action == RECOVERY_FINALIZE
        assert attempt.phase == PHASE_PUBLISHED

    def test_manifest_survives_finalization_until_state_handoff(self, tmp_path, spec):
        """Manifest survives finalization until state handoff."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_PUBLISHED)
        persist_attempt(publisher, attempt)

        publisher.mark_finalized(attempt, ["grants", "s3_cleanup", "staging_cleanup"])

        recovered = publisher.load_attempt(spec, expected_kind="full", recovery_identity=RECOVERY_IDENTITY)
        assert recovered.phase == PHASE_FINALIZED
        assert recovered.finalization == {
            "grants": True,
            "s3_cleanup": True,
            "staging_cleanup": True,
        }

        target_store = publisher.recovery_store(spec.name)
        with patch.object(
            target_store,
            "delete_fastsync_target_pointer",
            side_effect=RuntimeError("simulated crash"),
        ):
            with pytest.raises(RuntimeError, match="simulated crash"):
                publisher.complete_state_handoff(attempt)

        assert publisher.recovery_store(spec.name, RECOVERY_IDENTITY).load() is None
        assert target_store.load_fastsync_target_pointer().state == TARGET_ATTEMPT_COMPLETED
        assert publisher.load_attempt(spec, expected_kind="full", recovery_identity=RECOVERY_IDENTITY) is None
        assert target_store.load_fastsync_target_pointer() is None

    @patch("pipelinewise.fastsync.commons.snowflake_iceberg.pem2der", return_value=b"key")
    @patch("pipelinewise.fastsync.commons.snowflake_iceberg.snowflake.connector.connect")
    def test_s3_free_query_adapter_honors_role(self, mock_connect, _mock_pem2der):
        """S3 free query adapter honors role."""
        adapter = SnowflakeQueryAdapter(
            {
                "user": "user",
                "private_key": "/key.pem",
                "account": "account",
                "dbname": "database",
                "warehouse": "warehouse",
                "role": "loader",
            }
        )

        adapter.open_connection({"attempt_id": "one"})

        mock_connect.assert_called_once_with(
            user="user",
            private_key=b"key",
            account="account",
            database="database",
            warehouse="warehouse",
            role="loader",
            authenticator="SNOWFLAKE_JWT",
            autocommit=True,
            session_parameters={
                "QUOTED_IDENTIFIERS_IGNORE_CASE": "FALSE",
                "QUERY_TAG": '{"attempt_id":"one"}',
            },
        )

    def test_query_adapter_transaction_commits_or_rolls_back(self):
        """Query adapter transaction commits or rolls back."""
        adapter = SnowflakeQueryAdapter({})
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        adapter.open_connection = MagicMock(return_value=connection)

        adapter.execute_transaction(["UPDATE one", "DELETE two"], {"phase": "publication"})

        assert cursor.execute.call_args_list == [call("UPDATE one"), call("DELETE two")]
        connection.commit.assert_called_once_with()
        connection.rollback.assert_not_called()

        connection.reset_mock()
        cursor.execute.side_effect = RuntimeError("failed")
        with pytest.raises(RuntimeError, match="failed"):
            adapter.execute_transaction(["UPDATE one"])
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()

    def test_query_adapter_reduces_statement_timeout_after_connect(self):
        """Connection elapsed time is deducted from the statement timeout."""
        adapter = SnowflakeQueryAdapter({})
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.rowcount = 1
        cursor.fetchall.return_value = [{"QUERY_ID": "query-id"}]
        adapter.open_connection = MagicMock(return_value=connection)

        with patch(
            "pipelinewise.fastsync.commons.snowflake_iceberg.time.monotonic",
            side_effect=[10.0, 12.2],
        ):
            rows = adapter.query_with_timeout("SELECT query history", {"tag": "one"}, 5.0)

        assert rows == [{"QUERY_ID": "query-id"}]
        adapter.open_connection.assert_called_once_with(
            login_timeout=5,
            network_timeout=5,
            socket_timeout=5,
        )
        cursor.execute.assert_called_once_with(
            "SELECT query history",
            {"tag": "one"},
            timeout=3,
        )

    def test_query_adapter_rejects_connection_that_exhausts_lookup_deadline(self):
        """A connection that consumes the lookup budget cannot start a statement."""
        adapter = SnowflakeQueryAdapter({})
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        adapter.open_connection = MagicMock(return_value=connection)

        with (
            patch(
                "pipelinewise.fastsync.commons.snowflake_iceberg.time.monotonic",
                side_effect=[10.0, 15.0],
            ),
            pytest.raises(TimeoutError, match="before statement execution"),
        ):
            adapter.query_with_timeout("SELECT query history", {"tag": "one"}, 5.0)

        cursor.execute.assert_not_called()
