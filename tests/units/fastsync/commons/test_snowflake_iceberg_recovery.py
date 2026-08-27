"""Tests for Snowflake Iceberg durable recovery state."""

# pylint: disable=too-many-lines

import json
import os
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from pipelinewise.fastsync.commons import (
    snowflake_iceberg_routes,
    snowflake_iceberg_versions as versions,
    utils,
)
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    IcebergColumn,
    IcebergRecoveryStore,
    IcebergTargetAttemptPointer,
    IcebergTableSpec,
    MANAGED_ICEBERG_V3_SPEC,
    PHASE_FINALIZED,
    PHASE_PREPARED,
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_STAGING_CREATED,
    PHASE_SUBMITTED,
    PHASE_UPLOADED,
    PartialSyncBoundary,
    PUBLICATION_MISSING_CTAS,
    PUBLICATION_PARTIAL_MERGE,
    PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
    RECOVERY_FINALIZE,
    RECOVERY_PUBLISH,
    RECOVERY_RESTART_STAGING,
    RECOVERY_STATE_HANDOFF,
    RecoveryManifestError,
    SnowflakeIcebergPublisher,
    SnowflakeObjectName,
    TARGET_ATTEMPT_ACTIVE,
    TARGET_ATTEMPT_COMPLETED,
    TARGET_ATTEMPT_RESERVED,
    TableCompatibilityError,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    build_recovery_identity,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    RECOVERY_IDENTITY,
    FakeSnowflake,
    future_version_spec,
    make_attempt,
    missing_snapshot,
    persist_attempt,
    replace_nullable,
)


class TestManifestRecovery:
    """Validate recovery manifests and target pointers."""

    def test_recovery_store_round_trips_atomically_and_supports_outer_lock(self, tmp_path, spec):
        """Recovery store round trips atomically and supports outer lock."""
        store = IcebergRecoveryStore(str(tmp_path), spec.name)
        attempt = make_attempt(spec, phase=PHASE_PREPARED)

        with store.locked():
            store.save(attempt)
            recovered = store.load()

        assert recovered.as_dict() == attempt.as_dict()
        assert recovered.is_recovery is True
        assert os.stat(store.path).st_mode & 0o777 == 0o600
        assert not list(tmp_path.glob("*.tmp"))

        with store.locked():
            store.delete(attempt.attempt_id)
        assert not os.path.exists(store.path)

    @pytest.mark.parametrize('iceberg_version', (2, 4, 3.0))
    def test_tampered_attempt_version_fails_before_snowflake(
        self,
        tmp_path,
        spec,
        iceberg_version,
    ):
        """Recovery rejects unsupported durable format state before querying."""
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake(), str(tmp_path)
        )
        attempt = make_attempt(spec, phase=PHASE_PREPARED)
        persist_attempt(publisher, attempt)
        store = publisher.recovery_store(spec.name, RECOVERY_IDENTITY)
        manifest_path = Path(store.path)
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        manifest['iceberg_version'] = iceberg_version
        manifest_path.write_text(json.dumps(manifest), encoding='utf-8')

        with pytest.raises(
            RecoveryManifestError,
            match='table format contract is unsupported',
        ):
            publisher.load_attempt(
                spec,
                expected_kind='full',
                recovery_identity=RECOVERY_IDENTITY,
            )

        assert publisher.snowflake.queries == []

    def test_supported_future_version_identity_cannot_resume_v3_attempt(
        self,
        tmp_path,
        spec,
        monkeypatch,
    ):
        """A future supported version remains a different recovery contract."""
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake(), str(tmp_path)
        )
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        publisher.prepare_full_sync(
            spec,
            {'lsn': '1/2'},
            recovery_identity=RECOVERY_IDENTITY,
        )
        future_spec = future_version_spec()
        monkeypatch.setattr(
            versions,
            'MANAGED_ICEBERG_VERSION_SPECS',
            versions.managed_iceberg_version_registry(
                MANAGED_ICEBERG_V3_SPEC,
                future_spec,
            ),
        )
        changed_identity = build_recovery_identity(
            'fastsync',
            {'route': 'unit-test'},
            transformation_config={},
            stream_identity={
                'tap_id': 'unit-test',
                'route': 'unit-test',
                'table': 'source.table',
            },
            target_table_format='iceberg',
            iceberg_version=4,
        )
        publisher.inspect_table.reset_mock()

        with pytest.raises(
            RecoveryManifestError,
            match='different source, target',
        ):
            publisher.load_attempt(
                spec,
                expected_kind='full',
                recovery_identity=changed_identity,
            )

        publisher.inspect_table.assert_not_called()
        assert publisher.snowflake.queries == []

    def test_recovery_store_identity_does_not_alias_quoted_names_with_dots(self, tmp_path):
        """Recovery store identity does not alias quoted names with dots."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        first = publisher.recovery_store(SnowflakeObjectName("A.B", "C", "D"))
        second = publisher.recovery_store(SnowflakeObjectName("A", "B.C", "D"))

        assert first is not second
        assert first.path != second.path

    def test_stream_manifest_finds_and_rejects_changed_target_mapping(self, tmp_path, spec):
        """Stream manifest finds and rejects changed target mapping."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        original_identity = build_recovery_identity(
            "fastsync",
            {"target": spec.name.key},
            transformation_config={},
            stream_identity={"tap_id": "tap", "route": "route", "table": "source.table"},
            target_table_format="iceberg",
            iceberg_version=3,
        )
        changed_spec = IcebergTableSpec(
            SnowflakeObjectName("DATABASE", "OTHER_SCHEMA", "TABLE"),
            spec.columns,
            spec.primary_key,
        )
        changed_identity = build_recovery_identity(
            "fastsync",
            {"target": changed_spec.name.key},
            transformation_config={},
            stream_identity={"tap_id": "tap", "route": "route", "table": "source.table"},
            target_table_format="iceberg",
            iceberg_version=3,
        )

        attempt = publisher.prepare_full_sync(
            spec,
            {"lsn": "1/2"},
            recovery_identity=original_identity,
        )

        assert (
            publisher.recovery_store(spec.name, original_identity).path
            == publisher.recovery_store(changed_spec.name, changed_identity).path
        )
        with pytest.raises(RecoveryManifestError, match="different source, target"):
            publisher.load_attempt(
                changed_spec.name,
                expected_kind="full",
                recovery_identity=changed_identity,
            )
        assert publisher.recovery_store(spec.name, original_identity).load().attempt_id == attempt.attempt_id

    @pytest.mark.parametrize(
        "stream_identity",
        (
            {"tap_id": "other-tap", "route": "route", "table": "source.table"},
            {"tap_id": "tap", "route": "other-route", "table": "source.table"},
            {"tap_id": "tap", "route": "route", "table": "source.other_table"},
        ),
    )
    def test_target_pointer_rejects_another_stream_before_source_work(
        self,
        tmp_path,
        spec,
        stream_identity,
    ):
        """Target pointer rejects another stream before source work."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        original_identity = build_recovery_identity(
            "fastsync",
            {"target": spec.name.key},
            transformation_config={},
            stream_identity={
                "tap_id": "tap",
                "route": "route",
                "table": "source.table",
            },
            target_table_format="iceberg",
            iceberg_version=3,
        )
        changed_identity = build_recovery_identity(
            "fastsync",
            {"target": spec.name.key},
            transformation_config={},
            stream_identity=stream_identity,
            target_table_format="iceberg",
            iceberg_version=3,
        )
        original = publisher.prepare_full_sync(
            spec,
            {"lsn": "1/2"},
            recovery_identity=original_identity,
        )
        publisher.inspect_table.reset_mock()

        with pytest.raises(RecoveryManifestError, match="different source stream"):
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=changed_identity,
            )

        publisher.inspect_table.assert_not_called()
        assert publisher.recovery_store(spec.name, original_identity).load().attempt_id == original.attempt_id

    def test_reservation_without_stream_is_cleared_before_new_boundary(self, tmp_path, spec):
        """Reservation without stream is cleared before new boundary."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_PREPARED)
        target_store = publisher.recovery_store(spec.name)
        target_store.save_fastsync_target_pointer(
            IcebergTargetAttemptPointer.from_attempt(
                attempt,
                TARGET_ATTEMPT_RESERVED,
            )
        )

        assert (
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            )
            is None
        )
        assert target_store.load_fastsync_target_pointer() is None

    def test_prepare_leaves_only_reservation_when_stream_manifest_write_crashes(
        self,
        tmp_path,
        spec,
    ):
        """Prepare leaves only reservation when stream manifest write crashes."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        stream_store = publisher.recovery_store(spec.name, RECOVERY_IDENTITY)

        with patch.object(stream_store, "save", side_effect=RuntimeError("disk lost")):
            with pytest.raises(RuntimeError, match="disk lost"):
                publisher.prepare_full_sync(
                    spec,
                    {"lsn": "1/2"},
                    recovery_identity=RECOVERY_IDENTITY,
                )

        pointer = publisher.recovery_store(spec.name).load_fastsync_target_pointer()
        assert pointer.state == TARGET_ATTEMPT_RESERVED
        assert stream_store.load() is None
        assert (
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            )
            is None
        )

    def test_reservation_with_stream_promotes_to_active_after_crash(self, tmp_path, spec):
        """Reservation with stream promotes to active after crash."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_PREPARED)
        persist_attempt(publisher, attempt, pointer_state=TARGET_ATTEMPT_RESERVED)

        recovered = publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        )

        assert recovered.attempt_id == attempt.attempt_id
        assert publisher.recovery_store(spec.name).load_fastsync_target_pointer().state == TARGET_ATTEMPT_ACTIVE

    def test_active_pointer_without_stream_fails_closed(self, tmp_path, spec):
        """Active pointer without stream fails closed."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_PREPARED)
        publisher.recovery_store(spec.name).save_fastsync_target_pointer(
            IcebergTargetAttemptPointer.from_attempt(
                attempt,
                TARGET_ATTEMPT_ACTIVE,
            )
        )

        with pytest.raises(RecoveryManifestError, match="has no stream manifest"):
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            )

    def test_completed_pointer_rejects_nonfinalized_stream(self, tmp_path, spec):
        """Completed pointer rejects nonfinalized stream."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_STAGED)
        persist_attempt(publisher, attempt, pointer_state=TARGET_ATTEMPT_COMPLETED)

        with pytest.raises(RecoveryManifestError, match="unsafe stream manifest"):
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            )

        assert publisher.recovery_store(spec.name, RECOVERY_IDENTITY).load() is not None
        assert publisher.recovery_store(spec.name).load_fastsync_target_pointer().state == TARGET_ATTEMPT_COMPLETED

    def test_completed_pointer_recovers_crash_between_manifest_and_pointer_delete(
        self,
        tmp_path,
        spec,
    ):
        """Completed pointer recovers crash between manifest and pointer delete."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_FINALIZED)
        attempt.finalization = {
            snowflake_iceberg_routes.FINALIZATION_GRANTS: True,
            snowflake_iceberg_routes.FINALIZATION_S3_CLEANUP: True,
            snowflake_iceberg_routes.FINALIZATION_STAGING_CLEANUP: True,
        }
        persist_attempt(publisher, attempt, pointer_state=TARGET_ATTEMPT_COMPLETED)
        stream_store = publisher.recovery_store(spec.name, RECOVERY_IDENTITY)
        stream_store.delete(attempt.attempt_id)

        assert (
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            )
            is None
        )
        assert publisher.recovery_store(spec.name).load_fastsync_target_pointer() is None

    def test_corrupt_and_mismatched_target_pointers_fail_closed(self, tmp_path, spec):
        """Corrupt and mismatched target pointers fail closed."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_PREPARED)
        persist_attempt(publisher, attempt)
        target_store = publisher.recovery_store(spec.name)
        pointer = target_store.load_fastsync_target_pointer().as_dict()
        pointer["kind"] = "partial"
        utils.save_dict_to_json(target_store.fastsync_target_pointer_path, pointer)

        with pytest.raises(RecoveryManifestError, match="inconsistent"):
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            )

        with open(
            target_store.fastsync_target_pointer_path,
            "w",
            encoding="utf-8",
        ) as pointer_file:
            pointer_file.write("{")
        with pytest.raises(RecoveryManifestError, match="Cannot read"):
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            )

    def test_multiple_stream_manifests_for_one_target_fail_closed(self, tmp_path, spec):
        """Multiple stream manifests for one target fail closed."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        original = make_attempt(spec, phase=PHASE_PREPARED)
        persist_attempt(publisher, original)
        other_identity = build_recovery_identity(
            "fastsync",
            {"target": spec.name.key},
            transformation_config={},
            stream_identity={
                "tap_id": "other",
                "route": "route",
                "table": "source.other",
            },
            target_table_format="iceberg",
            iceberg_version=3,
        )
        other = make_attempt(spec, phase=PHASE_PREPARED)
        other.recovery_identity = other_identity
        publisher.recovery_store(spec.name, other_identity).save(other)

        with pytest.raises(RecoveryManifestError, match="Multiple Iceberg FastSync"):
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=other_identity,
            )

    def test_fastsync_rejects_target_keyed_manual_conversion_attempt(self, tmp_path, spec):
        """Fastsync rejects target keyed manual conversion attempt."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        manual = make_attempt(spec, phase=PHASE_PREPARED)
        manual.recovery_identity = build_recovery_identity(
            "manual_conversion",
            {"target": spec.name.key},
        )
        publisher.recovery_store(spec.name).save(manual)

        with pytest.raises(RecoveryManifestError, match="conversion attempt is active"):
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            )
        assert publisher.snowflake.queries == []

    def test_recovery_store_rejects_corrupt_or_wrong_attempt_manifest(self, tmp_path, spec):
        """Recovery store rejects corrupt or wrong attempt manifest."""
        store = IcebergRecoveryStore(str(tmp_path), spec.name)
        os.makedirs(tmp_path, exist_ok=True)
        with open(store.path, "w", encoding="utf-8") as manifest_file:
            manifest_file.write("{")

        with pytest.raises(RecoveryManifestError, match="Cannot read"):
            store.load()

        os.remove(store.path)
        attempt = make_attempt(spec)
        store.save(attempt)
        with pytest.raises(RecoveryManifestError, match="attempt changed"):
            store.delete("different-attempt")

    def test_prepare_persists_boundary_before_export_and_reuses_it(self, tmp_path, spec):
        """Prepare persists boundary before export and reuses it."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())

        attempt = publisher.prepare_full_sync(
            spec,
            {"lsn": "1/2"},
            {"state": "next"},
            recovery_identity=RECOVERY_IDENTITY,
        )
        recovered = publisher.load_attempt(spec, expected_kind="full", recovery_identity=RECOVERY_IDENTITY)
        duplicate = publisher.prepare_full_sync(spec, {"lsn": "later"}, recovery_identity=RECOVERY_IDENTITY)

        assert recovered.attempt_id == attempt.attempt_id
        assert duplicate.attempt_id == attempt.attempt_id
        assert duplicate.source_bookmark == {"lsn": "1/2"}
        assert duplicate.is_recovery is True
        assert publisher.inspect_table.call_count == 1


class TestRouteRecovery:
    """Validate route-specific recovery identity."""

    def test_partial_boundary_drift_finds_manifest_and_rejects_replay(
        self, tmp_path, spec
    ):
        """A stable stream key finds, then rejects, a changed range contract."""
        stream_identity = {
            'tap_id': 'unit-test',
            'route': 'unit-test',
            'table': 'source.table',
        }
        original_identity = build_recovery_identity(
            'fastsync',
            {'route': 'unit-test', 'partial_boundary': {'end': '<S>10'}},
            transformation_config={},
            stream_identity=stream_identity,
            target_table_format='iceberg',
            iceberg_version=3,
        )
        changed_identity = build_recovery_identity(
            'fastsync',
            {'route': 'unit-test', 'partial_boundary': {'end': '<S>11'}},
            transformation_config={},
            stream_identity=stream_identity,
            target_table_format='iceberg',
            iceberg_version=3,
        )
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        attempt = publisher.prepare_partial_sync(
            spec,
            {'lsn': '1/2'},
            PartialSyncBoundary('ID', 1, 10),
            recovery_identity=original_identity,
        )

        assert (
            publisher.recovery_store(spec.name, original_identity).path
            == publisher.recovery_store(spec.name, changed_identity).path
        )
        with pytest.raises(
            RecoveryManifestError, match='different source, target'
        ):
            publisher.load_attempt(
                spec,
                expected_kind='partial',
                recovery_identity=changed_identity,
            )
        assert (
            publisher.recovery_store(spec.name, original_identity)
            .load()
            .attempt_id
            == attempt.attempt_id
        )

    def test_recovery_rejects_staging_configuration_drift(self, tmp_path, spec):
        """Recovery rejects staging configuration drift."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        staging_config = {
            "s3_bucket": "original-bucket",
            "s3_key_prefix": "loads",
            "stage": "original-stage",
            "file_format": "original-format",
        }
        attempt = publisher.prepare_full_sync(
            spec,
            {"lsn": "1/2"},
            recovery_identity=RECOVERY_IDENTITY,
            staging_config=staging_config,
        )

        assert attempt.context["staging_config"] == staging_config
        assert (
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
                staging_config=staging_config,
            ).attempt_id
            == attempt.attempt_id
        )

        changed = {**staging_config, "s3_bucket": "different-bucket"}
        with pytest.raises(RecoveryManifestError, match="staging configuration changed"):
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
                staging_config=changed,
            )
        with pytest.raises(RecoveryManifestError, match="staging configuration changed"):
            publisher.prepare_full_sync(
                spec,
                {"lsn": "later"},
                recovery_identity=RECOVERY_IDENTITY,
                staging_config=changed,
            )

    @pytest.mark.parametrize(
        ("value", "expected"),
        (
            (Decimal("123.4500"), {"type": "decimal", "value": "123.4500"}),
            (date(2026, 8, 19), {"type": "date", "value": "2026-08-19"}),
            (
                datetime(2026, 8, 19, 12, 34, 56, 789000),
                {"type": "datetime", "value": "2026-08-19T12:34:56.789000"},
            ),
            (time(12, 34, 56), {"type": "time", "value": "12:34:56"}),
        ),
    )
    def test_partial_boundary_evidence_is_json_safe(self, tmp_path, spec, value, expected):
        """Partial boundary evidence is json safe."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())

        attempt = publisher.prepare_partial_sync(
            spec,
            {"boundary": "saved"},
            PartialSyncBoundary(
                'ID',
                value,
            ),
            recovery_identity=RECOVERY_IDENTITY,
        )

        assert attempt.context["start_value"] == expected
        assert attempt.context["end_value"] is None
        assert attempt.context["end_is_unbounded"] is True
        json.dumps(attempt.as_dict())

    def test_partial_sync_without_key_fails_before_inspection_or_manifest(self, tmp_path, spec):
        """Partial sync without key fails before inspection or manifest."""
        no_key = IcebergTableSpec(spec.name, tuple(replace_nullable(column) for column in spec.columns), ())
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock()

        with pytest.raises(TableCompatibilityError, match="requires a primary key"):
            publisher.prepare_partial_sync(
                no_key,
                {},
                PartialSyncBoundary("ID", 1),
                recovery_identity=RECOVERY_IDENTITY,
            )

        publisher.inspect_table.assert_not_called()
        assert publisher.recovery_store(no_key.name, RECOVERY_IDENTITY).load() is None

    def test_load_attempt_uses_persisted_schema_after_source_schema_change(self, tmp_path, spec):
        """Load attempt uses persisted schema after source schema change."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        original = publisher.prepare_full_sync(spec, {"lsn": "1/2"}, recovery_identity=RECOVERY_IDENTITY)
        changed = IcebergTableSpec(spec.name, spec.columns + (IcebergColumn("NEW", "TEXT"),), spec.primary_key)

        recovered = publisher.load_attempt(changed, expected_kind="full", recovery_identity=RECOVERY_IDENTITY)
        duplicate_prepare = publisher.prepare_full_sync(changed, {"lsn": "later"}, recovery_identity=RECOVERY_IDENTITY)

        assert recovered.table_spec == spec
        assert recovered.attempt_id == original.attempt_id
        assert duplicate_prepare.table_spec == spec
        assert duplicate_prepare.source_bookmark == {"lsn": "1/2"}

    @pytest.mark.parametrize(
        "phase",
        (
            PHASE_PREPARED,
            PHASE_UPLOADED,
            PHASE_STAGING_CREATED,
            PHASE_STAGED,
            PHASE_SUBMITTED,
            PHASE_PUBLISHED,
            PHASE_FINALIZED,
        ),
    )
    @pytest.mark.parametrize(
        "route_case",
        (
            ("full", "partial", PUBLICATION_MISSING_CTAS, {}),
            (
                "partial",
                "full",
                PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
                {
                    "column_name": "ID",
                    "start_value": 1,
                    "end_value": 10,
                    "end_is_unbounded": False,
                    "drop_target": True,
                    "delete_mode": "hard",
                },
            ),
        ),
    )
    def test_route_kind_isolated_across_phases(
        self,
        tmp_path,
        spec,
        phase,
        route_case,
    ):
        """A table-scoped manifest can be resumed only by its creating route."""
        stored_kind, expected_kind, method, context = route_case
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(
            spec,
            phase=phase,
            kind=stored_kind,
            method=method,
            context=context,
        )
        if phase == PHASE_FINALIZED:
            attempt.finalization = {
                snowflake_iceberg_routes.FINALIZATION_GRANTS: True,
                snowflake_iceberg_routes.FINALIZATION_S3_CLEANUP: True,
                snowflake_iceberg_routes.FINALIZATION_STAGING_CLEANUP: True,
            }
            if context.get('replacement_metadata') is not None:
                attempt.finalization[
                    snowflake_iceberg_routes.FINALIZATION_METADATA
                ] = True
        store = publisher.recovery_store(spec.name, RECOVERY_IDENTITY)
        persist_attempt(publisher, attempt)

        with pytest.raises(
            RecoveryManifestError,
            match=rf"belongs to {stored_kind} sync; cannot resume it as {expected_kind} sync",
        ):
            publisher.load_attempt(
                spec,
                expected_kind=expected_kind,
                recovery_identity=RECOVERY_IDENTITY,
            )

        persisted = store.load()
        assert persisted.attempt_id == attempt.attempt_id
        assert persisted.kind == stored_kind
        assert persisted.phase == phase

    def test_prepare_rejects_other_route_manifest(self, tmp_path, spec):
        """Prepare cannot silently reuse a persisted attempt from another route."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(
            spec,
            kind="partial",
            method=PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
            context={
                "column_name": "ID",
                "start_value": 1,
                "end_value": None,
                "end_is_unbounded": True,
                "drop_target": True,
                "delete_mode": "hard",
            },
        )
        persist_attempt(publisher, attempt)
        publisher.inspect_table = MagicMock()

        with pytest.raises(RecoveryManifestError, match="cannot resume it as full sync"):
            publisher.prepare_full_sync(spec, {"lsn": "later"}, recovery_identity=RECOVERY_IDENTITY)

        publisher.inspect_table.assert_not_called()
        assert publisher.recovery_store(spec.name, RECOVERY_IDENTITY).load().attempt_id == attempt.attempt_id

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        (
            ("load_id", "not-a-uuid", "attempt identity"),
            ("attempt_id", "", "attempt identity"),
            ("method", PUBLICATION_PARTIAL_MERGE, "kind or publication method"),
            ("staging_table", "ORDERS", "staging table is unsafe"),
        ),
    )
    def test_load_attempt_rejects_unsafe_production_identity(self, tmp_path, spec, field, value, message):
        """Load attempt rejects unsafe production identity."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec)
        setattr(attempt, field, value)
        persist_attempt(publisher, attempt)

        with pytest.raises(RecoveryManifestError, match=message):
            publisher.load_attempt(spec, expected_kind="full", recovery_identity=RECOVERY_IDENTITY)


class TestStagingRecovery:
    """Validate durable staging progress."""

    @pytest.mark.parametrize(
        ("s3_keys", "message"),
        (
            (["part-1", ""], "must be non-empty strings"),
            (["part-1", 1], "must be non-empty strings"),
            (["part-1", "part-1"], "must be unique"),
            (
                ["part-1", "part-1", ""],
                "must be non-empty strings",
            ),
        ),
    )
    def test_planned_upload_keys_are_validated_before_persistence(
        self,
        tmp_path,
        spec,
        s3_keys,
        message,
    ):
        """Invalid or duplicate upload plans never reach durable state."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        attempt = publisher.prepare_full_sync(
            spec,
            {"lsn": "1/2"},
            recovery_identity=RECOVERY_IDENTITY,
        )

        with pytest.raises(RecoveryManifestError, match=message):
            publisher.record_planned_uploads(attempt, s3_keys)

        recovered = publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        )
        assert recovered.phase == PHASE_PREPARED
        assert recovered.s3_keys == []

    def test_staging_progress_is_durable_and_only_prepublication_can_abort(self, tmp_path, spec):
        """Staging progress is durable and only prepublication can abort."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        attempt = publisher.prepare_full_sync(spec, {"lsn": "1/2"}, recovery_identity=RECOVERY_IDENTITY)

        publisher.record_planned_uploads(attempt, ["part-1", "part-2"])
        recovered = publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        )
        assert recovered.phase == PHASE_PREPARED
        assert recovered.s3_keys == [
            "part-1",
            "part-2",
        ]

        publisher.record_uploaded(attempt, ["part-1", "part-2"])
        recovered = publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        )
        assert recovered.phase == PHASE_UPLOADED
        assert recovered.s3_keys == [
            "part-1",
            "part-2",
        ]

        publisher.record_staging_created(attempt)
        assert (
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            ).phase
            == PHASE_STAGING_CREATED
        )
        publisher.record_staged(attempt, row_count=12, row_fingerprint="checksum")
        assert (
            publisher.load_attempt(
                spec,
                expected_kind="full",
                recovery_identity=RECOVERY_IDENTITY,
            ).phase
            == PHASE_STAGED
        )

        attempt.phase = PHASE_SUBMITTED
        with pytest.raises(RecoveryManifestError, match="Cannot abort"):
            publisher.abort(attempt)

        attempt.phase = PHASE_STAGED
        publisher.abort(attempt)
        assert publisher.load_attempt(spec, expected_kind="full", recovery_identity=RECOVERY_IDENTITY) is None

    def test_upload_completion_must_match_durable_plan(self, tmp_path, spec):
        """Upload completion must match durable plan."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        attempt = publisher.prepare_full_sync(spec, {"lsn": "1/2"}, recovery_identity=RECOVERY_IDENTITY)

        with pytest.raises(RecoveryManifestError, match="do not match the persisted plan"):
            publisher.record_uploaded(attempt, ["unplanned-part"])

        publisher.record_planned_uploads(attempt, ["planned-part"])
        with pytest.raises(RecoveryManifestError, match="do not match the persisted plan"):
            publisher.record_uploaded(attempt, ["different-part"])

        recovered = publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        )
        assert recovered.phase == PHASE_PREPARED
        assert recovered.s3_keys == ["planned-part"]

    def test_sigkill_equivalent_upload_keeps_all_planned_keys_for_restart_cleanup(self, tmp_path, spec):
        """Sigkill equivalent upload keeps all planned keys for restart cleanup."""
        snowflake = MagicMock()
        snowflake._get_s3_key.side_effect = (  # pylint: disable=protected-access
            lambda file_part: f"loads/{os.path.basename(file_part)}"
        )
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        attempt = publisher.prepare_full_sync(spec, {"lsn": "1/2"}, recovery_identity=RECOVERY_IDENTITY)
        file_parts = ["/tmp/export.part0", "/tmp/export.part1"]
        planned_s3_keys = snowflake_iceberg_routes.plan_staging_uploads(publisher, attempt, snowflake, file_parts)
        plans_observed_during_upload = []

        def interrupted_upload(file_part, tmp_dir=None):
            assert tmp_dir == "/tmp"
            plans_observed_during_upload.append(
                publisher.load_attempt(
                    spec,
                    expected_kind="full",
                    recovery_identity=RECOVERY_IDENTITY,
                ).s3_keys
            )
            if file_part.endswith("part1"):
                raise SystemExit("simulated SIGKILL boundary")
            return f"loads/{os.path.basename(file_part)}"

        snowflake.upload_to_s3.side_effect = interrupted_upload
        with pytest.raises(SystemExit, match="simulated SIGKILL boundary"):
            utils.upload_files_to_s3(
                snowflake,
                file_parts,
                "/tmp",
                "staging-bucket",
                planned_s3_keys=planned_s3_keys,
            )

        assert plans_observed_during_upload == [planned_s3_keys, planned_s3_keys]
        recovered = publisher.load_attempt(spec, expected_kind="full", recovery_identity=RECOVERY_IDENTITY)
        assert recovered.phase == PHASE_PREPARED
        assert recovered.s3_keys == planned_s3_keys
        snowflake.s3.delete_object.assert_not_called()

        snowflake_iceberg_routes.restart_staging(
            publisher,
            snowflake,
            {"s3_bucket": "staging-bucket"},
            spec.name.schema,
            spec.name.table,
            recovered,
        )

        assert snowflake.s3.delete_object.call_args_list == [
            call(Bucket="staging-bucket", Key=s3_key) for s3_key in planned_s3_keys
        ]
        snowflake.drop_table.assert_called_once_with(
            spec.name.schema,
            spec.name.table,
            is_temporary=True,
            max_attempts=3,
            staging_table_name=attempt.staging_table,
        )
        reset_attempt = publisher.load_attempt(spec, expected_kind="full", recovery_identity=RECOVERY_IDENTITY)
        assert reset_attempt.phase == PHASE_PREPARED
        assert reset_attempt.s3_keys == []

    @pytest.mark.parametrize(
        ("row_count", "row_fingerprint"),
        ((None, "hash"), (1, None), (True, "hash"), (-1, "hash"), (1, "")),
    )
    def test_record_staged_requires_complete_content_evidence(self, tmp_path, spec, row_count, row_fingerprint):
        """Record staged requires complete content evidence."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_STAGING_CREATED)

        with pytest.raises(RecoveryManifestError, match="row count and row fingerprint"):
            publisher.record_staged(
                attempt,
                row_count=row_count,
                row_fingerprint=row_fingerprint,
            )

        assert attempt.phase == PHASE_STAGING_CREATED

    def test_load_attempt_rejects_published_phase_without_content_evidence(self, tmp_path, spec):
        """Load attempt rejects published phase without content evidence."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=PHASE_PUBLISHED)
        attempt.expected_row_fingerprint = None
        persist_attempt(publisher, attempt)

        with pytest.raises(RecoveryManifestError, match="row count and row fingerprint"):
            publisher.load_attempt(spec, expected_kind="full", recovery_identity=RECOVERY_IDENTITY)

    @pytest.mark.parametrize(
        ("phase", "action"),
        (
            (PHASE_PREPARED, RECOVERY_RESTART_STAGING),
            (PHASE_UPLOADED, RECOVERY_RESTART_STAGING),
            (PHASE_STAGING_CREATED, RECOVERY_RESTART_STAGING),
            (PHASE_STAGED, RECOVERY_PUBLISH),
            (PHASE_PUBLISHED, RECOVERY_FINALIZE),
            (PHASE_FINALIZED, RECOVERY_STATE_HANDOFF),
        ),
    )
    def test_recovery_phase_has_one_safe_next_action(self, tmp_path, spec, phase, action):
        """Recovery phase has one safe next action."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        attempt = make_attempt(spec, phase=phase)
        if phase == PHASE_FINALIZED:
            attempt.finalization = {
                snowflake_iceberg_routes.FINALIZATION_GRANTS: True,
                snowflake_iceberg_routes.FINALIZATION_S3_CLEANUP: True,
                snowflake_iceberg_routes.FINALIZATION_STAGING_CLEANUP: True,
            }

        assert publisher.reconcile(attempt).action == action
