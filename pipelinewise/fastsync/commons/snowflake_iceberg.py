"""Shared Snowflake-managed Iceberg publication and recovery primitives."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import time
from typing import Any, Dict, Iterable, Optional, Tuple
from uuid import uuid4

import snowflake.connector

from pipelinewise.fastsync.commons.snowflake_iceberg_publication import (
    QueryHistoryLookupError,
    QueryHistoryVisibilityTimeoutError,
    SnowflakeIcebergPublicationService,
    _QueryHistoryRecoveryPolicy,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    PHASE_FINALIZED,
    PHASE_PREPARED,
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_STAGING_CREATED,
    PHASE_SUBMITTED,
    PHASE_UPLOADED,
    PUBLICATION_ADDITIVE_OVERWRITE,
    PUBLICATION_INSERT_OVERWRITE,
    PUBLICATION_MISSING_CTAS,
    PUBLICATION_PARTIAL_BOOTSTRAP_CTAS,
    PUBLICATION_PARTIAL_MERGE,
    PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
    PUBLICATION_REPLACEMENT_CTAS,
    RECOVERY_FINALIZE,
    RECOVERY_PUBLISH,
    RECOVERY_RESTART_STAGING,
    RECOVERY_STATE_HANDOFF,
    TABLE_FORMAT_MISSING,
    TABLE_FORMAT_NATIVE,
    TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG,
    TARGET_ATTEMPT_ABORTING,
    TARGET_ATTEMPT_ACTIVE,
    TARGET_ATTEMPT_COMPLETED,
    TARGET_ATTEMPT_RESERVED,
    IcebergColumn,
    IcebergPublicationAttempt,
    IcebergRecoveryStore,
    IcebergTargetAttemptPointer,
    IcebergTableSpec,
    PublicationPlan,
    RecoveryManifestError,
    RecoveryOutcome,
    SnowflakeObjectName,
    SnowflakeTableMetadata,
    SnowflakeTableSnapshot,
    TableFormatDiscoveryError,
    _json_safe_boundary,
    _sql_hash,
    canonical_iceberg_type as _canonical_iceberg_type,
    quote_identifier,
    sql_string_literal,
    validate_recovery_identity,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    AmbiguousPublicationError,
    IcebergFinalizationService,
    IcebergPublicationError as _IcebergPublicationError,
    StagingPrimaryKeyError,
    TableCompatibilityError,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_inspection import SnowflakeTableInspector
from pipelinewise.fastsync.commons.snowflake_iceberg_coordination import RecoveryCoordinator
from pipelinewise.fastsync.commons.snowflake_iceberg_manifest import (
    ConversionManifestPayload,
    FullSyncManifestPayload,
    PartialSyncManifestPayload,
    manifest_payload,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    MANAGED_ICEBERG_VERSION_SPECS,
    MANAGED_ICEBERG_V3_SPEC,
    MANAGED_ICEBERG_TABLE_OPTIONS_BY_VERSION,
    MANAGED_ICEBERG_V3_TABLE_OPTIONS,
    ManagedIcebergVersionSpec,
    SUPPORTED_MANAGED_ICEBERG_TABLE_FORMATS,
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
    managed_iceberg_version_registry,
    managed_iceberg_version_spec,
    repository_contract,
)
from pipelinewise.fastsync.commons.snowflake_sql_client import SnowflakeSqlClient
from pipelinewise.utils import pem2der


IcebergPublicationError = _IcebergPublicationError
canonical_iceberg_type = _canonical_iceberg_type
DEFAULT_QUERY_HISTORY_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_QUERY_HISTORY_POLL_TIMEOUT_SECONDS = 900

__all__ = (
    'MANAGED_ICEBERG_TABLE_OPTIONS_BY_VERSION',
    'MANAGED_ICEBERG_VERSION_SPECS',
    'MANAGED_ICEBERG_V3_SPEC',
    'MANAGED_ICEBERG_V3_TABLE_OPTIONS',
    'PHASE_FINALIZED',
    'PHASE_PREPARED',
    'PHASE_PUBLISHED',
    'PHASE_STAGED',
    'PHASE_STAGING_CREATED',
    'PHASE_SUBMITTED',
    'PHASE_UPLOADED',
    'PUBLICATION_ADDITIVE_OVERWRITE',
    'PUBLICATION_INSERT_OVERWRITE',
    'PUBLICATION_MISSING_CTAS',
    'PUBLICATION_PARTIAL_BOOTSTRAP_CTAS',
    'PUBLICATION_PARTIAL_MERGE',
    'PUBLICATION_PARTIAL_REPLACEMENT_CTAS',
    'PUBLICATION_REPLACEMENT_CTAS',
    'RECOVERY_FINALIZE',
    'RECOVERY_PUBLISH',
    'RECOVERY_RESTART_STAGING',
    'RECOVERY_STATE_HANDOFF',
    'SUPPORTED_MANAGED_ICEBERG_TABLE_FORMATS',
    'TABLE_FORMAT_MANAGED_ICEBERG_V3',
    'TABLE_FORMAT_MISSING',
    'TABLE_FORMAT_NATIVE',
    'TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG',
    'TARGET_ATTEMPT_ABORTING',
    'TARGET_ATTEMPT_ACTIVE',
    'TARGET_ATTEMPT_COMPLETED',
    'TARGET_ATTEMPT_RESERVED',
    'AmbiguousPublicationError',
    'ConversionManifestPayload',
    'DEFAULT_QUERY_HISTORY_POLL_INTERVAL_SECONDS',
    'DEFAULT_QUERY_HISTORY_POLL_TIMEOUT_SECONDS',
    'FullSyncManifestPayload',
    'IcebergColumn',
    'IcebergPublicationAttempt',
    'IcebergPublicationError',
    'IcebergRecoveryStore',
    'IcebergTargetAttemptPointer',
    'IcebergTableSpec',
    'ManagedIcebergVersionSpec',
    'PartialSyncManifestPayload',
    'PartialSyncBoundary',
    'PublicationPlan',
    'QueryHistoryLookupError',
    'QueryHistoryVisibilityTimeoutError',
    'RecoveryManifestError',
    'RecoveryOutcome',
    'SnowflakeIcebergPublisher',
    'SnowflakeObjectName',
    'SnowflakeQueryAdapter',
    'SnowflakeTableMetadata',
    'SnowflakeTableSnapshot',
    'StagingPrimaryKeyError',
    'TableCompatibilityError',
    'TableFormatDiscoveryError',
    '_sql_hash',
    'canonical_iceberg_type',
    'managed_iceberg_version_registry',
    'managed_iceberg_version_spec',
    'repository_contract',
    'quote_identifier',
    'sql_string_literal',
    'validate_recovery_identity',
)


@dataclass(frozen=True)
class PartialSyncBoundary:
    """Resolved PartialSync range and replacement intent for recovery."""

    where_clause_sql: str
    start_value: Any = None
    end_value: Any = None
    drop_target: bool = False

    def as_context(self) -> Dict[str, Any]:
        """Return stable manifest evidence for the resolved range."""
        return {
            'where_clause_sql': self.where_clause_sql,
            'start_value': _json_safe_boundary(self.start_value),
            'end_value': _json_safe_boundary(self.end_value),
            'end_is_unbounded': self.end_value is None,
            'drop_target': self.drop_target,
            'delete_mode': 'hard',
        }


class SnowflakeQueryAdapter(SnowflakeSqlClient):
    """S3-free Snowflake executor for core discovery and manual conversion."""

    def create_query_tag(
        self,
        query_tag_props: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Serialize an exact, stable Snowflake query tag."""
        return json.dumps(query_tag_props or {}, sort_keys=True, separators=(',', ':'))

    def _connect(self, **kwargs):
        return snowflake.connector.connect(**kwargs)

    def _private_key(self):
        return pem2der(self.connection_config['private_key'])

    @staticmethod
    def _monotonic():
        return time.monotonic()


class SnowflakeIcebergPublisher:  # pylint: disable=too-many-public-methods,too-many-instance-attributes
    """Plan, publish, and recover Snowflake-managed Iceberg v3 loads."""

    def __init__(
        self,
        snowflake_adapter,
        runtime_dir: str,
        history_poll_attempts: Optional[int] = None,
        history_poll_interval_seconds: float = DEFAULT_QUERY_HISTORY_POLL_INTERVAL_SECONDS,
        *,
        history_poll_timeout_seconds: float = DEFAULT_QUERY_HISTORY_POLL_TIMEOUT_SECONDS,
        history_lookup_timeout_seconds: float = 30.0,
    ):
        self.snowflake = snowflake_adapter
        self.runtime_dir = runtime_dir
        self.history_policy = _QueryHistoryRecoveryPolicy(
            history_poll_attempts,
            history_poll_interval_seconds,
            history_poll_timeout_seconds,
            history_lookup_timeout_seconds,
        )
        self.history_snowflake = self._history_query_adapter(snowflake_adapter)
        self.inspector = SnowflakeTableInspector(snowflake_adapter)
        self.publication_service = SnowflakeIcebergPublicationService(self)
        self.finalization_service = IcebergFinalizationService(self)
        self.recovery_coordinator = RecoveryCoordinator(
            runtime_dir,
            self._validate_production_attempt,
        )

    def plan_full_sync(self, attempt, spec):
        """Build the guarded FullSync publication plan."""
        return self.publication_service.plan_full_sync(attempt, spec)

    def plan_partial_sync(self, attempt, spec):
        """Build the guarded PartialSync publication plan."""
        return self.publication_service.plan_partial_sync(attempt, spec)

    def publish_full_sync(self, attempt, spec):
        """Publish a staged FullSync attempt."""
        return self.publication_service.publish_full_sync(attempt, spec)

    def publish_partial_sync(self, attempt, spec):
        """Publish a staged PartialSync attempt."""
        return self.publication_service.publish_partial_sync(attempt, spec)

    def reconcile(self, attempt, spec=None):
        """Return the only safe action for a durable attempt."""
        return self.publication_service.reconcile(attempt, spec)

    def restore_metadata(self, attempt):
        """Restore metadata not copied by replacement CTAS."""
        return self.publication_service.restore_metadata(attempt)

    def mark_finalized(self, attempt, completed_actions=None):
        """Persist completion of caller-owned finalization actions."""
        return self.finalization_service.mark_finalized(
            attempt,
            completed_actions,
        )

    def record_finalization_action(self, attempt, action):
        """Persist one idempotent finalization action."""
        return self.finalization_service.record_finalization_action(
            attempt,
            action,
        )

    def complete_state_handoff(self, attempt):
        """Delete recovery state after the bookmark is durable."""
        return self.finalization_service.complete_state_handoff(attempt)

    def _full_method(self, spec, snapshot, iceberg_version):
        return self.publication_service._full_method(  # pylint: disable=protected-access
            spec,
            snapshot,
            iceberg_version,
        )

    def _partial_method(self, spec, snapshot, drop_target, iceberg_version):
        return self.publication_service._partial_method(  # pylint: disable=protected-access
            spec,
            snapshot,
            drop_target,
            iceberg_version,
        )

    def _preflight_replacement(self, target, destination_spec=None):
        return self.publication_service._preflight_replacement(  # pylint: disable=protected-access
            target,
            destination_spec,
        )

    def _validate_partial_staging_primary_key(
        self,
        attempt,
        spec,
        query_phase,
    ):
        return self.publication_service._validate_partial_staging_primary_key(  # pylint: disable=protected-access
            attempt,
            spec,
            query_phase,
        )

    def _content_evidence(
        self,
        spec,
        table,
        project,
        where_clause='',
        query_tag=None,
    ):
        return self.publication_service._content_evidence(  # pylint: disable=protected-access
            spec,
            table,
            project,
            where_clause,
            query_tag,
        )

    def _verify_published(self, attempt, spec):
        return self.publication_service._verify_published(  # pylint: disable=protected-access
            attempt,
            spec,
        )

    def _verify_replacement_metadata(self, attempt):
        return self.publication_service._verify_replacement_metadata(  # pylint: disable=protected-access
            attempt
        )

    @staticmethod
    def _history_query_adapter(snowflake_adapter):
        if callable(getattr(snowflake_adapter, 'query_with_timeout', None)):
            return snowflake_adapter
        config = getattr(snowflake_adapter, 'connection_config', None)
        required = ('account', 'dbname', 'user', 'private_key', 'warehouse')
        if callable(getattr(config, 'get', None)) and all(config.get(key) for key in required):
            return SnowflakeQueryAdapter(config)
        return snowflake_adapter

    def recovery_store(
        self,
        target: SnowflakeObjectName,
        recovery_identity: Optional[Dict[str, Any]] = None,
    ) -> IcebergRecoveryStore:
        """Return one reentrant manifest store for a target or source stream."""
        return self.recovery_coordinator.recovery_store(target, recovery_identity)

    def table_lock(
        self,
        target: SnowflakeObjectName,
        recovery_identity: Optional[Dict[str, Any]] = None,
    ):
        """Serialize both the source stream and physical target lifecycle."""
        return self.recovery_coordinator.table_lock(target, recovery_identity)

    def load_attempt(
        self,
        spec_or_target,
        expected_kind: str,
        recovery_identity: Dict[str, Any],
        staging_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[IcebergPublicationAttempt]:
        """Load recovery state before resolving a new source boundary."""
        validate_recovery_identity(recovery_identity)
        target = (
            spec_or_target.name
            if isinstance(spec_or_target, IcebergTableSpec)
            else spec_or_target
        )
        if not isinstance(target, SnowflakeObjectName):
            raise TypeError('Iceberg recovery lookup requires a target object name')
        with self.table_lock(target, recovery_identity):
            attempt = self._reconcile_target_attempt(
                target,
                recovery_identity,
            )
            self._validate_expected_kind(attempt, expected_kind)
            if attempt is not None:
                self._validate_recovery_identity(attempt, recovery_identity)
                self._validate_production_attempt(attempt)
                self._validate_staging_config(attempt, staging_config)
            return attempt

    def _reconcile_target_attempt(
        self,
        target: SnowflakeObjectName,
        recovery_identity: Dict[str, Any],
    ) -> Optional[IcebergPublicationAttempt]:
        """Reconcile the target pointer with its canonical stream manifest."""
        return self.recovery_coordinator.reconcile_target_attempt(
            target,
            recovery_identity,
        )

    @staticmethod
    def _validate_recovery_identity(attempt, expected_identity):
        RecoveryCoordinator.validate_attempt_identity(attempt, expected_identity)

    @staticmethod
    def _validate_expected_kind(attempt, expected_kind):
        if expected_kind not in ('full', 'partial'):
            raise ValueError('Expected Iceberg recovery kind must be full or partial')
        if attempt is not None and attempt.kind != expected_kind:
            raise RecoveryManifestError(
                f'Iceberg recovery manifest belongs to {attempt.kind} sync; '
                f'cannot resume it as {expected_kind} sync'
            )

    @staticmethod
    def _validate_staging_config(attempt, staging_config):
        if (
            staging_config is not None
            and attempt.manifest_payload.staging_config != staging_config
        ):
            raise RecoveryManifestError(
                'Iceberg recovery staging configuration changed after the source boundary was captured'
            )

    @staticmethod
    def _validate_production_attempt(attempt: IcebergPublicationAttempt) -> None:
        attempt.validate_table_format_contract()
        allowed_methods = {
            'full': {
                PUBLICATION_MISSING_CTAS,
                PUBLICATION_INSERT_OVERWRITE,
                PUBLICATION_ADDITIVE_OVERWRITE,
                PUBLICATION_REPLACEMENT_CTAS,
            },
            'partial': {
                PUBLICATION_PARTIAL_BOOTSTRAP_CTAS,
                PUBLICATION_PARTIAL_MERGE,
                PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
            },
        }
        if attempt.kind not in allowed_methods or attempt.method not in allowed_methods[attempt.kind]:
            raise RecoveryManifestError('Iceberg recovery manifest kind or publication method is invalid')
        if not all(
            re.fullmatch(r'[0-9a-f]{32}', identifier or '')
            for identifier in (attempt.load_id, attempt.attempt_id)
        ):
            raise RecoveryManifestError('Iceberg recovery manifest attempt identity is invalid')
        expected_staging = attempt.target.staging_name(attempt.load_id)
        if attempt.staging_table != expected_staging:
            raise RecoveryManifestError('Iceberg recovery manifest staging table is unsafe')
        if attempt.kind == 'partial':
            payload = attempt.manifest_payload
            if (
                not isinstance(payload.where_clause_sql, str)
                or not payload.where_clause_sql.strip()
                or not isinstance(payload.end_is_unbounded, bool)
                or payload.delete_mode != 'hard'
            ):
                raise RecoveryManifestError('Iceberg PartialSync recovery context is invalid')

    def discover_table_format(self, schema_name: str, table_name: str) -> str:
        """Return the exact physical format of a table in the configured database."""
        return self.inspector.discover_table_format(schema_name, table_name)

    def _discover_table_row(self, target: SnowflakeObjectName) -> Optional[Dict[str, Any]]:
        return self.inspector.discover_table_row(target)

    def inspect_table(self, target: SnowflakeObjectName) -> SnowflakeTableSnapshot:
        """Inspect the exact target format, schema, key, and object identity."""
        return self.inspector.inspect_table(target)

    def prepare_full_sync(
        self,
        spec: IcebergTableSpec,
        source_bookmark: Dict[str, Any],
        intended_state: Optional[Dict[str, Any]] = None,
        *,
        recovery_identity: Dict[str, Any],
        staging_config: Optional[Dict[str, Any]] = None,
    ) -> IcebergPublicationAttempt:
        """Persist a FullSync boundary and immutable publication decision."""
        payload_context = {}
        if staging_config is not None:
            payload_context['staging_config'] = dict(staging_config)
        return self._prepare(
            'full',
            spec,
            source_bookmark,
            intended_state,
            recovery_identity,
            FullSyncManifestPayload.from_context(payload_context),
        )

    def prepare_partial_sync(
        self,
        spec: IcebergTableSpec,
        source_bookmark: Dict[str, Any],
        boundary: PartialSyncBoundary,
        *,
        recovery_identity: Dict[str, Any],
        staging_config: Optional[Dict[str, Any]] = None,
    ) -> IcebergPublicationAttempt:
        """Persist a PartialSync boundary, range evidence, and publication decision."""
        if not spec.primary_key:
            raise TableCompatibilityError('Iceberg PartialSync requires a primary key')
        payload_context = boundary.as_context()
        if staging_config is not None:
            payload_context['staging_config'] = dict(staging_config)
        return self._prepare(
            'partial',
            spec,
            source_bookmark,
            None,
            recovery_identity,
            PartialSyncManifestPayload.from_context(payload_context),
        )

    def _prepare(
        self,
        kind,
        spec,
        source_bookmark,
        intended_state,
        recovery_identity,
        payload,
    ):
        validate_recovery_identity(recovery_identity)
        with self.table_lock(spec.name, recovery_identity):
            existing = self._reconcile_target_attempt(
                spec.name,
                recovery_identity,
            )
            if existing is not None:
                self._validate_expected_kind(existing, kind)
                self._validate_recovery_identity(existing, recovery_identity)
                self._validate_production_attempt(existing)
                self._validate_staging_config(
                    existing, payload.staging_config
                )
                return existing

            snapshot = self.inspect_table(spec.name)
            load_id = uuid4().hex
            staging_table = spec.name.staging_name(load_id)
            method, payload = self._method_for_snapshot(
                kind,
                spec,
                snapshot,
                payload,
                recovery_identity['iceberg_version'],
            )
            attempt = IcebergPublicationAttempt(
                load_id=load_id,
                attempt_id=uuid4().hex,
                kind=kind,
                table_spec=spec,
                source_bookmark=dict(source_bookmark or {}),
                intended_state=intended_state,
                staging_table=staging_table,
                method=method,
                pre_publication_target_fingerprint=snapshot.fingerprint,
                target_table_format=recovery_identity['target_table_format'],
                iceberg_version=recovery_identity['iceberg_version'],
                recovery_identity=dict(recovery_identity),
                context=payload.as_context(),
            )
            self._validate_production_attempt(attempt)
            self._persist_new_attempt(attempt)
            return attempt

    def _persist_new_attempt(self, attempt: IcebergPublicationAttempt) -> None:
        """Create the target pointer before publishing the stream manifest."""
        self.recovery_coordinator.persist_new_attempt(attempt)

    def _method_for_snapshot(self, kind, spec, snapshot, payload, iceberg_version):
        if kind == 'full':
            method, _ = self._full_method(spec, snapshot, iceberg_version)
        else:
            method, _ = self._partial_method(
                spec,
                snapshot,
                bool(payload.drop_target),
                iceberg_version,
            )
        if method in (PUBLICATION_REPLACEMENT_CTAS, PUBLICATION_PARTIAL_REPLACEMENT_CTAS):
            context = payload.as_context()
            context['replacement_metadata'] = self._preflight_replacement(
                spec.name,
                spec,
            ).as_dict()
            payload = manifest_payload(kind, context)
        return method, payload

    def record_planned_uploads(
        self, attempt: IcebergPublicationAttempt, s3_keys: Iterable[str]
    ) -> None:
        """Persist deterministic S3 keys before the first upload can begin."""
        if attempt.phase != PHASE_PREPARED:
            raise RecoveryManifestError(
                f'Cannot plan Iceberg uploads in phase {attempt.phase}'
            )
        planned_s3_keys = list(s3_keys)
        if any(not isinstance(s3_key, str) or not s3_key for s3_key in planned_s3_keys):
            raise RecoveryManifestError('Iceberg staging S3 keys must be non-empty strings')
        if len(set(planned_s3_keys)) != len(planned_s3_keys):
            raise RecoveryManifestError('Iceberg staging S3 keys must be unique')
        self._transition(attempt, PHASE_PREPARED, s3_keys=planned_s3_keys)

    def record_uploaded(self, attempt: IcebergPublicationAttempt, s3_keys: Iterable[str]) -> None:
        """Persist upload completion only when it matches the durable key plan."""
        uploaded_s3_keys = list(s3_keys)
        if attempt.phase != PHASE_PREPARED:
            raise RecoveryManifestError(
                f'Cannot complete Iceberg uploads in phase {attempt.phase}'
            )
        if uploaded_s3_keys != attempt.s3_keys:
            raise RecoveryManifestError(
                'Uploaded Iceberg staging keys do not match the persisted plan'
            )
        self._transition(attempt, PHASE_UPLOADED)

    def record_staging_created(self, attempt: IcebergPublicationAttempt) -> None:
        """Persist that the attempt owns its native staging table."""
        self._transition(attempt, PHASE_STAGING_CREATED)

    def staging_evidence(
        self,
        attempt: IcebergPublicationAttempt,
        spec: IcebergTableSpec,
        loaded_row_count: Optional[int] = None,
    ) -> Tuple[int, str]:
        """Return deterministic evidence for the canonical staged projection."""
        if attempt.phase != PHASE_STAGING_CREATED:
            raise RecoveryManifestError('Iceberg staging evidence requires a created staging table')
        source = spec.name.with_table(attempt.staging_table)
        self._validate_partial_staging_primary_key(attempt, spec, 'staging_key_validation')
        row_count, row_fingerprint = self._content_evidence(
            spec, source, project=True, query_tag={**attempt.query_tag, 'phase': 'staging_evidence'},
        )
        if loaded_row_count is not None and row_count != loaded_row_count:
            raise RecoveryManifestError(
                'Iceberg staging row count does not match the completed COPY'
            )
        return row_count, row_fingerprint

    def record_staged(
        self,
        attempt: IcebergPublicationAttempt,
        s3_keys: Optional[Iterable[str]] = None,
        row_count: Optional[int] = None,
        row_fingerprint: Optional[str] = None,
    ) -> None:
        """Persist completed staging evidence before publication."""
        attempt.validate_staging_evidence(row_count, row_fingerprint)
        updates = {
            'expected_row_count': row_count,
            'expected_row_fingerprint': row_fingerprint,
        }
        if s3_keys is not None:
            updates['s3_keys'] = list(s3_keys)
        self._transition(attempt, PHASE_STAGED, **updates)

    def reset_staging(self, attempt: IcebergPublicationAttempt) -> None:
        """Reset a safely cleaned pre-publication attempt for re-export."""
        if attempt.phase in (PHASE_SUBMITTED, PHASE_PUBLISHED, PHASE_FINALIZED):
            raise RecoveryManifestError('Cannot reset staging after publication submission')
        self._transition(
            attempt,
            PHASE_PREPARED,
            s3_keys=[],
            expected_row_count=None,
            expected_row_fingerprint=None,
        )

    def abort(self, attempt: IcebergPublicationAttempt) -> None:
        """Remove a manifest only after caller-owned pre-publication cleanup succeeds."""
        if attempt.phase in (PHASE_SUBMITTED, PHASE_PUBLISHED, PHASE_FINALIZED):
            raise RecoveryManifestError('Cannot abort after publication submission')
        self._abort_attempt_cleanup(attempt)

    def _transition(self, attempt: IcebergPublicationAttempt, phase: str, **updates) -> None:
        self.recovery_coordinator.transition(attempt, phase, **updates)

    def _save_active_attempt(self, attempt: IcebergPublicationAttempt) -> None:
        self.recovery_coordinator.save_active_attempt(attempt)

    def _rearm_partial_merge_replay(
        self,
        attempt: IcebergPublicationAttempt,
    ) -> None:
        self.recovery_coordinator.rearm_partial_merge_replay(attempt)

    def _complete_attempt_cleanup(
        self,
        attempt: IcebergPublicationAttempt,
    ) -> None:
        self.recovery_coordinator.complete_attempt_cleanup(attempt)

    def _abort_attempt_cleanup(
        self,
        attempt: IcebergPublicationAttempt,
    ) -> None:
        self.recovery_coordinator.abort_attempt_cleanup(attempt)
