"""Planning, publication, and reconciliation for managed Iceberg loads."""

# pylint: disable=too-many-lines

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import time
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from pipelinewise.fastsync.commons import utils
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
    IcebergColumn,
    IcebergPublicationAttempt,
    IcebergTableSpec,
    PublicationPlan,
    RecoveryManifestError,
    RecoveryOutcome,
    SnowflakeObjectName,
    SnowflakeTableMetadata,
    SnowflakeTableSnapshot,
    TableFormatDiscoveryError,
    _publication_query_type,
    _row_value,
    _snowflake_boolean,
    _sql_hash,
    quote_identifier,
    sql_string_literal,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    AmbiguousPublicationError,
    RetryableQueryHistoryRecoveryError,
    StagingPrimaryKeyError,
    TableCompatibilityError,
    content_evidence_mismatch,
    validate_required_finalization_actions,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    managed_iceberg_version_spec,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_staging import (
    validate_partial_staging_primary_key,
)


_PUBLICATION_SUBMITTED_AT = 'publication_submitted_at'
_TERMINAL_HISTORY_STATUSES = frozenset({
    'success',
    'failed_with_error',
    'failed_with_incident',
})
_QUERY_HISTORY_SQL = (
    'SELECT QUERY_ID, QUERY_TEXT, QUERY_TYPE, EXECUTION_STATUS '
    'FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER('
    "END_TIME_RANGE_START => DATEADD('minute', -5, TO_TIMESTAMP_LTZ(%(submitted_at)s)), "
    "END_TIME_RANGE_END => LEAST(CURRENT_TIMESTAMP(), "
    "DATEADD('hour', 24, TO_TIMESTAMP_LTZ(%(submitted_at)s))), "
    'RESULT_LIMIT => 10000)) '
    'WHERE QUERY_TAG = %(query_tag)s ORDER BY START_TIME'
)


@dataclass(frozen=True)
class _QueryHistoryRecoveryPolicy:
    """Bound elapsed time and individual lookups during recovery."""

    max_attempts: Optional[int]
    poll_interval_seconds: float
    timeout_seconds: float
    lookup_timeout_seconds: float

    def __post_init__(self):
        if (
            self.max_attempts is not None
            and (
                isinstance(self.max_attempts, bool)
                or not isinstance(self.max_attempts, int)
                or self.max_attempts <= 0
            )
        ):
            raise ValueError('history_poll_attempts must be a positive integer or None')
        for name, value in (
            ('history_poll_interval_seconds', self.poll_interval_seconds),
            ('history_poll_timeout_seconds', self.timeout_seconds),
            ('history_lookup_timeout_seconds', self.lookup_timeout_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f'{name} must be a positive finite number')


@dataclass(frozen=True)
class _QueryHistoryPollResult:
    """Observed query-history state at the recovery deadline or terminal row."""

    rows: Tuple[Dict[str, Any], ...]
    poll_count: int
    elapsed_seconds: float
    statuses: Tuple[str, ...]


class QueryHistoryVisibilityTimeoutError(RetryableQueryHistoryRecoveryError):
    """A publication did not become terminal before the recovery deadline."""

    def __init__(self, attempt_id, elapsed_seconds, poll_count, last_statuses):
        self.attempt_id = attempt_id
        self.elapsed_seconds = elapsed_seconds
        self.poll_count = poll_count
        self.last_statuses = tuple(last_statuses)
        status_text = ', '.join(self.last_statuses) if self.last_statuses else 'none visible'
        super().__init__(
            f'Snowflake query history did not expose a terminal publication for attempt '
            f'{attempt_id} after {poll_count} lookups in {elapsed_seconds:.3f}s; '
            f'last statuses: {status_text}'
        )


class QueryHistoryLookupError(RetryableQueryHistoryRecoveryError):
    """A bounded Snowflake query-history lookup failed."""

    def __init__(self, attempt_id, elapsed_seconds, poll_count):
        self.attempt_id = attempt_id
        self.elapsed_seconds = elapsed_seconds
        self.poll_count = poll_count
        super().__init__(
            f'Snowflake query-history lookup failed for attempt {attempt_id} '
            f'on lookup {poll_count} after {elapsed_seconds:.3f}s'
        )


class SnowflakeIcebergPublicationService:
    """Publication behavior with an explicit publisher dependency."""

    def __init__(self, publisher):
        self.publisher = publisher

    @property
    def snowflake(self):
        """Return the explicitly composed Snowflake dependency."""
        return self.publisher.snowflake

    @property
    def history_snowflake(self):
        """Return the bounded query-history dependency."""
        return self.publisher.history_snowflake

    @property
    def history_policy(self):
        """Return the configured query-history recovery policy."""
        return self.publisher.history_policy

    def inspect_table(self, target):
        """Inspect through the publisher's composed catalog service."""
        return self.publisher.inspect_table(target)

    def _discover_table_row(self, target):
        return self.publisher._discover_table_row(  # pylint: disable=protected-access
            target
        )

    def _validate_production_attempt(self, attempt):
        return self.publisher._validate_production_attempt(  # pylint: disable=protected-access
            attempt
        )

    def _transition(self, attempt, phase, **updates):
        return self.publisher._transition(  # pylint: disable=protected-access
            attempt,
            phase,
            **updates,
        )

    def _save_active_attempt(self, attempt):
        return self.publisher._save_active_attempt(  # pylint: disable=protected-access
            attempt
        )

    def plan_full_sync(
        self, attempt: IcebergPublicationAttempt, spec: IcebergTableSpec
    ) -> PublicationPlan:
        """Build a guarded FullSync publication plan from persisted evidence."""
        self._validate_production_attempt(attempt)
        iceberg_version = attempt.iceberg_version
        expected_table_format = managed_iceberg_version_spec(
            iceberg_version
        ).table_format
        snapshot = self.inspect_table(spec.name)
        if snapshot.fingerprint == attempt.pre_publication_target_fingerprint:
            method, additions = self._full_method(
                spec, snapshot, iceberg_version
            )
        elif attempt.method == PUBLICATION_ADDITIVE_OVERWRITE:
            compatibility, additions = self._compatibility(spec, snapshot.spec)
            if (
                snapshot.table_format != expected_table_format
                or compatibility not in ('exact', 'additive')
            ):
                raise RecoveryManifestError('Iceberg target changed after the source boundary was captured')
            method = PUBLICATION_ADDITIVE_OVERWRITE
            if compatibility == 'exact':
                attempt.update_manifest_payload({
                    'schema_evolution_applied': True,
                })
                self._save_active_attempt(attempt)
        else:
            raise RecoveryManifestError('Iceberg target changed after the source boundary was captured')
        if attempt.method != method:
            raise RecoveryManifestError('Iceberg publication method changed after the source boundary was captured')
        if method == PUBLICATION_REPLACEMENT_CTAS:
            self.publisher._verify_replacement_metadata(  # pylint: disable=protected-access
                attempt
            )
        preparation = tuple(
            f'ALTER ICEBERG TABLE {spec.name.quoted} ADD COLUMN {column.definition}'
            for column in additions
        )
        if method in (PUBLICATION_MISSING_CTAS, PUBLICATION_REPLACEMENT_CTAS):
            metadata = (
                SnowflakeTableMetadata.from_dict(
                    attempt.manifest_payload.replacement_metadata
                )
                if method == PUBLICATION_REPLACEMENT_CTAS else None
            )
            publication = (
                self._ctas_sql(
                    spec,
                    attempt.staging_table,
                    replace_table=method == PUBLICATION_REPLACEMENT_CTAS,
                    metadata=metadata,
                    iceberg_version=iceberg_version,
                ),
            )
        else:
            publication = (self._insert_overwrite_sql(spec, attempt.staging_table),)
        return PublicationPlan(method, preparation, publication, snapshot.fingerprint, attempt.query_tag)

    def plan_partial_sync(
        self, attempt: IcebergPublicationAttempt, spec: IcebergTableSpec
    ) -> PublicationPlan:
        """Build a guarded PartialSync publication plan from persisted evidence."""
        self._validate_production_attempt(attempt)
        iceberg_version = attempt.iceberg_version
        expected_table_format = managed_iceberg_version_spec(
            iceberg_version
        ).table_format
        if not spec.primary_key:
            raise TableCompatibilityError('Iceberg PartialSync requires a primary key')
        snapshot = self.inspect_table(spec.name)
        drop_target = bool(attempt.manifest_payload.drop_target)
        if snapshot.fingerprint == attempt.pre_publication_target_fingerprint:
            method, additions = self._partial_method(
                spec, snapshot, drop_target, iceberg_version
            )
        elif attempt.method == PUBLICATION_PARTIAL_MERGE:
            compatibility, additions = self._compatibility(spec, snapshot.spec)
            if (
                snapshot.table_format != expected_table_format
                or compatibility not in ('exact', 'additive')
            ):
                raise RecoveryManifestError('Iceberg target changed after the partial range was resolved')
            method = PUBLICATION_PARTIAL_MERGE
            if compatibility == 'exact':
                attempt.update_manifest_payload({
                    'schema_evolution_applied': True,
                })
                self._save_active_attempt(attempt)
        else:
            raise RecoveryManifestError('Iceberg target changed after the partial range was resolved')
        if attempt.method != method:
            raise RecoveryManifestError('Iceberg publication method changed after the partial range was resolved')
        if method == PUBLICATION_PARTIAL_REPLACEMENT_CTAS:
            self.publisher._verify_replacement_metadata(  # pylint: disable=protected-access
                attempt
            )

        preparation = tuple(
            f'ALTER ICEBERG TABLE {spec.name.quoted} ADD COLUMN {column.definition}'
            for column in additions
        )
        if method == PUBLICATION_PARTIAL_BOOTSTRAP_CTAS:
            publication = (
                self._ctas_sql(
                    spec,
                    attempt.staging_table,
                    iceberg_version=iceberg_version,
                ),
            )
        elif method == PUBLICATION_PARTIAL_REPLACEMENT_CTAS:
            metadata = SnowflakeTableMetadata.from_dict(
                attempt.manifest_payload.replacement_metadata
            )
            publication = (
                self._ctas_sql(
                    spec,
                    attempt.staging_table,
                    replace_table=True,
                    metadata=metadata,
                    iceberg_version=iceberg_version,
                ),
            )
        else:
            publication = self._partial_merge_sql(spec, attempt)
        return PublicationPlan(method, preparation, publication, snapshot.fingerprint, attempt.query_tag)

    def _partial_method(
        self, spec, snapshot, drop_target, iceberg_version
    ):
        if snapshot.table_format == TABLE_FORMAT_MISSING:
            return PUBLICATION_PARTIAL_BOOTSTRAP_CTAS, ()
        self._require_managed_version(snapshot, spec.name, iceberg_version)
        self._reject_text_variant_mismatch(spec, snapshot.spec)
        if drop_target:
            return PUBLICATION_PARTIAL_REPLACEMENT_CTAS, ()
        compatibility, additions = self._compatibility(spec, snapshot.spec)
        if compatibility == 'incompatible':
            raise TableCompatibilityError(
                f'Existing Iceberg table {spec.name.quoted} is incompatible with PartialSync; '
                'use drop_target_table for explicit replacement'
            )
        return PUBLICATION_PARTIAL_MERGE, additions

    @staticmethod
    def _require_managed_version(
        snapshot: SnowflakeTableSnapshot,
        target: SnowflakeObjectName,
        iceberg_version: int,
    ) -> None:
        expected_format = managed_iceberg_version_spec(
            iceberg_version
        ).table_format
        if snapshot.table_format != expected_format:
            raise TableCompatibilityError(
                f'Expected managed Iceberg v{iceberg_version} table '
                f'{target.quoted}, found {snapshot.table_format}'
            )

    def _full_method(
        self,
        expected: IcebergTableSpec,
        snapshot: SnowflakeTableSnapshot,
        iceberg_version: int,
    ) -> Tuple[str, Tuple[IcebergColumn, ...]]:
        if snapshot.table_format == TABLE_FORMAT_MISSING:
            return PUBLICATION_MISSING_CTAS, ()
        self._require_managed_version(
            snapshot, expected.name, iceberg_version
        )
        self._reject_text_variant_mismatch(expected, snapshot.spec)
        compatibility, additions = self._compatibility(expected, snapshot.spec)
        if compatibility == 'exact':
            return PUBLICATION_INSERT_OVERWRITE, ()
        if compatibility == 'additive':
            return PUBLICATION_ADDITIVE_OVERWRITE, additions
        return PUBLICATION_REPLACEMENT_CTAS, ()

    @staticmethod
    def _reject_text_variant_mismatch(
        expected: IcebergTableSpec,
        actual: Optional[IcebergTableSpec],
    ) -> None:
        if actual is None:
            return
        expected_columns = {column.name: column for column in expected.columns}
        for actual_column in actual.columns:
            expected_column = expected_columns.get(actual_column.name)
            if expected_column is None:
                continue
            data_types = {expected_column.data_type, actual_column.data_type}
            if 'VARIANT' in data_types and any(
                data_type.startswith('VARCHAR') for data_type in data_types
            ):
                raise TableCompatibilityError(
                    f'Iceberg column {actual_column.name} requires an explicit TEXT/VARIANT migration'
                )

    @staticmethod
    def _compatibility(
        expected: IcebergTableSpec, actual: Optional[IcebergTableSpec]
    ) -> Tuple[str, Tuple[IcebergColumn, ...]]:
        if actual is None or expected.primary_key != actual.primary_key:
            return 'incompatible', ()
        expected_columns = {column.name: column for column in expected.columns}
        actual_columns = {column.name: column for column in actual.columns}
        for name, actual_column in actual_columns.items():
            if expected_columns.get(name) != actual_column:
                return 'incompatible', ()
        additions = tuple(
            column for column in expected.columns
            if column.name not in actual_columns
        )
        if any(not column.nullable for column in additions):
            return 'incompatible', ()
        return ('additive', additions) if additions else ('exact', ())

    def _preflight_replacement(
        self,
        target: SnowflakeObjectName,
        destination_spec: IcebergTableSpec,
    ) -> SnowflakeTableMetadata:
        self._assert_replacement_dependencies_absent(target)
        column_rows = self._replacement_columns(target)
        table_row = self._discover_table_row(target)
        if table_row is None:
            raise TableCompatibilityError(
                f'Cannot replace missing Iceberg table {target.quoted}'
            )
        owner = self._replacement_owner(target, table_row)
        explicit_grants = self._replacement_grants(target)
        table_tags = self._replacement_table_tags(target)
        table_comment = _row_value(table_row, 'comment', '') or None
        comments_by_column = {
            str(_row_value(row, 'column_name')): str(_row_value(row, 'comment'))
            for row in column_rows
            if _row_value(row, 'comment', '') not in (None, '')
        }
        column_comments = tuple(
            (column.name, comments_by_column[column.name])
            for column in destination_spec.columns
            if column.name in comments_by_column
        )
        return SnowflakeTableMetadata(
            table_comment=table_comment,
            column_comments=column_comments,
            owner=owner,
            explicit_grants=explicit_grants,
            table_tags=table_tags,
        )

    def _assert_replacement_dependencies_absent(self, target: SnowflakeObjectName) -> None:
        policy_rows = self.snowflake.query(
            f'SELECT * FROM TABLE({quote_identifier(target.database)}.INFORMATION_SCHEMA.POLICY_REFERENCES('
            'REF_ENTITY_NAME => %(target)s, REF_ENTITY_DOMAIN => \'TABLE\'))',
            {'target': target.quoted},
        )
        if policy_rows:
            raise TableCompatibilityError(
                f'Cannot replace {target.quoted} while masking or row-access policies are attached'
            )
        column_tag_rows = self.snowflake.query(
            f'SELECT * FROM TABLE({quote_identifier(target.database)}.INFORMATION_SCHEMA.'
            'TAG_REFERENCES_ALL_COLUMNS(%(target)s, \'TABLE\'))',
            {'target': target.quoted},
        )
        direct_column_tags = [
            row for row in column_tag_rows
            if str(_row_value(row, 'level', '')).upper() == 'COLUMN'
            and str(_row_value(row, 'apply_method', '')).upper() != 'INHERITED'
        ]
        if direct_column_tags:
            raise TableCompatibilityError(
                f'Cannot replace {target.quoted} while column tags are attached'
            )
        stream_rows = self.snowflake.query('SHOW STREAMS IN ACCOUNT')
        dependent_streams = [
            row for row in stream_rows
            if self._stream_references_target(row, target)
        ]
        if dependent_streams:
            raise TableCompatibilityError(
                f'Cannot replace {target.quoted} while dependent streams exist'
            )
        constraint_rows = self.snowflake.query(
            "SELECT 'DIRECT' AS DEPENDENCY_KIND, CONSTRAINT_TYPE, "
            'CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME '
            f'FROM {quote_identifier(target.database)}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS '
            'WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s '
            "AND CONSTRAINT_TYPE <> 'PRIMARY KEY' "
            'UNION ALL '
            "SELECT 'INBOUND_FOREIGN_KEY', foreign_key.CONSTRAINT_TYPE, "
            'foreign_key.CONSTRAINT_NAME, foreign_key.TABLE_SCHEMA, foreign_key.TABLE_NAME '
            f'FROM {quote_identifier(target.database)}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS '
            'AS reference '
            f'JOIN {quote_identifier(target.database)}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS '
            'AS referenced ON referenced.CONSTRAINT_CATALOG = reference.UNIQUE_CONSTRAINT_CATALOG '
            'AND referenced.CONSTRAINT_SCHEMA = reference.UNIQUE_CONSTRAINT_SCHEMA '
            'AND referenced.CONSTRAINT_NAME = reference.UNIQUE_CONSTRAINT_NAME '
            f'JOIN {quote_identifier(target.database)}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS '
            'AS foreign_key ON foreign_key.CONSTRAINT_CATALOG = reference.CONSTRAINT_CATALOG '
            'AND foreign_key.CONSTRAINT_SCHEMA = reference.CONSTRAINT_SCHEMA '
            'AND foreign_key.CONSTRAINT_NAME = reference.CONSTRAINT_NAME '
            'WHERE referenced.TABLE_SCHEMA = %(schema)s '
            'AND referenced.TABLE_NAME = %(table)s',
            {'schema': target.schema, 'table': target.table},
        )
        exported_key_rows = self.snowflake.query(
            f'SHOW EXPORTED KEYS IN TABLE {target.quoted}'
        )
        if constraint_rows or exported_key_rows:
            raise TableCompatibilityError(
                f'Cannot replace {target.quoted} while secondary constraints '
                'or inbound foreign keys exist'
            )

    @staticmethod
    def _stream_references_target(row, target: SnowflakeObjectName) -> bool:
        source_type = str(_row_value(row, 'source_type', '')).upper()
        if source_type not in ('TABLE', 'VIEW'):
            return False
        object_lists = [_row_value(row, 'table_name', '')]
        if source_type == 'VIEW':
            object_lists.append(_row_value(row, 'base_tables', ''))
        try:
            referenced_objects = {
                object_name
                for object_list in object_lists
                for object_name in SnowflakeObjectName.parse_list(object_list)
            }
        except ValueError as exc:
            raise TableCompatibilityError(
                'Cannot prove Snowflake stream dependencies because SHOW STREAMS '
                'returned invalid source object metadata'
            ) from exc
        return target in referenced_objects

    def _replacement_columns(self, target: SnowflakeObjectName):
        rows = self.snowflake.query(
            'SELECT COLUMN_NAME, COMMENT, COLUMN_DEFAULT, IS_IDENTITY '
            f'FROM {quote_identifier(target.database)}.INFORMATION_SCHEMA.COLUMNS '
            'WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s '
            'ORDER BY ORDINAL_POSITION',
            {'schema': target.schema, 'table': target.table},
        )
        unsupported_columns = [
            row for row in rows
            if _row_value(row, 'column_default', False) not in (None, False)
            or _snowflake_boolean(_row_value(row, 'is_identity', False), 'is_identity')
        ]
        if unsupported_columns:
            raise TableCompatibilityError(
                f'Cannot replace {target.quoted} while defaults or identity columns are present'
            )
        return rows

    def _replacement_owner(
        self, target: SnowflakeObjectName, table_row: Dict[str, Any]
    ) -> str:
        clustering_key = _row_value(table_row, 'cluster_by', '') or ''
        if str(clustering_key).strip():
            raise TableCompatibilityError(
                f'Cannot replace {target.quoted} while a clustering key is configured'
            )
        owner = _row_value(table_row, 'owner', '')
        owner_role_type = str(
            _row_value(table_row, 'owner_role_type', '')
        ).upper()
        role_rows = self.snowflake.query('SELECT CURRENT_ROLE() AS CURRENT_ROLE')
        current_role = _row_value(role_rows[0], 'current_role', '') if len(role_rows) == 1 else ''
        if (
            not all(isinstance(value, str) and value for value in (owner, current_role))
            or owner_role_type != 'ROLE'
        ):
            raise TableCompatibilityError(
                f'Cannot prove account-role ownership and the current role for {target.quoted}'
            )
        if owner != current_role:
            raise TableCompatibilityError(
                f'Cannot replace {target.quoted} as role {current_role}; '
                f'the owning role {owner} must execute the replacement'
            )
        return owner

    def _replacement_grants(
        self, target: SnowflakeObjectName
    ) -> Tuple[Tuple[str, str, str, bool], ...]:
        rows = self.snowflake.query(f'SHOW GRANTS ON TABLE {target.quoted}')
        grants = []
        for row in rows:
            privilege = str(_row_value(row, 'privilege')).upper()
            if privilege == 'OWNERSHIP':
                continue
            granted_to = str(_row_value(row, 'granted_to')).upper()
            grantee_name = str(_row_value(row, 'grantee_name'))
            grant_option = _snowflake_boolean(
                _row_value(row, 'grant_option', False),
                'grant_option',
            )
            grants.append((privilege, granted_to, grantee_name, grant_option))
        return tuple(sorted(grants))

    def _replacement_table_tags(
        self, target: SnowflakeObjectName
    ) -> Tuple[Tuple[str, str, str, str], ...]:
        rows = self.snowflake.query(
            'SELECT TAG_DATABASE, TAG_SCHEMA, TAG_NAME, TAG_VALUE, APPLY_METHOD, LEVEL '
            f'FROM TABLE({quote_identifier(target.database)}.INFORMATION_SCHEMA.'
            'TAG_REFERENCES(%(target)s, \'TABLE\'))',
            {'target': target.quoted},
        )
        tags = [
            (
                str(_row_value(row, 'tag_database')),
                str(_row_value(row, 'tag_schema')),
                str(_row_value(row, 'tag_name')),
                str(_row_value(row, 'tag_value')),
            )
            for row in rows
            if str(_row_value(row, 'level', '')).upper() == 'TABLE'
            and str(_row_value(row, 'apply_method', '')).upper() != 'INHERITED'
        ]
        return tuple(sorted(tags))

    def _verify_replacement_metadata(self, attempt: IcebergPublicationAttempt) -> None:
        expected = SnowflakeTableMetadata.from_dict(
            attempt.manifest_payload.replacement_metadata or {}
        )
        if self.publisher._preflight_replacement(  # pylint: disable=protected-access
            attempt.target,
            attempt.table_spec,
        ) != expected:
            raise RecoveryManifestError(
                'Iceberg target metadata changed after the source boundary was captured'
            )

    def restore_metadata(self, attempt: IcebergPublicationAttempt) -> None:
        """Restore comments lost by a guarded replacement CTAS."""
        metadata_value = attempt.manifest_payload.replacement_metadata
        if not metadata_value:
            return
        metadata = SnowflakeTableMetadata.from_dict(metadata_value)
        query_tag = {**attempt.query_tag, 'phase': 'restore_metadata'}
        for column_name, comment in metadata.column_comments:
            self.snowflake.query(
                f'ALTER ICEBERG TABLE {attempt.target.quoted} '
                f'ALTER COLUMN {quote_identifier(column_name)} COMMENT '
                f'{sql_string_literal(comment)}',
                query_tag_props=query_tag,
            )

    def publish_full_sync(
        self, attempt: IcebergPublicationAttempt, spec: IcebergTableSpec
    ) -> PublicationPlan:
        """Publish a staged FullSync and durably record its completion."""
        return self._publish(attempt, spec, self.plan_full_sync(attempt, spec), transactional=False)

    def publish_partial_sync(
        self, attempt: IcebergPublicationAttempt, spec: IcebergTableSpec
    ) -> PublicationPlan:
        """Publish a staged PartialSync and durably record its completion."""
        if attempt.phase in (PHASE_STAGED, PHASE_SUBMITTED):
            try:
                self._validate_partial_staging_primary_key(
                    attempt,
                    spec,
                    query_phase='pre_publication_key_validation',
                )
            except StagingPrimaryKeyError as exc:
                if attempt.phase == PHASE_SUBMITTED:
                    raise StagingPrimaryKeyError(
                        f'{exc}; the existing SUBMITTED transaction may already have committed. '
                        'Preserve its staging table and recovery manifest; manual recovery is required'
                    ) from exc
                self._transition(
                    attempt,
                    PHASE_STAGING_CREATED,
                    expected_row_count=None,
                    expected_row_fingerprint=None,
                )
                raise
        if (
            attempt.phase == PHASE_SUBMITTED
            and attempt.method == PUBLICATION_PARTIAL_MERGE
        ):
            self.publisher._rearm_partial_merge_replay(  # pylint: disable=protected-access
                attempt
            )
        return self._publish(attempt, spec, self.plan_partial_sync(attempt, spec), transactional=True)

    def _publish(self, attempt, spec, plan, transactional):
        if attempt.phase not in (PHASE_STAGED, PHASE_SUBMITTED):
            raise RecoveryManifestError(f'Cannot publish Iceberg attempt in phase {attempt.phase}')
        attempt.require_staging_evidence()
        if attempt.table_spec.fingerprint != spec.fingerprint:
            raise RecoveryManifestError('Iceberg publication schema changed after staging')

        snapshot = self.inspect_table(spec.name)
        if snapshot.fingerprint != plan.target_fingerprint:
            raise RecoveryManifestError('Iceberg target changed after publication planning')

        for statement in plan.preparation_statements:
            self.snowflake.query(statement, query_tag_props={**attempt.query_tag, 'phase': 'schema_evolution'})
        if plan.preparation_statements:
            evolved = self.inspect_table(spec.name)
            compatibility, additions = self._compatibility(spec, evolved.spec)
            if compatibility != 'exact' or additions:
                raise RecoveryManifestError('Iceberg schema evolution did not produce the staged schema')
            attempt.update_manifest_payload({
                'schema_evolution_applied': True,
            })
            self._save_active_attempt(attempt)

        attempt.method = plan.method
        publication_evidence = {}
        if len(plan.publication_statements) == 1:
            publication_evidence.update({
                'publication_query_hash': _sql_hash(
                    plan.publication_statements[0]
                ),
                'publication_query_type': _publication_query_type(plan.method),
            })
        publication_evidence[_PUBLICATION_SUBMITTED_AT] = time.time()
        attempt.update_manifest_payload(publication_evidence)
        self._transition(attempt, PHASE_SUBMITTED)
        if transactional and len(plan.publication_statements) > 1:
            self.snowflake.execute_transaction(
                plan.publication_statements,
                query_tag_props=attempt.query_tag,
            )
        else:
            for statement in plan.publication_statements:
                self.snowflake.query(statement, query_tag_props=attempt.query_tag)

        self.publisher._verify_published(  # pylint: disable=protected-access
            attempt,
            spec,
        )
        self._transition(attempt, PHASE_PUBLISHED)
        return plan

    def _verify_published(self, attempt: IcebergPublicationAttempt, spec: IcebergTableSpec) -> None:
        attempt.require_staging_evidence()
        try:
            snapshot = self.inspect_table(spec.name)
        except (TableFormatDiscoveryError, TableCompatibilityError) as exc:
            raise RecoveryManifestError(
                f'Published Iceberg target does not satisfy the managed-v3 '
                f'table contract: {exc}'
            ) from exc
        expected_format = managed_iceberg_version_spec(
            attempt.iceberg_version
        ).table_format
        if snapshot.table_format != expected_format or snapshot.spec is None:
            raise RecoveryManifestError(
                'Published target is not the requested managed Iceberg version'
            )
        compatibility, additions = self._compatibility(spec, snapshot.spec)
        if compatibility != 'exact' or additions:
            raise RecoveryManifestError('Published Iceberg target schema does not match the staged schema')
        where_clause = (
            attempt.manifest_payload.where_clause_sql
            if attempt.method == PUBLICATION_PARTIAL_MERGE
            else ''
        )
        target_evidence = self._content_evidence(
            spec,
            spec.name,
            project=False,
            where_clause=where_clause,
            query_tag={**attempt.query_tag, 'phase': 'publication_validation'},
        )
        expected_evidence = (
            attempt.expected_row_count,
            attempt.expected_row_fingerprint,
        )
        if target_evidence != expected_evidence:
            raise RecoveryManifestError(
                content_evidence_mismatch(
                    'Published Iceberg target contents do not match staging',
                    expected_evidence,
                    target_evidence,
                )
            )
        if attempt.method in (
            PUBLICATION_REPLACEMENT_CTAS,
            PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
        ):
            self.publisher._verify_replacement_metadata(  # pylint: disable=protected-access
                attempt
            )

    def _content_evidence(
        self,
        spec: IcebergTableSpec,
        table: SnowflakeObjectName,
        project: bool,
        where_clause: str = '',
        query_tag: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, str]:
        select_list = spec.projection if project else spec.quoted_columns
        rows = self.snowflake.query(
            'SELECT COUNT(*) AS ROW_COUNT, HASH_AGG(*) AS ROW_FINGERPRINT '
            f'FROM (SELECT {select_list} FROM {table.quoted}{where_clause})',
            query_tag_props=query_tag,
        )
        if len(rows) != 1:
            raise RecoveryManifestError(
                f'Snowflake did not return Iceberg content evidence for {table.quoted}'
            )
        row_count = _row_value(rows[0], 'row_count')
        row_fingerprint = _row_value(rows[0], 'row_fingerprint')
        if row_count is None or row_fingerprint is None:
            raise RecoveryManifestError(
                f'Snowflake returned incomplete Iceberg content evidence for {table.quoted}'
            )
        return int(row_count), str(row_fingerprint)

    def _validate_partial_staging_primary_key(self, attempt, spec, query_phase):
        validate_partial_staging_primary_key(
            self.snowflake, attempt, spec, query_phase
        )

    def reconcile(
        self, attempt: IcebergPublicationAttempt, spec: Optional[IcebergTableSpec] = None
    ) -> RecoveryOutcome:
        """Return the only safe next action for a durable attempt."""
        spec = spec or attempt.table_spec
        if attempt.phase in (PHASE_PREPARED, PHASE_UPLOADED, PHASE_STAGING_CREATED):
            return RecoveryOutcome(RECOVERY_RESTART_STAGING, attempt)
        if attempt.phase == PHASE_STAGED:
            return RecoveryOutcome(RECOVERY_PUBLISH, attempt)
        if attempt.phase == PHASE_SUBMITTED:
            if attempt.method == PUBLICATION_PARTIAL_MERGE:
                return RecoveryOutcome(RECOVERY_PUBLISH, attempt)
            self._reconcile_query_history(attempt, spec)
            action = RECOVERY_FINALIZE if attempt.phase == PHASE_PUBLISHED else RECOVERY_PUBLISH
            return RecoveryOutcome(action, attempt)
        if attempt.phase == PHASE_PUBLISHED:
            return RecoveryOutcome(RECOVERY_FINALIZE, attempt)
        if attempt.phase == PHASE_FINALIZED:
            validate_required_finalization_actions(attempt)
            return RecoveryOutcome(RECOVERY_STATE_HANDOFF, attempt)
        raise RecoveryManifestError(f'Cannot reconcile Iceberg phase {attempt.phase}')

    def _reconcile_query_history(
        self, attempt: IcebergPublicationAttempt, spec: IcebergTableSpec
    ) -> None:
        tag_creator = getattr(self.snowflake, 'create_query_tag', None)
        tag = (
            tag_creator(attempt.query_tag)
            if callable(tag_creator)
            else json.dumps(attempt.query_tag, sort_keys=True, separators=(',', ':'))
        )
        submitted_at = self._publication_submission_time(attempt)
        poll_result = self._poll_query_history(
            attempt,
            tag,
            submitted_at,
        )
        terminal = [
            row for row in poll_result.rows
            if str(_row_value(row, 'execution_status')).lower() in _TERMINAL_HISTORY_STATUSES
        ]
        successful = [
            row for row in terminal
            if str(_row_value(row, 'execution_status')).lower() == 'success'
        ]
        if not terminal:
            raise QueryHistoryVisibilityTimeoutError(
                attempt.attempt_id,
                poll_result.elapsed_seconds,
                poll_result.poll_count,
                poll_result.statuses,
            )
        if len(terminal) != 1:
            raise AmbiguousPublicationError(
                f'Expected one terminal Snowflake publication for attempt {attempt.attempt_id}; '
                f'found {len(terminal)}'
            )
        history_row = terminal[0]
        expected_hash = attempt.manifest_payload.publication_query_hash
        expected_type = attempt.manifest_payload.publication_query_type
        if not expected_hash or not expected_type:
            raise RecoveryManifestError('Submitted Iceberg attempt lacks publication query evidence')
        if _sql_hash(str(_row_value(history_row, 'query_text'))) != expected_hash:
            raise AmbiguousPublicationError('Snowflake query history text does not match the submitted publication')
        query_type = str(_row_value(history_row, 'query_type')).upper()
        if successful and query_type != expected_type:
            raise AmbiguousPublicationError('Snowflake query history type does not match the submitted publication')
        if not successful:
            if query_type not in (expected_type, 'UNKNOWN'):
                raise AmbiguousPublicationError(
                    'Snowflake query history type does not match the submitted publication'
                )
            attempt.attempt_id = uuid4().hex
            attempt.update_manifest_payload(remove=(
                'publication_query_hash',
                'publication_query_type',
                _PUBLICATION_SUBMITTED_AT,
            ))
            self._transition(attempt, PHASE_STAGED, query_id=None)
            return
        attempt.query_id = str(_row_value(successful[0], 'query_id'))
        self.publisher._verify_published(  # pylint: disable=protected-access
            attempt,
            spec,
        )
        self._transition(attempt, PHASE_PUBLISHED)

    @staticmethod
    def _publication_submission_time(attempt):
        submitted_at = attempt.manifest_payload.publication_submitted_at
        if (
            isinstance(submitted_at, bool)
            or not isinstance(submitted_at, (int, float))
            or not math.isfinite(submitted_at)
            or submitted_at <= 0
        ):
            raise RecoveryManifestError(
                'Submitted Iceberg attempt lacks a valid publication submission time'
            )
        return float(submitted_at)

    def _poll_query_history(self, attempt, tag, submitted_at):
        policy = self.history_policy
        started = time.monotonic()
        deadline = started + policy.timeout_seconds
        rows = []
        statuses = ()
        poll_count = 0
        while policy.max_attempts is None or poll_count < policy.max_attempts:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            poll_count += 1
            try:
                rows = self._query_history_rows(
                    tag,
                    submitted_at,
                    min(policy.lookup_timeout_seconds, remaining),
                )
            except Exception as exc:
                elapsed_seconds = max(0.0, time.monotonic() - started)
                raise QueryHistoryLookupError(
                    attempt.attempt_id,
                    elapsed_seconds,
                    poll_count,
                ) from exc
            statuses = tuple(
                str(_row_value(row, 'execution_status')).lower()
                for row in rows
            )
            if any(status in _TERMINAL_HISTORY_STATUSES for status in statuses):
                break
            if policy.max_attempts is not None and poll_count >= policy.max_attempts:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(policy.poll_interval_seconds, remaining))
        return _QueryHistoryPollResult(
            tuple(rows),
            poll_count,
            max(0.0, time.monotonic() - started),
            statuses,
        )

    def _query_history_rows(self, tag, submitted_at, timeout_seconds):
        params = {'query_tag': tag, 'submitted_at': submitted_at}
        bounded_query = getattr(self.history_snowflake, 'query_with_timeout', None)
        if callable(bounded_query):
            return bounded_query(_QUERY_HISTORY_SQL, params, timeout_seconds)
        return self.history_snowflake.query(_QUERY_HISTORY_SQL, params)

    @staticmethod
    def _ctas_sql(
        spec: IcebergTableSpec,
        staging_table: str,
        replace_table: bool = False,
        metadata: Optional[SnowflakeTableMetadata] = None,
        *,
        iceberg_version: int,
    ) -> str:
        try:
            version_spec = managed_iceberg_version_spec(iceberg_version)
        except ValueError as exc:
            raise RecoveryManifestError(
                'Iceberg CTAS requires a supported exact integer version'
            ) from exc
        replace_sql = ' OR REPLACE' if replace_table else ''
        copy_metadata = ' COPY GRANTS COPY TAGS' if replace_table else ''
        column_comments = dict(metadata.column_comments) if metadata else {}
        definitions = ', '.join(
            column.definition + (
                f' COMMENT {sql_string_literal(column_comments[column.name])}'
                if column.name in column_comments else ''
            )
            for column in spec.columns
        )
        table_comment = (
            f' COMMENT = {sql_string_literal(metadata.table_comment)}'
            if metadata and metadata.table_comment is not None else ''
        )
        source = spec.name.with_table(staging_table).quoted
        table_options = version_spec.table_options_sql
        return (
            f'CREATE{replace_sql} ICEBERG TABLE {spec.name.quoted} '
            f'({definitions}{spec.primary_key_clause}) '
            f"CATALOG = 'SNOWFLAKE'{copy_metadata} "
            f'ICEBERG_VERSION = {iceberg_version} '
            f'{table_options}{table_comment} '
            f'AS SELECT {spec.projection} FROM {source}'
        )

    @staticmethod
    def _insert_overwrite_sql(spec: IcebergTableSpec, staging_table: str) -> str:
        source = spec.name.with_table(staging_table).quoted
        return (
            f'INSERT OVERWRITE INTO {spec.name.quoted} ({spec.quoted_columns}) '
            f'SELECT {spec.projection} FROM {source}'
        )

    @staticmethod
    def _partial_merge_sql(
        spec: IcebergTableSpec, attempt: IcebergPublicationAttempt
    ) -> Tuple[str, ...]:
        if not spec.primary_key:
            raise TableCompatibilityError('Iceberg PartialSync requires a primary key')
        target = spec.name.quoted
        source_name = spec.name.with_table(attempt.staging_table)
        source = source_name.quoted
        source_alias = 'SOURCE'
        target_alias = 'TARGET'
        where_clause = attempt.manifest_payload.where_clause_sql
        join = ' AND '.join(
            f'{quote_identifier(source_alias)}.{quote_identifier(key)} = '
            f'{quote_identifier(target_alias)}.{quote_identifier(key)}'
            for key in spec.primary_key
        )
        updates = ', '.join(
            f'{quote_identifier(target_alias)}.{column.quoted_name} = '
            f'{quote_identifier(source_alias)}.{column.quoted_name}'
            for column in spec.columns
        )
        values = ', '.join(
            f'{quote_identifier(source_alias)}.{column.quoted_name}' for column in spec.columns
        )
        return (
            f'UPDATE {target} SET {quote_identifier(utils.SDC_DELETED_AT)} = CURRENT_TIMESTAMP()'
            f'{where_clause} AND {quote_identifier(utils.SDC_DELETED_AT)} IS NULL',
            f'MERGE INTO {target} AS {quote_identifier(target_alias)} '
            f'USING {source} AS {quote_identifier(source_alias)} ON {join} '
            f'WHEN MATCHED THEN UPDATE SET {updates} '
            f'WHEN NOT MATCHED THEN INSERT ({spec.quoted_columns}) VALUES ({values})',
            f'DELETE FROM {target}{where_clause} '
            f'AND {quote_identifier(utils.SDC_DELETED_AT)} IS NOT NULL',
        )


# Compatibility alias for integrations that imported the pre-composition name.
SnowflakeIcebergPublicationMixin = SnowflakeIcebergPublicationService
