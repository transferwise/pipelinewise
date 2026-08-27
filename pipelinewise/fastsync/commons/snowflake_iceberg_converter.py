"""Manual conversion of native Snowflake tables to managed Iceberg v3."""

import logging

from typing import Any, Dict, List, Optional, Tuple

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    PHASE_FINALIZED,
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_STAGING_CREATED,
    PHASE_SUBMITTED,
    IcebergPublicationAttempt,
    RecoveryManifestError,
    quote_identifier,
    sql_string_literal,
    validate_recovery_identity,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_coordination import (
    RecoveryCoordinator,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_inspection import (
    exact_named_table_rows,
    physical_table_format,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_conversion_evidence import (
    SnowflakeConversionEvidenceService,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_conversion_recovery import (
    SnowflakeConversionFinalizationValidator,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_validation import (
    CONVERSION_CUTOVER_OUTAGE_WARNING,
    ConversionMetadata,
    ConversionTableState,
    NativeColumn,
    NativeToIcebergConversionError,
    SnowflakeTableName,
    assert_iceberg_table_spec,
    assert_managed_v3,
    assert_supported_metadata,
    grantee_sql as _grantee_sql,
    manual_recovery_identity as _manual_recovery_identity,
    parse_native_columns,
    snowflake_boolean as _snowflake_boolean,
    stream_references_table,
    _value as _row_value,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    MANAGED_ICEBERG_V3_SPEC,
    is_exact_integer,
    managed_iceberg_version_spec,
)


ICEBERG_V3 = MANAGED_ICEBERG_V3_SPEC.version
MAX_SNOWFLAKE_IDENTIFIER_LENGTH = 255
EVENTUAL_NATIVE = 'native'
EVENTUAL_ICEBERG = 'iceberg'

TABLE_FORMAT_MISSING = 'missing'
TABLE_FORMAT_NATIVE = 'native'
TABLE_FORMAT_ICEBERG = 'iceberg'
ROLLBACK_REQUIRED = 'rollback_required'

LOGGER = logging.getLogger(__name__)


def _new_conversion_attempt(
    table: SnowflakeTableName,
    eventual: str,
    metadata: ConversionMetadata,
    recovery_identity: Dict[str, Any],
) -> IcebergPublicationAttempt:
    """Create a manual attempt in the shared credential-free manifest format."""
    return IcebergPublicationAttempt.new(
        kind='manual_conversion',
        table_spec=metadata.table_spec(table),
        source_bookmark={},
        staging_table=table.with_suffix('_ICEBERG').table,
        method='manual_conversion',
        pre_publication_target_fingerprint=metadata.fingerprint,
        recovery_identity=recovery_identity,
        target_table_format=TABLE_FORMAT_ICEBERG,
        iceberg_version=ICEBERG_V3,
        intended_state={'target_table_format': eventual},
        context={
            'eventual': eventual,
            'backup_table': table.with_suffix('_NATIVE').table,
            'source_schema_fingerprint': metadata.fingerprint,
        },
    )


class SnowflakeNativeToIcebergConverter:  # pylint: disable=too-few-public-methods
    """Safely build and optionally promote a managed Iceberg v3 copy."""

    def __init__(self, snowflake, runtime_dir: str):
        self.snowflake = snowflake
        self.runtime_dir = runtime_dir
        self.recovery_coordinator = RecoveryCoordinator(runtime_dir)
        self.evidence_service = SnowflakeConversionEvidenceService(self)
        self._query_tag_context = {}
        self.finalization_validator = SnowflakeConversionFinalizationValidator(
            self._query,
            self._inspect_source,
            self._assert_equal_contents,
            self._manual_recovery_error,
        )

    def _assert_equal_contents(self, source, destination, columns):
        return self.evidence_service._assert_equal_contents(  # pylint: disable=protected-access
            source,
            destination,
            columns,
        )

    def convert(
        self,
        fqtn: str,
        eventual: str = EVENTUAL_NATIVE,
        iceberg_version: int = ICEBERG_V3,
    ) -> None:
        """Convert one target table while retaining a native rollback point."""
        if eventual not in (EVENTUAL_NATIVE, EVENTUAL_ICEBERG):
            raise ValueError('eventual must be native or iceberg')
        if not is_exact_integer(iceberg_version) or iceberg_version != ICEBERG_V3:
            raise ValueError('Only managed Iceberg version 3 conversion is supported')

        table = SnowflakeTableName.parse(fqtn)
        self._validate_companion_names(table)
        self._validate_database(table)
        recovery_identity = _manual_recovery_identity(
            self.snowflake.connection_config
        )
        store = self.recovery_coordinator.recovery_store(table)
        LOGGER.warning(
            'All writers to %s must remain stopped until conversion validation completes',
            table.quoted,
        )
        if eventual == EVENTUAL_ICEBERG:
            LOGGER.warning(CONVERSION_CUTOVER_OUTAGE_WARNING, table.quoted)
        with self.recovery_coordinator.table_lock(table):
            if store.load_fastsync_target_pointer() is not None:
                raise NativeToIcebergConversionError(
                    f'A FastSync Iceberg attempt is pending recovery for {table.quoted}'
                )
            manifest = store.load()
            if manifest is not None:
                self._validate_manifest(
                    manifest,
                    table,
                    eventual,
                    recovery_identity,
                )
            else:
                manifest = self._recover_completed_without_manifest(
                    table,
                    eventual,
                    recovery_identity,
                )
                if manifest is None:
                    metadata = self._inspect_source(table)
                    manifest = _new_conversion_attempt(
                        table,
                        eventual,
                        metadata,
                        recovery_identity,
                    )
                    self.recovery_coordinator.save_conversion_attempt(manifest)

            self._query_tag_context = {
                'load_id': manifest.load_id,
                'attempt_id': manifest.attempt_id,
                'schema': table.schema,
                'table': table.table,
            }
            self._run_attempt(table, manifest)

    def _query(self, sql: str, params=None, phase: str = None):
        query_tag = {
            'phase': phase,
            'publication_method': 'manual_conversion',
        }
        query_tag.update(self._query_tag_context)
        return self.snowflake.query(
            sql,
            params=params,
            query_tag_props=query_tag,
        )

    def _validate_database(self, table: SnowflakeTableName) -> None:
        configured_database = self.snowflake.connection_config.get('dbname')
        if not isinstance(configured_database, str) or not configured_database:
            raise NativeToIcebergConversionError('The target configuration does not define a Snowflake database')
        if configured_database.startswith('"') and configured_database.endswith('"'):
            configured_identifier = configured_database[1:-1].replace('""', '"')
        else:
            configured_identifier = configured_database.upper()
        if table.database != configured_identifier:
            raise NativeToIcebergConversionError(
                f'{table.quoted} is outside the configured target database '
                f'{configured_database!r}'
            )

    @staticmethod
    def _validate_companion_names(table: SnowflakeTableName) -> None:
        for suffix in ('_ICEBERG', '_NATIVE'):
            if len(table.with_suffix(suffix).table) > MAX_SNOWFLAKE_IDENTIFIER_LENGTH:
                raise NativeToIcebergConversionError(
                    f'{table.quoted} is too long to create the reserved {suffix} companion'
                )

    @staticmethod
    def _validate_manifest(
        manifest: IcebergPublicationAttempt,
        table: SnowflakeTableName,
        eventual: str,
        recovery_identity: Dict[str, Any],
    ) -> None:
        try:
            validate_recovery_identity(manifest.recovery_identity)
        except RecoveryManifestError as exc:
            raise NativeToIcebergConversionError(
                'Conversion manifest recovery identity is invalid'
            ) from exc
        if manifest.recovery_identity != recovery_identity:
            raise NativeToIcebergConversionError(
                'Conversion manifest belongs to a different Snowflake target identity'
            )
        if manifest.kind != 'manual_conversion' or manifest.target.key != table.key:
            raise NativeToIcebergConversionError(
                'Conversion manifest table does not match the requested table'
            )
        payload = manifest.manifest_payload
        manifest_eventual = payload.eventual
        if manifest_eventual != eventual:
            raise NativeToIcebergConversionError(
                'Conversion attempt is already in progress with '
                f'eventual={manifest_eventual}'
            )
        if (
            manifest.target_table_format != TABLE_FORMAT_ICEBERG
            or not is_exact_integer(manifest.iceberg_version)
            or manifest.iceberg_version != ICEBERG_V3
        ):
            raise NativeToIcebergConversionError(
                'Conversion manifest requests an unsupported Iceberg version'
            )
        schema_fingerprint = payload.source_schema_fingerprint
        identifiers_are_valid = all(
            isinstance(identifier, str) and identifier
            for identifier in (manifest.load_id, manifest.attempt_id)
        )
        companions_are_valid = (
            manifest.staging_table == table.with_suffix('_ICEBERG').table
            and payload.backup_table == table.with_suffix('_NATIVE').table
        )
        fingerprint_is_valid = (
            isinstance(schema_fingerprint, str)
            and len(schema_fingerprint) == 64
            and not set(schema_fingerprint).difference('0123456789abcdef')
            and manifest.pre_publication_target_fingerprint == schema_fingerprint
        )
        if not all((
            identifiers_are_valid,
            companions_are_valid,
            fingerprint_is_valid,
            manifest.method == 'manual_conversion',
        )):
            raise NativeToIcebergConversionError(
                'Conversion manifest identity or schema fingerprint is invalid'
            )

    def _recover_completed_without_manifest(
        self,
        table: SnowflakeTableName,
        eventual: str,
        recovery_identity: Dict[str, Any],
    ) -> Optional[IcebergPublicationAttempt]:
        state = self._table_state(table)
        if state == ConversionTableState(
            TABLE_FORMAT_NATIVE,
            TABLE_FORMAT_ICEBERG,
            TABLE_FORMAT_MISSING,
        ):
            metadata = self._inspect_source(table)
            assert_managed_v3(self._query, table.with_suffix('_ICEBERG'))
            self._assert_equal_contents(table, table.with_suffix('_ICEBERG'), metadata.columns)
            return _new_conversion_attempt(
                table,
                eventual,
                metadata,
                recovery_identity,
            )

        if state == ConversionTableState(
            TABLE_FORMAT_ICEBERG,
            TABLE_FORMAT_MISSING,
            TABLE_FORMAT_NATIVE,
        ):
            if eventual != EVENTUAL_ICEBERG:
                raise self._manual_recovery_error(table)
            metadata = self._inspect_source(table.with_suffix('_NATIVE'))
            assert_managed_v3(self._query, table)
            self._assert_equal_contents(table.with_suffix('_NATIVE'), table, metadata.columns)
            return _new_conversion_attempt(
                table,
                eventual,
                metadata,
                recovery_identity,
            )

        if state != ConversionTableState(
            TABLE_FORMAT_NATIVE,
            TABLE_FORMAT_MISSING,
            TABLE_FORMAT_MISSING,
        ):
            raise self._manual_recovery_error(table)
        return None

    def _run_attempt(
        self,
        table: SnowflakeTableName,
        manifest: IcebergPublicationAttempt,
    ) -> None:
        state = self._table_state(table)
        payload = manifest.manifest_payload
        if manifest.phase == PHASE_FINALIZED:
            self.finalization_validator.validate_finalized(
                table,
                manifest,
                state,
            )
            self.recovery_coordinator.delete_conversion_attempt(manifest)
            return
        if payload.rollback_required is True:
            state = self._finish_required_rollback(
                table,
                manifest,
                state,
            )
        if state == ConversionTableState(
            TABLE_FORMAT_ICEBERG,
            TABLE_FORMAT_MISSING,
            TABLE_FORMAT_NATIVE,
        ):
            self._finalize_or_restore_native(table, manifest)
            return

        if state == ConversionTableState(
            TABLE_FORMAT_MISSING,
            TABLE_FORMAT_ICEBERG,
            TABLE_FORMAT_NATIVE,
        ):
            if payload.eventual != EVENTUAL_ICEBERG:
                raise self._manual_recovery_error(table)
            metadata = self._inspect_source(table.with_suffix('_NATIVE'))
            if metadata.fingerprint != payload.source_schema_fingerprint:
                raise NativeToIcebergConversionError(
                    'Native backup schema no longer matches the conversion manifest'
                )
            staging = table.with_suffix('_ICEBERG')
            assert_managed_v3(self._query, staging)
            assert_iceberg_table_spec(self._query, staging, manifest.table_spec)
            assert_supported_metadata(staging, metadata, self._inspect_source(staging))
            self._assert_equal_contents(table.with_suffix('_NATIVE'), staging, metadata.columns)
            self._promote_staging(table, manifest)
            return

        if state == ConversionTableState(
            TABLE_FORMAT_MISSING,
            TABLE_FORMAT_MISSING,
            TABLE_FORMAT_NATIVE,
        ):
            self._restore_native(table)
            state = self._table_state(table)

        if state not in (
            ConversionTableState(
                TABLE_FORMAT_NATIVE,
                TABLE_FORMAT_MISSING,
                TABLE_FORMAT_MISSING,
            ),
            ConversionTableState(
                TABLE_FORMAT_NATIVE,
                TABLE_FORMAT_ICEBERG,
                TABLE_FORMAT_MISSING,
            ),
        ):
            raise self._manual_recovery_error(table)

        metadata = self._inspect_source(table)
        if metadata.fingerprint != payload.source_schema_fingerprint:
            raise NativeToIcebergConversionError(
                f'The schema of {table.quoted} changed after conversion started'
            )

        if state.staging == TABLE_FORMAT_MISSING:
            self._create_staging(table, metadata, manifest)
        else:
            assert_managed_v3(self._query, table.with_suffix('_ICEBERG'))
        staging = table.with_suffix('_ICEBERG')
        assert_iceberg_table_spec(self._query, staging, manifest.table_spec)

        row_count, row_hash = self._assert_equal_contents(
            table,
            staging,
            metadata.columns,
        )
        manifest.expected_row_count = row_count
        manifest.expected_row_fingerprint = str(row_hash)
        self._transition(manifest, PHASE_STAGED)

        self._apply_metadata(staging, metadata)
        assert_supported_metadata(staging, metadata, self._inspect_source(staging))
        self._transition(manifest, PHASE_SUBMITTED)

        if payload.eventual == EVENTUAL_NATIVE:
            self._transition(manifest, PHASE_FINALIZED)
            self.recovery_coordinator.delete_conversion_attempt(manifest)
            return

        self._cut_over(table, manifest)

    def _create_staging(
        self,
        table: SnowflakeTableName,
        metadata: ConversionMetadata,
        manifest: IcebergPublicationAttempt,
    ) -> None:
        sql = self._create_iceberg_ctas(table, metadata, manifest.iceberg_version)
        try:
            self._query(sql, phase='create_staging')
        except Exception:
            state = self._table_state(table)
            if state.staging != TABLE_FORMAT_ICEBERG:
                raise
            LOGGER.warning(
                'Iceberg staging creation committed despite a lost client response'
            )

        assert_managed_v3(self._query, table.with_suffix('_ICEBERG'))
        self._transition(manifest, PHASE_STAGING_CREATED)

    @staticmethod
    def _create_iceberg_ctas(table: SnowflakeTableName, metadata: ConversionMetadata, iceberg_version: int) -> str:
        try:
            version_spec = managed_iceberg_version_spec(iceberg_version)
        except ValueError as exc:
            raise NativeToIcebergConversionError(
                'Conversion manifest requests an unsupported Iceberg version'
            ) from exc
        definitions = [
            column.ddl_definition(
                column.nullable and column.name not in metadata.primary_key
            )
            for column in metadata.columns
        ]
        if metadata.primary_key:
            primary_key = ', '.join(
                quote_identifier(column) for column in metadata.primary_key
            )
            definitions.append(f'PRIMARY KEY ({primary_key})')
        projection = ', '.join(column.projection for column in metadata.columns)
        table_options = version_spec.table_options_sql
        statement = (
            f'CREATE ICEBERG TABLE {table.with_suffix("_ICEBERG").quoted} '
            f'({", ".join(definitions)}) '
            f"CATALOG = 'SNOWFLAKE' ICEBERG_VERSION = {iceberg_version} "
            f'{table_options}'
        )
        if metadata.table_comment is not None:
            statement += f' COMMENT={sql_string_literal(metadata.table_comment)}'
        return f'{statement} AS SELECT {projection} FROM {table.quoted}'

    def _cut_over(
        self,
        table: SnowflakeTableName,
        manifest: IcebergPublicationAttempt,
    ) -> None:
        try:
            self._query(
                f'ALTER TABLE {table.quoted} RENAME TO '
                f'{table.with_suffix("_NATIVE").quoted}',
                phase='rename_native',
            )
        except Exception:
            state = self._table_state(table)
            if state != ConversionTableState(
                TABLE_FORMAT_MISSING,
                TABLE_FORMAT_ICEBERG,
                TABLE_FORMAT_NATIVE,
            ):
                raise
            LOGGER.warning('Native rename committed despite a lost client response')

        self._promote_staging(table, manifest)

    def _promote_staging(
        self,
        table: SnowflakeTableName,
        manifest: IcebergPublicationAttempt,
    ) -> None:
        try:
            self._query(
                f'ALTER ICEBERG TABLE {table.with_suffix("_ICEBERG").quoted} '
                f'RENAME TO {table.quoted}',
                phase='promote_iceberg',
            )
        except Exception as promotion_error:
            state = self._table_state(table)
            if state == ConversionTableState(
                TABLE_FORMAT_ICEBERG,
                TABLE_FORMAT_MISSING,
                TABLE_FORMAT_NATIVE,
            ):
                LOGGER.warning(
                    'Iceberg promotion committed despite a lost client response'
                )
            elif state == ConversionTableState(
                TABLE_FORMAT_MISSING,
                TABLE_FORMAT_ICEBERG,
                TABLE_FORMAT_NATIVE,
            ):
                self._restore_native(table)
                raise promotion_error
            else:
                raise self._manual_recovery_error(table) from promotion_error

        self._transition(manifest, PHASE_PUBLISHED)
        self._finalize_or_restore_native(table, manifest)

    def _finalize_or_restore_native(
        self,
        table: SnowflakeTableName,
        manifest: IcebergPublicationAttempt,
    ) -> None:
        try:
            self.finalization_validator.validate_promoted(table, manifest)
        except Exception:
            manifest.update_manifest_payload({ROLLBACK_REQUIRED: True})
            self.recovery_coordinator.save_conversion_attempt(manifest)
            try:
                self._rollback_promoted(table)
            except Exception as rollback_error:
                raise self._manual_recovery_error(table) from rollback_error
            manifest.update_manifest_payload(remove=(ROLLBACK_REQUIRED,))
            self._transition(manifest, PHASE_SUBMITTED)
            raise
        self._transition(manifest, PHASE_FINALIZED)
        self.recovery_coordinator.delete_conversion_attempt(manifest)

    def _finish_required_rollback(
        self,
        table: SnowflakeTableName,
        manifest: IcebergPublicationAttempt,
        state: ConversionTableState,
    ) -> ConversionTableState:
        promoted = ConversionTableState(
            TABLE_FORMAT_ICEBERG,
            TABLE_FORMAT_MISSING,
            TABLE_FORMAT_NATIVE,
        )
        rollback_started = ConversionTableState(
            TABLE_FORMAT_MISSING,
            TABLE_FORMAT_ICEBERG,
            TABLE_FORMAT_NATIVE,
        )
        restored = ConversionTableState(
            TABLE_FORMAT_NATIVE,
            TABLE_FORMAT_ICEBERG,
            TABLE_FORMAT_MISSING,
        )
        if state == promoted:
            self._rollback_promoted(table)
        elif state == rollback_started:
            self._restore_native(table)
        elif state != restored:
            raise self._manual_recovery_error(table)

        state = self._table_state(table)
        if state != restored:
            raise self._manual_recovery_error(table)
        manifest.update_manifest_payload(remove=(ROLLBACK_REQUIRED,))
        self._transition(manifest, PHASE_SUBMITTED)
        return state

    def _rollback_promoted(self, table: SnowflakeTableName) -> None:
        try:
            self._query(
                f'ALTER ICEBERG TABLE {table.quoted} RENAME TO '
                f'{table.with_suffix("_ICEBERG").quoted}',
                phase='rollback_iceberg',
            )
        except Exception:
            state = self._table_state(table)
            if state != ConversionTableState(
                TABLE_FORMAT_MISSING,
                TABLE_FORMAT_ICEBERG,
                TABLE_FORMAT_NATIVE,
            ):
                raise
            LOGGER.warning(
                'Iceberg rollback rename committed despite a lost client response'
            )
        self._restore_native(table)

    def _restore_native(self, table: SnowflakeTableName) -> None:
        try:
            self._query(
                f'ALTER TABLE {table.with_suffix("_NATIVE").quoted} '
                f'RENAME TO {table.quoted}',
                phase='restore_native',
            )
        except Exception:
            state = self._table_state(table)
            if state != ConversionTableState(
                TABLE_FORMAT_NATIVE,
                TABLE_FORMAT_ICEBERG,
                TABLE_FORMAT_MISSING,
            ) and state != ConversionTableState(
                TABLE_FORMAT_NATIVE,
                TABLE_FORMAT_MISSING,
                TABLE_FORMAT_MISSING,
            ):
                raise
            LOGGER.warning(
                'Native restoration committed despite a lost client response'
            )

    def _transition(
        self,
        manifest: IcebergPublicationAttempt,
        phase: str,
    ) -> None:
        """Persist a conversion phase only through its legal lifecycle graph."""
        manifest.transition_to(phase)
        self.recovery_coordinator.save_conversion_attempt(manifest)

    def _inspect_source(self, table: SnowflakeTableName) -> ConversionMetadata:
        columns = parse_native_columns(self._query(
            'SELECT "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT", '
            '"IS_IDENTITY", "NUMERIC_PRECISION", "NUMERIC_SCALE", "COMMENT" '
            f'FROM {quote_identifier(table.database)}."INFORMATION_SCHEMA"."COLUMNS" '
            'WHERE "TABLE_SCHEMA" = %(schema)s AND "TABLE_NAME" = %(table)s '
            'ORDER BY "ORDINAL_POSITION"',
            {'schema': table.schema, 'table': table.table},
            phase='metadata',
        ))
        primary_key = self._primary_key(table)
        self._assert_primary_key(columns, primary_key, table)
        self._assert_no_other_constraints(table)
        self._assert_no_streams(table)
        self._assert_no_policies(table)
        self._assert_no_column_tags(table)
        table_comment, owner, owner_role_type = self._table_metadata(table)
        grants = tuple(self._query(
            f'SHOW GRANTS ON TABLE {table.quoted}',
            phase='metadata',
        ))
        tags = tuple(self._table_tags(table))
        return ConversionMetadata(
            columns=columns,
            primary_key=primary_key,
            owner=owner,
            owner_role_type=owner_role_type,
            table_comment=table_comment,
            grants=grants,
            tags=tags,
        )

    def _primary_key(self, table: SnowflakeTableName) -> Tuple[str, ...]:
        rows = self._query(
            f'SHOW PRIMARY KEYS IN TABLE {table.quoted}',
            phase='metadata',
        )
        try:
            ordered_rows = sorted(
                rows,
                key=lambda row: int(_row_value(row, 'KEY_SEQUENCE')),
            )
        except (TypeError, ValueError) as exc:
            raise NativeToIcebergConversionError(
                'Snowflake returned invalid primary-key ordering metadata'
            ) from exc
        return tuple(_row_value(row, 'COLUMN_NAME') for row in ordered_rows)

    def _assert_no_other_constraints(self, table: SnowflakeTableName) -> None:
        rows = self._query(
            "SELECT 'DIRECT' AS \"DEPENDENCY_KIND\", \"CONSTRAINT_TYPE\", "
            '"CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME" '
            f'FROM {quote_identifier(table.database)}."INFORMATION_SCHEMA"."TABLE_CONSTRAINTS" '
            'WHERE "TABLE_SCHEMA" = %(schema)s AND "TABLE_NAME" = %(table)s '
            "AND \"CONSTRAINT_TYPE\" <> 'PRIMARY KEY' "
            'UNION ALL '
            "SELECT 'INBOUND_FOREIGN_KEY', foreign_key.\"CONSTRAINT_TYPE\", "
            'foreign_key."CONSTRAINT_NAME", foreign_key."TABLE_SCHEMA", '
            'foreign_key."TABLE_NAME" '
            f'FROM {quote_identifier(table.database)}."INFORMATION_SCHEMA".'
            '"REFERENTIAL_CONSTRAINTS" AS reference '
            f'JOIN {quote_identifier(table.database)}."INFORMATION_SCHEMA".'
            '"TABLE_CONSTRAINTS" AS referenced '
            'ON referenced."CONSTRAINT_CATALOG" = reference."UNIQUE_CONSTRAINT_CATALOG" '
            'AND referenced."CONSTRAINT_SCHEMA" = reference."UNIQUE_CONSTRAINT_SCHEMA" '
            'AND referenced."CONSTRAINT_NAME" = reference."UNIQUE_CONSTRAINT_NAME" '
            f'JOIN {quote_identifier(table.database)}."INFORMATION_SCHEMA".'
            '"TABLE_CONSTRAINTS" AS foreign_key '
            'ON foreign_key."CONSTRAINT_CATALOG" = reference."CONSTRAINT_CATALOG" '
            'AND foreign_key."CONSTRAINT_SCHEMA" = reference."CONSTRAINT_SCHEMA" '
            'AND foreign_key."CONSTRAINT_NAME" = reference."CONSTRAINT_NAME" '
            'WHERE referenced."TABLE_SCHEMA" = %(schema)s '
            'AND referenced."TABLE_NAME" = %(table)s',
            {'schema': table.schema, 'table': table.table},
            phase='metadata',
        )
        exported_keys = self._query(
            f'SHOW EXPORTED KEYS IN TABLE {table.quoted}',
            phase='metadata',
        )
        if rows or exported_keys:
            raise NativeToIcebergConversionError(
                f'{table.quoted} has non-primary-key constraints or inbound '
                'foreign keys that manual conversion cannot preserve safely'
            )

    def _assert_primary_key(
        self,
        columns: Tuple[NativeColumn, ...],
        primary_key: Tuple[str, ...],
        table: SnowflakeTableName,
    ) -> None:
        column_names = {column.name for column in columns}
        if any(not isinstance(column, str) or column not in column_names for column in primary_key):
            raise NativeToIcebergConversionError(
                'Snowflake returned invalid primary-key metadata'
            )
        if not primary_key:
            return
        predicate = ' OR '.join(
            f'{quote_identifier(column)} IS NULL' for column in primary_key
        )
        rows = self._query(
            f'SELECT COUNT(*) AS "NULL_KEY_COUNT" FROM {table.quoted} WHERE {predicate}',
            phase='metadata',
        )
        null_count = _row_value(rows[0], 'NULL_KEY_COUNT') if rows else None
        if null_count is None:
            raise NativeToIcebergConversionError(
                'Snowflake did not return primary-key nullability evidence'
            )
        if int(null_count) > 0:
            raise NativeToIcebergConversionError(
                f'{table.quoted} contains NULL primary-key values'
            )

    def _table_metadata(
        self,
        table: SnowflakeTableName,
    ) -> Tuple[Optional[str], str, str]:
        rows = self._query(
            'SELECT "COMMENT", "CLUSTERING_KEY", "TABLE_OWNER" '
            f'FROM {quote_identifier(table.database)}."INFORMATION_SCHEMA"."TABLES" '
            'WHERE "TABLE_SCHEMA" = %(schema)s AND "TABLE_NAME" = %(table)s',
            {'schema': table.schema, 'table': table.table},
            phase='metadata',
        )
        if len(rows) != 1:
            raise NativeToIcebergConversionError(
                f'Snowflake did not return exact table metadata for {table.quoted}'
            )
        if _row_value(rows[0], 'CLUSTERING_KEY') is not None:
            raise NativeToIcebergConversionError(
                f'{table.quoted} has a clustering key that manual conversion '
                'cannot preserve safely'
            )
        owner = _row_value(rows[0], 'TABLE_OWNER')
        table_rows = self._query(
            f'SHOW TABLES IN SCHEMA {table.quoted_schema} STARTS WITH '
            f'{sql_string_literal(table.table)}',
            phase='metadata',
        )
        exact_rows = [
            row for row in table_rows
            if _row_value(row, 'name') == table.table
        ]
        shown_owner = (
            _row_value(exact_rows[0], 'owner')
            if len(exact_rows) == 1
            else None
        )
        owner_role_type = (
            str(_row_value(exact_rows[0], 'owner_role_type', '')).upper()
            if len(exact_rows) == 1
            else ''
        )
        role_rows = self._query(
            'SELECT CURRENT_ROLE() AS "CURRENT_ROLE"',
            phase='metadata',
        )
        current_role = (
            _row_value(role_rows[0], 'CURRENT_ROLE')
            if len(role_rows) == 1
            else None
        )
        if not all(
            isinstance(value, str) and value
            for value in (owner, shown_owner, current_role)
        ) or owner != shown_owner or owner_role_type != 'ROLE':
            raise NativeToIcebergConversionError(
                f'Cannot prove account-role ownership and the current role for {table.quoted}'
            )
        if owner != current_role:
            raise NativeToIcebergConversionError(
                f'Cannot convert {table.quoted} as role {current_role}; '
                f'the owning role {owner} must execute the conversion'
            )
        return _row_value(rows[0], 'COMMENT'), owner, owner_role_type

    def _assert_no_streams(self, table: SnowflakeTableName) -> None:
        rows = self._query(
            'SHOW STREAMS IN ACCOUNT',
            phase='metadata',
        )
        if not isinstance(rows, (list, tuple)):
            raise NativeToIcebergConversionError(
                'Snowflake returned invalid stream dependency metadata'
            )
        if any(stream_references_table(row, table) for row in rows):
            raise NativeToIcebergConversionError(
                f'{table.quoted} has dependent streams; conversion would make them stale'
            )

    def _assert_no_policies(self, table: SnowflakeTableName) -> None:
        rows = self._query(
            'SELECT "POLICY_KIND", "POLICY_NAME", "REF_COLUMN_NAME" '
            f'FROM TABLE({quote_identifier(table.database)}."INFORMATION_SCHEMA".'
            'POLICY_REFERENCES(REF_ENTITY_NAME => %(table)s, '
            "REF_ENTITY_DOMAIN => 'TABLE'))",
            {'table': table.quoted},
            phase='metadata',
        )
        if rows:
            raise NativeToIcebergConversionError(
                f'{table.quoted} has masking, row-access, or other policies that '
                'manual conversion cannot preserve safely'
            )

    def _assert_no_column_tags(self, table: SnowflakeTableName) -> None:
        rows = self._query(
            'SELECT "TAG_DATABASE", "TAG_SCHEMA", "TAG_NAME", "TAG_VALUE", '
            '"APPLY_METHOD", "LEVEL", "COLUMN_NAME" '
            f'FROM TABLE({quote_identifier(table.database)}."INFORMATION_SCHEMA".'
            "TAG_REFERENCES_ALL_COLUMNS(%(table)s, 'TABLE'))",
            {'table': table.quoted},
            phase='metadata',
        )
        direct_column_tags = [
            row for row in rows
            if str(_row_value(row, 'LEVEL', '')).upper() == 'COLUMN'
            and str(_row_value(row, 'APPLY_METHOD', '')).upper() != 'INHERITED'
        ]
        if direct_column_tags:
            raise NativeToIcebergConversionError(
                f'{table.quoted} has direct column tags that manual conversion '
                'cannot preserve safely'
            )

    def _table_tags(self, table: SnowflakeTableName) -> List[Dict[str, Any]]:
        rows = self._query(
            'SELECT "TAG_DATABASE", "TAG_SCHEMA", "TAG_NAME", "TAG_VALUE", '
            '"APPLY_METHOD", "LEVEL" '
            f'FROM TABLE({quote_identifier(table.database)}."INFORMATION_SCHEMA".'
            "TAG_REFERENCES(%(table)s, 'TABLE'))",
            {'table': table.quoted},
            phase='metadata',
        )
        return [
            row for row in rows
            if str(_row_value(row, 'LEVEL', '')).upper() == 'TABLE'
            and str(_row_value(row, 'APPLY_METHOD', '')).upper() != 'INHERITED'
        ]

    def _apply_metadata(
        self,
        destination: SnowflakeTableName,
        metadata: ConversionMetadata,
    ) -> None:
        for grant in metadata.grants:
            privilege = _row_value(grant, 'privilege')
            grantee_type = str(_row_value(grant, 'granted_to', '')).upper()
            grantee = _row_value(grant, 'grantee_name')
            if str(privilege).upper() == 'OWNERSHIP':
                continue
            privilege_sql = str(privilege).upper()
            if (
                not privilege_sql
                or not isinstance(grantee, str)
                or any(not (character.isalnum() or character in (' ', '_')) for character in privilege_sql)
            ):
                raise NativeToIcebergConversionError(
                    f'Unsupported Snowflake grant on {destination.quoted}'
                )
            grant_option = _snowflake_boolean(
                _row_value(grant, 'grant_option', False)
            )
            self._query(
                f'GRANT {privilege_sql} ON TABLE {destination.quoted} TO '
                f'{_grantee_sql(grantee_type, grantee)}'
                f'{" WITH GRANT OPTION" if grant_option else ""}',
                phase='metadata',
            )

        for tag in metadata.tags:
            tag_name = '.'.join(
                quote_identifier(str(_row_value(tag, field)))
                for field in ('TAG_DATABASE', 'TAG_SCHEMA', 'TAG_NAME')
            )
            tag_value = _row_value(tag, 'TAG_VALUE')
            if not isinstance(tag_value, str):
                raise NativeToIcebergConversionError(
                    f'Snowflake returned invalid tag metadata for {destination.quoted}'
                )
            self._query(
                f'ALTER ICEBERG TABLE {destination.quoted} SET TAG '
                f'{tag_name} = {sql_string_literal(tag_value)}',
                phase='metadata',
            )

    def _table_state(self, table: SnowflakeTableName) -> ConversionTableState:
        rows = self._query(
            f'SHOW TABLES IN SCHEMA {table.quoted_schema} STARTS WITH '
            f'{sql_string_literal(table.table)}',
            phase='reconcile',
        )
        formats = {}
        expected_names = {
            table.table,
            table.with_suffix('_ICEBERG').table,
            table.with_suffix('_NATIVE').table,
        }
        for row in exact_named_table_rows(rows, tuple(expected_names)):
            name = _row_value(row, 'name')
            formats[name] = physical_table_format(
                row,
                _snowflake_boolean,
                TABLE_FORMAT_ICEBERG,
                TABLE_FORMAT_NATIVE,
            )
        return ConversionTableState(
            formats.get(table.table, TABLE_FORMAT_MISSING),
            formats.get(table.with_suffix('_ICEBERG').table, TABLE_FORMAT_MISSING),
            formats.get(table.with_suffix('_NATIVE').table, TABLE_FORMAT_MISSING),
        )

    @staticmethod
    def _manual_recovery_error(table: SnowflakeTableName) -> NativeToIcebergConversionError:
        return NativeToIcebergConversionError(
            f'Cannot safely reconcile {table.quoted}; inspect the primary, '
            '_ICEBERG, _NATIVE tables and the conversion manifest before retrying'
        )
