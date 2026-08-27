"""Terminal validation for native-to-Iceberg conversion recovery."""

from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    IcebergPublicationAttempt,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    content_evidence_mismatch,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_validation import (
    ConversionTableState,
    NativeToIcebergConversionError,
    SnowflakeTableName,
    assert_iceberg_table_spec,
    assert_managed_v3,
    assert_supported_metadata,
)


_EVENTUAL_ICEBERG = 'iceberg'
_EVENTUAL_NATIVE = 'native'
_FORMAT_ICEBERG = 'iceberg'
_FORMAT_MISSING = 'missing'
_FORMAT_NATIVE = 'native'


class SnowflakeConversionFinalizationValidator:
    """Revalidate final conversion state through narrow injected operations."""

    def __init__(
        self,
        query,
        inspect_source,
        assert_equal_contents,
        manual_recovery_error,
    ):
        self.query = query
        self.inspect_source = inspect_source
        self.assert_equal_contents = assert_equal_contents
        self.manual_recovery_error = manual_recovery_error

    def validate_finalized(
        self,
        table: SnowflakeTableName,
        manifest: IcebergPublicationAttempt,
        state: ConversionTableState,
    ) -> None:
        """Prove a terminal physical state before its manifest is removed."""
        payload = manifest.manifest_payload
        if payload.rollback_required is not None:
            raise NativeToIcebergConversionError(
                'Finalized conversion manifest has an unsafe rollback marker'
            )
        if payload.eventual == _EVENTUAL_NATIVE:
            if state != ConversionTableState(
                _FORMAT_NATIVE,
                _FORMAT_ICEBERG,
                _FORMAT_MISSING,
            ):
                raise self.manual_recovery_error(table)
            self._validate_native_copy(table, manifest)
            return
        if payload.eventual != _EVENTUAL_ICEBERG or state != ConversionTableState(
            _FORMAT_ICEBERG,
            _FORMAT_MISSING,
            _FORMAT_NATIVE,
        ):
            raise self.manual_recovery_error(table)
        self.validate_promoted(table, manifest)

    def _validate_native_copy(
        self,
        table: SnowflakeTableName,
        manifest: IcebergPublicationAttempt,
    ) -> None:
        metadata = self.inspect_source(table)
        if metadata.fingerprint != manifest.manifest_payload.source_schema_fingerprint:
            raise NativeToIcebergConversionError(
                f'The schema of {table.quoted} changed after conversion started'
            )
        staging = table.with_suffix('_ICEBERG')
        assert_managed_v3(self.query, staging)
        assert_iceberg_table_spec(self.query, staging, manifest.table_spec)
        assert_supported_metadata(
            staging,
            metadata,
            self.inspect_source(staging),
        )
        row_count, row_hash = self.assert_equal_contents(
            table,
            staging,
            metadata.columns,
        )
        self._record_verified_evidence(
            manifest,
            row_count,
            row_hash,
            'Finalized Iceberg companion contents do not match staged '
            'conversion evidence',
        )

    def validate_promoted(
        self,
        table: SnowflakeTableName,
        manifest: IcebergPublicationAttempt,
    ) -> None:
        """Validate a promoted Iceberg table against its native backup."""
        metadata = self.inspect_source(table.with_suffix('_NATIVE'))
        if metadata.fingerprint != manifest.manifest_payload.source_schema_fingerprint:
            raise NativeToIcebergConversionError(
                'Native backup schema no longer matches the conversion manifest'
            )
        assert_managed_v3(self.query, table)
        assert_iceberg_table_spec(self.query, table, manifest.table_spec)
        assert_supported_metadata(table, metadata, self.inspect_source(table))
        row_count, row_hash = self.assert_equal_contents(
            table.with_suffix('_NATIVE'),
            table,
            metadata.columns,
        )
        self._record_verified_evidence(
            manifest,
            row_count,
            row_hash,
            'Promoted Iceberg contents do not match staged conversion evidence',
        )

    @staticmethod
    def _record_verified_evidence(
        manifest: IcebergPublicationAttempt,
        row_count: int,
        row_hash: int,
        mismatch_prefix: str,
    ) -> None:
        expected_evidence = (
            manifest.expected_row_count,
            manifest.expected_row_fingerprint,
        )
        actual_evidence = (row_count, str(row_hash))
        if expected_evidence[0] is not None and expected_evidence != actual_evidence:
            raise NativeToIcebergConversionError(
                content_evidence_mismatch(
                    mismatch_prefix,
                    expected_evidence,
                    actual_evidence,
                )
            )
        manifest.expected_row_count = row_count
        manifest.expected_row_fingerprint = str(row_hash)
