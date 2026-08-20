"""Exact row-multiset evidence for native-to-Iceberg conversion."""

from typing import Tuple

from pipelinewise.fastsync.commons.snowflake_iceberg_model import quote_identifier
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    content_evidence_mismatch,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_validation import (
    NativeColumn,
    NativeToIcebergConversionError,
    SnowflakeTableName,
    _value as _row_value,
)


class SnowflakeConversionEvidenceService:  # pylint: disable=too-few-public-methods
    """Compare projected native rows with their Iceberg destination."""

    def __init__(self, converter):
        self.converter = converter

    def _content_evidence(
        self,
        table: SnowflakeTableName,
        columns: Tuple[NativeColumn, ...],
        project: bool,
    ) -> Tuple[int, int]:
        select_list = ', '.join(
            column.projection if project else quote_identifier(column.name)
            for column in columns
        )
        rows = self.converter._query(  # pylint: disable=protected-access
            'SELECT COUNT(*) AS "ROW_COUNT", HASH_AGG(*) AS "ROW_HASH" '
            f'FROM (SELECT {select_list} FROM {table.quoted})',
            phase='validation',
        )
        if len(rows) != 1:
            raise NativeToIcebergConversionError(
                f'Snowflake did not return conversion evidence for {table.quoted}'
            )
        row_count = _row_value(rows[0], 'ROW_COUNT')
        row_hash = _row_value(rows[0], 'ROW_HASH')
        if row_count is None or row_hash is None:
            raise NativeToIcebergConversionError(
                f'Snowflake returned incomplete conversion evidence for {table.quoted}'
            )
        return int(row_count), int(row_hash)

    def _assert_equal_contents(
        self,
        source: SnowflakeTableName,
        destination: SnowflakeTableName,
        columns: Tuple[NativeColumn, ...],
    ) -> Tuple[int, int]:
        source_evidence = self._content_evidence(source, columns, project=True)
        target_evidence = self._content_evidence(destination, columns, project=False)
        if source_evidence != target_evidence:
            prefix = (
                f'{destination.quoted} does not match the projected contents of '
                f'{source.quoted}'
            )
            raise NativeToIcebergConversionError(
                content_evidence_mismatch(prefix, source_evidence, target_evidence)
            )
        return source_evidence


# Compatibility alias for integrations that imported the pre-composition name.
SnowflakeConversionEvidenceMixin = SnowflakeConversionEvidenceService
