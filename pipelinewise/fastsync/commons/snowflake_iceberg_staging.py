"""Data-integrity validation for transformed Iceberg staging tables."""

from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    IcebergPublicationAttempt,
    IcebergTableSpec,
    _row_value,
    quote_identifier,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    RecoveryManifestError,
    StagingPrimaryKeyError,
    TableCompatibilityError,
)


def validate_partial_staging_primary_key(
    snowflake,
    attempt: IcebergPublicationAttempt,
    spec: IcebergTableSpec,
    query_phase: str,
) -> None:
    """Reject transformed PartialSync staging with incomplete or duplicate keys."""
    if attempt.kind != 'partial':
        return
    if not spec.primary_key:
        raise TableCompatibilityError('Iceberg PartialSync requires a primary key')
    columns = {column.name: column for column in spec.columns}
    key_projection = ', '.join(
        columns[key].projection() for key in spec.primary_key
    )
    key_names = ', '.join(
        quote_identifier(key) for key in spec.primary_key
    )
    projected_alias = quote_identifier('PW_PROJECTED_STAGE')
    key_columns = ', '.join(
        f'{projected_alias}.{quote_identifier(key)}' for key in spec.primary_key
    )
    null_condition = ' OR '.join(
        f'{projected_alias}.{quote_identifier(key)} IS NULL'
        for key in spec.primary_key
    )
    source = spec.name.with_table(attempt.staging_table)
    rows = snowflake.query(
        'SELECT COALESCE(MAX("PW_NULL_KEY"), 0) AS "HAS_NULL_KEY", '
        'COALESCE(MAX(CASE WHEN "PW_KEY_COUNT" > 1 THEN 1 ELSE 0 END), 0) '
        'AS "HAS_DUPLICATE_KEY" '
        'FROM (SELECT '
        f'CASE WHEN {null_condition} THEN 1 ELSE 0 END AS "PW_NULL_KEY", '
        'COUNT(*) AS "PW_KEY_COUNT" '
        f'FROM (SELECT {key_projection} FROM {source.quoted}) AS {projected_alias} '
        f'GROUP BY {key_columns}) AS "PW_KEY_GROUPS"',
        query_tag_props={**attempt.query_tag, 'phase': query_phase},
    )
    if len(rows) != 1:
        raise RecoveryManifestError(
            f'Snowflake did not return primary-key integrity evidence for {source.quoted}'
        )
    has_null = _integrity_flag(rows[0], 'has_null_key')
    has_duplicate = _integrity_flag(rows[0], 'has_duplicate_key')
    if not has_null and not has_duplicate:
        return
    violations = []
    if has_null:
        violations.append('NULL components')
    if has_duplicate:
        violations.append('duplicate groups')
    raise StagingPrimaryKeyError(
        f'Iceberg PartialSync staging for {spec.name.quoted} contains '
        f'{" and ".join(violations)} for primary-key columns {key_names}; '
        'this invocation was blocked before publication SQL'
    )


def _integrity_flag(row, name: str) -> bool:
    value = str(_row_value(row, name)).upper()
    if value in ('0', 'FALSE'):
        return False
    if value in ('1', 'TRUE'):
        return True
    raise RecoveryManifestError(
        f'Snowflake returned invalid primary-key integrity evidence for {name}'
    )
