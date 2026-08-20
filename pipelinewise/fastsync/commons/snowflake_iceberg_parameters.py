"""Version-specific Snowflake-managed Iceberg table parameter contracts."""

from typing import Any, Callable, Dict, Sequence

from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    SnowflakeObjectName,
    TableFormatDiscoveryError,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    MANAGED_ICEBERG_V3_SPEC,
    managed_iceberg_version_spec,
)


def assert_managed_v3_copy_on_write_parameter(
    rows: Sequence[Dict[str, Any]],
    target: SnowflakeObjectName,
) -> None:
    """Require an explicit table-level managed-v3 copy-on-write setting."""
    _assert_managed_copy_on_write_parameter(
        rows,
        target,
        MANAGED_ICEBERG_V3_SPEC,
    )


def _assert_managed_copy_on_write_parameter(
    rows: Sequence[Dict[str, Any]],
    target: SnowflakeObjectName,
    version_spec,
) -> None:
    version_spec.validate_parameter_rows(rows, target, version_spec)


def assert_managed_iceberg_table_parameters(
    query: Callable[..., Sequence[Dict[str, Any]]],
    target: SnowflakeObjectName,
    version: int,
    **query_kwargs: Any,
) -> None:
    """Validate the table-level parameter contract for a managed version."""
    try:
        version_spec = managed_iceberg_version_spec(version)
    except ValueError as exc:
        raise TableFormatDiscoveryError(
            f'No managed-Iceberg table parameter contract is defined for '
            f'version {version}'
        ) from exc
    parameter = version_spec.merge_on_read_parameter
    try:
        rows = query(
            f"SHOW PARAMETERS LIKE '{parameter}' "
            f'IN TABLE {target.quoted}',
            **query_kwargs,
        )
    except Exception as exc:
        raise TableFormatDiscoveryError(
            f'Snowflake could not read {parameter} for '
            f'{target.quoted}; PipelineWise cannot prove its copy-on-write contract'
        ) from exc
    _assert_managed_copy_on_write_parameter(rows, target, version_spec)


def validated_managed_iceberg_table_format(
    query: Callable[..., Sequence[Dict[str, Any]]],
    target: SnowflakeObjectName,
    version: int,
) -> str:
    """Return a supported managed format after its table contract is proven."""
    try:
        table_format = managed_iceberg_version_spec(version).table_format
    except ValueError as exc:
        raise TableFormatDiscoveryError(
            f'Snowflake returned unsupported ICEBERG_VERSION {version} for '
            f'{target.quoted}'
        ) from exc
    assert_managed_iceberg_table_parameters(query, target, version)
    return table_format
