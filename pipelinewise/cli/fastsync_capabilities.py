"""Format-aware FastSync capability policy."""
from dataclasses import dataclass
from types import MappingProxyType
from typing import FrozenSet, Literal, Mapping, Optional, Tuple, Union

from .constants import ConnectorType


TABLE_FORMAT_NATIVE = 'native'
TABLE_FORMAT_ICEBERG = 'iceberg'
FastSyncOperation = Literal['full_sync', 'partial_sync']
CapabilityKey = Tuple[str, ConnectorType, ConnectorType]
CompatibilityPairs = Mapping[ConnectorType, FrozenSet[ConnectorType]]


@dataclass(frozen=True)
class FastSyncCapabilities:
    """FastSync operations implemented for one tap, target, and table format."""

    full_sync: bool = False
    partial_sync: bool = False

    @property
    def available(self) -> bool:
        """Return whether either FastSync operation is implemented."""
        return self.full_sync or self.partial_sync

    def supports(self, operation: Optional[FastSyncOperation] = None) -> bool:
        """Return whether the requested operation is implemented."""
        if operation is None:
            return self.available
        if operation == 'full_sync':
            return self.full_sync
        if operation == 'partial_sync':
            return self.partial_sync
        raise ValueError(f'Unsupported FastSync operation: {operation}')


_NO_FASTSYNC = FastSyncCapabilities()

_FASTSYNC_CAPABILITIES: Mapping[CapabilityKey, FastSyncCapabilities] = MappingProxyType({
    (
        TABLE_FORMAT_NATIVE,
        ConnectorType.TAP_MYSQL,
        ConnectorType.TARGET_SNOWFLAKE,
    ): FastSyncCapabilities(full_sync=True, partial_sync=True),
    (
        TABLE_FORMAT_NATIVE,
        ConnectorType.TAP_MYSQL,
        ConnectorType.TARGET_POSTGRES,
    ): FastSyncCapabilities(full_sync=True),
    (
        TABLE_FORMAT_NATIVE,
        ConnectorType.TAP_POSTGRES,
        ConnectorType.TARGET_SNOWFLAKE,
    ): FastSyncCapabilities(full_sync=True, partial_sync=True),
    (
        TABLE_FORMAT_NATIVE,
        ConnectorType.TAP_POSTGRES,
        ConnectorType.TARGET_POSTGRES,
    ): FastSyncCapabilities(full_sync=True),
    (
        TABLE_FORMAT_NATIVE,
        ConnectorType.TAP_MONGODB,
        ConnectorType.TARGET_SNOWFLAKE,
    ): FastSyncCapabilities(full_sync=True),
    (
        TABLE_FORMAT_NATIVE,
        ConnectorType.TAP_MONGODB,
        ConnectorType.TARGET_POSTGRES,
    ): FastSyncCapabilities(full_sync=True),
    (
        TABLE_FORMAT_NATIVE,
        ConnectorType.TAP_YUGABYTE,
        ConnectorType.TARGET_SNOWFLAKE,
    ): FastSyncCapabilities(full_sync=True),
    (
        TABLE_FORMAT_NATIVE,
        ConnectorType.TAP_YUGABYTE,
        ConnectorType.TARGET_POSTGRES,
    ): FastSyncCapabilities(full_sync=True),
    (
        TABLE_FORMAT_ICEBERG,
        ConnectorType.TAP_MYSQL,
        ConnectorType.TARGET_SNOWFLAKE,
    ): FastSyncCapabilities(full_sync=True, partial_sync=True),
    (
        TABLE_FORMAT_ICEBERG,
        ConnectorType.TAP_POSTGRES,
        ConnectorType.TARGET_SNOWFLAKE,
    ): FastSyncCapabilities(full_sync=True, partial_sync=True),
})


def _compatibility_pairs(
    table_format: str,
    operation: FastSyncOperation,
) -> CompatibilityPairs:
    pairs = {}
    for (
        registered_format,
        tap_type,
        target_type,
    ), capabilities in _FASTSYNC_CAPABILITIES.items():
        if registered_format == table_format and capabilities.supports(operation):
            pairs.setdefault(tap_type, set()).add(target_type)
    return MappingProxyType(
        {tap_type: frozenset(target_types) for tap_type, target_types in pairs.items()}
    )


# Deprecated immutable compatibility views. New code should call the resolver.
FASTSYNC_PAIRS = _compatibility_pairs(TABLE_FORMAT_NATIVE, 'full_sync')
ICEBERG_FASTSYNC_PAIRS = _compatibility_pairs(TABLE_FORMAT_ICEBERG, 'full_sync')
PARTIAL_SYNC_PAIRS = _compatibility_pairs(TABLE_FORMAT_NATIVE, 'partial_sync')


def resolve_fastsync_capabilities(
    tap_type: Union[str, ConnectorType],
    target_type: Union[str, ConnectorType],
    target_table_format: Optional[str] = None,
) -> FastSyncCapabilities:
    """Resolve FastSync support; an omitted table format retains native behavior."""
    table_format = (
        TABLE_FORMAT_NATIVE if target_table_format is None else target_table_format
    )
    if table_format not in {TABLE_FORMAT_NATIVE, TABLE_FORMAT_ICEBERG}:
        raise ValueError(f'Unsupported target table format: {table_format}')

    return _FASTSYNC_CAPABILITIES.get(
        (
            table_format,
            ConnectorType(tap_type),
            ConnectorType(target_type),
        ),
        _NO_FASTSYNC,
    )
