"""Typed manifest payloads and lifecycle transitions for Iceberg recovery."""

from copy import deepcopy
from dataclasses import dataclass, field
import math
import re
from typing import Any, Dict, FrozenSet, Optional, Type, Union

from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    PHASE_FINALIZED,
    PHASE_PREPARED,
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_STAGING_CREATED,
    PHASE_SUBMITTED,
    PHASE_UPLOADED,
    RecoveryManifestError,
)


_PUBLICATION_FIELDS = frozenset({
    'publication_query_hash',
    'publication_query_type',
    'publication_submitted_at',
    'replacement_metadata',
    'schema_evolution_applied',
    'staging_config',
})
_PARTIAL_FIELDS = _PUBLICATION_FIELDS | frozenset({
    'delete_mode',
    'drop_target',
    'end_is_unbounded',
    'end_value',
    'start_value',
    'where_clause_sql',
})
_CONVERSION_FIELDS = frozenset({
    'backup_table',
    'eventual',
    'rollback_required',
    'source_schema_fingerprint',
})
_SHA256 = re.compile(r'^[0-9a-f]{64}$')


def _invalid_payload() -> RecoveryManifestError:
    return RecoveryManifestError('Iceberg recovery manifest payload is invalid')


def _validate_publication_fields(value: Dict[str, Any]) -> None:
    for name in ('staging_config', 'replacement_metadata'):
        if name in value and not isinstance(value[name], dict):
            raise _invalid_payload()
    if (
        'schema_evolution_applied' in value
        and not isinstance(value['schema_evolution_applied'], bool)
    ):
        raise _invalid_payload()
    if 'publication_query_hash' in value and (
        not isinstance(value['publication_query_hash'], str)
        or not _SHA256.fullmatch(value['publication_query_hash'])
    ):
        raise _invalid_payload()
    if 'publication_query_type' in value and (
        not isinstance(value['publication_query_type'], str)
        or not value['publication_query_type'].strip()
    ):
        raise _invalid_payload()
    if 'publication_submitted_at' in value:
        submitted_at = value['publication_submitted_at']
        if (
            isinstance(submitted_at, bool)
            or not isinstance(submitted_at, (int, float))
            or not math.isfinite(submitted_at)
            or submitted_at <= 0
        ):
            raise _invalid_payload()


def _valid_boundary(value: Any) -> bool:
    if value is None or isinstance(value, (bool, int, str)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    return (
        isinstance(value, dict)
        and set(value) == {'type', 'value'}
        and value.get('type') in ('date', 'datetime', 'decimal', 'time')
        and isinstance(value.get('value'), str)
        and bool(value['value'])
    )


def _validate_partial_fields(value: Dict[str, Any]) -> None:
    _validate_publication_fields(value)
    if 'where_clause_sql' in value and (
        not isinstance(value['where_clause_sql'], str)
        or not value['where_clause_sql'].strip()
    ):
        raise _invalid_payload()
    for name in ('end_is_unbounded', 'drop_target'):
        if name in value and not isinstance(value[name], bool):
            raise _invalid_payload()
    if 'delete_mode' in value and value['delete_mode'] != 'hard':
        raise _invalid_payload()
    if any(
        name in value and not _valid_boundary(value[name])
        for name in ('start_value', 'end_value')
    ):
        raise _invalid_payload()


def _validate_conversion_fields(value: Dict[str, Any]) -> None:
    if 'eventual' in value and value['eventual'] not in ('native', 'iceberg'):
        raise _invalid_payload()
    if 'backup_table' in value and (
        not isinstance(value['backup_table'], str)
        or not value['backup_table']
    ):
        raise _invalid_payload()
    if 'source_schema_fingerprint' in value and (
        not isinstance(value['source_schema_fingerprint'], str)
        or not _SHA256.fullmatch(value['source_schema_fingerprint'])
    ):
        raise _invalid_payload()
    if 'rollback_required' in value and not isinstance(
        value['rollback_required'],
        bool,
    ):
        raise _invalid_payload()


def _field(value: Dict[str, Any], name: str) -> Any:
    return deepcopy(value.get(name))


def _extensions(value: Dict[str, Any], known_fields: FrozenSet[str]) -> Dict[str, Any]:
    return {
        key: deepcopy(item)
        for key, item in value.items()
        if key not in known_fields
    }


def _context(
    present_fields: FrozenSet[str],
    values: Dict[str, Any],
    extensions: Dict[str, Any],
) -> Dict[str, Any]:
    context = deepcopy(extensions)
    context.update({name: deepcopy(values[name]) for name in present_fields})
    return context


@dataclass(frozen=True)
class FullSyncManifestPayload:  # pylint: disable=too-many-instance-attributes
    """Typed FullSync-specific durable context."""

    staging_config: Optional[Dict[str, Any]] = None
    replacement_metadata: Optional[Dict[str, Any]] = None
    schema_evolution_applied: Optional[bool] = None
    publication_query_hash: Optional[str] = None
    publication_query_type: Optional[str] = None
    publication_submitted_at: Optional[float] = None
    extensions: Dict[str, Any] = field(default_factory=dict)
    present_fields: FrozenSet[str] = field(default_factory=frozenset)

    @classmethod
    def from_context(cls, value: Dict[str, Any]) -> 'FullSyncManifestPayload':
        """Build a typed payload without discarding forward-compatible fields."""
        _validate_publication_fields(value)
        return cls(
            staging_config=_field(value, 'staging_config'),
            replacement_metadata=_field(value, 'replacement_metadata'),
            schema_evolution_applied=_field(value, 'schema_evolution_applied'),
            publication_query_hash=_field(value, 'publication_query_hash'),
            publication_query_type=_field(value, 'publication_query_type'),
            publication_submitted_at=_field(value, 'publication_submitted_at'),
            extensions=_extensions(value, _PUBLICATION_FIELDS),
            present_fields=frozenset(value).intersection(_PUBLICATION_FIELDS),
        )

    def as_context(self) -> Dict[str, Any]:
        """Return the exact legacy context representation."""
        return _context(
            self.present_fields,
            {
                'staging_config': self.staging_config,
                'replacement_metadata': self.replacement_metadata,
                'schema_evolution_applied': self.schema_evolution_applied,
                'publication_query_hash': self.publication_query_hash,
                'publication_query_type': self.publication_query_type,
                'publication_submitted_at': self.publication_submitted_at,
            },
            self.extensions,
        )


@dataclass(frozen=True)
class PartialSyncManifestPayload(FullSyncManifestPayload):
    """Typed PartialSync boundary and publication context."""

    where_clause_sql: Optional[str] = None
    start_value: Any = None
    end_value: Any = None
    end_is_unbounded: Optional[bool] = None
    drop_target: Optional[bool] = None
    delete_mode: Optional[str] = None

    @classmethod
    def from_context(cls, value: Dict[str, Any]) -> 'PartialSyncManifestPayload':
        """Build a typed payload without discarding forward-compatible fields."""
        _validate_partial_fields(value)
        return cls(
            staging_config=_field(value, 'staging_config'),
            replacement_metadata=_field(value, 'replacement_metadata'),
            schema_evolution_applied=_field(value, 'schema_evolution_applied'),
            publication_query_hash=_field(value, 'publication_query_hash'),
            publication_query_type=_field(value, 'publication_query_type'),
            publication_submitted_at=_field(value, 'publication_submitted_at'),
            extensions=_extensions(value, _PARTIAL_FIELDS),
            present_fields=frozenset(value).intersection(_PARTIAL_FIELDS),
            where_clause_sql=_field(value, 'where_clause_sql'),
            start_value=_field(value, 'start_value'),
            end_value=_field(value, 'end_value'),
            end_is_unbounded=_field(value, 'end_is_unbounded'),
            drop_target=_field(value, 'drop_target'),
            delete_mode=_field(value, 'delete_mode'),
        )

    def as_context(self) -> Dict[str, Any]:
        """Return the exact legacy context representation."""
        return _context(
            self.present_fields,
            {
                'staging_config': self.staging_config,
                'replacement_metadata': self.replacement_metadata,
                'schema_evolution_applied': self.schema_evolution_applied,
                'publication_query_hash': self.publication_query_hash,
                'publication_query_type': self.publication_query_type,
                'publication_submitted_at': self.publication_submitted_at,
                'where_clause_sql': self.where_clause_sql,
                'start_value': self.start_value,
                'end_value': self.end_value,
                'end_is_unbounded': self.end_is_unbounded,
                'drop_target': self.drop_target,
                'delete_mode': self.delete_mode,
            },
            self.extensions,
        )


@dataclass(frozen=True)
class ConversionManifestPayload:
    """Typed native-to-Iceberg conversion context."""

    eventual: Optional[str] = None
    backup_table: Optional[str] = None
    source_schema_fingerprint: Optional[str] = None
    rollback_required: Optional[bool] = None
    extensions: Dict[str, Any] = field(default_factory=dict)
    present_fields: FrozenSet[str] = field(default_factory=frozenset)

    @classmethod
    def from_context(cls, value: Dict[str, Any]) -> 'ConversionManifestPayload':
        """Build a typed payload without discarding forward-compatible fields."""
        _validate_conversion_fields(value)
        return cls(
            eventual=_field(value, 'eventual'),
            backup_table=_field(value, 'backup_table'),
            source_schema_fingerprint=_field(value, 'source_schema_fingerprint'),
            rollback_required=_field(value, 'rollback_required'),
            extensions=_extensions(value, _CONVERSION_FIELDS),
            present_fields=frozenset(value).intersection(_CONVERSION_FIELDS),
        )

    def as_context(self) -> Dict[str, Any]:
        """Return the exact legacy context representation."""
        return _context(
            self.present_fields,
            {
                'eventual': self.eventual,
                'backup_table': self.backup_table,
                'source_schema_fingerprint': self.source_schema_fingerprint,
                'rollback_required': self.rollback_required,
            },
            self.extensions,
        )


ManifestPayload = Union[
    FullSyncManifestPayload,
    PartialSyncManifestPayload,
    ConversionManifestPayload,
]

_PAYLOAD_TYPES: Dict[str, Type[ManifestPayload]] = {
    'full': FullSyncManifestPayload,
    'partial': PartialSyncManifestPayload,
    'manual_conversion': ConversionManifestPayload,
}


def manifest_payload(kind: str, context: Dict[str, Any]) -> ManifestPayload:
    """Return the kind-specific typed view over compatible context state."""
    payload_type = _PAYLOAD_TYPES.get(kind)
    if payload_type is None:
        raise RecoveryManifestError(
            f'Unsupported Iceberg recovery manifest kind: {kind}'
        )
    if not isinstance(context, dict):
        raise RecoveryManifestError('Iceberg recovery manifest context is invalid')
    return payload_type.from_context(context)


def serialize_manifest_payload(
    kind: str,
    payload: ManifestPayload,
) -> Dict[str, Any]:
    """Serialize authoritative typed state for a manifest-v1 compatibility envelope."""
    expected_type = _PAYLOAD_TYPES.get(kind)
    if expected_type is None or not isinstance(payload, expected_type):
        raise RecoveryManifestError('Iceberg recovery manifest payload is invalid')
    return {
        'payload_version': 1,
        'payload_type': kind,
        'values': payload.as_context(),
    }


def load_manifest_payload(
    kind: str,
    serialized: Optional[Dict[str, Any]],
    legacy_context: Dict[str, Any],
) -> ManifestPayload:
    """Load new payload state or adapt a manifest written before payload typing."""
    if serialized is None:
        return manifest_payload(kind, legacy_context)
    if (
        not isinstance(serialized, dict)
        or serialized.get('payload_version') != 1
        or serialized.get('payload_type') != kind
        or not isinstance(serialized.get('values'), dict)
    ):
        raise RecoveryManifestError('Iceberg recovery manifest payload is invalid')
    payload = manifest_payload(kind, serialized['values'])
    if payload.as_context() != legacy_context:
        raise RecoveryManifestError(
            'Iceberg recovery manifest payload does not match its compatibility context'
        )
    return payload


_PRODUCTION_TRANSITIONS = {
    PHASE_PREPARED: frozenset((PHASE_PREPARED, PHASE_UPLOADED)),
    PHASE_UPLOADED: frozenset((PHASE_PREPARED, PHASE_STAGING_CREATED)),
    PHASE_STAGING_CREATED: frozenset((PHASE_PREPARED, PHASE_STAGED)),
    PHASE_STAGED: frozenset((PHASE_PREPARED, PHASE_STAGING_CREATED, PHASE_SUBMITTED)),
    PHASE_SUBMITTED: frozenset((PHASE_STAGED, PHASE_PUBLISHED)),
    PHASE_PUBLISHED: frozenset((PHASE_FINALIZED,)),
    PHASE_FINALIZED: frozenset(),
}
_CONVERSION_TRANSITIONS = {
    PHASE_PREPARED: frozenset((
        PHASE_STAGING_CREATED,
        PHASE_STAGED,
        PHASE_SUBMITTED,
        PHASE_PUBLISHED,
        PHASE_FINALIZED,
    )),
    PHASE_STAGING_CREATED: frozenset((
        PHASE_STAGED,
        PHASE_SUBMITTED,
        PHASE_PUBLISHED,
        PHASE_FINALIZED,
    )),
    PHASE_STAGED: frozenset((
        PHASE_STAGED,
        PHASE_SUBMITTED,
        PHASE_PUBLISHED,
        PHASE_FINALIZED,
    )),
    PHASE_SUBMITTED: frozenset((
        PHASE_STAGED,
        PHASE_SUBMITTED,
        PHASE_PUBLISHED,
        PHASE_FINALIZED,
    )),
    PHASE_PUBLISHED: frozenset((PHASE_SUBMITTED, PHASE_FINALIZED)),
    PHASE_FINALIZED: frozenset(),
}


def validate_phase_transition(kind: str, current_phase: str, next_phase: str) -> None:
    """Reject lifecycle movement outside the explicit kind-specific graph."""
    transitions = (
        _CONVERSION_TRANSITIONS
        if kind == 'manual_conversion'
        else _PRODUCTION_TRANSITIONS
        if kind in ('full', 'partial')
        else None
    )
    if transitions is None or next_phase not in transitions.get(current_phase, frozenset()):
        raise RecoveryManifestError(
            f'Invalid Iceberg {kind} recovery transition: '
            f'{current_phase} -> {next_phase}'
        )
