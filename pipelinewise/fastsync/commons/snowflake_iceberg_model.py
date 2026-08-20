"""Data models and durable state for Snowflake-managed Iceberg publication."""

# pylint: disable=too-many-lines

from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from hashlib import sha256
import fcntl
import json
import os
import re
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from uuid import uuid4

from pipelinewise.fastsync.commons import utils
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    PHASE_FINALIZED,
    PHASE_PREPARED,
    PHASE_PUBLISHED,
    PHASE_STAGED,
    PHASE_STAGING_CREATED,
    PHASE_SUBMITTED,
    PHASE_UPLOADED,
    RecoveryManifestError,
    TableFormatDiscoveryError,
    validate_phase_finalization_actions,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_manifest import (
    ManifestPayload,
    load_manifest_payload,
    manifest_payload,
    serialize_manifest_payload,
    validate_phase_transition,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    RECOVERY_IDENTITY_VERSION,
    TRANSFORMATION_SEMANTICS_VERSION,
    is_supported_managed_iceberg_version,
    managed_iceberg_version_spec,
)


TABLE_FORMAT_MISSING = 'missing'
TABLE_FORMAT_NATIVE = 'native'
TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG = 'unsupported_external_iceberg'

PUBLICATION_MISSING_CTAS = 'missing_ctas'
PUBLICATION_INSERT_OVERWRITE = 'insert_overwrite'
PUBLICATION_ADDITIVE_OVERWRITE = 'additive_overwrite'
PUBLICATION_REPLACEMENT_CTAS = 'replacement_ctas'
PUBLICATION_PARTIAL_BOOTSTRAP_CTAS = 'partial_bootstrap_ctas'
PUBLICATION_PARTIAL_MERGE = 'partial_merge'
PUBLICATION_PARTIAL_REPLACEMENT_CTAS = 'partial_replacement_ctas'

RECOVERY_RESTART_STAGING = 'restart_staging'
RECOVERY_PUBLISH = 'publish'
RECOVERY_FINALIZE = 'finalize'
RECOVERY_STATE_HANDOFF = 'state_handoff'

TARGET_ATTEMPT_RESERVED = 'reserved'
TARGET_ATTEMPT_ACTIVE = 'active'
TARGET_ATTEMPT_ABORTING = 'aborting'
TARGET_ATTEMPT_COMPLETED = 'completed'

_VALID_TARGET_ATTEMPT_STATES = {
    TARGET_ATTEMPT_ABORTING,
    TARGET_ATTEMPT_ACTIVE,
    TARGET_ATTEMPT_COMPLETED,
    TARGET_ATTEMPT_RESERVED,
}

_VALID_PHASES = {
    PHASE_PREPARED,
    PHASE_UPLOADED,
    PHASE_STAGING_CREATED,
    PHASE_STAGED,
    PHASE_SUBMITTED,
    PHASE_PUBLISHED,
    PHASE_FINALIZED,
}
_IDENTIFIER = r'\s*(?:"((?:""|[^"])*)"|([A-Za-z_][A-Za-z0-9_$]*))\s*'
_FQTN_PATTERN = re.compile(rf'^{_IDENTIFIER}\.{_IDENTIFIER}\.{_IDENTIFIER}$')
_COLUMN_PATTERN = re.compile(rf'^{_IDENTIFIER}(.+?)\s*$')


def quote_identifier(identifier: str) -> str:
    """Quote one exact Snowflake identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def sql_string_literal(value: str) -> str:
    """Quote a Snowflake string literal."""
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"


def _sql_hash(statement: str) -> str:
    normalized = re.sub(r'\s+', ' ', statement.strip().rstrip(';')).upper()
    return sha256(normalized.encode('utf-8')).hexdigest()


def validate_recovery_identity(value: Any) -> None:
    """Reject malformed or unversioned durable recovery identities."""
    if not isinstance(value, dict):
        raise RecoveryManifestError('Iceberg recovery manifest identity is missing')
    scope = value.get('scope')
    expected_keys = {'identity_version', 'scope', 'fingerprint'}
    if scope == 'fastsync':
        expected_keys.update({
            'stream_fingerprint',
            'target_table_format',
            'iceberg_version',
            'transformation_semantics_version',
            'transformation_fingerprint',
        })
    if (
        set(value) != expected_keys
        or value.get('identity_version') != RECOVERY_IDENTITY_VERSION
        or scope not in ('fastsync', 'manual_conversion')
        or not _is_sha256(value.get('fingerprint'))
    ):
        raise RecoveryManifestError('Iceberg recovery manifest identity is invalid')
    if scope == 'fastsync' and not _valid_fastsync_identity(value):
        raise RecoveryManifestError('Iceberg recovery manifest transformation identity is invalid')


def _valid_fastsync_identity(value: Dict[str, Any]) -> bool:
    return (
        _is_sha256(value.get('stream_fingerprint'))
        and value.get('target_table_format') == 'iceberg'
        and is_supported_managed_iceberg_version(
            value.get('iceberg_version')
        )
        and value.get('transformation_semantics_version')
        == TRANSFORMATION_SEMANTICS_VERSION
        and _is_sha256(value.get('transformation_fingerprint'))
    )


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and not set(value).difference('0123456789abcdef')
    )


def _validate_manifest_s3_keys(value: Any) -> List[str]:
    """Require the exact safe staging-key collection persisted by planning."""
    if (
        not isinstance(value, list)
        or any(not isinstance(s3_key, str) or not s3_key for s3_key in value)
        or len(set(value)) != len(value)
    ):
        raise RecoveryManifestError(
            'Iceberg recovery manifest S3 keys are invalid'
        )
    return list(value)


def _publication_query_type(method: str) -> str:
    if method in (
        PUBLICATION_MISSING_CTAS,
        PUBLICATION_REPLACEMENT_CTAS,
        PUBLICATION_PARTIAL_BOOTSTRAP_CTAS,
        PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
    ):
        return 'CREATE_TABLE_AS_SELECT'
    if method in (PUBLICATION_INSERT_OVERWRITE, PUBLICATION_ADDITIVE_OVERWRITE):
        return 'INSERT'
    if method == PUBLICATION_PARTIAL_MERGE:
        return 'MERGE'
    raise RecoveryManifestError(f'Unsupported Iceberg publication method: {method}')


def _row_value(row: Dict[str, Any], name: str, default: Any = None) -> Any:
    for key in (name, name.upper(), name.lower()):
        if key in row:
            return row[key]
    if default is not None:
        return default
    raise TableFormatDiscoveryError(f"Snowflake metadata did not return '{name}'")


def _snowflake_boolean(value: Any, field_name: str) -> bool:
    if value is True or str(value).upper() in ('Y', 'YES', 'TRUE'):
        return True
    if value is False or str(value).upper() in ('N', 'NO', 'FALSE'):
        return False
    raise TableFormatDiscoveryError(
        f"Snowflake metadata returned invalid '{field_name}' value: {value!r}"
    )


def _identifier_from_match(groups: Sequence[Optional[str]]) -> str:
    quoted, unquoted = groups
    if quoted is not None:
        return quoted.replace('""', '"')
    return unquoted.upper()


def _json_safe_boundary(value: Any) -> Any:
    """Return stable JSON evidence for a resolved PartialSync boundary."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return {'type': 'decimal', 'value': str(value)}
    if isinstance(value, datetime):
        return {'type': 'datetime', 'value': value.isoformat()}
    if isinstance(value, date):
        return {'type': 'date', 'value': value.isoformat()}
    if isinstance(value, datetime_time):
        return {'type': 'time', 'value': value.isoformat()}
    raise RecoveryManifestError(
        f'Unsupported PartialSync boundary evidence type: {type(value).__name__}'
    )


@dataclass(frozen=True)
class SnowflakeObjectName:
    """An exact three-part Snowflake object name."""

    database: str
    schema: str
    table: str

    def __post_init__(self) -> None:
        if not all(isinstance(value, str) and value for value in (self.database, self.schema, self.table)):
            raise ValueError('Snowflake database, schema, and table names must be non-empty strings')

    @classmethod
    def parse(cls, fqtn: str) -> 'SnowflakeObjectName':
        """Parse an exact quoted or unquoted database.schema.table name."""
        match = _FQTN_PATTERN.fullmatch(fqtn)
        if not match:
            raise ValueError('Expected a three-part Snowflake table name')
        groups = match.groups()
        return cls(*(_identifier_from_match(groups[index:index + 2]) for index in range(0, 6, 2)))

    @classmethod
    def parse_list(cls, qualified_names: str) -> Tuple['SnowflakeObjectName', ...]:
        """Parse Snowflake's comma-separated list of qualified object names."""
        if not isinstance(qualified_names, str) or not qualified_names.strip():
            raise ValueError('Expected one or more qualified Snowflake object names')
        parts = []
        part_start = 0
        in_quotes = False
        index = 0
        while index < len(qualified_names):
            character = qualified_names[index]
            if character == '"':
                if (
                    in_quotes
                    and index + 1 < len(qualified_names)
                    and qualified_names[index + 1] == '"'
                ):
                    index += 2
                    continue
                in_quotes = not in_quotes
            elif character == ',' and not in_quotes:
                parts.append(qualified_names[part_start:index].strip())
                part_start = index + 1
            index += 1
        if in_quotes:
            raise ValueError('Snowflake object list contains an unterminated identifier')
        parts.append(qualified_names[part_start:].strip())
        if any(not part for part in parts):
            raise ValueError('Snowflake object list contains an empty identifier')
        return tuple(cls.parse(part) for part in parts)

    @property
    def quoted(self) -> str:
        """Return the exactly quoted three-part name."""
        return '.'.join(quote_identifier(value) for value in (self.database, self.schema, self.table))

    @property
    def key(self) -> str:
        """Return a human-readable qualified name for diagnostics and query tags."""
        return '.'.join((self.database, self.schema, self.table))

    def with_table(self, table: str) -> 'SnowflakeObjectName':
        """Return the same database and schema with another table name."""
        return replace(self, table=table)

    def with_suffix(self, suffix: str) -> 'SnowflakeObjectName':
        """Return this object with a suffix appended to its table identifier."""
        return self.with_table(f'{self.table}{suffix}')

    def staging_name(self, load_id: str) -> str:
        """Return a stable, length-safe staging identifier for one logical load."""
        digest = sha256(f'{self.key}:{load_id}'.encode('utf-8')).hexdigest()[:16].upper()
        suffix = f'_PW_ICEBERG_{digest}'
        return f'{self.table[:255 - len(suffix)]}{suffix}'


def canonical_iceberg_type(data_type: str, iceberg_version: int = 3) -> str:
    """Normalize a type through the complete managed-version strategy."""
    return managed_iceberg_version_spec(iceberg_version).canonical_type(data_type)


@dataclass(frozen=True)
class IcebergColumn:
    """One canonical managed-Iceberg column."""

    name: str
    data_type: str
    nullable: bool = True
    iceberg_version: int = field(default=3, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            'data_type',
            canonical_iceberg_type(self.data_type, self.iceberg_version),
        )

    @classmethod
    def from_sql_definition(
        cls,
        definition: str,
        nullable: bool = True,
        iceberg_version: int = 3,
    ) -> 'IcebergColumn':
        """Parse one FastSync target column definition."""
        match = _COLUMN_PATTERN.fullmatch(definition)
        if not match:
            raise ValueError(f'Invalid FastSync column definition: {definition}')
        name = _identifier_from_match(match.groups()[:2])
        data_type = managed_iceberg_version_spec(
            iceberg_version
        ).canonical_fastsync_type(match.group(3))
        return cls(name, data_type, nullable, iceberg_version)

    @classmethod
    def from_snowflake_row(
        cls,
        row: Dict[str, Any],
        iceberg_version: int = 3,
    ) -> 'IcebergColumn':
        """Build a canonical column from INFORMATION_SCHEMA.COLUMNS metadata."""
        name, data_type, nullable = managed_iceberg_version_spec(
            iceberg_version
        ).canonical_existing_column(row)
        return cls(name, data_type, nullable, iceberg_version)

    @property
    def quoted_name(self) -> str:
        """Return the exactly quoted column name."""
        return quote_identifier(self.name)

    @property
    def definition(self) -> str:
        """Return the explicit Iceberg column definition."""
        nullability = '' if self.nullable else ' NOT NULL'
        return f'{self.quoted_name} {self.data_type}{nullability}'

    def projection(self, source_alias: Optional[str] = None) -> str:
        """Return an explicit typed projection from the staging column."""
        source = f'{quote_identifier(source_alias)}.' if source_alias else ''
        return f'CAST({source}{self.quoted_name} AS {self.data_type}) AS {self.quoted_name}'

    def as_dict(self) -> Dict[str, Any]:
        """Return the credential-free manifest representation."""
        value = {
            'name': self.name,
            'data_type': self.data_type,
            'nullable': self.nullable,
        }
        if self.iceberg_version != 3:
            value['iceberg_version'] = self.iceberg_version
        return value


@dataclass(frozen=True)
class IcebergTableSpec:
    """Canonical target schema used by every Iceberg publication method."""

    name: SnowflakeObjectName
    columns: Tuple[IcebergColumn, ...]
    primary_key: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        column_versions = {column.iceberg_version for column in self.columns}
        if len(column_versions) > 1:
            raise ValueError(
                'Every Iceberg table column must use the same managed version'
            )
        names = [column.name for column in self.columns]
        if len(names) != len(set(names)):
            raise ValueError('Iceberg column names must be unique')
        missing_primary_keys = [key for key in self.primary_key if key not in names]
        if missing_primary_keys:
            raise ValueError(f'Primary-key columns are missing from the schema: {missing_primary_keys}')

    @classmethod
    def from_fastsync(
        cls,
        database: str,
        schema: str,
        table: str,
        columns: Sequence[str],
        primary_keys: Optional[Sequence[str]],
        iceberg_version: int = 3,
    ) -> 'IcebergTableSpec':
        """Convert FastSync columns through the requested managed strategy."""
        version_spec = managed_iceberg_version_spec(iceberg_version)
        primary_key_names = tuple(_parse_identifier(value) for value in (primary_keys or ()))
        parsed = [
            IcebergColumn.from_sql_definition(
                value,
                iceberg_version=iceberg_version,
            )
            for value in columns
        ]
        metadata = (
            IcebergColumn(
                utils.SDC_EXTRACTED_AT,
                version_spec.physical_type_for_logical('timestamp_ntz'),
                iceberg_version=iceberg_version,
            ),
            IcebergColumn(
                utils.SDC_BATCHED_AT,
                version_spec.physical_type_for_logical('timestamp_ntz'),
                iceberg_version=iceberg_version,
            ),
            IcebergColumn(
                utils.SDC_DELETED_AT,
                version_spec.physical_type_for_logical('text'),
                iceberg_version=iceberg_version,
            ),
        )
        parsed_names = {column.name for column in parsed}
        parsed.extend(column for column in metadata if column.name not in parsed_names)
        parsed = [replace(column, nullable=column.name not in primary_key_names) for column in parsed]
        return cls(SnowflakeObjectName(database, schema, table), tuple(parsed), primary_key_names)

    @property
    def fingerprint(self) -> str:
        """Return a stable fingerprint for the canonical ordered schema."""
        payload = {
            'columns': [column.as_dict() for column in self.columns],
            'primary_key': list(self.primary_key),
        }
        return sha256(json.dumps(payload, sort_keys=True).encode('utf-8')).hexdigest()

    @property
    def quoted_columns(self) -> str:
        """Return the ordered, quoted column list."""
        return ', '.join(column.quoted_name for column in self.columns)

    @property
    def column_definitions(self) -> str:
        """Return the ordered explicit column definitions."""
        return ', '.join(column.definition for column in self.columns)

    @property
    def projection(self) -> str:
        """Return the ordered staging projection."""
        return ', '.join(column.projection() for column in self.columns)

    @property
    def primary_key_clause(self) -> str:
        """Return the optional out-of-line primary-key clause."""
        if not self.primary_key:
            return ''
        keys = ', '.join(quote_identifier(key) for key in self.primary_key)
        return f', PRIMARY KEY ({keys})'

    def as_dict(self) -> Dict[str, Any]:
        """Return the credential-free manifest representation."""
        return {
            'name': {
                'database': self.name.database,
                'schema': self.name.schema,
                'table': self.name.table,
            },
            'columns': [column.as_dict() for column in self.columns],
            'primary_key': list(self.primary_key),
        }

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> 'IcebergTableSpec':
        """Rebuild a table specification from manifest data."""
        return cls(
            SnowflakeObjectName(**value['name']),
            tuple(IcebergColumn(**column) for column in value['columns']),
            tuple(value.get('primary_key', ())),
        )


def _parse_identifier(identifier: str) -> str:
    match = re.fullmatch(_IDENTIFIER, identifier)
    if not match:
        raise ValueError(f'Invalid Snowflake identifier: {identifier}')
    return _identifier_from_match(match.groups())


@dataclass(frozen=True)
class SnowflakeTableSnapshot:
    """The exact physical state used for planning and stale-writer detection."""

    table_format: str
    spec: Optional[IcebergTableSpec]
    identity: Optional[str]

    @property
    def fingerprint(self) -> str:
        """Return the physical target fingerprint used by stale-writer checks."""
        payload = {
            'table_format': self.table_format,
            'spec': self.spec.as_dict() if self.spec else None,
            'identity': self.identity,
        }
        return sha256(json.dumps(payload, sort_keys=True, default=str).encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class SnowflakeTableMetadata:
    """Replacement-safe metadata that must be restored after CTAS."""

    table_comment: Optional[str] = None
    column_comments: Tuple[Tuple[str, str], ...] = ()
    owner: Optional[str] = None
    explicit_grants: Tuple[Tuple[str, str, str, bool], ...] = ()
    table_tags: Tuple[Tuple[str, str, str, str], ...] = ()

    def as_dict(self) -> Dict[str, Any]:
        """Return metadata in manifest-safe form."""
        return {
            'table_comment': self.table_comment,
            'column_comments': [list(comment) for comment in self.column_comments],
            'owner': self.owner,
            'explicit_grants': [list(grant) for grant in self.explicit_grants],
            'table_tags': [list(tag) for tag in self.table_tags],
        }

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> 'SnowflakeTableMetadata':
        """Rebuild replacement metadata from a manifest."""
        return cls(
            value.get('table_comment'),
            tuple(tuple(comment) for comment in value.get('column_comments', ())),
            value.get('owner'),
            tuple(
                (
                    str(grant[0]),
                    str(grant[1]),
                    str(grant[2]),
                    bool(grant[3]),
                )
                for grant in value.get('explicit_grants', ())
            ),
            tuple(
                (str(tag[0]), str(tag[1]), str(tag[2]), str(tag[3]))
                for tag in value.get('table_tags', ())
            ),
        )


@dataclass(frozen=True)
class PublicationPlan:
    """Statements and guard evidence for one publication attempt."""

    method: str
    preparation_statements: Tuple[str, ...]
    publication_statements: Tuple[str, ...]
    target_fingerprint: str
    query_tag: Dict[str, str]

    @property
    def statements(self) -> Tuple[str, ...]:
        """Return preparation and publication statements in execution order."""
        return self.preparation_statements + self.publication_statements


@dataclass
class IcebergPublicationAttempt:  # pylint: disable=too-many-instance-attributes
    """Credential-free durable state for one table publication."""

    load_id: str
    attempt_id: str
    kind: str
    table_spec: IcebergTableSpec
    source_bookmark: Dict[str, Any]
    intended_state: Optional[Dict[str, Any]]
    staging_table: str
    method: Optional[str]
    pre_publication_target_fingerprint: str  # pylint: disable=invalid-name
    target_table_format: str
    iceberg_version: int
    phase: str = PHASE_PREPARED
    s3_keys: List[str] = field(default_factory=list)
    expected_row_count: Optional[int] = None
    expected_row_fingerprint: Optional[str] = None
    query_id: Optional[str] = None
    recovery_identity: Dict[str, Any] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    finalization: Dict[str, bool] = field(default_factory=dict)
    is_recovery: bool = False
    _manifest_payload: Optional[ManifestPayload] = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        self._manifest_payload = manifest_payload(self.kind, self.context)
        self.context = self._manifest_payload.as_context()

    @classmethod
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def new(
        cls,
        kind: str,
        table_spec: IcebergTableSpec,
        source_bookmark: Optional[Dict[str, Any]],
        staging_table: str,
        method: Optional[str],
        pre_publication_target_fingerprint: str,  # pylint: disable=invalid-name
        recovery_identity: Dict[str, Any],
        target_table_format: str,
        iceberg_version: int,
        intended_state: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> 'IcebergPublicationAttempt':
        """Create an attempt with new load and submission identities."""
        load_id = uuid4().hex
        attempt = cls(
            load_id=load_id,
            attempt_id=uuid4().hex,
            kind=kind,
            table_spec=table_spec,
            source_bookmark=dict(source_bookmark or {}),
            intended_state=intended_state,
            staging_table=staging_table,
            method=method,
            pre_publication_target_fingerprint=pre_publication_target_fingerprint,
            target_table_format=target_table_format,
            iceberg_version=iceberg_version,
            recovery_identity=dict(recovery_identity),
            context=dict(context or {}),
        )
        attempt.validate_table_format_contract()
        return attempt

    @property
    def target(self) -> SnowflakeObjectName:
        """Return the persisted target name."""
        return self.table_spec.name

    @property
    def query_tag(self) -> Dict[str, str]:
        """Return the exact tag identifying one publication submission."""
        query_tag = {
            'ppw_component': 'fastsync',
            'load_id': self.load_id,
            'attempt_id': self.attempt_id,
            'phase': 'publication',
            'target': self.target.key,
        }
        if self.method:
            query_tag['publication_method'] = self.method
        return query_tag

    @property
    def manifest_payload(self) -> ManifestPayload:
        """Return a defensive copy of authoritative typed manifest state."""
        if self._manifest_payload is None:
            raise RecoveryManifestError(
                'Iceberg recovery manifest payload is not initialized'
            )
        return deepcopy(self._manifest_payload)

    def update_manifest_payload(
        self,
        updates: Optional[Dict[str, Any]] = None,
        remove: Sequence[str] = (),
    ) -> None:
        """Update typed payload state and refresh its legacy context projection."""
        context = self.manifest_payload.as_context()
        for name in remove:
            context.pop(name, None)
        context.update(dict(updates or {}))
        self._manifest_payload = manifest_payload(self.kind, context)
        self.context = self._manifest_payload.as_context()

    def transition_to(self, phase: str) -> None:
        """Move to a phase only when the kind-specific lifecycle permits it."""
        self.validate_transition_to(phase)
        self.phase = phase

    def validate_transition_to(self, phase: str) -> None:
        """Validate a lifecycle transition without mutating the attempt."""
        validate_phase_transition(self.kind, self.phase, phase)

    def as_dict(self) -> Dict[str, Any]:
        """Return the durable, credential-free manifest representation."""
        self.validate_table_format_contract()
        s3_keys = _validate_manifest_s3_keys(self.s3_keys)
        finalization = validate_phase_finalization_actions(self)
        payload = self.manifest_payload
        compatibility_context = payload.as_context()
        self.context = compatibility_context
        return {
            'manifest_version': 1,
            'load_id': self.load_id,
            'attempt_id': self.attempt_id,
            'kind': self.kind,
            'table_spec': self.table_spec.as_dict(),
            'schema_fingerprint': self.table_spec.fingerprint,
            'source_bookmark': self.source_bookmark,
            'intended_state': self.intended_state,
            'target': self.target.key,
            'target_parts': [self.target.database, self.target.schema, self.target.table],
            'staging_table': self.staging_table,
            'method': self.method,
            'pre_publication_target_fingerprint': self.pre_publication_target_fingerprint,
            'target_table_format': self.target_table_format,
            'iceberg_version': self.iceberg_version,
            'phase': self.phase,
            's3_keys': s3_keys,
            'expected_row_count': self.expected_row_count,
            'expected_row_fingerprint': self.expected_row_fingerprint,
            'query_id': self.query_id,
            'recovery_identity': self.recovery_identity,
            'publication_query_hash': compatibility_context.get(
                'publication_query_hash'
            ),
            'publication_query_type': compatibility_context.get(
                'publication_query_type'
            ),
            'query_tag': self.query_tag,
            'context': compatibility_context,
            'payload': serialize_manifest_payload(self.kind, payload),
            'finalization': finalization,
        }

    def validate_table_format_contract(self) -> None:
        """Reject unsupported format state or FastSync identity drift."""
        if (
            self.target_table_format != 'iceberg'
            or not is_supported_managed_iceberg_version(
                self.iceberg_version
            )
        ):
            raise RecoveryManifestError(
                'Iceberg recovery manifest table format contract is unsupported'
            )
        if any(
            column.iceberg_version != self.iceberg_version
            for column in self.table_spec.columns
        ):
            raise RecoveryManifestError(
                'Iceberg recovery manifest columns do not match its managed version'
            )
        validate_recovery_identity(self.recovery_identity)
        if self.recovery_identity['scope'] == 'fastsync' and (
            self.recovery_identity['target_table_format']
            != self.target_table_format
            or self.recovery_identity['iceberg_version']
            != self.iceberg_version
        ):
            raise RecoveryManifestError(
                'Iceberg recovery manifest table format contract does not match its identity'
            )

    @staticmethod
    def validate_staging_evidence(row_count: Any, row_fingerprint: Any) -> None:
        """Reject incomplete deterministic content evidence."""
        if (
            isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
            or not isinstance(row_fingerprint, str)
            or not row_fingerprint
        ):
            raise RecoveryManifestError(
                'Iceberg publication requires a row count and row fingerprint'
            )

    def require_staging_evidence(self) -> None:
        """Reject publication state without complete deterministic content evidence."""
        self.validate_staging_evidence(
            self.expected_row_count, self.expected_row_fingerprint
        )

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> 'IcebergPublicationAttempt':
        """Validate and rebuild a durable publication attempt."""
        if value.get('manifest_version') != 1:
            raise RecoveryManifestError('Unsupported Iceberg recovery manifest version')
        if (
            'source_bookmark' not in value
            or not isinstance(value['source_bookmark'], dict)
        ):
            raise RecoveryManifestError(
                'Iceberg recovery manifest source bookmark is invalid'
            )
        if 's3_keys' not in value:
            raise RecoveryManifestError(
                'Iceberg recovery manifest S3 keys are invalid'
            )
        s3_keys = _validate_manifest_s3_keys(value['s3_keys'])
        phase = value.get('phase')
        if phase not in _VALID_PHASES:
            raise RecoveryManifestError(f'Invalid Iceberg recovery phase: {phase}')
        spec = IcebergTableSpec.from_dict(value['table_spec'])
        if value.get('schema_fingerprint') != spec.fingerprint:
            raise RecoveryManifestError('Iceberg recovery manifest schema fingerprint is invalid')
        if value.get('target') != spec.name.key or value.get('target_parts') != [
            spec.name.database,
            spec.name.schema,
            spec.name.table,
        ]:
            raise RecoveryManifestError('Iceberg recovery manifest target identity is invalid')
        legacy_context = value.get('context', {})
        if not isinstance(legacy_context, dict):
            raise RecoveryManifestError(
                'Iceberg recovery manifest context is invalid'
            )
        payload = load_manifest_payload(
            value['kind'],
            value.get('payload'),
            legacy_context,
        )
        attempt = cls(
            load_id=value['load_id'],
            attempt_id=value['attempt_id'],
            kind=value['kind'],
            table_spec=spec,
            source_bookmark=deepcopy(value['source_bookmark']),
            intended_state=value.get('intended_state'),
            staging_table=value['staging_table'],
            method=value.get('method'),
            pre_publication_target_fingerprint=value['pre_publication_target_fingerprint'],
            target_table_format=value.get('target_table_format'),
            iceberg_version=value.get('iceberg_version'),
            phase=phase,
            s3_keys=s3_keys,
            expected_row_count=value.get('expected_row_count'),
            expected_row_fingerprint=value.get('expected_row_fingerprint'),
            query_id=value.get('query_id'),
            recovery_identity=dict(value.get('recovery_identity') or {}),
            context=payload.as_context(),
            finalization=value.get('finalization'),
            is_recovery=True,
        )
        attempt.validate_table_format_contract()
        if phase in (PHASE_STAGED, PHASE_SUBMITTED, PHASE_PUBLISHED, PHASE_FINALIZED):
            attempt.require_staging_evidence()
        validate_phase_finalization_actions(attempt)
        return attempt


@dataclass(frozen=True)
class IcebergTargetAttemptPointer:
    """Target-keyed reference to one canonical FastSync recovery manifest."""

    target: SnowflakeObjectName
    stream_fingerprint: str
    recovery_fingerprint: str
    kind: str
    state: str

    @classmethod
    def from_attempt(cls, attempt: IcebergPublicationAttempt, state: str) -> 'IcebergTargetAttemptPointer':
        """Build the target reference for a FastSync attempt."""
        validate_recovery_identity(attempt.recovery_identity)
        if attempt.recovery_identity['scope'] != 'fastsync':
            raise RecoveryManifestError('Only FastSync attempts can use the target attempt pointer')
        pointer = cls(
            target=attempt.target,
            stream_fingerprint=attempt.recovery_identity['stream_fingerprint'],
            recovery_fingerprint=attempt.recovery_identity['fingerprint'],
            kind=attempt.kind,
            state=state,
        )
        pointer.validate()
        return pointer

    def validate(self) -> None:
        """Reject malformed target references before recovery decisions."""
        if (
            not _is_sha256(self.stream_fingerprint)
            or not _is_sha256(self.recovery_fingerprint)
            or self.kind not in ('full', 'partial')
            or self.state not in _VALID_TARGET_ATTEMPT_STATES
        ):
            raise RecoveryManifestError('Iceberg FastSync target attempt pointer is invalid')

    def as_dict(self) -> Dict[str, Any]:
        """Return the durable target reference."""
        return {
            'pointer_version': 1,
            'target': self.target.key,
            'target_parts': [self.target.database, self.target.schema, self.target.table],
            'stream_fingerprint': self.stream_fingerprint,
            'recovery_fingerprint': self.recovery_fingerprint,
            'kind': self.kind,
            'state': self.state,
        }

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> 'IcebergTargetAttemptPointer':
        """Validate and rebuild a durable target reference."""
        expected_keys = {
            'pointer_version', 'target', 'target_parts', 'stream_fingerprint',
            'recovery_fingerprint', 'kind', 'state',
        }
        if not isinstance(value, dict) or set(value) != expected_keys:
            raise RecoveryManifestError('Iceberg FastSync target attempt pointer is invalid')
        target_parts = value.get('target_parts')
        if value.get('pointer_version') != 1 or not isinstance(target_parts, list) or len(target_parts) != 3:
            raise RecoveryManifestError('Iceberg FastSync target attempt pointer is invalid')
        target = SnowflakeObjectName(*target_parts)
        if value.get('target') != target.key:
            raise RecoveryManifestError('Iceberg FastSync target attempt pointer target is invalid')
        pointer = cls(
            target=target,
            stream_fingerprint=value.get('stream_fingerprint'),
            recovery_fingerprint=value.get('recovery_fingerprint'),
            kind=value.get('kind'),
            state=value.get('state'),
        )
        pointer.validate()
        return pointer


class IcebergRecoveryStore:
    """Atomically persist one locked recovery manifest per target table."""

    def __init__(
        self,
        runtime_dir: str,
        target: SnowflakeObjectName,
        recovery_key: Optional[str] = None,
    ):
        manifest_identity = json.dumps(
            (
                ['stream', recovery_key]
                if recovery_key is not None
                else ['target', target.database, target.schema, target.table]
            ),
            ensure_ascii=False,
            separators=(',', ':'),
        )
        digest = sha256(manifest_identity.encode('utf-8')).hexdigest()[:24]
        self.path = os.path.join(os.path.realpath(runtime_dir), f'iceberg-recovery-{digest}.json')
        self.lock_path = f'{self.path}.lock'
        self.fastsync_target_pointer_path = None
        if recovery_key is None:
            self.fastsync_target_pointer_path = os.path.join(
                os.path.realpath(runtime_dir), f'iceberg-fastsync-target-{digest}.json'
            )
        self._lock_file = None
        self._lock_depth = 0

    @contextmanager
    def locked(self) -> Iterator[None]:
        """Hold a reentrant process lock for a complete manifest lifecycle step."""
        if self._lock_depth:
            self._lock_depth += 1
            try:
                yield
            finally:
                self._lock_depth -= 1
            return

        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.lock_path, 'a', encoding='utf-8') as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            self._lock_file = lock_file
            self._lock_depth = 1
            try:
                yield
            finally:
                self._lock_depth = 0
                self._lock_file = None
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _load(self) -> Optional[IcebergPublicationAttempt]:
        if not os.path.exists(self.path):
            return None
        try:
            with open(self.path, encoding='utf-8') as manifest_file:
                return IcebergPublicationAttempt.from_dict(json.load(manifest_file))
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise RecoveryManifestError(f'Cannot read Iceberg recovery manifest {self.path}') from exc

    def load_locked(self) -> Optional[IcebergPublicationAttempt]:
        """Load while a recovery coordinator owns the target lifecycle lock."""
        return self._load()

    def load(self) -> Optional[IcebergPublicationAttempt]:
        """Load and validate the current manifest under the table lock."""
        with self.locked():
            return self._load()

    def save(self, attempt: IcebergPublicationAttempt) -> None:
        """Atomically persist an attempt under the table lock."""
        with self.locked():
            utils.save_dict_to_json(self.path, attempt.as_dict())

    def _delete(self, attempt_id: Optional[str] = None) -> None:
        current = self._load()
        if current is None:
            return
        if attempt_id is not None and current.attempt_id != attempt_id:
            raise RecoveryManifestError('Iceberg recovery attempt changed before cleanup')
        os.remove(self.path)
        utils._fsync_directory(os.path.dirname(self.path))  # pylint: disable=protected-access

    def delete_locked(self, attempt_id: Optional[str] = None) -> None:
        """Delete while a recovery coordinator owns the target lifecycle lock."""
        self._delete(attempt_id)

    def delete(self, attempt_id: Optional[str] = None) -> None:
        """Delete only the expected attempt under the table lock."""
        with self.locked():
            self._delete(attempt_id)

    def _require_target_store(self) -> str:
        if self.fastsync_target_pointer_path is None:
            raise RecoveryManifestError('FastSync target attempt pointers require a target-keyed recovery store')
        return self.fastsync_target_pointer_path

    def _load_fastsync_target_pointer(self) -> Optional[IcebergTargetAttemptPointer]:
        pointer_path = self._require_target_store()
        if not os.path.exists(pointer_path):
            return None
        try:
            with open(pointer_path, encoding='utf-8') as pointer_file:
                return IcebergTargetAttemptPointer.from_dict(json.load(pointer_file))
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise RecoveryManifestError(f'Cannot read Iceberg FastSync target attempt pointer {pointer_path}') from exc

    def load_fastsync_target_pointer(self) -> Optional[IcebergTargetAttemptPointer]:
        """Load the FastSync target reference under this target's lock."""
        with self.locked():
            return self._load_fastsync_target_pointer()

    def save_fastsync_target_pointer(self, pointer: IcebergTargetAttemptPointer) -> None:
        """Atomically persist the FastSync target reference under the target lock."""
        pointer_path = self._require_target_store()
        pointer.validate()
        with self.locked():
            utils.save_dict_to_json(pointer_path, pointer.as_dict())

    def delete_fastsync_target_pointer(
        self,
        stream_fingerprint: Optional[str] = None,
    ) -> None:
        """Delete only the expected FastSync target reference under the target lock."""
        pointer_path = self._require_target_store()
        with self.locked():
            current = self._load_fastsync_target_pointer()
            if current is None:
                return
            if stream_fingerprint is not None and current.stream_fingerprint != stream_fingerprint:
                raise RecoveryManifestError(
                    'Iceberg FastSync target attempt changed before cleanup'
                )
            os.remove(pointer_path)
            utils._fsync_directory(os.path.dirname(pointer_path))  # pylint: disable=protected-access


@dataclass(frozen=True)
class RecoveryOutcome:
    """The only safe next action for a recovered attempt."""

    action: str
    attempt: IcebergPublicationAttempt
