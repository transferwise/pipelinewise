"""Supported managed Iceberg versions and durable identity construction."""

from dataclasses import dataclass
from hashlib import sha256
import json
import re
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    TableCompatibilityError,
    TableFormatDiscoveryError,
)
from pipelinewise.fastsync.commons.snowflake_types import (
    SNOWFLAKE_MAX_VARCHAR,
    SNOWFLAKE_MAX_VARCHAR_LENGTH,
)


ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ICEBERG_MERGE_ON_READ_BEHAVIOR'
_ICEBERG_STRING_TYPES = {
    'TEXT',
    'STRING',
    'VARCHAR',
    'CHAR',
    'CHARACTER',
    'CHARACTER VARYING',
}
_V3_LOGICAL_TO_PHYSICAL_TYPES = {
    'binary': 'binary(67108864)',
    'boolean': 'boolean',
    'date': 'date',
    'float': 'double',
    'number': 'number(38,0)',
    'text': 'varchar(134217728)',
    'time': 'time(6)',
    'timestamp_ntz': 'timestamp_ntz(6)',
    'variant': 'variant',
}
_V3_SIMPLE_TYPES = {
    **dict.fromkeys(
        _ICEBERG_STRING_TYPES,
        _V3_LOGICAL_TO_PHYSICAL_TYPES['text'].upper(),
    ),
    'BINARY': _V3_LOGICAL_TO_PHYSICAL_TYPES['binary'].upper(),
    'VARBINARY': _V3_LOGICAL_TO_PHYSICAL_TYPES['binary'].upper(),
    'FLOAT': _V3_LOGICAL_TO_PHYSICAL_TYPES['float'].upper(),
    'FLOAT4': _V3_LOGICAL_TO_PHYSICAL_TYPES['float'].upper(),
    'FLOAT8': _V3_LOGICAL_TO_PHYSICAL_TYPES['float'].upper(),
    'DOUBLE': _V3_LOGICAL_TO_PHYSICAL_TYPES['float'].upper(),
    'DOUBLE PRECISION': _V3_LOGICAL_TO_PHYSICAL_TYPES['float'].upper(),
    'REAL': _V3_LOGICAL_TO_PHYSICAL_TYPES['float'].upper(),
    'BOOLEAN': _V3_LOGICAL_TO_PHYSICAL_TYPES['boolean'].upper(),
    'BOOL': _V3_LOGICAL_TO_PHYSICAL_TYPES['boolean'].upper(),
    'DATE': _V3_LOGICAL_TO_PHYSICAL_TYPES['date'].upper(),
    'TIME': _V3_LOGICAL_TO_PHYSICAL_TYPES['time'].upper(),
    'TIMESTAMP': _V3_LOGICAL_TO_PHYSICAL_TYPES['timestamp_ntz'].upper(),
    'DATETIME': _V3_LOGICAL_TO_PHYSICAL_TYPES['timestamp_ntz'].upper(),
    'TIMESTAMP_NTZ': _V3_LOGICAL_TO_PHYSICAL_TYPES['timestamp_ntz'].upper(),
    'TIMESTAMP_LTZ': 'TIMESTAMP_LTZ(6)',
    'TIMESTAMP_TZ': 'TIMESTAMP_LTZ(6)',
    'VARIANT': _V3_LOGICAL_TO_PHYSICAL_TYPES['variant'].upper(),
    'OBJECT': _V3_LOGICAL_TO_PHYSICAL_TYPES['variant'].upper(),
    'ARRAY': _V3_LOGICAL_TO_PHYSICAL_TYPES['variant'].upper(),
}
_FASTSYNC_BASE_TO_LOGICAL_TYPE = {
    **dict.fromkeys(_ICEBERG_STRING_TYPES, 'text'),
    'BINARY': 'binary',
    'VARBINARY': 'binary',
    'FLOAT': 'float',
    'FLOAT4': 'float',
    'FLOAT8': 'float',
    'DOUBLE': 'float',
    'DOUBLE PRECISION': 'float',
    'REAL': 'float',
    'BOOLEAN': 'boolean',
    'BOOL': 'boolean',
    'DATE': 'date',
    'TIME': 'time',
    'TIMESTAMP': 'timestamp_ntz',
    'DATETIME': 'timestamp_ntz',
    'TIMESTAMP_NTZ': 'timestamp_ntz',
    'VARIANT': 'variant',
    'OBJECT': 'variant',
    'ARRAY': 'variant',
}


def _row_value(row: Dict[str, Any], name: str, default: Any = None) -> Any:
    for key in (name, name.upper(), name.lower()):
        if key in row:
            return row[key]
    if default is not None:
        return default
    raise TableFormatDiscoveryError(
        f"Snowflake metadata did not return '{name}'"
    )


def parse_exact_integer_metadata(value: Any) -> int:
    """Parse Snowflake integer metadata without truncating floats or booleans."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and re.fullmatch(r'[+-]?\d+', value.strip()):
        return int(value.strip())
    raise ValueError(f'Expected exact integer metadata, found {value!r}')


def _canonical_v3_type(data_type: str) -> str:
    if not isinstance(data_type, str) or not data_type.strip():
        raise ValueError('Snowflake Iceberg column type must be a non-empty string')
    normalized = re.sub(r'\s+', ' ', data_type.strip().upper())
    base_type = normalized.split('(', maxsplit=1)[0].strip()
    if base_type in ('NUMBER', 'NUMERIC', 'DECIMAL', 'FIXED'):
        match = re.search(r'\((\d+)\s*,\s*(\d+)\)', normalized)
        return (
            f'NUMBER({match.group(1)},{match.group(2)})'
            if match
            else 'NUMBER(38,0)'
        )
    try:
        return _V3_SIMPLE_TYPES[base_type]
    except KeyError as exc:
        raise ValueError(
            f'Unsupported Snowflake Iceberg column type: {data_type}'
        ) from exc


def _v3_column_identity(row: Dict[str, Any]) -> Tuple[str, str]:
    if not isinstance(row, dict):
        raise TableFormatDiscoveryError(
            'Snowflake returned malformed managed Iceberg column metadata'
        )
    try:
        column_name = _row_value(row, 'column_name')
        data_type = _row_value(row, 'data_type')
    except TableFormatDiscoveryError as exc:
        raise TableFormatDiscoveryError(
            'Snowflake returned malformed managed Iceberg column metadata'
        ) from exc
    if (
        not isinstance(column_name, str)
        or not column_name.strip()
        or not isinstance(data_type, str)
        or not data_type.strip()
    ):
        raise TableFormatDiscoveryError(
            'Snowflake returned malformed managed Iceberg column metadata'
        )
    return column_name, data_type


def _v3_numeric_type(row: Dict[str, Any]) -> str:
    try:
        precision = parse_exact_integer_metadata(
            _row_value(row, 'numeric_precision')
        )
        scale = parse_exact_integer_metadata(
            _row_value(row, 'numeric_scale')
        )
    except (TableFormatDiscoveryError, ValueError) as exc:
        raise TableFormatDiscoveryError(
            'Snowflake returned malformed managed Iceberg numeric metadata'
        ) from exc
    return f'NUMBER({precision},{scale})'


def _v3_column_nullable(row: Dict[str, Any]) -> bool:
    try:
        nullable_value = _row_value(row, 'is_nullable')
    except TableFormatDiscoveryError as exc:
        raise TableFormatDiscoveryError(
            'Snowflake returned malformed managed Iceberg column nullability metadata'
        ) from exc
    if (
        not isinstance(nullable_value, str)
        or nullable_value.strip().upper() not in ('YES', 'NO')
    ):
        raise TableFormatDiscoveryError(
            'Snowflake returned malformed managed Iceberg column nullability metadata'
        )
    return nullable_value.strip().upper() == 'YES'


def _canonical_v3_existing_column(
    row: Dict[str, Any],
) -> Tuple[str, str, bool]:
    column_name, data_type = _v3_column_identity(row)
    base_type = data_type.strip().upper().split('(', maxsplit=1)[0].strip()
    if base_type in _ICEBERG_STRING_TYPES:
        varchar_length = _row_value(
            row,
            'character_maximum_length',
            'missing',
        )
        if (
            isinstance(varchar_length, bool)
            or varchar_length != SNOWFLAKE_MAX_VARCHAR_LENGTH
        ):
            raise TableCompatibilityError(
                f'Iceberg column "{column_name.replace(chr(34), chr(34) * 2)}" '
                f'CHARACTER_MAXIMUM_LENGTH is {varchar_length!r}; expected '
                f'{SNOWFLAKE_MAX_VARCHAR_LENGTH}. Widen to '
                f'{SNOWFLAKE_MAX_VARCHAR} with ALTER ICEBERG TABLE, or recreate '
                'the table before retrying; PipelineWise does not alter existing '
                'column widths automatically'
            )
    if data_type.upper() in ('NUMBER', 'NUMERIC', 'DECIMAL'):
        data_type = _v3_numeric_type(row)
    if data_type.upper() in (
        'TIME',
        'TIMESTAMP',
        'TIMESTAMP_NTZ',
        'TIMESTAMP_LTZ',
        'TIMESTAMP_TZ',
    ):
        precision = _row_value(row, 'datetime_precision')
        try:
            exact_precision = parse_exact_integer_metadata(precision)
        except ValueError:
            exact_precision = None
        if exact_precision != 6:
            raise TableCompatibilityError(
                f'Snowflake Iceberg column {column_name} has unsupported '
                f'{data_type} precision {precision}; expected 6'
            )
        data_type = f'{data_type}(6)'
    return column_name, data_type, _v3_column_nullable(row)


def _validate_v3_parameter_rows(rows, target, spec) -> None:
    parameter = spec.merge_on_read_parameter
    if not isinstance(rows, (list, tuple)) or len(rows) != 1:
        raise TableFormatDiscoveryError(
            f'Snowflake did not return exactly one {parameter} parameter row '
            f'for {target.quoted}; PipelineWise cannot prove its copy-on-write contract'
        )
    row = rows[0]
    if not isinstance(row, dict):
        raise TableFormatDiscoveryError(
            f'Snowflake returned malformed {parameter} metadata for '
            f'{target.quoted}'
        )
    try:
        key = _row_value(row, 'key')
        value = _row_value(row, 'value')
        level = _row_value(row, 'level')
    except TableFormatDiscoveryError as exc:
        raise TableFormatDiscoveryError(
            f'Snowflake returned malformed {parameter} metadata for '
            f'{target.quoted}'
        ) from exc
    if not all(
        isinstance(item, str) and item.strip()
        for item in (key, value, level)
    ):
        raise TableFormatDiscoveryError(
            f'Snowflake returned malformed {parameter} metadata for '
            f'{target.quoted}'
        )
    if key.strip().upper() != parameter:
        raise TableFormatDiscoveryError(
            f'Snowflake returned the wrong parameter metadata for '
            f'{target.quoted}; expected {parameter}'
        )
    if (
        value.strip().upper() != spec.merge_on_read_behavior
        or level.strip().upper() != spec.copy_on_write_level
    ):
        raise TableCompatibilityError(
            f'{target.quoted} must set {parameter} = '
            f"'{spec.merge_on_read_behavior}' explicitly at "
            f'{spec.copy_on_write_level} level before '
            f'PipelineWise can write it; found value {value!r} at level '
            f'{level!r}. PipelineWise does not alter existing tables automatically'
        )


def _valid_spec_identity(spec) -> bool:
    return all((
        isinstance(spec.version, int),
        not isinstance(spec.version, bool),
        spec.version > 0,
        spec.table_format == f'managed_iceberg_v{spec.version}',
    ))


def _valid_mapping_entry(name, value) -> bool:
    return all((
        isinstance(name, str),
        bool(name.strip()),
        isinstance(value, str),
        bool(value.strip()),
    ))


def _valid_type_mapping(spec, required_logical_types) -> bool:
    if (
        set(spec.logical_to_physical_types) != required_logical_types
        or not callable(spec.canonical_type)
        or not all(
            _valid_mapping_entry(name, value)
            for name, value in spec.logical_to_physical_types.items()
        )
    ):
        return False
    try:
        canonical_types = [
            spec.canonical_type(value)
            for value in spec.logical_to_physical_types.values()
        ]
    except (AttributeError, TypeError, ValueError):
        return False
    return all(isinstance(value, str) and value for value in canonical_types)


def _valid_copy_on_write_contract(spec) -> bool:
    return all((
        bool(spec.merge_on_read_parameter),
        bool(spec.merge_on_read_behavior),
        spec.copy_on_write_level == 'TABLE',
        spec.table_options.get(spec.merge_on_read_parameter)
        == spec.merge_on_read_behavior,
    ))


@dataclass(frozen=True)
class ManagedIcebergVersionSpec:  # pylint: disable=too-many-instance-attributes
    """All Snowflake table contracts owned by one managed Iceberg version."""

    version: int
    table_format: str
    logical_to_physical_types: Mapping[str, str]
    table_options: Mapping[str, Any]
    merge_on_read_parameter: str
    merge_on_read_behavior: str
    copy_on_write_level: str
    canonical_type: Callable[[str], str]
    canonical_existing_column: Callable[
        [Dict[str, Any]],
        Tuple[str, str, bool],
    ]
    validate_parameter_rows: Callable[
        [Sequence[Dict[str, Any]], Any, 'ManagedIcebergVersionSpec'],
        None,
    ]

    def __post_init__(self) -> None:
        required_logical_types = {
            'binary',
            'boolean',
            'date',
            'float',
            'number',
            'text',
            'time',
            'timestamp_ntz',
            'variant',
        }
        if not all((
            _valid_spec_identity(self),
            _valid_type_mapping(self, required_logical_types),
            _valid_copy_on_write_contract(self),
            all(callable(hook) for hook in (
                self.canonical_type,
                self.canonical_existing_column,
                self.validate_parameter_rows,
            )),
        )):
            raise ValueError('Managed Iceberg version specification is incomplete')
        object.__setattr__(
            self,
            'logical_to_physical_types',
            MappingProxyType(dict(self.logical_to_physical_types)),
        )
        object.__setattr__(
            self,
            'table_options',
            MappingProxyType(dict(self.table_options)),
        )

    @property
    def table_options_sql(self) -> str:
        """Render semantic table options into deterministic Snowflake SQL."""
        values = []
        for name, value in self.table_options.items():
            if value is True:
                rendered = 'TRUE'
            elif value is False:
                rendered = 'FALSE'
            elif isinstance(value, str):
                rendered = "'" + value.replace("'", "''") + "'"
            else:
                rendered = str(value)
            values.append(f'{name} = {rendered}')
        return ' '.join(values)

    def physical_type_for_logical(self, logical_type: str) -> str:
        """Return one canonical physical type from this version's executable map."""
        try:
            declared_type = self.logical_to_physical_types[logical_type]
        except KeyError as exc:
            raise ValueError(
                f'Unsupported managed Iceberg logical type: {logical_type!r}'
            ) from exc
        return self.canonical_type(declared_type)

    def canonical_fastsync_type(self, data_type: str) -> str:
        """Map a FastSync definition through this version's logical strategy."""
        if not isinstance(data_type, str) or not data_type.strip():
            raise ValueError('FastSync column type must be a non-empty string')
        normalized = re.sub(r'\s+', ' ', data_type.strip().upper())
        base_type = normalized.split('(', maxsplit=1)[0].strip()
        logical_type = _FASTSYNC_BASE_TO_LOGICAL_TYPE.get(base_type)
        if base_type in ('NUMBER', 'NUMERIC', 'DECIMAL', 'FIXED'):
            logical_type = 'number' if '(' not in normalized else None
        if logical_type is None:
            return self.canonical_type(data_type)
        return self.physical_type_for_logical(logical_type)


MANAGED_ICEBERG_V3_SPEC = ManagedIcebergVersionSpec(
    version=3,
    table_format='managed_iceberg_v3',
    logical_to_physical_types=_V3_LOGICAL_TO_PHYSICAL_TYPES,
    table_options={
        'DATA_RETENTION_TIME_IN_DAYS': 1,
        'TARGET_FILE_SIZE': '16MB',
        'ENABLE_DATA_COMPACTION': True,
        ICEBERG_MERGE_ON_READ_BEHAVIOR: 'DISABLED',
    },
    merge_on_read_parameter=ICEBERG_MERGE_ON_READ_BEHAVIOR,
    merge_on_read_behavior='DISABLED',
    copy_on_write_level='TABLE',
    canonical_type=_canonical_v3_type,
    canonical_existing_column=_canonical_v3_existing_column,
    validate_parameter_rows=_validate_v3_parameter_rows,
)


def managed_iceberg_version_registry(
    *specs: ManagedIcebergVersionSpec,
) -> Mapping[int, ManagedIcebergVersionSpec]:
    """Build an immutable registry of complete, uniquely versioned strategies."""
    registry = {}
    for spec in specs:
        if not isinstance(spec, ManagedIcebergVersionSpec):
            raise ValueError(
                'Managed Iceberg version registry entries must be complete specifications'
            )
        if spec.version in registry:
            raise ValueError(
                f'Duplicate managed Iceberg version specification: {spec.version}'
            )
        registry[spec.version] = spec
    return MappingProxyType(registry)


MANAGED_ICEBERG_VERSION_SPECS = managed_iceberg_version_registry(
    MANAGED_ICEBERG_V3_SPEC,
)

# Compatibility aliases keep the existing facade stable while every consumer
# derives its values from the single version registry.
TABLE_FORMAT_MANAGED_ICEBERG_V3 = MANAGED_ICEBERG_V3_SPEC.table_format
MANAGED_ICEBERG_V3_MERGE_ON_READ_BEHAVIOR = (
    MANAGED_ICEBERG_V3_SPEC.merge_on_read_behavior
)
MANAGED_ICEBERG_V3_TABLE_OPTIONS = MANAGED_ICEBERG_V3_SPEC.table_options_sql
SUPPORTED_MANAGED_ICEBERG_TABLE_FORMATS = MappingProxyType({
    version: spec.table_format
    for version, spec in MANAGED_ICEBERG_VERSION_SPECS.items()
})
MANAGED_ICEBERG_TABLE_OPTIONS_BY_VERSION = MappingProxyType({
    version: spec.table_options_sql
    for version, spec in MANAGED_ICEBERG_VERSION_SPECS.items()
})

RECOVERY_IDENTITY_VERSION = 2
TRANSFORMATION_SEMANTICS_VERSION = 1


def is_exact_integer(value: Any) -> bool:
    """Return whether a value is an integer but not a boolean."""
    return isinstance(value, int) and not isinstance(value, bool)


def is_supported_managed_iceberg_version(value: Any) -> bool:
    """Return whether an exact integer version has an implementation."""
    if not is_exact_integer(value):
        return False
    spec = MANAGED_ICEBERG_VERSION_SPECS.get(value)
    return (
        isinstance(spec, ManagedIcebergVersionSpec)
        and spec.version == value
    )


def managed_iceberg_version_spec(value: Any) -> ManagedIcebergVersionSpec:
    """Return the complete implemented contract for an exact version."""
    if not is_supported_managed_iceberg_version(value):
        raise ValueError(f'Unsupported managed Iceberg version: {value!r}')
    return MANAGED_ICEBERG_VERSION_SPECS[value]


def managed_iceberg_spec_for_table_format(
    table_format: str,
) -> ManagedIcebergVersionSpec:
    """Return the one complete strategy matching a discovered table format."""
    matches = [
        spec
        for version, spec in MANAGED_ICEBERG_VERSION_SPECS.items()
        if (
            is_supported_managed_iceberg_version(version)
            and spec.table_format == table_format
        )
    ]
    if len(matches) != 1:
        raise ValueError(
            f'Unsupported managed Iceberg table format: {table_format!r}'
        )
    return matches[0]


def repository_contract() -> Dict[str, Any]:
    """Return the dependency-free cross-connector managed-Iceberg contract."""
    versions = {}
    for version, spec in sorted(MANAGED_ICEBERG_VERSION_SPECS.items()):
        versions[str(version)] = {
            'version': spec.version,
            'physical_format': spec.table_format,
            'logical_to_physical_types': dict(spec.logical_to_physical_types),
            'table_options': dict(spec.table_options),
            'copy_on_write': {
                'parameter': spec.merge_on_read_parameter,
                'value': spec.merge_on_read_behavior,
                'required_metadata_level': spec.copy_on_write_level,
            },
        }
    return {
        'supported_versions': sorted(MANAGED_ICEBERG_VERSION_SPECS),
        'versions': versions,
    }


def _canonical_json_hash(value: Any) -> str:
    try:
        serialized = json.dumps(
            value,
            ensure_ascii=False,
            separators=(',', ':'),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            'Iceberg recovery identity must be JSON serializable'
        ) from exc
    return sha256(serialized.encode('utf-8')).hexdigest()


def build_recovery_identity(
    scope: str,
    identity: Dict[str, Any],
    transformation_config: Optional[Dict[str, Any]] = None,
    stream_identity: Optional[Dict[str, Any]] = None,
    *,
    target_table_format: Optional[str] = None,
    iceberg_version: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a credential-free, versioned identity for durable recovery."""
    if scope not in ('fastsync', 'manual_conversion'):
        raise ValueError('Iceberg recovery identity scope is invalid')
    if not isinstance(identity, dict):
        raise ValueError('Iceberg recovery identity input must be a dictionary')

    fingerprint_input = {
        'identity_version': RECOVERY_IDENTITY_VERSION,
        'scope': scope,
        'identity': identity,
    }
    result = {
        'identity_version': RECOVERY_IDENTITY_VERSION,
        'scope': scope,
    }
    if scope == 'fastsync':
        if not isinstance(stream_identity, dict) or not stream_identity:
            raise ValueError(
                'FastSync recovery requires a stable stream identity'
            )
        if (
            target_table_format != 'iceberg'
            or not is_supported_managed_iceberg_version(iceberg_version)
        ):
            raise ValueError(
                'FastSync recovery requires a supported managed Iceberg table format'
            )
        stream_fingerprint = _canonical_json_hash(stream_identity)
        fingerprint_input.update({
            'stream_fingerprint': stream_fingerprint,
            'target_table_format': target_table_format,
            'iceberg_version': iceberg_version,
        })
        result.update({
            'stream_fingerprint': stream_fingerprint,
            'target_table_format': target_table_format,
            'iceberg_version': iceberg_version,
        })
    elif any(
        value is not None
        for value in (stream_identity, target_table_format, iceberg_version)
    ):
        raise ValueError(
            'Manual conversion does not accept a stream or table format identity'
        )
    if transformation_config is not None:
        if not isinstance(transformation_config, dict):
            raise ValueError(
                'FastSync transformation configuration must be a dictionary'
            )
        transformation_fingerprint = _canonical_json_hash(
            transformation_config
        )
        transformation = {
            'semantics_version': TRANSFORMATION_SEMANTICS_VERSION,
            'fingerprint': transformation_fingerprint,
        }
        fingerprint_input['transformation'] = transformation
        result.update({
            'transformation_semantics_version': TRANSFORMATION_SEMANTICS_VERSION,
            'transformation_fingerprint': transformation_fingerprint,
        })
    result['fingerprint'] = _canonical_json_hash(fingerprint_input)
    return result
