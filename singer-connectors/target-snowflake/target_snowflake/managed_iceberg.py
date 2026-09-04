"""Managed-Iceberg table contracts for target-snowflake."""

from dataclasses import dataclass
import re
from types import MappingProxyType
from typing import Any, Callable, Dict, FrozenSet, Mapping, Optional, Sequence, Tuple

from target_snowflake.exceptions import (
    TableFormatDiscoveryException,
    TableFormatMismatchException,
)


TABLE_FORMAT_MISSING = 'missing'
TABLE_FORMAT_NATIVE = 'native'
TABLE_FORMAT_MANAGED_ICEBERG_V3 = 'managed_iceberg_v3'
TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG = 'unsupported_external_iceberg'

ICEBERG_MERGE_ON_READ_PARAMETER = 'ICEBERG_MERGE_ON_READ_BEHAVIOR'
ICEBERG_COPY_ON_WRITE_VALUE = 'DISABLED'
SNOWFLAKE_MAX_VARCHAR_LENGTH = 134217728
SNOWFLAKE_MAX_VARCHAR_TYPE = f'varchar({SNOWFLAKE_MAX_VARCHAR_LENGTH})'
SNOWFLAKE_MAX_BINARY_LENGTH = 67108864
SNOWFLAKE_MAX_BINARY_TYPE = f'binary({SNOWFLAKE_MAX_BINARY_LENGTH})'
SNOWFLAKE_STRING_TYPES = frozenset({
    'CHAR',
    'CHARACTER',
    'CHARACTER VARYING',
    'STRING',
    'TEXT',
    'VARCHAR',
})
SNOWFLAKE_TIMESTAMP_TYPES = frozenset({
    'TIMESTAMP_LTZ',
    'TIMESTAMP_NTZ',
    'TIMESTAMP_TZ',
})

_V3_SIMPLE_TYPES = {
    **dict.fromkeys(SNOWFLAKE_STRING_TYPES, SNOWFLAKE_MAX_VARCHAR_TYPE.upper()),
    'BINARY': SNOWFLAKE_MAX_BINARY_TYPE.upper(),
    'VARBINARY': SNOWFLAKE_MAX_BINARY_TYPE.upper(),
    'FLOAT': 'DOUBLE',
    'FLOAT4': 'DOUBLE',
    'FLOAT8': 'DOUBLE',
    'DOUBLE': 'DOUBLE',
    'DOUBLE PRECISION': 'DOUBLE',
    'REAL': 'DOUBLE',
    'BOOLEAN': 'BOOLEAN',
    'BOOL': 'BOOLEAN',
    'DATE': 'DATE',
    'TIME': 'TIME(6)',
    'TIMESTAMP': 'TIMESTAMP_NTZ(6)',
    'DATETIME': 'TIMESTAMP_NTZ(6)',
    'TIMESTAMP_NTZ': 'TIMESTAMP_NTZ(6)',
    'TIMESTAMP_LTZ': 'TIMESTAMP_LTZ(6)',
    'TIMESTAMP_TZ': 'TIMESTAMP_LTZ(6)',
    'VARIANT': 'VARIANT',
    'OBJECT': 'VARIANT',
    'ARRAY': 'VARIANT',
}

_LOGICAL_COLUMN_TYPES = frozenset({
    'binary',
    'boolean',
    'date',
    'float',
    'number',
    'text',
    'time',
    'timestamp_ntz',
    'variant',
})

Query = Callable[..., Sequence[Dict[str, Any]]]
ExistingTableValidator = Callable[[Query, str, str, str, str], None]
MismatchReason = Callable[[str], str]
CanonicalType = Callable[[str], str]
CanonicalExistingColumn = Callable[[Dict[str, Any], str], Tuple[str, str, bool]]


@dataclass(frozen=True)
class ColumnTypeCompatibility:
    """Version-specific compatibility rules for existing target columns."""

    compatible_pairs: FrozenSet[Tuple[str, str]]
    forbidden_pairs: FrozenSet[FrozenSet[str]]
    mismatch_reason: MismatchReason

    def __post_init__(self):
        if not callable(self.mismatch_reason):
            raise ValueError('Column compatibility requires a mismatch-reason function')
        try:
            compatible_pairs = frozenset(
                tuple(pair) for pair in self.compatible_pairs
            )
            forbidden_pairs = frozenset(
                frozenset(pair) for pair in self.forbidden_pairs
            )
        except TypeError as exc:
            raise ValueError('Column compatibility pairs must be iterable') from exc
        if any(
            len(pair) != 2
            or any(
                not isinstance(value, str)
                or not value.strip()
                or value != value.upper()
                for value in pair
            )
            for pair in (*compatible_pairs, *forbidden_pairs)
        ):
            raise ValueError(
                'Column compatibility pairs must contain two uppercase type names'
            )
        object.__setattr__(self, 'compatible_pairs', compatible_pairs)
        object.__setattr__(self, 'forbidden_pairs', forbidden_pairs)


@dataclass(frozen=True)
class ManagedIcebergContract:  # pylint: disable=too-many-instance-attributes
    """One complete target-snowflake implementation of a managed version."""

    version: int
    table_format: str
    column_types: Mapping[str, str]
    table_option_semantics: Mapping[str, Any]
    copy_on_write_level: str
    canonical_type: CanonicalType
    canonical_existing_column: CanonicalExistingColumn
    existing_table_validator: ExistingTableValidator
    type_compatibility: ColumnTypeCompatibility

    def __post_init__(self):
        if (
            not isinstance(self.version, int)
            or isinstance(self.version, bool)
            or self.version <= 0
        ):
            raise ValueError('Managed Iceberg contract version must be a positive exact integer')
        if not isinstance(self.table_format, str) or not self.table_format:
            raise ValueError('Managed Iceberg contract requires a table format')
        expected_table_format = f'managed_iceberg_v{self.version}'
        if self.table_format != expected_table_format:
            raise ValueError(
                'Managed Iceberg contract table format must identify its exact version: '
                f'{expected_table_format}'
            )
        if set(self.column_types) != _LOGICAL_COLUMN_TYPES:
            raise ValueError(
                'Managed Iceberg contract must map every logical column type exactly once'
            )
        if any(
            not isinstance(name, str)
            or not name.strip()
            or not isinstance(value, str)
            or not value.strip()
            for name, value in self.column_types.items()
        ):
            raise ValueError(
                'Managed Iceberg contract column mappings must be non-empty strings'
            )
        if not self.table_option_semantics:
            raise ValueError('Managed Iceberg contract requires table option semantics')
        if (
            self.table_option_semantics.get(ICEBERG_MERGE_ON_READ_PARAMETER)
            != ICEBERG_COPY_ON_WRITE_VALUE
            or self.copy_on_write_level != 'TABLE'
        ):
            raise ValueError(
                'Managed Iceberg contract requires table-level copy-on-write semantics'
            )
        if not all(callable(hook) for hook in (
            self.canonical_type,
            self.canonical_existing_column,
            self.existing_table_validator,
        )):
            raise ValueError('Managed Iceberg contract requires complete validation hooks')
        try:
            canonical_types = [
                self.canonical_type(value)
                for value in self.column_types.values()
            ]
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError(
                'Managed Iceberg contract column mappings must be executable'
            ) from exc
        if any(
            not isinstance(value, str) or not value
            for value in canonical_types
        ):
            raise ValueError(
                'Managed Iceberg contract column mappings must be executable'
            )
        if not isinstance(self.type_compatibility, ColumnTypeCompatibility):
            raise ValueError('Managed Iceberg contract requires column compatibility rules')
        object.__setattr__(self, 'column_types', MappingProxyType(dict(self.column_types)))
        object.__setattr__(
            self,
            'table_option_semantics',
            MappingProxyType(dict(self.table_option_semantics)),
        )

    @property
    def table_options(self):
        """Return deterministic CREATE TABLE option syntax."""
        return ' '.join(
            f'{name}={_snowflake_option_literal(value)}'
            for name, value in self.table_option_semantics.items()
        )

    def repository_contract(self):
        """Return the dependency-free cross-layer parity representation."""
        return {
            'version': self.version,
            'physical_format': self.table_format,
            'logical_to_physical_types': dict(self.column_types),
            'table_options': dict(self.table_option_semantics),
            'copy_on_write': {
                'parameter': ICEBERG_MERGE_ON_READ_PARAMETER,
                'value': self.table_option_semantics[ICEBERG_MERGE_ON_READ_PARAMETER],
                'required_metadata_level': self.copy_on_write_level,
            },
        }


@dataclass(frozen=True)
class RequestedTableFormat:
    """Validated target format requested by one connector invocation."""

    requested_format: str
    expected_physical_format: str
    iceberg_contract: Optional[ManagedIcebergContract]
    config_source: str

    @property
    def is_iceberg(self):
        """Return whether the request selects managed Iceberg."""
        return self.iceberg_contract is not None

    @property
    def iceberg_version(self):
        """Return the selected managed version, if any."""
        return self.iceberg_contract.version if self.iceberg_contract else None


@dataclass(frozen=True)
class ColumnChangePlan:
    """Pure schema-evolution plan consumed by DbSync."""

    additions: Tuple[str, ...]
    replacements: Tuple[Tuple[str, str], ...]


def _quote_identifier(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _snowflake_option_literal(value):
    if isinstance(value, bool):
        return str(value).upper()
    if isinstance(value, str):
        return sql_string_literal(value)
    return str(value)


def sql_string_literal(value):
    """Quote one Snowflake string literal."""
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"


def _base_snowflake_type(value):
    return re.sub(r'\(.*\)', '', value).strip()


_MISSING = object()


def _row_value(row, name, default=_MISSING):
    """Return a SHOW result value without assuming DictCursor key casing."""
    for key in (name, name.upper(), name.lower()):
        if key in row:
            return row[key]
    if default is not _MISSING:
        return default
    raise TableFormatDiscoveryException(f"Snowflake metadata did not return '{name}'")


def _snowflake_boolean(value, field_name):
    if value is True or str(value).upper() in ('Y', 'YES', 'TRUE'):
        return True
    if value is False or str(value).upper() in ('N', 'NO', 'FALSE'):
        return False
    raise TableFormatDiscoveryException(
        f"Snowflake metadata returned invalid '{field_name}' value: {value!r}"
    )


def parse_exact_integer_metadata(value):
    """Parse Snowflake integer metadata without truncating floats or booleans."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and re.fullmatch(r'[+-]?\d+', value.strip()):
        return int(value.strip())
    raise ValueError(f'Expected exact integer metadata, found {value!r}')


def _exact_name_rows(rows, name):
    return [row for row in rows if _row_value(row, 'name') == name]


def validate_managed_iceberg_v3_copy_on_write(parameter_rows, table_fqtn):
    """Require an explicit table-level copy-on-write setting for managed v3."""
    migration_instruction = (
        f'Run ALTER ICEBERG TABLE {table_fqtn} SET '
        f"{ICEBERG_MERGE_ON_READ_PARAMETER} = '{ICEBERG_COPY_ON_WRITE_VALUE}' before retrying. "
        'PipelineWise does not alter existing table settings automatically.'
    )
    if not isinstance(parameter_rows, (list, tuple)) or len(parameter_rows) != 1:
        raise TableFormatDiscoveryException(
            f'Snowflake did not return exactly one {ICEBERG_MERGE_ON_READ_PARAMETER} '
            f'row for managed Iceberg v3 table {table_fqtn}. {migration_instruction}'
        )

    if not isinstance(parameter_rows[0], dict):
        raise TableFormatDiscoveryException(
            f'Snowflake returned malformed {ICEBERG_MERGE_ON_READ_PARAMETER} metadata '
            f'for managed Iceberg v3 table {table_fqtn}. {migration_instruction}'
        )

    try:
        key = _row_value(parameter_rows[0], 'key')
        value = _row_value(parameter_rows[0], 'value')
        level = _row_value(parameter_rows[0], 'level')
    except TableFormatDiscoveryException as exc:
        raise TableFormatDiscoveryException(
            f'Snowflake returned malformed {ICEBERG_MERGE_ON_READ_PARAMETER} metadata '
            f'for managed Iceberg v3 table {table_fqtn}. {migration_instruction}'
        ) from exc

    if not all(
        isinstance(item, str) and item.strip()
        for item in (key, value, level)
    ):
        raise TableFormatDiscoveryException(
            f'Snowflake returned malformed {ICEBERG_MERGE_ON_READ_PARAMETER} metadata '
            f'for managed Iceberg v3 table {table_fqtn}. {migration_instruction}'
        )
    normalized_key = key.strip().upper()
    normalized_value = value.strip().upper()
    normalized_level = level.strip().upper()
    if (
        normalized_key != ICEBERG_MERGE_ON_READ_PARAMETER
        or normalized_value != ICEBERG_COPY_ON_WRITE_VALUE
        or normalized_level != 'TABLE'
    ):
        raise TableFormatDiscoveryException(
            f'Managed Iceberg v3 table {table_fqtn} has '
            f'{ICEBERG_MERGE_ON_READ_PARAMETER} value {value!r} at level {level!r}; '
            f"PipelineWise requires value '{ICEBERG_COPY_ON_WRITE_VALUE}' at level 'TABLE'. "
            f'{migration_instruction}'
        )


def _canonical_managed_iceberg_v3_type(data_type):
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


def _v3_column_identity(row, table_fqtn):
    if not isinstance(row, dict):
        raise TableFormatDiscoveryException(
            f'Snowflake returned malformed column metadata for managed '
            f'Iceberg v3 table {table_fqtn}'
        )
    try:
        column_name = _row_value(row, 'column_name')
        data_type = _row_value(row, 'data_type')
    except TableFormatDiscoveryException as exc:
        raise TableFormatDiscoveryException(
            f'Snowflake returned malformed column metadata for managed '
            f'Iceberg v3 table {table_fqtn}'
        ) from exc
    if (
        not isinstance(column_name, str)
        or not column_name.strip()
        or not isinstance(data_type, str)
        or not data_type.strip()
    ):
        raise TableFormatDiscoveryException(
            f'Snowflake returned malformed column metadata for managed '
            f'Iceberg v3 table {table_fqtn}'
        )
    return column_name, data_type


def _v3_numeric_type(row, table_fqtn):
    try:
        precision = parse_exact_integer_metadata(
            _row_value(row, 'numeric_precision')
        )
        scale = parse_exact_integer_metadata(
            _row_value(row, 'numeric_scale')
        )
    except (TableFormatDiscoveryException, ValueError) as exc:
        raise TableFormatDiscoveryException(
            f'Snowflake returned malformed numeric metadata for managed '
            f'Iceberg v3 table {table_fqtn}'
        ) from exc
    return f'NUMBER({precision},{scale})'


def _v3_column_nullable(row, table_fqtn):
    try:
        nullable_value = _row_value(row, 'is_nullable')
    except TableFormatDiscoveryException as exc:
        raise TableFormatDiscoveryException(
            f'Snowflake returned malformed nullability metadata for managed '
            f'Iceberg v3 table {table_fqtn}'
        ) from exc
    if (
        not isinstance(nullable_value, str)
        or nullable_value.strip().upper() not in ('YES', 'NO')
    ):
        raise TableFormatDiscoveryException(
            f'Snowflake returned malformed nullability metadata for managed '
            f'Iceberg v3 table {table_fqtn}'
        )
    return nullable_value.strip().upper() == 'YES'


def _canonical_v3_existing_column(row, table_fqtn):
    """Return one validated, canonical managed-v3 column metadata tuple."""
    column_name, data_type = _v3_column_identity(row, table_fqtn)

    base_type = data_type.strip().upper().split('(', maxsplit=1)[0].strip()
    if base_type in SNOWFLAKE_STRING_TYPES:
        varchar_length = _row_value(
            row,
            'character_maximum_length',
            'missing',
        )
        if (
            isinstance(varchar_length, bool)
            or varchar_length != SNOWFLAKE_MAX_VARCHAR_LENGTH
        ):
            raise TableFormatMismatchException(
                f'Managed Iceberg v3 column {_quote_identifier(column_name)} in '
                f'{table_fqtn} has CHARACTER_MAXIMUM_LENGTH {varchar_length!r}; '
                f'expected {SNOWFLAKE_MAX_VARCHAR_LENGTH}. Widen the column to '
                f'{SNOWFLAKE_MAX_VARCHAR_TYPE.upper()} with ALTER ICEBERG TABLE, or '
                f'recreate the table before retrying. PipelineWise does not alter '
                f'existing column widths automatically'
            )
    if data_type.upper() in ('NUMBER', 'NUMERIC', 'DECIMAL'):
        data_type = _v3_numeric_type(row, table_fqtn)
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
            raise TableFormatMismatchException(
                f'Managed Iceberg v3 column {_quote_identifier(column_name)} in '
                f'{table_fqtn} has unsupported {data_type} precision '
                f'{precision}; expected 6'
            )
        data_type = f'{data_type}(6)'
    try:
        canonical_type = _canonical_managed_iceberg_v3_type(data_type)
    except ValueError as exc:
        raise TableFormatMismatchException(
            f'Managed Iceberg v3 column {_quote_identifier(column_name)} in '
            f'{table_fqtn} has unsupported type {data_type}'
        ) from exc
    return column_name, canonical_type, _v3_column_nullable(row, table_fqtn)


def validate_managed_iceberg_v3_columns(column_rows, table_fqtn):
    """Require canonical managed-v3 column metadata for an existing table."""
    if not isinstance(column_rows, (list, tuple)) or not column_rows:
        raise TableFormatDiscoveryException(
            f'Snowflake did not return column metadata for managed Iceberg v3 '
            f'table {table_fqtn}'
        )
    for row in column_rows:
        _canonical_v3_existing_column(row, table_fqtn)


def _validate_managed_iceberg_v3_table(
    query,
    database_name,
    schema_name,
    table_name,
    table_fqtn,
):
    validate_managed_iceberg_v3_copy_on_write(
        query(
            f"SHOW PARAMETERS LIKE '{ICEBERG_MERGE_ON_READ_PARAMETER}' "
            f'IN TABLE {table_fqtn}'
        ),
        table_fqtn,
    )
    column_rows = query(
        'SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, '
        'DATETIME_PRECISION, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE '
        f'FROM {_quote_identifier(database_name)}.INFORMATION_SCHEMA.COLUMNS '
        'WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s '
        'ORDER BY ORDINAL_POSITION',
        params={'schema': schema_name, 'table': table_name},
    )
    validate_managed_iceberg_v3_columns(column_rows, table_fqtn)


def _managed_iceberg_v3_mismatch_reason(current_type):
    if current_type == 'TEXT':
        return (
            'the explicit Iceberg v3 mapping requires VARIANT; '
            'migrate the column explicitly before replication'
        )
    if current_type == 'VARIANT':
        return (
            f'the current Singer schema requires {SNOWFLAKE_MAX_VARCHAR_TYPE.upper()}; '
            'migrate the column explicitly before replication'
        )
    raise ValueError(
        f'Unsupported Iceberg TEXT/VARIANT mismatch: {current_type}, version 3'
    )


_MANAGED_ICEBERG_V3_CONTRACT = ManagedIcebergContract(
    version=3,
    table_format=TABLE_FORMAT_MANAGED_ICEBERG_V3,
    column_types={
        'binary': SNOWFLAKE_MAX_BINARY_TYPE,
        'boolean': 'boolean',
        'date': 'date',
        'float': 'double',
        'number': 'number(38,0)',
        'text': SNOWFLAKE_MAX_VARCHAR_TYPE,
        'time': 'time(6)',
        'timestamp_ntz': 'timestamp_ntz(6)',
        'variant': 'variant',
    },
    table_option_semantics={
        'DATA_RETENTION_TIME_IN_DAYS': 1,
        'TARGET_FILE_SIZE': 'AUTO',
        'STORAGE_SERIALIZATION_POLICY': 'COMPATIBLE',
        'ENABLE_DATA_COMPACTION': True,
        ICEBERG_MERGE_ON_READ_PARAMETER: ICEBERG_COPY_ON_WRITE_VALUE,
    },
    copy_on_write_level='TABLE',
    canonical_type=_canonical_managed_iceberg_v3_type,
    canonical_existing_column=_canonical_v3_existing_column,
    existing_table_validator=_validate_managed_iceberg_v3_table,
    type_compatibility=ColumnTypeCompatibility(
        compatible_pairs=frozenset({('FLOAT', 'DOUBLE')}),
        forbidden_pairs=frozenset({
            frozenset({'TEXT', 'VARIANT'}),
            frozenset({'VARCHAR', 'VARIANT'}),
        }),
        mismatch_reason=_managed_iceberg_v3_mismatch_reason,
    ),
)


def managed_iceberg_contract_registry(*contracts):
    """Build an immutable registry of complete, uniquely versioned contracts."""
    registry = {}
    for contract in contracts:
        if not isinstance(contract, ManagedIcebergContract):
            raise ValueError(
                'Managed Iceberg registry entries must be complete contracts'
            )
        if contract.version in registry:
            raise ValueError(
                f'Duplicate managed Iceberg contract version: {contract.version}'
            )
        registry[contract.version] = contract
    return MappingProxyType(registry)


SUPPORTED_MANAGED_ICEBERG_CONTRACTS = managed_iceberg_contract_registry(
    _MANAGED_ICEBERG_V3_CONTRACT,
)
SUPPORTED_MANAGED_ICEBERG_FORMATS = MappingProxyType({
    version: contract.table_format
    for version, contract in SUPPORTED_MANAGED_ICEBERG_CONTRACTS.items()
})
ICEBERG_TABLE_FORMATS = frozenset({
    *SUPPORTED_MANAGED_ICEBERG_FORMATS.values(),
    TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG,
})


def repository_contract():
    """Return the connector's repository-wide managed-Iceberg contract."""
    return {
        'supported_versions': sorted(SUPPORTED_MANAGED_ICEBERG_CONTRACTS),
        'versions': {
            str(version): contract.repository_contract()
            for version, contract in SUPPORTED_MANAGED_ICEBERG_CONTRACTS.items()
        },
    }


def is_supported_iceberg_version(iceberg_version):
    """Return whether a value resolves to one complete version contract."""
    if not isinstance(iceberg_version, int) or isinstance(iceberg_version, bool):
        return False
    contract = SUPPORTED_MANAGED_ICEBERG_CONTRACTS.get(iceberg_version)
    return (
        isinstance(contract, ManagedIcebergContract)
        and contract.version == iceberg_version
    )


def get_managed_iceberg_contract(iceberg_version):
    """Return a complete managed-version strategy."""
    if not is_supported_iceberg_version(iceberg_version):
        raise ValueError(f'Unsupported managed Iceberg version: {iceberg_version!r}')
    return SUPPORTED_MANAGED_ICEBERG_CONTRACTS[iceberg_version]


def requested_table_format(config):
    """Return the validated native or managed format requested by config."""
    requested_format = config.get('target_table_format', TABLE_FORMAT_NATIVE)
    config_source = (
        'target_table_format'
        if 'target_table_format' in config
        else 'the default target format'
    )
    if requested_format == 'iceberg':
        contract = get_managed_iceberg_contract(config.get('iceberg_version'))
        return RequestedTableFormat(
            requested_format=requested_format,
            expected_physical_format=contract.table_format,
            iceberg_contract=contract,
            config_source=config_source,
        )
    return RequestedTableFormat(
        requested_format=TABLE_FORMAT_NATIVE,
        expected_physical_format=TABLE_FORMAT_NATIVE,
        iceberg_contract=None,
        config_source=config_source,
    )


def managed_iceberg_contract(parameter_rows, table_fqtn):
    """Resolve Snowflake ICEBERG_VERSION metadata to a complete contract."""
    version_rows = [
        row for row in parameter_rows
        if str(_row_value(row, 'key')).upper() == 'ICEBERG_VERSION'
    ]
    if len(version_rows) != 1:
        raise TableFormatDiscoveryException(
            f'Snowflake did not return one ICEBERG_VERSION for {table_fqtn}'
        )
    try:
        iceberg_version = parse_exact_integer_metadata(
            _row_value(version_rows[0], 'value')
        )
    except (TypeError, ValueError) as exc:
        raise TableFormatDiscoveryException(
            f'Snowflake returned an invalid ICEBERG_VERSION for {table_fqtn}'
        ) from exc

    try:
        return get_managed_iceberg_contract(iceberg_version)
    except ValueError as exc:
        raise TableFormatDiscoveryException(
            f'Snowflake returned unsupported ICEBERG_VERSION {iceberg_version} for {table_fqtn}'
        ) from exc


def managed_iceberg_format(parameter_rows, table_fqtn):
    """Return the supported managed format reported by Snowflake metadata."""
    return managed_iceberg_contract(parameter_rows, table_fqtn).table_format


def discover_table_format(query, database_name, schema_name, table_name):
    """Discover and validate the exact physical format of a target table."""
    database_name = database_name.upper()
    schema_name = schema_name.upper()
    table_name = table_name.upper()
    schema_fqtn = '.'.join(
        _quote_identifier(identifier) for identifier in (database_name, schema_name)
    )
    table_literal = sql_string_literal(table_name)

    exact_tables = _exact_name_rows(
        query(f'SHOW TABLES IN SCHEMA {schema_fqtn} STARTS WITH {table_literal}'),
        table_name,
    )
    if not exact_tables:
        return TABLE_FORMAT_MISSING
    if len(exact_tables) != 1:
        raise TableFormatDiscoveryException(
            f'Snowflake returned multiple exact matches for {schema_fqtn}.{_quote_identifier(table_name)}'
        )
    if not _snowflake_boolean(_row_value(exact_tables[0], 'is_iceberg'), 'is_iceberg'):
        return TABLE_FORMAT_NATIVE

    exact_iceberg_tables = _exact_name_rows(
        query(f'SHOW ICEBERG TABLES IN SCHEMA {schema_fqtn} STARTS WITH {table_literal}'),
        table_name,
    )
    if len(exact_iceberg_tables) != 1:
        raise TableFormatDiscoveryException(
            f'Snowflake Iceberg metadata is incomplete for {schema_fqtn}.{_quote_identifier(table_name)}'
        )

    catalog_name = _row_value(exact_iceberg_tables[0], 'catalog_name')
    if not isinstance(catalog_name, str) or not catalog_name.strip():
        raise TableFormatDiscoveryException(
            f'Snowflake returned an invalid catalog_name for {schema_fqtn}.{_quote_identifier(table_name)}'
        )
    if catalog_name.upper() != 'SNOWFLAKE':
        return TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG

    table_fqtn = '.'.join(
        _quote_identifier(identifier)
        for identifier in (database_name, schema_name, table_name)
    )
    contract = managed_iceberg_contract(
        query(f"SHOW PARAMETERS LIKE 'ICEBERG_VERSION' IN TABLE {table_fqtn}"),
        table_fqtn,
    )
    contract.existing_table_validator(
        query,
        database_name,
        schema_name,
        table_name,
        table_fqtn,
    )
    return contract.table_format


def validate_table_format_config(config):
    """Validate target-visible settings; PipelineWise owns source-route policy."""
    errors = []
    if 'iceberg_create' in config:
        errors.append(
            "'iceberg_create' is no longer supported; use "
            "'target_table_format': 'iceberg' with integer 'iceberg_version': 3"
        )

    target_table_format_is_set = 'target_table_format' in config
    target_table_format = config.get('target_table_format')
    iceberg_version_is_set = 'iceberg_version' in config
    iceberg_version = config.get('iceberg_version')
    valid_iceberg_version = is_supported_iceberg_version(iceberg_version)
    if target_table_format_is_set and target_table_format not in ('native', 'iceberg'):
        errors.append("'target_table_format' must be either 'native' or 'iceberg'")
    elif target_table_format == 'iceberg' and not valid_iceberg_version:
        errors.append("'iceberg_version' must be integer 3 when 'target_table_format' is 'iceberg'")
    elif target_table_format != 'iceberg' and iceberg_version_is_set:
        errors.append("'iceberg_version' is only valid when 'target_table_format' is 'iceberg'")

    if target_table_format == 'iceberg' and config.get('hard_delete') is not True:
        errors.append("'hard_delete' must be true when 'target_table_format' is 'iceberg'")

    return errors


def _native_column_type(schema_property):
    property_type = schema_property['type']
    property_format = schema_property.get('format')
    col_type = 'text'
    if 'object' in property_type or 'array' in property_type:
        col_type = 'variant'
    elif property_format == 'date-time':
        col_type = 'timestamp_ntz'
    elif property_format == 'date':
        col_type = 'date'
    elif property_format == 'time':
        col_type = 'time'
    elif property_format == 'binary':
        col_type = 'binary'
    elif 'number' in property_type:
        col_type = 'float'
    elif 'integer' in property_type and 'string' in property_type:
        col_type = 'text'
    elif 'integer' in property_type:
        col_type = 'number'
    elif 'boolean' in property_type:
        col_type = 'boolean'
    return col_type


def _iceberg_column_type(col_type, iceberg_version=3):
    contract = get_managed_iceberg_contract(iceberg_version)
    return contract.column_types[col_type]


def column_type(schema_property, is_iceberg_table=False, iceberg_version=None):
    """Return a native or explicitly versioned managed-Iceberg column type."""
    if is_iceberg_table and not is_supported_iceberg_version(iceberg_version):
        raise ValueError('Iceberg type mapping requires integer version 3')
    if iceberg_version is not None and not is_iceberg_table:
        raise ValueError('An Iceberg version cannot be used for a native table')

    native_type = _native_column_type(schema_property)
    if is_iceberg_table:
        return _iceberg_column_type(native_type, iceberg_version)
    if native_type == 'text':
        return SNOWFLAKE_MAX_VARCHAR_TYPE
    return native_type


def iceberg_text_variant_mismatch_reason(current_type, iceberg_version):
    """Return the version-specific diagnostic for a forbidden type transition."""
    if not is_supported_iceberg_version(iceberg_version):
        raise ValueError('Iceberg TEXT/VARIANT comparison requires integer version 3')
    compatibility = get_managed_iceberg_contract(iceberg_version).type_compatibility
    return compatibility.mismatch_reason(current_type)


def column_clause(name, schema_property, is_iceberg_table=False, iceberg_version=None):
    """Generate a DDL column definition."""
    return f'{safe_column_name(name)} {column_type(schema_property, is_iceberg_table, iceberg_version)}'


def safe_column_name(name):
    """Generate a Snowflake-compatible quoted column name."""
    return f'"{name}"'.upper()


def create_iceberg_table_query(table_name, columns, primary_key, iceberg_version):
    """Build managed-Iceberg CREATE TABLE SQL from a complete version contract."""
    if not is_supported_iceberg_version(iceberg_version):
        raise ValueError('Iceberg table creation requires integer version 3')
    contract = get_managed_iceberg_contract(iceberg_version)
    p_columns = ', '.join(list(columns) + list(primary_key))
    return (
        f'CREATE ICEBERG TABLE IF NOT EXISTS {table_name} ({p_columns}) '
        f"CATALOG='SNOWFLAKE' ICEBERG_VERSION={contract.version} "
        f'{contract.table_options}'
    )


def _replacement_for_existing_column(
    name,
    properties_schema,
    current_type,
    *,
    is_iceberg_table,
    iceberg_version,
    contract,
):
    definition = column_clause(
        name,
        properties_schema,
        is_iceberg_table,
        iceberg_version,
    )
    new_type = column_type(
        properties_schema,
        is_iceberg_table,
        iceberg_version,
    ).upper()
    base_new_type = _base_snowflake_type(new_type)
    if current_type == base_new_type:
        return None
    if (
        _base_snowflake_type(current_type) in SNOWFLAKE_STRING_TYPES
        and base_new_type == 'VARCHAR'
    ):
        return None
    if (
        base_new_type == 'TIMESTAMP_NTZ'
        and _base_snowflake_type(current_type) in SNOWFLAKE_TIMESTAMP_TYPES
    ):
        return None

    compatibility = contract.type_compatibility if contract else None
    if compatibility and (current_type, base_new_type) in compatibility.compatible_pairs:
        return None
    if compatibility and frozenset({current_type, base_new_type}) in compatibility.forbidden_pairs:
        raise TableFormatMismatchException(
            f'Iceberg column {name.upper()} is {current_type}, but '
            f'{compatibility.mismatch_reason(current_type)}'
        )
    return safe_column_name(name), definition


def plan_column_changes(
    flatten_schema,
    existing_column_types,
    is_iceberg_table=False,
    iceberg_version=None,
):
    """Return additions and replacements without executing Snowflake DDL."""
    contract = None
    if is_iceberg_table:
        if not is_supported_iceberg_version(iceberg_version):
            raise ValueError('Iceberg type mapping requires integer version 3')
        contract = get_managed_iceberg_contract(iceberg_version)
    elif iceberg_version is not None:
        raise ValueError('An Iceberg version cannot be used for a native table')

    normalized_existing_types = {
        name.upper(): data_type.upper()
        for name, data_type in existing_column_types.items()
    }
    additions = []
    replacements = []
    for name, properties_schema in flatten_schema.items():
        name_upper = name.upper()
        if name_upper not in normalized_existing_types:
            additions.append(column_clause(
                name,
                properties_schema,
                is_iceberg_table,
                iceberg_version,
            ))
            continue

        current_type = normalized_existing_types[name_upper]
        replacement = _replacement_for_existing_column(
            name,
            properties_schema,
            current_type,
            is_iceberg_table=is_iceberg_table,
            iceberg_version=iceberg_version,
            contract=contract,
        )
        if replacement:
            replacements.append(replacement)

    return ColumnChangePlan(tuple(additions), tuple(replacements))
