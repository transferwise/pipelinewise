"""Schema models and exact validation for manual Iceberg conversion."""

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    IcebergColumn,
    IcebergTableSpec,
    SnowflakeObjectName,
    TableCompatibilityError,
    TableFormatDiscoveryError,
    canonical_iceberg_type,
    quote_identifier,
    sql_string_literal,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_parameters import assert_managed_iceberg_table_parameters
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    MANAGED_ICEBERG_V3_SPEC,
    build_recovery_identity,
    parse_exact_integer_metadata,
)
from pipelinewise.fastsync.commons.snowflake_types import SNOWFLAKE_MAX_VARCHAR_LENGTH


ICEBERG_V3 = MANAGED_ICEBERG_V3_SPEC.version
ICEBERG_VARCHAR_LENGTH = SNOWFLAKE_MAX_VARCHAR_LENGTH
CONVERSION_CUTOVER_OUTAGE_WARNING = (
    'eventual=iceberg conversion requires a controlled reader-and-writer outage '
    'for %s. The primary table name is temporarily absent during promotion and '
    'rollback and can remain absent after an interruption. Retry the identical '
    'command before resuming readers or writers.'
)


class NativeToIcebergConversionError(RuntimeError):
    """Raised when a native table cannot be converted safely."""


class SnowflakeTableName(SnowflakeObjectName):
    """Shared object name with conversion-specific parsing and schema SQL."""

    @classmethod
    def parse(cls, fqtn: str) -> 'SnowflakeTableName':
        if not isinstance(fqtn, str) or not fqtn.strip():
            raise ValueError('Table must be a non-empty fully qualified name')
        parsed = SnowflakeObjectName.parse(fqtn)
        return cls(parsed.database, parsed.schema, parsed.table)

    @property
    def quoted_schema(self) -> str:
        """Return the exact database and schema identifier."""
        return '.'.join(
            quote_identifier(value) for value in (self.database, self.schema)
        )


@dataclass(frozen=True)
class ConversionTableState:
    """Physical state of the primary and reserved companion names."""

    original: str
    staging: str
    backup: str


@dataclass(frozen=True)
class NativeColumn:
    """Canonical native column metadata used for an Iceberg v3 projection."""

    name: str
    data_type: str
    nullable: bool
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    comment: Optional[str] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> 'NativeColumn':
        """Build a column from Snowflake INFORMATION_SCHEMA metadata."""
        name = _value(row, 'COLUMN_NAME')
        data_type = _value(row, 'DATA_TYPE')
        nullable = _value(row, 'IS_NULLABLE')
        if not isinstance(name, str) or not isinstance(data_type, str):
            raise NativeToIcebergConversionError(
                'Snowflake column metadata did not include COLUMN_NAME and DATA_TYPE'
            )
        if nullable not in ('YES', 'NO'):
            raise NativeToIcebergConversionError(
                f'Snowflake returned invalid nullability for column {name}'
            )
        if _value(row, 'COLUMN_DEFAULT') is not None:
            raise NativeToIcebergConversionError(
                f'Column {name} has a default that manual conversion cannot preserve safely'
            )
        if str(_value(row, 'IS_IDENTITY', 'NO')).upper() != 'NO':
            raise NativeToIcebergConversionError(
                f'Column {name} is an identity column that manual conversion cannot preserve safely'
            )
        return cls(
            name=name,
            data_type=data_type.upper(),
            nullable=nullable == 'YES',
            numeric_precision=_value(row, 'NUMERIC_PRECISION'),
            numeric_scale=_value(row, 'NUMERIC_SCALE'),
            comment=_value(row, 'COMMENT'),
        )

    @property
    def iceberg_type(self) -> str:
        """Return the explicit Snowflake-managed Iceberg v3 type."""
        if self.data_type == 'NUMBER':
            if self.numeric_precision is None or self.numeric_scale is None:
                raise NativeToIcebergConversionError(
                    f'Snowflake returned incomplete NUMBER metadata for column {self.name}'
                )
            data_type = f'NUMBER({self.numeric_precision},{self.numeric_scale})'
        else:
            data_type = self.data_type
        try:
            return canonical_iceberg_type(data_type)
        except ValueError as exc:
            raise NativeToIcebergConversionError(
                f'Native Snowflake type {self.data_type} on column {self.name} '
                'is not supported by managed Iceberg v3 conversion'
            ) from exc

    @property
    def iceberg_column(self) -> IcebergColumn:
        """Return the shared canonical Iceberg column model."""
        return IcebergColumn(self.name, self.iceberg_type, self.nullable)

    @property
    def definition(self) -> str:
        """Return the explicit Iceberg column definition."""
        return self.ddl_definition(self.nullable)

    def ddl_definition(self, nullable: bool) -> str:
        """Return a definition with the target's required nullability."""
        definition = IcebergColumn(self.name, self.iceberg_type, nullable).definition
        if self.comment is not None:
            definition += f' COMMENT {sql_string_literal(self.comment)}'
        return definition

    @property
    def projection(self) -> str:
        """Return the deterministic conversion projection for this column."""
        return self.iceberg_column.projection()


def parse_native_columns(
    rows: Iterable[Dict[str, Any]],
) -> Tuple[NativeColumn, ...]:
    """Build a non-empty ordered native column model."""
    columns = tuple(NativeColumn.from_row(row) for row in rows)
    if not columns:
        raise NativeToIcebergConversionError('The native table has no visible columns')
    if len({column.name for column in columns}) != len(columns):
        raise NativeToIcebergConversionError(
            'Snowflake returned duplicate column metadata'
        )
    return columns


@dataclass(frozen=True)
class ConversionMetadata:
    """Source schema and metadata needed before publication."""

    columns: Tuple[NativeColumn, ...]
    primary_key: Tuple[str, ...]
    owner: str
    owner_role_type: str
    table_comment: Optional[str]
    grants: Tuple[Dict[str, Any], ...]
    tags: Tuple[Dict[str, Any], ...]

    @property
    def fingerprint(self) -> str:
        """Return a deterministic schema fingerprint without source data."""
        payload = {
            'columns': [asdict(column) for column in self.columns],
            'primary_key': self.primary_key,
            'owner': self.owner,
            'owner_role_type': self.owner_role_type,
            'table_comment': self.table_comment,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode('utf-8')
        ).hexdigest()

    def table_spec(self, table: SnowflakeObjectName) -> IcebergTableSpec:
        """Return the shared canonical table model used by recovery."""
        return IcebergTableSpec(
            table,
            tuple(
                IcebergColumn(
                    column.name,
                    column.iceberg_type,
                    column.nullable and column.name not in self.primary_key,
                )
                for column in self.columns
            ),
            self.primary_key,
        )


def _value(row: Dict[str, Any], name: str, default=None):
    for key in (name, name.upper(), name.lower()):
        if key in row:
            return row[key]
    return default


def snowflake_boolean(value: Any) -> bool:
    """Return strict Snowflake boolean metadata."""
    if value is True or str(value).upper() in ('Y', 'YES', 'TRUE'):
        return True
    if value is False or str(value).upper() in ('N', 'NO', 'FALSE'):
        return False
    raise NativeToIcebergConversionError(
        f'Snowflake returned invalid boolean metadata: {value!r}'
    )


def grantee_sql(grantee_type: str, grantee: str) -> str:
    """Return a safely quoted supported grant recipient."""
    if grantee_type == 'ROLE':
        return f'ROLE {quote_identifier(grantee)}'
    if grantee_type == 'DATABASE_ROLE':
        identifiers = grantee.split('.')
        if len(identifiers) == 2 and all(identifiers):
            qualified_name = '.'.join(
                quote_identifier(value) for value in identifiers
            )
            return f'DATABASE ROLE {qualified_name}'
    raise NativeToIcebergConversionError(
        f'Unsupported Snowflake grantee metadata: {grantee_type} {grantee!r}'
    )


def manual_recovery_identity(connection_config: Dict[str, Any]) -> Dict[str, Any]:
    """Bind conversion recovery to the configured Snowflake principal."""
    return build_recovery_identity(
        'manual_conversion',
        {
            'target': {
                'account': connection_config.get('account'),
                'database': connection_config.get('dbname'),
                'user': connection_config.get('user'),
                'role': connection_config.get('role'),
            },
        },
    )


def stream_references_table(
    row: Dict[str, Any],
    table: SnowflakeObjectName,
) -> bool:
    """Return whether one visible stream directly or indirectly tracks a table."""
    if not isinstance(row, dict):
        raise NativeToIcebergConversionError(
            'Snowflake returned invalid stream dependency metadata'
        )
    source_type = str(_value(row, 'source_type', '')).upper()
    if source_type not in ('TABLE', 'VIEW'):
        return False
    object_lists = [_value(row, 'table_name', '')]
    if source_type == 'VIEW':
        object_lists.append(_value(row, 'base_tables', ''))
    try:
        references = {
            reference
            for object_list in object_lists
            for reference in SnowflakeObjectName.parse_list(object_list)
        }
    except ValueError as exc:
        raise NativeToIcebergConversionError(
            'Snowflake returned invalid stream dependency metadata'
        ) from exc
    target = (table.database, table.schema, table.table)
    return any(
        (reference.database, reference.schema, reference.table) == target
        for reference in references
    )


def _inspect_iceberg_table_spec(
    query: Callable,
    table: SnowflakeObjectName,
) -> IcebergTableSpec:
    column_rows = query(
        'SELECT "COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", '
        '"NUMERIC_SCALE", "DATETIME_PRECISION", '
        '"CHARACTER_MAXIMUM_LENGTH", "IS_NULLABLE" '
        f'FROM {quote_identifier(table.database)}."INFORMATION_SCHEMA"."COLUMNS" '
        'WHERE "TABLE_SCHEMA" = %(schema)s AND "TABLE_NAME" = %(table)s '
        'ORDER BY "ORDINAL_POSITION"',
        {'schema': table.schema, 'table': table.table},
        phase='reconcile',
    )
    key_rows = query(
        f'SHOW PRIMARY KEYS IN TABLE {table.quoted}',
        phase='reconcile',
    )
    try:
        columns = tuple(IcebergColumn.from_snowflake_row(row) for row in column_rows)
        primary_key = tuple(
            str(_value(row, 'COLUMN_NAME'))
            for row in sorted(
                key_rows,
                key=lambda row: int(_value(row, 'KEY_SEQUENCE')),
            )
        )
        return IcebergTableSpec(table, columns, primary_key)
    except TableCompatibilityError as exc:
        raise NativeToIcebergConversionError(str(exc)) from exc
    except (TableFormatDiscoveryError, TypeError, ValueError) as exc:
        raise NativeToIcebergConversionError(
            f'Snowflake returned invalid schema metadata for {table.quoted}'
        ) from exc


def assert_iceberg_table_spec(
    query: Callable,
    table: SnowflakeObjectName,
    expected: IcebergTableSpec,
) -> None:
    """Require exact ordered columns, types, nullability, and primary key."""
    actual = _inspect_iceberg_table_spec(query, table)
    expected_at_name = IcebergTableSpec(table, expected.columns, expected.primary_key)
    if actual != expected_at_name:
        raise NativeToIcebergConversionError(
            f'{table.quoted} schema or primary key does not match the conversion source'
        )


def _grant_signature(grant: Dict[str, Any]) -> Optional[Tuple[Any, ...]]:
    privilege = _value(grant, 'privilege')
    if str(privilege).upper() == 'OWNERSHIP':
        return None
    grantee_type = _value(grant, 'granted_to')
    grantee = _value(grant, 'grantee_name')
    if not all(isinstance(value, str) and value for value in (privilege, grantee_type, grantee)):
        raise NativeToIcebergConversionError('Snowflake returned invalid grant metadata')
    return (
        privilege.upper(),
        grantee_type.upper(),
        grantee,
        snowflake_boolean(_value(grant, 'grant_option', False)),
    )


def _supported_metadata_signature(metadata) -> Tuple[Any, ...]:
    grants = tuple(sorted(
        signature
        for signature in (_grant_signature(grant) for grant in metadata.grants)
        if signature is not None
    ))
    tags = []
    for tag in metadata.tags:
        signature = tuple(
            _value(tag, field)
            for field in ('TAG_DATABASE', 'TAG_SCHEMA', 'TAG_NAME', 'TAG_VALUE')
        )
        if not all(isinstance(value, str) for value in signature):
            raise NativeToIcebergConversionError('Snowflake returned invalid tag metadata')
        tags.append(signature)
    return (
        tuple((column.name, column.comment) for column in metadata.columns),
        metadata.owner,
        metadata.owner_role_type,
        metadata.table_comment,
        grants,
        tuple(sorted(tags)),
    )


def assert_supported_metadata(table, expected, actual) -> None:
    """Require exact ownership, comments, grants, and direct table tags."""
    if _supported_metadata_signature(actual) != _supported_metadata_signature(expected):
        raise NativeToIcebergConversionError(
            f'{table.quoted} ownership, comments, grants, or tags do not match '
            'the conversion source'
        )


def assert_managed_v3(query: Callable, table: SnowflakeObjectName) -> None:
    """Require an exact Snowflake-managed Iceberg v3 table."""
    rows = query(
        f'SHOW ICEBERG TABLES IN SCHEMA '
        f'{quote_identifier(table.database)}.{quote_identifier(table.schema)} '
        f'STARTS WITH {sql_string_literal(table.table)}',
        phase='reconcile',
    )
    exact = [row for row in rows if _value(row, 'name') == table.table]
    catalog = _value(exact[0], 'catalog_name', '') if len(exact) == 1 else ''
    if str(catalog).upper() != 'SNOWFLAKE':
        raise NativeToIcebergConversionError(
            f'{table.quoted} is not a Snowflake-managed Iceberg table'
        )
    parameters = query(
        f"SHOW PARAMETERS LIKE 'ICEBERG_VERSION' IN TABLE {table.quoted}",
        phase='reconcile',
    )
    versions = [
        _value(row, 'value') for row in parameters
        if str(_value(row, 'key', '')).upper() == 'ICEBERG_VERSION'
    ]
    try:
        valid = (
            len(versions) == 1
            and parse_exact_integer_metadata(versions[0]) == ICEBERG_V3
        )
    except ValueError:
        valid = False
    if not valid:
        raise NativeToIcebergConversionError(
            f'{table.quoted} is not managed Iceberg version 3'
        )
    try:
        assert_managed_iceberg_table_parameters(
            query,
            table,
            3,
            phase='reconcile',
        )
    except (TableFormatDiscoveryError, TableCompatibilityError) as exc:
        raise NativeToIcebergConversionError(str(exc)) from exc
