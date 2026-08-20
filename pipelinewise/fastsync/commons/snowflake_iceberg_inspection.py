"""Snowflake catalog inspection for managed Iceberg publication."""

from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

import snowflake.connector

from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    TABLE_FORMAT_MISSING,
    TABLE_FORMAT_NATIVE,
    TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG,
    IcebergColumn,
    IcebergTableSpec,
    SnowflakeObjectName,
    SnowflakeTableSnapshot,
    TableFormatDiscoveryError,
    _row_value,
    _snowflake_boolean,
    quote_identifier,
    sql_string_literal,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_parameters import (
    validated_managed_iceberg_table_format,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    managed_iceberg_spec_for_table_format,
    parse_exact_integer_metadata,
)


def exact_named_table_rows(
    rows: Iterable[Dict[str, Any]],
    expected_names: Sequence[str],
) -> Tuple[Dict[str, Any], ...]:
    """Select exact SHOW TABLES names without wildcard-like interpretation."""
    names = frozenset(expected_names)
    return tuple(row for row in rows if _row_value(row, 'name') in names)


def physical_table_format(
    row: Dict[str, Any],
    boolean_parser,
    iceberg_format: str,
    native_format: str,
) -> str:
    """Classify one SHOW TABLES row using the caller's error taxonomy."""
    return (
        iceberg_format
        if boolean_parser(_row_value(row, 'is_iceberg'))
        else native_format
    )


class SnowflakeTableInspector:
    """Read exact table format, schema, key, and physical identity."""

    def __init__(self, snowflake_adapter):
        self.snowflake = snowflake_adapter

    def discover_table_format(self, schema_name: str, table_name: str) -> str:
        """Return the exact physical format in the configured database."""
        database_name = self.snowflake.connection_config['dbname'].upper()
        target = SnowflakeObjectName(
            database_name,
            schema_name.upper(),
            table_name.upper(),
        )
        table_row = self.discover_table_row(target)
        if table_row is None:
            return TABLE_FORMAT_MISSING
        physical_format = physical_table_format(
            table_row,
            lambda value: _snowflake_boolean(value, 'is_iceberg'),
            'iceberg',
            TABLE_FORMAT_NATIVE,
        )
        if physical_format == TABLE_FORMAT_NATIVE:
            return TABLE_FORMAT_NATIVE

        rows = self.snowflake.query(
            f'SHOW ICEBERG TABLES IN SCHEMA '
            f'{target.with_table(target.schema).quoted.rsplit(".", 1)[0]} '
            f'STARTS WITH {sql_string_literal(target.table)}'
        )
        exact_rows = exact_named_table_rows(rows, (target.table,))
        if len(exact_rows) != 1:
            raise TableFormatDiscoveryError(
                f'Snowflake Iceberg metadata is incomplete for {target.quoted}'
            )
        catalog_name = _row_value(exact_rows[0], 'catalog_name')
        if not isinstance(catalog_name, str) or not catalog_name.strip():
            raise TableFormatDiscoveryError(
                f'Snowflake returned an invalid catalog_name for {target.quoted}'
            )
        if catalog_name.upper() != 'SNOWFLAKE':
            return TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG

        rows = self.snowflake.query(
            f"SHOW PARAMETERS LIKE 'ICEBERG_VERSION' IN TABLE {target.quoted}"
        )
        version_rows = [
            row
            for row in rows
            if str(_row_value(row, 'key')).upper() == 'ICEBERG_VERSION'
        ]
        if len(version_rows) != 1:
            raise TableFormatDiscoveryError(
                f'Snowflake did not return one ICEBERG_VERSION for {target.quoted}'
            )
        try:
            version = parse_exact_integer_metadata(
                _row_value(version_rows[0], 'value')
            )
        except (TypeError, ValueError) as exc:
            raise TableFormatDiscoveryError(
                f'Snowflake returned an invalid ICEBERG_VERSION for {target.quoted}'
            ) from exc
        return validated_managed_iceberg_table_format(
            self.snowflake.query,
            target,
            version,
        )

    def discover_table_row(
        self,
        target: SnowflakeObjectName,
    ) -> Optional[Dict[str, Any]]:
        """Return the one exact SHOW TABLES row for a target."""
        schema_fqtn = '.'.join(
            quote_identifier(value) for value in (target.database, target.schema)
        )
        try:
            rows = self.snowflake.query(
                f'SHOW TABLES IN SCHEMA {schema_fqtn} '
                f'STARTS WITH {sql_string_literal(target.table)}'
            )
        except snowflake.connector.errors.ProgrammingError:
            schema_rows = self.snowflake.query(
                f'SHOW SCHEMAS IN DATABASE {quote_identifier(target.database)} '
                f'STARTS WITH {sql_string_literal(target.schema)}'
            )
            exact_schemas = [
                row for row in schema_rows if _row_value(row, 'name') == target.schema
            ]
            if not exact_schemas:
                return None
            raise
        exact_rows = exact_named_table_rows(rows, (target.table,))
        if not exact_rows:
            return None
        if len(exact_rows) != 1:
            raise TableFormatDiscoveryError(
                f'Snowflake returned multiple exact matches for {target.quoted}'
            )
        return exact_rows[0]

    def inspect_table(self, target: SnowflakeObjectName) -> SnowflakeTableSnapshot:
        """Inspect the exact target format, schema, key, and object identity."""
        table_row = self.discover_table_row(target)
        if table_row is None:
            return SnowflakeTableSnapshot(TABLE_FORMAT_MISSING, None, None)
        table_format = self.discover_table_format(target.schema, target.table)
        identity = str(
            _row_value(table_row, 'id', _row_value(table_row, 'created_on', target.key))
        )
        try:
            version_spec = managed_iceberg_spec_for_table_format(table_format)
        except ValueError:
            return SnowflakeTableSnapshot(table_format, None, identity)

        columns = self.snowflake.query(
            'SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, '
            'DATETIME_PRECISION, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE '
            f'FROM {quote_identifier(target.database)}.INFORMATION_SCHEMA.COLUMNS '
            'WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s '
            'ORDER BY ORDINAL_POSITION',
            {'schema': target.schema, 'table': target.table},
        )
        primary_key_rows = self.snowflake.query(
            f'SHOW PRIMARY KEYS IN TABLE {target.quoted}'
        )
        primary_key = tuple(
            str(_row_value(row, 'column_name'))
            for row in sorted(
                primary_key_rows,
                key=lambda row: int(_row_value(row, 'key_sequence')),
            )
        )
        spec = IcebergTableSpec(
            target,
            tuple(
                IcebergColumn.from_snowflake_row(row, version_spec.version)
                for row in columns
            ),
            primary_key,
        )
        return SnowflakeTableSnapshot(table_format, spec, identity)
