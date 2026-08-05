"""Database adapters for data-diff checks.

One adapter per database type, providing dialect-specific SQL generation, column
and index metadata, table sizing, and checksum normalization. Checksum expressions
must agree numerically across every dialect, since a source and target are compared
by their sums: see ``checksum_integer`` and ``normalize_checksum_value``.
"""

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from time import perf_counter

import psycopg2.extras


MAX_SAFE_FULL_SCAN_ROWS = 100_000
# Used only when a table has no planner statistics. Deliberately the densest
# realistic packing, so an unanalyzed large table blocks rather than slips through.
ROWS_PER_PAGE_ESTIMATE = 250
# Distinguishes "probed, no visibility column" from "not probed yet"; None is a real
# result here, so it cannot double as the sentinel.
_UNPROBED = object()
SCHEMA_CHECK = "schema_compatibility"
CHECKSUM_CHECK = "row_checksum"
CHECKSUM_HEX_DIGITS = 12
CHECKSUM_MASK = (2 ** (CHECKSUM_HEX_DIGITS * 4)) - 1
CHECKSUM_OFFSET = CHECKSUM_MASK // 2
METRIC_EXPRESSIONS = {
    "row_count": "COUNT(*)",
    "distinct_key_count": "COUNT(DISTINCT {key})",
    "null_key_count": "COUNT(*) - COUNT({key})",
    "duplicate_key_count": "COUNT({key}) - COUNT(DISTINCT {key})",
    "min_key": "MIN({key})",
    "max_key": "MAX({key})",
}
TIMESTAMP_TYPES = (
    "timestamp with time zone",
    "timestamp without time zone",
    "timestamp",
    "datetime",
    "timestamp_ltz",
    "timestamp_ntz",
    "timestamp_tz",
)


class DataDiffExecutionError(RuntimeError):
    """Raised for unsafe plans, missing metadata, or execution failures."""


@dataclass
class MetricQueryResult:
    """Canonical metric values and the duration of their shared query."""

    values: dict
    duration_seconds: float


def _quote(identifier: str, quote_character: str) -> str:
    if not identifier or "\x00" in identifier:
        raise DataDiffExecutionError("Database identifiers must be non-empty and cannot contain NUL")
    return f"{quote_character}{identifier.replace(quote_character, quote_character * 2)}{quote_character}"


def _canonical_number(value) -> str:
    number = Decimal(str(value))
    if number == number.to_integral():
        return str(number.quantize(Decimal(1)))
    return format(number.normalize(), "f")


def canonical_value(value):
    """Convert driver-specific values into stable JSON comparison values."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (Decimal, float, int)):
        return _canonical_number(value)
    if isinstance(value, datetime):
        aware = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        return aware.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _utc_boundary(value: datetime, data_type: str):
    value = value.astimezone(timezone.utc)
    normalized_type = data_type.lower()
    naive_types = ("timestamp without time zone", "datetime", "timestamp_ntz")
    if any(data_type_name in normalized_type for data_type_name in naive_types):
        return value.replace(tzinfo=None)
    return value


def _validate_timestamp_type(column: dict, qualified_name: str):
    data_type = column["data_type"].lower()
    if not any(data_type.startswith(supported) for supported in TIMESTAMP_TYPES):
        raise DataDiffExecutionError(
            f"Data-diff timestamp column {qualified_name} has unsupported type "
            f"'{column['data_type']}'"
        )


class DatabaseAdapter:
    """Common SQL and result behavior for one check-side connection."""

    quote_character = '"'

    def __init__(self, connection, statement_timeout_seconds: int):
        self.connection = connection
        self.statement_timeout_seconds = statement_timeout_seconds

    def close(self):
        """Close the physical database connection."""
        self.connection.close()

    def quote(self, identifier):
        """Quote one identifier according to this database dialect."""
        return _quote(identifier, self.quote_character)

    def qualified_table(self, schema, table):
        """Return a quoted two-part table name."""
        return f"{self.quote(schema)}.{self.quote(table)}"

    def resolve_columns(
        self,
        schema: str,
        table: str,
        requested: list,
        *,
        allow_missing: bool = False,
    ) -> dict:
        """Resolve requested columns to exact names and data types."""
        raise NotImplementedError

    def normalize_checksum_value(self, column: dict) -> str:
        """Return a stable textual representation for one checksum column."""
        name = self.quote(column["name"])
        kind = column["checksum_kind"]
        if kind == "string":
            return f"CAST({name} AS TEXT)"
        if kind == "exact_numeric":
            factor = 10 ** column["checksum_scale"]
            return f"CAST(ROUND({name} * {factor}, 0) AS DECIMAL(38, 0))::TEXT"
        if kind == "boolean":
            # The ELSE branch must not swallow NULL, or a NULL source column hashes
            # identically to a FALSE target one and the mismatch is invisible.
            return f"CASE WHEN {name} IS NULL THEN NULL WHEN {name} THEN '1' ELSE '0' END"
        if kind == "timestamp":
            return f"TO_CHAR({name}, 'YYYY-MM-DD HH24:MI:SS.US')"
        if kind == "time":
            return f"TO_CHAR({name}, 'HH24:MI:SS.US')"
        if kind == "binary":
            return f"ENCODE({name}, 'hex')"
        raise DataDiffExecutionError(f"Unsupported checksum kind: {kind}")

    def checksum_integer(self, payload: str) -> str:
        """Convert the low 48 bits of an MD5 value to a centred integer."""
        return (
            f"(('x' || SUBSTRING(MD5({payload}), 21))::bit(48)::bigint "
            f"- {CHECKSUM_OFFSET})"
        )

    def checksum_expression(self, columns: list) -> str:
        """Return a bounded aggregate checksum over ordered row values."""
        field_hashes = []
        for column in columns:
            normalized = self.normalize_checksum_value(column)
            field_hashes.append(
                f"CASE WHEN {normalized} IS NULL THEN MD5('N') "
                f"ELSE MD5(CONCAT('V', {normalized})) END"
            )
        row_payload = f"CONCAT({', '.join(field_hashes)})"
        return f"SUM({self.checksum_integer(row_payload)})"

    def indexes(self, schema: str, table: str) -> list:
        """Return ordered index-column metadata for the table."""
        raise NotImplementedError

    def table_rows(self, schema: str, table: str) -> int:
        """Return the approximate row count of a table from catalog statistics."""
        raise NotImplementedError

    def execute_metrics(self, sql: str, params: tuple, checks: tuple) -> MetricQueryResult:
        """Execute one aggregate query and canonicalize its values."""
        started = perf_counter()
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if row is None:
                raise DataDiffExecutionError("Aggregate query returned no row")
            if not isinstance(row, dict):
                row = dict(zip((description[0] for description in cursor.description), row))
        values_by_name = {str(key).lower(): value for key, value in row.items()}
        return MetricQueryResult(
            values={check: canonical_value(values_by_name[check]) for check in checks},
            duration_seconds=perf_counter() - started,
        )


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL metadata, plan, and aggregate operations."""

    def resolve_columns(self, schema, table, requested, *, allow_missing=False):
        with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT column_name, data_type, udt_name, numeric_precision,
                       numeric_scale, datetime_precision,
                       character_maximum_length, is_nullable
                  FROM information_schema.columns
                 WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            available = cursor.fetchall()
        return _match_columns(
            schema, table, requested, available, allow_missing=allow_missing
        )

    def indexes(self, schema, table):
        """Return index metadata, marking which indexes can serve a window scan.

        ``is_usable`` is what the preflight may rely on. An index is unusable when
        it is partial (``indpred``), built on expressions rather than plain columns
        (``indexprs``), still building or invalid (``indisready`` / ``indisvalid``),
        or uses an access method that cannot answer a range predicate — a hash
        index supports equality only, and BRIN gives block ranges, not ordering.
        Unusable indexes are still returned so the preflight evidence records them.
        """
        with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT index_class.relname AS index_name,
                       array_agg(attribute.attname ORDER BY key_part.ordinality) AS columns,
                       index_meta.indisunique AS is_unique,
                       access_method.amname AS access_method,
                       index_meta.indpred IS NOT NULL AS is_partial,
                       index_meta.indexprs IS NOT NULL AS is_expression,
                       index_meta.indisvalid AS is_valid,
                       index_meta.indisready AS is_ready,
                       (
                           index_meta.indpred IS NULL
                           AND index_meta.indexprs IS NULL
                           AND index_meta.indisvalid
                           AND index_meta.indisready
                           AND access_method.amname = 'btree'
                       ) AS is_usable
                  FROM pg_index index_meta
                  JOIN pg_class table_class ON table_class.oid = index_meta.indrelid
                  JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
                  JOIN pg_class index_class ON index_class.oid = index_meta.indexrelid
                  JOIN pg_am access_method ON access_method.oid = index_class.relam
                  JOIN LATERAL unnest(index_meta.indkey) WITH ORDINALITY key_part(attnum, ordinality)
                    ON TRUE
                  JOIN pg_attribute attribute
                    ON attribute.attrelid = table_class.oid
                   AND attribute.attnum = key_part.attnum
                 WHERE namespace.nspname = %s AND table_class.relname = %s
                 GROUP BY index_class.relname, index_meta.indisunique,
                          access_method.amname, index_meta.indpred,
                          index_meta.indexprs, index_meta.indisvalid,
                          index_meta.indisready
                 ORDER BY index_class.relname
                """,
                (schema, table),
            )
            return [dict(row) for row in cursor.fetchall()]

    def table_rows(self, schema, table):
        """Return the approximate row count of a table, including its partitions.

        Only leaf relations contribute. A partitioned parent reports the same
        reltuples as its partitions, so counting both double-counts every row.

        ``reltuples <= 0`` means the planner has no usable estimate, not that the
        table is empty: ANALYZE on an empty table records 0, and rows inserted
        afterwards do not update it. Physical size is the honest signal in that
        case, and a genuinely empty table occupies no pages so it still passes.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                WITH RECURSIVE tree AS (
                    SELECT table_class.oid, table_class.reltuples
                      FROM pg_class table_class
                      JOIN pg_namespace namespace
                        ON namespace.oid = table_class.relnamespace
                     WHERE namespace.nspname = %s AND table_class.relname = %s
                    UNION ALL
                    SELECT child.oid, child.reltuples
                      FROM tree
                      JOIN pg_inherits inherits ON inherits.inhparent = tree.oid
                      JOIN pg_class child ON child.oid = inherits.inhrelid
                )
                SELECT COALESCE(SUM(
                    CASE
                        WHEN tree.reltuples > 0 THEN tree.reltuples
                        -- No statistics at all: pg_stats is empty too, so the only
                        -- honest input is physical size. Assume the densest
                        -- plausible packing so the guard fails closed — a table
                        -- large enough to matter is blocked rather than waved
                        -- through. ROWS_PER_PAGE_ESTIMATE is that upper bound.
                        ELSE (pg_relation_size(tree.oid) / 8192) * %s
                    END
                ), 0)::bigint AS rows
                  FROM tree
                 WHERE NOT EXISTS (
                     SELECT 1 FROM pg_inherits parent_of
                      WHERE parent_of.inhparent = tree.oid
                 )
                """,
                (schema, table, ROWS_PER_PAGE_ESTIMATE),
            )
            row = cursor.fetchone()
        if not row:
            return 0
        return int(row["rows"] if isinstance(row, dict) else row[0])


class MySQLAdapter(DatabaseAdapter):
    """MySQL and MariaDB metadata, plan, and aggregate operations."""

    quote_character = "`"

    def __init__(self, connection, statement_timeout_seconds: int):
        super().__init__(connection, statement_timeout_seconds)
        # Cached per connection: one catalog probe, however many tables follow.
        self._visibility = _UNPROBED

    def resolve_columns(self, schema, table, requested, *, allow_missing=False):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type,
                       COLUMN_TYPE AS udt_name,
                       NUMERIC_PRECISION AS numeric_precision,
                       NUMERIC_SCALE AS numeric_scale,
                       DATETIME_PRECISION AS datetime_precision,
                       CHARACTER_MAXIMUM_LENGTH AS character_maximum_length,
                       IS_NULLABLE AS is_nullable
                  FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (schema, table),
            )
            available = cursor.fetchall()
        return _match_columns(
            schema, table, requested, available, allow_missing=allow_missing
        )

    def normalize_checksum_value(self, column):
        name = self.quote(column["name"])
        kind = column["checksum_kind"]
        if kind == "string":
            return f"CAST({name} AS CHAR)"
        if kind == "exact_numeric":
            factor = 10 ** column["checksum_scale"]
            return f"CAST(CAST(ROUND({name} * {factor}, 0) AS DECIMAL(38, 0)) AS CHAR)"
        if kind == "boolean":
            # NULL must stay NULL so the outer marker distinguishes it from FALSE.
            return f"IF({name} IS NULL, NULL, IF({name}, '1', '0'))"
        # Specifiers are doubled because PyMySQL interpolates client-side: a bare
        # %Y raises "unsupported format character" before the query is sent.
        if kind == "timestamp":
            return f"DATE_FORMAT({name}, '%%Y-%%m-%%d %%H:%%i:%%s.%%f')"
        if kind == "time":
            return f"DATE_FORMAT({name}, '%%H:%%i:%%s.%%f')"
        if kind == "binary":
            return f"LOWER(HEX({name}))"
        raise DataDiffExecutionError(f"Unsupported checksum kind: {kind}")

    def checksum_integer(self, payload):
        # CONV returns a string, so subtracting coerces to DOUBLE and SUM() loses
        # integer precision. The DECIMAL cast keeps MySQL agreeing with the others.
        return (
            f"(CAST(CONV(SUBSTRING(MD5({payload}), 21), 16, 10) AS DECIMAL(38, 0)) "
            f"- {CHECKSUM_OFFSET})"
        )

    def _visibility_column(self):
        """Return the STATISTICS column marking an index the optimizer will skip.

        MySQL 8 spells it IS_VISIBLE ('NO' when INVISIBLE), MariaDB 10.6 spells it
        IGNORED ('YES' when IGNORED), and MySQL 5.7 has neither. Probed rather than
        assumed from VERSION(), which forks and backports make unreliable.
        """
        if self._visibility is _UNPROBED:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COLUMN_NAME AS column_name
                      FROM information_schema.COLUMNS
                     WHERE TABLE_SCHEMA = 'information_schema'
                       AND TABLE_NAME = 'STATISTICS'
                       AND COLUMN_NAME IN ('IS_VISIBLE', 'IGNORED')
                    """
                )
                available = {
                    str(row["column_name"] if isinstance(row, dict) else row[0]).upper()
                    for row in cursor.fetchall()
                }
            # IS_VISIBLE first: a fork exposing both is MySQL-compatible.
            self._visibility = next(
                (name for name in ("IS_VISIBLE", "IGNORED") if name in available), None
            )
        return self._visibility

    def indexes(self, schema, table):
        visibility_column = self._visibility_column()
        # The two columns carry opposite senses, so normalise to one flag here rather
        # than at the comparison: NULL means "no such column", never "hidden".
        hidden_value = "NO" if visibility_column == "IS_VISIBLE" else "YES"
        visibility = (
            f"{visibility_column} AS visibility" if visibility_column else "NULL AS visibility"
        )
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT INDEX_NAME AS index_name, COLUMN_NAME AS column_name,
                       SEQ_IN_INDEX AS sequence, NON_UNIQUE AS non_unique,
                       INDEX_TYPE AS index_type, SUB_PART AS sub_part,
                       {visibility}
                  FROM information_schema.STATISTICS
                 WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX
                """,
                (schema, table),
            )
            rows = cursor.fetchall()
        grouped = {}
        for row in rows:
            hidden = str(row["visibility"] or "").upper() == hidden_value
            entry = grouped.setdefault(
                row["index_name"],
                {
                    "index_name": row["index_name"],
                    "columns": [],
                    "is_unique": not row["non_unique"],
                    "access_method": (row["index_type"] or "").lower(),
                    # No partial indexes in MySQL, so what disqualifies is the access
                    # method, a prefix length (cannot order a range), and an index the
                    # optimizer is told to ignore -- EXPLAIN reports no possible keys
                    # for those, so treating one as usable certifies a full scan.
                    "is_usable": (
                        str(row["index_type"] or "").upper() == "BTREE"
                        and row["sub_part"] is None
                        and not hidden
                    ),
                },
            )
            entry["columns"].append(row["column_name"])
        return list(grouped.values())

    def table_rows(self, schema, table):
        """Return the approximate row count of a table, including its partitions."""
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(SUM(TABLE_ROWS), 0) AS row_count
                  FROM information_schema.PARTITIONS
                 WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (schema, table),
            )
            row = cursor.fetchone()
        count = int((row["row_count"] if isinstance(row, dict) else row[0]) or 0)
        if count:
            return count
        # PARTITIONS reports nothing for some engines, so fall back to TABLES.
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(TABLE_ROWS, 0) AS row_count
                  FROM information_schema.TABLES
                 WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (schema, table),
            )
            row = cursor.fetchone()
        if not row:
            return 0
        return int((row["row_count"] if isinstance(row, dict) else row[0]) or 0)


class SnowflakeAdapter(DatabaseAdapter):
    """Snowflake metadata and aggregate operations."""

    def resolve_columns(self, schema, table, requested, *, allow_missing=False):
        import snowflake.connector  # pylint: disable=import-outside-toplevel

        with self.connection.cursor(snowflake.connector.DictCursor) as cursor:
            cursor.execute(
                """
                SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type,
                       DATA_TYPE AS udt_name,
                       NUMERIC_PRECISION AS numeric_precision,
                       NUMERIC_SCALE AS numeric_scale,
                       DATETIME_PRECISION AS datetime_precision,
                       CHARACTER_MAXIMUM_LENGTH AS character_maximum_length,
                       IS_NULLABLE AS is_nullable
                  FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (schema, table),
            )
            available = cursor.fetchall()
        return _match_columns(
            schema, table, requested, available, allow_missing=allow_missing
        )

    def normalize_checksum_value(self, column):
        name = self.quote(column["name"])
        kind = column["checksum_kind"]
        if kind == "string":
            return f"CAST({name} AS VARCHAR)"
        if kind == "exact_numeric":
            factor = 10 ** column["checksum_scale"]
            return f"CAST(ROUND({name} * {factor}, 0) AS NUMBER(38, 0))::VARCHAR"
        if kind == "boolean":
            # NULL must stay NULL so the outer marker distinguishes it from FALSE.
            return f"IFF({name} IS NULL, NULL, IFF({name}, '1', '0'))"
        if kind == "timestamp":
            return (
                f"TO_CHAR(CONVERT_TIMEZONE('UTC', {name}), "
                "'YYYY-MM-DD HH24:MI:SS.FF6')"
            )
        if kind == "time":
            return f"TO_CHAR({name}, 'HH24:MI:SS.FF6')"
        if kind == "binary":
            return f"LOWER(HEX_ENCODE({name}))"
        raise DataDiffExecutionError(f"Unsupported checksum kind: {kind}")

    def checksum_integer(self, payload):
        return (
            f"(BITAND(MD5_NUMBER_LOWER64({payload}), {CHECKSUM_MASK}) "
            f"- {CHECKSUM_OFFSET})"
        )

    def execute_metrics(self, sql, params, checks):
        import snowflake.connector  # pylint: disable=import-outside-toplevel

        started = perf_counter()
        with self.connection.cursor(snowflake.connector.DictCursor) as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if row is None:
                raise DataDiffExecutionError("Aggregate query returned no row")
        values_by_name = {str(key).lower(): value for key, value in row.items()}
        return MetricQueryResult(
            values={check: canonical_value(values_by_name[check]) for check in checks},
            duration_seconds=perf_counter() - started,
        )

    def indexes(self, schema, table):  # pragma: no cover - targets are not preflighted
        return []

    def table_rows(self, schema, table):  # pragma: no cover - targets are not preflighted
        return 0


def _match_columns(schema, table, requested, available, *, allow_missing=False):
    normalized_available = [
        {str(key).lower(): value for key, value in dict(row).items()}
        for row in available
    ]
    matches = {}
    for requested_name in requested:
        exact = [
            row for row in normalized_available
            if row["column_name"] == requested_name
        ]
        candidates = exact or [
            row for row in normalized_available
            if row["column_name"].lower() == requested_name.lower()
        ]
        if not candidates and allow_missing:
            matches[requested_name] = {
                "name": requested_name,
                "data_type": None,
                "missing": True,
            }
            continue
        if len(candidates) != 1:
            raise DataDiffExecutionError(
                f"Column '{requested_name}' does not resolve uniquely in {schema}.{table}"
            )
        matches[requested_name] = dict(candidates[0])
        matches[requested_name].update(
            name=candidates[0]["column_name"],
            missing=False,
        )
    return matches
