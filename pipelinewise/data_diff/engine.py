"""Execute one bounded check: connect, preflight, then compare aggregates.

Connections are read-only, UTC, and statement-timeout bounded. ``run_check`` is the
entry point; ``preflight_source`` decides whether the source may be read at all and
is persisted before either aggregate runs.
"""

import hashlib
import json
import os
import ssl
import tempfile

from datetime import datetime

import psycopg2
import pymysql

from .adapters import (
    CHECKSUM_CHECK,
    MAX_SAFE_FULL_SCAN_ROWS,
    METRIC_EXPRESSIONS,
    SCHEMA_CHECK,
    TIMESTAMP_TYPES,
    DataDiffExecutionError,
    DatabaseAdapter,
    MetricQueryResult,
    MySQLAdapter,
    PostgresAdapter,
    SnowflakeAdapter,
    _match_columns,
    _quote,
    _utc_boundary,
    _validate_timestamp_type,
    canonical_value,
)
from .comparison import (
    UnsupportedComparisonError,
    checksum_columns,
    metric_passes,
    schema_compatibility_result,
)
from .credentials import pem_to_der

# Symbols re-exported from .adapters for use by callers and tests.
__all__ = [
    "TIMESTAMP_TYPES",
    "DatabaseAdapter",
    "MetricQueryResult",
    "_match_columns",
    "_quote",
    "canonical_value",
    "build_metric_query",
    "connect_source",
    "connect_target",
    "preflight_source",
    "run_check",
]


def _publish_preflight(on_preflight, preflight: dict) -> None:
    """Hand the decided preflight to the caller before any source read.

    Every branch that assigns a preflight must call this, including the
    metadata-only one: a run that persists no preflight leaves ``preflight_id``
    NULL, and the placeholder logic in the runner then treats a preflight that was
    computed as one that never ran.
    """
    if on_preflight is not None:
        on_preflight(preflight)


def _metadata_preflight(check: dict, column_pairs: list) -> dict:
    payload = {
        "source_schema": check["source_schema"],
        "source_table": check["source_table"],
        "target_schema": check["target_schema"],
        "target_table": check["target_table"],
        "columns": [logical_name for logical_name, _source, _target in column_pairs],
    }
    fingerprint = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return {
        "status": "PASS",
        "query_fingerprint": fingerprint,
        "index_metadata": [],
        "findings": ["Metadata-only check; no source data query was executed"],
        "table_rows": None,
        "row_limit": None,
        "has_leading_index": None,
    }


# pylint: disable=too-many-arguments
def build_metric_query(
    adapter: DatabaseAdapter,
    schema: str,
    table: str,
    key_column: str,
    timestamp_column: str,
    checks: tuple,
    *,
    checksum_columns_for_query: list = None,
) -> str:
    """Build a parameterized, half-open aggregate query."""
    key = adapter.quote(key_column)
    expressions = []
    for check in checks:
        if check == CHECKSUM_CHECK:
            expression = adapter.checksum_expression(checksum_columns_for_query or [])
        else:
            expression = METRIC_EXPRESSIONS[check].format(key=key)
        expressions.append(f"{expression} AS {adapter.quote(check)}")
    predicates = [
        f"{adapter.quote(timestamp_column)} >= %s",
        f"{adapter.quote(timestamp_column)} < %s",
    ]
    return (
        f"SELECT {', '.join(expressions)} "
        f"FROM {adapter.qualified_table(schema, table)} "
        f"WHERE {' AND '.join(predicates)}"
    )


def preflight_source(
    adapter: DatabaseAdapter,
    schema: str,
    table: str,
    timestamp_column: str,
    sql: str,
    params: tuple,
) -> dict:
    """Block a check whose source table is large and cannot be read by timestamp.

    Deliberately catalog metadata rather than a query plan. A plan reports rows
    *emitted* rather than rows read, so its estimates cannot bound source cost, and it
    costs a round trip per run. Table size and index shape are cheap and stable.

    This certifies the table is *readable* by timestamp, not that any given window
    will use the index: a window selecting most of the table is planned as a
    sequential scan, correctly. statement_timeout_seconds is the real cost bound.
    """
    # Hence the window is ignored -- readability is a property of the table, not of
    # one window, and a plan check per run would not change any verdict here.
    del params
    fingerprint = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    try:
        indexes = adapter.indexes(schema, table)
        table_rows = adapter.table_rows(schema, table)
        has_leading_index = any(
            index.get("is_usable")
            and index.get("columns")
            and index["columns"][0].lower() == timestamp_column.lower()
            for index in indexes
        )

        findings = []
        if not has_leading_index:
            unusable = [
                index["index_name"]
                for index in indexes
                if not index.get("is_usable")
                and index.get("columns")
                and index["columns"][0].lower() == timestamp_column.lower()
            ]
            findings.append(
                f"No usable source index starts with timestamp column "
                f"'{timestamp_column}'"
                + (
                    f"; ignored as unusable: {', '.join(unusable)}"
                    if unusable else ""
                )
            )
        blocked = not has_leading_index and table_rows > MAX_SAFE_FULL_SCAN_ROWS
        if blocked:
            findings.append(
                f"Source table holds {table_rows} rows (safe limit "
                f"{MAX_SAFE_FULL_SCAN_ROWS}) and every window must scan all of them. "
                "Add a source index beginning with the timestamp column; "
                "PipelineWise will not create it automatically"
            )
        return {
            "status": "BLOCKED" if blocked else "PASS",
            "query_fingerprint": fingerprint,
            "index_metadata": indexes,
            "findings": findings,
            # Persisted so a PASS is auditable: the size and the limit that let it
            # through, not only prose in the findings of a BLOCK.
            "table_rows": table_rows,
            "row_limit": MAX_SAFE_FULL_SCAN_ROWS,
            "has_leading_index": has_leading_index,
        }
    except Exception as exc:
        return {
            "status": "ERROR",
            "query_fingerprint": fingerprint,
            "index_metadata": [],
            "findings": [],
            "table_rows": None,
            "row_limit": MAX_SAFE_FULL_SCAN_ROWS,
            "has_leading_index": None,
            "error": str(exc),
        }


def connect_source(check: dict, connection_config: dict) -> DatabaseAdapter:
    """Open a source connection with UTC, read-only, and timeout controls."""
    # Never 0: PostgreSQL and MySQL both read 0 as "no limit", so a sub-second
    # configured budget would silently remove the timeout instead of tightening it.
    timeout = max(1, int(check["statement_timeout_seconds"]))
    if check["source_type"] == "tap-postgres":
        return _connect_postgres(
            connection_config,
            timeout,
            use_replica=True,
            application_name="pipelinewise-data-diff-source",
        )

    if check["source_type"] == "tap-mysql":
        connection_args = {
            "host": connection_config.get("replica_host", connection_config["host"]),
            "port": int(connection_config.get("replica_port", connection_config["port"])),
            "user": connection_config.get("replica_user", connection_config["user"]),
            "password": connection_config.get("replica_password", connection_config["password"]),
            "database": connection_config["dbname"],
            "charset": connection_config.get("charset", "utf8"),
            "cursorclass": pymysql.cursors.DictCursor,
            "read_timeout": timeout + 5,
            "write_timeout": timeout + 5,
            "autocommit": False,
        }
        ssl_context, ssl_temp_dir = _mysql_ssl_context(connection_config)
        # Opportunistic TLS, matching tap-mysql and FastSync. Without it a server
        # requiring encryption rejects us with a misleading "Access denied".
        connection_args["ssl"] = ssl_context if ssl_context else {"": True}
        try:
            connection = pymysql.connect(**connection_args)
        finally:
            if ssl_temp_dir:
                ssl_temp_dir.cleanup()
        with connection.cursor() as cursor:
            cursor.execute("SET SESSION time_zone = '+00:00'")
            if connection_config.get("engine", "mysql") == "mariadb":
                cursor.execute("SET SESSION max_statement_time = %s", (timeout,))
            else:
                cursor.execute("SET SESSION max_execution_time = %s", (timeout * 1000,))
            cursor.execute("START TRANSACTION READ ONLY")
        return MySQLAdapter(connection, timeout)

    raise DataDiffExecutionError(f"Unsupported source type: {check['source_type']}")


def _connect_postgres(
    connection_config: dict,
    timeout: int,
    *,
    use_replica: bool,
    application_name: str,
) -> PostgresAdapter:
    """Open a direct or replica PostgreSQL connection for bounded read-only work."""
    prefix = "replica_" if use_replica else ""
    connection_args = {
        "host": connection_config.get(f"{prefix}host", connection_config["host"]),
        "port": connection_config.get(f"{prefix}port", connection_config["port"]),
        "user": connection_config.get(f"{prefix}user", connection_config["user"]),
        "password": connection_config.get(
            f"{prefix}password", connection_config["password"]
        ),
        "dbname": connection_config["dbname"],
        "connect_timeout": min(timeout, 30),
        "application_name": application_name,
    }
    if connection_config.get("ssl") == "true":
        connection_args["sslmode"] = "require"
    connection = psycopg2.connect(**connection_args)
    connection.set_session(readonly=True, autocommit=False)
    with connection.cursor() as cursor:
        cursor.execute("SELECT set_config('TimeZone', 'UTC', true)")
        cursor.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            (f"{timeout}s",),
        )
    return PostgresAdapter(connection, timeout)


def _mysql_ssl_context(connection_config: dict):
    if connection_config.get("ssl") != "true" and not connection_config.get("ssl_ca"):
        return None, None

    ca_value = connection_config.get("ssl_ca")
    if ca_value and os.path.exists(ca_value):
        context = ssl.create_default_context(cafile=ca_value)
    elif ca_value:
        context = ssl.create_default_context(cadata=ca_value)
    else:
        context = ssl.create_default_context()

    certificate = connection_config.get("ssl_cert")
    private_key = connection_config.get("ssl_key")
    if not certificate and not private_key:
        return context, None
    if not certificate or not private_key:
        raise DataDiffExecutionError("MySQL ssl_cert and ssl_key must be configured together")

    temp_dir = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
        prefix="pipelinewise-data-diff-tls-"
    )
    try:
        certificate_path = certificate
        private_key_path = private_key
        if not os.path.exists(certificate):
            certificate_path = os.path.join(temp_dir.name, "client-cert.pem")
            with open(certificate_path, "w", encoding="utf-8") as certificate_file:
                certificate_file.write(certificate)
        if not os.path.exists(private_key):
            private_key_path = os.path.join(temp_dir.name, "client-key.pem")
            with open(private_key_path, "w", encoding="utf-8") as private_key_file:
                private_key_file.write(private_key)
        context.load_cert_chain(certificate_path, private_key_path)
    except Exception:
        temp_dir.cleanup()
        raise
    return context, temp_dir


def connect_target(check: dict, connection_config: dict) -> DatabaseAdapter:
    """Open a supported read-only target connection with UTC and timeout controls."""
    # Never 0, which both engines read as "no limit". See connect_source.
    timeout = max(1, int(check["statement_timeout_seconds"]))
    if check["target_type"] == "target-postgres":
        return _connect_postgres(
            connection_config,
            timeout,
            use_replica=False,
            application_name="pipelinewise-data-diff-target",
        )
    if check["target_type"] != "target-snowflake":
        raise DataDiffExecutionError(
            f"Unsupported target type: {check['target_type']}"
        )

    import snowflake.connector  # pylint: disable=import-outside-toplevel

    connect_args = {
        "user": connection_config["user"],
        "account": connection_config["account"],
        "database": connection_config["dbname"],
        "warehouse": connection_config["warehouse"],
        "role": connection_config.get("role"),
        "autocommit": True,
        "session_parameters": {
            "TIMEZONE": "UTC",
            "STATEMENT_TIMEOUT_IN_SECONDS": timeout,
            "QUOTED_IDENTIFIERS_IGNORE_CASE": "FALSE",
            "QUERY_TAG": json.dumps(
                {"ppw_component": "data-diff", "check_id": str(check["check_id"])}
            ),
        },
    }
    if connection_config.get("private_key"):
        connect_args.update(
            authenticator="SNOWFLAKE_JWT",
            private_key=pem_to_der(connection_config["private_key"]),
        )
    elif connection_config.get("password"):
        connect_args["password"] = connection_config["password"]
    else:
        raise DataDiffExecutionError("Snowflake target needs private_key or password authentication")
    return SnowflakeAdapter(snowflake.connector.connect(**connect_args), timeout)


def _unique_columns(columns: list) -> list:
    seen = set()
    result = []
    for column in columns:
        normalized = column.lower()
        if normalized not in seen:
            seen.add(normalized)
            result.append(column)
    return result


def _require_resolved(columns: dict, name: str, qualified_table: str) -> dict:
    column = columns[name]
    if column.get("missing"):
        raise DataDiffExecutionError(
            f"Column '{name}' does not exist in {qualified_table}"
        )
    return column


# pylint: disable=too-many-branches,too-many-locals,too-many-statements
def run_check(
    check: dict,
    source_config: dict,
    target_config: dict,
    window_start: datetime,
    window_end: datetime,
    on_preflight=None,
) -> tuple:
    """Preflight and execute one bounded aggregate query on each side.

    ``on_preflight`` is invoked with the preflight dict as soon as it is decided
    and before either aggregate runs, so the caller can persist it while the
    source is still untouched. Its return value is ignored.
    """
    source = connect_source(check, source_config)
    try:
        target = connect_target(check, target_config)
    except Exception:
        source.close()
        raise
    try:
        source_compare = list(check.get("source_compare_columns") or [])
        target_compare = list(check.get("target_compare_columns") or [])
        source_requested = _unique_columns([
            check["source_key_column"], check["source_timestamp_column"],
            *source_compare,
        ])
        target_requested = _unique_columns([
            check["target_key_column"], check["target_timestamp_column"],
            *target_compare,
        ])

        source_columns = source.resolve_columns(
            check["source_schema"], check["source_table"], source_requested,
            allow_missing=True,
        )
        target_columns = target.resolve_columns(
            check["target_schema"], check["target_table"], target_requested,
            allow_missing=True,
        )
        source_table_name = f"{check['source_schema']}.{check['source_table']}"
        target_table_name = f"{check['target_schema']}.{check['target_table']}"
        source_key_column = _require_resolved(
            source_columns, check["source_key_column"], source_table_name
        )
        source_timestamp_column = _require_resolved(
            source_columns, check["source_timestamp_column"], source_table_name
        )
        target_key_column = _require_resolved(
            target_columns, check["target_key_column"], target_table_name
        )
        target_timestamp_column = _require_resolved(
            target_columns, check["target_timestamp_column"], target_table_name
        )

        _validate_timestamp_type(
            source_timestamp_column,
            f"{source_table_name}.{check['source_timestamp_column']}",
        )
        _validate_timestamp_type(
            target_timestamp_column,
            f"{target_table_name}.{check['target_timestamp_column']}",
        )

        column_pairs = [
            (check["source_key_column"], source_key_column, target_key_column),
            (
                check["source_timestamp_column"],
                source_timestamp_column,
                target_timestamp_column,
            ),
        ]
        paired_names = {
            check["source_key_column"].lower(),
            check["source_timestamp_column"].lower(),
        }
        for source_name, target_name in zip(source_compare, target_compare):
            if source_name.lower() in paired_names:
                continue
            paired_names.add(source_name.lower())
            column_pairs.append(
                (source_name, source_columns[source_name], target_columns[target_name])
            )

        checks = tuple(check["checks"])
        results_by_type = {}
        if SCHEMA_CHECK in checks:
            results_by_type[SCHEMA_CHECK] = schema_compatibility_result(column_pairs)

        source_checksum_columns = []
        target_checksum_columns = []
        metric_checks = [item for item in checks if item != SCHEMA_CHECK]
        if CHECKSUM_CHECK in metric_checks:
            try:
                source_checksum_columns, target_checksum_columns = checksum_columns(
                    column_pairs
                )
            except UnsupportedComparisonError as exc:
                metric_checks.remove(CHECKSUM_CHECK)
                results_by_type[CHECKSUM_CHECK] = {
                    "check_type": CHECKSUM_CHECK,
                    "status": "ERROR",
                    "source_value": None,
                    "target_value": None,
                    "source_query_seconds": None,
                    "target_query_seconds": None,
                    "error": str(exc),
                }

        source_params = (
            _utc_boundary(window_start, source_timestamp_column["data_type"]),
            _utc_boundary(window_end, source_timestamp_column["data_type"]),
        )
        target_params = (
            _utc_boundary(window_start, target_timestamp_column["data_type"]),
            _utc_boundary(window_end, target_timestamp_column["data_type"]),
        )

        if metric_checks:
            metric_checks = tuple(metric_checks)
            source_sql = build_metric_query(
                source, check["source_schema"], check["source_table"],
                source_key_column["name"], source_timestamp_column["name"],
                metric_checks, checksum_columns_for_query=source_checksum_columns,
            )
            target_sql = build_metric_query(
                target, check["target_schema"], check["target_table"],
                target_key_column["name"], target_timestamp_column["name"],
                metric_checks,
                checksum_columns_for_query=target_checksum_columns,
            )
            preflight = preflight_source(
                source, check["source_schema"], check["source_table"],
                source_timestamp_column["name"], source_sql, source_params,
            )
            _publish_preflight(on_preflight, preflight)
            if preflight["status"] != "PASS":
                return preflight, [], None

            source_result = source.execute_metrics(
                source_sql, source_params, metric_checks
            )
            target_result = target.execute_metrics(
                target_sql, target_params, metric_checks
            )
            for check_type in metric_checks:
                source_value = source_result.values[check_type]
                target_value = target_result.values[check_type]
                passed = metric_passes(check_type, source_value, target_value)
                results_by_type[check_type] = {
                    "check_type": check_type,
                    "status": "PASS" if passed else "FAIL",
                    "source_value": source_value,
                    "target_value": target_value,
                    "source_query_seconds": source_result.duration_seconds,
                    "target_query_seconds": target_result.duration_seconds,
                    "error": None,
                }
        else:
            preflight = _metadata_preflight(check, column_pairs)
            _publish_preflight(on_preflight, preflight)

        results = [results_by_type[check_type] for check_type in checks]
        statuses = {result["status"] for result in results}
        status = "ERROR" if "ERROR" in statuses else ("FAIL" if "FAIL" in statuses else "PASS")
        return preflight, results, status
    finally:
        try:
            source.connection.rollback()
        finally:
            source.close()
            target.close()
