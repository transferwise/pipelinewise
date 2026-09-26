from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from pipelinewise.data_diff.comparison import (
    UnsupportedComparisonError,
    checksum_columns,
    metric_passes,
    schema_compatibility_result,
)
from pipelinewise.data_diff.engine import (
    MAX_SAFE_FULL_SCAN_ROWS,
    DatabaseAdapter,
    DataDiffExecutionError,
    HistoricalWindowNotReady,
    MetricQueryResult,
    MySQLAdapter,
    PostgresAdapter,
    SnowflakeAdapter,
    _ordered_results,
    build_metric_query,
    canonical_value,
    connect_source,
    connect_target,
    preflight_source,
    run_check,
)


def _usable_index(columns, name="idx", **overrides):
    """Index metadata as a healthy btree index reports it."""
    return {"index_name": name, "columns": columns, "is_usable": True, **overrides}


class FakeAdapter(DatabaseAdapter):
    def __init__(self, *, indexes, table_rows=0):
        self._indexes = indexes
        self._table_rows = table_rows

    def indexes(self, _schema, _table):
        return self._indexes

    def table_rows(self, _schema, _table):
        return self._table_rows


class RunAdapter(FakeAdapter):
    def __init__(self, columns, values=None, minimum=None):
        super().__init__(indexes=[])
        self.columns = columns
        self.values = values or {}
        self.connection = Mock()
        self.executed = []
        self.minimum = minimum
        self.minimum_queries = []

    def resolve_columns(self, _schema, _table, requested, *, allow_missing=False):
        resolved = {}
        for name in requested:
            if name in self.columns:
                resolved[name] = self.columns[name]
            elif allow_missing:
                resolved[name] = {"name": name, "data_type": None, "missing": True}
            else:
                raise AssertionError(name)
        return resolved

    def execute_metrics(self, sql, params, checks):
        self.executed.append((sql, params, checks))
        return MetricQueryResult(
            values={check: self.values[check] for check in checks},
            duration_seconds=0.25,
        )

    def minimum_timestamp(self, schema, table, column, cutoff):
        self.minimum_queries.append((schema, table, column, cutoff))
        return self.minimum

    def close(self):
        self.connection.close()


def test_final_results_require_every_declared_check():
    with pytest.raises(KeyError, match='row_count'):
        _ordered_results({}, ('row_count',))

    assert _ordered_results({}, ('row_count',), allow_missing=True) == []


def test_metric_query_is_half_open_parameterized_and_quotes_identifiers():
    adapter = FakeAdapter(indexes=[])
    sql = build_metric_query(
        adapter,
        'public',
        'order"items',
        'select',
        'updated_at',
        (
            "row_count", "distinct_key_count", "null_key_count",
            "duplicate_key_count", "min_key", "max_key",
        ),
    )

    assert 'FROM "public"."order""items"' in sql
    assert '"updated_at" >= %s' in sql
    assert '"updated_at" < %s' in sql
    assert "COUNT(DISTINCT \"select\")" in sql
    assert 'COUNT(*) - COUNT("select") AS "null_key_count"' in sql
    assert (
        'COUNT("select") - COUNT(DISTINCT "select") AS "duplicate_key_count"'
        in sql
    )


@pytest.mark.parametrize("adapter_type", [PostgresAdapter, MySQLAdapter, SnowflakeAdapter])
def test_metric_query_excludes_null_timestamps_for_every_adapter(adapter_type):
    adapter = adapter_type(None, 30)
    sql = build_metric_query(
        adapter, "public", "payments", "id", "updated_at", ("row_count",),
    )

    timestamp = adapter.quote("updated_at")
    assert f"{timestamp} < %s" in sql
    assert "IS NULL" not in sql
    assert f"{timestamp} >= %s" in sql
    assert sql.count("%s") == 2


@pytest.mark.parametrize("adapter_type", [PostgresAdapter, MySQLAdapter, SnowflakeAdapter])
@pytest.mark.parametrize("as_dict", [False, True])
def test_minimum_timestamp_is_parameterized_and_normalizes_driver_values(adapter_type, as_dict):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    value = datetime(2026, 7, 1, 8, tzinfo=timezone(timedelta(hours=2)))
    cursor.fetchone.return_value = {"MIN(timestamp)": value} if as_dict else (value,)
    adapter = adapter_type(connection, 30)
    cutoff = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    column = _column('updated"at', "timestamp without time zone")

    minimum = adapter.minimum_timestamp("public", "payments", column, cutoff)

    assert minimum == datetime(2026, 7, 1, 6, tzinfo=timezone.utc)
    sql, params = cursor.execute.call_args.args
    quoted = adapter.quote(column["name"])
    assert f"SELECT MIN({quoted})" in sql
    assert f"WHERE {quoted} < %s" in sql
    assert params == (cutoff.replace(tzinfo=None),)


@pytest.mark.parametrize("value", [None, datetime(2026, 7, 1, 6)])
def test_minimum_timestamp_handles_empty_and_naive_results(value):
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (value,)
    adapter = PostgresAdapter(connection, 30)
    cutoff = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)

    result = adapter.minimum_timestamp("public", "payments", _column("ts", "timestamp"), cutoff)

    assert result == (None if value is None else value.replace(tzinfo=timezone.utc))


def _column(name, data_type, **metadata):
    return {
        "name": name,
        "data_type": data_type,
        "missing": False,
        **metadata,
    }


def test_schema_compatibility_uses_replication_type_families_and_reports_missing():
    compatible = schema_compatibility_result([
        ("id", _column("id", "bigint"), _column("ID", "NUMBER")),
        (
            "created_at",
            _column("created_at", "date"),
            _column("CREATED_AT", "TIMESTAMP_NTZ"),
        ),
        ("status", _column("status", "text"), _column("STATUS", "VARCHAR")),
    ])
    missing = schema_compatibility_result([
        (
            "status",
            _column("status", "text"),
            {"name": "STATUS", "data_type": None, "missing": True},
        )
    ])

    assert compatible["status"] == "PASS"
    assert missing["status"] == "FAIL"
    assert missing["error"] is not None
    assert "target missing" in str(missing["error"])
    assert missing["target_value"]["columns"][0]["resolved_name"] is None


def test_checksum_uses_common_typed_normalization_for_each_dialect():
    pairs = [
        (
            "id",
            _column("id", "bigint", numeric_scale=0),
            _column("ID", "NUMBER", numeric_scale=0),
        ),
        (
            "updated_at",
            _column("updated_at", "timestamp without time zone"),
            _column("UPDATED_AT", "TIMESTAMP_NTZ"),
        ),
        ("status", _column("status", "text"), _column("STATUS", "VARCHAR")),
    ]
    source_columns, target_columns = checksum_columns(pairs)

    postgres_sql = build_metric_query(
        FakeAdapter(indexes=[]),
        "public", "payments", "id", "updated_at", ("row_checksum",),
        checksum_columns_for_query=source_columns,
    )
    mysql_sql = build_metric_query(
        MySQLAdapter(None, 30),
        "public", "payments", "id", "updated_at", ("row_checksum",),
        checksum_columns_for_query=source_columns,
    )
    snowflake_sql = build_metric_query(
        SnowflakeAdapter(None, 30),
        "PUBLIC", "PAYMENTS", "ID", "UPDATED_AT", ("row_checksum",),
        checksum_columns_for_query=target_columns,
    )
    postgres_target_sql = build_metric_query(
        PostgresAdapter(None, 30),
        "replicated", "payments", "id", "updated_at", ("row_checksum",),
        checksum_columns_for_query=target_columns,
    )

    assert "SUBSTRING(MD5(" in postgres_sql
    assert "TO_CHAR(\"updated_at\", 'YYYY-MM-DD HH24:MI:SS.US')" in postgres_sql
    assert "CONV(SUBSTRING(MD5(" in mysql_sql
    # Doubled in the SQL, single once PyMySQL interpolates the bound parameters.
    assert "DATE_FORMAT(`updated_at`, '%%Y-%%m-%%d %%H:%%i:%%s.%%f')" in mysql_sql
    assert "DATE_FORMAT(`updated_at`, '%Y-%m-%d %H:%i:%s.%f')" in mysql_sql % (
        "2026-07-01", "2026-07-02",
    )
    assert "MD5_NUMBER_LOWER64" in snowflake_sql
    assert "CONVERT_TIMEZONE('UTC', \"UPDATED_AT\")" in snowflake_sql
    assert "CASE WHEN" in postgres_sql
    assert "SUBSTRING(MD5(" in postgres_target_sql
    assert '"replicated"."payments"' in postgres_target_sql


def test_checksum_rejects_approximate_and_missing_columns():
    with pytest.raises(UnsupportedComparisonError, match="unsupported type family"):
        checksum_columns([
            (
                "amount",
                _column("amount", "double precision"),
                _column("AMOUNT", "FLOAT"),
            )
        ])

    with pytest.raises(UnsupportedComparisonError, match="incompatible"):
        checksum_columns([
            (
                "status",
                _column("status", "text"),
                {"name": "STATUS", "data_type": None, "missing": True},
            )
        ])

    missing = {"name": "status", "data_type": None, "missing": True}
    with pytest.raises(UnsupportedComparisonError) as exc:
        checksum_columns([("status", missing, missing)])
    assert str(exc.value) == (
        "row_checksum column 'status' has incompatible source and target types "
        "(missing, missing)"
    )


def test_key_integrity_checks_require_zero_on_both_sides():
    assert metric_passes("row_count", "2", "2")
    assert metric_passes("null_key_count", "0", "0")
    assert metric_passes("duplicate_key_count", 0, Decimal("0"))
    assert not metric_passes("null_key_count", "1", "1")
    assert not metric_passes("duplicate_key_count", "0", "2")


@pytest.mark.parametrize("historical", [False, True])
def test_schema_only_run_uses_metadata_without_executing_aggregate_query(historical):
    source = RunAdapter({
        "id": _column("id", "bigint", numeric_scale=0),
        "updated_at": _column("updated_at", "timestamp without time zone"),
        "status": _column("status", "text"),
    })
    target = RunAdapter({
        "ID": _column("ID", "NUMBER", numeric_scale=0),
        "UPDATED_AT": _column("UPDATED_AT", "TIMESTAMP_NTZ"),
        "STATUS": _column("STATUS", "VARCHAR"),
    })
    check = {
        "source_schema": "public",
        "source_table": "payments",
        "target_schema": "PUBLIC",
        "target_table": "PAYMENTS",
        "source_key_column": "id",
        "target_key_column": "ID",
        "source_timestamp_column": "updated_at",
        "target_timestamp_column": "UPDATED_AT",
        "source_compare_columns": ["status"],
        "target_compare_columns": ["STATUS"],
        "checks": ["schema_compatibility"],
    }
    start = None if historical else datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    end = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)

    with patch(
        "pipelinewise.data_diff.engine.connect_source",
        return_value=source,
    ), patch(
        "pipelinewise.data_diff.engine.connect_target",
        return_value=target,
    ):
        preflight, results, status = run_check(check, {}, {}, start, end)

    assert status == "PASS"
    assert results[0]["check_type"] == "schema_compatibility"
    assert "Metadata-only" in preflight["findings"][0]
    assert source.executed == []
    assert target.executed == []
    assert source.minimum_queries == target.minimum_queries == []


@pytest.mark.parametrize("historical", [False, True])
@pytest.mark.parametrize("later_side", ["source", "target"])
def test_run_check_binds_the_same_window_on_source_and_target(historical, later_side):
    source = RunAdapter({
        "id": _column("id", "bigint"),
        "updated_at": _column("updated_at", "timestamp without time zone"),
    }, values={"row_count": "2"})
    target = RunAdapter({
        "ID": _column("ID", "NUMBER"),
        "UPDATED_AT": _column("UPDATED_AT", "TIMESTAMP_NTZ"),
    }, values={"row_count": "2"})
    check = {
        "source_schema": "public", "source_table": "payments",
        "target_schema": "PUBLIC", "target_table": "PAYMENTS",
        "source_key_column": "id", "target_key_column": "ID",
        "source_timestamp_column": "updated_at",
        "target_timestamp_column": "UPDATED_AT",
        "checks": ["row_count"],
    }
    regular_start = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    earlier_start = regular_start - timedelta(days=10)
    overlap_start = regular_start - timedelta(days=7)
    source.minimum = overlap_start if later_side == "source" else earlier_start
    target.minimum = overlap_start if later_side == "target" else earlier_start
    window_start = None if historical else regular_start
    window_end = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    published = []
    resolved = []

    def record_preflight(preflight):
        assert source.minimum_queries == target.minimum_queries == []
        assert source.executed == target.executed == []
        published.append(preflight)

    def record_start(start):
        assert len(published) == 1
        assert source.executed == target.executed == []
        resolved.append(start)

    with patch(
        "pipelinewise.data_diff.engine.connect_source", return_value=source,
    ), patch(
        "pipelinewise.data_diff.engine.connect_target", return_value=target,
    ):
        preflight, results, status = run_check(
            check, {}, {}, window_start, window_end,
            on_preflight=record_preflight, on_window_start=record_start,
        )

    assert preflight["status"] == status == "PASS"
    assert published == [preflight]
    assert results[0]["status"] == "PASS"
    expected_start = overlap_start if historical else regular_start
    expected_params = (expected_start.replace(tzinfo=None), window_end.replace(tzinfo=None))
    assert resolved == ([overlap_start] if historical else [])
    assert len(source.minimum_queries) == len(target.minimum_queries) == int(historical)
    for sql, params, checks in (source.executed[0], target.executed[0]):
        assert params == expected_params
        assert checks == ("row_count",)
        assert " < %s" in sql
        assert " >= %s" in sql
        assert "IS NULL" not in sql


@pytest.mark.parametrize("empty_side", ["source", "target", "both"])
@pytest.mark.parametrize("checks", [["row_count"], ["schema_compatibility", "row_count"]])
def test_historical_scan_does_not_pass_without_timestamped_data_on_both_sides(empty_side, checks):
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    end = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    columns = {"id": _column("id", "bigint"), "ts": _column("ts", "timestamp")}
    source = RunAdapter(columns, minimum=None if empty_side in ("source", "both") else start)
    target = RunAdapter(columns, minimum=None if empty_side in ("target", "both") else start)
    check = {
        "source_schema": "public", "source_table": "payments",
        "target_schema": "public", "target_table": "payments",
        "source_key_column": "id", "target_key_column": "id",
        "source_timestamp_column": "ts", "target_timestamp_column": "ts",
        "checks": checks,
    }
    expected_error = HistoricalWindowNotReady if empty_side == "both" else DataDiffExecutionError
    with patch("pipelinewise.data_diff.engine.connect_source", return_value=source), patch(
        "pipelinewise.data_diff.engine.connect_target", return_value=target,
    ), pytest.raises(expected_error, match="non-NULL timestamp before the cutoff") as raised:
        run_check(check, {}, {}, None, end)

    assert type(raised.value) is expected_error
    assert end.isoformat() in str(raised.value)
    assert ("both source and target" if empty_side == "both" else f"{empty_side} has no") in str(raised.value)
    assert source.executed == target.executed == []


@pytest.mark.parametrize(
    "checks,source_type,target_type,expected_results",
    [
        (["schema_compatibility", "row_count"], "text", "bigint", [("schema_compatibility", "FAIL")]),
        (["row_checksum", "row_count"], "double precision", "double precision", [("row_checksum", "ERROR")]),
    ],
)
def test_empty_history_keeps_known_metadata_failures(checks, source_type, target_type, expected_results):
    columns = {"id": _column("id", "bigint"), "ts": _column("ts", "timestamp")}
    source = RunAdapter({**columns, "value": _column("value", source_type)})
    target = RunAdapter({**columns, "value": _column("value", target_type)})
    check = {
        "source_schema": "public", "source_table": "payments",
        "target_schema": "public", "target_table": "payments",
        "source_key_column": "id", "target_key_column": "id",
        "source_timestamp_column": "ts", "target_timestamp_column": "ts",
        "source_compare_columns": ["value"], "target_compare_columns": ["value"],
        "checks": checks,
    }
    resolved = []
    with patch("pipelinewise.data_diff.engine.connect_source", return_value=source), patch(
        "pipelinewise.data_diff.engine.connect_target", return_value=target,
    ):
        preflight, results, status = run_check(
            check, {}, {}, None, datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
            on_window_start=resolved.append,
        )

    assert preflight["status"] == "PASS"
    assert status == "ERROR"
    assert [(result["check_type"], result["status"]) for result in results] == expected_results
    assert results[0]["error"]
    assert resolved == []
    assert len(source.minimum_queries) == len(target.minimum_queries) == 1
    assert source.executed == target.executed == []


def test_blocked_historical_preflight_does_not_read_minimum_timestamps():
    columns = {"id": _column("id", "bigint"), "ts": _column("ts", "timestamp")}
    source, target = RunAdapter(columns), RunAdapter(columns)
    source._table_rows = 100_001
    check = {
        "source_schema": "public", "source_table": "payments",
        "target_schema": "public", "target_table": "payments",
        "source_key_column": "id", "target_key_column": "id",
        "source_timestamp_column": "ts", "target_timestamp_column": "ts",
        "checks": ["row_count"],
    }
    with patch("pipelinewise.data_diff.engine.connect_source", return_value=source), patch(
        "pipelinewise.data_diff.engine.connect_target", return_value=target,
    ):
        preflight, results, status = run_check(
            check, {}, {}, None,
            datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
        )
    assert preflight["status"] == "BLOCKED"
    assert results == []
    assert status is None
    assert source.minimum_queries == target.minimum_queries == []


@pytest.mark.parametrize("discover", [False, True])
def test_year_one_is_a_real_historical_boundary_and_is_not_rediscovered(discover):
    start = datetime.min.replace(tzinfo=timezone.utc)
    end = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)
    columns = {"id": _column("id", "bigint"), "ts": _column("ts", "timestamp without time zone")}
    source = RunAdapter(columns, {"row_count": "2"}, minimum=start)
    target = RunAdapter(columns, {"row_count": "2"}, minimum=start)
    check = {
        "source_schema": "public", "source_table": "payments",
        "target_schema": "public", "target_table": "payments",
        "source_key_column": "id", "target_key_column": "id",
        "source_timestamp_column": "ts", "target_timestamp_column": "ts",
        "checks": ["row_count"],
    }
    resolved = []
    with patch("pipelinewise.data_diff.engine.connect_source", return_value=source), patch(
        "pipelinewise.data_diff.engine.connect_target", return_value=target,
    ):
        _preflight, _results, status = run_check(
            check, {}, {}, None if discover else start, end, on_window_start=resolved.append,
        )

    assert status == "PASS"
    assert resolved == ([start] if discover else [])
    assert len(source.minimum_queries) == len(target.minimum_queries) == int(discover)
    assert source.executed[0][1] == target.executed[0][1] == (
        start.replace(tzinfo=None), end.replace(tzinfo=None),
    )


def test_unsupported_historical_metrics_do_not_discover_or_convert_window_bounds():
    columns = {
        "id": _column("id", "bigint"), "ts": _column("ts", "timestamp"),
        "value": _column("value", "double precision"),
    }
    source, target = RunAdapter(columns), RunAdapter(columns)
    check = {
        "source_schema": "public", "source_table": "payments",
        "target_schema": "public", "target_table": "payments",
        "source_key_column": "id", "target_key_column": "id",
        "source_timestamp_column": "ts", "target_timestamp_column": "ts",
        "source_compare_columns": ["value"], "target_compare_columns": ["value"],
        "checks": ["row_checksum"],
    }
    published = []
    with patch("pipelinewise.data_diff.engine.connect_source", return_value=source), patch(
        "pipelinewise.data_diff.engine.connect_target", return_value=target,
    ), patch("pipelinewise.data_diff.engine._utc_boundary") as convert_boundary:
        preflight, results, status = run_check(
            check, {}, {}, None, datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
            on_preflight=published.append,
        )

    assert status == "ERROR"
    assert results[0]["status"] == "ERROR"
    assert "unsupported type family" in results[0]["error"]
    assert published == [preflight]
    assert source.minimum_queries == target.minimum_queries == []
    assert source.executed == target.executed == []
    convert_boundary.assert_not_called()


@pytest.mark.parametrize("failing_side", ["source", "target"])
def test_historical_minimum_failure_keeps_the_start_unresolved_and_closes_connections(failing_side):
    columns = {"id": _column("id", "bigint"), "ts": _column("ts", "timestamp")}
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    source = RunAdapter(columns, minimum=start)
    target = RunAdapter(columns, minimum=start)
    failing = source if failing_side == "source" else target
    failing.minimum_timestamp = Mock(side_effect=DataDiffExecutionError("MIN query timed out"))
    check = {
        "source_schema": "public", "source_table": "payments",
        "target_schema": "public", "target_table": "payments",
        "source_key_column": "id", "target_key_column": "id",
        "source_timestamp_column": "ts", "target_timestamp_column": "ts",
        "checks": ["row_count"],
    }
    published, resolved = [], []
    with patch("pipelinewise.data_diff.engine.connect_source", return_value=source), patch(
        "pipelinewise.data_diff.engine.connect_target", return_value=target,
    ), pytest.raises(DataDiffExecutionError, match="MIN query timed out"):
        run_check(
            check, {}, {}, None, datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
            on_preflight=published.append, on_window_start=resolved.append,
        )

    assert len(published) == 1
    assert published[0]["status"] == "PASS"
    assert resolved == []
    assert source.executed == target.executed == []
    source.connection.rollback.assert_called_once()
    source.connection.close.assert_called_once()
    target.connection.close.assert_called_once()


def test_run_check_closes_source_when_target_connect_fails():
    source = RunAdapter({})
    check = {"source_schema": "public", "source_table": "payments"}
    start = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    end = datetime(2026, 7, 22, 13, tzinfo=timezone.utc)

    with patch(
        "pipelinewise.data_diff.engine.connect_source",
        return_value=source,
    ), patch(
        "pipelinewise.data_diff.engine.connect_target",
        side_effect=DataDiffExecutionError("target unavailable"),
    ):
        with pytest.raises(DataDiffExecutionError):
            run_check(check, {}, {}, start, end)

    source.connection.close.assert_called_once()


def test_preflight_blocks_only_large_tables_without_a_timestamp_index():
    blocked = preflight_source(
        FakeAdapter(indexes=[], table_rows=100_001),
        "public", "payments", "updated_at", "SELECT 1", (),
    )
    small = preflight_source(
        FakeAdapter(indexes=[], table_rows=100),
        "public", "payments", "updated_at", "SELECT 1", (),
    )
    # A leading timestamp index makes windowed reads possible at any size.
    indexed = preflight_source(
        FakeAdapter(indexes=[_usable_index(["updated_at", "id"])], table_rows=10_000_000),
        "public", "payments", "updated_at", "SELECT 1", (),
    )

    assert blocked["status"] == "BLOCKED"
    assert "100001 rows" in " ".join(blocked["findings"])
    assert small["status"] == "PASS"
    assert indexed["status"] == "PASS"


def test_preflight_reports_a_missing_index_without_blocking_a_small_table():
    preflight = preflight_source(
        FakeAdapter(indexes=[_usable_index(["id"])], table_rows=500),
        "public", "payments", "updated_at", "SELECT 1", (),
    )

    assert preflight["status"] == "PASS"
    assert "No usable source index starts with timestamp column" in preflight["findings"][0]


def test_preflight_records_an_error_without_blocking_the_run_silently():
    class FailingAdapter(FakeAdapter):
        def indexes(self, _schema, _table):
            raise RuntimeError("catalog unavailable")

    preflight = preflight_source(
        FailingAdapter(indexes=[]),
        "public", "payments", "updated_at", "SELECT 1", (),
    )

    assert preflight["status"] == "ERROR"
    assert preflight["error"] == "catalog unavailable"


def test_canonical_value_normalizes_numbers_and_timestamps_to_utc():
    instant = datetime(2026, 7, 22, 14, 0, tzinfo=timezone(timedelta(hours=2)))

    assert canonical_value(Decimal("1.000")) == "1"
    assert canonical_value(1) == canonical_value(Decimal("1"))
    assert canonical_value(1.50) == "1.5"
    assert canonical_value(instant) == "2026-07-22T12:00:00Z"
    assert canonical_value(datetime(2026, 7, 22, 12, 0)) == "2026-07-22T12:00:00Z"


def _connection_with_cursor(server_version="8.0.39"):
    cursor = Mock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    connection = Mock(closed=False)
    connection.cursor.return_value = cursor_context
    connection.get_server_info.return_value = server_version
    return connection, cursor


def test_postgres_source_is_read_only_utc_and_time_limited():
    connection, cursor = _connection_with_cursor()
    check = {"source_type": "tap-postgres", "statement_timeout_seconds": 300}
    config = {
        "host": "primary",
        "replica_host": "replica",
        "port": 5432,
        "user": "reader",
        "password": "secret",
        "dbname": "payments",
    }

    with patch(
        "pipelinewise.data_diff.engine.psycopg2.connect",
        return_value=connection,
    ) as connect:
        connect_source(check, config)

    assert connect.call_args.kwargs["host"] == "replica"
    connection.set_session.assert_called_once_with(readonly=True, autocommit=False)
    assert cursor.execute.call_args_list == [
        call("SELECT set_config('TimeZone', 'UTC', true)"),
        call("SELECT set_config('statement_timeout', %s, true)", ("300s",)),
    ]


@pytest.mark.parametrize(
    "server_version",
    ["11.4.10-MariaDB-log", "5.5.5-10.6.18-MariaDB-ubu2004-log"],
)
def test_mariadb_source_is_detected_and_time_limited_without_engine(server_version):
    connection, cursor = _connection_with_cursor(server_version)
    check = {"source_type": "tap-mysql", "statement_timeout_seconds": 120}
    config = {
        "host": "source",
        "port": 3306,
        "user": "reader",
        "password": "secret",
        "dbname": "payments",
    }

    with patch(
        "pipelinewise.data_diff.engine.pymysql.connect",
        return_value=connection,
    ):
        connect_source(check, config)

    assert cursor.execute.call_args_list == [
        call("SET SESSION time_zone = '+00:00'"),
        call("SET SESSION max_statement_time = %s", (120,)),
        call("START TRANSACTION READ ONLY"),
    ]


def test_mysql_source_is_detected_and_time_limited_without_engine():
    connection, cursor = _connection_with_cursor("8.0.39")
    check = {"source_type": "tap-mysql", "statement_timeout_seconds": 120}
    config = {
        "host": "source",
        "port": 3306,
        "user": "reader",
        "password": "secret",
        "dbname": "payments",
    }

    with patch(
        "pipelinewise.data_diff.engine.pymysql.connect",
        return_value=connection,
    ):
        connect_source(check, config)

    assert cursor.execute.call_args_list == [
        call("SET SESSION time_zone = '+00:00'"),
        call("SET SESSION max_execution_time = %s", (120_000,)),
        call("START TRANSACTION READ ONLY"),
    ]


@pytest.mark.parametrize(
    "engine,server_version,timeout_call",
    [
        (
            "mariadb",
            "8.0.39",
            call("SET SESSION max_statement_time = %s", (120,)),
        ),
        (
            "mysql",
            "11.4.10-MariaDB-log",
            call("SET SESSION max_execution_time = %s", (120_000,)),
        ),
    ],
)
def test_explicit_engine_overrides_server_version(
    engine, server_version, timeout_call
):
    connection, cursor = _connection_with_cursor(server_version)
    check = {"source_type": "tap-mysql", "statement_timeout_seconds": 120}
    config = {
        "host": "source",
        "port": 3306,
        "user": "reader",
        "password": "secret",
        "dbname": "payments",
        "engine": engine,
    }

    with patch(
        "pipelinewise.data_diff.engine.pymysql.connect",
        return_value=connection,
    ):
        connect_source(check, config)

    assert cursor.execute.call_args_list == [
        call("SET SESSION time_zone = '+00:00'"),
        timeout_call,
        call("START TRANSACTION READ ONLY"),
    ]


def test_mysql_source_attempts_tls_without_explicit_ssl_config():
    """tap-mysql and FastSync always offer TLS. Data-diff must match them, or a
    server that requires encryption rejects it as 'Access denied'."""
    connection, _ = _connection_with_cursor()
    check = {"source_type": "tap-mysql", "statement_timeout_seconds": 60}
    config = {
        "host": "source",
        "port": 3306,
        "user": "reader",
        "password": "secret",
        "dbname": "payments",
        "engine": "mariadb",
    }

    with patch(
        "pipelinewise.data_diff.engine.pymysql.connect",
        return_value=connection,
    ) as connect:
        connect_source(check, config)

    assert connect.call_args.kwargs["ssl"] == {"": True}


def test_mysql_source_prefers_an_explicit_ssl_context():
    connection, _ = _connection_with_cursor()
    check = {"source_type": "tap-mysql", "statement_timeout_seconds": 60}
    config = {
        "host": "source",
        "port": 3306,
        "user": "reader",
        "password": "secret",
        "dbname": "payments",
        "engine": "mariadb",
        "ssl": "true",
    }

    with patch(
        "pipelinewise.data_diff.engine.pymysql.connect",
        return_value=connection,
    ) as connect:
        connect_source(check, config)

    assert connect.call_args.kwargs["ssl"] != {"": True}


def test_snowflake_target_session_is_utc_and_time_limited():
    connection = Mock()
    check = {
        "check_id": "check-id",
        "target_type": "target-snowflake",
        "statement_timeout_seconds": 90,
    }
    config = {
        "user": "reader",
        "password": "secret",
        "account": "account",
        "dbname": "analytics",
        "warehouse": "QUALITY",
    }

    with patch("snowflake.connector.connect", return_value=connection) as connect:
        connect_target(check, config)

    parameters = connect.call_args.kwargs["session_parameters"]
    assert parameters["TIMEZONE"] == "UTC"
    assert parameters["STATEMENT_TIMEOUT_IN_SECONDS"] == 90


def test_postgres_target_is_direct_read_only_utc_and_time_limited():
    connection, cursor = _connection_with_cursor()
    check = {
        "target_type": "target-postgres",
        "statement_timeout_seconds": 75,
    }
    config = {
        "host": "target",
        "replica_host": "must-not-be-used",
        "port": 5432,
        "user": "target_reader",
        "password": "secret",
        "dbname": "analytics",
        "ssl": "true",
    }

    with patch(
        "pipelinewise.data_diff.engine.psycopg2.connect",
        return_value=connection,
    ) as connect:
        adapter = connect_target(check, config)

    assert isinstance(adapter, PostgresAdapter)
    assert connect.call_args.kwargs["host"] == "target"
    assert connect.call_args.kwargs["user"] == "target_reader"
    assert connect.call_args.kwargs["sslmode"] == "require"
    assert (
        connect.call_args.kwargs["application_name"]
        == "pipelinewise-data-diff-target"
    )
    connection.set_session.assert_called_once_with(readonly=True, autocommit=False)
    assert cursor.execute.call_args_list == [
        call("SELECT set_config('TimeZone', 'UTC', true)"),
        call("SELECT set_config('statement_timeout', %s, true)", ("75s",)),
    ]


def test_unsupported_target_type_is_rejected():
    with pytest.raises(DataDiffExecutionError, match="Unsupported target type"):
        connect_target(
            {
                "target_type": "target-unknown",
                "statement_timeout_seconds": 30,
            },
            {},
        )


def test_mysql_checksum_sql_survives_client_side_interpolation():
    """PyMySQL interpolates SQL client-side, so DATE_FORMAT specifiers must be
    doubled. A bare %Y raises before the query ever reaches MySQL."""
    adapter = MySQLAdapter(Mock(), statement_timeout_seconds=60)
    sql = build_metric_query(
        adapter,
        "payments",
        "transfers",
        "transfer_id",
        "updated_at",
        ("row_count", "row_checksum"),
        checksum_columns_for_query=[
            {"name": "updated_at", "checksum_kind": "timestamp"},
            {"name": "status", "checksum_kind": "string"},
        ],
    )

    # The two window boundaries every metric query binds.
    interpolated = sql % ("2026-07-01 00:00:00", "2026-07-02 00:00:00")

    # MySQL must receive single specifiers, whatever the SQL carried.
    assert "DATE_FORMAT" in interpolated
    assert "%Y-%m-%d %H:%i:%s.%f" in interpolated
    assert "%%" not in interpolated


def test_mysql_time_checksum_sql_survives_client_side_interpolation():
    adapter = MySQLAdapter(Mock(), statement_timeout_seconds=60)

    normalized = adapter.normalize_checksum_value(
        {"name": "started_at", "checksum_kind": "time"}
    )

    assert normalized % () == "DATE_FORMAT(`started_at`, '%H:%i:%s.%f')"


def _cursor_returning(row):
    cursor = MagicMock()
    cursor.__enter__ = Mock(return_value=cursor)
    cursor.__exit__ = Mock(return_value=False)
    cursor.fetchone.return_value = row
    connection = Mock()
    connection.cursor.return_value = cursor
    return connection, cursor


def test_snowflake_execute_metrics_rejects_missing_row():
    connection, _cursor = _cursor_returning(None)
    adapter = SnowflakeAdapter(connection, statement_timeout_seconds=60)

    with pytest.raises(DataDiffExecutionError, match="Aggregate query returned no row"):
        adapter.execute_metrics(
            "SELECT COUNT(*) AS row_count", (), ("row_count",)
        )


def test_postgres_table_rows_reads_catalog_statistics():
    connection, cursor = _cursor_returning({"rows": 100_000_000})
    adapter = PostgresAdapter(connection, statement_timeout_seconds=60)

    assert adapter.table_rows("public", "transfers") == 100_000_000
    # Partitions must be summed with the parent, which reports none of its own.
    assert "pg_inherits" in cursor.execute.call_args.args[0]


def test_mysql_table_rows_prefers_partition_totals():
    connection, _cursor = _cursor_returning({"row_count": 250_000})
    adapter = MySQLAdapter(connection, statement_timeout_seconds=60)

    assert adapter.table_rows("payments", "transfers") == 250_000


def test_mysql_table_rows_falls_back_when_partitions_report_nothing():
    cursor = MagicMock()
    cursor.__enter__ = Mock(return_value=cursor)
    cursor.__exit__ = Mock(return_value=False)
    cursor.fetchone.side_effect = [{"row_count": 0}, {"row_count": 4_200}]
    connection = Mock()
    connection.cursor.return_value = cursor
    adapter = MySQLAdapter(connection, statement_timeout_seconds=60)

    assert adapter.table_rows("payments", "transfers") == 4_200


def test_boolean_null_is_distinguishable_from_false_in_every_dialect():
    # A NULL that collapses to the FALSE branch hashes identically to FALSE, so a
    # source NULL against a target FALSE would be an invisible mismatch.
    column = {"name": "is_active", "checksum_kind": "boolean"}

    for adapter in (
        PostgresAdapter(None, 30), MySQLAdapter(None, 30), SnowflakeAdapter(None, 30)
    ):
        expression = adapter.normalize_checksum_value(column)
        assert "IS NULL" in expression, adapter.__class__.__name__


def test_mysql_checksum_integer_casts_before_subtracting():
    # CONV returns a string; subtracting from it coerces to DOUBLE and SUM() then
    # loses integer precision over a large window.
    expression = MySQLAdapter(None, 30).checksum_integer("payload")

    assert "CAST(CONV(" in expression
    assert "AS DECIMAL(38, 0))" in expression


def test_checksum_numeric_scale_keeps_the_wider_precision():
    # Rounding to the target's narrower scale would make a lossy replica hash
    # identically to its source.
    pairs = [(
        "amount",
        _column("amount", "numeric", numeric_scale=4),
        _column("AMOUNT", "NUMBER", numeric_scale=2),
    )]

    source_columns, target_columns = checksum_columns(pairs)

    assert source_columns[0]["checksum_scale"] == 4
    assert target_columns[0]["checksum_scale"] == 4


def test_preflight_ignores_an_unusable_timestamp_index():
    # A partial, invalid, not-ready or non-btree index cannot serve a window scan,
    # so it must not count as proof the table is readable by timestamp.
    unusable = _usable_index(
        ["updated_at"], name="partial_idx", is_usable=False, is_partial=True
    )
    preflight = preflight_source(
        FakeAdapter(indexes=[unusable], table_rows=100_001),
        "public", "payments", "updated_at", "SELECT 1", (),
    )

    assert preflight["status"] == "BLOCKED"
    # The unusable index is still reported, so the evidence explains the block.
    assert "partial_idx" in " ".join(preflight["findings"])
    assert preflight["index_metadata"] == [unusable]


def test_preflight_persists_its_decision_inputs():
    # A PASS must record the size and limit it was judged against, or the verdict
    # cannot be re-checked after either changes.
    preflight = preflight_source(
        FakeAdapter(indexes=[_usable_index(["updated_at"])], table_rows=7),
        "public", "payments", "updated_at", "SELECT 1", (),
    )

    assert preflight["status"] == "PASS"
    assert preflight["table_rows"] == 7
    assert preflight["row_limit"] == 100_000
    assert preflight["has_leading_index"] is True


def test_preflight_verdict_does_not_depend_on_the_window():
    # Deliberate, pinned so nobody "fixes" it into a window-width gate: a wide
    # window over a well indexed table is expensive but optimal.
    adapter = FakeAdapter(indexes=[_usable_index(["updated_at"])], table_rows=10_000_000)
    narrow = preflight_source(
        adapter, "public", "payments", "updated_at", "SELECT 1",
        ("2026-07-01", "2026-07-01 00:01:00"),
    )
    wide = preflight_source(
        adapter, "public", "payments", "updated_at", "SELECT 1",
        ("2020-01-01", "2030-01-01"),
    )

    assert narrow["status"] == wide["status"] == "PASS"
    assert narrow["query_fingerprint"] == wide["query_fingerprint"]


@pytest.mark.parametrize("configured,expected", [(0, 1), (0.5, 1), (30, 30)])
def test_statement_timeout_never_reaches_zero(configured, expected):
    # Both engines read 0 as "no limit", so a sub-second budget must round up to a
    # real one rather than silently disabling the timeout.
    connection, cursor = _connection_with_cursor()
    check = {"source_type": "tap-mysql", "statement_timeout_seconds": configured}
    config = {
        "host": "source", "port": 3306, "user": "reader",
        "password": "secret", "dbname": "payments", "engine": "mariadb",
    }

    with patch(
        "pipelinewise.data_diff.engine.pymysql.connect", return_value=connection
    ):
        connect_source(check, config)

    assert call("SET SESSION max_statement_time = %s", (expected,)) in (
        cursor.execute.call_args_list
    )


def test_every_preflight_branch_is_published_before_execution():
    """Including the metadata-only branch, which has no aggregates to order against.

    A run that publishes nothing leaves dd_run_attempts.preflight_id NULL on success, and
    the runner's placeholder logic then reports a computed preflight as never built.
    """
    published = []

    source = RunAdapter({
        "id": _column("id", "bigint", numeric_scale=0),
        "updated_at": _column("updated_at", "timestamp without time zone"),
    })
    target = RunAdapter({
        "id": _column("id", "bigint", numeric_scale=0),
        "updated_at": _column("updated_at", "timestamp without time zone"),
    })
    check = {
        "checks": ["schema_compatibility"],
        "source_type": "tap-postgres",
        "target_type": "target-postgres",
        "source_schema": "public",
        "source_table": "payments",
        "target_schema": "replicated",
        "target_table": "payments",
        "source_key_column": "id",
        "target_key_column": "id",
        "source_timestamp_column": "updated_at",
        "target_timestamp_column": "updated_at",
        "source_compare_columns": [],
        "target_compare_columns": [],
        "statement_timeout_seconds": 30,
    }

    with patch(
        "pipelinewise.data_diff.engine.connect_source", return_value=source
    ), patch(
        "pipelinewise.data_diff.engine.connect_target", return_value=target
    ):
        preflight, _results, status = run_check(
            check, {}, {},
            datetime(2026, 7, 31, 11, tzinfo=timezone.utc),
            datetime(2026, 7, 31, 12, tzinfo=timezone.utc),
            on_preflight=published.append,
        )

    assert status == "PASS"
    # No aggregate ran, but the preflight is still handed out exactly once.
    assert published == [preflight]
    assert source.executed == []


def _mysql_index_cursor(visibility_columns, index_rows):
    """Fake a STATISTICS cursor: first the catalog probe, then the index rows."""
    cursor = MagicMock()
    cursor.__enter__ = Mock(return_value=cursor)
    cursor.__exit__ = Mock(return_value=False)
    cursor.fetchall.side_effect = [
        [{"column_name": name} for name in visibility_columns],
        index_rows,
    ]
    connection = Mock()
    connection.cursor.return_value = cursor
    return connection, cursor


@pytest.mark.parametrize(
    "visibility_column,hidden_value,visible_value",
    [
        # MySQL 8 and MariaDB 10.6 spell this with opposite senses.
        ("IS_VISIBLE", "NO", "YES"),
        ("IGNORED", "YES", "NO"),
    ],
)
def test_mysql_hidden_index_is_not_usable(
    visibility_column, hidden_value, visible_value
):
    """An INVISIBLE/IGNORED index yields no possible keys, so it cannot certify a scan."""
    rows = [
        {
            "index_name": "ix_hidden", "column_name": "updated_at", "sequence": 1,
            "non_unique": 1, "index_type": "BTREE", "sub_part": None,
            "visibility": hidden_value,
        },
        {
            "index_name": "ix_live", "column_name": "created_at", "sequence": 1,
            "non_unique": 1, "index_type": "BTREE", "sub_part": None,
            "visibility": visible_value,
        },
    ]
    connection, cursor = _mysql_index_cursor([visibility_column], rows)
    adapter = MySQLAdapter(connection, statement_timeout_seconds=60)

    indexes = {index["index_name"]: index for index in adapter.indexes("db", "payments")}

    assert indexes["ix_hidden"]["is_usable"] is False
    assert indexes["ix_live"]["is_usable"] is True
    assert visibility_column in cursor.execute.call_args_list[-1].args[0]


def test_mysql_without_a_visibility_column_keeps_btree_indexes_usable():
    """MySQL 5.7 has neither column, so the probe must not disqualify every index."""
    # The query selects NULL AS visibility when neither column exists.
    rows = [{
        "index_name": "ix_ts", "column_name": "updated_at", "sequence": 1,
        "non_unique": 1, "index_type": "BTREE", "sub_part": None,
        "visibility": None,
    }]
    connection, cursor = _mysql_index_cursor([], rows)
    adapter = MySQLAdapter(connection, statement_timeout_seconds=60)

    assert adapter.indexes("db", "payments")[0]["is_usable"] is True
    assert "IS_VISIBLE" not in cursor.execute.call_args_list[-1].args[0]
    assert "IGNORED" not in cursor.execute.call_args_list[-1].args[0]


def test_mysql_visibility_column_is_probed_once_per_connection():
    cursor = MagicMock()
    cursor.__enter__ = Mock(return_value=cursor)
    cursor.__exit__ = Mock(return_value=False)
    cursor.fetchall.side_effect = [[{"column_name": "IGNORED"}], [], []]
    connection = Mock()
    connection.cursor.return_value = cursor
    adapter = MySQLAdapter(connection, statement_timeout_seconds=60)

    adapter.indexes("db", "first")
    adapter.indexes("db", "second")

    # One probe plus one query per table: the catalog is not re-read per table.
    assert cursor.execute.call_count == 3


def test_hidden_leading_index_blocks_a_large_source_table():
    """The end-to-end consequence: a hidden index must not certify a full scan."""
    adapter = Mock()
    adapter.indexes.return_value = [{
        "index_name": "ix_ts", "columns": ["updated_at"], "is_unique": False,
        "access_method": "btree", "is_usable": False,
    }]
    adapter.table_rows.return_value = MAX_SAFE_FULL_SCAN_ROWS + 1

    preflight = preflight_source(
        adapter, "db", "payments", "updated_at", "SELECT 1", ()
    )

    assert preflight["status"] == "BLOCKED"
    assert preflight["has_leading_index"] is False
    # Naming it separates "no index" from "an index you disabled".
    assert any("ix_ts" in finding for finding in preflight["findings"])
