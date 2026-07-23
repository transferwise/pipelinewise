import json

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pipelinewise.data_diff.config import (
    DataDiffConfigError,
    extract_check_definitions,
    parse_duration,
)
# pylint: disable=missing-function-docstring,invalid-name


def _config(tmp_path, *, backend=True, transformed=False):
    del tmp_path
    global_config = {
        "backend_db": {
            "host": "backend",
            "port": 5432,
            "user": "pipelinewise",
            "password": "secret",
            "dbname": "pipelinewise",
        }
    } if backend else {}
    transformations = [{"column": "updated_at", "type": "HASH"}] if transformed else []
    targets = {
        "snowflake": {
            "id": "snowflake",
            "name": "Snowflake",
            "type": "target-snowflake",
            "db_conn": {"dbname": "analytics"},
            "taps": [
                {
                    "id": "payments",
                    "name": "Payments",
                    "type": "tap-postgres",
                    "target": "snowflake",
                    "db_conn": {
                        "host": "source",
                        "port": 5432,
                        "user": "reader",
                        "password": "source-secret",
                        "dbname": "payments",
                    },
                    "schemas": [
                        {
                            "source_schema": "public",
                            "target_schema": "replicated_payments",
                            "tables": [
                                {
                                    "table_name": "transfers-v2",
                                    "replication_method": "LOG_BASED",
                                    "transformations": transformations,
                                    "data_diff": {
                                        "checks": [
                                            "schema_compatibility",
                                            "row_count",
                                            "distinct_key_count",
                                            "null_key_count",
                                            "duplicate_key_count",
                                            "min_key",
                                            "max_key",
                                            "row_checksum",
                                        ],
                                        "key_column": "transfer_id",
                                        "timestamp_column": "updated_at",
                                        "compare_columns": ["status", "amount"],
                                        "frequency": "0 * * * *",
                                        "window_start": "-1d6h",
                                        "window_end": "-6h",
                                        "statement_timeout": "2min",
                                    },
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    }
    return SimpleNamespace(global_config=global_config, targets=targets)


def _definitions(config):
    return extract_check_definitions(
        config.global_config,
        config.targets,
    )


def _use_postgres_target(config, source_type="tap-postgres"):
    target = config.targets.pop("snowflake")
    target["id"] = "postgres"
    target["name"] = "PostgreSQL"
    target["type"] = "target-postgres"
    target["db_conn"] = {"dbname": "analytics"}
    tap = target["taps"][0]
    tap["target"] = "postgres"
    tap["type"] = source_type
    config.targets["postgres"] = target
    return config


@pytest.mark.parametrize(
    ("value", "seconds"),
    [("1s", 1), ("2min", 120), ("1d6h", 108_000), ("2w", 1_209_600)],
)
def test_parse_duration(value, seconds):
    assert parse_duration(value) == seconds


@pytest.mark.parametrize("value", ["", "1", "1hour", "1h2h", "0s", "-1h"])
def test_parse_duration_rejects_ambiguous_or_non_positive_values(value):
    with pytest.raises(DataDiffConfigError):
        parse_duration(value)


def test_extracts_deterministic_credential_free_definition(tmp_path):
    definition = _definitions(_config(tmp_path))[0]

    assert definition.frequency == "0 * * * *"
    assert definition.window_start_seconds == 108_000
    assert definition.window_end_seconds == 21_600
    assert definition.target_schema == "REPLICATED_PAYMENTS"
    assert definition.target_table == "V2"
    assert definition.target_key_column == "TRANSFER_ID"
    assert definition.source_compare_columns == ("status", "amount")
    assert definition.target_compare_columns == ("STATUS", "AMOUNT")
    assert "password" not in json.dumps(definition.canonical_config)
    assert definition.config_hash == _definitions(_config(tmp_path))[0].config_hash


@pytest.mark.parametrize("source_type", ["tap-postgres", "tap-mysql"])
def test_postgres_target_routes_use_target_postgres_identifiers(tmp_path, source_type):
    definition = _definitions(
        _use_postgres_target(_config(tmp_path), source_type)
    )[0]

    assert definition.target_type == "target-postgres"
    assert definition.target_schema == "replicated_payments"
    assert definition.target_table == "v2"
    assert definition.target_key_column == "transfer_id"
    assert definition.target_timestamp_column == "updated_at"
    assert definition.target_compare_columns == ("status", "amount")


def test_ignores_definitions_when_no_backend_is_configured(tmp_path):
    # Data-diff is opt-in: without backend_db, definitions are dropped and
    # import_config still succeeds. Patched not caplog: logging.conf sets propagate=0.
    with patch('pipelinewise.data_diff.config.LOGGER') as logger:
        assert _definitions(_config(tmp_path, backend=False)) == []

    assert 'backend_db' in logger.warning.call_args.args[0]


# An empty frequency is caught earlier by the missing-timing check, which reports a
# more useful message, so it is not covered here.
@pytest.mark.parametrize("frequency", ["1h", "15min", "every hour", "0 * * *"])
def test_rejects_a_frequency_that_is_not_a_crontab_expression(tmp_path, frequency):
    # Rejected at import so the scheduler never loads an unschedulable definition.
    config = _config(tmp_path)
    table = config.targets["snowflake"]["taps"][0]["schemas"][0]["tables"][0]
    table["data_diff"]["frequency"] = frequency

    with pytest.raises(DataDiffConfigError, match="crontab expression"):
        _definitions(config)


def test_rejects_transformed_comparison_columns(tmp_path):
    with pytest.raises(DataDiffConfigError, match="cannot be transformed"):
        _definitions(_config(tmp_path, transformed=True))


def test_row_checksum_requires_explicit_compare_columns(tmp_path):
    config = _config(tmp_path)
    data_diff = config.targets["snowflake"]["taps"][0]["schemas"][0]["tables"][0][
        "data_diff"
    ]
    data_diff.pop("compare_columns")

    with pytest.raises(DataDiffConfigError, match="row_checksum requires"):
        _definitions(config)


def test_rejects_transformed_row_checksum_column(tmp_path):
    config = _config(tmp_path)
    table = config.targets["snowflake"]["taps"][0]["schemas"][0]["tables"][0]
    table["transformations"] = [{"column": "amount", "type": "HASH"}]

    with pytest.raises(DataDiffConfigError, match="amount"):
        _definitions(config)


def test_tap_timing_defaults_are_inherited_and_table_values_override(tmp_path):
    config = _config(tmp_path)
    tap = config.targets["snowflake"]["taps"][0]
    data_diff = tap["schemas"][0]["tables"][0]["data_diff"]
    tap["data_diff_defaults"] = {
        "frequency": "0 */2 * * *",
        "window_start": "-8h",
        "window_end": "-4h",
        "statement_timeout": "3min",
    }
    data_diff.pop("frequency")
    data_diff.pop("window_start")
    data_diff.pop("window_end")
    data_diff["statement_timeout"] = "1min"

    definition = _definitions(config)[0]

    assert definition.frequency == "0 */2 * * *"
    assert definition.window_start_seconds == 28800
    assert definition.window_end_seconds == 14400
    assert definition.statement_timeout_seconds == 60


def test_requires_timing_on_table_or_tap_defaults(tmp_path):
    config = _config(tmp_path)
    data_diff = config.targets["snowflake"]["taps"][0]["schemas"][0]["tables"][0][
        "data_diff"
    ]
    data_diff.pop("frequency")
    data_diff.pop("window_start")

    with pytest.raises(DataDiffConfigError, match="data_diff_defaults"):
        _definitions(config)


def test_rejects_window_end_further_from_fire_than_window_start(tmp_path):
    config = _config(tmp_path)
    data_diff = config.targets["snowflake"]["taps"][0]["schemas"][0]["tables"][0][
        "data_diff"
    ]
    data_diff["window_start"] = "-1h"
    data_diff["window_end"] = "-6h"

    with pytest.raises(DataDiffConfigError, match="window_end must be closer to fire time"):
        _definitions(config)
