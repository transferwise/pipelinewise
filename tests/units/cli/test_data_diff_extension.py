import json

from pathlib import Path

import pytest

from pipelinewise.cli import utils
from pipelinewise.cli.config import Config


def _config(tmp_path):
    config = Config(str(tmp_path))
    config.global_config = {
        "backend_db": {
            "host": "backend",
            "port": 5432,
            "user": "pipelinewise",
            "password": "secret",
            "dbname": "pipelinewise",
            "ddl_user": "pipelinewise_ddl",
            "ddl_password": "ddl_secret",
        }
    }
    config.targets = {
        "postgres": {
            "id": "postgres",
            "name": "PostgreSQL",
            "type": "target-postgres",
            "db_conn": {"dbname": "analytics"},
            "taps": [
                {
                    "id": "payments",
                    "name": "Payments",
                    "type": "tap-postgres",
                    "target": "postgres",
                    "db_conn": {"dbname": "payments"},
                    "data_diff_defaults": {
                        "frequency": "0 * * * *",
                        "window_start": "-1d",
                    },
                    "schemas": [
                        {
                            "source_schema": "public",
                            "target_schema": "payments",
                            "tables": [
                                {
                                    "table_name": "transfers",
                                    "replication_method": "LOG_BASED",
                                    "data_diff": {
                                        "checks": ["row_count"],
                                        "key_column": "id",
                                        "timestamp_column": "updated_at",
                                    },
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    }
    return config


def _contains_data_diff(value):
    if isinstance(value, dict):
        return "data_diff" in value or any(_contains_data_diff(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_data_diff(item) for item in value)
    return False


def test_main_schema_accepts_data_diff_extension(tmp_path):
    config = _config(tmp_path)
    tap = config.targets["postgres"]["taps"][0]

    utils.validate(tap, utils.load_schema("tap"))
    utils.validate(config.global_config, utils.load_schema("config"))

    tap["schemas"][0]["tables"][0]["data_diff"]["unknown"] = True
    with pytest.raises(Exception):
        utils.validate(tap, utils.load_schema("tap"))


def test_main_json_excludes_data_diff_extension(tmp_path):
    config = _config(tmp_path)
    config.save()

    generated = [json.loads(path.read_text(encoding="utf-8")) for path in Path(tmp_path).rglob("*.json")]

    assert generated
    assert all(not _contains_data_diff(document) for document in generated)


def test_main_config_builds_data_diff_definitions(tmp_path):
    definitions = _config(tmp_path).get_data_diff_definitions()

    assert len(definitions) == 1
    assert definitions[0].full_check_name == "postgres/payments/public/transfers"
