import json

import pytest

from pipelinewise.cli import utils
from pipelinewise.data_diff.runtime import RuntimeConnectorConfigLoader, load_definition_input


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _check():
    return {"target_id": "snowflake", "tap_id": "payments"}


def test_loader_renders_secret_lookups_in_generated_configs(tmp_path, monkeypatch):
    monkeypatch.setenv("PPW_SECRET_SOURCE", 'pa"ss')
    monkeypatch.setenv("PPW_SECRET_TARGET", "key")
    _write(tmp_path / "snowflake" / "payments" / "config.json",
           '{"host": "source", "password": {{ env_var[\'PPW_SECRET_SOURCE\'] | tojson }}}')
    _write(tmp_path / "snowflake" / "config.json",
           '{"account": "acme", "private_key": {{ env_var[\'PPW_SECRET_TARGET\'] | tojson }}}')

    source, target = RuntimeConnectorConfigLoader(str(tmp_path), utils.render_env_var_lookups)(_check())

    assert source == {"host": "source", "password": 'pa"ss'}
    assert target == {"account": "acme", "private_key": "key"}


def test_loader_without_a_renderer_reads_plain_json(tmp_path):
    _write(tmp_path / "snowflake" / "payments" / "config.json", '{"password": "plain"}')
    _write(tmp_path / "snowflake" / "config.json", '{"account": "acme"}')

    assert RuntimeConnectorConfigLoader(str(tmp_path))(_check()) == ({"password": "plain"}, {"account": "acme"})


def test_loader_fails_rather_than_connecting_without_a_secret(tmp_path, monkeypatch):
    monkeypatch.delenv("PPW_SECRET_MISSING", raising=False)
    _write(tmp_path / "snowflake" / "payments" / "config.json",
           '{"password": {{ env_var[\'PPW_SECRET_MISSING\'] | tojson }}}')
    _write(tmp_path / "snowflake" / "config.json", "{}")

    with pytest.raises(TypeError, match="Undefined"):
        RuntimeConnectorConfigLoader(str(tmp_path), utils.render_env_var_lookups)(_check())


def _definition_input(tap_id="payments", target_id="snowflake"):
    return {
        "target": {"id": target_id, "type": "target-snowflake", "db_conn": {"dbname": "analytics"}},
        "tap": {"id": tap_id, "type": "tap-postgres", "db_conn": {"dbname": "payments"}, "schemas": []},
    }


def test_definition_input_is_read_from_the_tap_directory(tmp_path):
    _write(tmp_path / "snowflake" / "payments" / "data_diff.json", json.dumps(_definition_input()))

    assert load_definition_input(str(tmp_path), "snowflake", "payments") == _definition_input()


def test_missing_definition_input_is_an_error_not_an_empty_tap(tmp_path):
    with pytest.raises(ValueError, match="does not exist"):
        load_definition_input(str(tmp_path), "snowflake", "payments")


@pytest.mark.parametrize(
    "document, message",
    [
        (_definition_input(tap_id="other"), "describes tap 'other'"),
        (_definition_input(target_id="other"), "in target 'other'"),
        ({"tap": _definition_input()["tap"]}, "must contain 'target' and 'tap'"),
    ],
)
def test_definition_input_must_describe_the_requested_tap(tmp_path, document, message):
    _write(tmp_path / "snowflake" / "payments" / "data_diff.json", json.dumps(document))

    with pytest.raises(ValueError, match=message):
        load_definition_input(str(tmp_path), "snowflake", "payments")
