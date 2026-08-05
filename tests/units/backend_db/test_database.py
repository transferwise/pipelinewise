import ast

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from sqlalchemy.engine import make_url

from pipelinewise.backend_db import BackendDatabase, BackendDatabaseConfigError
from pipelinewise.cli import utils


def _config():
    return {
        "host": "backend",
        "port": 5432,
        "user": "backend_user",
        "password": "secret",
        "dbname": "pipelinewise_backend",
        "ddl_user": "backend_ddl_user",
        "ddl_password": "ddl_secret",
    }


def _parse_alembic_url(value):
    """Undo ConfigParser escaping before parsing the captured URL."""
    return make_url(value.replace("%%", "%"))


def test_connection_enforces_utc_and_sets_consumer_application_name():
    connection = Mock(closed=False)
    with patch(
        "pipelinewise.backend_db.database.psycopg2.connect",
        return_value=connection,
    ) as connect:
        database = BackendDatabase.from_config(
            _config(),
            application_name="pipelinewise-data-diff",
        )
        database.connect()

    assert connect.call_args.kwargs["options"] == "-c timezone=UTC"
    assert connect.call_args.kwargs["application_name"] == "pipelinewise-data-diff"
    assert connect.call_args.kwargs["password"] == "secret"


def test_missing_configuration_is_rejected_without_opening_a_connection():
    config = _config()
    config.pop("password")

    with pytest.raises(BackendDatabaseConfigError, match="password"):
        BackendDatabase.from_config(config, application_name="consumer")


@pytest.mark.parametrize("missing", ["ddl_user", "ddl_password"])
def test_ddl_credentials_are_mandatory(missing):
    config = _config()
    config.pop(missing)

    with pytest.raises(BackendDatabaseConfigError, match=missing):
        BackendDatabase.from_config(config, application_name="consumer")


def test_migrations_use_the_ddl_identity_and_queries_use_the_application_one():
    config = _config()
    config["sslmode"] = "verify-full"

    database = BackendDatabase.from_config(config, application_name="consumer")

    assert database._ddl_config["user"] == "backend_ddl_user"
    assert database._ddl_config["password"] == "ddl_secret"
    assert database._ddl_config["sslmode"] == "verify-full"
    assert database._connection_config["user"] == "backend_user"


def test_migrate_passes_sslmode_and_the_application_user_to_alembic():
    config = _config()
    config["sslmode"] = "verify-full"
    database = BackendDatabase.from_config(config, application_name="consumer")

    alembic_config = Mock()
    options = {}
    alembic_config.set_main_option.side_effect = options.__setitem__

    with patch("alembic.config.Config", return_value=alembic_config), patch("alembic.command.upgrade") as upgrade:
        database.migrate()

    upgrade.assert_called_once()
    # sslmode must reach the migration connection: it is the highest-privilege one.
    url = _parse_alembic_url(options["sqlalchemy.url"])
    assert url.query["sslmode"] == "verify-full"
    assert url.query["options"] == "-c timezone=UTC"
    # The migration grants to this role, so it has to know the application user.
    assert options["pipelinewise_application_user"] == "backend_user"


def test_migrate_preserves_special_url_characters_and_multiple_pg_options():
    config = _config()
    config.update(
        {
            "host": "2001:db8::1",
            "port": 5433,
            "dbname": "pipelinewise/backend",
            "ddl_user": "backend ddl+user@example.com",
            "ddl_password": "ddl:p+a%ss word/@",
            "sslmode": "verify-full",
        }
    )
    database = BackendDatabase.from_config(config, application_name="consumer")
    database._ddl_config["options"] = "-c timezone=UTC -c statement_timeout=5000"

    alembic_config = Mock()
    options = {}
    alembic_config.set_main_option.side_effect = options.__setitem__

    with patch("alembic.config.Config", return_value=alembic_config), patch("alembic.command.upgrade"):
        database.migrate()

    url = _parse_alembic_url(options["sqlalchemy.url"])
    assert url.host == config["host"]
    assert url.port == config["port"]
    assert url.database == config["dbname"]
    assert url.username == config["ddl_user"]
    assert url.password == config["ddl_password"]
    assert url.query == {
        "options": "-c timezone=UTC -c statement_timeout=5000",
        "sslmode": "verify-full",
    }


def test_reusing_the_application_credentials_for_ddl_is_allowed():
    config = _config()
    config["ddl_user"] = config["user"]
    config["ddl_password"] = config["password"]

    database = BackendDatabase.from_config(config, application_name="consumer")

    assert database._ddl_config["user"] == config["user"]


def test_config_schema_accepts_every_key_from_config_is_read():
    # The schema sets additionalProperties: false, so a key the runtime reads but
    # the schema omits makes the documented config fail validation.
    config = _config()
    config["sslmode"] = "verify-full"
    config["connect_timeout"] = 10

    utils.validate(instance={"backend_db": config}, schema=utils.load_schema("config"))

    config.pop("ddl_user")
    with pytest.raises(Exception, match="ddl_user"):
        utils.validate(instance={"backend_db": config}, schema=utils.load_schema("config"))


def test_cursor_commits_successful_transactions():
    cursor = Mock()
    connection = Mock(closed=False)
    connection.cursor.return_value = cursor
    database = BackendDatabase({})
    database._connection = connection

    with database.cursor() as yielded:
        assert yielded is cursor

    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()
    cursor.close.assert_called_once_with()


def test_cursor_rolls_back_failed_transactions():
    cursor = Mock()
    connection = Mock(closed=False)
    connection.cursor.return_value = cursor
    database = BackendDatabase({})
    database._connection = connection

    with pytest.raises(RuntimeError):
        with database.cursor():
            raise RuntimeError("failure")

    connection.rollback.assert_called_once_with()
    connection.commit.assert_not_called()
    cursor.close.assert_called_once_with()


def test_backend_database_has_no_data_diff_dependency():
    package = Path(__file__).parents[3] / "pipelinewise" / "backend_db"
    forbidden = []
    for path in package.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports = [node.module]
            else:
                imports = []
            forbidden.extend(
                imported
                for imported in imports
                if imported == "pipelinewise.data_diff" or imported.startswith("pipelinewise.data_diff.")
            )

    assert forbidden == []
