"""Reusable PostgreSQL connection and transaction handling."""

import logging
import os
import sys
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from sqlalchemy.engine import URL

LOGGER = logging.getLogger(__name__)


class BackendDatabaseConfigError(ValueError):
    """Raised when the backend database is not fully configured."""


class BackendDatabase:
    """A reusable UTC PostgreSQL connection with atomic dictionary cursors."""

    def __init__(self, connection_config: dict, ddl_config: dict = None):
        self._connection_config = connection_config.copy()
        self._ddl_config = ddl_config.copy() if ddl_config else None
        self._connection = None

    @classmethod
    def from_config(
        cls,
        config: dict,
        *,
        application_name: str,
    ):
        """Create a connection from a ``backend_db`` configuration block.

        Alembic migrations run as ``ddl_user``, which is mandatory. Set it to the
        same credentials as ``user`` to run migrations as the application user.
        """
        required = ("host", "user", "password", "dbname", "ddl_user", "ddl_password")
        missing = [key for key in required if not config.get(key)]
        if missing:
            raise BackendDatabaseConfigError(
                "backend_db is missing required fields: " + ", ".join(missing)
            )

        params = {
            "host": config["host"],
            "port": config.get("port", 5432),
            "user": config["user"],
            "password": config["password"],
            "dbname": config["dbname"],
            "connect_timeout": config.get("connect_timeout", 10),
            "options": "-c timezone=UTC",
            "application_name": application_name,
        }
        if config.get("sslmode"):
            params["sslmode"] = config["sslmode"]

        ddl_config = {
            "host": config["host"],
            "port": config.get("port", 5432),
            "user": config["ddl_user"],
            "password": config["ddl_password"],
            "dbname": config["dbname"],
            "options": "-c timezone=UTC",
        }
        if config.get("sslmode"):
            ddl_config["sslmode"] = config["sslmode"]

        return cls(params, ddl_config=ddl_config)

    def connect(self):
        """Open the backend connection when necessary."""
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(**self._connection_config)
            self._connection.autocommit = False
        return self._connection

    def close(self):
        """Close an open backend connection."""
        if self._connection is not None and not self._connection.closed:
            self._connection.close()

    @contextmanager
    def cursor(self):
        """Yield a dictionary cursor in an atomic transaction."""
        connection = self.connect()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *_args):
        self.close()

    def migrate(self):
        """Run Alembic migrations to bring the schema up to date.

        Runs as ``ddl_user``, which ``from_config`` always supplies.
        """
        # Alembic registers plugins at import time, logging INFO to stderr. Detach
        # the root stderr handler around the import to keep migrations quiet.
        root = logging.getLogger()
        stderr_handlers = [
            handler for handler in root.handlers
            if isinstance(handler, logging.StreamHandler) and handler.stream is sys.stderr
        ]
        for handler in stderr_handlers:
            root.removeHandler(handler)
        try:
            # Imported lazily so the stderr handler is detached before Alembic's
            # import-time plugin registration emits INFO logs.
            from alembic import command  # pylint: disable=import-outside-toplevel
            from alembic.config import Config  # pylint: disable=import-outside-toplevel

            migrations_dir = os.path.join(os.path.dirname(__file__), "migrations")
            alembic_cfg = Config(os.path.join(migrations_dir, "alembic.ini"))
            alembic_cfg.set_main_option("script_location", migrations_dir)

            # from_config always supplies a DDL config; the fallback covers instances
            # constructed directly.
            conn = self._ddl_config if self._ddl_config else self._connection_config
            query = {
                key: conn[key]
                for key in ("options", "sslmode")
                if conn.get(key)
            }
            url = URL.create(
                "postgresql+psycopg2",
                username=conn["user"],
                password=conn["password"],
                host=conn["host"],
                port=conn.get("port", 5432),
                database=conn["dbname"],
                query=query,
            )
            url_string = url.render_as_string(hide_password=False)
            alembic_cfg.set_main_option(
                "sqlalchemy.url", url_string.replace("%", "%%")
            )

            # ddl_user owns every object it creates, so the application user needs
            # explicit grants. Migrations read this to know who to grant to.
            alembic_cfg.set_main_option(
                "pipelinewise_application_user", self._connection_config["user"]
            )

            command.upgrade(alembic_cfg, "head")
        finally:
            for handler in stderr_handlers:
                root.addHandler(handler)
        LOGGER.info("Backend DB schema is up to date (user: %s)", conn['user'])
