"""S3-free Snowflake SQL execution shared by FastSync and recovery tools."""

import math
import time

import snowflake.connector

from pipelinewise.utils import pem2der


class SnowflakeSqlClient:
    """Execute Snowflake SQL with consistent authentication and transactions."""

    sql_logger = None
    ignore_cleanup_errors = False

    def __init__(self, connection_config):
        self.connection_config = connection_config

    def create_query_tag(self, query_tag_props=None):
        """Return a serialized query tag; concrete clients own its shape."""
        raise NotImplementedError

    def _connect(self, **kwargs):
        return snowflake.connector.connect(**kwargs)

    def _private_key(self):
        return pem2der(self.connection_config['private_key'])

    @staticmethod
    def _monotonic():
        return time.monotonic()

    def open_connection(
        self,
        query_tag_props=None,
        autocommit=True,
        *,
        login_timeout=None,
        network_timeout=None,
        socket_timeout=None,
    ):
        """Open a JWT-authenticated connection with optional recovery bounds."""
        timeout_options = {
            name: value
            for name, value in (
                ('login_timeout', login_timeout),
                ('network_timeout', network_timeout),
                ('socket_timeout', socket_timeout),
            )
            if value is not None
        }
        return self._connect(
            user=self.connection_config['user'],
            private_key=self._private_key(),
            account=self.connection_config['account'],
            database=self.connection_config['dbname'],
            warehouse=self.connection_config['warehouse'],
            role=self.connection_config.get('role'),
            authenticator='SNOWFLAKE_JWT',
            autocommit=autocommit,
            session_parameters={
                'QUOTED_IDENTIFIERS_IGNORE_CASE': 'FALSE',
                'QUERY_TAG': self.create_query_tag(query_tag_props),
            },
            **timeout_options,
        )

    def query(self, query, params=None, query_tag_props=None):
        """Execute one statement and return dictionary rows when present."""
        if self.sql_logger is not None:
            self.sql_logger.debug('Running query: %s', query)
        with self.open_connection(query_tag_props) as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall() if cursor.description else []

    def query_with_timeout(self, query, params, timeout_seconds):
        """Execute one lookup within connector and statement deadlines."""
        deadline = self._monotonic() + timeout_seconds
        connection_timeout = max(1, math.ceil(timeout_seconds))
        with self.open_connection(
            login_timeout=connection_timeout,
            network_timeout=connection_timeout,
            socket_timeout=connection_timeout,
        ) as connection:
            remaining_seconds = deadline - self._monotonic()
            if remaining_seconds <= 0:
                raise TimeoutError(
                    'Snowflake query-history lookup deadline elapsed before statement execution'
                )
            statement_timeout = max(1, math.ceil(remaining_seconds))
            with connection.cursor(snowflake.connector.DictCursor) as cursor:
                cursor.execute(query, params, timeout=statement_timeout)
                return cursor.fetchall() if cursor.description else []

    def execute_transaction(self, queries, query_tag_props=None):
        """Execute all statements in one explicit transaction."""
        connection = self.open_connection(query_tag_props, autocommit=False)
        try:
            with connection.cursor() as cursor:
                for query in queries:
                    if self.sql_logger is not None:
                        self.sql_logger.debug('Running transaction query: %s', query)
                    cursor.execute(query)
            connection.commit()
        except Exception:
            self._rollback(connection)
            raise
        finally:
            self._close(connection)

    def _rollback(self, connection):
        if not self.ignore_cleanup_errors:
            connection.rollback()
            return
        try:
            connection.rollback()
        except Exception:  # pragma: no cover - driver-specific cleanup failure
            self.sql_logger.warning(
                'Failed to roll back Snowflake publication transaction',
                exc_info=True,
            )

    def _close(self, connection):
        if not self.ignore_cleanup_errors:
            connection.close()
            return
        try:
            connection.close()
        except Exception:  # pragma: no cover - driver-specific cleanup failure
            self.sql_logger.warning(
                'Failed to close Snowflake publication connection',
                exc_info=True,
            )
