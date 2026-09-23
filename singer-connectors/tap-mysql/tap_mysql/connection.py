#!/usr/bin/env python3


import backoff
import pymysql
import ssl
import singer

from pymysql.constants import CLIENT

LOGGER = singer.get_logger('tap_mysql')

CONNECT_TIMEOUT_SECONDS = 30

MARIADB_ENGINE = 'mariadb'
MYSQL_ENGINE = 'mysql'
_REPORTED_SESSION_ENGINE_SELECTIONS = set()

MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX = 'PIPELINEWISE_CONTROL:'
MYSQL_BINLOG_RETRY_PENDING_ENV = 'PIPELINEWISE_MYSQL_BINLOG_RETRY_PENDING'
MYSQL_BINLOG_DISCONNECT_MARKER = {
    'type': 'PIPELINEWISE_CONTROL',
    'component': 'tap-mysql',
    'event': 'binlog_stream_disconnected',
    'version': 1,
}


class BinlogStreamDisconnectedError(RuntimeError):
    """A file-position stream must restart from target-acknowledged state."""


DEFAULT_NET_WRITE_TIMEOUT_SQL = 'SET @@session.net_write_timeout=3600'
DEFAULT_SESSION_SQLS = ['SET @@session.time_zone="+0:00"',
                        'SET @@session.wait_timeout=28800',
                        'SET @@session.net_read_timeout=3600',
                        DEFAULT_NET_WRITE_TIMEOUT_SQL,
                        'SET @@session.innodb_lock_wait_timeout=3600']
MARIADB_MAX_STATEMENT_TIME_SQL = 'SET @@session.max_statement_time=0'
MYSQL_MAX_EXECUTION_TIME_SQL = 'SET @@session.max_execution_time=0'


def resolve_source_engine(connection, configured_engine=None):
    """Resolve and retain the configured or detected source engine."""
    if configured_engine is None:
        engine = connection.resolved_engine
        if engine is None:
            engine = MARIADB_ENGINE if 'mariadb' in connection.get_server_info().lower() else MYSQL_ENGINE
            connection.resolved_engine = engine
        engine_source = 'detected'
    else:
        engine = str(configured_engine).lower()
        connection.resolved_engine = engine
        engine_source = 'configured'

    selection = (engine, engine_source)
    if selection not in _REPORTED_SESSION_ENGINE_SELECTIONS:
        LOGGER.info('Using %s source engine (%s)', engine, engine_source)
        _REPORTED_SESSION_ENGINE_SELECTIONS.add(selection)
    else:
        LOGGER.debug('Using %s source engine (%s)', engine, engine_source)
    return engine


def default_session_sqls(connection, configured_engine=None):
    """Return defaults compatible with the resolved source server."""
    engine = resolve_source_engine(connection, configured_engine)
    session_sqls = list(DEFAULT_SESSION_SQLS)
    if engine == MARIADB_ENGINE:
        session_sqls.append(MARIADB_MAX_STATEMENT_TIME_SQL)
    elif engine == MYSQL_ENGINE:
        session_sqls.append(MYSQL_MAX_EXECUTION_TIME_SQL)
    return session_sqls


@backoff.on_exception(backoff.expo,
                      (pymysql.err.OperationalError),
                      max_tries=5,
                      factor=2)
def connect_with_backoff(connection):
    connection.connect()
    run_session_sqls(connection)

    return connection


def run_session_sqls(connection):
    configured_session_sqls = connection.session_sqls
    session_sqls = [
        (sql, sql in (MARIADB_MAX_STATEMENT_TIME_SQL, MYSQL_MAX_EXECUTION_TIME_SQL))
        for sql in default_session_sqls(connection, connection.configured_engine)
    ]
    session_sqls.extend((sql, False) for sql in (
        configured_session_sqls if isinstance(configured_session_sqls, list) else []
    ))

    warnings = []
    for sql, optional_timeout in session_sqls:
        try:
            run_sql(connection, sql)
        except pymysql.err.OperationalError as exc:
            if not optional_timeout or not exc.args or exc.args[0] != 1193:
                raise
            warnings.append(
                f'Built-in timeout not applied: {sql}; server does not support this variable. '
                'Check the configured source engine.'
            )
        except pymysql.err.InternalError as exc:
            warnings.append(f'Could not set session variable `{sql}`: {exc}')

    if warnings:
        LOGGER.warning('Encountered non-fatal errors when configuring session that could impact performance:')
    for warning in warnings:
        LOGGER.warning(warning)


def run_sql(connection, sql):
    with connection.cursor() as cur:
        cur.execute(sql)


def parse_internal_hostname(hostname):
    # special handling for google cloud
    if ":" in hostname:
        parts = hostname.split(":")
        if len(parts) == 3:
            return parts[0] + ":" + parts[2]
        return parts[0] + ":" + parts[1]

    return hostname


class MySQLConnection(pymysql.connections.Connection):
    def __init__(self, config):
        # Google Cloud's SSL involves a self-signed certificate. This certificate's
        # hostname matches the form {instance}:{box}. The hostname displayed in the
        # Google Cloud UI is of the form {instance}:{region}:{box} which
        # necessitates the "parse_internal_hostname" function to get the correct
        # hostname to match.
        # The "internal_hostname" config variable allows for matching the SSL
        # against a host that doesn't match the host we are connecting to. In the
        # case of Google Cloud, we will be connecting to an IP, not the hostname
        # the SSL certificate expects.
        # The "ssl.match_hostname" function is patched to check against the
        # internal hostname rather than the host of the connection. In the event
        # that the connection fails, the patch is reverted by reassigning the
        # patched out method to it's original spot.

        args = {
            "user": config["user"],
            "password": config["password"],
            "host": config["host"],
            "port": int(config["port"]),
            "cursorclass": config.get("cursorclass") or pymysql.cursors.SSCursor,
            "connect_timeout": CONNECT_TIMEOUT_SECONDS,
            "charset": "utf8mb4",
        }

        ssl_arg = {"": True}

        if config.get("database"):
            args["database"] = config["database"]

        # Attempt self-signed SSL if config vars are present
        use_self_signed_ssl = config.get("ssl_ca") and config.get("ssl_cert") and config.get("ssl_key")

        if use_self_signed_ssl:
            LOGGER.info("Using custom certificate authority")

            # The SSL module requires files not data, so we have to write out the
            # data to files. After testing with `tempfile.NamedTemporaryFile`
            # objects, I kept getting "File name too long" errors as the temp file
            # names were > 99 chars long in some cases. Since the box is ephemeral,
            # we don't need to worry about cleaning them up.
            with open("ca.pem", "wb") as ca_file:
                ca_file.write(config["ssl_ca"].encode('utf-8'))

            with open("cert.pem", "wb") as cert_file:
                cert_file.write(config["ssl_cert"].encode('utf-8'))

            with open("key.pem", "wb") as key_file:
                key_file.write(config["ssl_key"].encode('utf-8'))

            ctx = ssl.create_default_context(cafile="./ca.pem")
            ctx.load_cert_chain(certfile="./cert.pem", keyfile="./key.pem")

            if config.get("internal_hostname"):
                parsed_hostname = parse_internal_hostname(config["internal_hostname"])
                # This tells Python to verify the cert against THIS name,
                # even if we are connecting to an IP address.
                ctx.check_hostname = True
                server_hostname = parsed_hostname
            else:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_REQUIRED  # Or ssl.CERT_NONE if preferred
                server_hostname = None

            ssl_arg = ctx

            args["server_hostname"] = server_hostname

        super().__init__(defer_connect=True, ssl=ssl_arg, **args)

        # Attempt SSL
        if config.get("ssl") == 'true' and not use_self_signed_ssl:
            LOGGER.info("Attempting SSL connection")
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ssl_arg = ctx  # Assign the context to ssl_arg
            self.client_flag |= CLIENT.SSL

        self.configured_engine = config.get('engine') if 'engine' in config else None
        self.resolved_engine = (
            str(self.configured_engine).lower()
            if self.configured_engine is not None
            else None
        )
        self.session_sqls = config.get('session_sqls', [])

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()


def make_connection_wrapper(config):
    class ConnectionWrapper(MySQLConnection):
        def __init__(self, *args, **kwargs):
            self._fail_on_disconnect = False
            super().__init__({**config, 'cursorclass': kwargs.get('cursorclass')})

            connect_with_backoff(self)
            # The decoder also uses this wrapper for retryable information_schema lookups.
            self._fail_on_disconnect = kwargs.get('db') != 'information_schema' and not config.get('use_gtid', False)

        def _read_packet(self, *args, **kwargs):
            try:
                return super()._read_packet(*args, **kwargs)
            except pymysql.OperationalError as exc:
                # The decoder's automatic file-position reconnect can skip unread rows or their table map.
                if self._fail_on_disconnect and exc.args[0] in {2006, 2013}:
                    raise BinlogStreamDisconnectedError(
                        'Binlog connection lost; restart replication from the durable checkpoint.') from exc
                raise

    return ConnectionWrapper


def fetch_server_id(mysql_conn: MySQLConnection) -> int:
    """
    Finds server ID
    Args:
        mysql_conn: Mysql connection instance

    Returns: server ID
    """
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SELECT @@server_id")
            server_id = cur.fetchone()[0]

            return server_id


def fetch_server_uuid(mysql_conn: MySQLConnection) -> str:
    """
    Finds server UUID
    Args:
        mysql_conn: Mysql connection instance

    Returns: server UUID
    """
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SELECT @@server_uuid")
            server_uuid = cur.fetchone()[0]

            return server_uuid
