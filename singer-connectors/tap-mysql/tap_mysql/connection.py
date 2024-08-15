#!/usr/bin/env python3
# pylint: disable=missing-docstring,arguments-differ,missing-function-docstring

import backoff
import pymysql
import ssl
import singer

from pymysql.constants import CLIENT

LOGGER = singer.get_logger('tap_mysql')

CONNECT_TIMEOUT_SECONDS = 30

# We need to hold onto this for self-signed SSL
MATCH_HOSTNAME = ssl.match_hostname
MARIADB_ENGINE = 'mariadb'
MYSQL_ENGINE = 'mysql'

DEFAULT_SESSION_SQLS = ['SET @@session.time_zone="+0:00"',
                        'SET @@session.wait_timeout=28800',
                        'SET @@session.net_read_timeout=3600',
                        'SET @@session.innodb_lock_wait_timeout=3600']


@backoff.on_exception(backoff.expo,
                      (pymysql.err.OperationalError),
                      max_tries=5,
                      factor=2)
def connect_with_backoff(connection):
    connection.connect()
    run_session_sqls(connection)

    return connection


def run_session_sqls(connection):
    session_sqls = connection.session_sqls

    warnings = []
    if session_sqls and isinstance(session_sqls, list):
        for sql in session_sqls:
            try:
                run_sql(connection, sql)
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
            "charset": "utf8",
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

            ssl_arg = {
                "ca": "./ca.pem",
                "cert": "./cert.pem",
                "key": "./key.pem",
            }

            # override match hostname for google cloud
            if config.get("internal_hostname"):
                parsed_hostname = parse_internal_hostname(config["internal_hostname"])
                ssl.match_hostname = lambda cert, hostname: MATCH_HOSTNAME(cert, parsed_hostname)# pylint: disable=W1505

        super().__init__(defer_connect=True, ssl=ssl_arg, **args)

        # Attempt SSL
        if config.get("ssl") == 'true' and not use_self_signed_ssl:
            LOGGER.info("Attempting SSL connection")
            self.ssl = True
            self.ctx = ssl.create_default_context()
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE
            self.client_flag |= CLIENT.SSL

        self.session_sqls = config.get("session_sqls", DEFAULT_SESSION_SQLS)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()


def make_connection_wrapper(config):
    class ConnectionWrapper(MySQLConnection):
        def __init__(self, *args, **kwargs):  # pylint: disable=unused-argument
            config["cursorclass"] = kwargs.get('cursorclass')
            super().__init__(config)

            connect_with_backoff(self)

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
