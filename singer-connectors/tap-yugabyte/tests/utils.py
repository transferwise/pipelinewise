import io
import os
import time

import psycopg2
import psycopg2.extras

from psycopg2.extensions import quote_ident
from singer import get_logger

LOGGER = get_logger()

# How long to keep retrying a slot drop while YugabyteDB still reports it active
# (ysql_cdc_active_replication_slot_window_ms, default 5 minutes, bounds the lag
# between a consumer disconnecting and the slot's `active` flag clearing).
_DROP_SLOT_RETRY_ATTEMPTS = 30
_DROP_SLOT_RETRY_INTERVAL_SECONDS = 2


class SingerOutput(io.StringIO):
    """Capture Singer writers that use either stdout text or stdout.buffer."""

    @property
    def buffer(self):
        return self

    def write(self, value):
        return super().write(value.decode('utf-8') if isinstance(value, bytes) else value)


def get_test_connection_config():
    """Build a tap connection config from the TAP_YUGABYTE_* environment variables."""
    try:
        return {
            'host': os.environ['TAP_YUGABYTE_HOST'],
            'port': os.environ['TAP_YUGABYTE_PORT'],
            'user': os.environ['TAP_YUGABYTE_USER'],
            'password': os.environ['TAP_YUGABYTE_PASSWORD'],
            'dbname': os.environ['TAP_YUGABYTE_DB'],
            'tap_id': 'tap_test',
            'limit': None,
            'max_run_seconds': 43200,
            'break_at_end_lsn': True,
            'logical_poll_total_seconds': 10,
        }
    except KeyError as exc:
        raise Exception(
            'set TAP_YUGABYTE_HOST, TAP_YUGABYTE_PORT, TAP_YUGABYTE_USER, '
            'TAP_YUGABYTE_PASSWORD, TAP_YUGABYTE_DB'
        ) from exc


def get_test_connection():
    """Open an autocommit connection to the test database."""
    conn_config = get_test_connection_config()

    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
        conn_config['host'],
        conn_config['dbname'],
        conn_config['user'],
        conn_config['password'],
        conn_config['port'])

    LOGGER.info('connecting to %s', conn_config['host'])

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True

    return conn


def build_col_sql(col, cur):
    if col.get('quoted'):
        return '{} {}'.format(quote_ident(col['name'], cur), col['type'])

    return '{} {}'.format(col['name'], col['type'])


def build_table(table, cur):
    create_sql = 'CREATE TABLE {}\n'.format(quote_ident(table['name'], cur))
    col_sql = map(lambda c: build_col_sql(c, cur), table['columns'])
    pks = [c['name'] for c in table['columns'] if c.get('primary_key')]
    if len(pks) != 0:
        pk_sql = ',\n CONSTRAINT {}  PRIMARY KEY({})'.format(quote_ident(table['name'] + '_pk', cur), ' ,'.join(pks))
    else:
        pk_sql = ''

    return '{} ( {} {})'.format(create_sql, ',\n'.join(col_sql), pk_sql)


def ensure_test_table(table_spec):
    """Recreate the table described by table_spec, dropping any previous version."""
    with get_test_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute('DROP TABLE if exists {} cascade'.format(quote_ident(table_spec['name'], cur)))

            sql = build_table(table_spec, cur)
            LOGGER.info('create table sql: %s', sql)
            cur.execute(sql)
            cur.execute('ANALYZE {}'.format(quote_ident(table_spec['name'], cur)))


def drop_table(table_name):
    with get_test_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute('DROP TABLE IF EXISTS {} cascade'.format(quote_ident(table_name, cur)))


def insert_record(cursor, table_name, data, retry_serialization_failures=False):
    """Insert one row via a parameterized statement, columns in sorted-key order."""
    keys = sorted(data.keys())
    values = [data[k] for k in keys]

    columns_sql = ', '.join(quote_ident(k, cursor) for k in keys)
    placeholders_sql = ', '.join(['%s'] * len(keys))

    insert_sql = 'INSERT INTO {} ( {} ) VALUES ( {} )'.format(
        quote_ident(table_name, cursor), columns_sql, placeholders_sql)
    LOGGER.info('INSERT: %s', insert_sql)
    if retry_serialization_failures:
        execute_retrying_serialization_failures(cursor, insert_sql, values)
    else:
        cursor.execute(insert_sql, values)


def execute_retrying_serialization_failures(cur, sql, params=None, attempts=5, delay=0.5):
    """Execute a statement immediately after DDL, retrying on YugabyteDB's transient
    catalog-version-bump SerializationFailure (the query layer marks it unretryable
    itself once any rows have been returned to the client, so callers must retry) or the
    InFailedSqlTransaction it can leave behind on this same session/cursor."""
    for attempt in range(attempts):
        try:
            cur.execute(sql, params)
            return
        except (psycopg2.errors.SerializationFailure, psycopg2.errors.InFailedSqlTransaction):
            if attempt == attempts - 1:
                raise
            cur.connection.rollback()
            time.sleep(delay)


def set_replication_method_for_stream(stream, method):
    """Select every stream-level metadata entry and set its replication-method."""
    for entry in stream['metadata']:
        if not entry['breadcrumb']:
            entry['metadata']['selected'] = True
            entry['metadata']['replication-method'] = method
    return stream


def lsn_to_int(lsn):
    """Parse a pg_lsn-formatted 'HI/LO' string into the 64-bit integer it encodes.

    YugabyteDB's HYBRID_TIME slots still report restart_lsn/confirmed_flush_lsn
    in this hex HI/LO wire format, even though the value itself is a hybrid-time
    bigint rather than a WAL byte offset.
    """
    if lsn is None:
        return None
    hi, lo = lsn.split('/')
    return (int(hi, 16) << 32) + int(lo, 16)


def create_replication_slot(tap_id='tap_test'):
    """Create a wal2json/HYBRID_TIME replication slot named the same way FastSync would."""
    conn_config = get_test_connection_config()
    slot_name = f"pipelinewise_{conn_config['dbname']}_{tap_id}"
    sql = (
        f"select pg_create_logical_replication_slot("
        f"'{slot_name}', 'wal2json', false, false, 'HYBRID_TIME');"
    )

    with get_test_connection() as conn:
        with conn.cursor() as cur:
            LOGGER.info('Creating replication slot: %s', sql)
            cur.execute(sql)


_RETRYABLE_SLOT_DROP_ERRORS = (
    psycopg2.errors.InFailedSqlTransaction,
    psycopg2.errors.SerializationFailure,
)


def drop_replication_slot(tap_id='tap_test'):
    """Drop a replication slot, retrying while YugabyteDB still reports it active or is
    still settling the catalog-version bump a recent CDC session/DDL left behind (which
    can surface as InFailedSqlTransaction/SerializationFailure on this same statement)."""
    conn_config = get_test_connection_config()
    slot_name = f"pipelinewise_{conn_config['dbname']}_{tap_id}"
    sql = f"SELECT pg_drop_replication_slot('{slot_name}');"

    with get_test_connection() as conn:
        for attempt in range(_DROP_SLOT_RETRY_ATTEMPTS):
            try:
                with conn.cursor() as cur:
                    LOGGER.info('Dropping replication slot: %s', sql)
                    cur.execute(sql)
                return
            except psycopg2.Error as ex:
                retryable = 'is active' in str(ex) or isinstance(ex, _RETRYABLE_SLOT_DROP_ERRORS)
                if not retryable or attempt == _DROP_SLOT_RETRY_ATTEMPTS - 1:
                    raise
                conn.rollback()
                time.sleep(_DROP_SLOT_RETRY_INTERVAL_SECONDS)


class MockedConnect:
    """Mocks psycopg2.connect so full_table sync can be unit tested without a live DB."""

    class cursor:  # noqa: N801 pylint: disable=invalid-name
        return_value = 1234
        counter_limit = 3
        fetchone_return_value = [5]

        def __init__(self, *args, **kwargs):
            self.counter = 0

        def __enter__(self):
            return self

        def __exit__(self, *args, **kwargs):
            pass

        def __iter__(self):
            return self

        def __next__(self):
            self.counter += 1
            if self.counter < self.counter_limit:
                return [self.return_value]
            raise StopIteration

        def fetchone(self):
            return self.fetchone_return_value

        def execute(self, *args, **kwargs):
            pass

    def __enter__(self):
        pass

    def __exit__(self, *args, **kwargs):
        pass

    def __init__(self, *args, **kwargs):
        pass
