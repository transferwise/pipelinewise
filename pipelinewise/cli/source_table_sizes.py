"""Read source table allocation sizes without scanning replicated rows."""
from pathlib import Path
import ssl
from tempfile import TemporaryDirectory

import pymysql

from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.commons.tap_yugabyte import FastSyncTapYugabyte


MYSQL_TABLE_SIZE_SQL = (
    'SELECT DATA_LENGTH + INDEX_LENGTH FROM information_schema.TABLES '
    'WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s'
)
POSTGRES_TABLE_SIZE_SQL = """
WITH RECURSIVE relations AS (
    SELECT c.oid, c.relkind
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('r', 'p', 'm', 'v', 'f')
    UNION ALL
    SELECT c.oid, c.relkind
    FROM relations parent
    JOIN pg_catalog.pg_inherits i ON i.inhparent = parent.oid
    JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
)
SELECT CASE WHEN bool_or(relkind IN ('v', 'f')) THEN NULL
            ELSE COALESCE(SUM(pg_catalog.pg_total_relation_size(oid))
                          FILTER (WHERE relkind IN ('r', 'm')), 0) END,
       COUNT(*)
FROM (SELECT DISTINCT oid, relkind FROM relations) physical_relations
"""

# YugabyteDB reports NULL pg_table_size for colocated tables; keep it NULL rather than
# letting COALESCE or SUM report a misleading partial or zero size.
YUGABYTE_TABLE_SIZE_SQL = """
WITH RECURSIVE relations AS (
    SELECT c.oid, c.relkind
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('r', 'p', 'm', 'v', 'f')
    UNION ALL
    SELECT c.oid, c.relkind
    FROM relations parent
    JOIN pg_catalog.pg_inherits i ON i.inhparent = parent.oid
    JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
), physical_relations AS (
    SELECT DISTINCT oid, relkind,
           CASE WHEN relkind IN ('r', 'm') THEN pg_catalog.pg_table_size(oid) END AS data_bytes,
           CASE WHEN relkind IN ('r', 'm') THEN pg_catalog.pg_indexes_size(oid) END AS index_bytes
    FROM relations
)
SELECT CASE WHEN bool_or(relkind IN ('v', 'f'))
                 OR bool_or(relkind IN ('r', 'm') AND (data_bytes IS NULL OR index_bytes IS NULL)) THEN NULL
            ELSE COALESCE(SUM(data_bytes + index_bytes) FILTER (WHERE relkind IN ('r', 'm')), 0) END,
       COUNT(*)
FROM physical_relations
"""
SOURCE_TABLE_SIZE_SQL = {
    'tap-mysql': MYSQL_TABLE_SIZE_SQL,
    'tap-postgres': POSTGRES_TABLE_SIZE_SQL,
    'tap-yugabyte': YUGABYTE_TABLE_SIZE_SQL,
}


def _mysql_ssl(config):
    """Load optional PEM credentials before connecting, keeping temporary keys private."""
    if not config.get('ssl_ca'):
        return {'': True}
    context = ssl.create_default_context(cadata=config['ssl_ca'])
    context.check_hostname = False
    if config.get('ssl_cert') and config.get('ssl_key'):
        with TemporaryDirectory() as directory:
            cert_path = Path(directory) / 'cert.pem'
            key_path = Path(directory) / 'key.pem'
            cert_path.write_text(config['ssl_cert'], encoding='utf-8')
            key_path.write_text(config['ssl_key'], encoding='utf-8')
            context.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    return context


def open_source_connection(tap_type, config):
    """Connect to the configured source host for read-only metadata queries."""
    if tap_type == 'tap-mysql':
        return pymysql.connect(
            host=config['host'], port=int(config.get('port', 3306)),
            user=config['user'], password=config['password'],
            charset=config.get('charset', 'utf8mb4'), ssl=_mysql_ssl(config),
            connect_timeout=10, read_timeout=60, write_timeout=60, autocommit=True,
        )
    if tap_type == 'tap-postgres':
        return FastSyncTapPostgres.get_connection(config, prioritize_primary=True)
    if tap_type == 'tap-yugabyte':
        return FastSyncTapYugabyte.admin_connection(config)
    raise ValueError('This command only supports mysql, postgres and yugabyte taps')


def read_source_table_size(cursor, tap_type, schema, table):
    """Return table allocation bytes, or null for relations without local storage."""
    cursor.execute(SOURCE_TABLE_SIZE_SQL[tap_type], (schema, table))
    row = cursor.fetchone()
    if row is None or (tap_type != 'tap-mysql' and row[1] == 0):
        raise ValueError(f'Cannot read source table size for {schema}.{table}; check source access and catalog')
    return int(row[0]) if row[0] is not None else None
