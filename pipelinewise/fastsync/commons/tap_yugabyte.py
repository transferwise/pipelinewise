import datetime
import decimal
import glob
import logging
import os
import re
import time
import psycopg2
import psycopg2.extras

from argparse import Namespace
from typing import Callable, Dict, Optional, Tuple

from . import utils, split_gzip, yb_retry
from .partial_sync_boundary import PartialSyncBoundary
from ...utils import safe_column_name

LOGGER = logging.getLogger(__name__)

# YugabyteDB keeps a slot's `active` flag set for up to
# ysql_cdc_active_replication_slot_window_ms (default 5 minutes) after the last consumer
# disconnects; pg_drop_replication_slot fails with "slot is active" during that window,
# so slot administration retries instead of failing the resync outright. Dropping a slot
# shortly after heavy CDC activity can also transiently surface as InFailedSqlTransaction
# or SerializationFailure while the catalog-version bump from that activity settles.
_DROP_SLOT_RETRY_ATTEMPTS = 10
_DROP_SLOT_RETRY_INTERVAL_SECONDS = 30
_RETRYABLE_DROP_SLOT_ERRORS = (
    psycopg2.errors.InFailedSqlTransaction,
    psycopg2.errors.SerializationFailure,
)

# Slot administration runs in-process from the main pipelinewise virtualenv, whose stock
# psycopg2-binary rejects the YugabyteDB driver's load balancing connection options.
_DRIVER_SPECIFIC_CONNECTION_OPTIONS = ('load_balance', 'topology_keys')

# MISMATCHED_SCHEMA, one of the errors the general read-retry policy in yb_retry
# covers: a `yb_read_time`-pinned read validates its session's cached catalog
# snapshot against the cluster's current catalog version, and a concurrent DDL
# elsewhere can bump that version after the snapshot was pinned but before the
# tablet server's heartbeat-driven propagation reaches this session. The boundary
# itself stays valid, so the read just needs asking again.


class FastSyncTapYugabyte:
    """
    Common functions for fastsync from a YugabyteDB (YSQL) database
    """

    def __init__(self, connection_config, tap_type_to_target_type, target_quote=None):
        self.connection_config = connection_config
        self.tap_type_to_target_type = tap_type_to_target_type
        self.target_quote = target_quote
        self.hstore_as_json = False
        self.conn = None
        self.curr = None
        # HYBRID_TIME boundary of the replication slot created for this table's LOG_BASED
        # sync; set by fetch_current_log_pos() and consumed by copy_table() to pin the bulk
        # export snapshot to the exact point the CDC stream will resume from.
        self._snapshot_ht = None

    @staticmethod
    def generate_replication_slot_name(dbname, tap_id=None, prefix='pipelinewise'):
        """Generate replication slot name with

        :param str dbname: Database name that will be part of the replication slot name
        :param str tap_id: Optional. If provided then it will be appended to the end of the slot name
        :param str prefix: Optional. Defaults to 'pipelinewise'
        :return: well formatted lowercased replication slot name
        :rtype: str
        """
        if tap_id:
            tap_id = f'_{tap_id}'
        else:
            tap_id = ''

        slot_name = f'{prefix}_{dbname}{tap_id}'.lower()

        # Replace invalid characters to ensure replication slot name is in accordance with YSQL spec
        return re.sub('[^a-z0-9_]', '_', slot_name)

    @classmethod
    def admin_connection(cls, connection_config: Dict):
        """
        Open a slot-administration connection without driver-specific options.

        Args:
            connection_config: Dictionary with db credentials
        Returns:
            psycopg2 Connection instance
        """
        return cls.get_connection({
            key: value
            for key, value in connection_config.items()
            if key not in _DRIVER_SPECIFIC_CONNECTION_OPTIONS
        })

    @classmethod
    def _run_slot_statement(cls, connection, slot_name: str, phase: str, statement: Callable) -> None:
        """Run one slot statement, retrying YugabyteDB's transient active-slot errors."""
        for attempt in range(1, _DROP_SLOT_RETRY_ATTEMPTS + 1):
            try:
                with connection.cursor() as cur:
                    statement(cur)
                return
            except psycopg2.Error as exc:
                retryable = 'is active' in str(exc) or isinstance(exc, _RETRYABLE_DROP_SLOT_ERRORS)
                if not retryable or attempt == _DROP_SLOT_RETRY_ATTEMPTS:
                    raise
                LOGGER.info(
                    'Slot "%s" %s failed (%s), retrying in %s seconds (attempt %s/%s)',
                    slot_name,
                    phase,
                    exc.__class__.__name__,
                    _DROP_SLOT_RETRY_INTERVAL_SECONDS,
                    attempt,
                    _DROP_SLOT_RETRY_ATTEMPTS,
                )
                connection.rollback()
                time.sleep(_DROP_SLOT_RETRY_INTERVAL_SECONDS)

    @classmethod
    def drop_slot(cls, connection_config: Dict) -> None:
        """
        Drop the logical replication slot used by this tap, tolerating YugabyteDB's
        lingering "slot is active" window after the last consumer disconnects.

        Args:
            connection_config: Dictionary with db credentials
        """
        LOGGER.info('Attempting to drop slot ...')
        connection = cls.admin_connection(connection_config)
        slot_name = cls.generate_replication_slot_name(
            connection_config['dbname'], connection_config['tap_id']
        )

        def drop(cur):
            cur.execute(
                f'SELECT pg_drop_replication_slot(slot_name) '
                f"FROM pg_replication_slots WHERE slot_name = '{slot_name}';"
            )
            LOGGER.info('Number of dropped slots: %s', cur.rowcount)

        try:
            cls._run_slot_statement(connection, slot_name, 'drop', drop)
        finally:
            connection.close()

    @classmethod
    def reset_slot(cls, connection_config: Dict, *, before_reset: Callable[[], Optional[str]]) -> None:
        """Validate one tap-specific slot, invalidate state, then replace the slot."""
        LOGGER.info('Attempting to reset slot ...')

        connection = cls.admin_connection(connection_config)
        try:
            slot_name, slot_exists = cls._preflight_slot_reset(connection, connection_config)
            # State must be durable before DROP: losing its response cannot restore the old
            # HybridTime boundary.
            backup_path = before_reset()
            phase = 'drop' if slot_exists else 'create'
            try:
                if slot_exists:
                    LOGGER.info('Dropping the slot "%s"', slot_name)
                    cls._run_slot_statement(
                        connection,
                        slot_name,
                        'drop',
                        lambda cur: cur.execute('SELECT pg_drop_replication_slot(%s)', (slot_name,)),
                    )
                phase = 'create'
                LOGGER.info('Creating the slot "%s"', slot_name)
                cls._run_slot_statement(
                    connection,
                    slot_name,
                    'create',
                    lambda cur: cur.execute(
                        'SELECT * FROM pg_create_logical_replication_slot(%s, %s, %s, %s, %s)',
                        (slot_name, 'wal2json', False, False, 'HYBRID_TIME'),
                    ),
                )
            except psycopg2.Error as exc:
                raise RuntimeError(
                    f'YugabyteDB slot reset failed during {phase} for "{slot_name}"; '
                    'the source-side outcome may be uncertain. Tap bookmarks remain invalidated. '
                    f'Pre-reset state backup: {backup_path or "no previous state file"}. '
                    'Keep scheduled replication stopped, resolve the source error, and rerun '
                    'the unfiltered fast_sync (adding --force only to bypass the size limit). '
                    'Do not restore old LOG_BASED bookmarks '
                    'after a completed or uncertain slot drop.'
                ) from exc
        finally:
            connection.close()

    @classmethod
    def _preflight_slot_reset(cls, connection, connection_config: Dict) -> Tuple[str, bool]:
        """Reject shared or incompatible slots and wait out the lingering active window."""
        database = connection_config['dbname']
        legacy_name = cls.generate_replication_slot_name(database)
        slot_name = cls.generate_replication_slot_name(database, connection_config['tap_id'])
        if slot_name == legacy_name or len(slot_name) > 63:
            raise RuntimeError('Slot reset requires a distinct tap-specific slot name of at most 63 characters.')

        for attempt in range(1, _DROP_SLOT_RETRY_ATTEMPTS + 1):
            with connection.cursor() as cur:
                cur.execute(
                    'SELECT slot_name, database, plugin, active FROM pg_replication_slots '
                    'WHERE slot_name IN (%s, %s)',
                    (legacy_name, slot_name),
                )
                slots = {row[0]: row[1:] for row in cur.fetchall()}

            if legacy_name in slots:
                raise RuntimeError(
                    f'Cannot reset legacy YugabyteDB slot "{legacy_name}": it may be shared by other taps. '
                    'Coordinate migration to tap-specific slots with your DBA before retrying. '
                    'No source or state changes were made.'
                )
            if slot_name not in slots:
                return slot_name, False

            slot_database, plugin, active = slots[slot_name]
            if slot_database != database or plugin != 'wal2json':
                raise RuntimeError(
                    f'Cannot reset YugabyteDB slot "{slot_name}": it must use wal2json and belong to the '
                    'configured database. No source or state changes were made.'
                )
            if not active:
                return slot_name, True
            # `active` stays set for the whole post-disconnect window, so it only proves a live
            # consumer once the window has elapsed; waiting here keeps state intact meanwhile.
            if attempt == _DROP_SLOT_RETRY_ATTEMPTS:
                raise RuntimeError(
                    f'Cannot reset YugabyteDB slot "{slot_name}": it is still active after '
                    f'{_DROP_SLOT_RETRY_ATTEMPTS * _DROP_SLOT_RETRY_INTERVAL_SECONDS} seconds, longer than '
                    'ysql_cdc_active_replication_slot_window_ms, so a consumer is likely still streaming it. '
                    'Stop every consumer of this slot and retry. No source or state changes were made.'
                )
            LOGGER.info(
                'Slot "%s" is still active, waiting %s seconds for YugabyteDB\'s active-slot '
                'window to elapse (attempt %s/%s)',
                slot_name,
                _DROP_SLOT_RETRY_INTERVAL_SECONDS,
                attempt,
                _DROP_SLOT_RETRY_ATTEMPTS,
            )
            time.sleep(_DROP_SLOT_RETRY_INTERVAL_SECONDS)

    @classmethod
    def get_connection(cls, connection_config: Dict):
        """
        Class method to create a YSQL connection instance with autocommit enabled

        Args:
            connection_config: Dictionary containing the db connection details
        Returns:
            psycopg2 Connection instance
        """
        # Keyword arguments rather than a conninfo string: the psycopg2-yugabyte driver parses
        # load_balance/topology_keys out of a DSN with a regex that rejects quoted values,
        # and unquoted interpolation would break on passwords containing spaces or quotes.
        conn_params = {
            'host': connection_config['host'],
            'port': connection_config['port'],
            'user': connection_config['user'],
            'password': connection_config['password'],
            'dbname': connection_config['dbname'],
        }

        if connection_config.get('ssl') == 'true':
            conn_params['sslmode'] = 'require'

        if connection_config.get('load_balance'):
            conn_params['load_balance'] = connection_config['load_balance']

        if connection_config.get('topology_keys'):
            conn_params['topology_keys'] = connection_config['topology_keys']

        conn = psycopg2.connect(**conn_params)

        # Set connection to autocommit
        conn.autocommit = True

        LOGGER.info('Connection to YSQL server established')

        return conn

    def open_connection(self):
        """
        Open connection
        """
        self.conn = self.get_connection(self.connection_config)
        self.curr = self.conn.cursor()

    def close_connection(self, silent=False):
        """
        Close source connection
        """
        connection = self.conn
        self.conn = None
        self.curr = None

        if connection is None:
            return

        try:
            connection.close()
        except Exception as exc:
            if not silent:
                LOGGER.exception(exc)
                LOGGER.info('Connection seems to be already closed.')

    def query(self, query, params=None):
        """
        Run query
        """
        LOGGER.info('Running query: %s', query)
        with self.conn as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []

    def create_replication_slot(self):
        """
        Create the HYBRID_TIME logical replication slot for this tap, tolerating one that
        already exists.
        """
        slot_name = self.generate_replication_slot_name(
            self.connection_config['dbname'], self.connection_config['tap_id']
        )
        try:
            self.query(
                f"SELECT * FROM pg_create_logical_replication_slot("
                f"'{slot_name}', 'wal2json', false, false, 'HYBRID_TIME')"
            )
        except Exception as exc:
            # ERROR: replication slot already exists SQL state: 42710
            if hasattr(exc, 'pgcode') and exc.pgcode == '42710':
                pass
            else:
                raise exc

    def fetch_current_log_pos(self):
        """
        Create (if needed) the replication slot and return its HYBRID_TIME boundary.

        Unlike Postgres's byte-offset WAL LSN, the `lsn` returned by
        `pg_create_logical_replication_slot` is a placeholder pg_lsn string, not a usable
        boundary; the real HYBRID_TIME boundary is `yb_restart_commit_ht` from
        `pg_replication_slots`.
        """
        self.create_replication_slot()

        slot_name = self.generate_replication_slot_name(
            self.connection_config['dbname'], self.connection_config['tap_id']
        )
        result = self.query(
            f"SELECT yb_restart_commit_ht FROM pg_replication_slots WHERE slot_name = '{slot_name}'"
        )
        if not result:
            raise Exception(f'Replication slot {slot_name} not found after creation')

        current_lsn = result[0].get('yb_restart_commit_ht')
        # Pin the upcoming copy_table() bulk export to this exact boundary so the initial
        # snapshot and the CDC stream's start point are provably consistent.
        self._snapshot_ht = current_lsn

        return {'lsn': current_lsn, 'version': 1}

    def fetch_current_incremental_key_pos(self, table, replication_key):
        """
        Get the actual incremental key position in the table
        """
        schema_name, table_name = table.split('.')
        result = self.query(
            f'SELECT MAX({replication_key}) AS key_value FROM {schema_name}."{table_name}"'
        )
        if not result:
            raise Exception(
                f'Cannot get replication key value for table: {table}'
            )

        yb_key_value = result[0].get('key_value')

        if yb_key_value is None:
            LOGGER.warning('No replication value found for table %s, returning empty bookmark', table)
            return {}

        key_value = yb_key_value

        # Convert YSQL date/datetime format to JSON friendly values
        if isinstance(yb_key_value, datetime.datetime):
            key_value = yb_key_value.isoformat()

        elif isinstance(yb_key_value, datetime.date):
            key_value = yb_key_value.isoformat() + 'T00:00:00'

        elif isinstance(yb_key_value, decimal.Decimal):
            key_value = float(yb_key_value)

        return {
            'replication_key': replication_key,
            'replication_key_value': key_value,
            'version': 1,
        }

    def get_primary_keys(self, table):
        """
        Get the primary key of a table
        """
        schema_name, table_name = table.split('.')

        sql = """
            SELECT attribute.attname
            FROM pg_catalog.pg_index AS index_def
            JOIN pg_catalog.pg_class AS table_class
              ON table_class.oid = index_def.indrelid
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = table_class.relnamespace
            CROSS JOIN LATERAL unnest(index_def.indkey)
              WITH ORDINALITY AS key_column(attnum, key_ordinality)
            JOIN pg_catalog.pg_attribute AS attribute
              ON attribute.attrelid = table_class.oid
             AND attribute.attnum = key_column.attnum
            WHERE namespace.nspname = %s
              AND table_class.relname = %s
              AND index_def.indisprimary
            ORDER BY key_column.key_ordinality
        """
        pk_specs = self.query(sql, (schema_name, table_name))
        if len(pk_specs) > 0:
            return [safe_column_name(k[0], self.target_quote) for k in pk_specs]

        return None

    def get_table_columns(self, table_name, max_num=None, date_type='date'):
        """
        Get YSQL table column details from information_schema
        """
        table_dict = utils.tablename_to_dict(table_name)

        if max_num:
            decimals = len(max_num.split('.')[1]) if '.' in max_num else 0

            decimal_format = f"""
              'CASE WHEN "' || column_name || '" IS NULL THEN NULL ELSE GREATEST(LEAST({max_num}, ROUND("' || column_name || '"::numeric , {decimals})), -{max_num}) END'
            """  # noqa: E501
            integer_format = """
              '"' || column_name || '"'
            """
        else:
            decimal_format = """
              '"' || column_name || '"'
            """
            integer_format = decimal_format

        schema_name = table_dict.get('schema_name')
        table_name = table_dict.get('table_name')
        hstore_projection = (
            "WHEN udt_name = 'hstore' THEN 'hstore_to_json(\"' || "
            "column_name || '\") AS \"' || column_name || '\"'"
            if self.hstore_as_json else ''
        )

        sql = f"""
                SELECT
                    column_name
                    ,CASE WHEN udt_name = 'hstore' THEN 'hstore' ELSE data_type END AS data_type
                    ,safe_sql_value
                    ,character_maximum_length
                FROM (SELECT
                column_name,
                data_type,
                udt_name,
                CASE
                    WHEN data_type = 'ARRAY' THEN 'array_to_json("' || column_name || '") AS ' || column_name
                    {hstore_projection}
                    WHEN data_type = 'date' THEN
                       'CASE WHEN "' ||column_name|| E'" < \\'0001-01-01\\' '
                            'OR "' ||column_name|| E'" > \\'9999-12-31\\' THEN \\'9999-12-31\\' '
                            'ELSE "' ||column_name|| '"::{date_type} END AS "' ||column_name|| '"'
                    WHEN udt_name = 'time' THEN 'replace("' || column_name || E'"::varchar,\\\'24:00:00\\\',\\\'00:00:00\\\') AS ' || column_name
                    WHEN udt_name = 'timetz' THEN 'replace(("' || column_name || E'" at time zone \'\'UTC\'\')::time::varchar,\\\'24:00:00\\\',\\\'00:00:00\\\') AS ' || column_name
                    WHEN udt_name in ('timestamp', 'timestamptz') THEN
                       'CASE WHEN "' ||column_name|| E'" < \\'0001-01-01 00:00:00.000\\' '
                            'OR "' ||column_name|| E'" > \\'9999-12-31 23:59:59.999\\' THEN \\'9999-12-31 23:59:59.999\\' '
                            'ELSE "' ||column_name|| '" END AS "' ||column_name|| '"'
                    WHEN data_type IN ('double precision', 'numeric', 'decimal', 'real') THEN {decimal_format} || ' AS ' || column_name
                    WHEN data_type IN ('smallint', 'integer', 'bigint', 'serial', 'bigserial') THEN {integer_format} || ' AS ' || column_name
                    ELSE '"'||column_name||'"'
                END AS safe_sql_value,
                character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                    AND table_name = '{table_name}'
                ORDER BY ordinal_position
                ) AS x
            """  # noqa: E501

        return self.query(sql)

    def map_column_types_to_target(self, table_name):
        """
        Map YSQL column types to equivalent types in target
        """
        yb_columns = self.get_table_columns(table_name)
        mapped_columns = []
        for pc in yb_columns:
            column_type = (
                'VARIANT'
                if pc[1] == 'hstore' and self.hstore_as_json
                else self.tap_type_to_target_type(pc[1])
            )
            # YSQL bit type can have length greater than 1
            # most targets would want to map length 1 to boolean and the rest to number
            if isinstance(column_type, list):
                column_type = column_type[1 if pc[3] > 1 else 0]
            mapping = '{} {}'.format(
                safe_column_name(pc[0], self.target_quote), column_type
            )
            mapped_columns.append(mapping)

        return {
            'columns': mapped_columns,
            'primary_key': self.get_primary_keys(table_name),
            'source_column_names': [column[0] for column in yb_columns],
        }

    def copy_table(
        self,
        table_name,
        path,
        max_num=None,
        date_type='date',
        split_large_files=False,
        split_file_chunk_size_mb=1000,
        split_file_max_chunks=20,
        compress=True,
        boundary=None,
    ):
        """
        Export data from table to a zipped csv
        Args:
            table_name: Fully qualified table name to export
            path: Path where to create the zip file(s) with the exported data
            split_large_files: Split large files to multiple pieces and create multiple zip files
                               with -partXYZ postfix in the filename. (Default: False)
            split_file_chunk_size_mb: File chunk sizes if `split_large_files` enabled. (Default: 1000)
            split_file_max_chunks: Max number of chunks if `split_large_files` enabled. (Default: 20)
        """
        full_table_name = table_name

        def _discard_partial_export(_attempt, _exc):
            # a failed copy_expert leaves a truncated .gz (and any -partNNN chunks)
            # behind; the retry re-opens the base path but would not remove the
            # extra chunks, so they are cleared before the export starts over
            for stale in glob.glob(f'{path}*'):
                try:
                    os.remove(stale)
                except OSError:
                    LOGGER.warning('Could not remove partial export file %s', stale)

        def _export():
            # A failed attempt may have lost the connection, not just the
            # statement: a terminated backend or a tablet leader election leaves
            # self.conn closed, and psycopg2 then raises InterfaceError
            # ('connection already closed') on the first use of self.curr. That
            # error carries no SQLSTATE, so the policy reads it as transient and
            # re-runs an operation that cannot ever succeed -- measured against a
            # real pg_terminate_backend mid-COPY: attempt 1 failed with the real
            # COPY error, attempts 2-8 failed instantly on the dead cursor, and
            # the export burned all 8 attempts and ~46s of backoff before giving
            # up. Reconnecting here rather than in before_retry keeps a failure
            # to reconnect inside the retry loop, where it is itself retried;
            # raising out of before_retry would escape the loop entirely.
            #
            # Only a connection psycopg2 has actually marked closed is replaced,
            # so an error that leaves the session usable -- a catalog-version
            # bump, say -- still retries on the same connection. The yb_read_time
            # pin below is re-issued on every attempt, so a fresh session is
            # correctly pinned.
            if getattr(self.conn, 'closed', 0):
                LOGGER.info('Source connection is closed; reopening it for this '
                            'export attempt')
                self.close_connection(silent=True)
                self.open_connection()

            if self._snapshot_ht is not None:
                # Session-level GUC; must be its own statement, not inside a transaction
                # block (YugabyteDB rejects `SET LOCAL yb_read_time` inside BEGIN/COMMIT).
                LOGGER.info('Pinning export snapshot to yb_read_time %s ht', self._snapshot_ht)
                self.curr.execute(f"SET yb_read_time TO '{self._snapshot_ht} ht'")

            table_columns = self.get_table_columns(full_table_name, max_num, date_type)
            column_safe_sql_values = [c.get('safe_sql_value') for c in table_columns]

            # If self.get_table_columns returns zero row then table not exist
            if len(column_safe_sql_values) == 0:
                raise Exception(f'{full_table_name} table not found.')

            source_boundary = (
                boundary.source_sql(
                    'postgres',
                    [column[0] for column in table_columns],
                )
                if boundary is not None
                else None
            )

            schema_name, bare_table_name = full_table_name.split('.')

            column_safe_sql_values = column_safe_sql_values + [
                "now() AT TIME ZONE 'UTC' AS _SDC_EXTRACTED_AT",
                "now() AT TIME ZONE 'UTC' AS _SDC_BATCHED_AT",
                'null _SDC_DELETED_AT'
            ]

            if source_boundary is not None:
                where_clause = self.curr.mogrify(
                    source_boundary.statement,
                    source_boundary.parameters,
                )
                if isinstance(where_clause, bytes):
                    connection_encoding = self.curr.connection.encoding
                    python_encoding = psycopg2.extensions.encodings.get(
                        connection_encoding, connection_encoding
                    )
                    where_clause = where_clause.decode(python_encoding)
            else:
                where_clause = ''

            sql = f"""COPY (SELECT {','.join(column_safe_sql_values)}
            FROM {schema_name}."{bare_table_name}"{where_clause}) TO STDOUT with CSV DELIMITER ','
            """

            LOGGER.info('Exporting data: %s', sql)

            gzip_splitter = split_gzip.open(
                path,
                mode='wb',
                chunk_size_mb=split_file_chunk_size_mb,
                max_chunks=split_file_max_chunks if split_large_files else 0,
                compress=compress,
            )

            with gzip_splitter as split_gzip_files:
                self.curr.copy_expert(sql, split_gzip_files, size=131072)

        # the export is a read that rewrites its own output from scratch, so any
        # non-permanent failure -- a read restart, a tablet move, a catalog-version
        # bump under a pinned yb_read_time -- is simply run again
        yb_retry.retry_read(
            _export,
            f'bulk export of {full_table_name}',
            before_retry=_discard_partial_export,
        )

    def export_source_table_data(
            self, args: Namespace, tap_id: str,
            boundary: PartialSyncBoundary = None) -> list:
        """Exporting data from the source table"""
        filename = utils.gen_export_filename(tap_id=tap_id, table=args.table, sync_type='partialsync')
        filepath = os.path.join(args.temp_dir, filename)

        self.copy_table(
            args.table,
            filepath,
            split_large_files=args.target.get('split_large_files'),
            split_file_chunk_size_mb=args.target.get('split_file_chunk_size_mb'),
            split_file_max_chunks=args.target.get('split_file_max_chunks'),
            boundary=boundary
        )
        file_parts = glob.glob(f'{filepath}*')
        return file_parts
