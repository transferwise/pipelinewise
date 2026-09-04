import datetime
import decimal
import logging
import re
import time
import psycopg2
import psycopg2.extras

from typing import Dict

from . import utils, split_gzip
from ...utils import safe_column_name

LOGGER = logging.getLogger(__name__)

# YugabyteDB keeps a slot's `active` flag set for up to
# ysql_cdc_active_replication_slot_window_ms (default 5 minutes) after the last consumer
# disconnects; pg_drop_replication_slot fails with "slot is active" during that window,
# so drop_slot retries instead of failing the resync outright. Dropping a slot shortly
# after heavy CDC activity can also transiently surface as InFailedSqlTransaction or
# SerializationFailure while the catalog-version bump from that activity settles.
_DROP_SLOT_RETRY_ATTEMPTS = 10
_DROP_SLOT_RETRY_INTERVAL_SECONDS = 30
_RETRYABLE_DROP_SLOT_ERRORS = (
    psycopg2.errors.InFailedSqlTransaction,  # pylint: disable=no-member
    psycopg2.errors.SerializationFailure,  # pylint: disable=no-member
)


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
    def drop_slot(cls, connection_config: Dict) -> None:
        """
        Drop the logical replication slot used by this tap, tolerating YugabyteDB's
        lingering "slot is active" window after the last consumer disconnects.

        Args:
            connection_config: Dictionary with db credentials
        """
        LOGGER.info('Attempting to drop slot ...')
        connection = cls.get_connection(connection_config)
        slot_name = cls.generate_replication_slot_name(
            connection_config['dbname'], connection_config['tap_id']
        )

        try:
            for attempt in range(1, _DROP_SLOT_RETRY_ATTEMPTS + 1):
                try:
                    with connection.cursor() as cur:
                        cur.execute(
                            f'SELECT pg_drop_replication_slot(slot_name) '
                            f"FROM pg_replication_slots WHERE slot_name = '{slot_name}';"
                        )
                        LOGGER.info('Number of dropped slots: %s', cur.rowcount)
                    return
                except psycopg2.Error as exc:
                    retryable = 'is active' in str(exc) or isinstance(exc, _RETRYABLE_DROP_SLOT_ERRORS)
                    if not retryable or attempt == _DROP_SLOT_RETRY_ATTEMPTS:
                        raise
                    LOGGER.info(
                        'Slot "%s" drop failed (%s), retrying in %s seconds (attempt %s/%s)',
                        slot_name,
                        exc.__class__.__name__,
                        _DROP_SLOT_RETRY_INTERVAL_SECONDS,
                        attempt,
                        _DROP_SLOT_RETRY_ATTEMPTS,
                    )
                    connection.rollback()
                    time.sleep(_DROP_SLOT_RETRY_INTERVAL_SECONDS)
        finally:
            connection.close()

    @classmethod
    def get_connection(cls, connection_config: Dict):
        """
        Class method to create a YSQL connection instance with autocommit enabled

        Args:
            connection_config: Dictionary containing the db connection details
        Returns:
            psycopg2 Connection instance
        """
        template = "host='{}' port='{}' user='{}' password='{}' dbname='{}'"
        conn_string = template.format(
            connection_config['host'],
            connection_config['port'],
            connection_config['user'],
            connection_config['password'],
            connection_config['dbname'],
        )

        if connection_config.get('ssl') == 'true':
            conn_string += " sslmode='require'"

        conn = psycopg2.connect(conn_string)

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

    # pylint: disable=invalid-name
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
            """  # noqa E501 pylint: disable=line-too-long
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

        # pylint: disable = line-too-long
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
        # pylint: enable = line-too-long

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

    # pylint: disable=too-many-arguments, too-many-positional-arguments
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
        if self._snapshot_ht is not None:
            # Session-level GUC; must be its own statement, not inside a transaction block
            # (YugabyteDB rejects `SET LOCAL yb_read_time` inside BEGIN/COMMIT).
            LOGGER.info('Pinning export snapshot to yb_read_time %s ht', self._snapshot_ht)
            self.curr.execute(f"SET yb_read_time TO '{self._snapshot_ht} ht'")

        table_columns = self.get_table_columns(table_name, max_num, date_type)
        column_safe_sql_values = [c.get('safe_sql_value') for c in table_columns]

        # If self.get_table_columns returns zero row then table not exist
        if len(column_safe_sql_values) == 0:
            raise Exception(f'{table_name} table not found.')

        schema_name, table_name = table_name.split('.')

        column_safe_sql_values = column_safe_sql_values + [
            "now() AT TIME ZONE 'UTC' AS _SDC_EXTRACTED_AT",
            "now() AT TIME ZONE 'UTC' AS _SDC_BATCHED_AT",
            'null _SDC_DELETED_AT'
        ]

        sql = f"""COPY (SELECT {','.join(column_safe_sql_values)}
        FROM {schema_name}."{table_name}") TO STDOUT with CSV DELIMITER ','
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
