import datetime
import decimal
import logging
import re
import sys
import psycopg2
import psycopg2.extras

from typing import Dict


from . import utils, split_gzip
from ...utils import safe_column_name

LOGGER = logging.getLogger(__name__)


class FastSyncTapPostgres:
    """
    Common functions for fastsync from a Postgres database
    """

    def __init__(self, connection_config, tap_type_to_target_type, target_quote=None):
        self.connection_config = connection_config
        self.tap_type_to_target_type = tap_type_to_target_type
        self.target_quote = target_quote
        self.conn = None
        self.curr = None
        self.primary_host_conn = None
        self.primary_host_curr = None

    @staticmethod
    def generate_replication_slot_name(dbname, tap_id=None, prefix='pipelinewise'):
        """Generate replication slot name with

        :param str dbname: Database name that will be part of the replication slot name
        :param str tap_id: Optional. If provided then it will be appended to the end of the slot name
        :param str prefix: Optional. Defaults to 'pipelinewise'
        :return: well formatted lowercased replication slot name
        :rtype: str
        """
        # Add tap_id to the end of the slot name if provided
        if tap_id:
            tap_id = f'_{tap_id}'
        # Convert None to empty string
        else:
            tap_id = ''

        slot_name = f'{prefix}_{dbname}{tap_id}'.lower()

        # Replace invalid characters to ensure replication slot name is in accordance with Postgres spec
        return re.sub('[^a-z0-9_]', '_', slot_name)

    @classmethod
    def __get_slot_name(cls, connection, dbname: str, tap_id: str,) -> str:
        """
        Finds the right slot name to use and returns it

        Args:
            connection: pg connection instance
            dbname: db name
            tap_id: Id of tha tap

        Returns:
            String: slot name

        """
        # Replication hosts pattern versions
        slot_name_v15 = cls.generate_replication_slot_name(dbname)
        slot_name_v16 = cls.generate_replication_slot_name(dbname, tap_id)

        v15_slots_count = 0

        try:
            # Backward compatibility: try to locate existing v15 slot first. PPW <= 0.15.0
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(f"SELECT * FROM pg_replication_slots WHERE slot_name = '{slot_name_v15}';")
                v15_slots_count = cur.rowcount

        except psycopg2.Error:
            LOGGER.exception('Error while looking for slots', exc_info=sys.exc_info())
        finally:
            if v15_slots_count > 0:
                slot_name = slot_name_v15
            else:
                slot_name = slot_name_v16

        return slot_name

    @classmethod
    def drop_slot(cls, connection_config: Dict) -> None:
        """
        Dropping the logical replication slot from primary server

        Args:
            connection_config: Dictionary with db credentials
        """
        LOGGER.info('Attempting to drop slot ...')

        LOGGER.debug('Creating a connection to Primary server ..')
        connection = cls.get_connection(connection_config, prioritize_primary=True)
        LOGGER.debug('Connection to Primary server created.')

        try:
            slot_name = cls.__get_slot_name(connection, connection_config['dbname'], connection_config['tap_id'])

            LOGGER.info('Dropping the slot "%s"', slot_name)
            # drop the replication host
            with connection.cursor() as cur:
                cur.execute(f'SELECT pg_drop_replication_slot(slot_name) '
                            f"FROM pg_replication_slots WHERE slot_name = '{slot_name}';")
                LOGGER.info('Number of dropped slots: %s', cur.rowcount)

        finally:
            connection.close()

    @classmethod
    def get_connection(cls, connection_config: Dict, prioritize_primary: bool = False):
        """
        Class method to create a pg connection instance with autocommit enabled
        Connection is either to the primary or a replica if its credentials are given

        Args:
            prioritize_primary: boolean to control whether to connect to primary or replica
            connection_config: Dictionary containing the db connection details
        Returns:
            pg Connection instance
        """
        template = "host='{}' port='{}' user='{}' password='{}' dbname='{}'"

        if prioritize_primary:
            LOGGER.info('Connecting to primary server')
            conn_string = template.format(
                connection_config['host'],
                connection_config['port'],
                connection_config['user'],
                connection_config['password'],
                connection_config['dbname'])
        else:
            LOGGER.info('Connecting to replica')
            conn_string = template.format(
                # Fastsync is using replica_{host|port|user|password} values from the config by default
                # to avoid making heavy load on the primary source database when syncing large tables
                #
                # If replica_{host|port|user|password} values are not defined in the config then it's
                # using the normal credentials to connect
                connection_config.get('replica_host', connection_config['host']),
                connection_config.get('replica_port', connection_config['port']),
                connection_config.get('replica_user', connection_config['user']),
                connection_config.get('replica_password', connection_config['password']),
                connection_config['dbname'])

        if 'ssl' in connection_config and connection_config['ssl'] == 'true':
            conn_string += " sslmode='require'"

        conn = psycopg2.connect(conn_string)

        # Set connection to autocommit
        conn.autocommit = True

        LOGGER.info('Connection to PGSQL server established')

        return conn

    def open_connection(self):
        """
        Open connection
        """
        self.conn = self.get_connection(self.connection_config, prioritize_primary=False)
        self.curr = self.conn.cursor()

    def close_connection(self):
        """
        Close connection
        """
        self.conn.close()

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

    def primary_host_query(self, query, params=None):
        """
        Run query on the primary host
        """
        LOGGER.info('Running query: %s', query)
        with self.primary_host_conn as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []

    # pylint: disable=no-member
    def create_replication_slot(self):
        """
        Create replication slot on the primary host

        IMPORTANT:
        Replication slot name is different after PPW >=0.16.0 and it's using a
        new pattern: pipelinewise_<dbname>_<tap_id>         PPW >= 0.16.0
        old pattern: pipelinewise_<dbname>                  PPW <= 0.15.x

        For backward compatibility and to keep the existing replication slots usable
        we check if there's any existing replication slot with the old format.
        If exists we keep using the old one but please note that using the old
        format you won't be able to do LOG_BASED replication from the same postgres
        database by multiple taps. If that the case then you need to drop the old
        replication slot and full-resync the new taps.
        """
        try:
            slot_name = self.__get_slot_name(self.primary_host_conn,
                                             self.connection_config['dbname'],
                                             self.connection_config['tap_id'])

            # Create the replication host
            self.primary_host_query(f"SELECT * FROM pg_create_logical_replication_slot('{slot_name}', 'wal2json')")
        except Exception as exc:
            # ERROR: replication slot already exists SQL state: 42710
            if hasattr(exc, 'pgcode') and exc.pgcode == '42710':
                pass
            else:
                raise exc

    # pylint: disable=too-many-branches,no-member,chained-comparison
    def fetch_current_log_pos(self):
        """
        Get the actual wal position in Postgres
        """
        # Create replication slot dedicated connection
        # Always use Primary server for creating replication_slot
        self.primary_host_conn = self.get_connection(self.connection_config, prioritize_primary=True)
        self.primary_host_curr = self.primary_host_conn.cursor()

        # Make sure PostgreSQL version is 9.4 or higher
        result = self.primary_host_query(
            "SELECT setting::int AS version FROM pg_settings WHERE name='server_version_num'")
        version = result[0].get('version')

        # Do not allow minor versions with PostgreSQL BUG #15114
        if (version >= 110000) and (version < 110002):
            raise Exception('PostgreSQL upgrade required to minor version 11.2')
        if (version >= 100000) and (version < 100007):
            raise Exception('PostgreSQL upgrade required to minor version 10.7')
        if (version >= 90600) and (version < 90612):
            raise Exception('PostgreSQL upgrade required to minor version 9.6.12')
        if (version >= 90500) and (version < 90516):
            raise Exception('PostgreSQL upgrade required to minor version 9.5.16')
        if (version >= 90400) and (version < 90421):
            raise Exception('PostgreSQL upgrade required to minor version 9.4.21')
        if version < 90400:
            raise Exception('Logical replication not supported before PostgreSQL 9.4')

        # Create replication slot
        self.create_replication_slot()

        # Close replication slot dedicated connection
        self.primary_host_conn.close()

        # is replica_host set ?
        if self.connection_config.get('replica_host'):
            # Get latest applied lsn from replica_host
            if version >= 100000:
                result = self.query('SELECT pg_last_wal_replay_lsn() AS current_lsn')
            elif version >= 90400:
                result = self.query('SELECT pg_last_xlog_replay_location() AS current_lsn')
            else:
                raise Exception('Logical replication not supported before PostgreSQL 9.4')
        else:
            # Get current lsn from primary host
            if version >= 100000:
                result = self.query('SELECT pg_current_wal_lsn() AS current_lsn')
            elif version >= 90400:
                result = self.query('SELECT pg_current_xlog_location() AS current_lsn')
            else:
                raise Exception('Logical replication not supported before PostgreSQL 9.4')

        current_lsn = result[0].get('current_lsn')
        file, index = current_lsn.split('/')
        lsn = (int(file, 16) << 32) + int(index, 16)

        return {
            'lsn': lsn,
            'version': 1
        }

    # pylint: disable=invalid-name
    def fetch_current_incremental_key_pos(self, table, replication_key):
        """
        Get the actual incremental key position in the table
        """
        schema_name, table_name = table.split('.')
        result = self.query(f'SELECT MAX({replication_key}) AS key_value FROM {schema_name}."{table_name}"')
        if len(result) == 0:
            raise Exception('Cannot get replication key value for table: {}'.format(table))

        postgres_key_value = result[0].get('key_value')
        key_value = postgres_key_value

        # Convert postgres data/datetime format to JSON friendly values
        if isinstance(postgres_key_value, datetime.datetime):
            key_value = postgres_key_value.isoformat()

        elif isinstance(postgres_key_value, datetime.date):
            key_value = postgres_key_value.isoformat() + 'T00:00:00'

        elif isinstance(postgres_key_value, decimal.Decimal):
            key_value = float(postgres_key_value)

        return {
            'replication_key': replication_key,
            'replication_key_value': key_value,
            'version': 1
        }

    def get_primary_keys(self, table):
        """
        Get the primary key of a table
        """
        schema_name, table_name = table.split('.')

        sql = """SELECT pg_attribute.attname
                    FROM pg_index, pg_class, pg_attribute, pg_namespace
                    WHERE
                        pg_class.oid = '{}."{}"'::regclass AND
                        indrelid = pg_class.oid AND
                        pg_class.relnamespace = pg_namespace.oid AND
                        pg_attribute.attrelid = pg_class.oid AND
                        pg_attribute.attnum = any(pg_index.indkey)
                    AND indisprimary""".format(schema_name, table_name)
        pk_specs = self.query(sql)
        if len(pk_specs) > 0:
            return [safe_column_name(k[0], self.target_quote) for k in pk_specs]

        return None

    def get_table_columns(self, table_name, max_num=None, date_type='date'):
        """
        Get PG table column details from information_schema
        """
        table_dict = utils.tablename_to_dict(table_name)

        if max_num:
            decimals = len(max_num.split('.')[1]) if '.' in max_num else 0
            decimal_format = f"""
              'CASE WHEN "' || column_name || '" IS NULL THEN NULL ELSE GREATEST(LEAST({max_num}, ROUND("' || column_name || '"::numeric , {decimals})), -{max_num}) END'
            """
            integer_format = """
              '"' || column_name || '"'
            """
        else:
            decimal_format = """
              '"' || column_name || '"'
            """
            integer_format = decimal_format

        sql = """
                SELECT
                    column_name
                    ,data_type
                    ,safe_sql_value
                    ,character_maximum_length
                FROM (SELECT
                column_name,
                data_type,
                CASE
                    WHEN data_type = 'ARRAY' THEN 'array_to_json("' || column_name || '") AS ' || column_name
                    WHEN data_type = 'date' THEN column_name || '::{} AS ' || column_name
                    WHEN udt_name = 'time' THEN 'replace("' || column_name || E'"::varchar,\\\'24:00:00\\\',\\\'00:00:00\\\') AS ' || column_name
                    WHEN udt_name = 'timetz' THEN 'replace(("' || column_name || E'" at time zone \'\'UTC\'\')::time::varchar,\\\'24:00:00\\\',\\\'00:00:00\\\') AS ' || column_name
                    WHEN udt_name in ('timestamp', 'timestamptz') THEN
                       'CASE WHEN "' ||column_name|| E'" < \\'0001-01-01 00:00:00.000\\' '
                            'OR "' ||column_name|| E'" > \\'9999-12-31 23:59:59.999\\' THEN \\'9999-12-31 23:59:59.999\\' '
                            'ELSE "' ||column_name|| '" END AS "' ||column_name|| '"'
                    WHEN data_type IN ('double precision', 'numeric', 'decimal', 'real') THEN {} || ' AS ' || column_name
                    WHEN data_type IN ('smallint', 'integer', 'bigint', 'serial', 'bigserial') THEN {} || ' AS ' || column_name
                    ELSE '"'||column_name||'"'
                END AS safe_sql_value,
                character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = '{}'
                    AND table_name = '{}'
                ORDER BY ordinal_position
                ) AS x
            """.format(date_type, decimal_format, integer_format, table_dict.get('schema_name'), table_dict.get('table_name'))
        return self.query(sql)

    def map_column_types_to_target(self, table_name):
        """
        Map PG column types to equivalent types in target
        """
        postgres_columns = self.get_table_columns(table_name)
        mapped_columns = []
        for pc in postgres_columns:
            column_type = self.tap_type_to_target_type(pc[1])
            # postgres bit type can have length greater than 1
            # most targets would want to map length 1 to boolean and the rest to number
            if isinstance(column_type, list):
                column_type = column_type[1 if pc[3] > 1 else 0]
            mapping = '{} {}'.format(safe_column_name(pc[0], self.target_quote), column_type)
            mapped_columns.append(mapping)

        return {
            'columns': mapped_columns,
            'primary_key': self.get_primary_keys(table_name)
        }

    # pylint: disable=too-many-arguments
    def copy_table(self,
                   table_name,
                   path,
                   max_num=None,
                   date_type='date',
                   split_large_files=False,
                   split_file_chunk_size_mb=1000,
                   split_file_max_chunks=20,
                   compress=True
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
        table_columns = self.get_table_columns(table_name, max_num, date_type)
        column_safe_sql_values = [c.get('safe_sql_value') for c in table_columns]

        # If self.get_table_columns returns zero row then table not exist
        if len(column_safe_sql_values) == 0:
            raise Exception('{} table not found.'.format(table_name))

        schema_name, table_name = table_name.split('.')

        sql = """COPY (SELECT {}
        ,now() AT TIME ZONE 'UTC'
        ,now() AT TIME ZONE 'UTC'
        ,null
        FROM {}."{}") TO STDOUT with CSV DELIMITER ','
        """.format(','.join(column_safe_sql_values), schema_name, table_name)
        LOGGER.info('Exporting data: %s', sql)

        gzip_splitter = split_gzip.open(path,
                                        mode='wb',
                                        chunk_size_mb=split_file_chunk_size_mb,
                                        max_chunks=split_file_max_chunks if split_large_files else 0,
                                        compress=compress)

        with gzip_splitter as split_gzip_files:
            self.curr.copy_expert(sql, split_gzip_files, size=131072)
