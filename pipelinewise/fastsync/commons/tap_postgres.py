import datetime
import decimal
import gzip
import logging
import psycopg2
import psycopg2.extras

from . import utils
from ...utils import safe_column_name

LOGGER = logging.getLogger(__name__)


class FastSyncTapPostgres:
    """
    Common functions for fastsync from a Postgres database
    """

    def __init__(self, connection_config, tap_type_to_target_type):
        self.connection_config = connection_config
        self.tap_type_to_target_type = tap_type_to_target_type
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
        return f'{prefix}_{dbname}{tap_id}'.lower()

    def open_connection(self):
        """
        Open connection
        """
        conn_string = "host='{}' port='{}' user='{}' password='{}' dbname='{}'".format(
            # Fastsync is using replica_{host|port|user|password} values from the config by default
            # to avoid making heavy load on the primary source database when syncing large tables
            #
            # If replica_{host|port|user|password} values are not defined in the config then it's
            # using the normal credentials to connect
            self.connection_config.get('replica_host', self.connection_config['host']),
            self.connection_config.get('replica_port', self.connection_config['port']),
            self.connection_config.get('replica_user', self.connection_config['user']),
            self.connection_config.get('replica_password', self.connection_config['password']),
            self.connection_config['dbname'])
        self.conn = psycopg2.connect(conn_string)
        # Set connection to autocommit
        self.conn.autocommit = True
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
            # Replication hosts pattern versions
            slot_name_v15 = self.generate_replication_slot_name(dbname=self.connection_config['dbname'])
            slot_name_v16 = self.generate_replication_slot_name(dbname=self.connection_config['dbname'],
                                                                tap_id=self.connection_config['tap_id'])

            # Backward compatibility: try to locate existing v15 slot first. PPW <= 0.15.0
            v15_slots = self.primary_host_query(f'SELECT * FROM pg_replication_slots'
                                                f" WHERE slot_name = '{slot_name_v15}'")
            if len(v15_slots) > 0:
                slot_name = slot_name_v15
            else:
                slot_name = slot_name_v16

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
        primary_host_conn_string = "host='{}' port='{}' user='{}' password='{}' dbname='{}'".format(
            self.connection_config['host'],
            self.connection_config['port'],
            self.connection_config['user'],
            self.connection_config['password'],
            self.connection_config['dbname']
        )
        self.primary_host_conn = psycopg2.connect(primary_host_conn_string)
        # Set connection to autocommit
        self.primary_host_conn.autocommit = True
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

    def get_primary_key(self, table):
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
        primary_key = self.query(sql)
        if len(primary_key) > 0:
            return primary_key[0][0]

        return None

    def get_table_columns(self, table_name):
        """
        Get MySQL table column details from information_schema
        """
        table_dict = utils.tablename_to_dict(table_name)
        sql = """
                SELECT
                    column_name
                    ,data_type
                    ,safe_sql_value
                FROM (SELECT
                column_name,
                data_type,
                CASE
                    WHEN data_type = 'ARRAY' THEN 'array_to_json("' || column_name || '") AS ' || column_name
                    WHEN udt_name = 'time' THEN 'replace("' || column_name || E'"::varchar,\\\'24:00:00\\\',\\\'00:00:00\\\') AS ' || column_name
                    WHEN udt_name = 'timetz' THEN 'replace(("' || column_name || E'" at time zone \'\'UTC\'\')::time::varchar,\\\'24:00:00\\\',\\\'00:00:00\\\') AS ' || column_name
                    ELSE '"'||column_name||'"'
                END AS safe_sql_value
                FROM information_schema.columns
                WHERE table_schema = '{}'
                    AND table_name = '{}'
                ORDER BY ordinal_position
                ) AS x
            """.format(table_dict.get('schema_name'), table_dict.get('table_name'))
        return self.query(sql)

    def map_column_types_to_target(self, table_name):
        """
        Map MySQL column types to equivalent types in target
        """
        postgres_columns = self.get_table_columns(table_name)
        mapped_columns = ['{} {}'.format(safe_column_name(pc[0]),
                                         self.tap_type_to_target_type(pc[1])) for pc in postgres_columns]

        return {
            'columns': mapped_columns,
            'primary_key': safe_column_name(self.get_primary_key(table_name))
        }

    def copy_table(self, table_name, path):
        """
        Export data from table to a zipped csv
        """
        table_columns = self.get_table_columns(table_name)
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
        with gzip.open(path, 'wt') as gzfile:
            self.curr.copy_expert(sql, gzfile, size=131072)
