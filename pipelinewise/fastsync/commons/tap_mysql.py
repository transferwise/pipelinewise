import pymysql
import gzip
import csv
import datetime
import decimal
import gzip

import pymysql
from pymysql import InterfaceError, OperationalError

from . import utils


class FastSyncTapMySql:
    """
    Common functions for fastsync from a MySQL database
    """

    def __init__(self, connection_config, tap_type_to_target_type):
        self.connection_config = connection_config
        self.connection_config['charset'] = connection_config.get('charset', 'utf8')
        self.connection_config['export_batch_rows'] = connection_config.get('export_batch_rows', 20000)
        self.tap_type_to_target_type = tap_type_to_target_type
        self.conn = None
        self.conn_unbuffered = None

    def open_connections(self):
        """
        Open connection
        """
        self.conn = pymysql.connect(
            # Fastsync is using bulk_sync_{host|port|user|password} values from the config by default
            # to avoid making heavy load on the primary source database when syncing large tables
            #
            # If bulk_sync_{host|port|user|password} values are not defined in the config then it's
            # using the normal credentials to connect
            host=self.connection_config.get('bulk_sync_host', self.connection_config['host']),
            port=int(self.connection_config.get('bulk_sync_port', self.connection_config['port'])),
            user=self.connection_config.get('bulk_sync_user', self.connection_config['user']),
            password=self.connection_config.get('bulk_sync_password', self.connection_config['password']),
            charset=self.connection_config['charset'],
            cursorclass=pymysql.cursors.DictCursor)
        self.conn_unbuffered = pymysql.connect(
            # Fastsync is using bulk_sync_{host|port|user|password} values from the config by default
            # to avoid making heavy load on the primary source database when syncing large tables
            #
            # If bulk_sync_{host|port|user|password} values are not defined in the config then it's
            # using the normal credentials to connect
            host=self.connection_config.get('bulk_sync_host', self.connection_config['host']),
            port=int(self.connection_config.get('bulk_sync_port', self.connection_config['port'])),
            user=self.connection_config.get('bulk_sync_user', self.connection_config['user']),
            password=self.connection_config.get('bulk_sync_password', self.connection_config['password']),
            charset=self.connection_config['charset'],
            cursorclass=pymysql.cursors.SSCursor)

    def close_connections(self, silent=False):
        """
        Close connection
        """
        try:
            self.conn.close()
            self.conn_unbuffered.close()
        except Exception as exc:
            if not silent:
                utils.log(exc)
                utils.log('Connections seem to be already closed.')

    def query(self, query, params=None, return_as_cursor=False, n_retry=1):
        """
        Run query
        """
        utils.log('MYSQL - Running query: {}'.format(query))
        try:
            with self.conn as cur:
                cur.execute(query, params)

                if return_as_cursor:
                    return cur

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []
        except (InterfaceError, OperationalError) as exc:
            utils.log(f'Exception happened during running a query. Number of retries: {n_retry}. {exc}')
            if n_retry > 0:
                utils.log('Reopening the connections.')
                self.close_connections(silent=True)
                self.open_connections()
                utils.log('Retrying to run a query.')
                return self.query(query,
                                  params=params,
                                  return_as_cursor=return_as_cursor,
                                  n_retry=n_retry - 1)

            raise exc

    def fetch_current_log_pos(self):
        """
        Get the actual binlog position in MySQL
        """
        result = self.query('SHOW MASTER STATUS')
        if len(result) == 0:
            raise Exception('MySQL binary logging is not enabled.')

        binlog_pos = result[0]

        return {
            'log_file': binlog_pos.get('File'),
            'log_pos': binlog_pos.get('Position'),
            'version': binlog_pos.get('version', 1)
        }

    # pylint: disable=invalid-name
    def fetch_current_incremental_key_pos(self, table, replication_key):
        """
        Get the actual incremental key position in the table
        """
        result = self.query('SELECT MAX({}) AS key_value FROM {}'.format(replication_key, table))
        if len(result) == 0:
            raise Exception('Cannot get replication key value for table: {}'.format(table))

        mysql_key_value = result[0].get('key_value')
        key_value = mysql_key_value

        # Convert msyql data/datetime format to JSON friendly values
        if isinstance(mysql_key_value, datetime.datetime):
            key_value = mysql_key_value.isoformat()

        elif isinstance(mysql_key_value, datetime.date):
            key_value = mysql_key_value.isoformat() + 'T00:00:00'

        elif isinstance(mysql_key_value, decimal.Decimal):
            key_value = float(mysql_key_value)

        return {
            'replication_key': replication_key,
            'replication_key_value': key_value,
            'version': 1
        }

    def get_primary_key(self, table_name):
        """
        Get the primary key of a table
        """
        sql = "SHOW KEYS FROM {} WHERE Key_name = 'PRIMARY'".format(table_name)
        primary_key = self.query(sql)
        if len(primary_key) > 0:
            return primary_key[0].get('Column_name')

        return None

    def get_table_columns(self, table_name):
        """
        Get MySQL table column details from information_schema
        """
        table_dict = utils.tablename_to_dict(table_name)
        sql = """
                SELECT column_name,
                    data_type,
                    column_type,
                    CONCAT("REPLACE(", safe_sql_value, ", '\n', ' ')") safe_sql_value
                FROM (SELECT column_name,
                            data_type,
                            column_type,
                            CASE
                            WHEN data_type IN ('blob', 'tinyblob', 'mediumblob', 'longblob', 'geometry')
                                    THEN concat('hex(', column_name, ')')
                            WHEN data_type IN ('binary', 'varbinary')
                                    THEN concat('hex(trim(trailing CHAR(0x00) from ',COLUMN_NAME,'))')
                            WHEN data_type IN ('bit')
                                    THEN concat('cast(`', column_name, '` AS unsigned)')
                            WHEN data_type IN ('datetime', 'timestamp', 'date')
                                    THEN concat('nullif(`', column_name, '`,"0000-00-00 00:00:00")')
                            WHEN column_type IN ('tinyint(1)')
                                    THEN concat('CASE WHEN `' , column_name , '` is null THEN null WHEN `' , column_name , '` = 0 THEN 0 ELSE 1 END')
                            WHEN column_name = 'raw_data_hash'
                                    THEN concat('hex(', column_name, ')')
                            ELSE concat('cast(`', column_name, '` AS char CHARACTER SET utf8)')
                                END AS safe_sql_value,
                            ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = '{}'
                        AND table_name = '{}') x
                ORDER BY
                        ordinal_position
            """.format(table_dict.get('schema_name'), table_dict.get('table_name'))
        return self.query(sql)

    def map_column_types_to_target(self, table_name):
        """
        Map MySQL columnn types to equivalent types in target
        """
        mysql_columns = self.get_table_columns(table_name)
        mapped_columns = [
            '{} {}'.format(pc.get('column_name'),
                           self.tap_type_to_target_type(pc.get('data_type'), pc.get('column_type')))
            for pc in mysql_columns]

        return {
            'columns': mapped_columns,
            'primary_key': self.get_primary_key(table_name)
        }

    # pylint: disable=too-many-locals
    def copy_table(self, table_name, path):
        """
        Export data from table to a zipped csv
        """
        table_columns = self.get_table_columns(table_name)
        column_safe_sql_values = [c.get('safe_sql_value') for c in table_columns]

        # If self.get_table_columns returns zero row then table not exist
        if len(column_safe_sql_values) == 0:
            raise Exception('{} table not found.'.format(table_name))

        sql = """SELECT {}
        ,CONVERT_TZ( NOW(),@@session.time_zone,'+00:00') AS _SDC_EXTRACTED_AT
        ,CONVERT_TZ( NOW(),@@session.time_zone,'+00:00') AS _SDC_BATCHED_AT
        ,null AS _SDC_DELETED_AT
        FROM {}
        """.format(','.join(column_safe_sql_values), table_name)
        export_batch_rows = self.connection_config['export_batch_rows']
        exported_rows = 0
        with self.conn_unbuffered as cur:
            cur.execute(sql)
            with gzip.open(path, 'wt') as gzfile:
                writer = csv.writer(gzfile,
                                    delimiter=',',
                                    quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)

                while True:
                    rows = cur.fetchmany(export_batch_rows)

                    # No more rows to fetch, stop loop
                    if not rows:
                        break

                    # Log export status
                    exported_rows += len(rows)
                    if len(rows) == export_batch_rows:
                        # Then we believe this to be just an interim batch and not the final one so report on progress

                        utils.log(
                            "Exporting batch from {} to {} rows from {}...".format((exported_rows - export_batch_rows),
                                                                                   exported_rows, table_name))
                    # Write rows to file in one go
                    writer.writerows(rows)

                utils.log('Exported total of {} rows from {}...'.format(exported_rows, table_name))
