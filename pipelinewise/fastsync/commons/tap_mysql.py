import csv
import datetime
import decimal
import logging
from typing import Tuple

import pymysql
from pymysql import InterfaceError, OperationalError

from ...utils import safe_column_name
from . import split_gzip, utils

LOGGER = logging.getLogger(__name__)

DEFAULT_CHARSET = 'utf8'
DEFAULT_EXPORT_BATCH_ROWS = 50000
DEFAULT_SESSION_SQLS = [
    'SET @@session.time_zone="+0:00"',
    'SET @@session.wait_timeout=28800',
    'SET @@session.net_read_timeout=3600',
    'SET @@session.innodb_lock_wait_timeout=3600',
]


class FastSyncTapMySql:
    """
    Common functions for fastsync from a MySQL database
    """

    def __init__(self, connection_config: dict, tap_type_to_target_type, target_quote=None):
        self.connection_config = connection_config
        self.connection_config['charset'] = connection_config.get(
            'charset', DEFAULT_CHARSET
        )
        self.connection_config['export_batch_rows'] = connection_config.get(
            'export_batch_rows', DEFAULT_EXPORT_BATCH_ROWS
        )
        self.connection_config['session_sqls'] = connection_config.get(
            'session_sqls', DEFAULT_SESSION_SQLS
        )
        self.tap_type_to_target_type = tap_type_to_target_type
        self.target_quote = target_quote
        self.conn = None
        self.conn_unbuffered = None
        self.is_replica = False

    def get_connection_parameters(self) -> Tuple[dict, bool]:
        """
        Method to get connection parameters
        Connection is either to the primary or a replica if its credentials are given

        Args:
            connection_config: dictionary containing the db connection details
        Returns:
            dict with credentials
        """

        is_replica = False

        if 'replica_host' in self.connection_config:
            is_replica = True

        host = self.connection_config.get('replica_host', self.connection_config['host'])
        port = int(self.connection_config.get('replica_port', self.connection_config['port']))
        user = self.connection_config.get('replica_user', self.connection_config['user'])
        password = self.connection_config.get('replica_password', self.connection_config['password'])
        charset = self.connection_config['charset']

        return ({
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'charset': charset,
        }, is_replica)

    def open_connections(self):
        """
        Open connection
        """

        # Fastsync is using replica_{host|port|user|password} values from the config by default
        # to avoid making heavy load on the primary source database when syncing large tables
        #
        # If replica_{host|port|user|password} values are not defined in the config then it's
        # using the normal credentials to connect

        conn_params, is_replica = self.get_connection_parameters()

        self.is_replica = is_replica

        self.conn = pymysql.connect(
            **conn_params,
            cursorclass=pymysql.cursors.DictCursor,
        )
        self.conn_unbuffered = pymysql.connect(
            **conn_params,
            cursorclass=pymysql.cursors.SSCursor,
        )

        # Set session variables by running a list of SQLs which is defined
        # in the optional session_sqls connection parameters
        self.run_session_sqls()

    def run_session_sqls(self):
        """
        Run list of SQLs from the "session_sqls" optional connection parameter
        """
        session_sqls = self.connection_config.get('session_sqls', DEFAULT_SESSION_SQLS)

        warnings = []
        if session_sqls and isinstance(session_sqls, list):
            for sql in session_sqls:
                try:
                    self.query(sql)
                    self.query(sql, self.conn_unbuffered)
                except pymysql.err.InternalError:
                    warnings.append(f'Could not set session variable: {sql}')

        if warnings:
            LOGGER.warning(
                'Encountered non-fatal errors when configuring session that could impact performance:'
            )
        for warning in warnings:
            LOGGER.warning(warning)

    def close_connections(self, silent=False):
        """
        Close connection
        """
        try:
            self.conn.close()
            self.conn_unbuffered.close()
        except Exception as exc:
            if not silent:
                LOGGER.exception(exc)
                LOGGER.info('Connections seem to be already closed.')

    # pylint: disable=too-many-arguments
    def query(self, query, conn=None, params=None, return_as_cursor=False, n_retry=1):
        """
        Run query
        """
        LOGGER.info('Running query: %s', query)
        if conn is None:
            conn = self.conn

        try:
            with conn as cur:
                cur.execute(query, params)

                if return_as_cursor:
                    return cur

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []
        except (InterfaceError, OperationalError) as exc:
            LOGGER.exception(
                'Exception happened during running a query. Number of retries: %s. %s',
                n_retry,
                exc,
            )
            if n_retry > 0:
                LOGGER.info('Reopening the connections.')
                self.close_connections(silent=True)
                self.open_connections()
                LOGGER.info('Retrying to run a query.')
                return self.query(
                    query,
                    params=params,
                    return_as_cursor=return_as_cursor,
                    n_retry=n_retry - 1,
                )

            raise exc

    def fetch_current_log_pos(self):
        """
        Get the actual binlog position in MySQL
        """
        if self.is_replica:
            result = self.query('SHOW SLAVE STATUS')
            if len(result) == 0:
                raise Exception('MySQL binary logging is not enabled.')
            binlog_pos = result[0]
            log_file = binlog_pos.get('Master_Log_File')
            log_pos = binlog_pos.get('Read_Master_Log_Pos')
            version = binlog_pos.get('version', 1)

        else:
            result = self.query('SHOW MASTER STATUS')
            if len(result) == 0:
                raise Exception('MySQL binary logging is not enabled.')
            binlog_pos = result[0]
            log_file = binlog_pos.get('File')
            log_pos = binlog_pos.get('Position')
            version = binlog_pos.get('version', 1)

        return {
            'log_file': log_file,
            'log_pos': log_pos,
            'version': version,
        }

    # pylint: disable=invalid-name
    def fetch_current_incremental_key_pos(self, table, replication_key):
        """
        Get the actual incremental key position in the table
        """
        result = self.query(
            'SELECT MAX({}) AS key_value FROM {}'.format(replication_key, table)
        )
        if len(result) == 0:
            raise Exception(
                'Cannot get replication key value for table: {}'.format(table)
            )

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
            'version': 1,
        }

    def get_primary_keys(self, table_name):
        """
        Get the primary key of a table
        """
        table_dict = utils.tablename_to_dict(table_name)
        sql = "SHOW KEYS FROM `{}`.`{}` WHERE Key_name = 'PRIMARY'".format(
            table_dict['schema_name'], table_dict['table_name']
        )
        pk_specs = self.query(sql)
        if len(pk_specs) > 0:
            return [
                safe_column_name(k.get('Column_name'), self.target_quote)
                for k in pk_specs
            ]

        return None

    def get_table_columns(self, table_name, max_num=None, date_type='date'):
        """
        Get MySQL table column details from information_schema
        """
        table_dict = utils.tablename_to_dict(table_name)

        if max_num:
            decimals = len(max_num.split('.')[1]) if '.' in max_num else 0
            decimal_format = f"""
              CONCAT('GREATEST(LEAST({max_num}, ROUND(`', column_name, '`, {decimals})), -{max_num})')
            """
            integer_format = """
              CONCAT('`', column_name, '`')
            """
        else:
            decimal_format = """
              CONCAT('`', column_name, '`')
            """
            integer_format = decimal_format

        schema_name = table_dict.get('schema_name')
        table_name = table_dict.get('table_name')

        sql = f"""
                SELECT column_name,
                    data_type,
                    column_type,
                    safe_sql_value
                FROM (SELECT column_name,
                            data_type,
                            column_type,
                            CASE
                            WHEN data_type IN ('blob', 'tinyblob', 'mediumblob', 'longblob')
                                    THEN CONCAT('REPLACE(hex(`', column_name, '`)', ", '\n', ' ')")
                            WHEN data_type IN ('binary', 'varbinary')
                                    THEN concat('REPLACE(REPLACE(hex(trim(trailing CHAR(0x00) from `',COLUMN_NAME,'`))', ", '\n', ' '), '\r', '')")
                            WHEN data_type IN ('bit')
                                    THEN concat('cast(`', column_name, '` AS unsigned)')
                            WHEN data_type IN ('date')
                                    THEN concat('nullif(CAST(`', column_name, '` AS {date_type}),STR_TO_DATE("0000-00-00 00:00:00", "%Y-%m-%d %T"))')
                            WHEN data_type IN ('datetime', 'timestamp')
                                    THEN concat('nullif(`', column_name, '`,STR_TO_DATE("0000-00-00 00:00:00", "%Y-%m-%d %T"))')
                            WHEN column_type IN ('tinyint(1)')
                                    THEN concat('CASE WHEN `' , column_name , '` is null THEN null WHEN `' , column_name , '` = 0 THEN 0 ELSE 1 END')
                            WHEN column_type IN ('geometry', 'point', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'multipolygon', 'geometrycollection')
                                    THEN concat('ST_AsGeoJSON(', column_name, ')')
                            WHEN column_name = 'raw_data_hash'
                                    THEN concat('REPLACE(REPLACE(hex(`', column_name, '`)', ", '\n', ' '), '\r', '')")
                            WHEN data_type IN ('double', 'numeric', 'float', 'decimal', 'real')
                                    THEN {decimal_format}
                            WHEN data_type IN ('smallint', 'integer', 'bigint', 'mediumint', 'int')
                                    THEN {integer_format}
                            ELSE concat('REPLACE(REPLACE(REPLACE(cast(`', column_name, '` AS char CHARACTER SET utf8)', ", '\n', ' '), '\r', ''), '\0', '')")
                                END AS safe_sql_value,
                            ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = '{schema_name}'
                        AND table_name = '{table_name}') x
                ORDER BY
                        ordinal_position
            """  # noqa: E501
        return self.query(sql)

    def map_column_types_to_target(self, table_name):
        """
        Map MySQL column types to equivalent types in target
        """
        mysql_columns = self.get_table_columns(table_name)
        mapped_columns = [
            '{} {}'.format(
                safe_column_name(pc.get('column_name'), self.target_quote),
                self.tap_type_to_target_type(
                    pc.get('data_type'), pc.get('column_type')
                ),
            )
            for pc in mysql_columns
        ]

        return {
            'columns': mapped_columns,
            'primary_key': self.get_primary_keys(table_name),
        }

    # pylint: disable=too-many-locals
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
        table_columns = self.get_table_columns(table_name, max_num, date_type)
        column_safe_sql_values = [c.get('safe_sql_value') for c in table_columns]

        # If self.get_table_columns returns zero row then table not exist
        if len(column_safe_sql_values) == 0:
            raise Exception('{} table not found.'.format(table_name))

        table_dict = utils.tablename_to_dict(table_name)
        sql = """SELECT {}
        ,CONVERT_TZ( NOW(),@@session.time_zone,'+00:00') AS _SDC_EXTRACTED_AT
        ,CONVERT_TZ( NOW(),@@session.time_zone,'+00:00') AS _SDC_BATCHED_AT
        ,null AS _SDC_DELETED_AT
        FROM `{}`.`{}`
        """.format(
            ','.join(column_safe_sql_values),
            table_dict['schema_name'],
            table_dict['table_name'],
        )
        export_batch_rows = self.connection_config['export_batch_rows']
        exported_rows = 0
        with self.conn_unbuffered as cur:
            cur.execute(sql)
            gzip_splitter = split_gzip.open(
                path,
                mode='wt',
                chunk_size_mb=split_file_chunk_size_mb,
                max_chunks=split_file_max_chunks if split_large_files else 0,
                compress=compress,
            )

            with gzip_splitter as split_gzip_files:
                writer = csv.writer(
                    split_gzip_files,
                    delimiter=',',
                    quotechar='"',
                    quoting=csv.QUOTE_MINIMAL,
                )

                while True:
                    rows = cur.fetchmany(export_batch_rows)

                    # No more rows to fetch, stop loop
                    if not rows:
                        break

                    # Log export status
                    exported_rows += len(rows)
                    if len(rows) == export_batch_rows:
                        # Then we believe this to be just an interim batch and not the final one so report on progress

                        LOGGER.info(
                            'Exporting batch from %s to %s rows from %s...',
                            (exported_rows - export_batch_rows),
                            exported_rows,
                            table_name,
                        )
                    # Write rows to file in one go
                    writer.writerows(rows)

                LOGGER.info(
                    'Exported total of %s rows from %s...', exported_rows, table_name
                )
