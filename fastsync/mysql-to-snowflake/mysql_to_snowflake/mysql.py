import pymysql
import gzip
import csv
import os

import mysql_to_snowflake.utils as utils


class MySql:
    def __init__(self, connection_config):
        self.connection_config = connection_config
        self.connection_config['charset'] = connection_config.get('charset', 'utf8')
        self.connection_config['export_batch_rows'] = connection_config.get('export_batch_rows', 20000)


    def mysql_type_to_snowflake(self, data_type, column_type):
        return {
            'char':'VARCHAR',
            'varchar':'VARCHAR',
            'binary':'VARCHAR',
            'varbinary':'VARCHAR',
            'blob':'VARCHAR',
            'tinyblob':'VARCHAR',
            'mediumblob':'VARCHAR',
            'longblob':'VARCHAR',
            'geometry':'VARCHAR',
            'text':'VARCHAR',
            'tinytext':'VARCHAR',
            'mediumtext':'VARCHAR',
            'longtext':'VARCHAR',
            'enum':'VARCHAR',
            'int':'NUMBER',
            'tinyint':'BOOLEAN' if column_type == 'tinyint(1)' else 'NUMBER',
            'smallint':'NUMBER',
            'bigint':'NUMBER',
            'bit':'BOOLEAN',
            'decimal':'FLOAT',
            'double':'FLOAT',
            'float':'FLOAT',
            'bool':'BOOLEAN',
            'boolean':'BOOLEAN',
            'date':'TIMESTAMP_NTZ(3)',
            'datetime':'TIMESTAMP_NTZ(3)',
            'timestamp':'TIMESTAMP_NTZ(3)',
        }.get(data_type, 'VARCHAR')


    def open_connection(self):
        self.conn = pymysql.connect(
            host = self.connection_config['host'],
            port = self.connection_config['port'],
            user = self.connection_config['user'],
            password = self.connection_config['password'],
            charset = self.connection_config['charset'],
            cursorclass = pymysql.cursors.DictCursor
        )
        self.conn_unbuffered = pymysql.connect(
            host = self.connection_config['host'],
            port = self.connection_config['port'],
            user = self.connection_config['user'],
            password = self.connection_config['password'],
            charset = self.connection_config['charset'],
            cursorclass = pymysql.cursors.SSCursor
        )


    def close_connection(self):
        self.conn.close()


    def query(self, query, params=None, return_as_cursor=False):
        utils.log("MYSQL - Running query: {}".format(query))
        with self.conn as cur:
            cur.execute(
                query,
                params
            )

            if return_as_cursor:
                return cur

            if cur.rowcount > 0:
                return cur.fetchall()
            else:
                return []


    def fetch_current_log_file_and_pos(self):
        result = self.query("SHOW MASTER STATUS")
        if len(result) == 0:
            raise Exception("MySQL binary logging is not enabled.")
        else:
            return result[0]


    def get_primary_key(self, table_name):
        sql = "SHOW KEYS FROM {} WHERE Key_name = 'PRIMARY'".format(table_name)
        pk = self.query(sql)
        if len(pk) > 0:
            return pk[0].get('Column_name')
        else:
            return None


    def get_table_columns(self, table_name):
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
                            WHEN data_type IN ('blob', 'tinyblob', 'mediumblob', 'longblob', 'binary', 'varbinary', 'geometry')
                                    THEN concat('hex(', column_name, ')')
                            WHEN data_type IN ('bit')
                                    THEN concat('cast(`', column_name, '` AS unsigned)')
                            WHEN data_type IN ('datetime', 'timestamp', 'date')
                                    THEN concat('nullif(`', column_name, '`,"0000-00-00 00:00:00")')
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
            """.format(table_dict.get('schema'), table_dict.get('name'))
        return self.query(sql)


    def snowflake_ddl(self, table_name, target_schema, is_temporary):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('name') if not is_temporary else table_dict.get('temp_name')

        mysql_columns = self.get_table_columns(table_name)
        snowflake_columns = ["{} {}".format(pc.get('column_name'), self.mysql_type_to_snowflake(pc.get('data_type'), pc.get('column_type'))) for pc in mysql_columns]
        primary_key = self.get_primary_key(table_name)
        if primary_key:
            snowflake_ddl = "CREATE OR REPLACE TABLE {}.{} ({}, PRIMARY KEY ({}))".format(target_schema, target_table, ', '.join(snowflake_columns), primary_key)
        else:
            snowflake_ddl = "CREATE OR REPLACE TABLE {}.{} ({})".format(target_schema, target_table, ', '.join(snowflake_columns))
        return(snowflake_ddl)


    def copy_table(self, table_name, path):
        table_columns = self.get_table_columns(table_name)
        column_safe_sql_values = [c.get('safe_sql_value') for c in table_columns]

        # If self.get_table_columns returns zero row then table not exist
        if len(column_safe_sql_values) == 0:
            raise Exception("{} table not found.".format(table_name))

        sql = "SELECT {} FROM {}".format(','.join(column_safe_sql_values), table_name)
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
                    exported_rows += export_batch_rows
                    utils.log("Exported {} rows from {}...".format(exported_rows, table_name))

                    # Write rows to file
                    for row in rows:
                        writer.writerow(row)

