import pymysql
import gzip

import mysql_to_snowflake.utils as utils


class MySql:
    def __init__(self, connection_config):
        self.connection_config = connection_config
        self.connection_config['charset'] = connection_config.get('charset', 'utf8')
        self.connection_config['export_batch_rows'] = connection_config.get('export_batch_rows', 20000)


    def mysql_type_to_snowflake(self, pg_type):
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
            'tinyint':'NUMBER',
            'smallint':'NUMBER',
            'bigint':'NUMBER',
            'bit':'NUMBER',
            'decimal':'NUMBER',
            'double':'NUMBER',
            'float':'NUMBER',
            'bool':'BOOLEAN',
            'boolean':'BOOLEAN',
            'date':'DATE',
            'datetime':'TIMESTAMP_NTZ',
            'timestamp':'TIMESTAMP_NTZ',
        }.get(pg_type, 'VARCHAR')


    def open_connection(self):
        self.conn = pymysql.connect(
            host = self.connection_config['host'],
            port = self.connection_config['port'],
            user = self.connection_config['user'],
            password = self.connection_config['password'],
            charset = self.connection_config['charset'],
            cursorclass = pymysql.cursors.DictCursor
        )


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
        return self.query(sql)[0].get('Column_name')


    def get_table_columns(self, table_name):
        table_dict = utils.tablename_to_dict(table_name)
        sql = """
                SELECT column_name,
                    data_type,
                    CONCAT("CASE WHEN ", column_name, " IS NULL THEN '' ELSE CONCAT('\\"', REPLACE(REPLACE(", safe_sql_value, ", '\\"', '\\"\\"'), '\n', ' '),'\\"') END") safe_sql_value
                FROM (SELECT column_name,
                            data_type,
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
        snowflake_columns = ["{} {}".format(pc.get('column_name'), self.mysql_type_to_snowflake(pc.get('data_type'))) for pc in mysql_columns]
        primary_key = self.get_primary_key(table_name)
        snowflake_ddl = "CREATE OR REPLACE TABLE {}.{} ({}, PRIMARY KEY ({}))".format(target_schema, target_table, ', '.join(snowflake_columns), primary_key)
        return(snowflake_ddl)


    def copy_table(self, table_name, path):
        table_columns = self.get_table_columns(table_name)
        column_safe_sql_values = [c.get('safe_sql_value') for c in table_columns]

        sql = "SELECT CONCAT_WS(',', {}) AS csv_row FROM {}".format(','.join(column_safe_sql_values), table_name)
        cur = self.query(sql, return_as_cursor=True)
        export_batch_rows = self.connection_config['export_batch_rows']
        exported_rows = 0


        # Write and zip exported rows in batches
        with gzip.open(path, 'wb') as gzfile:
            while True:
                # Calculate number of exported rows
                sql_rows = cur.fetchmany(export_batch_rows)
                if exported_rows + export_batch_rows < cur.rowcount:
                    exported_rows += export_batch_rows
                else:
                    exported_rows = cur.rowcount

                # No more rows to fetch, stop loop
                if not sql_rows:
                    break

                # Write exported rows to file
                utils.log("{}/{} rows exported from {}...".format(exported_rows, cur.rowcount, table_name))
                gzfile.write('\n'.join([r.get('csv_row') for r in sql_rows]).encode('utf-8'))
            
                # Add extra new line to the end of the file required by Snowflake
                gzfile.write('\n'.encode('utf-8'))
        
