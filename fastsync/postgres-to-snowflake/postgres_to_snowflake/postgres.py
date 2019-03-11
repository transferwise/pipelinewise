import psycopg2
from psycopg2 import extras
import gzip
import datetime

import postgres_to_snowflake.utils as utils


class Postgres:
    def __init__(self, connection_config):
        self.connection_config = connection_config


    def postgres_type_to_snowflake(self, pg_type):
        return {
            'char':'VARCHAR',
            'character':'VARCHAR',
            'varchar':'VARCHAR',
            'character varying':'VARCHAR',
            'text':'TEXT',
            'bit': 'BOOLEAN',
            'varbit':'NUMBER',
            'bit varying':'NUMBER',
            'smallint':'NUMBER',
            'int':'NUMBER',
            'integer':'NUMBER',
            'bigint':'NUMBER',
            'smallserial':'NUMBER',
            'serial':'NUMBER',
            'bigserial':'NUMBER',
            'numeric':'FLOAT',
            'double precision':'FLOAT',
            'real':'FLOAT',
            'bool':'BOOLEAN',
            'boolean':'BOOLEAN',
            'date':'TIMESTAMP_NTZ',
            'timestamp':'TIMESTAMP',
            'timestamp without time zone':'TIMESTAMP_NTZ',
            'timestamp with time zone':'TIMESTAMP_TZ',
            'time':'TIME',
            'time without time zone':'TIMESTAMP_NTZ',
            'time with time zone':'TIMESTAMP_TZ'
        }.get(pg_type, 'VARCHAR')


    def open_connection(self):
        conn_string = "host='{}' port='{}' user='{}' password='{}' dbname='{}'".format(
            # Fastsync is using bulk_sync_{host|port|user|password} values from the config by default
            # to avoid making heavy load on the primary source database when syncing large tables
            #
            # If bulk_sync_{host|port|user|password} values are not defined in the config then it's
            # using the normal credentials to connect
            self.connection_config.get('bulk_sync_host', self.connection_config['host']),
            self.connection_config.get('bulk_sync_port', self.connection_config['port']),
            self.connection_config.get('bulk_sync_user', self.connection_config['user']),
            self.connection_config.get('bulk_sync_password', self.connection_config['password']),
            self.connection_config['dbname']
        )
        self.conn = psycopg2.connect(conn_string)
        self.curr = self.conn.cursor()


    def close_connection(self):
        self.conn.close()


    def query(self, query, params=None):
        utils.log("POSTGRES - Running query: {}".format(query))
        with self.conn as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    query,
                    params
                )

                if cur.rowcount > 0:
                    return cur.fetchall()
                else:
                    return []


    def fetch_current_incremental_key_pos(self, table, replication_key):
        result = self.query("SELECT MAX({}) AS key_value FROM {}".format(replication_key, table))
        if len(result) == 0:
            raise Exception("Cannot get replication key value for table: {}".format(table))
        else:
            mysql_key_value = result[0].get("key_value")
            key_value = mysql_key_value

            # Convert msyql data/datetime format to JSON friendly values
            if isinstance(mysql_key_value, datetime.datetime):
                key_value = mysql_key_value.isoformat() + '+00:00'

            elif isinstance(mysql_key_value, datetime.date):
                key_value = mysql_key_value.isoformat() + 'T00:00:00+00:00'

            return {
                "key": replication_key,
                "key_value": key_value
            }


    def get_primary_key(self, table):
        sql = """SELECT pg_attribute.attname
                    FROM pg_index, pg_class, pg_attribute, pg_namespace
                    WHERE
                        pg_class.oid = '{}'::regclass AND
                        indrelid = pg_class.oid AND
                        pg_class.relnamespace = pg_namespace.oid AND
                        pg_attribute.attrelid = pg_class.oid AND
                        pg_attribute.attnum = any(pg_index.indkey)
                    AND indisprimary""".format(table)
        pk = self.query(sql)
        if len(pk) > 0:
            return pk[0][0]
        else:
            return None


    def get_table_columns(self, table_name):
        table_dict = utils.tablename_to_dict(table_name)
        sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{}' and table_name = '{}' ORDER BY ordinal_position".format(table_dict.get('schema'), table_dict.get('name'))
        return self.query(sql)


    def snowflake_ddl(self, table_name, target_schema, is_temporary):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('name') if not is_temporary else table_dict.get('temp_name')

        postgres_columns = self.get_table_columns(table_name)
        snowflake_columns = ["{} {}".format(pc[0], self.postgres_type_to_snowflake(pc[1])) for pc in postgres_columns]
        primary_key = self.get_primary_key(table_name)
        if primary_key:
            snowflake_ddl = "CREATE OR REPLACE TABLE {}.{} ({}, PRIMARY KEY ({}))".format(target_schema, target_table, ', '.join(snowflake_columns), primary_key)
        else:
            snowflake_ddl = "CREATE OR REPLACE TABLE {}.{} ({})".format(target_schema, target_table, ', '.join(snowflake_columns))
        return(snowflake_ddl)


    def copy_table(self, table_name, path):
        table_columns = self.get_table_columns(table_name)
        columns = [c[0] for c in table_columns]

        # If self.get_table_columns returns zero row then table not exist
        if len(columns) == 0:
            raise Exception("{} table not found.".format(table_name))

        sql = "COPY {} ({}) TO STDOUT with CSV DELIMITER ','".format(table_name, ','.join(columns))
        utils.log("POSTGRES - Exporting data: {}".format(sql))
        with gzip.open(path, 'wt') as gzfile:
            self.curr.copy_expert(sql, gzfile)

