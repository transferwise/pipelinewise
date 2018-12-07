import psycopg2
from psycopg2 import extras
import gzip


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
            'bit': 'NUMBER',
            'varbit':'NUMBER',
            'bit varying':'NUMBER',
            'smalling':'NUMBER',
            'int':'NUMBER',
            'integer':'NUMBER',
            'bigint':'NUMBER',
            'smallserial':'NUMBER',
            'serial':'NUMBER',
            'bigserial':'NUMBER',
            'numeric':'NUMBER',
            'double precision':'NUMBER',
            'real':'FLOAT',
            'bool':'BOOLEAN',
            'boolean':'BOOLEAN',
            'date':'DATE',
            'timestamp':'TIMESTAMP',
            'timestamp without time zone':'TIMESTAMP_NTZ',
            'timestamp with time zone':'TIMESTAMP_TZ',
            'time':'TIME',
            'time without time zone':'TIMESTAMP_NTZ',
            'time with time zone':'TIMESTAMP_TZ'
        }.get(pg_type, 'VARCHAR')

    def open_connection(self):
        conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
            self.connection_config['host'],
            self.connection_config['dbname'],
            self.connection_config['user'],
            self.connection_config['password'],
            self.connection_config['port']
        )
        self.conn = psycopg2.connect(conn_string)
        self.curr = self.conn.cursor()

    def query(self, query, params=None):
        print("POSTGRES - Running query: {}".format(query))
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

        return self.query(sql)[0][0]

    def get_table_columns(self, table):
        table_schema = table.split('.')[0]
        table_name = table.split('.')[1]

        sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{}' and table_name = '{}' ORDER BY ordinal_position".format(table_schema, table_name)
        return self.query(sql)

    def snowflake_ddl(self, table, target_schema, is_temporary):
        table_name = table.split('.')[1]
        if is_temporary:
            table_name += '_temp'

        postgres_columns = self.get_table_columns(table)
        snowflake_columns = ["{} {}".format(pc[0], self.postgres_type_to_snowflake(pc[1])) for pc in postgres_columns]
        primary_key = self.get_primary_key(table)
        snowflake_ddl = "CREATE OR REPLACE TABLE {}.{} ({}, PRIMARY KEY ({}))".format(target_schema, table_name, ', '.join(snowflake_columns), primary_key)
        return(snowflake_ddl)
        
    def copy_table(self, table, path):
        table_columns = self.get_table_columns(table)
        columns = [c[0] for c in table_columns]

        sql = "COPY {} ({}) TO STDOUT with CSV DELIMITER ','".format(table, ','.join(columns))
        print("POSTGRES - Exporting data: {}".format(sql))
        with gzip.open(path, 'wt') as gzfile:
            self.curr.copy_expert(sql, gzfile)