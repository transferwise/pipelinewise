from typing import Union

import psycopg2
import psycopg2.extras
import pymongo
import pymysql
import snowflake.connector
from google.cloud import bigquery

from pymongo.database import Database

from pipelinewise.fastsync.commons.target_bigquery import safe_name


# pylint: disable=too-many-arguments
def run_query_postgres(query, host, port, user, password, database):
    """Run and SQL query in a postgres database"""
    result_rows = []
    with psycopg2.connect(
        host=host, port=port, user=user, password=password, database=database
    ) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.rowcount > 0 and cur.description:
                result_rows = cur.fetchall()
    return result_rows


def run_query_mysql(query, host, port, user, password, database):
    """Run and SQL query in a mysql database"""
    result_rows = []
    with pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor,
        ssl={'': True}
    ) as cur:
        cur.execute(query)
        if cur.rowcount > 0:
            result_rows = cur.fetchall()
    return result_rows


def run_query_snowflake(query, account, database, warehouse, user, password):
    """Run and SQL query in a snowflake database"""
    result_rows = []
    with snowflake.connector.connect(
        account=account,
        database=database,
        warehouse=warehouse,
        user=user,
        password=password,
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.rowcount > 0:
                result_rows = cur.fetchall()
    return result_rows


def safe_name_bigquery(name):
    """Return the safe_name of a column in BigQuery"""
    return safe_name(name, quotes=False)


def delete_dataset_bigquery(dataset, project):
    """Run and SQL query in a BigQuery database"""
    client = bigquery.Client(project=project)
    client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)


def run_query_bigquery(query, project):
    """Run and SQL query in a BigQuery database"""
    client = bigquery.Client(project=project)
    query_job = client.query(query)
    return [r.values() for r in query_job.result()]


def run_query_redshift(query, host, port, user, password, database):
    """Redshift is compatible with postgres"""
    return run_query_postgres(query, host, port, user, password, database)


def sql_get_columns_for_table(table_schema: str, table_name: str) -> list:
    """Generate an SQL command that returns the list of column of a specific
    table. Compatible with MySQL/ MariaDB/ Postgres and Snowflake

    table_schema and table_name can be lowercase and uppercase strings.
    It's using the IN clause to avoid transforming the entire
    information_schema.columns table"""
    return f"""
    SELECT column_name
      FROM information_schema.columns
     WHERE table_schema IN ('{table_schema.upper()}', '{table_schema.lower()}')
       AND table_name IN ('{table_name.upper()}', '{table_name.lower()}')"""


def sql_get_columns_for_table_bigquery(table_schema: str, table_name: str) -> list:
    """Generate an SQL command that returns the list of column of a specific
    table. Compatible with MySQL/ MariaDB/ Postgres and Snowflake

    table_schema and table_name can be lowercase and uppercase strings.
    It's using the IN clause to avoid transforming the entire
    information_schema.columns table"""
    return f"""
    SELECT column_name
      FROM {table_schema}.INFORMATION_SCHEMA.COLUMNS
     WHERE table_name IN ('{table_name.upper()}', '{table_name.lower()}')"""


def sql_get_columns_mysql(schemas: list) -> str:
    """Generates an SQL command that gives the list of columns of every table
    in a specific schema from a mysql database"""
    sql_schemas = ', '.join(f"'{schema}'" for schema in schemas)

    return f"""
    SELECT table_name, GROUP_CONCAT(CONCAT(column_name, ':', data_type, ':', column_type)
                                    ORDER BY column_name SEPARATOR ';')
      FROM information_schema.columns
     WHERE table_schema IN ({sql_schemas})
     GROUP BY table_name
     ORDER BY table_name"""


def sql_get_columns_postgres(schemas: list) -> str:
    """Generates an SQL command that gives the list of columns of every table
    in a specific schema from a postgres database"""
    sql_schemas = ', '.join(f"'{schema}'" for schema in schemas)

    return f"""
    SELECT table_name, STRING_AGG(CONCAT(column_name, ':', data_type, ':'), ';' ORDER BY column_name)
     FROM information_schema.columns
    WHERE table_schema IN ({sql_schemas})
    GROUP BY table_name
    ORDER BY table_name"""


def sql_get_columns_snowflake(schemas: list) -> str:
    """Generates an SQL command that gives the list of columns of every table
    in a specific schema from a snowflake database"""
    sql_schemas = ', '.join(f"'{schema.upper()}'" for schema in schemas)
    return f"""
    SELECT table_name, LISTAGG(CONCAT(column_name, ':', REPLACE(data_type, 'TEXT', 'VARCHAR'), ':'), ';')
                       WITHIN GROUP (ORDER BY column_name)
     FROM information_schema.columns
    WHERE table_schema IN ({sql_schemas})
    GROUP BY table_name
    ORDER BY table_name"""


def sql_get_columns_bigquery(schemas: list) -> str:
    """Generates an SQL command that gives the list of columns of every table
    in a specific schema from a snowflake database"""
    table_queries = ' UNION ALL '.join(
        f"""
            SELECT table_name, column_name, data_type
            FROM `{schema}`.INFORMATION_SCHEMA.COLUMNS"""
        for schema in schemas
    )

    return f"""
    SELECT table_name, STRING_AGG(CONCAT(column_name, ':', data_type, ':'), ';' ORDER BY column_name)
    FROM ({table_queries})
    GROUP BY table_name
    ORDER BY table_name"""


def sql_get_columns_redshift(schemas: list) -> str:
    """Generates an SQL command that gives the list of columns of every table
    in a specific schema from a Redshift database"""
    sql_schemas = ', '.join(f"'{schema}'" for schema in schemas)
    return f"""
    SELECT table_name, LISTAGG(CONCAT(column_name, '::'), ';') WITHIN GROUP (ORDER BY column_name)
    FROM (
        SELECT
            TRIM(c.relname) table_name, a.attname column_name
        FROM
            pg_type t,
            pg_attribute a,
            pg_class c,
            pg_namespace ns,
            (SELECT TOP 1 1 FROM ppw_e2e_helper.dual)
        WHERE
                t.oid=a.atttypid
          AND a.attrelid = c.oid
          AND c.relnamespace = ns.oid
          AND t.typname NOT IN ('oid','xid','tid','cid')
          AND a.attname not in ('deletexid', 'insertxid')
          AND c.reltype != 0
          AND ns.nspname IN ({sql_schemas})
        )
    GROUP BY table_name
    ORDER BY table_name
    """


def sql_dynamic_row_count_mysql(schemas: list) -> str:
    """Generates ans SQL statement that counts the number of rows in
    every table in a specific schema(s) in a mysql database"""
    sql_schemas = ', '.join(f"'{schema}'" for schema in schemas)

    return f"""
    WITH table_list AS (
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema IN ({sql_schemas})
           AND table_type = 'BASE TABLE')
    SELECT CONCAT(
           GROUP_CONCAT(CONCAT("SELECT '",LOWER(table_name),"' tbl, COUNT(*) row_count FROM `",table_name, "`")
                        SEPARATOR " UNION "),
           ' ORDER BY tbl')
      FROM table_list
    """


def sql_dynamic_row_count_postgres(schemas: list) -> str:
    """Generates ans SQL statement that counts the number of rows in
    every table in a specific schema(s) in a postgres database"""
    sql_schemas = ', '.join(f"'{schema}'" for schema in schemas)

    return f"""
    WITH table_list AS (
        SELECT table_schema, table_name
          FROM information_schema.tables
         WHERE table_schema IN ({sql_schemas})
           AND table_type = 'BASE TABLE')
    SELECT CONCAT(
           STRING_AGG(CONCAT('SELECT ''', LOWER(table_name), ''' tbl, COUNT(*) row_count FROM ',
                             table_schema, '."', table_name, '"'),
                      ' UNION '),
           ' ORDER BY tbl')
      FROM table_list
    """


def sql_dynamic_row_count_snowflake(schemas: list) -> str:
    """Generates an SQL statement that counts the number of rows in
    every table in a specific schema(s) in a Snowflake database"""
    sql_schemas = ', '.join(f"'{schema.upper()}'" for schema in schemas)

    return f"""
    WITH table_list AS (
        SELECT table_schema, table_name
          FROM information_schema.tables
         WHERE table_schema IN ({sql_schemas})
           AND table_type = 'BASE TABLE')
    SELECT CONCAT(
           LISTAGG(CONCAT('SELECT ''', LOWER(table_name), ''' tbl, COUNT(*) row_count FROM ',
                          table_schema, '."', table_name, '"'),
                      ' UNION '),
           ' ORDER BY tbl')
      FROM table_list
    """


def sql_dynamic_row_count_bigquery(schemas: list) -> str:
    """Generates an SQL statement that counts the number of rows in
    every table in a specific schema(s) in a Snowflake database"""
    table_queries = ' UNION DISTINCT '.join(
        f"""
            SELECT table_schema, table_name
            FROM `{schema}`.INFORMATION_SCHEMA.TABLES
            WHERE table_type = 'BASE TABLE'"""
        for schema in schemas
    )

    return f"""
    WITH table_list AS ({table_queries})
    SELECT CONCAT(
           STRING_AGG(CONCAT('SELECT \\'', LOWER(table_name), '\\' tbl, COUNT(*) row_count FROM ',
                          table_schema, '.`', table_name, '`'),
                      ' UNION DISTINCT '),
           ' ORDER BY tbl')
      FROM table_list
    """


def sql_dynamic_row_count_redshift(schemas: list) -> str:
    """Generates an SQL statement that counts the number of rows in
    every table in a specific schema(s) in a Redshift database"""
    sql_schemas = ', '.join(f"'{schema}'" for schema in schemas)

    return f"""
    WITH table_list AS (
        SELECT schemaname, tablename
          FROM pg_tables
              ,(SELECT top 1 1 FROM ppw_e2e_helper.dual)
         WHERE schemaname IN ({sql_schemas}))
    SELECT LISTAGG(
             'SELECT ''' || LOWER(tablename) || ''' tbl, COUNT(*) row_count FROM ' || schemaname || '."' || tablename || '"',
             ' UNION ') WITHIN GROUP ( ORDER BY tablename )
           || 'ORDER BY tbl'
      FROM table_list
    """  # noqa: E501


def get_mongodb_connection(
    host: str,
    port: Union[str, int],
    user: str,
    password: str,
    database: str,
    auth_database: str,
) -> Database:
    """
    Creates a mongoDB connection to the db to sync from
    Returns: Database instance with established connection

    """
    return pymongo.MongoClient(
        host=host,
        port=int(port),
        username=user,
        password=password,
        authSource=auth_database,
    )[database]
