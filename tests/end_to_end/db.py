import psycopg2
import psycopg2.extras
import pymysql
import snowflake.connector


def _to_dict(columns, results):
    """
    Convertsthe resultset from postgres to dictionary
    interates the data and maps the columns to the values in result set and converts to dictionary
    :param columns: List - column names return when query is executed
    :param results: List / Tupple - result set from when query is executed
    :return: list of dictionary- mapped with table column name and to its values
    """

    all_results = []
    columns = [col.name for col in columns]
    if isinstance(results, list):
        for value in results:
            all_results.append(dict(zip(columns, value)))
    elif isinstance(results, tuple):
        all_results.append(dict(zip(columns, results)))

    return all_results

# pylint: disable=too-many-arguments
def run_query_postgres(query, host, port, user, password, database):
    """Run and SQL query in a postgres database"""
    result_rows = []
    with psycopg2.connect(host=host,
                          port=port,
                          user=user,
                          password=password,
                          database=database,
                          cursor_factory=psycopg2.extras.DictCursor) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute(query)

            if cur.rowcount > 0:
                result_rows = _to_dict(cur.description, cur.fetchall())

    return result_rows

def run_query_mysql(query, host, port, user, password, database):
    """Run and SQL query in a mysql database"""
    result_rows = []
    with pymysql.connect(host=host,
                         port=port,
                         user=user,
                         password=password,
                         database=database,
                         cursorclass=pymysql.cursors.DictCursor) as cur:
        cur.execute(query)

        if cur.rowcount > 0:
            result_rows = cur.fetchall()
    return result_rows


def run_query_snowflake(query, account, database, warehouse, user, password):
    """Run and SQL query in a snowflake database"""
    result_rows = []
    with snowflake.connector.connect(account=account,
                                     database=database,
                                     warehouse=warehouse,
                                     user=user,
                                     password=password,
                                     autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.rowcount > 0:
                result_rows = cur.fetchall()
    return result_rows

def run_query_redshift(query, host, port, user, password, database):
    """Redshift is compatible with postgres"""
    return run_query_postgres(query, host, port, user, password, database)
