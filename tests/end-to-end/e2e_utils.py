import os
import re
import shlex
import psycopg2
import subprocess
import snowflake.connector

from dotenv import load_dotenv


def load_env():
    """Load environment variables in priority order:
        1: Existing environment variables
        2: Docker compose .env environment variables"""
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../../dev-project/.env"))
    env = {
        'DB_TAP_POSTGRES_HOST': os.environ.get('DB_TAP_POSTGRES_HOST'),
        'DB_TAP_POSTGRES_PORT': os.environ.get('DB_TAP_POSTGRES_PORT'),
        'DB_TAP_POSTGRES_USER': os.environ.get('DB_TAP_POSTGRES_USER'),
        'DB_TAP_POSTGRES_PASSWORD': os.environ.get('DB_TAP_POSTGRES_PASSWORD'),
        'DB_TAP_POSTGRES_DB': os.environ.get('DB_TAP_POSTGRES_DB'),
        'DB_TAP_MYSQL_HOST': os.environ.get('DB_TAP_MYSQL_HOST'),
        'DB_TAP_MYSQL_PORT': os.environ.get('DB_TAP_MYSQL_PORT'),
        'DB_TAP_MYSQL_USER': os.environ.get('DB_TAP_MYSQL_USER'),
        'DB_TAP_MYSQL_PASSWORD': os.environ.get('DB_TAP_MYSQL_PASSWORD'),
        'DB_TAP_MYSQL_DB': os.environ.get('DB_TAP_MYSQL_DB'),
        'DB_TARGET_POSTGRES_HOST': os.environ.get('DB_TARGET_POSTGRES_HOST'),
        'DB_TARGET_POSTGRES_PORT': os.environ.get('DB_TARGET_POSTGRES_PORT'),
        'DB_TARGET_POSTGRES_USER': os.environ.get('DB_TARGET_POSTGRES_USER'),
        'DB_TARGET_POSTGRES_PASSWORD': os.environ.get('DB_TARGET_POSTGRES_PASSWORD'),
        'DB_TARGET_POSTGRES_DB': os.environ.get('DB_TARGET_POSTGRES_DB'),
        'TARGET_SNOWFLAKE_ACCOUNT': os.environ.get('TARGET_SNOWFLAKE_ACCOUNT'),
        'TARGET_SNOWFLAKE_DBNAME': os.environ.get('TARGET_SNOWFLAKE_DBNAME'),
        'TARGET_SNOWFLAKE_USER': os.environ.get('TARGET_SNOWFLAKE_USER'),
        'TARGET_SNOWFLAKE_PASSWORD': os.environ.get('TARGET_SNOWFLAKE_PASSWORD'),
        'TARGET_SNOWFLAKE_WAREHOUSE': os.environ.get('TARGET_SNOWFLAKE_WAREHOUSE'),
        'TARGET_SNOWFLAKE_AWS_ACCESS_KEY': os.environ.get('TARGET_SNOWFLAKE_AWS_ACCESS_KEY'),
        'TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY': os.environ.get('TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY'),
        'TARGET_SNOWFLAKE_S3_BUCKET': os.environ.get('TARGET_SNOWFLAKE_S3_BUCKET'),
        'TARGET_SNOWFLAKE_S3_KEY_PREFIX': os.environ.get('TARGET_SNOWFLAKE_S3_KEY_PREFIX'),
        'TARGET_SNOWFLAKE_STAGE': os.environ.get('TARGET_SNOWFLAKE_STAGE'),
        'TARGET_SNOWFLAKE_FILE_FORMAT': os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT'),
        'TAP_S3_CSV_SOURCE_AWS_KEY': os.environ.get('TAP_S3_CSV_SOURCE_AWS_KEY'),
        'TAP_S3_CSV_SOURCE_AWS_SECRET_ACCESS_KEY': os.environ.get('TAP_S3_CSV_SOURCE_AWS_SECRET_ACCESS_KEY'),
        'TAP_S3_CSV_SOURCE_BUCKET': os.environ.get('TAP_S3_CSV_SOURCE_BUCKET'),
    }

    return env

def find_run_tap_log_file(stdout, sync_engine=None):
    """Pipelinewise creates log file per running tap instances in a dynamically created directory:
        ~/.pipelinewise/<TARGET_ID>/<TAP_ID>/log

        Every log file matches the pattern:
        <TARGET_ID>-<TAP_ID>-<DATE>_<TIME>.<SYNC_ENGINE>.log.<STATUS>

        The generated full path is logged to STDOUT when tap starting"""
    if sync_engine:
        pattern = re.compile(r"Writing output into (.+\.{}\.log)".format(sync_engine))
    else:
        pattern = re.compile(r"Writing output into (.+\.log)")

    return pattern.search(stdout).group(1)

def run_query_postgres(env, query):
    with psycopg2.connect(host=env['DB_TARGET_POSTGRES_HOST'],
                          port=env['DB_TARGET_POSTGRES_PORT'],
                          user=env['DB_TARGET_POSTGRES_USER'],
                          password=env['DB_TARGET_POSTGRES_PASSWORD'],
                          database=env['DB_TARGET_POSTGRES_DB']) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute(query)

def run_query_snowflake(env, query):
    with snowflake.connector.connect(account=env['TARGET_SNOWFLAKE_ACCOUNT'],
                                     database=env['TARGET_SNOWFLAKE_DBNAME'],
                                     warehouse=env['TARGET_SNOWFLAKE_WAREHOUSE'],
                                     user=env['TARGET_SNOWFLAKE_USER'],
                                     password=env['TARGET_SNOWFLAKE_PASSWORD'],
                                     autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)

def run_command(command):
    """Run shell command and return returncode, stdout and stderr"""
    proc = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    x = proc.communicate()
    rc = proc.returncode
    stdout = x[0].decode('utf-8')
    stderr = x[1].decode('utf-8')

    return [rc, stdout, stderr]
