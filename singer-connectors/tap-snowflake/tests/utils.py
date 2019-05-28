import os
import singer
import snowflake.connector

import tap_snowflake
import tap_snowflake.sync_strategies.common as common
from tap_snowflake.connection import SnowflakeConnection

SCHEMA_NAME='tap_snowflake_test'

def get_db_config():
    config = {}
    config['account'] = os.environ.get('TAP_SNOWFLAKE_ACCOUNT')
    config['dbname'] = os.environ.get('TAP_SNOWFLAKE_DBNAME')
    config['user'] = os.environ.get('TAP_SNOWFLAKE_USER')
    config['password'] = os.environ.get('TAP_SNOWFLAKE_PASSWORD')
    config['warehouse'] = os.environ.get('TAP_SNOWFLAKE_WAREHOUSE')

    return config


def get_tap_config():
    config = {}
    config['filter_dbs'] = os.environ.get('TAP_SNOWFLAKE_DBNAME')
    config['filter_schemas'] = SCHEMA_NAME

    return config


def get_test_connection():
    db_config = get_db_config()
    snowflake_conn = SnowflakeConnection(db_config)

    with snowflake_conn.open_connection() as open_conn:
        with open_conn.cursor() as cur:          
            try:
                cur.execute('DROP SCHEMA IF EXISTS {}'.format(SCHEMA_NAME))
            except:
                pass
            cur.execute('CREATE SCHEMA {}'.format(SCHEMA_NAME))

    return snowflake_conn


def discover_catalog(connection):
    tap_config = get_tap_config()
    catalog = tap_snowflake.discover_catalog(connection, tap_config)
    streams = []

    for stream in catalog.streams:
        streams.append(stream)

    catalog.streams = streams

    return catalog


def set_replication_method_and_key(stream, r_method, r_key):
    new_md = singer.metadata.to_map(stream.metadata)
    old_md = new_md.get(())
    if r_method:
        old_md.update({'replication-method': r_method})

    if r_key:
        old_md.update({'replication-key': r_key})

    stream.metadata = singer.metadata.to_list(new_md)
    return stream
