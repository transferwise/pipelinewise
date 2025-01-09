import os
from sys import version_info as py_version

import singer

import tap_snowflake
from tap_snowflake.connection import SnowflakeConnection

SCHEMA_NAME = f'TAP_SNOWFLAKE_TEST_PY{py_version.major}_{py_version.minor}'


def get_db_config():
    return {
        'account': os.environ.get('TAP_SNOWFLAKE_ACCOUNT'),
        'dbname': os.environ.get('TAP_SNOWFLAKE_DBNAME'),
        'user': os.environ.get('TAP_SNOWFLAKE_USER'),
        'password': os.environ.get('TAP_SNOWFLAKE_PASSWORD', None),
        'private_key_path': os.environ.get('TAP_SNOWFLAKE_PRIVATE_KEY_PATH', None),
        'private_key_passphrase': os.environ.get('TAP_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', None),
        'warehouse': os.environ.get('TAP_SNOWFLAKE_WAREHOUSE'),
        'tables': 'FAKE_TABLES'
    }


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


def discover_catalog(snowflake_conn, config):
    catalog = tap_snowflake.discover_catalog(snowflake_conn, config)
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
