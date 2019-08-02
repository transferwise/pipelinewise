#!/usr/bin/env python3

import os
import sys
import time

import multiprocessing

from datetime import datetime, timedelta
from .mysql import MySql
from .snowflake import Snowflake
from . import utils


REQUIRED_CONFIG_KEYS = {
    'tap': [
        'host',
        'port',
        'user',
        'password'
    ],
    'target': [
        'account',
        'dbname',
        'warehouse',
        'user',
        'password',
        's3_bucket',
        'aws_access_key_id',
        'aws_secret_access_key'
    ]
}

lock = multiprocessing.Lock()

def get_cpu_cores():
    try:
        return multiprocessing.cpu_count()
    # Defaults to 1 core in case of any exception
    except Exception as exc:
        return 1


def table_to_dict(table, schema_name_postfix = None):
    schema_name = None
    table_name = table

    # Schema and table name can be derived if it's in <schema_nama>.<table_name> format
    s = table.split('.')
    if len(s) > 1:
        schema_name = s[0]
        table_name = s[1]

    return {
        'schema_name': schema_name,
        'table_name': table_name
    }


def get_target_schema(target_config, table):
    """
    Target schema name can be defined in multiple ways:

    1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
                                      not specified explicitly for a given stream in
                                      the `schema_mapping` object
    2: 'schema_mapping' key         : Target schema defined explicitly for a given stream.
                                      Example config.json:
                                            "schema_mapping": {
                                                "my_tap_stream_id": {
                                                    "target_schema": "my_snowflake_schema",
                                                }
                                            }
    """
    target_schema = None
    config_default_target_schema = target_config.get('default_target_schema', '').strip()
    config_schema_mapping = target_config.get('schema_mapping', {})

    table_dict = table_to_dict(table)
    table_schema = table_dict['schema_name']
    if config_schema_mapping and table_schema in config_schema_mapping:
        target_schema = config_schema_mapping[table_schema].get('target_schema')
    elif config_default_target_schema:
        target_schema = config_default_target_schema

    if not target_schema:
        raise Exception("Target schema name not defined in config. Neither 'default_target_schema' (string) nor 'schema_mapping' (object) defines target schema for {} stream.".format(table))

    return target_schema

def get_target_schemas(target_config, tables):
    target_schemas = []
    for t in tables:
        target_schemas.append(get_target_schema(target_config, t))

    return list(dict.fromkeys(target_schemas))


def cache_information_schema_columns(target_config, tables):
    snowflake = Snowflake(target_config)
    pipelinewise_schema = utils.tablename_to_dict(target_config['stage'])['schema']
    schemas_to_cache = get_target_schemas(target_config, tables)

    # Create an empty cache table if not exists
    snowflake.query("""
        CREATE TABLE IF NOT EXISTS {}.columns (table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, data_type VARCHAR)
    """.format(pipelinewise_schema))

    # Cache table columns from information_schema
    for schema_name in schemas_to_cache:
        utils.log("rebuilding information_schema cache for schema: {}".format(schemas_to_cache))

        # Delete existing data about the current schema
        snowflake.query("""
            DELETE FROM {}.columns
            WHERE LOWER(table_schema) = '{}'
        """.format(pipelinewise_schema, schema_name.lower()))

        # Insert the latest data from information_schema into the cache table
        snowflake.query("""
            INSERT INTO {}.columns
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE LOWER(table_schema) = '{}'
        """.format(pipelinewise_schema, schema_name.lower()))


def get_grantees(target_config, table):
    """
    Grantees can be defined in multiple ways:

    1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on every table to a given role
                                                        for every incoming stream if not specified explicitly
                                                        in the `schema_mapping` object
    2: 'target_schema_select_permissions' key          : Roles to grant USAGE and SELECT privileges defined explicitly
                                                        for a given stream.
                                                        Example config.json:
                                                            "schema_mapping": {
                                                                "my_tap_stream_id": {
                                                                    "target_schema_select_permissions": [ "role_with_select_privs" ]
                                                                }
                                                            }
    """
    grantees = []
    config_default_target_schema_select_permissions = target_config.get('default_target_schema_select_permissions', [])
    config_schema_mapping = target_config.get('schema_mapping', {})

    table_dict = table_to_dict(table)
    table_schema = table_dict['schema_name']
    if config_schema_mapping and table_schema in config_schema_mapping:
        grantees = config_schema_mapping[table_schema].get('target_schema_select_permissions', [])
    elif config_default_target_schema_select_permissions:
        grantees = config_default_target_schema_select_permissions

    return grantees


def sync_table(table):
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    mysql = MySql(args.tap)
    snowflake = Snowflake(args.target, args.transform)

    try:
        filename = "pipelinewise_fastsync_{}_{}.csv.gz".format(table, time.strftime("%Y%m%d-%H%M%S"))
        filepath = os.path.join(args.export_dir, filename)
        target_schema = get_target_schema(args.target, table)

        # Open connection and get binlog file position
        mysql.open_connection()

        # Get bookmark - Binlog position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(table, args.properties, mysql)

        # Exporting table data, get table definitions and close connection to avoid timeouts
        mysql.copy_table(table, filepath)
        create_temp_table_sql = mysql.snowflake_ddl(table, target_schema, True)
        create_target_table_sql = mysql.snowflake_ddl(table, target_schema, False)
        mysql.close_connection()

        # Uploading to S3
        s3_key = snowflake.upload_to_s3(filepath, table)
        os.remove(filepath)

        # Creating temp table in Snowflake
        snowflake.create_schema(target_schema)
        snowflake.query(create_temp_table_sql)

        # Load into Snowflake table
        snowflake.copy_to_table(s3_key, target_schema, table, True)

        # Obfuscate columns
        snowflake.obfuscate_columns(target_schema, table)

        # Create target table and swap with the temp table in Snowflake
        snowflake.query(create_target_table_sql)
        snowflake.swap_tables(target_schema, table)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        lock.acquire()
        try:
            utils.save_state_file(args.state, table, bookmark)
        finally:
            lock.release()

        # Table loaded, grant select on all tables in target schema
        for grantee in get_grantees(args.target, table):
            snowflake.grant_usage_on_schema(target_schema, grantee)
            snowflake.grant_select_on_schema(target_schema, grantee)

    except Exception as exc:
        return "{}: {}".format(table, exc)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    cpu_cores = get_cpu_cores()
    start_time = datetime.now()
    table_sync_excs = []

    # Log start info
    utils.log("""
        -------------------------------------------------------
        STARTING SYNC
        -------------------------------------------------------
            Tables selected to sync        : {}
            Total tables selected to sync  : {}
            CPU cores                      : {}
        -------------------------------------------------------
        """.format(
            args.tables,
            len(args.tables),
            cpu_cores
        ))

    # Start loading tables in parallel in spawning processes by
    # utilising all available CPU cores
    with multiprocessing.Pool(cpu_cores) as p:
        table_sync_excs = list(filter(None, p.map(sync_table, args.tables)))

    # Refresh information_schema columns cache
    cache_information_schema_columns(args.target, args.tables)

    # Log summary
    end_time = datetime.now()
    utils.log("""
        -------------------------------------------------------
        SYNC FINISHED - SUMMARY
        -------------------------------------------------------
            Total tables selected to sync  : {}
            Tables loaded successfully     : {}
            Exceptions during table sync   : {}

            CPU cores                      : {}
            Runtime                        : {}
        -------------------------------------------------------
        """.format(
            len(args.tables),
            len(args.tables) - len(table_sync_excs),
            str(table_sync_excs),
            cpu_cores,
            end_time  - start_time
        ))
    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    try:
        main_impl()
    except Exception as exc:
        utils.log("CRITICAL: {}".format(exc))
        raise exc

