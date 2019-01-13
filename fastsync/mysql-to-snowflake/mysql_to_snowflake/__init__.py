#!/usr/bin/env python3

import os

import mysql_to_snowflake.utils
import multiprocessing

from datetime import datetime, timedelta
from mysql_to_snowflake.mysql import MySql
from mysql_to_snowflake.snowflake import Snowflake


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
        postfix = "" if schema_name_postfix is None else schema_name_postfix
        schema_name = s[0] + postfix
        table_name = s[1]

    return {
        'schema_name': schema_name,
        'table_name': table_name
    }


def get_target_schema(target_config, table):
    # Target schema name can be defined in multiple ways:
    #
    #   1: 'schema' key : Target schema name defined explicitly
    #   2: 'dynamic_schema_name' key: Target schema name derived from the incoming stream id:
    #                                 i.e.: <schema_nama>-<table_name>
    if 'schema' in target_config and target_config['schema'].strip():
        return target_config['schema']
    elif 'dynamic_schema_name' in target_config and target_config['dynamic_schema_name']:
        postfix = target_config['dynamic_schema_name_postfix'] if 'dynamic_schema_name_postfix' in target_config else None
        table_dict = table_to_dict(table, postfix)

        if table_dict['schema_name'] is None:
            raise Exception("Cannot detect target schema name of table using '{}' format. Use it with schema names.".format(table))

        return table_dict['schema_name']
    else:
        raise Exception("Target schema name not defined in config. Neither 'schema' (string) nor 'dynamic_schema_name' (boolean) keys set in config.")


def sync_table(table):
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    mysql = MySql(args.tap)
    snowflake = Snowflake(args.target, args.transform)

    try:
        filename = '{}.csv.gz'.format(table)
        filepath = os.path.join(args.export_dir, filename)
        target_schema = get_target_schema(args.target, table)

        # Open connection and get binlog file position
        mysql.open_connection()
        binlog_pos = mysql.fetch_current_log_file_and_pos()

        # Exporting table data and close connection to avoid timeouts for huge tables
        mysql.copy_table(table, filepath)
        mysql.close_connection()

        # Uploading to S3
        s3_key = snowflake.upload_to_s3(filepath, table)
        os.remove(filepath)

        # Creating temp table in Snowflake
        snowflake.create_schema(target_schema)
        mysql.open_connection()
        snowflake.query(mysql.snowflake_ddl(table, target_schema, True))

        # Load into Snowflake table
        snowflake.copy_to_table(s3_key, target_schema, table, True)

        # Obfuscate columns
        snowflake.obfuscate_columns(target_schema, table)

        # Create target table in snowflake and swap with temp table
        snowflake.query(mysql.snowflake_ddl(table, target_schema, False))
        snowflake.swap_tables(target_schema, table)

        # Save binlog to singer state file
        utils.save_state_file(args.state, binlog_pos, table)
        mysql.close_connection()

        # Table loaded, grant select on all tables in target schema
        grant_select_to = args.target['grant_select_to'] if 'grant_select_to' in args.target else []
        for grantee in grant_select_to:
            snowflake.grant_usage_on_schema(target_schema, grantee)
            snowflake.grant_select_on_schema(target_schema, grantee)

    except Exception as exc:
        return exc


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
            Total tables selected to sync  : {}
            CPU cores                      : {}
        -------------------------------------------------------
        """.format(
            len(args.tables),
            cpu_cores
        ))

    # Start loading tables in parallel in spawning processes by
    # utilising all available CPU cores
    with multiprocessing.Pool(cpu_cores) as p:
        table_sync_excs = list(filter(None, p.map(sync_table, args.tables)))

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


def main():
    try:
        main_impl()
    except Exception as exc:
        utils.log("CRITICAL: {}".format(exc))
        raise exc

