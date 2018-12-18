#!/usr/bin/env python3

import os

import mysql_to_snowflake.utils
import multiprocessing

from datetime import datetime, timedelta
from mysql_to_snowflake.mysql import MySql
from mysql_to_snowflake.snowflake import Snowflake


REQUIRED_CONFIG_KEYS = {
    'mysql': [
        'host',
        'port',
        'user',
        'password'
    ],
    'snowflake': [
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


def sync_table(table):
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    mysql = MySql(args.mysql_config)
    snowflake = Snowflake(args.snowflake_config, args.transform_config)

    try:
        filename = '{}.csv.gz'.format(table)
        filepath = os.path.join(args.export_dir, filename)

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
        snowflake.create_schema(args.target_schema)
        mysql.open_connection()
        snowflake.query(mysql.snowflake_ddl(table, args.target_schema, True))
        snowflake.grant_select_on_table(args.target_schema, table, args.grant_select_to, True)

        # Load into Snowflake table
        snowflake.copy_to_table(s3_key, args.target_schema, table, True)

        # Obfuscate columns
        snowflake.obfuscate_columns(args.target_schema, table)

        # Create target table in snowflake and swap with temp table
        snowflake.query(mysql.snowflake_ddl(table, args.target_schema, False))
        snowflake.swap_tables(args.target_schema, table)

        # Save binlog to singer state file
        utils.save_state_file(args.state, binlog_pos, table)
        mysql.close_connection()

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

    # Every table loaded, grant select on all tables in target schema
    # Catch exception and (i.e. Role does not exist) and add into the exceptions array
    try:
        if args.grant_select_to:
            snowflake = Snowflake(args.snowflake_config, args.transform_config)
            snowflake.grant_usage_on_schema(args.target_schema, args.grant_select_to)
            snowflake.grant_select_on_schema(args.target_schema, args.grant_select_to)
    except Exception as exc:
        table_sync_excs.append(exc)

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

