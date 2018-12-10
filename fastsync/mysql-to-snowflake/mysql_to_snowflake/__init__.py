#!/usr/bin/env python3

import os

import mysql_to_snowflake.utils
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


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    mysql = MySql(args.mysql_config)
    snowflake = Snowflake(args.snowflake_config, args.transform_config)



    # Load tables one by one
    for table in args.tables:
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


def main():
    try:
        main_impl()
    except Exception as exc:
        utils.log("CRITICAL: {}".format(exc))
        raise exc

