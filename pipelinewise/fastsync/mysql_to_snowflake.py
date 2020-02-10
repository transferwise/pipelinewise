#!/usr/bin/env python3

import os
import sys
import time

import multiprocessing

from datetime import datetime
from .commons import utils
from .commons.tap_mysql import FastSyncTapMySql
from .commons.target_snowflake import FastSyncTargetSnowflake

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
        'user',
        'password',
        'warehouse',
        'aws_access_key_id',
        'aws_secret_access_key',
        's3_bucket',
        'stage',
        'file_format'
    ]
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(mysql_type, mysql_column_type):
    """Data type mapping from MySQL to Snowflake"""
    return {
        'char': 'VARCHAR',
        'varchar': 'VARCHAR',
        'binary': 'VARCHAR',
        'varbinary': 'BINARY',
        'blob': 'BINARY',
        'tinyblob': 'VARCHAR',
        'mediumblob': 'VARCHAR',
        'longblob': 'VARCHAR',
        'geometry': 'VARCHAR',
        'text': 'VARCHAR',
        'tinytext': 'VARCHAR',
        'mediumtext': 'VARCHAR',
        'longtext': 'VARCHAR',
        'enum': 'VARCHAR',
        'int': 'NUMBER',
        'tinyint': 'BOOLEAN' if mysql_column_type == 'tinyint(1)' else 'NUMBER',
        'smallint': 'NUMBER',
        'bigint': 'NUMBER',
        'bit': 'BOOLEAN',
        'decimal': 'FLOAT',
        'double': 'FLOAT',
        'float': 'FLOAT',
        'bool': 'BOOLEAN',
        'boolean': 'BOOLEAN',
        'date': 'TIMESTAMP_NTZ',
        'datetime': 'TIMESTAMP_NTZ',
        'timestamp': 'TIMESTAMP_NTZ',
    }.get(mysql_type, 'VARCHAR')


# pylint: disable=inconsistent-return-statements
def sync_table(table):
    """Sync one table"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)

    try:
        filename = 'pipelinewise_fastsync_{}_{}.csv.gz'.format(table, time.strftime('%Y%m%d-%H%M%S'))
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection and get binlog file position
        mysql.open_connections()

        # Get bookmark - Binlog position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(table, args.properties, mysql)

        # Exporting table data, get table definitions and close connection to avoid timeouts
        mysql.copy_table(table, filepath)
        snowflake_types = mysql.map_column_types_to_target(table)
        snowflake_columns = snowflake_types.get('columns', [])
        primary_key = snowflake_types.get('primary_key')
        mysql.close_connections()

        # Uploading to S3
        s3_key = snowflake.upload_to_s3(filepath, table, tmp_dir=args.temp_dir)
        os.remove(filepath)

        # Creating temp table in Snowflake
        snowflake.create_schema(target_schema)
        snowflake.create_table(target_schema, table, snowflake_columns, primary_key, is_temporary=True)

        # Load into Snowflake table
        snowflake.copy_to_table(s3_key, target_schema, table, is_temporary=True)

        # Obfuscate columns
        snowflake.obfuscate_columns(target_schema, table)

        # Create target table and swap with the temp table in Snowflake
        snowflake.create_table(target_schema, table, snowflake_columns, primary_key)
        snowflake.swap_tables(target_schema, table)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        LOCK.acquire()
        try:
            utils.save_state_file(args.state, table, bookmark)
        finally:
            LOCK.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table)
        utils.grant_privilege(target_schema, grantees, snowflake.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, snowflake.grant_select_on_schema)

    except Exception as exc:
        utils.log('CRITICAL: {}'.format(exc))
        return '{}: {}'.format(table, exc)


def main_impl():
    """Main sync logic"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    cpu_cores = utils.get_cpu_cores()
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
        """.format(args.tables,
                   len(args.tables),
                   cpu_cores))

    # Start loading tables in parallel in spawning processes by
    # utilising all available CPU cores
    with multiprocessing.Pool(cpu_cores) as proc:
        table_sync_excs = list(filter(None, proc.map(sync_table, args.tables)))

    # Refresh information_schema columns cache
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    snowflake.cache_information_schema_columns(args.tables)

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
        """.format(len(args.tables),
                   len(args.tables) - len(table_sync_excs),
                   str(table_sync_excs),
                   cpu_cores,
                   end_time - start_time))
    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        utils.log('CRITICAL: {}'.format(exc))
        raise exc
