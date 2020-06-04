#!/usr/bin/env python3
import logging
import os
import sys
import time
from functools import partial
from argparse import Namespace
import multiprocessing
from typing import Union

from datetime import datetime
from .commons import utils
from .commons.tap_mysql import FastSyncTapMySql
from .commons.target_redshift import FastSyncTargetRedshift

LOGGER = logging.getLogger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': [
        'host',
        'port',
        'user',
        'password'
    ],
    'target': [
        'host',
        'port',
        'user',
        'password',
        'dbname',
        's3_bucket'
    ]
}

DEFAULT_VARCHAR_LENGTH = 10000
SHORT_VARCHAR_LENGTH = 256
LONG_VARCHAR_LENGTH = 65535

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(mysql_type, mysql_column_type):
    """Data type mapping from MySQL to Redshift"""
    return {
        'char':'CHARACTER VARYING({})'.format(SHORT_VARCHAR_LENGTH),
        'varchar':'CHARACTER VARYING({})'.format(SHORT_VARCHAR_LENGTH),
        'binary':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'varbinary':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'blob':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'tinyblob':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'mediumblob':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'longblob':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'geometry':'CHARACTER VARYING({})'.format(DEFAULT_VARCHAR_LENGTH),
        'text':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'tinytext':'CHARACTER VARYING({})'.format(SHORT_VARCHAR_LENGTH),
        'mediumtext':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'longtext':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'enum':'CHARACTER VARYING({})'.format(DEFAULT_VARCHAR_LENGTH),
        'int':'NUMERIC NULL',
        'integer':'NUMERIC NULL',
        'tinyint':'BOOLEAN' if mysql_column_type == 'tinyint(1)' else 'NUMERIC NULL',
        'smallint':'NUMERIC NULL',
        'mediumint':'NUMERIC NULL',
        'bigint':'NUMERIC NULL',
        'bit':'BOOLEAN',
        'dec':'FLOAT',
        'decimal':'FLOAT',
        'double':'FLOAT',
        'float':'FLOAT',
        'bool':'BOOLEAN',
        'boolean':'BOOLEAN',
        'date':'TIMESTAMP WITHOUT TIME ZONE',
        'datetime':'TIMESTAMP WITHOUT TIME ZONE',
        'timestamp':'TIMESTAMP WITHOUT TIME ZONE',
        'year':'NUMERIC NULL',
        'json': 'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH)
    }.get(
        mysql_type,
        'CHARACTER VARYING({})'.format(DEFAULT_VARCHAR_LENGTH),
    )


def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)
    redshift = FastSyncTargetRedshift(args.target, args.transform)

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
        size_bytes = os.path.getsize(filepath)
        redshift_types = mysql.map_column_types_to_target(table)
        redshift_columns = redshift_types.get('columns', [])
        primary_key = redshift_types.get('primary_key')
        mysql.close_connections()

        # Uploading to S3
        s3_key = redshift.upload_to_s3(filepath, table)
        os.remove(filepath)

        # Creating temp table in Redshift
        redshift.drop_table(target_schema, table, is_temporary=True)
        redshift.create_table(target_schema, table, redshift_columns, primary_key, is_temporary=True)

        # Load into Redshift table
        redshift.copy_to_table(s3_key, target_schema, table, size_bytes, is_temporary=True)

        # Obfuscate columns
        redshift.obfuscate_columns(target_schema, table)

        # Create target table and swap with the temp table in Redshift
        redshift.swap_tables(target_schema, table)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        LOCK.acquire()
        try:
            utils.save_state_file(args.state, table, bookmark)
        finally:
            LOCK.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table)
        utils.grant_privilege(target_schema, grantees, redshift.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, redshift.grant_select_on_schema)

        return True

    except Exception as exc:
        LOGGER.exception(exc)
        return '{}: {}'.format(table, exc)


def main_impl():
    """Main sync logic"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    cpu_cores = utils.get_cpu_cores()
    start_time = datetime.now()
    table_sync_excs = []

    # Log start info
    LOGGER.info("""
        -------------------------------------------------------
        STARTING SYNC
        -------------------------------------------------------
            Tables selected to sync        : %s
            Total tables selected to sync  : %s
            CPU cores                      : %s
        -------------------------------------------------------
        """, args.tables, len(args.tables), cpu_cores)

    # Create target schemas sequentially, Redshift doesn't like it running in parallel
    redshift = FastSyncTargetRedshift(args.target, args.transform)
    redshift.create_schemas(args.tables)

    # Start loading tables in parallel in spawning processes by
    # utilising all available CPU cores
    with multiprocessing.Pool(cpu_cores) as proc:
        table_sync_excs = list(
            filter(lambda x: not isinstance(x, bool), proc.map(partial(sync_table, args=args), args.tables)))

    # Log summary
    end_time = datetime.now()
    LOGGER.info("""
        -------------------------------------------------------
        SYNC FINISHED - SUMMARY
        -------------------------------------------------------
            Total tables selected to sync  : %s
            Tables loaded successfully     : %s
            Exceptions during table sync   : %s

            CPU cores                      : %s
            Runtime                        : %s
        -------------------------------------------------------
        """, len(args.tables), len(args.tables) - len(table_sync_excs), str(table_sync_excs),
                cpu_cores, end_time - start_time)

    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
