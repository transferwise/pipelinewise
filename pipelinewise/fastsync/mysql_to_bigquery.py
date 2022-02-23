#!/usr/bin/env python3
import logging
import os
import sys
import glob
import time
from functools import partial
from argparse import Namespace
import multiprocessing
from typing import Union

from datetime import datetime
from .commons import utils
from .commons.tap_mysql import FastSyncTapMySql
from .commons.target_bigquery import FastSyncTargetBigquery

MAX_NUM = '99999999999999999999999999999.999999999'

LOGGER = logging.getLogger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': ['host', 'port', 'user', 'password'],
    'target': [
        'project_id',
    ],
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(mysql_type, mysql_column_type):
    """Data type mapping from MySQL to Bigquery"""
    return {
        'char': 'STRING',
        'varchar': 'STRING',
        'binary': 'STRING',
        'varbinary': 'STRING',
        'blob': 'STRING',
        'tinyblob': 'STRING',
        'mediumblob': 'STRING',
        'longblob': 'STRING',
        'geometry': 'STRING',
        'text': 'STRING',
        'tinytext': 'STRING',
        'mediumtext': 'STRING',
        'longtext': 'STRING',
        'enum': 'STRING',
        'int': 'INT64',
        'tinyint': 'BOOL' if mysql_column_type == 'tinyint(1)' else 'INT64',
        'smallint': 'INT64',
        'mediumint': 'INT64',
        'bigint': 'INT64',
        'bit': 'BOOL',
        'decimal': 'NUMERIC',
        'double': 'NUMERIC',
        'float': 'NUMERIC',
        'bool': 'BOOL',
        'boolean': 'BOOL',
        'date': 'TIMESTAMP',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'time': 'TIME',
    }.get(mysql_type, 'STRING')


# pylint: disable=too-many-locals
def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type, target_quote='`')
    bigquery = FastSyncTargetBigquery(args.target, args.transform)

    try:
        filename = 'pipelinewise_fastsync_{}_{}.csv'.format(
            table, time.strftime('%Y%m%d-%H%M%S')
        )
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection and get binlog file position
        mysql.open_connections()

        # Get bookmark - Binlog position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(table, args.properties, mysql)

        # Exporting table data, get table definitions and close connection to avoid timeouts
        mysql.copy_table(
            table, filepath, compress=False, max_num=MAX_NUM, date_type='datetime'
        )
        file_parts = glob.glob(f'{filepath}*')
        size_bytes = sum([os.path.getsize(file_part) for file_part in file_parts])
        bigquery_types = mysql.map_column_types_to_target(table)
        bigquery_columns = bigquery_types.get('columns', [])
        primary_key = bigquery_types.get('primary_key', [])
        mysql.close_connections()

        # Creating temp table in Bigquery
        bigquery.create_schema(target_schema)
        bigquery.create_table(target_schema, table, bigquery_columns, primary_key, is_temporary=True)

        # Load into Bigquery table
        for num, file_part in enumerate(file_parts):
            write_truncate = num == 0
            bigquery.copy_to_table(
                filepath,
                target_schema,
                table,
                size_bytes,
                is_temporary=True,
                write_truncate=write_truncate,
            )
            os.remove(file_part)

        # Obfuscate columns
        bigquery.obfuscate_columns(target_schema, table)

        # Create target table and swap with the temp table in Bigquery
        bigquery.create_table(target_schema, table, bigquery_columns, primary_key)
        bigquery.swap_tables(target_schema, table)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        LOCK.acquire()
        try:
            utils.save_state_file(args.state, table, bookmark)
        finally:
            LOCK.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table)
        utils.grant_privilege(target_schema, grantees, bigquery.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, bigquery.grant_select_on_schema)

        return True

    except Exception as exc:
        LOGGER.exception(exc)
        return '{}: {}'.format(table, exc)


def main_impl():
    """Main sync logic"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    pool_size = utils.get_pool_size(args.tap)
    start_time = datetime.now()
    table_sync_excs = []

    # Log start info
    LOGGER.info(
        """
        -------------------------------------------------------
        STARTING SYNC
        -------------------------------------------------------
            Tables selected to sync        : %s
            Total tables selected to sync  : %s
            Pool size                      : %s
        -------------------------------------------------------
        """,
        args.tables,
        len(args.tables),
        pool_size,
    )

    # Start loading tables in parallel in spawning processes
    with multiprocessing.Pool(pool_size) as proc:
        table_sync_excs = list(
            filter(
                lambda x: not isinstance(x, bool),
                proc.map(partial(sync_table, args=args), args.tables),
            )
        )

    # Log summary
    end_time = datetime.now()
    LOGGER.info(
        """
        -------------------------------------------------------
        SYNC FINISHED - SUMMARY
        -------------------------------------------------------
            Total tables selected to sync  : %s
            Tables loaded successfully     : %s
            Exceptions during table sync   : %s

            Pool size                      : %s
            Runtime                        : %s
        -------------------------------------------------------
        """,
        len(args.tables),
        len(args.tables) - len(table_sync_excs),
        str(table_sync_excs),
        pool_size,
        end_time - start_time,
    )

    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
