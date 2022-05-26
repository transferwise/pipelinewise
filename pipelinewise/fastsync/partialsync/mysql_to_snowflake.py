#!/usr/bin/env python3
import sys
import os
import glob
import re

from argparse import Namespace
import multiprocessing
from typing import Union
from datetime import datetime

from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake

from pipelinewise.logger import Logger
from pipelinewise.fastsync.commons import utils
from pipelinewise.fastsync.mysql_to_snowflake import REQUIRED_CONFIG_KEYS, tap_type_to_target_type

LOGGER = Logger().get_logger(__name__)
LOCK = multiprocessing.Lock()


def partial_sync_table(args: Namespace) -> Union[bool, str]:
    """Partial sync table for MySQL to Snowflake"""
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    tap_id = args.target.get('tap_id')

    try:
        mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)

        # Get bookmark - Binlog position or Incremental Key value
        mysql.open_connections()
        bookmark = utils.get_bookmark_for_table(args.table, args.properties, mysql)
        mysql.close_connections()

        file_parts = _export_source_table_data(args, tap_id)
        size_bytes = sum([os.path.getsize(file_part) for file_part in file_parts])
        s3_keys, s3_key_pattern = _upload_to_s3(snowflake, file_parts, args.temp_dir)
        _load_into_snowflake(snowflake, args, s3_keys, s3_key_pattern, size_bytes)
        _update_state_file(args, bookmark)

        return True
    except Exception as exc:
        LOGGER.exception(exc)
        return f'{args.table}: {exc}'


def _update_state_file(args, bookmark):
    # Save bookmark to singer state file
    # Lock to ensure that only one process writes the same state file at a time
    if not args.end_value:
        LOCK.acquire()
        try:
            utils.save_state_file(args.state, args.table, bookmark)
        finally:
            LOCK.release()


def _upload_to_s3(snowflake, file_parts, temp_dir):
    s3_keys = []
    for file_part in file_parts:
        s3_keys.append(snowflake.upload_to_s3(file_part, tmp_dir=temp_dir))
        os.remove(file_part)

    # Create a pattern that match all file parts by removing multipart suffix
    s3_key_pattern = (
        re.sub(r'\.part\d*$', '', s3_keys[0])
        if len(s3_keys) > 0
        else 'NO_FILES_TO_LOAD'
    )
    return s3_keys, s3_key_pattern


def _load_into_snowflake(snowflake, args, s3_keys, s3_key_pattern, size_bytes):

    # delete partial data from the table
    target_schema = utils.get_target_schema(args.target, args.table)
    table_dict = utils.tablename_to_dict(args.table)
    target_table = table_dict.get('table_name')
    where_clause = f'WHERE {args.column} >= {args.start_value}'
    if args.end_value:
        where_clause += f' AND {args.column} <= {args.end_value}'

    snowflake.query(f'DELETE FROM {target_schema}.{target_table} {where_clause}')
    # copy partial data into the table
    archive_load_files = args.target.get('archive_load_files', False)
    tap_id = args.target.get('tap_id')

    # Load into Snowflake table
    snowflake.copy_to_table(
        s3_key_pattern, target_schema, args.table, size_bytes, is_temporary=False
    )

    for s3_key in s3_keys:
        if archive_load_files:
            # Copy load file to archive
            snowflake.copy_to_archive(s3_key, tap_id, args.table)

        # Delete all file parts from s3
        snowflake.s3.delete_object(Bucket=args.target.get('s3_bucket'), Key=s3_key)


def _export_source_table_data(args, tap_id):
    filename = utils.gen_export_filename(tap_id=tap_id, table=args.table, sync_type='partialsync')
    filepath = os.path.join(args.temp_dir, filename)

    mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)
    # Open connection and get binlog file position
    mysql.open_connections()



    # Exporting table data, get table definitions and close connection to avoid timeouts
    where_clause_setting = {
        'column': args.column,
        'start_value': args.start_value,
        'end_value': args.end_value
    }
    mysql.copy_table(
        args.table,
        filepath,
        split_large_files=args.target.get('split_large_files'),
        split_file_chunk_size_mb=args.target.get('split_file_chunk_size_mb'),
        split_file_max_chunks=args.target.get('split_file_max_chunks'),
        where_clause_setting=where_clause_setting
    )
    file_parts = glob.glob(f'{filepath}*')
    mysql.close_connections()
    return file_parts


def main_impl():
    """Main sync logic"""

    args = utils.parse_args_for_partial_sync(REQUIRED_CONFIG_KEYS)
    pool_size = utils.get_pool_size(args.tap)
    start_time = datetime.now()
    table_sync_excs = []

    # Log start info
    LOGGER.info(
        '''
        -------------------------------------------------------
        STARTING PARTIAL SYNC
        -------------------------------------------------------
            Table selected to sync         : %s
            Column                         : %s
            Start value                    : %s
            End value                      : %s
            Pool size                      : %s
        -------------------------------------------------------
        ''', args.table, args.column, args.start_value, args.end_value, pool_size
    )

    partial_sync_table(args=args)

    # Log summary
    end_time = datetime.now()
    LOGGER.info(
        '''
        -------------------------------------------------------
        PARTIAL SYNC FINISHED - SUMMARY
        -------------------------------------------------------
            Table selected to sync         : %s
            Column                         : %s
            Start value                    : %s
            End value                      : %s
            Exceptions during table sync   : %s

            Runtime                        : %s
        -------------------------------------------------------
        ''', args.table, args.column, args.start_value, args.end_value, table_sync_excs, end_time - start_time
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


if __name__ == '__main__':
    main()
