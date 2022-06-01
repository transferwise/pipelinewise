#!/usr/bin/env python3
import os
import glob
import multiprocessing


from datetime import datetime
from typing import Union
from argparse import Namespace


from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.postgres_to_snowflake import REQUIRED_CONFIG_KEYS, tap_type_to_target_type
from pipelinewise.fastsync.commons import utils
from pipelinewise.fastsync.partialsync.utils import load_into_snowflake,upload_to_s3,update_state_file
from pipelinewise.logger import Logger

LOGGER = Logger().get_logger(__name__)

LOCK = multiprocessing.Lock()


def partial_sync_table(args: Namespace) -> Union[bool, str]:
    """Partial sync table for MySQL to Snowflake"""
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    tap_id = args.target.get('tap_id')
    dbname = args.tap.get('dbname')
    try:
        postgres = FastSyncTapPostgres(args.tap, tap_type_to_target_type)

        # Get bookmark - Binlog position or Incremental Key value
        postgres.open_connection()
        bookmark = utils.get_bookmark_for_table(args.table, args.properties, postgres, dbname=dbname)

        file_parts = _export_source_table_data(args, tap_id, postgres)
        postgres.close_connection()
        size_bytes = sum([os.path.getsize(file_part) for file_part in file_parts])
        s3_keys, s3_key_pattern = upload_to_s3(snowflake, file_parts, args.temp_dir)
        load_into_snowflake(snowflake, args, s3_keys, s3_key_pattern, size_bytes)
        update_state_file(args, bookmark, LOCK)

        return True
    except Exception as exc:
        LOGGER.exception(exc)
        return f'{args.table}: {exc}'


def _export_source_table_data(args, tap_id, postgres):
    filename = utils.gen_export_filename(tap_id=tap_id, table=args.table, sync_type='partialsync')
    filepath = os.path.join(args.temp_dir, filename)

    where_clause_setting = {
        'column': args.column,
        'start_value': args.start_value,
        'end_value': args.end_value
    }
    postgres.copy_table(
        args.table,
        filepath,
        split_large_files=args.target.get('split_large_files'),
        split_file_chunk_size_mb=args.target.get('split_file_chunk_size_mb'),
        split_file_max_chunks=args.target.get('split_file_max_chunks'),
        where_clause_setting=where_clause_setting
    )
    file_parts = glob.glob(f'{filepath}*')
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

    # if internal arg drop_pg_slot is set to True, then we drop the slot before starting resync
    if args.drop_pg_slot:
        FastSyncTapPostgres.drop_slot(args.tap)

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
        raise SystemExit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
