#!/usr/bin/env python3
import os

from argparse import Namespace
from typing import Union
from datetime import datetime

from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake

from pipelinewise.logger import Logger
from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.partialsync import utils

from pipelinewise.fastsync.mysql_to_snowflake import REQUIRED_CONFIG_KEYS, tap_type_to_target_type
from pipelinewise.fastsync.partialsync.utils import load_into_snowflake, upload_to_s3, update_state_file

LOGGER = Logger().get_logger(__name__)


def partial_sync_table(args: Namespace) -> Union[bool, str]:
    """Partial sync table for MySQL to Snowflake"""
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    tap_id = args.target.get('tap_id')

    try:
        mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)

        # Get bookmark - Binlog position or Incremental Key value
        mysql.open_connections()
        bookmark = common_utils.get_bookmark_for_table(args.table, args.properties, mysql)

        where_clause_setting = {
            'column': args.column,
            'start_value': args.start_value,
            'end_value': args.end_value
        }

        file_parts = mysql.export_source_table_data(args, tap_id, where_clause_setting)

        mysql.close_connections()
        size_bytes = sum([os.path.getsize(file_part) for file_part in file_parts])
        s3_keys, s3_key_pattern = upload_to_s3(snowflake, file_parts, args.temp_dir)
        load_into_snowflake(snowflake, args, s3_keys, s3_key_pattern, size_bytes)
        update_state_file(args, bookmark)

        return True
    except Exception as exc:
        LOGGER.exception(exc)
        return f'{args.table}: {exc}'


def main_impl():
    """Main sync logic"""

    args = utils.parse_args_for_partial_sync(REQUIRED_CONFIG_KEYS)
    start_time = datetime.now()

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
        -------------------------------------------------------
        ''', args.table, args.column, args.start_value, args.end_value
    )

    sync_excs = partial_sync_table(args=args)
    if isinstance(sync_excs, bool):
        sync_excs = None

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
        ''', args.table, args.column, args.start_value, args.end_value, sync_excs, end_time - start_time
    )

    if sync_excs is not None:
        raise SystemExit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
