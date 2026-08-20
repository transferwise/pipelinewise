#!/usr/bin/env python3
import multiprocessing
from functools import partial

from datetime import datetime
from typing import Union
from argparse import Namespace

from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.postgres_to_snowflake import REQUIRED_CONFIG_KEYS, tap_type_to_target_type
from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.commons import snowflake_iceberg_routes as iceberg_routes
from pipelinewise.fastsync.commons.rdbms_source import RdbmsSnowflakeSource
from pipelinewise.fastsync.partialsync import rdbms_to_snowflake
from pipelinewise.fastsync.partialsync import utils
from pipelinewise.logger import Logger

LOGGER = Logger().get_logger(__name__)


def _source_adapter():
    return RdbmsSnowflakeSource.postgres(
        FastSyncTapPostgres, tap_type_to_target_type
    )


def partial_sync_table(table: tuple, args: Namespace) -> Union[bool, str]:
    """PartialSync one PostgreSQL table to Snowflake."""
    return rdbms_to_snowflake.partial_sync_table(
        table,
        args,
        _source_adapter(),
        FastSyncTargetSnowflake,
        LOGGER,
    )


def main_impl():
    """Main sync logic"""
    args = utils.parse_args_for_partial_sync(REQUIRED_CONFIG_KEYS)
    iceberg_routes.validate_route_config(args.target)

    # changing back all quote tags to their original quote character
    args.start_value = utils.quote_tag_to_char(args.start_value)
    args.end_value = utils.quote_tag_to_char(args.end_value)

    start_time = datetime.now()

    pool_size = common_utils.get_pool_size(args.tap)
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

    sync_tables = utils.get_sync_tables(args)
    pool_size = len(sync_tables) if len(sync_tables) < pool_size else pool_size
    with multiprocessing.Pool(pool_size) as proc:
        sync_results = proc.map(
            partial(partial_sync_table, args=args),
            sync_tables.items(),
        )

    sync_excs = [result for result in sync_results if result is not True]

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

    if len(sync_excs) > 0:
        raise SystemExit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
