#!/usr/bin/env python3
import sys
import multiprocessing

from argparse import Namespace
from typing import Union
from functools import partial
from datetime import datetime

from ..logger import Logger
from .commons import utils
from .commons import rdbms_to_snowflake
from .commons import snowflake_iceberg_routes as iceberg_routes
from .commons.snowflake_types import SNOWFLAKE_MAX_VARCHAR
from .commons.rdbms_source import RdbmsSnowflakeSource
from .commons.tap_yugabyte import FastSyncTapYugabyte
from .commons.target_snowflake import FastSyncTargetSnowflake
from pipelinewise.utils import (get_tables_size,
                                filter_out_selected_tables,
                                get_maximum_value_from_list_of_dicts, get_schemas_of_tables_set)


LOGGER = Logger().get_logger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': [
        'host',
        'port',
        'user',
        'password',
        'dbname',
        'tap_id',  # tap_id is required to generate unique replication slot names
    ],
    'target': [
        'account',
        'dbname',
        'user',
        'private_key',
        'warehouse',
        's3_bucket',
        'stage',
        'file_format',
    ],
}


def tap_type_to_target_type(yb_type, *_):
    """Data type mapping from YugabyteDB (YSQL) to Snowflake"""
    return {
        'char': SNOWFLAKE_MAX_VARCHAR,
        'character': SNOWFLAKE_MAX_VARCHAR,
        'varchar': SNOWFLAKE_MAX_VARCHAR,
        'character varying': SNOWFLAKE_MAX_VARCHAR,
        'text': SNOWFLAKE_MAX_VARCHAR,
        'bit': 'BOOLEAN',
        'varbit': 'NUMBER',
        'bit varying': 'NUMBER',
        'smallint': 'NUMBER',
        'int': 'NUMBER',
        'integer': 'NUMBER',
        'bigint': 'NUMBER',
        'smallserial': 'NUMBER',
        'serial': 'NUMBER',
        'bigserial': 'NUMBER',
        'numeric': 'FLOAT',
        'double precision': 'FLOAT',
        'real': 'FLOAT',
        'bool': 'BOOLEAN',
        'boolean': 'BOOLEAN',
        'date': 'TIMESTAMP_NTZ',
        'timestamp': 'TIMESTAMP_NTZ',
        'timestamp without time zone': 'TIMESTAMP_NTZ',
        'timestamp with time zone': 'TIMESTAMP_NTZ',
        'time': 'TIME',
        'time without time zone': 'TIME',
        'time with time zone': 'TIME',
        # ARRAY is uppercase, because YSQL stores it in this format in information_schema.columns.data_type
        'ARRAY': 'VARIANT',
        'json': 'VARIANT',
        'jsonb': 'VARIANT',
    }.get(yb_type, SNOWFLAKE_MAX_VARCHAR)


def _source_adapter():
    return RdbmsSnowflakeSource.yugabyte(
        FastSyncTapYugabyte, tap_type_to_target_type
    )


def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one YugabyteDB table to Snowflake."""
    return rdbms_to_snowflake.sync_table(
        table,
        args,
        _source_adapter(),
        FastSyncTargetSnowflake,
        LOGGER,
        utils,
    )


def main_impl():
    """Main sync logic"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    iceberg_routes.validate_route_config(args.target)
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

    can_run_sync = True
    if args.autoresync_size:
        schemas = get_schemas_of_tables_set(args.tables)
        tap_obj = FastSyncTapYugabyte(args.tap, tap_type_to_target_type)
        for schema in schemas:
            all_tables_in_this_schema = get_tables_size(schema, tap_obj)
            only_selected_tables = filter_out_selected_tables(all_tables_in_this_schema, args.tables)
            table_with_maximum_size = get_maximum_value_from_list_of_dicts(only_selected_tables, 'table_size')
            if table_with_maximum_size.get('table_size') > float(args.autoresync_size):
                can_run_sync = False
                table_sync_excs.append(
                    f're-sync can not be done because size of table '
                    f'`{table_with_maximum_size["table_name"]}` is greater than `{args.autoresync_size}`!'
                    f' Use --force argument to force fast_sync!')

    # if internal arg drop_pg_slot is set to True, then we drop the slot before starting resync
    if args.drop_pg_slot:
        FastSyncTapYugabyte.drop_slot(args.tap)

    # Start loading tables in parallel in spawning processes
    if can_run_sync:
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
