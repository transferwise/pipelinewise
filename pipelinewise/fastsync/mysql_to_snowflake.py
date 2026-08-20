#!/usr/bin/env python3
import sys
import re
from functools import partial
from argparse import Namespace
import multiprocessing
from typing import Union

from datetime import datetime
from ..logger import Logger
from .commons import utils
from .commons import rdbms_to_snowflake
from .commons import snowflake_iceberg_routes as iceberg_routes
from .commons.snowflake_types import SNOWFLAKE_MAX_VARCHAR
from .commons.rdbms_source import RdbmsSnowflakeSource
from .commons.tap_mysql import FastSyncTapMySql
from .commons.target_snowflake import FastSyncTargetSnowflake
from pipelinewise.utils import (get_tables_size,
                                filter_out_selected_tables,
                                get_maximum_value_from_list_of_dicts, get_schemas_of_tables_set)

LOGGER = Logger().get_logger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': ['host', 'port', 'user', 'password'],
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


def _is_boolean_tinyint(mysql_column_type):
    """Return whether a MySQL TINYINT display width is exactly one."""
    return bool(
        mysql_column_type
        and re.fullmatch(
            r'tinyint\(1\)(?:\s+unsigned)?(?:\s+zerofill)?',
            mysql_column_type.strip(),
            flags=re.IGNORECASE,
        )
    )


def tap_type_to_target_type(mysql_type, mysql_column_type):
    """Data type mapping from MySQL to Snowflake"""
    return {
        'char': SNOWFLAKE_MAX_VARCHAR,
        'varchar': SNOWFLAKE_MAX_VARCHAR,
        'binary': 'BINARY',
        'varbinary': 'BINARY',
        'blob': SNOWFLAKE_MAX_VARCHAR,
        'tinyblob': SNOWFLAKE_MAX_VARCHAR,
        'mediumblob': SNOWFLAKE_MAX_VARCHAR,
        'longblob': SNOWFLAKE_MAX_VARCHAR,
        'geometry': 'VARIANT',
        'point': 'VARIANT',
        'linestring': 'VARIANT',
        'polygon': 'VARIANT',
        'multipoint': 'VARIANT',
        'multilinestring': 'VARIANT',
        'multipolygon': 'VARIANT',
        'geometrycollection': 'VARIANT',
        'text': SNOWFLAKE_MAX_VARCHAR,
        'tinytext': SNOWFLAKE_MAX_VARCHAR,
        'mediumtext': SNOWFLAKE_MAX_VARCHAR,
        'longtext': SNOWFLAKE_MAX_VARCHAR,
        'enum': SNOWFLAKE_MAX_VARCHAR,
        'int': 'NUMBER',
        'tinyint': 'BOOLEAN' if _is_boolean_tinyint(mysql_column_type) else 'NUMBER',
        'smallint': 'NUMBER',
        'mediumint': 'NUMBER',
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
        'time': 'TIME',
        'json': 'VARIANT',
    }.get(mysql_type, SNOWFLAKE_MAX_VARCHAR)


def _source_adapter():
    return RdbmsSnowflakeSource.mysql(
        FastSyncTapMySql, tap_type_to_target_type
    )


def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one MySQL or MariaDB table to Snowflake."""
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
        tap_obj = FastSyncTapMySql(args.tap, tap_type_to_target_type)
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
