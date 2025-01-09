#!/usr/bin/env python3
import os
import multiprocessing
from functools import partial

from argparse import Namespace
from typing import Union
from datetime import datetime

from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake

from pipelinewise.logger import Logger
from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.partialsync import utils

from pipelinewise.fastsync.mysql_to_snowflake import REQUIRED_CONFIG_KEYS, tap_type_to_target_type


LOGGER = Logger().get_logger(__name__)


# pylint: disable=too-many-locals
def partial_sync_table(table: tuple, args: Namespace) -> Union[bool, str]:
    """Partial sync table for MySQL to Snowflake"""
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    tap_id = args.target.get('tap_id')

    try:
        table_name = table[0]

        column_name = table[1]['column']

        drop_target_table = table[1]['drop_target_table']
        args.drop_target_table = drop_target_table
        args.table = table_name

        mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)

        mysql.open_connections()

        start_value = utils.validate_boundary_value(mysql.query, table[1]['start_value'])
        end_value = utils.validate_boundary_value(mysql.query, table[1]['end_value'])

        # Get bookmark - Binlog position or Incremental Key value
        bookmark = common_utils.get_bookmark_for_table(table_name, args.properties, mysql)

        target_schema = common_utils.get_target_schema(args.target, table_name)
        table_dict = common_utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name')

        target_sf = {
            'sf_object': snowflake,
            'schema': target_schema,
            'table': target_table,
            'temp': table_dict.get('temp_table_name')
        }

        snowflake_types = mysql.map_column_types_to_target(table_name)

        # making target table if not exists
        snowflake.create_schema(target_schema)
        snowflake.create_table(
            target_schema=target_schema,
            table_name=target_table,
            columns=snowflake_types['columns'],
            primary_key=snowflake_types.get('primary_key'),
            is_temporary=False,
            sort_columns=False,
            allow_replace_table=False
        )

        source_columns = snowflake_types.get('columns', [])
        columns_diff = utils.diff_source_target_columns(target_sf, source_columns=source_columns)

        start_value_for_query = start_value if start_value == 'NULL' else f'\'{start_value}\''
        where_clause_sql = f' WHERE {column_name} >= {start_value_for_query}'
        if end_value:
            where_clause_sql += f' AND {column_name} <= \'{end_value}\''

        # export data from source
        file_parts = mysql.export_source_table_data(args, tap_id, where_clause_sql)

        # mark partial data as deleted in the target
        snowflake.query(f'UPDATE {target_schema}."{target_table.upper()}"'
                        f' SET _SDC_DELETEd_AT = CURRENT_TIMESTAMP(){where_clause_sql} AND _SDC_DELETED_AT IS NULL')

        # Creating temp table in Snowflake
        primary_keys = snowflake_types.get('primary_key')
        snowflake.create_schema(target_schema)
        snowflake.create_table(
            target_schema, target_table, source_columns, primary_keys, is_temporary=True
        )

        mysql.close_connections()

        size_bytes = sum([os.path.getsize(file_part) for file_part in file_parts])
        _, s3_key_pattern = utils.upload_to_s3(snowflake, file_parts, args.temp_dir)

        utils.load_into_snowflake(
            target_sf, args, columns_diff, primary_keys, s3_key_pattern, size_bytes, where_clause_sql)

        if file_parts:
            utils.update_state_file(args, bookmark)

        return True
    except Exception as exc:
        LOGGER.exception(exc)
        return f'{table_name}: {exc}'


def main_impl():
    """Main sync logic"""

    args = utils.parse_args_for_partial_sync(REQUIRED_CONFIG_KEYS)

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
        sync_excs = list(
            filter(
                lambda x: not isinstance(x, bool),
                proc.map(partial(partial_sync_table, args=args), sync_tables.items())
            )
        )

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

    if len(sync_excs) > 0:
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
