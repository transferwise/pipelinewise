#!/usr/bin/env python3
import os
import multiprocessing
from functools import partial

from datetime import datetime
from typing import Union
from argparse import Namespace

from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.postgres_to_snowflake import REQUIRED_CONFIG_KEYS, tap_type_to_target_type
from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.partialsync import utils
from pipelinewise.logger import Logger

LOGGER = Logger().get_logger(__name__)


# pylint: disable=too-many-locals
def partial_sync_table(table: tuple, args: Namespace) -> Union[bool, str]:
    """Partial sync table for Postgres to Snowflake"""
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    tap_id = args.target.get('tap_id')
    dbname = args.tap.get('dbname')
    table_name = table[0]
    s3_keys = []
    target_schema = None
    target_table = None
    temp_created = False
    publication_status = {'attempted': False}
    grants_attempted = False
    try:
        column_name = table[1]['column']

        args.drop_target_table, args.table = (
            table[1]['drop_target_table'],
            table_name,
        )

        postgres = FastSyncTapPostgres(args.tap, tap_type_to_target_type)
        try:
            postgres.open_connection()

            start_value = utils.validate_boundary_value(postgres.query, table[1]['start_value'])
            end_value = utils.validate_boundary_value(postgres.query, table[1]['end_value'])

            if (
                start_value is utils.DYNAMIC_BOUNDARY_NOT_READY
                or end_value is utils.DYNAMIC_BOUNDARY_NOT_READY
            ):
                LOGGER.info('Dynamic boundary returned no value for %s; skipping PartialSync', table_name)
                return True

            bookmark = common_utils.get_bookmark_for_table(
                table_name, args.properties, postgres, dbname=dbname
            )
            snowflake_types = postgres.map_column_types_to_target(table_name)

            start_value_for_query = start_value if start_value == 'NULL' else f'\'{start_value}\''
            where_clause_sql = f' WHERE {column_name} >= {start_value_for_query}'
            if end_value is not None:
                where_clause_sql += f' AND {column_name} <= \'{end_value}\''

            file_parts = postgres.export_source_table_data(args, tap_id, where_clause_sql)
        finally:
            postgres.close_connection()

        target_schema = common_utils.get_target_schema(args.target, table_name)
        table_dict = common_utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name')
        target_sf = {
            'sf_object': snowflake,
            'schema': target_schema,
            'table': target_table,
            'temp': table_dict.get('temp_table_name'),
            'publication_status': publication_status,
        }
        source_columns = snowflake_types.get('columns', [])

        primary_keys = snowflake_types.get('primary_key')
        snowflake.create_schema(target_schema)
        temp_created = True
        snowflake.create_table(
            target_schema, target_table, source_columns, primary_keys, is_temporary=True
        )

        size_bytes = sum([os.path.getsize(file_part) for file_part in file_parts])
        s3_keys, s3_key_pattern = utils.upload_to_s3(snowflake, file_parts, args.temp_dir)

        utils.load_into_snowflake(
            target_sf, args, source_columns, primary_keys, s3_key_pattern,
            size_bytes, where_clause_sql,
        )
        publication_status['attempted'] = True
        temp_created = False

        grants_attempted = True
        common_utils.retry_snowflake_table_grants(
            snowflake, args.target, target_schema, table_name
        )
        utils.delete_s3_objects(snowflake, s3_keys, args.target.get('s3_bucket'))
        s3_keys = []
        utils.update_state_file(args, bookmark)

        return True
    except Exception as exc:
        LOGGER.exception(exc)
        return common_utils.partial_sync_failure_result(
            snowflake,
            args.target,
            table_name,
            target_schema,
            target_table,
            {
                's3_keys': getattr(exc, 's3_keys', s3_keys),
                'temp_created': temp_created,
                'publication_attempted': publication_status['attempted'],
                'grants_attempted': grants_attempted,
            },
            exc,
        )


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
