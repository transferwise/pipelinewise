#!/usr/bin/env python3
"""
Partial sync from PostgreSQL to Apache Iceberg on S3
"""
import os
import multiprocessing
from functools import partial
from argparse import Namespace
from typing import Union
from datetime import datetime

from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.commons.target_iceberg import FastSyncTargetIceberg
from pipelinewise.logger import Logger
from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.partialsync import utils
from pipelinewise.fastsync.postgres_to_iceberg import REQUIRED_CONFIG_KEYS, tap_type_to_target_type

LOGGER = Logger().get_logger(__name__)


def partial_sync_table(table: tuple, args: Namespace) -> Union[bool, str]:
    """Partial sync table for PostgreSQL to Iceberg"""
    iceberg = FastSyncTargetIceberg(args.target, args.transform)
    tap_id = args.target.get('tap_id')

    try:
        table_name = table[0]
        column_name = table[1]['column']
        drop_target_table = table[1]['drop_target_table']

        args.drop_target_table = drop_target_table
        args.table = table_name

        postgres = FastSyncTapPostgres(args.tap, tap_type_to_target_type)
        postgres.open_connection()

        start_value = utils.validate_boundary_value(postgres.query, table[1]['start_value'])
        end_value = utils.validate_boundary_value(postgres.query, table[1]['end_value'])

        # Get bookmark
        bookmark = common_utils.get_bookmark_for_table(table_name, args.properties, postgres)

        target_schema = common_utils.get_target_schema(args.target, table_name)
        table_dict = common_utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name')

        iceberg_types = postgres.map_column_types_to_target(table_name)

        # Create target table if not exists
        iceberg.create_schema(target_schema)
        iceberg.create_table(
            schema_name=target_schema,
            table_name=target_table,
            columns=iceberg_types['columns'],
            primary_key=iceberg_types.get('primary_key'),
            is_temporary=False
        )

        # Build where clause
        start_value_for_query = start_value if start_value == 'NULL' else f'\'{start_value}\''
        where_clause_sql = f' WHERE {column_name} >= {start_value_for_query}'
        if end_value:
            where_clause_sql += f' AND {column_name} <= \'{end_value}\''

        # Export data from source
        file_parts = postgres.export_source_table_data(args, tap_id, where_clause_sql)

        postgres.close_connection()

        # Upload to S3 and load into Iceberg
        for file_part in file_parts:
            size_bytes = os.path.getsize(file_part)

            # Upload to S3
            s3_key = common_utils.upload_to_s3(
                file_part,
                args.target,
                os.path.basename(file_part),
                tmp_dir=args.temp_dir
            )

            # Load into Iceberg
            # Note: Iceberg handles updates via MERGE operations
            # For partial sync, we append new data and Iceberg's ACID properties handle consistency
            iceberg.copy_to_table(
                s3_key,
                target_schema,
                target_table,
                size_bytes,
                skip_csv_header=True
            )

            os.remove(file_part)

        if file_parts:
            utils.update_state_file(args, bookmark)

        LOGGER.info('Partial sync completed for table: %s', table_name)
        return True

    except Exception as exc:
        LOGGER.exception('Failed partial sync for table %s', table_name)
        return f'{table_name}: {exc}'

    finally:
        iceberg.close_connection()


def main_impl():
    """Main sync logic"""
    args = utils.parse_args_for_partial_sync(REQUIRED_CONFIG_KEYS)

    # Change back all quote tags to their original quote character
    args.start_value = utils.quote_tag_to_char(args.start_value)
    args.end_value = utils.quote_tag_to_char(args.end_value)

    start_time = datetime.now()
    pool_size = common_utils.get_pool_size(args.tap)

    # Log start info
    LOGGER.info(
        '''
        -------------------------------------------------------
        STARTING PARTIAL SYNC (PostgreSQL to Iceberg)
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
        sync_excs = []

    end_time = datetime.now()

    # Log summary
    LOGGER.info(
        '''
        -------------------------------------------------------
        PARTIAL SYNC COMPLETED
        -------------------------------------------------------
            Total tables selected          : %s
            Tables completed successfully  : %s
            Exceptions during sync         : %s

            Runtime                        : %s
        -------------------------------------------------------
        ''',
        len(sync_tables),
        len(sync_tables) - len(sync_excs),
        str(sync_excs) if sync_excs else 'None',
        end_time - start_time
    )

    if sync_excs:
        LOGGER.error('Errors during partial sync: %s', sync_excs)
        return False

    return True


def main():
    """Main entry point"""
    result = main_impl()
    return 0 if result else 1


if __name__ == '__main__':
    main()
