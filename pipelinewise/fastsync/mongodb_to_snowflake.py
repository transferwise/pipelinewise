#!/usr/bin/env python3
import os
import sys
import multiprocessing

from typing import Union
from argparse import Namespace
from functools import partial
from datetime import datetime

from ..logger import Logger
from .commons import utils
from .commons.tap_mongodb import FastSyncTapMongoDB
from .commons.target_snowflake import FastSyncTargetSnowflake

LOGGER = Logger().get_logger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': [
        'host',
        'port',
        'user',
        'password',
        'auth_database',
        'dbname',
    ],
    'target': [
        'account',
        'dbname',
        'user',
        'password',
        'warehouse',
        's3_bucket',
        'stage',
        'file_format',
    ],
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(mongo_type):
    """Data type mapping from MongoDB to Snowflake"""
    return {
        'string': 'TEXT',
        'object': 'VARIANT',
        'array': 'VARIANT',
        'date': 'TIMESTAMP_NTZ',
        'datetime': 'TIMESTAMP_NTZ',
        'timestamp': 'TIMESTAMP_NTZ',
    }.get(mongo_type, 'VARCHAR')


# pylint: disable=too-many-locals
def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    mongodb = FastSyncTapMongoDB(args.tap, tap_type_to_target_type)
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    tap_id = args.target.get('tap_id')
    archive_load_files = args.target.get('archive_load_files', False)

    try:
        dbname = args.tap.get('dbname')
        filename = utils.gen_export_filename(tap_id=tap_id, table=table)
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection
        mongodb.open_connection()

        # Get bookmark - LSN position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(
            table, args.properties, mongodb, dbname=dbname
        )

        # Exporting table data, get table definitions and close connection to avoid timeouts
        mongodb.copy_table(table, filepath, args.temp_dir)
        size_bytes = os.path.getsize(filepath)
        snowflake_types = mongodb.map_column_types_to_target()
        snowflake_columns = snowflake_types.get('columns', [])
        primary_key = snowflake_types['primary_key']
        mongodb.close_connection()

        # Uploading to S3
        s3_key = snowflake.upload_to_s3(filepath, tmp_dir=args.temp_dir)
        # os.remove(filepath)

        # Creating temp table in Snowflake
        snowflake.create_schema(target_schema)
        snowflake.create_table(
            target_schema, table, snowflake_columns, primary_key, is_temporary=True
        )

        # Load into Snowflake table
        snowflake.copy_to_table(
            s3_key,
            target_schema,
            table,
            size_bytes,
            is_temporary=True,
            skip_csv_header=True,
        )

        if archive_load_files:
            # Copy load file to archive
            snowflake.copy_to_archive(s3_key, tap_id, table)

        # Delete file from s3
        snowflake.s3.delete_object(Bucket=args.target.get('s3_bucket'), Key=s3_key)

        # Obfuscate columns
        snowflake.obfuscate_columns(target_schema, table)

        # Create target table and swap with the temp table in Snowflake
        snowflake.create_table(target_schema, table, snowflake_columns, primary_key)
        snowflake.swap_tables(target_schema, table)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        LOCK.acquire()
        try:
            utils.save_state_file(args.state, table, bookmark)
        finally:
            LOCK.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table)
        utils.grant_privilege(target_schema, grantees, snowflake.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, snowflake.grant_select_on_schema)

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
