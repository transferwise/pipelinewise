#!/usr/bin/env python3
import os
import sys
import glob
import re
import multiprocessing

from argparse import Namespace
from typing import Union
from functools import partial
from datetime import datetime

from ..logger import Logger
from .commons import utils
from .commons.tap_postgres import FastSyncTapPostgres
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
        'password',
        'warehouse',
        's3_bucket',
        'stage',
        'file_format',
    ],
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(pg_type, *_):
    """Data type mapping from Postgres to Snowflake"""
    return {
        'char': 'VARCHAR',
        'character': 'VARCHAR',
        'varchar': 'VARCHAR',
        'character varying': 'VARCHAR',
        'text': 'VARCHAR',
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
        # ARRAY is uppercase, because postgres stores it in this format in information_schema.columns.data_type
        'ARRAY': 'VARIANT',
        'json': 'VARIANT',
        'jsonb': 'VARIANT',
    }.get(pg_type, 'VARCHAR')


# pylint: disable=too-many-locals
def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    postgres = FastSyncTapPostgres(args.tap, tap_type_to_target_type)
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    tap_id = args.target.get('tap_id')
    archive_load_files = args.target.get('archive_load_files', False)

    try:
        dbname = args.tap.get('dbname')
        filename = utils.gen_export_filename(tap_id=tap_id, table=table)
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection
        postgres.open_connection()

        # Get bookmark - LSN position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(
            table, args.properties, postgres, dbname=dbname
        )

        # Exporting table data, get table definitions and close connection to avoid timeouts
        postgres.copy_table(
            table,
            filepath,
            split_large_files=args.target.get('split_large_files'),
            split_file_chunk_size_mb=args.target.get('split_file_chunk_size_mb'),
            split_file_max_chunks=args.target.get('split_file_max_chunks'),
        )
        file_exist = os.path.exists(filepath)
        file_parts = glob.glob(f'{filepath}*')
        if len(file_parts) == 0 and file_exist:
            LOGGER.warning('DATA LOSS! -> %s', filepath)

        size_bytes = sum([os.path.getsize(file_part) for file_part in file_parts])
        snowflake_types = postgres.map_column_types_to_target(table)
        snowflake_columns = snowflake_types.get('columns', [])
        primary_key = snowflake_types.get('primary_key')
        postgres.close_connection()

        # Uploading to S3
        s3_keys = []
        for file_part in file_parts:
            s3_keys.append(snowflake.upload_to_s3(file_part, tmp_dir=args.temp_dir))
            os.remove(file_part)

        # Create a pattern that match all file parts by removing multipart suffix
        s3_key_pattern = (
            re.sub(r'\.part\d*$', '', s3_keys[0])
            if len(s3_keys) > 0
            else 'NO_FILES_TO_LOAD'
        )

        # Creating temp table in Snowflake
        snowflake.create_schema(target_schema)
        snowflake.create_table(
            target_schema, table, snowflake_columns, primary_key, is_temporary=True
        )

        # Load into Snowflake table
        snowflake.copy_to_table(
            s3_key_pattern, target_schema, table, size_bytes, is_temporary=True
        )

        for s3_key in s3_keys:
            if archive_load_files:
                # Copy load file to archive
                snowflake.copy_to_archive(s3_key, tap_id, table)

            # Delete all file parts from s3
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

    can_run_sync = True
    if args.autoresync_size:
        schemas = get_schemas_of_tables_set(args.tables)
        tap_obj = FastSyncTapPostgres(args.tap, tap_type_to_target_type)
        for schema in schemas:
            all_tables_in_this_schema = get_tables_size(schema, tap_obj)
            only_selected_tables = filter_out_selected_tables(all_tables_in_this_schema, args.tables)
            table_with_maximum_size = get_maximum_value_from_list_of_dicts(only_selected_tables, 'table_size')
            if table_with_maximum_size.get('table_size') > float(args.autoresync_size):
                can_run_sync = False
                table_sync_excs.append(
                    f're-sync can not be done because size of table '
                    f'`{table_with_maximum_size["table_name"]}` is greater than `{args.autoresync_size}`!'
                    f' Use --force argument to force sync_tables!')

    # if internal arg drop_pg_slot is set to True, then we drop the slot before starting resync
    if args.drop_pg_slot:
        FastSyncTapPostgres.drop_slot(args.tap)

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
