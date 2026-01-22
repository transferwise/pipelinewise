#!/usr/bin/env python3
import os
import sys
import glob
import re
from functools import partial
from argparse import Namespace
import multiprocessing
from typing import Union

from datetime import datetime
from ..logger import Logger
from .commons import utils
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

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(mysql_type, mysql_column_type):
    """Data type mapping from MySQL to Snowflake """
    return {
        'char': 'VARCHAR',
        'varchar': 'VARCHAR',
        'binary': 'BINARY',
        'varbinary': 'BINARY',
        'blob': 'VARCHAR',
        'tinyblob': 'VARCHAR',
        'mediumblob': 'VARCHAR',
        'longblob': 'VARCHAR',
        'geometry': 'VARIANT',
        'point': 'VARIANT',
        'linestring': 'VARIANT',
        'polygon': 'VARIANT',
        'multipoint': 'VARIANT',
        'multilinestring': 'VARIANT',
        'multipolygon': 'VARIANT',
        'geometrycollection': 'VARIANT',
        'text': 'VARCHAR',
        'tinytext': 'VARCHAR',
        'mediumtext': 'VARCHAR',
        'longtext': 'VARCHAR',
        'enum': 'VARCHAR',
        'int': 'NUMBER',
        'tinyint': 'BOOLEAN'
        if mysql_column_type and mysql_column_type.startswith('tinyint(1)')
        else 'NUMBER',
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
    }.get(mysql_type, 'VARCHAR')


# pylint: disable=too-many-locals
def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    tap_id = args.target.get('tap_id')
    archive_load_files = args.target.get('archive_load_files', False)

    try:
        filename = utils.gen_export_filename(tap_id=tap_id, table=table)
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connections
        mysql.open_connections()

        # Get bookmark - Binlog position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(table, args.properties, mysql)

        # Exporting table data, get table definitions and close connection to avoid timeouts
        mysql.copy_table(
            table,
            filepath,
            split_large_files=args.target.get('split_large_files'),
            split_file_chunk_size_mb=args.target.get('split_file_chunk_size_mb'),
            split_file_max_chunks=args.target.get('split_file_max_chunks'),
        )
        snowflake_types = mysql.map_column_types_to_target(table)

        mysql.close_connections()

        file_exist = os.path.exists(filepath)
        file_parts = glob.glob(f'{filepath}*')
        if len(file_parts) == 0 and file_exist:
            LOGGER.warning('DATA LOSS! -> %s', filepath)

        size_bytes = sum([os.path.getsize(file_part) for file_part in file_parts])
        snowflake_columns = snowflake_types.get('columns', [])
        primary_key = snowflake_types.get('primary_key')

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

    finally:
        # try closing connections again just in case, silence errors
        mysql.close_connections(silent=True)


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
                    f' Use --force argument to force sync_tables!')

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
