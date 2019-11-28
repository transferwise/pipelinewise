#!/usr/bin/env python3

import os
import sys
import time

import multiprocessing

from typing import Union
from datetime import datetime
from functools import partial
from argparse import Namespace

from .commons import utils
from .commons.tap_s3_csv import FastSyncTapS3Csv
from .commons.target_snowflake import FastSyncTargetSnowflake


REQUIRED_CONFIG_KEYS = {
    'tap': [
        'aws_access_key_id',
        'aws_secret_access_key',
        'bucket',
        'start_date'
    ],
    'target': [
        'account',
        'dbname',
        'user',
        'password',
        'warehouse',
        'aws_access_key_id',
        'aws_secret_access_key',
        's3_bucket',
        'stage',
        'file_format'
    ]
}

lock = multiprocessing.Lock()


def tap_type_to_target_type(csv_type):
    """Data type mapping from S3 csv to Snowflake"""

    return {
        'Integer': 'INTEGER',
        'Decimal': 'NUMBER',
        'String': 'VARCHAR',
        'Bool': 'VARCHAR' # The guess sometimes can be wrong, we'll use varchar for now.
    }.get(csv_type, 'VARCHAR')


def sync_table(table_name: str, args: Namespace)->Union[bool, str]:

    s3_csv = FastSyncTapS3Csv(args.tap, tap_type_to_target_type)
    snowflake =  FastSyncTargetSnowflake(args.target, args.transform)

    try:
        filename = "pipelinewise_fastsync_{}_{}_{}.csv.gz".format(args.tap['bucket'], table_name,
                                                                  time.strftime("%Y%m%d-%H%M%S"))
        filepath = os.path.join(args.export_dir, filename)

        target_schema = utils.get_target_schema(args.target, table_name)

        s3_csv.copy_table(table_name, filepath)

        snowflake_types = s3_csv.map_column_types_to_target(filepath, table_name)
        snowflake_columns = snowflake_types.get("columns", [])
        primary_key = snowflake_types["primary_key"]

        # Uploading to S3
        s3_key = snowflake.upload_to_s3(filepath, table_name)
        os.remove(filepath)

        # Creating temp table in Snowflake
        snowflake.create_schema(target_schema)
        snowflake.create_table(target_schema, table_name, snowflake_columns, primary_key,
                               is_temporary=True, sort_columns=True)

        # Load into Snowflake table
        snowflake.copy_to_table(s3_key, target_schema, table_name, is_temporary=True, skip_csv_header=True)

        # Obfuscate columns
        snowflake.obfuscate_columns(target_schema, table_name)

        # Create target table and swap with the temp table in Snowflake
        snowflake.create_table(target_schema, table_name, snowflake_columns, primary_key, sort_columns=True)
        snowflake.swap_tables(target_schema, table_name)

        # Get bookmark
        bookmark = utils.get_bookmark_for_table(table_name, args.properties, s3_csv)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        lock.acquire()
        try:
            utils.save_state_file(args.state, table_name, bookmark)
        finally:
            lock.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table_name)
        utils.grant_privilege(target_schema, grantees, snowflake.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, snowflake.grant_select_on_schema)

        return True

    except Exception as exc:
        utils.log(f"CRITICAL: {exc}")
        return f"{table_name}: {exc}"


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    cpu_cores = utils.get_cpu_cores()
    start_time = datetime.now()

    # Log start info
    utils.log("""
        -------------------------------------------------------
        STARTING SYNC
        -------------------------------------------------------
            Tables selected to sync        : {}
            Total tables selected to sync  : {}
            CPU cores                      : {}
        -------------------------------------------------------
        """.format(
            args.tables,
            len(args.tables),
            cpu_cores
        ))

    # Start loading tables in parallel in spawning processes by
    # utilising all available CPU cores
    with multiprocessing.Pool(cpu_cores) as p:
        table_sync_excs = list(filter(lambda x: not isinstance(x, bool),
                                      p.map(partial(sync_table, args=args), args.tables)))

    # Refresh information_schema columns cache
    snowflake = FastSyncTargetSnowflake(args.target, args.transform)
    snowflake.cache_information_schema_columns(args.tables)

    # Log summary
    end_time = datetime.now()
    utils.log("""
        -------------------------------------------------------
        SYNC FINISHED - SUMMARY
        -------------------------------------------------------
            Total tables selected to sync  : {}
            Tables loaded successfully     : {}
            Exceptions during table sync   : {}

            CPU cores                      : {}
            Runtime                        : {}
        -------------------------------------------------------
        """.format(
            len(args.tables),
            len(args.tables) - len(table_sync_excs),
            str(table_sync_excs),
            cpu_cores,
            end_time  - start_time
        ))
    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    try:
        main_impl()
    except Exception as exc:
        utils.log("CRITICAL: {}".format(exc))
        raise exc
