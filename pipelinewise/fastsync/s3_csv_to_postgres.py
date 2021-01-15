#!/usr/bin/env python3
import multiprocessing
import os
import sys
import time
from argparse import Namespace
from datetime import datetime
from functools import partial
from typing import Union

from ..logger import Logger
from .commons import utils
from .commons.tap_s3_csv import FastSyncTapS3Csv
from .commons.target_postgres import FastSyncTargetPostgres

LOGGER = Logger().get_logger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': [
        'aws_access_key_id',
        'aws_secret_access_key',
        'bucket',
        'start_date'
    ],
    'target': [
        'host',
        'port',
        'user',
        'password'
    ]
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(csv_type):
    """Data type mapping from S3 csv to Snowflake"""

    return {
        'integer': 'NUMERIC',
        'number': 'DOUBLE PRECISION',
        'string': 'CHARACTER VARYING',
        'boolean': 'CHARACTER VARYING',  # The guess sometimes can be wrong, we'll use varchar for now.
        'date': 'CHARACTER VARYING',  # The guess sometimes can be wrong, we'll use varchar for now.

        'date_override': 'TIMESTAMP WITHOUT TIME ZONE'    # Column type to use when date_override defined in YAML
    }.get(csv_type, 'CHARACTER VARYING')


def sync_table(table_name: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    s3_csv = FastSyncTapS3Csv(args.tap, tap_type_to_target_type)
    postgres = FastSyncTargetPostgres(args.target, args.transform)

    try:
        filename = 'pipelinewise_fastsync_{}_{}_{}.csv.gz'.format(args.tap['bucket'], table_name,
                                                                  time.strftime('%Y%m%d-%H%M%S'))
        filepath = os.path.join(args.temp_dir, filename)

        target_schema = utils.get_target_schema(args.target, table_name)

        s3_csv.copy_table(table_name, filepath)
        size_bytes = os.path.getsize(filepath)

        postgres_types = s3_csv.map_column_types_to_target(filepath, table_name)
        postgres_columns = postgres_types.get('columns', [])
        primary_key = postgres_types['primary_key']

        # Creating temp table in Postgres
        postgres.drop_table(target_schema, table_name, is_temporary=True)
        postgres.create_table(target_schema,
                              table_name,
                              postgres_columns,
                              primary_key,
                              is_temporary=True,
                              sort_columns=True)

        # Load into Postgres table
        postgres.copy_to_table(filepath, target_schema, table_name, size_bytes, is_temporary=True, skip_csv_header=True)
        os.remove(filepath)

        # Obfuscate columns
        postgres.obfuscate_columns(target_schema, table_name, is_temporary=True)

        # Create target table and swap with the temp table in Postgres
        postgres.swap_tables(target_schema, table_name)

        # Get bookmark
        bookmark = utils.get_bookmark_for_table(table_name, args.properties, s3_csv)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        LOCK.acquire()
        try:
            utils.save_state_file(args.state, table_name, bookmark)
        finally:
            LOCK.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table_name)
        utils.grant_privilege(target_schema, grantees, postgres.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, postgres.grant_select_on_schema)

        return True

    except Exception as exc:
        LOGGER.exception(exc)
        return f'{table_name}: {exc}'


def main_impl():
    """Main sync logic"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    cpu_cores = args.tap.get('fastsync_parallelism', utils.get_cpu_cores())
    start_time = datetime.now()

    # Log start info
    LOGGER.info("""
        -------------------------------------------------------
        STARTING SYNC
        -------------------------------------------------------
            Tables selected to sync        : %s
            Total tables selected to sync  : %s
            CPU cores                      : %s
        -------------------------------------------------------
        """, args.tables, len(args.tables), cpu_cores)

    # Create target schemas sequentially, Postgres doesn't like it running in parallel
    postgres_target = FastSyncTargetPostgres(args.target, args.transform)
    postgres_target.create_schemas(args.tables)

    # Start loading tables in parallel in spawning processes by
    # utilising all available CPU cores
    with multiprocessing.Pool(cpu_cores) as proc:
        table_sync_excs = list(
            filter(lambda x: not isinstance(x, bool), proc.map(partial(sync_table, args=args), args.tables)))

    # Log summary
    end_time = datetime.now()
    LOGGER.info("""
        -------------------------------------------------------
        SYNC FINISHED - SUMMARY
        -------------------------------------------------------
            Total tables selected to sync  : %s
            Tables loaded successfully     : %s
            Exceptions during table sync   : %s

            CPU cores                      : %s
            Runtime                        : %s
        -------------------------------------------------------
        """, len(args.tables), len(args.tables) - len(table_sync_excs),
                str(table_sync_excs), cpu_cores, end_time - start_time)

    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
