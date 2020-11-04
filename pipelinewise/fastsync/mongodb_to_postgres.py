#!/usr/bin/env python3
import os
import sys
import time
import multiprocessing

from typing import Union
from argparse import Namespace
from functools import partial
from datetime import datetime

from ..logger import Logger
from .commons import utils
from .commons.tap_mongodb import FastSyncTapMongoDB
from .commons.target_postgres import FastSyncTargetPostgres

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
        'host',
        'port',
        'user',
        'password'
    ]
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(mongo_type):
    """Data type mapping from MongoDB to Postgres"""
    return {
        'string': 'CHARACTER VARYING',
        'object': 'JSONB',
        'array': 'JSONB',
        'date': 'TIMESTAMP WITHOUT TIME ZONE',
        'datetime': 'TIMESTAMP WITHOUT TIME ZONE',
        'timestamp': 'TIMESTAMP WITHOUT TIME ZONE',
    }.get(mongo_type, 'CHARACTER VARYING')


# pylint: disable=too-many-locals
def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    mongodb = FastSyncTapMongoDB(args.tap, tap_type_to_target_type)
    postgres = FastSyncTargetPostgres(args.target, args.transform)

    try:
        filename = 'pipelinewise_fastsync_{}_{}.csv.gz'.format(table, time.strftime('%Y%m%d-%H%M%S'))
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection
        mongodb.open_connection()

        # Get bookmark - token of the most recent ChangeStream for logbased
        bookmark = utils.get_bookmark_for_table(table, args.properties, mongodb, dbname=args.tap.get('dbname'))

        # Exporting table data, get table definitions and close connection to avoid timeouts
        mongodb.copy_table(table, filepath, args.temp_dir)
        size_bytes = os.path.getsize(filepath)
        snowflake_types = mongodb.map_column_types_to_target()
        postgres_columns = snowflake_types.get('columns', [])
        primary_key = snowflake_types['primary_key']
        mongodb.close_connection()

        # Creating temp table in Postgres
        postgres.drop_table(target_schema, table, is_temporary=True)
        postgres.create_table(target_schema, table, postgres_columns, primary_key, is_temporary=True)

        # Load into Postgres table
        postgres.copy_to_table(filepath, target_schema, table, size_bytes, is_temporary=True, skip_csv_header=True)
        os.remove(filepath)

        # Obfuscate columns
        postgres.obfuscate_columns(target_schema, table, is_temporary=True)

        # Create target table and swap with the temp table in Postgres
        postgres.swap_tables(target_schema, table)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        LOCK.acquire()
        try:
            utils.save_state_file(args.state, table, bookmark)
        finally:
            LOCK.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table)
        utils.grant_privilege(target_schema, grantees, postgres.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, postgres.grant_select_on_schema)

        return True

    except Exception as exc:
        LOGGER.exception(exc)
        return '{}: {}'.format(table, exc)


def main_impl():
    """Main sync logic"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    cpu_cores = utils.get_cpu_cores()
    start_time = datetime.now()
    table_sync_excs = []

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
        """, len(args.tables), len(args.tables) - len(table_sync_excs), str(table_sync_excs),
                cpu_cores, end_time - start_time)

    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
