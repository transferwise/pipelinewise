#!/usr/bin/env python3
import logging
import multiprocessing
import os
import sys
import time
from datetime import datetime

from .commons import utils
from .commons.tap_postgres import FastSyncTapPostgres
from .commons.target_postgres import FastSyncTargetPostgres

LOGGER = logging.getLogger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': [
        'host',
        'port',
        'user',
        'password',
        'dbname',
        'tap_id'  # tap_id is required to generate unique replication slot names
    ],
    'target': [
        'host',
        'port',
        'user',
        'password'
    ]
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(pg_type):
    """Data type mapping from Postgres to Postgres"""
    return {
        'char': 'CHARACTER VARYING',
        'character': 'CHARACTER VARYING',
        'varchar': 'CHARACTER VARYING',
        'character varying': 'CHARACTER VARYING',
        'text': 'CHARACTER VARYING',
        'bit': 'BOOLEAN',
        'varbit': 'DOUBLE PRECISION',
        'bit varying': 'DOUBLE PRECISION',
        'smallint': 'SMALLINT NULL',
        'int': 'INTEGER NULL',
        'integer': 'INTEGER NULL',
        'bigint': 'BIGINT NULL',
        'smallserial': 'DOUBLE PRECISION',
        'serial': 'DOUBLE PRECISION',
        'bigserial': 'DOUBLE PRECISION',
        'numeric': 'DOUBLE PRECISION',
        'double precision': 'DOUBLE PRECISION',
        'real': 'DOUBLE PRECISION',
        'bool': 'BOOLEAN',
        'boolean': 'BOOLEAN',
        'date': 'TIMESTAMP WITHOUT TIME ZONE',
        'timestamp': 'TIMESTAMP WITHOUT TIME ZONE',
        'timestamp without time zone': 'TIMESTAMP WITHOUT TIME ZONE',
        'timestamp with time zone': 'TIMESTAMP WITH TIME ZONE',
        'time': 'TIME WITHOUT TIME ZONE',
        'time without time zone': 'TIME WITHOUT TIME ZONE',
        'time with time zone': 'TIME WITH TIME ZONE',
        # ARRAY is uppercase, because postgres stores it in this format in information_schema.columns.data_type
        'ARRAY': 'JSONB',
        'json': 'JSONB',
        'jsonb': 'JSONB'
    }.get(pg_type, 'CHARACTER VARYING')


# pylint: disable=inconsistent-return-statements
def sync_table(table):
    """Sync one table"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    postgres = FastSyncTapPostgres(args.tap, tap_type_to_target_type)
    postgres_target = FastSyncTargetPostgres(args.target, args.transform)

    try:
        dbname = args.tap.get('dbname')
        filename = 'pipelinewise_fastsync_{}_{}_{}.csv.gz'.format(dbname, table, time.strftime('%Y%m%d-%H%M%S'))
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection
        postgres.open_connection()

        # Get bookmark - LSN position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(table, args.properties, postgres, dbname=dbname)

        # Exporting table data, get table definitions and close connection to avoid timeouts
        postgres.copy_table(table, filepath)
        size_bytes = os.path.getsize(filepath)
        postgres_target_types = postgres.map_column_types_to_target(table)
        postgres_target_columns = postgres_target_types.get('columns', [])
        primary_key = postgres_target_types.get('primary_key')
        postgres.close_connection()

        # Creating temp table in Postgres
        postgres_target.create_schema(target_schema)
        postgres_target.drop_table(target_schema, table, is_temporary=True)
        postgres_target.create_table(target_schema, table, postgres_target_columns, primary_key, is_temporary=True)

        # Load into Postgres table
        postgres_target.copy_to_table(filepath, target_schema, table, size_bytes, is_temporary=True)
        os.remove(filepath)

        # Obfuscate columns
        postgres_target.obfuscate_columns(target_schema, table, is_temporary=True)

        # Create target table and swap with the temp table in Postgres
        postgres_target.swap_tables(target_schema, table)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        LOCK.acquire()
        try:
            utils.save_state_file(args.state, table, bookmark)
        finally:
            LOCK.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table)
        utils.grant_privilege(target_schema, grantees, postgres_target.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, postgres_target.grant_select_on_schema)

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

    # Start loading tables in parallel in spawning processes by
    # utilising all available CPU cores
    with multiprocessing.Pool(cpu_cores) as proc:
        table_sync_excs = list(filter(None, proc.map(sync_table, args.tables)))

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
