#!/usr/bin/env python3
import os
import sys
import multiprocessing

from functools import partial
from argparse import Namespace
from typing import Union
from datetime import datetime

from ..logger import Logger
from .commons import utils
from .commons.tap_postgres import FastSyncTapPostgres
from .commons.target_postgres import FastSyncTargetPostgres

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
    'target': ['host', 'port', 'user', 'password'],
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(pg_type, *_):
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
        'timestamp with time zone': 'TIMESTAMP WITHOUT TIME ZONE',
        'time': 'TIME WITHOUT TIME ZONE',
        'time without time zone': 'TIME WITHOUT TIME ZONE',
        'time with time zone': 'TIME WITH TIME ZONE',
        # ARRAY is uppercase, because postgres stores it in this format in information_schema.columns.data_type
        'ARRAY': 'JSONB',
        'json': 'JSONB',
        'jsonb': 'JSONB',
    }.get(pg_type, 'CHARACTER VARYING')


def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    postgres = FastSyncTapPostgres(args.tap, tap_type_to_target_type)
    postgres_target = FastSyncTargetPostgres(args.target, args.transform)

    try:
        dbname = args.tap.get('dbname')
        filename = utils.gen_export_filename(
            tap_id=args.target.get('tap_id'), table=table
        )
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection
        postgres.open_connection()

        # Get bookmark - LSN position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(
            table, args.properties, postgres, dbname=dbname
        )

        # Exporting table data, get table definitions and close connection to avoid timeouts
        postgres.copy_table(table, filepath)
        postgres_target_types = postgres.map_column_types_to_target(table)
        postgres_target_columns = postgres_target_types.get('columns', [])
        primary_key = postgres_target_types.get('primary_key')
        postgres.close_connection()

        # Creating temp table in Postgres
        postgres_target.drop_table(target_schema, table, is_temporary=True)
        postgres_target.create_table(
            target_schema,
            table,
            postgres_target_columns,
            primary_key,
            is_temporary=True,
        )

        # if table is empty, then there is no exported file at filepath
        if os.path.exists(filepath):
            size_bytes = os.path.getsize(filepath)

            # Load into Postgres table
            postgres_target.copy_to_table(
                filepath, target_schema, table, size_bytes, is_temporary=True
            )
            os.remove(filepath)

            # Obfuscate columns
            postgres_target.obfuscate_columns(target_schema, table, is_temporary=True)
        else:
            LOGGER.warning('Not export file has been generated, this is likely due to table being empty')

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
        utils.grant_privilege(
            target_schema, grantees, postgres_target.grant_usage_on_schema
        )
        utils.grant_privilege(
            target_schema, grantees, postgres_target.grant_select_on_schema
        )

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

    # if internal arg drop_pg_slot is set to True, then we drop the slot before starting resync
    if args.drop_pg_slot:
        FastSyncTapPostgres.drop_slot(args.tap)

    # Create target schemas sequentially, Postgres doesn't like it running in parallel
    postgres_target = FastSyncTargetPostgres(args.target, args.transform)
    postgres_target.create_schemas(args.tables)

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
