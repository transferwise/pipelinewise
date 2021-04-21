#!/usr/bin/env python3
import os
import sys
from functools import partial
from argparse import Namespace
import multiprocessing
from typing import Union

from datetime import datetime
from ..logger import Logger
from .commons import utils
from .commons.tap_mysql import FastSyncTapMySql
from .commons.target_postgres import FastSyncTargetPostgres

LOGGER = Logger().get_logger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': [
        'host',
        'port',
        'user',
        'password'
    ],
    'target': [
        'host',
        'port',
        'user',
        'password'
    ]
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(mysql_type, mysql_column_type):
    """Data type mapping from MySQL to Postgres"""
    return {
        'char': 'CHARACTER VARYING',
        'varchar': 'CHARACTER VARYING',
        'binary': 'CHARACTER VARYING',
        'varbinary': 'CHARACTER VARYING',
        'blob': 'CHARACTER VARYING',
        'tinyblob': 'CHARACTER VARYING',
        'mediumblob': 'CHARACTER VARYING',
        'longblob': 'CHARACTER VARYING',
        'geometry': 'JSONB',
        'point': 'JSONB',
        'linestring': 'JSONB',
        'polygon': 'JSONB',
        'multipoint': 'JSONB',
        'multilinestring': 'JSONB',
        'multipolygon': 'JSONB',
        'geometrycollection': 'JSONB',
        'text': 'CHARACTER VARYING',
        'tinytext': 'CHARACTER VARYING',
        'mediumtext': 'CHARACTER VARYING',
        'longtext': 'CHARACTER VARYING',
        'enum': 'CHARACTER VARYING',
        'int': 'BIGINT NULL' if (mysql_column_type and mysql_column_type == 'int(10) unsigned') else 'INTEGER NULL',
        'tinyint': 'BOOLEAN' if mysql_column_type and mysql_column_type.startswith('tinyint(1)') else 'SMALLINT NULL',
        'smallint': 'INTEGER NULL' if (mysql_column_type and mysql_column_type == 'smallint(5) unsigned') else 'SMALLINT NULL',
        'mediumint': 'INTEGER NULL',
        'bigint': 'BIGINT NULL',
        'bit': 'BOOLEAN',
        'decimal': 'DOUBLE PRECISION',
        'double': 'DOUBLE PRECISION',
        'float': 'DOUBLE PRECISION',
        'bool': 'BOOLEAN',
        'boolean': 'BOOLEAN',
        'date': 'TIMESTAMP WITHOUT TIME ZONE',
        'datetime': 'TIMESTAMP WITHOUT TIME ZONE',
        'timestamp': 'TIMESTAMP WITHOUT TIME ZONE',
        'time': 'TIME WITHOUT TIME ZONE',
        'json': 'JSONB',
    }.get(
        mysql_type,
        'CHARACTER VARYING',
    )


def sync_table(table: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)
    postgres = FastSyncTargetPostgres(args.target, args.transform)

    try:
        filename = utils.gen_export_filename(tap_id=args.target.get('tap_id'), table=table)
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection and get binlog file position
        mysql.open_connections()

        # Get bookmark - Binlog position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(table, args.properties, mysql)

        # Exporting table data, get table definitions and close connection to avoid timeouts
        mysql.copy_table(table, filepath)
        size_bytes = os.path.getsize(filepath)
        postgres_types = mysql.map_column_types_to_target(table)
        postgres_columns = postgres_types.get('columns', [])
        primary_key = postgres_types.get('primary_key')
        mysql.close_connections()

        # Creating temp table in Postgres
        postgres.drop_table(target_schema, table, is_temporary=True)
        postgres.create_table(target_schema, table, postgres_columns, primary_key, is_temporary=True)

        # Load into Postgres table
        postgres.copy_to_table(filepath, target_schema, table, size_bytes, is_temporary=True)
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
    pool_size = utils.get_pool_size(args.tap)
    start_time = datetime.now()
    table_sync_excs = []

    # Log start info
    LOGGER.info("""
        -------------------------------------------------------
        STARTING SYNC
        -------------------------------------------------------
            Tables selected to sync        : %s
            Total tables selected to sync  : %s
            Pool size                      : %s
        -------------------------------------------------------
        """, args.tables, len(args.tables), pool_size)

    # Create target schemas sequentially, Postgres doesn't like it running in parallel
    postgres_target = FastSyncTargetPostgres(args.target, args.transform)
    postgres_target.create_schemas(args.tables)

    # Start loading tables in parallel in spawning processes
    with multiprocessing.Pool(pool_size) as proc:
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

            Pool size                      : %s
            Runtime                        : %s
        -------------------------------------------------------
        """, len(args.tables), len(args.tables) - len(table_sync_excs), str(table_sync_excs),
                pool_size, end_time - start_time)

    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
