#!/usr/bin/env python3
"""
FastSync from PostgreSQL to Apache Iceberg on S3
"""
import logging
import os
import sys
from argparse import Namespace
from typing import List

from .commons import utils
from .commons.tap_postgres import FastSyncTapPostgres
from .commons.target_iceberg import FastSyncTargetIceberg

LOGGER = logging.getLogger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': [
        'host',
        'port',
        'user',
        'password',
        'dbname'
    ],
    'target': [
        'aws_access_key_id',
        'aws_secret_access_key',
        's3_bucket',
        'glue_catalog_id'
    ]
}

LOCK = utils.Lock()


def tap_type_to_target_type(postgres_type: str, *args) -> str:
    """
    Map PostgreSQL data types to Iceberg compatible types
    """
    # Numeric types
    if postgres_type in ('smallint', 'integer', 'bigint', 'int2', 'int4', 'int8'):
        return 'BIGINT'
    elif postgres_type in ('decimal', 'numeric', 'real', 'double precision'):
        return 'DOUBLE'
    elif postgres_type == 'money':
        return 'DOUBLE'

    # String types
    elif postgres_type in ('character varying', 'varchar', 'character', 'char', 'text'):
        return 'STRING'
    elif postgres_type == 'bytea':
        return 'BINARY'

    # Date and time types
    elif postgres_type == 'date':
        return 'DATE'
    elif postgres_type in ('timestamp without time zone', 'timestamp with time zone', 'timestamp', 'timestamptz'):
        return 'TIMESTAMP'
    elif postgres_type in ('time without time zone', 'time with time zone', 'time', 'timetz'):
        return 'STRING'
    elif postgres_type == 'interval':
        return 'STRING'

    # Boolean
    elif postgres_type in ('boolean', 'bool'):
        return 'BOOLEAN'

    # JSON
    elif postgres_type in ('json', 'jsonb'):
        return 'STRING'

    # Arrays
    elif postgres_type.endswith('[]'):
        return 'STRING'

    # UUID
    elif postgres_type == 'uuid':
        return 'STRING'

    # Network address types
    elif postgres_type in ('cidr', 'inet', 'macaddr', 'macaddr8'):
        return 'STRING'

    # Bit strings
    elif postgres_type in ('bit', 'bit varying'):
        return 'STRING'

    # Geometric types
    elif postgres_type in ('point', 'line', 'lseg', 'box', 'path', 'polygon', 'circle'):
        return 'STRING'

    # Range types
    elif postgres_type in ('int4range', 'int8range', 'numrange', 'tsrange', 'tstzrange', 'daterange'):
        return 'STRING'

    # Default to string
    else:
        return 'STRING'


def sync_table(table: str, args: Namespace) -> None:
    """
    Sync a single table from PostgreSQL to Iceberg
    """
    postgres = FastSyncTapPostgres(args.tap, tap_type_to_target_type)
    iceberg = FastSyncTargetIceberg(args.target, args.transform)

    try:
        filename = 'pipelinewise_fastsync_{}_{}_{}.csv.gz'.format(
            args.tap.get('dbname', args.target.get('default_target_schema', 'default')),
            table,
            utils.get_timestamp()
        )
        filepath = f'{args.temp_dir}/{filename}'
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection and get LSN position
        postgres.open_connection()

        # Get bookmark - PostgreSQL LSN
        bookmark = utils.get_bookmark_for_table(
            table, args.properties, postgres, dbname=args.tap.get('dbname')
        )

        # Exporting table data, get table definitions and close connection
        postgres.copy_table(table, filepath)
        size_bytes = os.path.getsize(filepath)
        iceberg_types = postgres.map_column_types_to_target(table)
        iceberg_columns = iceberg_types.get('columns', [])
        primary_key = iceberg_types.get('primary_key')

        postgres.close_connection()

        # Upload to S3
        s3_key = utils.upload_to_s3(
            filepath,
            args.target,
            filename,
            tmp_dir=args.temp_dir
        )
        os.remove(filepath)

        # Create schema and table in Iceberg
        iceberg.create_schema(target_schema)
        iceberg.create_table(
            target_schema,
            table,
            iceberg_columns,
            primary_key
        )

        # Load data from S3 to Iceberg
        iceberg.copy_to_table(
            s3_key,
            target_schema,
            table,
            size_bytes,
            skip_csv_header=True
        )

        # Save bookmark
        utils.save_bookmark_for_table(table, bookmark, args.state)

        # Table loaded, grant select privileges
        if args.target.get('grant_privilege'):
            grantees = args.target.get('grant_privilege')
            LOGGER.info('Granting privileges to %s...', grantees)
            # Note: Iceberg on S3 uses IAM for access control

        LOGGER.info('Table sync completed: %s', table)

    except Exception as exc:
        LOGGER.exception('Failed to sync table %s', table)
        raise exc

    finally:
        iceberg.close_connection()


def main_impl():
    """Main FastSync implementation"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    cpu_cores = utils.get_cpu_cores()
    start_time = utils.get_bookmark()
    table_sync_excs = []

    # Log start info
    LOGGER.info('Starting PostgreSQL to Iceberg FastSync')
    LOGGER.info('Tap database: %s', args.tap.get('dbname'))
    LOGGER.info('Target schema: %s', args.target.get('default_target_schema'))
    LOGGER.info('Using %d CPU cores', cpu_cores)

    # Create target Iceberg instance to validate connection
    try:
        iceberg = FastSyncTargetIceberg(args.target, args.transform)
        iceberg.close_connection()
    except Exception as exc:
        LOGGER.exception('Failed to connect to Iceberg/S3')
        sys.exit(1)

    # Run table syncs sequentially or in parallel
    table_sync_excs = utils.run_pool_of_workers(
        sync_table,
        args.tables,
        args,
        parallel=True if cpu_cores > 1 else False
    )

    # Log summary
    end_time = utils.get_bookmark()
    LOGGER.info('FastSync completed')
    LOGGER.info('Total tables: %d', len(args.tables))
    LOGGER.info('Successful: %d', len(args.tables) - len(table_sync_excs))
    LOGGER.info('Failed: %d', len(table_sync_excs))
    LOGGER.info('Runtime: %s', end_time - start_time)

    if len(table_sync_excs) > 0:
        LOGGER.error('Errors during sync:')
        for exc in table_sync_excs:
            LOGGER.error('  %s', exc)
        sys.exit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.exception('FastSync failed')
        sys.exit(1)


if __name__ == '__main__':
    main()
