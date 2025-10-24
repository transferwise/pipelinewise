#!/usr/bin/env python3
"""
FastSync from MySQL to Apache Iceberg on S3
"""
import logging
import os
import sys
from argparse import Namespace
from typing import List

from .commons import utils
from .commons.tap_mysql import FastSyncTapMySql
from .commons.target_iceberg import FastSyncTargetIceberg

LOGGER = logging.getLogger(__name__)

REQUIRED_CONFIG_KEYS = {
    'tap': [
        'host',
        'port',
        'user',
        'password'
    ],
    'target': [
        'aws_access_key_id',
        'aws_secret_access_key',
        's3_bucket',
        'glue_catalog_id'
    ]
}

LOCK = utils.Lock()


def tap_type_to_target_type(mysql_type: str, mysql_column_type: str) -> str:
    """
    Map MySQL data types to Iceberg compatible types
    """
    # Numeric types
    if mysql_type in ('int', 'tinyint', 'smallint', 'mediumint', 'bigint'):
        return 'BIGINT'
    elif mysql_type in ('decimal', 'numeric'):
        return 'NUMERIC'
    elif mysql_type in ('float', 'double', 'real'):
        return 'DOUBLE'

    # String types
    elif mysql_type in ('char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext'):
        return 'STRING'
    elif mysql_type in ('binary', 'varbinary', 'blob', 'tinyblob', 'mediumblob', 'longblob'):
        return 'BINARY'
    elif mysql_type in ('enum', 'set'):
        return 'STRING'

    # Date and time types
    elif mysql_type == 'date':
        return 'DATE'
    elif mysql_type in ('datetime', 'timestamp'):
        return 'TIMESTAMP'
    elif mysql_type in ('time', 'year'):
        return 'STRING'

    # Boolean
    elif mysql_type == 'bit':
        return 'BOOLEAN'

    # JSON
    elif mysql_type == 'json':
        return 'STRING'

    # Default to string
    else:
        return 'STRING'


def sync_table(table: str, args: Namespace) -> None:
    """
    Sync a single table from MySQL to Iceberg
    """
    mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)
    iceberg = FastSyncTargetIceberg(args.target, args.transform)

    try:
        filename = 'pipelinewise_fastsync_{}_{}_{}.csv.gz'.format(
            args.tap.get('dbname', args.target.get('default_target_schema', 'default')),
            table,
            utils.get_timestamp()
        )
        filepath = f'{args.temp_dir}/{filename}'
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection and get binlog position
        mysql.open_connection()

        # Get bookmark - MySQL log_file and log_pos
        bookmark = utils.get_bookmark_for_table(
            table, args.properties, mysql, dbname=args.tap.get('dbname')
        )

        # Exporting table data, get table definitions and close connection
        mysql.copy_table(table, filepath)
        size_bytes = os.path.getsize(filepath)
        iceberg_types = mysql.map_column_types_to_target(table)
        iceberg_columns = iceberg_types.get('columns', [])
        primary_key = iceberg_types.get('primary_key')

        mysql.close_connection()

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
    LOGGER.info('Starting MySQL to Iceberg FastSync')
    LOGGER.info('Tap schema: %s', args.tap.get('dbname'))
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
