#!/usr/bin/env python3
import multiprocessing
import os
import sys
from argparse import Namespace
from datetime import datetime
from functools import partial
from typing import Union

from ..logger import Logger
from .commons import utils
from .commons.tap_s3_csv import FastSyncTapS3Csv
from .commons.target_bigquery import FastSyncTargetBigquery

LOGGER = Logger().get_logger(__name__)


REQUIRED_CONFIG_KEYS = {
    'tap': [
        'bucket',
        'start_date'
    ],
    'target': [

        'project_id',
    ]
}

LOCK = multiprocessing.Lock()


def tap_type_to_target_type(csv_type):
    """Data type mapping from S3 csv to Bigquery"""

    return {
        'integer': 'INT64',
        'number': 'NUMERIC',
        'string': 'STRING',
        'boolean': 'STRING',  # The guess sometimes can be wrong, we'll use string for now.
        'date': 'STRING',  # The guess sometimes can be wrong, we'll use string for now.

        'date_override': 'TIMESTAMP'  # Column type to use when date_override defined in YAML
    }.get(csv_type, 'STRING')


def sync_table(table_name: str, args: Namespace) -> Union[bool, str]:
    """Sync one table"""
    s3_csv = FastSyncTapS3Csv(args.tap, tap_type_to_target_type, target_quote='`')
    bigquery = FastSyncTargetBigquery(args.target, args.transform)

    try:
        filename = utils.gen_export_filename(tap_id=args.target.get('tap_id'), table=table_name)
        filepath = os.path.join(args.temp_dir, filename)

        target_schema = utils.get_target_schema(args.target, table_name)

        s3_csv.copy_table(table_name, filepath)
        size_bytes = os.path.getsize(filepath)

        bigquery_types = s3_csv.map_column_types_to_target(filepath, table_name)
        bigquery_columns = bigquery_types.get('columns', [])

        # Creating temp table in Bigquery
        bigquery.create_schema(target_schema)
        bigquery.create_table(target_schema,
                               table_name,
                               bigquery_columns,
                               is_temporary=True,
                               sort_columns=True)

        # Load into Bigquery table
        bigquery.copy_to_table(filepath, target_schema, table_name, size_bytes, is_temporary=True, skip_csv_header=True)
        os.remove(filepath)

        # Obfuscate columns
        bigquery.obfuscate_columns(target_schema, table_name)

        # Create target table and swap with the temp table in Bigquery
        bigquery.create_table(target_schema, table_name, bigquery_columns)
        bigquery.swap_tables(target_schema, table_name)

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
        utils.grant_privilege(target_schema, grantees, bigquery.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, bigquery.grant_select_on_schema)

        return True

    except Exception as exc:
        LOGGER.exception(exc)
        return f'{table_name}: {exc}'


def main_impl():
    """Main sync logic"""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    pool_size = utils.get_pool_size(args.tap)
    start_time = datetime.now()

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

    # Start loading tables in parallel in spawning processes by
    # utilising all available Pool size
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
        """, len(args.tables), len(args.tables) - len(table_sync_excs),
                str(table_sync_excs), pool_size, end_time - start_time)

    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
