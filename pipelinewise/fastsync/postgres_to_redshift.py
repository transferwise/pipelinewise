#!/usr/bin/env python3

import os
import sys
import time

import multiprocessing

from datetime import datetime, timedelta
from .commons import utils
from .commons.tap_postgres import FastSyncTapPostgres
from .commons.target_redshift import FastSyncTargetRedshift


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
        'password',
        'dbname',
        'aws_access_key_id',
        'aws_secret_access_key',
        's3_bucket'
    ]
}

DEFAULT_VARCHAR_LENGTH = 10000
SHORT_VARCHAR_LENGTH = 256
LONG_VARCHAR_LENGTH = 65535

lock = multiprocessing.Lock()


def tap_type_to_target_type(pg_type):
    """Data type mapping from MySQL to Redshift"""
    return {
        'char':'CHARACTER VARYING({})'.format(DEFAULT_VARCHAR_LENGTH),
        'character':'CHARACTER VARYING({})'.format(DEFAULT_VARCHAR_LENGTH),
        'varchar':'CHARACTER VARYING({})'.format(DEFAULT_VARCHAR_LENGTH),
        'character varying':'CHARACTER VARYING({})'.format(DEFAULT_VARCHAR_LENGTH),
        'text':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'bit': 'BOOLEAN',
        'varbit':'NUMERIC NULL',
        'bit varying':'NUMERIC NULL',
        'smallint':'NUMERIC NULL',
        'int':'NUMERIC NULL',
        'integer':'NUMERIC NULL',
        'bigint':'NUMERIC NULL',
        'smallserial':'NUMERIC NULL',
        'serial':'NUMERIC NULL',
        'bigserial':'NUMERIC NULL',
        'numeric':'FLOAT',
        'double precision':'FLOAT',
        'real':'FLOAT',
        'bool':'BOOLEAN',
        'boolean':'BOOLEAN',
        'date':'TIMESTAMP WITHOUT TIME ZONE',
        'timestamp':'TIMESTAMP WITHOUT TIME ZONE',
        'timestamp without time zone':'TIMESTAMP WITHOUT TIME ZONE',
        'timestamp with time zone':'TIMESTAMP WITH TIME ZONE',
        'time':'CHARACTER VARYING({})'.format(SHORT_VARCHAR_LENGTH),
        'time without time zone':'CHARACTER VARYING({})'.format(SHORT_VARCHAR_LENGTH),
        'time with time zone':'CHARACTER VARYING({})'.format(SHORT_VARCHAR_LENGTH),
        'ARRAY':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),  # This is all uppercase, because postgres stores it in this format in information_schema.columns.data_type
        'json':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
        'jsonb':'CHARACTER VARYING({})'.format(LONG_VARCHAR_LENGTH),
    }.get(pg_type, 'CHARACTER VARYING({})'.format(DEFAULT_VARCHAR_LENGTH))


def sync_table(table):
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    postgres = FastSyncTapPostgres(args.tap, tap_type_to_target_type)
    redshift = FastSyncTargetRedshift(args.target, args.transform)

    try:
        dbname = args.tap.get("dbname")
        filename = "pipelinewise_fastsync_{}_{}_{}.csv.gz".format(dbname, table, time.strftime("%Y%m%d-%H%M%S"))
        filepath = os.path.join(args.temp_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection
        postgres.open_connection()

        # Get bookmark - LSN position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(table, args.properties, postgres, dbname=dbname)

        # Exporting table data, get table definitions and close connection to avoid timeouts
        postgres.copy_table(table, filepath)
        redshift_types = postgres.map_column_types_to_target(table)
        redshift_columns = redshift_types.get("columns", [])
        primary_key = redshift_types.get("primary_key")
        postgres.close_connection()

        # Uploading to S3
        s3_key = redshift.upload_to_s3(filepath, table)
        os.remove(filepath)

        # Creating temp table in Redshift
        redshift.drop_table(target_schema, table, is_temporary=True)
        redshift.create_table(target_schema, table, redshift_columns, primary_key, is_temporary=True)

        # Load into Redshift table
        redshift.copy_to_table(s3_key, target_schema, table, is_temporary=True)

        # Obfuscate columns
        redshift.obfuscate_columns(target_schema, table)

        # Create target table and swap with the temp table in Redshift
        redshift.swap_tables(target_schema, table)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        lock.acquire()
        try:
            utils.save_state_file(args.state, table, bookmark, dbname=dbname)
        finally:
            lock.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table)
        utils.grant_privilege(target_schema, grantees, redshift.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, redshift.grant_select_on_schema)

    except Exception as exc:
        utils.log("CRITICAL: {}".format(exc))
        return "{}: {}".format(table, exc)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    cpu_cores = utils.get_cpu_cores()
    start_time = datetime.now()
    table_sync_excs = []

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

    # Create target schemas sequentially, Redshift doesn't like it running in parallel
    redshift = FastSyncTargetRedshift(args.target, args.transform)
    for target_schema in utils.get_target_schemas(args.target, args.tables):
        redshift.create_schema(target_schema)

    # Start loading tables in parallel in spawning processes by
    # utilising all available CPU cores
    with multiprocessing.Pool(cpu_cores) as p:
        table_sync_excs = list(filter(None, p.map(sync_table, args.tables)))

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
