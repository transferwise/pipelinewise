#!/usr/bin/env python3

import os

import postgres_to_snowflake.utils
from postgres_to_snowflake.postgres import Postgres
from postgres_to_snowflake.snowflake import Snowflake


REQUIRED_CONFIG_KEYS = {
    'postgres': [
        'host',
        'port',
        'user',
        'password'
    ],
    'snowflake': [
        'account',
        'dbname',
        'warehouse',
        'user',
        'password',
        's3_bucket',
        'aws_access_key_id',
        'aws_secret_access_key'
    ]
}


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    postgres = Postgres(args.postgres_config)
    snowflake = Snowflake(args.snowflake_config, args.transform_config)
    table_sync_excs = []

    # Load tables one by one
    for table in args.tables:
        try:
            filename = '{}.csv.gz'.format(table)
            filepath = os.path.join(args.export_dir, filename)

            # Open connection
            postgres.open_connection()

            # Exporting table data and close connection to avoid timeouts for huge tables
            postgres.copy_table(table, filepath)
            postgres.close_connection()

            # Uploading to S3
            s3_key = snowflake.upload_to_s3(filepath, table)
            os.remove(filepath)

            # Creating temp table in Snowflake
            snowflake.create_schema(args.target_schema)
            postgres.open_connection()
            snowflake.query(postgres.snowflake_ddl(table, args.target_schema, True))

            # Load into Snowflake table
            snowflake.copy_to_table(s3_key, args.target_schema, table, True)

            # Obfuscate columns
            snowflake.obfuscate_columns(args.target_schema, table)

            # Create target table in snowflake and swap with temp table
            snowflake.query(postgres.snowflake_ddl(table, args.target_schema, False))
            snowflake.swap_tables(args.target_schema, table)

            postgres.close_connection()

        except Exception as exc:
            table_sync_excs.append(exc)

    # Log summary
    utils.log("""
        -------------------------------------------------------
        SYNC FINISHED - SUMMARY
        -------------------------------------------------------
            Total tables selected to sync  : {}
            Tables loaded successfully     : {}
            Exceptions during table sync   : {}
        -------------------------------------------------------
        """.format(
            len(args.tables),
            len(args.tables) - len(table_sync_excs),
            str(table_sync_excs)
        ))


def main():
    try:
        main_impl()
    except Exception as exc:
        utils.log("CRITICAL: {}".format(exc))
        raise exc

