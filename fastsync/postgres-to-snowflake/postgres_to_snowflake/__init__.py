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
    snowflake = Snowflake(args.snowflake_config)

    postgres.open_connection()

    for table in args.tables:
        filename = '{}.csv.gz'.format(table)
        filepath = os.path.join(args.export_dir, filename)

        print("Exporting {} into {}...".format(table, filepath))
        postgres.copy_table(table, filepath)

        print("Uploading to S3...")
        s3_key = snowflake.upload_to_s3(filepath, table)
        os.remove(filepath)

        print("Creating target table in Snowflake...")
        snowflake_ddl = postgres.snowflake_ddl(table, args.target_schema, False)
        snowflake.query(snowflake_ddl)
        snowflake_ddl = postgres.snowflake_ddl(table, args.target_schema, True)
        snowflake.query(snowflake_ddl)

        print("Load into Snowflake table...")
        snowflake.copy_to_table(s3_key, args.target_schema, table, True)

        print("Swap tables")
        snowflake.swap_tables(args.target_schema, table)


def main():
    try:
        main_impl()
    except Exception as exc:
        print("CRITICAL: {}".format(exc))
        raise exc

