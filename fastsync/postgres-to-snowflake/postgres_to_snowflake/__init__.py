#!/usr/bin/env python3

import os

import postgres_to_snowflake.utils
from postgres_to_snowflake.postgres import Postgres


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

    postgres.open_connection()

    for table in args.tables:
        filename = '{}.csv.gz'.format(table)
        filepath = os.path.join(args.export_dir, filename)
        print("Exporting {} into {}...".format(table, filepath))
        postgres.copy_table(table, filepath)

        print("Genarating Snowflake DDL...")
        snowflake_ddl = postgres.snowflake_ddl(table, args.target_schema)
        print(snowflake_ddl)


def main():
    try:
        main_impl()
    except Exception as exc:
        print("CRITICAL: {}".format(exc))
        raise exc

