import logging
import psycopg2
import psycopg2.extras
import json
import gzip
from typing import List

from . import utils

LOGGER = logging.getLogger(__name__)


# pylint: disable=missing-function-docstring,no-self-use,too-many-arguments
class FastSyncTargetPostgres:
    """
    Common functions for fastsync to Postgres
    """

    EXTRACTED_AT_COLUMN = '_SDC_EXTRACTED_AT'
    BATCHED_AT_COLUMN = '_SDC_BATCHED_AT'
    DELETED_AT_COLUMN = '_SDC_DELETED_AT'

    def __init__(self, connection_config, transformation_config=None):
        self.connection_config = connection_config
        self.transformation_config = transformation_config

    def open_connection(self):
        conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
            self.connection_config['host'],
            self.connection_config['dbname'],
            self.connection_config['user'],
            self.connection_config['password'],
            self.connection_config['port']
        )
        
        if 'ssl' in self.connection_config and self.connection_config['ssl'] == 'true':
            conn_string += " sslmode='require'"

        return psycopg2.connect(conn_string)

    def query(self, query, params=None):
        LOGGER.info('Running query: %s', query)
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)

                if cur.rowcount > 0 and cur.description:
                    return cur.fetchall()

                return []

    def create_schema(self, schema):
        sql = 'CREATE SCHEMA IF NOT EXISTS {}'.format(schema)
        self.query(sql)

    def create_schemas(self, tables):
        schemas = utils.get_target_schemas(self.connection_config, tables)
        for schema in schemas:
            self.create_schema(schema)

    def drop_table(self, target_schema, table_name, is_temporary=False):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        sql = 'DROP TABLE IF EXISTS {}."{}"'.format(target_schema, target_table.lower())
        self.query(sql)

    def create_table(self, target_schema: str, table_name: str, columns: List[str], primary_key: List[str],
                     is_temporary: bool = False, sort_columns=False):

        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        # skip the EXTRACTED, BATCHED and DELETED columns in case they exist because they gonna be added later
        columns = [c for c in columns if not (c.startswith(self.EXTRACTED_AT_COLUMN) or
                                              c.startswith(self.BATCHED_AT_COLUMN) or
                                              c.startswith(self.DELETED_AT_COLUMN))]

        columns += [f'{self.EXTRACTED_AT_COLUMN} TIMESTAMP WITHOUT TIME ZONE',
                    f'{self.BATCHED_AT_COLUMN} TIMESTAMP WITHOUT TIME ZONE',
                    f'{self.DELETED_AT_COLUMN} CHARACTER VARYING']

        # We need the sort the columns for some taps( for now tap-s3-csv)
        # because later on when copying a csv file into Snowflake
        # the csv file columns need to be in the same order as the the target table that will be created below
        if sort_columns:
            columns.sort()

        sql_columns = ','.join(columns).lower()
        sql_primary_keys = ','.join(primary_key).lower() if primary_key else None
        sql = f'CREATE TABLE IF NOT EXISTS {target_schema}."{target_table.lower()}" (' \
              f'{sql_columns}' \
              f'{f", PRIMARY KEY ({sql_primary_keys}))" if primary_key else ")"}'

        self.query(sql)

    def copy_to_table(self, filepath, target_schema: str, table_name: str, size_bytes: int,
                      is_temporary: bool = False, skip_csv_header: bool = False):
        LOGGER.info('Loading %s into Postgres...', filepath)
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                inserts = 0

                copy_sql = f"""COPY {target_schema}."{target_table.lower()}"
                FROM STDIN WITH (FORMAT CSV, HEADER {'TRUE' if skip_csv_header else 'FALSE'}, ESCAPE '"')
                """

                with gzip.open(filepath, 'rb') as file:
                    cur.copy_expert(copy_sql, file)

                inserts = cur.rowcount
                LOGGER.info('Loading into %s."%s": %s',
                            target_schema,
                            target_table.lower(),
                            json.dumps({'inserts': inserts, 'updates': 0, 'size_bytes': size_bytes}))

    # grant_... functions are common functions called by utils.py: grant_privilege function
    # "to_group" is not used here but exists for compatibility reasons with other database types
    # "to_group" is for databases that can grant to users and groups separately like Amazon Redshift
    # pylint: disable=unused-argument
    def grant_select_on_table(self, target_schema, table_name, role, is_temporary, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
            sql = 'GRANT SELECT ON {}."{}" TO GROUP {}'.format(target_schema, target_table.lower(), role)
            self.query(sql)

    # pylint: disable=unused-argument
    def grant_usage_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT USAGE ON SCHEMA {} TO GROUP {}'.format(target_schema, role)
            self.query(sql)

    # pylint: disable=unused-argument
    def grant_select_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT SELECT ON ALL TABLES IN SCHEMA {} TO GROUP {}'.format(target_schema, role)
            self.query(sql)

    # pylint: disable=duplicate-string-formatting-argument
    def obfuscate_columns(self, target_schema: str, table_name: str, is_temporary: bool = False):
        LOGGER.info('Applying obfuscation rules')
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
        transformations = self.transformation_config.get('transformations', [])
        trans_cols = []

        # Find obfuscation rule for the current table
        for trans in transformations:
            # Input table_name is formatted as {{schema}}.{{table}}
            # Stream name in taps transformation.json is formatted as {{schema}}-{{table}}
            #
            # We need to convert to the same format to find the transformation
            # has that has to be applied
            tap_stream_name_by_table_name = '{}-{}'.format(table_dict.get('schema_name'), table_dict.get('table_name'))
            if trans.get('tap_stream_name') == tap_stream_name_by_table_name:
                column = trans.get('field_id')
                transform_type = trans.get('type')
                if transform_type == 'SET-NULL':
                    trans_cols.append('"{}" = NULL'.format(column))
                elif transform_type == 'HASH':
                    trans_cols.append('"{}" = ENCODE(DIGEST("{}", \'sha1\'), \'hex\')'.format(column, column))
                elif 'HASH-SKIP-FIRST' in transform_type:
                    skip_first_n = transform_type[-1]
                    trans_cols.append('"{}" = CONCAT(SUBSTRING("{}", 1, {}),'
                                      'ENCODE(DIGEST(SUBSTRING("{}", {} + 1), \'sha1\'), \'hex\'))'
                                      .format(column,
                                              column,
                                              skip_first_n,
                                              column,
                                              skip_first_n))
                elif transform_type == 'MASK-DATE':
                    trans_cols.append('"{}" = TO_CHAR("{}"::DATE, \'YYYY-01-01\')::DATE'.format(column, column))
                elif transform_type == 'MASK-NUMBER':
                    trans_cols.append('"{}" = 0'.format(column))

        # Generate and run UPDATE if at least one obfuscation rule found
        if len(trans_cols) > 0:
            sql = f"""UPDATE {target_schema}."{target_table.lower()}"
            SET {','.join(trans_cols)}
            """

            self.query(sql)

    def swap_tables(self, schema, table_name):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name')
        temp_table = table_dict.get('temp_table_name')

        # Swap tables and drop the temp tamp
        self.query('DROP TABLE IF EXISTS {}."{}"'.format(schema, target_table.lower()))
        self.query('ALTER TABLE {}."{}" RENAME TO "{}"'.format(schema, temp_table.lower(), target_table.lower()))
