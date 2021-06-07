import logging
import json
import re
from typing import List, Dict

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.api_core import exceptions

from .transform_utils import TransformationHelper, SQLFlavor
from . import utils
utils.QUOTE_CHARACTER = '`'

LOGGER = logging.getLogger(__name__)

# tone down snowflake connector logging level.
logging.getLogger('bigquery.connector').setLevel(logging.WARNING)


def safe_name(name, quotes=True):
    name = name.replace('`', '')
    pattern = '[^a-zA-Z0-9]'
    removed_bad_chars = re.sub(pattern, '_', name).lower()
    if quotes:
        return '`{}`'.format(removed_bad_chars)
    return removed_bad_chars

class FastSyncTargetBigquery:
    def __init__(self, connection_config, transformation_config=None):
        self.connection_config = connection_config
        self.transformation_config = transformation_config

    def open_connection(self):
        project_id = self.connection_config['project_id']
        return bigquery.Client(project=project_id)

    def query(self, query, params=[]):
        def to_query_parameter(value):
            if isinstance(value, int):
                value_type = "INT64"
            elif isinstance(value, float):
                value_type = "NUMERIC"
            #TODO: repeated float here and in target
            elif isinstance(value, float):
                value_type = "FLOAT64"
            elif isinstance(value, bool):
                value_type = "BOOL"
            else:
                value_type = "STRING"
            return bigquery.ScalarQueryParameter(None, value_type, value)

        job_config = bigquery.QueryJobConfig()
        query_params = [to_query_parameter(p) for p in params]
        job_config.query_parameters = query_params

        queries = []
        if type(query) is list:
            queries.extend(query)
        else:
            queries = [query]

        client = self.open_connection()
        LOGGER.info("TARGET_BIGQUERY - Running query: {}".format(query))
        query_job = client.query(';\n'.join(queries), job_config=job_config)
        query_job.result()

        return query_job

    def create_schema(self, schema_name):
        temp_schema = self.connection_config.get('temp_schema', schema_name)

        client = self.open_connection()
        for schema in set([schema_name, temp_schema]):
            datasets = client.list_datasets()
            dataset_ids = [d.dataset_id.lower() for d in datasets]
            
            if schema.lower() not in dataset_ids:
                LOGGER.info("Schema '{}' does not exist. Creating...".format(schema))
                dataset = client.create_dataset(schema, exists_ok=True)

    def drop_table(self, target_schema, table_name, is_temporary=False):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = safe_name(table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name'))

        sql = "DROP TABLE IF EXISTS {}.{}".format(target_schema, target_table.lower())
        self.query(sql)

    def create_table(self, target_schema: str, table_name: str, columns: List[str],
                     is_temporary: bool = False, sort_columns=False):

        table_dict = utils.tablename_to_dict(table_name)
        target_table = safe_name(table_dict.get('table_name' if not is_temporary else 'temp_table_name').lower())

        # skip the EXTRACTED, BATCHED and DELETED columns in case they exist because they gonna be added later
        columns = [c for c in columns if not (
                                              c.upper().startswith(utils.SDC_EXTRACTED_AT.upper()) or
                                              c.upper().startswith(utils.SDC_BATCHED_AT.upper()) or
                                              c.upper().startswith(utils.SDC_DELETED_AT.upper()))]

        columns += [f'{utils.SDC_EXTRACTED_AT} TIMESTAMP',
                    f'{utils.SDC_BATCHED_AT} TIMESTAMP',
                    f'{utils.SDC_DELETED_AT} TIMESTAMP'
                    ]

        # We need the sort the columns for some taps( for now tap-s3-csv)
        # because later on when copying a csv file into Snowflake
        # the csv file columns need to be in the same order as the the target table that will be created below
        if sort_columns:
            columns.sort()

        columns = [c.lower() for c in columns]

        sql = f'CREATE OR REPLACE TABLE {target_schema}.{target_table} (' \
              f'{",".join(columns)})'

        self.query(sql)

    def copy_to_table(self, filepath, target_schema, table_name, size_bytes, is_temporary, skip_csv_header=False, allow_quoted_newlines=True, write_truncate=True):
        LOGGER.info("BIGQUERY - Loading {} into Bigquery...".format(filepath))
        table_dict = utils.tablename_to_dict(table_name)
        target_table = safe_name(table_dict.get('table_name' if not is_temporary else 'temp_table_name').lower(),
                                 quotes=False)

        client = self.open_connection()
        dataset_ref = client.dataset(target_schema)
        table_ref = dataset_ref.table(target_table)
        table_schema = client.get_table(table_ref).schema
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.schema = table_schema
        job_config.write_disposition = 'WRITE_TRUNCATE' if write_truncate else 'WRITE_APPEND'
        job_config.allow_quoted_newlines = allow_quoted_newlines
        job_config.skip_leading_rows = 1 if skip_csv_header else 0
        with open(filepath, 'rb') as exported_data:
            job = client.load_table_from_file(exported_data, table_ref, job_config=job_config)
        try:
            job.result()
        except exceptions.BadRequest as e:
            for error_row in job.errors:
                LOGGER.critical('ERROR: {}'.format(error_row['message']))
            raise e

        LOGGER.info('Job {}'.format(job))
        LOGGER.info('Job.output_rows {}'.format(job.output_rows))
        inserts = job.output_rows
        LOGGER.info('Loading into %s."%s": %s',
                    target_schema,
                    target_table,
                    json.dumps({'inserts': inserts, 'updates': 0, 'size_bytes': size_bytes}))

        LOGGER.info(job.errors)

    # grant_... functions are common functions called by utils.py: grant_privilege function
    # "to_group" is not used here but exists for compatibility reasons with other database types
    # "to_group" is for databases that can grant to users and groups separately like Amazon Redshift
    def grant_select_on_table(self, target_schema, table_name, role, is_temporary, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = safe_name(table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name'))
            sql = "GRANT SELECT ON {}.{} TO ROLE {}".format(target_schema, target_table, role)
            self.query(sql)

    def grant_usage_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = "GRANT USAGE ON SCHEMA {} TO ROLE {}".format(target_schema, role)
            self.query(sql)

    def grant_select_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ROLE {}".format(target_schema, role)
            self.query(sql)

    def obfuscate_columns(self, target_schema: str, table_name: str):
        """
        Apply any configured transformations to the given table
        Args:
            target_schema: target schema name
            table_name: table name
        """
        LOGGER.info('Starting obfuscation rules...')

        table_dict = utils.tablename_to_dict(table_name)
        temp_table = table_dict.get('temp_table_name')
        transformations = self.transformation_config.get('transformations', [])

        # Input table_name is formatted as {{schema}}.{{table}}
        # Stream name in taps transformation.json is formatted as {{schema}}-{{table}}
        #
        # We need to convert to the same format to find the transformation
        # has that has to be applied
        tap_stream_name_by_table_name = '{}-{}'.format(table_dict['schema_name'], table_dict['table_name']) \
            if table_dict['schema_name'] is not None else table_dict['table_name']

        # Find obfuscation rules for the current table
        # trans_map = self.__get_stream_transformation_map(tap_stream_name_by_table_name, transformations)
        trans_map = TransformationHelper.get_trans_in_sql_flavor(
            tap_stream_name_by_table_name,
            transformations,
            SQLFlavor('bigquery'))

        self.__apply_transformations(trans_map, target_schema, temp_table)

        LOGGER.info('Obfuscation rules applied.')


    def swap_tables(self, schema, table_name):
        project_id = self.connection_config['project_id']
        table_dict = utils.tablename_to_dict(table_name)
        target_table = safe_name(table_dict.get('table_name').lower(), quotes=False)
        temp_table = safe_name(table_dict.get('temp_table_name').lower(), quotes=False)

        # Swap tables and drop the temp table
        table_id = '{}.{}.{}'.format(project_id, schema, target_table)
        temp_table_id = '{}.{}.{}'.format(project_id, schema, temp_table)

        # we cant swap tables in bigquery, so we copy the temp into the table
        # then delete the temp table
        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        client = self.open_connection()
        replace_job = client.copy_table(temp_table_id, table_id, job_config=job_config)
        replace_job.result()

        # delete the temp table
        client.delete_table(temp_table_id)

    def __apply_transformations(self, transformations: List[Dict], target_schema: str, table_name: str) -> None:
        """
        Generate and execute the SQL queries based on the given transformations.
        Args:
            transformations: List of dictionaries in the form {"trans": "", conditions: "... AND ..."}
            target_schema: name of the target schema where the table lives
            table_name: the table name on which we want to apply the transformations
        """
        full_qual_table_name = '{}.{}'.format(safe_name(target_schema), safe_name(table_name))

        if transformations:
            all_cols_update_sql = ''

            # Conditional transformations will have to be executed one at time separately

            for trans_item in transformations:

                # If we have conditions, then we need to construct the query and execute it to transform the
                # single column conditionally
                if trans_item['conditions']:
                    sql = f'UPDATE {full_qual_table_name} ' \
                          f'SET {trans_item["trans"]} WHERE {trans_item["conditions"]};'

                    self.query(sql)

                # Otherwise, we can add this column to a general UPDATE query with no predicates
                else:

                    # if the variable is empty, then initialize it otherwise append the
                    # current transformation to it
                    if not all_cols_update_sql:
                        all_cols_update_sql = trans_item['trans']
                    else:
                        all_cols_update_sql = f'{all_cols_update_sql}, {trans_item["trans"]}'

            # If we have some non-conditional transformations then construct and execute a query
            if all_cols_update_sql:
                all_cols_update_sql = f'UPDATE {full_qual_table_name} SET {all_cols_update_sql} WHERE true;'

                self.query(all_cols_update_sql)
