import random
import logging

logger = logging.getLogger('Pipelinewise CLI')

def generate_tap_mysql_server_id():
    '''
    Generating a server id for a mysql tap that uniquely
    identifies the client. Server ID is required to avoid
    broken connection when multiple taps connecting to
    the same mysql server
    '''
    return 900000000 + random.randint(1, 90000000)


def generate_tap_s3_csv_to_table_mappings(tap):
    '''
    Generating a csv file to table mapping for an s3-csv
    tap. The mapping defines which files needs to be loaded into
    which table and what are the csv characteristics

    Example output that compatible with tap-s3-csv tap:
        "tables": [{
            "table_name": "my_table",
            "search_prefix": "feeds",
            "search_pattern": "export_file_.*.csv",
            "key_properties": ["id"],
            "delimiter": ","
        }]
    '''
    s3_csv_tables = []

    # Using the input tap YAML we can generate the
    # required config.json for the tap-s3-csv
    schemas = tap.get('schemas', []) if tap else None
    if schemas:
        # We take the first schema
        tables = schemas[0].get('tables', [])
        for table in tables:
            csv_to_table_mapping = table.get('s3_csv_mapping', {})
            if csv_to_table_mapping:
                csv_to_table_mapping['table_name'] = table['table_name']
                s3_csv_tables.append(csv_to_table_mapping)

    return s3_csv_tables


# Taps are implemented by different persons and teams without
# common naming convention and structures in the tap specific
# properties.json.
#
# Since PipelineWise is using common YAML configuration files across
# every supported tap we need to normalise the naming differences to
# implement common functions that work smootly with every tap.
# i.e. stream selection, transformations, etc.
def get_tap_properties(tap=None):
    return {
    'tap-mysql': {
        'tap_config_extras': {
            # Generate unique server id's to avoid broken connection
            # when multiple taps reading from the same mysql server
            'server_id': generate_tap_mysql_server_id()
        },
        'tap_stream_id_pattern': '{{schema_name}}-{{table_name}}',
        'tap_stream_name_pattern': '{{schema_name}}-{{table_name}}',
        'tap_catalog_argument': '--properties',
        'default_replication_method': 'LOG_BASED',
        'default_data_flattening_max_level': 0
    },

    'tap-postgres': {
        'tap_config_extras': {
            # PipelineWise doesn't support replicating from multiple
            # databases by one tap but tap-postgres does.
            #
            # To avoid problems of loading two tables with the same name
            # but from differnet dbs we force tap-postgres to filter only
            # the db that's in scope
            'filter_dbs': tap.get('db_conn', {}).get('dbname') if tap else None
        },
        'tap_stream_id_pattern': '{{database_name}}-{{schema_name}}-{{table_name}}',
        'tap_stream_name_pattern': '{{schema_name}}-{{table_name}}',
        'tap_catalog_argument': '--properties',
        'default_replication_method': 'LOG_BASED',
        'default_data_flattening_max_level': 0
    },

    'tap-kafka': {
        'tap_config_extras': {},
        'tap_stream_id_pattern': '{{table_name}}',
        'tap_stream_name_pattern': '{{table_name}}',
        'tap_catalog_argument': '--properties',
        'default_replication_method': 'LOG_BASED',
        'default_data_flattening_max_level': 10
    },

    'tap-zendesk': {
        'tap_config_extras': {},
        'tap_stream_id_pattern': '{{table_name}}',
        'tap_stream_name_pattern': '{{table_name}}',
        'tap_catalog_argument': '--catalog',
        'default_replication_method': 'INCREMENTAL',
        'default_data_flattening_max_level': 10
    },

    'tap-adwords': {
        'tap_config_extras': {},
        'tap_stream_id_pattern': '{{table_name}}',
        'tap_stream_name_pattern': '{{table_name}}',
        'tap_catalog_argument': '--catalog',
        'default_replication_method': 'INCREMENTAL',
        'default_data_flattening_max_level': 0
    },

    'tap-jira': {
        'tap_config_extras': {},
        'tap_stream_id_pattern': '{{table_name}}',
        'tap_stream_name_pattern': '{{table_name}}',
        'tap_catalog_argument': '--catalog',
        'default_replication_method': 'INCREMENTAL',
        'default_data_flattening_max_level': 0
    },

    'tap-s3-csv': {
        'tap_config_extras': {
            'tables': generate_tap_s3_csv_to_table_mappings(tap)
        },
        'tap_stream_id_pattern': '{{table_name}}',
        'tap_stream_name_pattern': '{{table_name}}',
        'tap_catalog_argument': '--properties',
        'default_replication_method': 'INCREMENTAL',
        'default_data_flattening_max_level': 0
    },

    'tap-snowflake': {
        'tap_config_extras': {
            # PipelineWise doesn't support replicating from multiple
            # databases by one tap but tap-postgres does.
            #
            # To avoid problems of loading two tables with the same name
            # but from differnet dbs we force tap-postgres to filter only
            # the db that's in scope
            'filter_dbs': tap.get('db_conn', {}).get('dbname') if tap else None
        },
        'tap_stream_id_pattern': '{{database_name}}-{{schema_name}}-{{table_name}}',
        'tap_stream_name_pattern': '{{schema_name}}-{{table_name}}',
        'tap_catalog_argument': '--properties',
        'default_replication_method': 'INCREMENTAL',
        'default_data_flattening_max_level': 0
    },

    'tap-salesforce': {
        'tap_config_extras': {
            'select_fields_by_default': True
        },
        'tap_stream_id_pattern': '{{table_name}}',
        'tap_stream_name_pattern': '{{table_name}}',
        'tap_catalog_argument': '--properties',
        'default_replication_method': 'INCREMENTAL',
        'default_data_flattening_max_level': 10
    },

    # Default values to use as a fallback method
    'DEFAULT': {
        'tap_config_extras': {},
        'tap_stream_id_pattern': '{{schema_name}}-{{table_name}}',
        'tap_stream_name_pattern': '{{schema_name}}-{{table_name}}',
        'tap_catalog_argument': '--catalog',
        'default_replication_method': 'LOG_BASED',
        'default_data_flattening_max_level': 0
    },
}
