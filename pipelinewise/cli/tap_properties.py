"""
PipelineWise CLI - Tap property details
"""
import random


def generate_tap_mysql_server_id():
    """
    Generating a server id for a mysql tap that uniquely
    identifies the client. Server ID is required to avoid
    broken connection when multiple taps connecting to
    the same mysql server
    """
    return 900000000 + random.randint(1, 90000000)


def generate_tap_s3_csv_to_table_mappings(tap):
    """
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
    """
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


def generate_tables_list(tap, as_string=False):
    """
    Generating table names from tap YAMLs in <SCHEMA_NAME>.<TABLE_NAME> format.
    Some tap configurations are required to specify list of tables.

    Example output as list:
        "tables": [
            "MY_SCHEMA.MY_TABLE_1",
            "MY_SCHEMA.MY_TABLE_2"
        ]

    Example output as comma separated string that compatible with tap-snowflake tap
        "tables": "MY_SCHEMA.MY_TABLE_1,MY_SCHEMA.MY_TABLE_2"
    """
    tables_list = []

    # Using the input tap YAML we can generate the required config.json
    schemas = tap.get('schemas', []) if tap else None
    if schemas:
        for schema in schemas:
            tables = schema.get('tables', [])
            for table in tables:
                schema_name = schema['source_schema']
                table_name = table['table_name']

                # Append table name with schema prefix
                tables_list.append(f'{schema_name}.{table_name}')

    # Return as comma separated string
    if as_string:
        return ','.join(tables_list)

    # Return as list
    return tables_list


# Taps are implemented by different persons and teams without
# common naming convention and structures in the tap specific
# properties.json.
#
# Since PipelineWise is using common YAML configuration files across
# every supported tap we need to normalise the naming differences to
# implement common functions that work smoothly with every tap.
# i.e. stream selection, transformations, etc.
def get_tap_properties(tap=None, temp_dir=None):
    """
    Returns the full dictionary of every tap properties
    """
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
                # Set tap_id to locate the corresponding replication slot
                'tap_id': tap['id'] if tap else None,
            },
            'tap_stream_id_pattern': '{{schema_name}}-{{table_name}}',
            'tap_stream_name_pattern': '{{schema_name}}-{{table_name}}',
            'tap_catalog_argument': '--properties',
            'default_replication_method': 'LOG_BASED',
            'default_data_flattening_max_level': 0
        },
        'tap-zuora': {
            'tap_config_extras': {
                'username': tap.get('db_conn', {}).get('username') if tap else None,
                'password': tap.get('db_conn', {}).get('password') if tap else None,
                'start_date': tap.get('db_conn', {}).get('start_date') if tap else None,
                'api_type': tap.get('db_conn', {}).get('api_type') if tap else None
            },
            'tap_stream_id_pattern': '{{table_name}}',
            'tap_stream_name_pattern': '{{table_name}}',
            'tap_catalog_argument': '--catalog',
            'default_replication_method': 'FULL_TABLE',
            'default_data_flattening_max_level': 10
        },
        'tap-oracle': {
            'tap_config_extras': {},
            'tap_stream_id_pattern': '{{schema_name}}-{{table_name}}',
            'tap_stream_name_pattern': '{{schema_name}}-{{table_name}}',
            'tap_catalog_argument': '--catalog',
            'default_replication_method': 'LOG_BASED',
            'default_data_flattening_max_level': 0
        },
        'tap-kafka': {
            'tap_config_extras': {
                'local_store_dir': temp_dir,
                'encoding': 'utf-8'
            },
            'tap_stream_id_pattern': '{{table_name}}',
            'tap_stream_name_pattern': '{{table_name}}',
            'tap_catalog_argument': '--properties',
            'default_replication_method': 'LOG_BASED',
            'default_data_flattening_max_level': 0
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
            'tap_config_extras': {
                'user_agent': 'PipelineWise - Tap Jira'
            },
            'tap_stream_id_pattern': '{{table_name}}',
            'tap_stream_name_pattern': '{{table_name}}',
            'tap_catalog_argument': '--properties',
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
                # Adding only the required list of tables to avoid long running discovery mode
                'tables': generate_tables_list(tap, as_string=True)
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
        'tap-mongodb': {
            'tap_config_extras': {
                'database': tap.get('db_conn', {}).get('dbname') if tap else None,
                'include_schemas_in_destination_stream_name': 'true'
            },
            'tap_stream_id_pattern': '{{database_name}}-{{table_name}}',
            'tap_stream_name_pattern': '{{database_name}}-{{table_name}}',
            'tap_catalog_argument': '--catalog',
            'default_replication_method': 'LOG_BASED',
            'default_data_flattening_max_level': 0
        },
        'tap-google-analytics': {
            'tap_config_extras': {},
            'tap_stream_id_pattern': '{{table_name}}',
            'tap_stream_name_pattern': '{{table_name}}',
            'tap_catalog_argument': '--catalog',
            'default_replication_method': 'INCREMENTAL',
            'default_data_flattening_max_level': 0
        },
        'tap-github': {
            'tap_config_extras': {
                # Generate unique server id's to avoid broken connection
                # when multiple taps reading from the same mysql server
                'server_id': generate_tap_mysql_server_id()
            },
            'tap_stream_id_pattern': '{{table_name}}',
            'tap_stream_name_pattern': '{{table_name}}',
            'tap_catalog_argument': '--properties',
            'default_replication_method': 'LOG_BASED',
            'default_data_flattening_max_level': 0
        },
        'tap-shopify': {
            'tap_config_extras': {},
            'tap_stream_id_pattern': '{{table_name}}',
            'tap_stream_name_pattern': '{{table_name}}',
            'tap_catalog_argument': '--catalog',
            'default_replication_method': 'INCREMENTAL',
            'default_data_flattening_max_level': 0
        },
        'tap-slack': {
            'tap_config_extras': {},
            'tap_stream_id_pattern': '{{table_name}}',
            'tap_stream_name_pattern': '{{table_name}}',
            'tap_catalog_argument': '--catalog',
            'default_replication_method': 'LOG_BASED',
            'default_data_flattening_max_level': 0
        },
        'tap-mixpanel': {
            'tap_config_extras': {
                'user_agent': 'PipelineWise - Tap Mixpanel',
                # Do not denest properties by default
                'denest_properties': tap.get('db_conn', {}).get('denest_properties', 'false') if tap else None
            },
            'tap_stream_id_pattern': '{{table_name}}',
            'tap_stream_name_pattern': '{{table_name}}',
            'tap_catalog_argument': '--catalog',
            'default_replication_method': 'LOG_BASED',
            'default_data_flattening_max_level': 0
        },
        'tap-twilio': {
            'tap_config_extras': {},
            'tap_stream_id_pattern': '{{table_name}}',
            'tap_stream_name_pattern': '{{table_name}}',
            'tap_catalog_argument': '--catalog',
            'default_replication_method': 'INCREMENTAL',
            'default_data_flattening_max_level': 0
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
