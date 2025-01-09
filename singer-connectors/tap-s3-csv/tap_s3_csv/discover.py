"""
Discovery mode is connecting to the data source and collecting information that is required for running the tap.
"""
from typing import List, Dict

from singer import metadata
from tap_s3_csv import s3


def discover_streams(config: Dict)-> List[Dict]:
    """
    Run discovery mode for every stream in the tap configuration
    :param config: connection and streams configuration
    :return: list of information  about every stream
    """
    streams = []

    for table_spec in config['tables']:
        schema = discover_schema(config, table_spec)
        streams.append({'stream': table_spec['table_name'],
                        'tap_stream_id': table_spec['table_name'],
                        'schema': schema,
                        'metadata': load_metadata(table_spec, schema)
                        })
    return streams


def discover_schema(config: Dict, table_spec: Dict) -> Dict:
    """
    Detects the json schema of the given table/stream
    :param config: connection and streams configuration
    :param table_spec: table specs
    :return: detected schema
    """
    sampled_schema = s3.get_sampled_schema_for_table(config, table_spec)

    # Raise an exception if schema cannot sampled. Empty schema will fail and target side anyways
    if not sampled_schema:
        raise ValueError(f"{table_spec.get('search_prefix', '')} - {table_spec.get('search_pattern', '')}"
            "file(s) has no data and cannot analyse the content to generate the required schema.")

    return sampled_schema


def load_metadata(table_spec: Dict, schema: Dict)-> List:
    """
    Creates metadata for the given stream using its specs and schema
    :param table_spec: stream/table specs
    :param schema: stream's json schema
    :return: metadata as a list
    """
    mdata = metadata.new()

    mdata = metadata.write(mdata, (), 'table-key-properties', table_spec.get('key_properties', []))

    for field_name in schema.get('properties', {}).keys():
        if table_spec.get('key_properties', []) and field_name in table_spec.get('key_properties', []):
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)
