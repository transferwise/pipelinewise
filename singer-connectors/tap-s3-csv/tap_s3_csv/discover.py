from singer import metadata
from tap_s3_csv import s3


def discover_streams(config):
    streams = []

    # TODO! i want to access tables
    for table_spec in config['properties']['tables']:
        schema = discover_schema(config, table_spec)
        streams.append({'stream': table_spec['table_name'], 'tap_stream_id': table_spec['table_name'], 'schema': schema, 'metadata': load_metadata(table_spec, schema)})
    return streams


def discover_schema(config, table_spec):
    sampled_schema = s3.get_sampled_schema_for_table(config, table_spec)
    return sampled_schema


def load_metadata(table_spec, schema):
    mdata = metadata.new()

    mdata = metadata.write(mdata, (), 'table-key-properties', table_spec['key_properties'])

    for field_name in schema.get('properties', {}).keys():
        if table_spec.get('key_properties', []) and field_name in table_spec.get('key_properties', []):
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)
