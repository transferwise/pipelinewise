from singer import metadata, get_logger
from tap_s3_csv import s3

LOGGER = get_logger()


def discover_streams(config):
    streams = []

    bucket = config.get('bucket')
    file_extension = config.get('file_extension')
    search_prefix = config.get('search_prefix')

    files_list = s3.list_files_in_bucket(bucket, search_prefix, file_extension)
    for s3_object in files_list:
        table_name = s3_object['Key']

        # We only care about files with our defined extension
        if file_extension not in table_name[-5:]:
            continue

        LOGGER.info('Got s3 object: {}'.format(s3_object))
        schema = discover_schema_for_table(config, table_name)
        streams.append({'table_name': table_name,
                        'stream': table_name,
                        'tap_stream_id': table_name,
                        'schema': schema,
                        'metadata': load_metadata(schema)
                        })
    return streams


def discover_schema_for_table(config, table_name):
    sampled_schema = s3.get_sampled_schema_for_table(config, table_name)
    return sampled_schema


def load_metadata(schema):
    mdata = metadata.new()

    mdata = metadata.write(mdata, (), 'table-key-properties', {})

    for field_name in schema.get('properties', {}).keys():
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')
        mdata = metadata.write(mdata, ('properties', field_name), 'selected-by-default', True)
        #mdata = metadata.write(mdata, ('properties', field_name), 'selected', True)

    return metadata.to_list(mdata)
