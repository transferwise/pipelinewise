import json
from singer import metadata
import pdb

def default_streams(kafka_config):
    if 'schema' in kafka_config and kafka_config['schema']:
        schema = json.loads(kafka_config['schema'])
    else:
        schema = {}

    if 'primary_keys' in kafka_config and kafka_config['primary_keys']:
        pks = json.loads(kafka_config['primary_keys'])
    else:
        pks = []

    mdata = {}
    metadata.write(mdata, (), 'table-key-properties', pks)

    return [{'tap_stream_id': kafka_config['topic'], 'metadata' : metadata.to_list(mdata), 'schema' : schema}]
