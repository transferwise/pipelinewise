import sys
import simplejson as json
import singer
from singer import  metadata
import tap_postgres.db as post_db

def should_sync_column(md_map, field_name):
    field_metadata = md_map.get(('properties', field_name), {})
    return singer.should_sync_field(field_metadata.get('inclusion'),
                                    field_metadata.get('selected'),
                                    True)

def write_schema_message(schema_message):
    sys.stdout.write(json.dumps(schema_message, use_decimal=True) + '\n')
    sys.stdout.flush()

def send_schema_message(stream, bookmark_properties):
    s_md = metadata.to_map(stream['metadata'])
    if s_md.get((), {}).get('is-view'):
        key_properties = s_md.get((), {}).get('view-key-properties', [])
    else:
        key_properties = s_md.get((), {}).get('table-key-properties', [])

    schema_message = {'type' : 'SCHEMA',
                      'stream' : post_db.calculate_destination_stream_name(stream, s_md),
                      'schema' : stream['schema'],
                      'key_properties' : key_properties,
                      'bookmark_properties': bookmark_properties}

    write_schema_message(schema_message)
