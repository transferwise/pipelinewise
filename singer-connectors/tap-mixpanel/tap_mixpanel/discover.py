from singer.catalog import Catalog, CatalogEntry, Schema

from tap_mixpanel.schema import get_schemas
from tap_mixpanel.streams import STREAMS


def discover(client, properties_flag):
    """Run the discovery mode, prepare the catalog file and return catalog.

    Args:
        client (MixpanelClient): Client object to make http calls.
        properties_flag (str): Setting this argument to `true` ensures that new properties on
                               events and engage records are captured.

    Returns:
        singer.Catalog: Catalog object having schema and metadata of all the streams.
    """
    schemas, field_metadata = get_schemas(client, properties_flag)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=STREAMS[stream_name].key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
