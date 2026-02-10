import json
import os

import singer
from singer import metadata

from tap_mixpanel.client import MixpanelPaymentRequiredError
from tap_mixpanel.streams import STREAMS

LOGGER = singer.get_logger()


# Reference:
# https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata


def get_abs_path(path):
    """Get the absolute path for the schema files.

    Args:
        path (str): Path from current folder to schema file.

    Returns:
        str: Full path to schema file.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schema(client, properties_flag, stream_name):
    """Creates schema for a stream by loading schema file and appending dynamic
    fields schema if necessary.

    Args:
        client (MixpanelClient): Client to make http calls.
        properties_flag (str): Setting this argument to `true` ensures that new properties on
                               events and engage records are captured.
        stream_name (str): Name of stream whose schema is to create.

    Returns:
        dict: Returns schema of the stream.
    """
    schema_path = get_abs_path(f"schemas/{stream_name}.json")

    with open(schema_path, encoding="utf-8") as file:
        schema = json.load(file)

    # Set whether to allow additional properties for engage and export endpoints
    # Event and Engage properties are dynamic and depend on the properties provided on upload,
    #   when the Event or Engage (user/person) was created.
    # Depending on the tap config parameter select_properties_by_default,
    #   the json schema should allow additional properties (additionalProperties = true).
    if stream_name in ("engage", "export") and str(properties_flag).lower() == "true":
        schema["additionalProperties"] = True
    else:
        schema["additionalProperties"] = False

    if stream_name == "engage":
        properties = client.request(
            method="GET",
            url=f"https://{client.__api_domain}/api/2.0",
            path="engage/properties",
            params={"limit": 2000},
            endpoint="engage_properties",
        )
        if properties.get("status") == "ok":
            results = properties.get("results", {})
            for key, val in results.items():
                if key[0:1] == "$":
                    new_key = f"mp_reserved_{key[1:]}"
                else:
                    new_key = key

                # property_type: string, number, boolean, datetime, object, list
                # Reference:
                # https://help.mixpanel.com/hc/en-us/articles/115004547063-Properties-Supported-Data-Types
                property_type = val.get("type")

                types = {
                    "boolean": {
                        "type": ["null", "boolean"]},
                    "number": {
                        "type": ["null", "string"],
                        "format": "singer.decimal"},
                    "datetime": {
                        "type": ["null", "string"],
                        "format": "date-time"},
                    "object": {
                        "type": ["null", "object"],
                        "additionalProperties": True,
                    },
                    "list": {
                        "type": ["null", "array"],
                        "items": {}},
                    "string": {
                        "type": ["null", "string"]},
                }

                if property_type in types:
                    # Make the types a list containing all types starting with the one returned to us by the API
                    this_type = [types.pop(property_type)]
                    this_type += list(types.values())

                else:
                    this_type = list(types.values())

                schema["properties"][new_key] = {"anyOf": this_type}

    if stream_name == "export":
        # Event properties endpoint:
        #  https://developer.mixpanel.com/docs/data-export-api#section-hr-span-style-font-family-courier-top-span
        results = client.request(
            method="GET",
            url=f"https://{client.__api_domain}/api/2.0",
            path="events/properties/top",
            params={"limit": 2000},
            endpoint="event_properties",
        )
        for key, val in results.items():
            if key[0:1] == "$":
                new_key = f"mp_reserved_{key[1:]}"
            else:
                new_key = key

            # String ONLY for event properties (no other datatypes)
            # Reference: https://help.mixpanel.com/hc/en-us/articles/360001355266-Event-Properties#field-size-character-limits-for-event-properties
            schema["properties"][new_key] = {"type": ["null", "string"]}

    return schema


def get_schemas(client, properties_flag):
    """Load the schema references, prepare metadata for each streams and return
    schema and metadata for the catalog.

    Args:
        client (MixpanelClient): Client object to make http calls.
        properties_flag (bool): Setting this argument to true ensures that new properties on
                                   events and engage records are captured.

    Returns:
        tuple: Returns tuple of Schemas and metadata.
    """
    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():
        # When the client detects disable_engage_endpoint, skip discovering the stream
        if stream_name == "engage" and client.disable_engage_endpoint:
            LOGGER.warning(
                "Mixpanel returned a 402 indicating the Engage endpoint and stream is unavailable. Skipping."
            )
            continue

        try:
            schema = get_schema(client, properties_flag, stream_name)
        except MixpanelPaymentRequiredError:
            LOGGER.warning(
                "Mixpanel returned a 402 from the %s API so %s stream will be skipped.",
                stream_name,
                stream_name,
            )
            continue

        schemas[stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.key_properties,
            valid_replication_keys=stream_metadata.replication_keys,
            replication_method=stream_metadata.replication_method,
        )

        mdata = metadata.to_map(mdata)

        if stream_metadata.replication_keys:
            mdata = metadata.write(
                mdata,
                ("properties",
                 stream_metadata.replication_keys[0]),
                "inclusion",
                "automatic",
            )

        mdata = metadata.to_list(mdata)

        field_metadata[stream_name] = mdata

    return schemas, field_metadata
