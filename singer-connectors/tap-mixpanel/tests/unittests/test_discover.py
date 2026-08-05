from unittest.mock import Mock, patch

from tap_mixpanel.discover import discover
from tap_mixpanel.streams import Export, STREAMS


@patch.dict(STREAMS, {"export": Export}, clear=True)
@patch(
    "tap_mixpanel.schema.get_schema",
    return_value={
        "type": "object",
        "properties": {
            "mp_reserved_insert_id": {"type": ["null", "string"]},
            "time": {"type": ["null", "integer"]},
        },
    },
)
def test_export_discovery_declares_insert_id_as_key(mock_get_schema):
    client = Mock(disable_engage_endpoint=False)
    catalog = discover(
        client,
        properties_flag=False,
        denest_properties_flag="false",
    )
    export = catalog.get_stream("export")
    root_metadata = next(
        item["metadata"] for item in export.metadata if not item["breadcrumb"]
    )

    assert export.key_properties == ["mp_reserved_insert_id"]
    assert root_metadata["table-key-properties"] == ["mp_reserved_insert_id"]
    mock_get_schema.assert_called_once_with(client, False, "false", "export")
