from unittest.mock import Mock

from tap_mixpanel.schema import get_schema


def test_export_discovery_keeps_dynamic_properties_nested_when_denesting_disabled():
    client = Mock(api_domain="mixpanel.com")

    schema = get_schema(
        client,
        properties_flag=True,
        denest_properties_flag="false",
        stream_name="export",
    )

    assert "properties" in schema["properties"]
    assert schema["additionalProperties"] is False
    assert "Environment" not in schema["properties"]
    assert "environment" not in schema["properties"]
    client.request.assert_not_called()


def test_export_discovery_exposes_dynamic_properties_when_denesting_enabled():
    client = Mock(api_domain="mixpanel.com")
    client.request.return_value = {"screen_name": {}}

    schema = get_schema(
        client,
        properties_flag=False,
        denest_properties_flag="true",
        stream_name="export",
    )

    assert "properties" not in schema["properties"]
    assert schema["properties"]["screen_name"] == {"type": ["null", "string"]}
    client.request.assert_called_once_with(
        method="GET",
        url="https://mixpanel.com/api/2.0",
        path="events/properties/top",
        params={"limit": 2000},
        endpoint="event_properties",
    )
