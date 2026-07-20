.. _tap-salesforce:

Tap Salesforce
--------------

The Salesforce tap extracts Salesforce objects through either the Bulk API or
the REST API. Create a Salesforce connected app and complete its OAuth flow to
obtain a client ID, client secret, and refresh token.

Generate a starting configuration with ``pipelinewise init`` (see
:ref:`generating_pipelines`), then edit ``tap_salesforce.yml``. The
``start_date`` must be an RFC 3339 timestamp and limits how far back the tap
queries records. Set ``api_type`` to either ``BULK`` or ``REST``.

.. code-block:: yaml

    id: "salesforce"
    name: "Salesforce"
    type: "tap-salesforce"
    owner: "data-team@example.com"

    db_conn:
      client_id: "<CLIENT_ID>"
      client_secret: "<CLIENT_SECRET>"
      refresh_token: "<REFRESH_TOKEN>"
      start_date: "2024-01-01T00:00:00Z"
      api_type: "BULK"

    target: "snowflake"
    batch_size_rows: 20000
    stream_buffer_size: 0
    default_target_schema: "salesforce"

    schemas:
      - source_schema: "salesforce"
        target_schema: "salesforce"
        tables:
          - table_name: "Account"
          - table_name: "Contact"
          - table_name: "Opportunity"

The tap discovers most Salesforce objects. Use
``pipelinewise discover_tap --tap salesforce --target snowflake`` to inspect the objects available
to the connected user before finalising the ``tables`` list.
