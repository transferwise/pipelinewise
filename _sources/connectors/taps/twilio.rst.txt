.. _tap-twilio:

Tap Twilio
----------

The Twilio tap extracts standard Twilio, TaskRouter, and Programmable Chat
resources. Generate a starting configuration with ``pipelinewise init`` (see
:ref:`generating_pipelines`), then edit ``tap_twilio.yml``.

``account_sid`` and ``auth_token`` are required. ``start_date`` is the default
incremental starting point when a stream has no bookmark. ``user_agent`` is an
optional identifier, such as a team email address, for API request logging.

.. code-block:: yaml

    id: "twilio"
    name: "Twilio"
    type: "tap-twilio"
    owner: "data-team@example.com"

    db_conn:
      account_sid: "<TWILIO_ACCOUNT_SID>"
      auth_token: "<TWILIO_AUTH_TOKEN>"
      start_date: "2024-01-01T00:00:00Z"
      user_agent: "data-team@example.com"

    target: "snowflake"
    batch_size_rows: 20000
    stream_buffer_size: 0
    default_target_schema: "twilio"

    schemas:
      - source_schema: "twilio"
        target_schema: "twilio"
        tables:
          - table_name: "workspaces"
          - table_name: "activities"
          - table_name: "events"
          - table_name: "tasks"

Some streams use ``FULL_TABLE`` internally and may retrieve a large amount of
data on every run. In particular, add ``members`` and ``chat_messages`` only
after checking their expected volume. Run
``pipelinewise discover_tap --tap twilio --target snowflake`` to see the full stream list.
