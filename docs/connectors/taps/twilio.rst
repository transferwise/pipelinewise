.. _tap-twilio:

Twilio source
=============

``tap-twilio`` extracts standard Twilio, TaskRouter, and Programmable Chat
resources.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Load path
   * - Twilio
     - Experimental
     - Singer only


Configuration
-------------

.. code-block:: yaml

   id: "twilio"
   name: "Twilio"
   type: "tap-twilio"
   owner: "data-platform@example.com"
   db_conn:
     account_sid: "<ACCOUNT_SID>"
     auth_token: "{{ env_var['TWILIO_AUTH_TOKEN'] }}"
     start_date: "2024-01-01T00:00:00Z"
     user_agent: "data-platform@example.com"
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

``account_sid`` and ``auth_token`` are required. ``start_date`` is the initial
incremental boundary when no bookmark exists. Some streams are full-table and
can retrieve substantial data on every run; check ``members`` and
``chat_messages`` volume before selecting them.

Use discovery to inspect the full stream list and verify API permissions:

.. code-block:: bash

   pipelinewise discover_tap --tap twilio --target snowflake
