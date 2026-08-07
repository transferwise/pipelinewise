.. _tap-zendesk:

Zendesk source
==============

``tap-zendesk`` extracts ticket, user, organisation, and reference data through
the Zendesk API.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Load path
   * - Zendesk
     - Experimental
     - Singer only


Configuration
-------------

.. code-block:: yaml

   id: "zendesk"
   name: "Zendesk"
   type: "tap-zendesk"
   owner: "data-platform@example.com"
   db_conn:
     access_token: "{{ env_var['ZENDESK_TOKEN'] }}"
     subdomain: "example"
     start_date: "2024-01-01T00:00:00Z"
     rate_limit: 1000
     max_workers: 10
     batch_size: 50
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   default_target_schema: "zendesk"
   schemas:
     - source_schema: "zendesk"
       target_schema: "zendesk"
       tables:
         - table_name: "tickets"
         - table_name: "ticket_audits"
         - table_name: "ticket_comments"
         - table_name: "users"

``start_date`` excludes older data when a stream has no bookmark.
``rate_limit``, ``max_workers``, and ``batch_size`` trade run time for API load;
increase them only after observing Zendesk throttling and target throughput.

Discover the full stream catalog and test deleted records, custom fields,
permissions, and retry duplication before production use.
