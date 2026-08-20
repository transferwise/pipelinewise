.. _tap-salesforce:

Salesforce source
=================

``tap-salesforce`` extracts Salesforce objects through the Bulk or REST API.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Load path
   * - Salesforce
     - Experimental
     - Singer only


Prerequisites
-------------

Create a connected app and complete its OAuth flow. The resulting principal must
have API and field access to every selected object. Field-level security can make
discovery or extraction incomplete without producing a database-style permission
error.


Configuration
-------------

.. code-block:: yaml

   id: "salesforce"
   name: "Salesforce"
   type: "tap-salesforce"
   owner: "data-platform@example.com"
   db_conn:
     client_id: "<CLIENT_ID>"
     client_secret: "{{ env_var['SALESFORCE_CLIENT_SECRET'] }}"
     refresh_token: "{{ env_var['SALESFORCE_REFRESH_TOKEN'] }}"
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

``start_date`` is an RFC 3339 timestamp. Set ``api_type`` to ``BULK`` or
``REST``. Discover the objects visible to the connected user before finalising
the table list:

.. code-block:: bash

   pipelinewise discover_tap --tap salesforce --target snowflake

Test API quotas, deleted-record handling, schema changes, and large objects
before production use.


Snowflake Iceberg tables
------------------------

When the destination is ``target-snowflake``, Salesforce can select managed
Iceberg v3 at tap level:

.. code-block:: yaml

   target_table_format: iceberg
   iceberg_version: 3
   hard_delete: true

This route remains Singer-only, including ``FULL_TABLE`` streams; it does not
enable FastSync FullSync or PartialSync. ``hard_delete: true`` is required and is
the default. Salesforce's default ``data_flattening_max_level: 10`` remains
valid. Set the level to ``0`` only when nested values should remain unflattened
and use the explicit-v3 ``VARIANT`` mapping. See :ref:`snowflake_iceberg`.
