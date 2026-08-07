.. _tap-snowflake:

Snowflake source
================

``tap-snowflake`` extracts Snowflake tables through the standard Singer path.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Replication
   * - Snowflake
     - Experimental
     - ``FULL_TABLE`` and ``INCREMENTAL``; no FastSync


Prerequisites
-------------

The Snowflake role needs warehouse usage, database and schema usage, and
``SELECT`` on every replicated table. Choose a warehouse with enough capacity
for the configured query cadence.


Configuration
-------------

.. code-block:: yaml

   id: "snowflake_source"
   name: "Operational Snowflake"
   type: "tap-snowflake"
   owner: "data-platform@example.com"
   db_conn:
     account: "<ACCOUNT>"
     dbname: "<DATABASE>"
     user: "<USER>"
     password: "{{ env_var['SNOWFLAKE_PASSWORD'] }}"
     warehouse: "<WAREHOUSE>"
   target: "postgres_dwh"
   batch_size_rows: 20000
   stream_buffer_size: 0
   schemas:
     - source_schema: "PUBLIC"
       target_schema: "repl_snowflake"
       tables:
         - table_name: "ORDERS"
           replication_method: "INCREMENTAL"
           replication_key: "UPDATED_AT"

``INCREMENTAL`` requires a stable key whose new value is greater than or equal
to the saved bookmark. It does not capture deletes. Use ``FULL_TABLE`` when no
suitable key exists and account for the cost of scanning the entire source table
on every run.

Common settings are documented in :ref:`yaml_configuration`; generate the full
template with ``pipelinewise init``.
