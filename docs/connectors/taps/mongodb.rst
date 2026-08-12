.. _tap-mongodb:

MongoDB source
==============

``tap-mongodb`` extracts collections with full-table or log-based replication.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Native transfer
   * - MongoDB
     - Experimental
     - FullSync to PostgreSQL or Snowflake; no PartialSync


Prerequisites
-------------

The user needs read access to the source database and authentication database.
LOG_BASED replication requires a replica set and access to its change stream or
oplog. Confirm the source's retention can cover the longest expected outage.


Configuration
-------------

.. code-block:: yaml

   id: "mongodb"
   name: "MongoDB source"
   type: "tap-mongodb"
   owner: "data-platform@example.com"
   db_conn:
     host: "mongo1.example.com,mongo2.example.com"
     auth_database: "admin"
     dbname: "orders"
     username: "<USER>"
     password: "{{ env_var['MONGODB_PASSWORD'] }}"
     replica_set: "rs0"
   target: "snowflake"
   batch_size_rows: 1000
   stream_buffer_size: 0
   schemas:
     - source_schema: "orders"
       target_schema: "repl_orders"
       tables:
         - table_name: "payments"
           replication_method: "LOG_BASED"

.. list-table:: Connector-specific settings
   :header-rows: 1
   :widths: 28 20 20 32
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``auth_database``
     - Yes
     - —
     - Database against which credentials are authenticated.
   * - ``replica_set``
     - For LOG_BASED
     - —
     - Replica set used for change capture.
   * - ``update_buffer_size``
     - No
     - ``1``
     - Buffers detected updates before emitting them.
   * - ``await_time_ms``
     - No
     - ``1000``
     - Controls how long log-based polling waits for changes.
   * - ``fastsync_parallelism``
     - No
     - CPU count
     - Controls concurrent FullSync exports.

MongoDB documents can produce wide or changing schemas. Test nested document
shape, arrays, and schema evolution against the selected target before production
use. Generate the full template with ``pipelinewise init``.
