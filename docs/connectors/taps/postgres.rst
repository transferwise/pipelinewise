.. _tap-postgres:

PostgreSQL source
=================

``tap-postgres`` extracts tables with full-table, key-based incremental, or
wal2json logical replication.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Bulk transfer
   * - PostgreSQL
     - Available
     - FullSync to PostgreSQL or Snowflake; PartialSync to Snowflake, including
       managed Iceberg v3


Prerequisites
-------------

The runtime user needs ``CONNECT`` on the database, ``USAGE`` on each source
schema, and ``SELECT`` on replicated tables. Grant default privileges if future
tables must be discovered automatically.

LOG_BASED replication also requires:

- a connection to the writable primary;
- ``wal_level=logical`` and sufficient ``max_replication_slots`` and
  ``max_wal_senders`` capacity;
- the `wal2json <https://github.com/eulerto/wal2json>`_ plugin with format
  version 2 support; and
- permission to create and consume a logical replication slot.

PipelineWise creates one slot for the tap database. PostgreSQL retains WAL needed
by that slot, so monitor retained WAL and do not remove the slot while the tap is
active.


Configuration
-------------

.. code-block:: yaml

   id: "orders"
   name: "Orders PostgreSQL"
   type: "tap-postgres"
   owner: "data-platform@example.com"
   db_conn:
     host: "<HOST>"
     port: 5432
     user: "<USER>"
     password: "{{ env_var['POSTGRES_PASSWORD'] }}"
     dbname: "orders"
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   schemas:
     - source_schema: "public"
       target_schema: "repl_orders"
       tables:
         - table_name: "payments"
           replication_method: "LOG_BASED"

.. list-table:: Connector-specific settings
   :header-rows: 1
   :widths: 27 18 18 37
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``replica_host``
     - No
     - Primary host
     - Offloads FastSync reads; logical replication remains on the primary.
   * - ``filter_schemas``
     - No
     - All visible schemas
     - Limits discovery to a comma-separated schema list.
   * - ``max_run_seconds``
     - No
     - ``43200``
     - Stops a logical replication run after this duration.
   * - ``logical_poll_total_seconds``
     - No
     - ``10800``
     - Stops after this total idle polling period.
   * - ``break_at_end_lsn``
     - No
     - ``true``
     - Stops after reaching the WAL boundary captured at startup.
   * - ``ssl``
     - No
     - Connector default
     - Uses PostgreSQL ``sslmode=require`` when enabled.
   * - ``limit``
     - No
     - Unlimited
     - Bounds rows returned by an incremental query.
   * - ``fastsync_parallelism``
     - No
     - CPU count
     - Controls concurrent FastSync table exports.

Common tap settings are documented in :ref:`yaml_configuration`. Generate the
full template with ``pipelinewise init``.

Snowflake Singer, FullSync, and PartialSync can target managed Iceberg v3 with
explicit tap-level configuration. See :ref:`snowflake_iceberg`.
Snowflake Singer loading, FullSync, and PartialSync preserve line breaks, tabs,
CSV punctuation, Unicode, and literal backslash sequences in string values.
PostgreSQL ``hstore`` values map to Snowflake ``VARIANT`` only on that explicit
v3 route; native mappings remain unchanged.


Acknowledgement and recovery
----------------------------

Consuming WAL does not by itself advance the slot's safe flush position.
PipelineWise sends feedback only up to the minimum target-acknowledged LSN stored
in ``state.json``. Missing, unreadable, invalid, or regressing state retains the
previous safe LSN.

Before consuming ongoing LOG_BASED WAL on PostgreSQL 9.6 or later, PipelineWise
attempts to emit a transactional ``pg_logical_emit_message`` in each tap
database when the tap user can execute the function. The transaction's commit
provides a decodable bookmark boundary even when the selected tables are idle.
Slot feedback advances through that boundary only after the target acknowledges
the state.

If the function is unavailable or inaccessible, replication continues using
the captured current-WAL boundary. Fully filtered idle WAL may then require a
later decodable message before the bookmark and acknowledgement can advance.

After an unexpected termination, restart the same tap without advancing state.
Unacknowledged WAL remains replayable while the slot exists. Resync only when the
slot or required WAL is unavailable, and monitor retained WAL during a prolonged
target outage. See :ref:`stream_buffering` and :ref:`troubleshooting`.
