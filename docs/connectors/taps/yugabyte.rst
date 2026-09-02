.. _tap-yugabyte:

YugabyteDB source
=================

``tap-yugabyte`` extracts tables with full-table, key-based incremental, or
wal2json logical replication, speaking YugabyteDB's PostgreSQL-compatible YSQL
wire protocol.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Bulk transfer
   * - YugabyteDB
     - Experimental
     - FullSync to PostgreSQL or Snowflake; no PartialSync


Prerequisites
-------------

YSQL speaks the PostgreSQL wire protocol on port **5433**, not 5432. Pointing the
tap at 5432 usually reaches a co-located PostgreSQL instance instead of the
cluster.

The runtime user needs ``CONNECT`` on the database, ``USAGE`` on each source
schema, and ``SELECT`` on replicated tables. Discovery reads
``information_schema``, which is privilege-filtered: a table the user cannot
read is silently absent from the catalog rather than reported as an error.

LOG_BASED replication also requires:

- the `wal2json <https://github.com/eulerto/wal2json>`_ output plugin, which
  ships pre-packaged with YugabyteDB; and
- permission to create and consume a logical replication slot.

FastSync owns the replication slot's full lifecycle: it creates a
``wal2json`` slot with LSN type ``HYBRID_TIME`` on first sync and drops it
when the tap is removed, retrying a transient "slot is active" error for up
to five minutes to tolerate YugabyteDB's post-disconnect active-slot window.
Slot names follow ``pipelinewise_<dbname>_<tap_id>``. YugabyteDB LSNs are
HybridTime values, not comparable byte offsets, so they are only meaningful
relative to their own slot.


Configuration
-------------

.. code-block:: yaml

   id: "orders"
   name: "Orders YugabyteDB"
   type: "tap-yugabyte"
   owner: "data-platform@example.com"
   db_conn:
     host: "<HOST>"
     port: 5433
     user: "<USER>"
     password: "{{ env_var['YUGABYTE_PASSWORD'] }}"
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
     - ``0`` (disabled)
     - Stops after this total idle polling period.
   * - ``break_at_end_lsn``
     - No
     - ``true``
     - Stops after reaching the HybridTime boundary captured at startup.
   * - ``ssl``
     - No
     - Connector default
     - Uses YSQL ``sslmode=require`` when set to ``"true"``.
   * - ``itersize``
     - No
     - ``20000``
     - Server-side cursor fetch size for non-FastSync scans.
   * - ``limit``
     - No
     - Unlimited
     - Bounds rows returned by an incremental query.
   * - ``debug_lsn``
     - No
     - ``false``
     - Adds the current LSN as an automatic stream property when set to
       ``"true"``.
   * - ``fastsync_parallelism``
     - No
     - CPU count
     - Controls concurrent FastSync table exports.

Common tap settings are documented in :ref:`yaml_configuration`. Generate the
full template with ``pipelinewise init``.

Arrays, enums, composite types, and other user-defined types are discovered
with inclusion ``unsupported`` and excluded from the sync. ``hstore`` is
available on YugabyteDB and is exported as-is; it is not mapped to
Snowflake ``VARIANT``, unlike tap-postgres's explicit Iceberg v3 route.


Replication behaviour
----------------------

``INCREMENTAL`` requires ``replication_key``. The bookmark is compared
inclusively (``>=``), so the boundary row is re-emitted on every run and the
target must deduplicate on the primary key. Deletes are not captured.

``FULL_TABLE`` resumes an interrupted sync with parameterized primary-key
keyset pagination, since YugabyteDB's distributed MVCC exposes no
cluster-wide monotonic row version like PostgreSQL's ``xmin``. Tables without
a declared primary key fall back to a plain, non-resumable full scan.

``LOG_BASED`` bootstraps its initial scan from a snapshot pinned to the
replication slot's own restart boundary (``SET yb_read_time``), so the
snapshot and the streaming start position are provably consistent. An
interrupted bootstrap resumes from a ``bootstrap_in_progress`` bookmark
rather than an ``xmin`` sentinel. Views and materialized views are rejected
for ``LOG_BASED``.


Acknowledgement and recovery
-----------------------------

Consuming WAL does not by itself advance the slot's safe flush position.
PipelineWise sends feedback only up to the minimum target-acknowledged LSN
stored in ``state.json``. Missing, unreadable, invalid, or regressing state
retains the previous safe LSN.

Before consuming ongoing LOG_BASED changes, PipelineWise attempts to emit a
transactional ``pg_logical_emit_message`` in each tap database when the tap
user can execute the function. The transaction's commit provides a
decodable bookmark boundary even when the selected tables are idle. Slot
feedback advances through that boundary only after the target acknowledges
the state.

If the function is unavailable or inaccessible, replication continues using
the captured current boundary. Fully filtered idle streams may then require
a later decodable message before the bookmark and acknowledgement can
advance.

After an unexpected termination, restart the same tap without advancing
state. Unacknowledged changes remain replayable while the slot exists.
Resync only when the slot is unavailable, and monitor
``ysql_cdc_active_replication_slot_window_ms`` (default five minutes) when
tearing down a tap: dropping a slot immediately after the last consumer
disconnects can transiently fail with "slot is active." See
:ref:`stream_buffering` and :ref:`troubleshooting`.
