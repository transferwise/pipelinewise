.. _tap-postgres:

PostgreSQL source
=================

``tap-postgres`` extracts tables with full-table, key-based incremental, or
native pgoutput logical replication from PostgreSQL 14 or later. This source
minimum applies to every Singer replication method and PipelineWise
FullSync/PartialSync; it does not constrain PostgreSQL targets or the
PipelineWise backend database.

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
- permission to create, copy, advance, consume, and remove the tap's logical
  replication slots;
- ``EXECUTE`` on
  ``pg_catalog.pg_logical_emit_message(boolean, text, text)``;
- permission to create and manage the tap publication; and
- a valid primary key with ``REPLICA IDENTITY DEFAULT`` on every selected table
  and every physical leaf of a selected partition root.

PipelineWise creates or validates ``pw_pub_<tap_id>``. The publication contains
exactly the selected tables, publishes ``insert``, ``update``, and ``delete``,
excludes ``truncate``, and uses ``publish_via_partition_root = true``. It does
not allow row filters or column lists. The runtime role must be able to update
the publication comment because PipelineWise stores crash-safe transaction-fence
state there. It preserves an existing comment inside that metadata.

A DBA can pre-create the exact publication when the runtime role cannot create
or alter it. The runtime role must still own the publication so it can maintain
the fence comment. A selection change requires the DBA to update the table set
before retrying if the runtime role does not own every selected table.

PipelineWise uses the primary key as both the pgoutput row identity and the
Singer target merge key. Replica-identity indexes and ``REPLICA IDENTITY FULL``
are rejected because they can identify a different row from the target key. A
primary-key-changing update emits a delete for the old key before the new row.
If PostgreSQL omits an unchanged selected TOAST value from that update,
PipelineWise fails and requires a full resync instead of creating an incomplete
row under the new key. Changing a table's primary-key definition also requires
a whole-table full resync; an in-run identity change is rejected before the
tap refreshes its schema.

Before attaching or writing to a new partition, give that leaf the same primary
key and ``REPLICA IDENTITY DEFAULT``. Attach an empty leaf, then write through
the partition root. PostgreSQL
does not publish rows that already existed in a table when it was attached. If a
non-empty table must become a partition, separately resync those existing rows
before relying on ongoing CDC. PipelineWise validates current leaves during
preflight, but PostgreSQL can attach another leaf after the check.

LOG_BASED selection rejects ordinary PostgreSQL inheritance parents because
their child changes would not use the selected parent stream identity. A
partition root may contain only local ordinary or partitioned descendants;
foreign-table partitions are outside the local WAL stream. Select supported
physical tables separately or resync them through another supported route.

PipelineWise creates a native pgoutput slot named ``pipelinewise_<tap_id>`` and
a publication named ``pw_pub_<tap_id>``. A LOG_BASED PostgreSQL tap ID must
contain only lowercase letters, digits, and underscores and be at most 50
characters. PostgreSQL retains WAL needed by the slot, so monitor retained WAL
and do not remove it while the tap is active.
The tap ID must also differ from the database name after lowercasing and
replacing punctuation with underscores, to avoid the historical shared-slot name.

When the pgoutput slot is absent, PipelineWise can copy the historical
tap-specific ``pipelinewise_<dbname>_<tap_id>`` wal2json slot at its confirmed
LSN. It will not claim ``pipelinewise_<dbname>`` because that older
database-wide slot may serve another tap. A DBA must first migrate a
database-wide slot to the dedicated tap-specific name.

PipelineWise prepares the publication, emits a unique transactional logical
message, and consumes wal2json through that message's commit. Once the target
acknowledges the bridge state, PipelineWise advances pgoutput to the same LSN
and switches on the next run. Pgoutput then emits another transactional message.
A successful target acknowledgement of that boundary confirms pgoutput
consumption and allows PipelineWise to remove the tap-specific wal2json slot.
This works even when no selected rows changed. A failed target write preserves
the old slot, state, and WAL for retry.

Migration temporarily needs one additional replication-slot entry. Keep
wal2json installed until all old slots have been retired. On servers with
``output_plugin_libraries``, keep both ``pgoutput`` and ``wal2json`` in that
allowlist during migration. PostgreSQL 14.24 introduced this setting with only
the bundled plugins allowed by default; see the
`PostgreSQL release notes <https://www.postgresql.org/docs/release/14.24/>`_.
Automatic migration
is rejected when a selected table is a partition root because a new leaf could
otherwise appear while the fixed wal2json bridge filter is running. Coordinate
a whole-tap resync and removal or renaming of the old slot with the DBA instead.

Publication setup records a pending fence in the publication comment and waits
for transactions that can predate a new, changed, or first-adopted publication.
This prevents an older catalog snapshot from committing after the migration
boundary. Long-running transactions can delay setup. A prepared transaction
blocks setup until the DBA resolves it. PipelineWise records the ready fence
before it creates or copies a slot boundary.

When FastSync reads from ``replica_host``, it waits up to 300 seconds for the
replica to replay the primary's publication and slot boundary before exporting.
A Singer initial LOG_BASED load with ``use_secondary`` also waits for its
primary snapshot bookmark. That wait uses ``publication_fence_timeout_seconds``.
A timeout stops before exporting or saving a new snapshot bookmark. Resolve
replica lag and retry. Logical schema refreshes read from the primary.

Interrupted Singer full-table snapshots resume in transaction-age order. This
keeps their saved position consistent when transaction IDs gain a digit or wrap.

The pgoutput connection uses ISO date output, PostgreSQL interval output, and
round-trip floating-point output. Source role formatting defaults cannot swap
day and month or reduce the precision of replicated floating-point values.

An explicit, unfiltered ``fast_sync`` on a tap containing LOG_BASED tables
resets the tap-specific slot once before workers start, with or without
``--force``. Filtered FastSync, automatic initial loads, and standalone PartialSync
retain it. See :ref:`resync_postgres_slot_reset` for exact commands, legacy-slot
safety, state backups, pending-Iceberg guards, and non-atomic reset recovery.


Configuration
-------------

.. code-block:: yaml

   id: "orders_ingest"
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
   * - ``publication_fence_timeout_seconds``
     - No
     - ``300``
     - Fails publication setup when transactions that predate its catalog change
       remain open past this positive, finite number of seconds. Also bounds
       replica catch-up before Singer initial LOG_BASED loads.
   * - ``break_at_end_lsn``
     - No
     - ``true``
     - Stops after decoding the transaction commit containing this run's
       logical boundary message.
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
Snowflake FullSync and PartialSync apply top-level :ref:`transformations` in the
source SELECT before CSV generation, including managed Iceberg v3. Unsupported
rules fail before export; transformed INCREMENTAL replication keys are rejected
because checkpoints require their raw values.

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

After logical replication starts, PipelineWise emits one unique transactional
``pg_logical_emit_message`` in each tap database. It starts pgoutput with logical
messages enabled and matches the tap-specific prefix and unique token. The
transaction's commit end LSN becomes the run boundary even when the selected
tables are idle. With the default ``break_at_end_lsn: true``, the tap stops after
decoding that commit. Slot feedback advances through the boundary only after the
target acknowledges the state.

If the function is unavailable, the role cannot execute it, or the matching
message cannot be decoded, the run fails. PipelineWise does not substitute a
sampled WAL position because that would not prove a decoded transaction boundary.

After an unexpected termination, restart the same tap without advancing state.
Unacknowledged WAL remains replayable while the slot exists. Resync only when the
slot or required WAL is unavailable, and monitor retained WAL during a prolonged
target outage. See :ref:`stream_buffering` and :ref:`troubleshooting`.
