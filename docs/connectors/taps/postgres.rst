.. _tap-postgres:

PostgreSQL source
=================

``tap-postgres`` extracts tables with full-table, key-based incremental, or
native pgoutput logical replication. Sources require PostgreSQL **14 or later**.
Releases before 14.18, 15.13, 16.9, or 17.5 remain supported but log a warning.
Those earlier minor releases have a catalog-cache defect that can omit changes
after publication updates or decode values incorrectly after type changes. The
defect also affects wal2json and does not depend on using logical messages for
run boundaries.
PipelineWise executes ``ALTER PUBLICATION ... ADD TABLE`` on the source
when extending publication membership. If this overlaps source writes, the
defect can suppress row-change events before PipelineWise receives them.
Target schema comparison cannot recover those omitted events. See the
`PostgreSQL fix <https://github.com/postgres/postgres/commit/9f21be08e884c75089e939bae86a47eca176e0af>`_.

The PostgreSQL 14 minimum applies to every Singer replication method and
PipelineWise FullSync/PartialSync. It does not constrain PostgreSQL targets or
the backend database.

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
- a valid, non-deferrable primary key with ``REPLICA IDENTITY DEFAULT`` on every selected table
  and every physical leaf of a selected partition root.

PipelineWise creates or validates ``pw_pub_<tap_id>``. It adds every selected
LOG_BASED table before creating a snapshot boundary or consuming WAL. Preparation
never removes an existing member, including during a filtered run or a switch
between snapshot and CDC phases. Deselected tables can therefore remain in the
publication; the tap filters their changes. Deliberate removal requires a separate
DBA operation with replication stopped.

The publication publishes ``insert``, ``update``, and ``delete``. It excludes
``truncate`` and uses ``publish_via_partition_root = true``. Row filters and
column lists are not supported. The runtime role must be able to update the
publication comment because PipelineWise stores transaction-fence state there.
It preserves an existing comment inside that metadata.

A DBA can pre-create the publication when the runtime role cannot add tables.
The runtime role must still own the publication so it can maintain the fence
comment. A new selection requires the DBA to add its tables before retrying if
the runtime role does not own those tables.

PipelineWise uses the primary key as both the pgoutput row identity and the
Singer target merge key. Replica-identity indexes and ``REPLICA IDENTITY FULL``
are rejected because they can identify a different row from the target key. A
primary-key-changing update emits a delete for the old key before the new row.
An unchanged toasted key is recovered from the event's old identity when present.
INCLUDE columns in a primary-key index are ordinary data columns, not merge keys.
If PostgreSQL omits an unchanged selected non-key TOAST value from that update,
PipelineWise fails and requires a full resync instead of creating an incomplete
row under the new key. Changing a table's primary-key definition also requires
a whole-table full resync; an in-run identity change is rejected before the
tap refreshes its schema. Historical events are checked against the columns in
their Relation message, so a later added column does not invalidate an earlier
primary-key update.

PipelineWise does not replicate generated columns through pgoutput. It rejects
LOG_BASED selection of these columns on every supported PostgreSQL version before
changing the publication. Use FULL_TABLE or INCREMENTAL for an affected table. A standalone
Singer deployment can exclude the column in its catalog if the destination does
not need it. PipelineWise YAML has no column-exclusion setting. Do not remove
this guard to force replication: the destination value would become stale.
Inspect each source before upgrading:

.. code-block:: sql

   SHOW server_version;
   SHOW server_encoding;
   SELECT namespace.nspname AS schema_name, relation.relname AS table_name,
          attribute.attname AS generated_column
   FROM pg_attribute AS attribute
   JOIN pg_class AS relation ON relation.oid = attribute.attrelid
   JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
   WHERE attribute.attgenerated <> '' AND NOT attribute.attisdropped
   ORDER BY 1, 2, 3;

Refresh discovery for tables whose primary key has INCLUDE columns. If an
existing destination used an INCLUDE column as part of its merge key, perform
a full table resync to rebuild it with the corrected key.

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
Tap IDs must be unique across every database and PipelineWise project using the
same PostgreSQL cluster. Slot names belong to the cluster, not one database.
A conflicting slot is rejected before use.

When the pgoutput slot is absent, PipelineWise can copy the historical
tap-specific ``pipelinewise_<dbname>_<tap_id>`` wal2json slot at its confirmed
LSN. Historical names use PostgreSQL's 63-byte truncation rule. Ambiguous
collisions are rejected. PipelineWise will not claim ``pipelinewise_<dbname>``
because that older database-wide slot may serve another tap. A new tap with no
saved history can create its own slot while preserving the shared slot. An
existing tap whose bookmarks depend on the shared slot needs DBA coordination
or an explicit whole-tap resync.

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
an explicit whole-tap resync instead. A verified migration that has already
reached pgoutput can continue without replaying the wal2json bridge.

Publication setup records a pending fence in the publication comment and waits
for transactions with an assigned transaction ID that predate the publication
change. Long readers without a transaction ID do not block it. Prepared
transactions must be resolved by the DBA. The wait reuses one connection and
reports blockers. PipelineWise records the ready fence before creating or
copying a slot boundary.

``publication_fence_timeout_seconds`` bounds this wait and publication DDL lock
and statement timeouts. A timeout leaves preparation retryable. Resolve the
reported writer or conflicting DDL and retry without advancing bookmarks.

When FastSync reads from ``replica_host``, it waits up to 300 seconds for the
replica to replay the primary's publication and slot boundary before exporting.
A Singer initial LOG_BASED load with ``use_secondary`` also waits for its
primary snapshot bookmark. That wait uses ``publication_fence_timeout_seconds``.
The wait and snapshot use the same connection, including when the replica
endpoint balances connections. The boundary is a committed, flushed logical
message, so an idle primary does not leave the wait beyond the last replayable
record. A timeout stops before exporting or saving a new snapshot bookmark.
Resolve replica lag and retry. Logical schema refreshes read from the primary.

Interrupted Singer snapshots restart from the first row. They retain the table
version and original CDC start LSN. This avoids relying on transaction IDs that
can wrap or be frozen between runs. LOG_BASED tables have primary keys, so
replayed rows merge into their existing target rows. The first checkpoint marks
the snapshot incomplete even before its first row. For standalone FULL_TABLE
streams without a key, repeated records can be appended; use FullSync when the
destination needs replacement semantics.

The pgoutput connection uses ISO date output, PostgreSQL interval output, and
round-trip floating-point output. Source role formatting defaults cannot swap
day and month or reduce the precision of replicated floating-point values.
Pgoutput uses UTF8 on the wire. The wal2json bridge decodes its output using the
source database encoding, including databases that use LATIN1.

An explicit, unfiltered ``fast_sync`` on a tap containing LOG_BASED tables
resets the tap-specific slot once before workers start, with or without
``--force``. Filtered FastSync, automatic initial loads, and standalone PartialSync
retain it. See :ref:`resync_postgres_slot_reset` for exact commands, legacy-slot
safety, state backups, pending-Iceberg guards, and non-atomic reset recovery.


.. _postgres_tap_rename:

Renaming a historical tap ID
----------------------------

Use ``previous_tap_id`` when an existing wal2json tap ID does not meet the new
naming rules. Stop scheduling both identities before importing the change.
Keep the source host, port, database, user, and target unchanged.
The target account or host, database, schema mapping, table format, selected
streams, and transformations must also stay unchanged. Import checks these
against the old generated configuration on the first import and on retries.
Missing old configuration or a changed destination prevents state adoption.
Change credentials independently if needed, but finish the rename before
changing the data selection or destination layout.

1. Replace the YAML ``id`` with a valid, cluster-unique value.
2. Add top-level ``previous_tap_id`` with the exact old ID. Remove the old tap's
   YAML definition from the project, but preserve its generated runtime files.
3. Run ``pipelinewise import_config --dir <project> --taps <new_id>``. It copies
   the old state before discovery and preserves the old runtime files and slot.
   Repeating import preserves any progress already made under the new identity.
4. Run the new tap through migration. Keep ``previous_tap_id`` until pgoutput
   consumption has been acknowledged and the old slot has been retired.

An existing migration or incomplete whole-tap reset must finish before renaming.
Do not run both identities concurrently. A plain rename without
``previous_tap_id`` retains the usual deletion behaviour described in
:ref:`yaml_configuration`.

Configuration
-------------

.. code-block:: yaml

   id: "orders_ingest"
   # previous_tap_id: "orders-old"  # Only for an existing wal2json tap rename.
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
   * - ``previous_tap_id``
     - No
     - None
     - Preserves the old state and identifies its wal2json slot during an explicit
       tap rename. See :ref:`postgres_tap_rename`.
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
     - Bounds publication DDL and the wait for earlier writers. Also bounds
       replica catch-up before Singer initial LOG_BASED loads. Must be positive
       and finite.
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
previous safe LSN. Server keepalives do not implicitly advance that position.
PostgreSQL and Snowflake targets establish an initial durable checkpoint by
flushing pending streams, then continue per-stream checkpoints. A failed flush
cannot acknowledge incomplete target data.

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
