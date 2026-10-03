.. _tap-postgres:

PostgreSQL source
=================

``tap-postgres`` extracts tables with full-table, key-based incremental, or
pgoutput logical replication from PostgreSQL 14 or later. This source minimum
applies to every Singer replication method and PipelineWise FullSync/PartialSync;
it does not constrain PostgreSQL targets or the PipelineWise backend database.

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
- permission to create and consume a logical replication slot;
- ``EXECUTE`` on the text overload of ``pg_catalog.pg_logical_emit_message``
  for durable snapshot and progress boundaries; and
- permission to create a publication (database ``CREATE``), own its selected
  tables, and update its membership and comment. A DBA may precreate an exact
  publication, but the runtime still needs permission to maintain it.

Pgoutput is built into PostgreSQL. Keep wal2json installed until existing taps
finish migration. Migration temporarily needs two slots per tap. Sources below
14.18, 15.13, 16.9, or 17.5 remain supported with a warning about upstream
catalog-cache fixes. Upgrade to a patched minor version where possible.

PipelineWise names both the slot and publication ``ppw_slot_<tap_id>``. Tap IDs
must contain only lowercase ASCII letters, digits and underscores, and be at
most 50 characters. They must be unique across databases on the same cluster.
PipelineWise creates one tap-specific slot in the source database. PostgreSQL retains WAL needed
by that slot, so monitor retained WAL and do not remove the slot while the tap is
active.

PipelineWise checks boundary permission before preparing the publication or
creating a slot. If your DBA revoked the default public grant, ask them to grant
``EXECUTE`` on the text overload present on your server to the replication role.
This permission is required even though this release does not decode messages.
An ordinary current-WAL position cannot safely replace a durable snapshot fence.

An explicit, unfiltered ``fast_sync`` on a tap containing LOG_BASED tables
resets the tap-specific slot once before workers start, with or without
``--force``. Filtered FastSync, automatic initial loads, and standalone PartialSync
retain it. See :ref:`resync_postgres_slot_reset` for exact commands, legacy-slot
safety, state backups, pending-Iceberg guards, and non-atomic reset recovery.


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
   * - ``previous_tap_id``
     - No
     - None
     - Tap-level alias for a deliberate historical-ID rename during migration;
       see :ref:`postgres_tap_rename`.
   * - ``replica_host``
     - No
     - Primary host
     - Offloads FastSync reads to a physical standby of the configured primary.
       A non-recovering endpoint is rejected; logical replication stays on the primary.
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
     - Stops at a decoded commit crossing the startup boundary. Quiet sources
       can wait for the idle timeout.
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

Run boundaries still use decoded commits and numeric LSNs. The tap emits the
existing transactional progress marker. Startup and snapshot preparation require
permission to emit it; a later emission failure can reuse the sampled run
boundary. Pgoutput does not request
``messages=true`` in this release, so the marker itself is not decoded.
PostgreSQL 15+ can filter transactions without published row changes. Quiet
sources can therefore wait until ``logical_poll_total_seconds`` (default three
hours) or ``max_run_seconds`` expires. Neither keepalives nor a timeout prove
that a commit was delivered to the target, and they do not advance bookmarks.
Set an appropriate idle timeout for scheduled taps.

Before the startup boundary, the tap checkpoints at completed transactions after
10,000 emitted row changes or 60 seconds. Large transactions must finish before
a checkpoint can be emitted. After the boundary, every commit can checkpoint.
Feedback still waits for target acknowledgement, and overlap replay retains its
original slot position until the shared boundary is acknowledged.

After an unexpected termination, restart the same tap without advancing state.
Unacknowledged WAL remains replayable while the slot exists. Resync only when the
slot or required WAL is unavailable, and monitor retained WAL during a prolonged
target outage. See :ref:`stream_buffering` and :ref:`troubleshooting`.


Migration from wal2json
-----------------------

Release 0.94.0 is the migration release. Release 0.95.0 will remove the migration
and all remaining wal2json code and tests. Complete migration on 0.94.0 before upgrading.
This is a roll-forward transition; restoring an old state file does not restore
WAL discarded when a slot was dropped.

PipelineWise prepares the publication and waits for existing source writers to
finish before creating a fresh pgoutput slot. It consumes the dedicated
``pipelinewise_<database>_<tap_id>`` wal2json slot through a later complete
transaction. After the target acknowledges that bridge, PipelineWise atomically
persists promotion in ``state.json`` and drops wal2json, including with
``break_at_end_lsn: false``. It never uses ``pg_copy_logical_replication_slot``.

Pgoutput starts from its original consistent LSN without advancing its slot.
It replays the overlap, keeping bookmarks monotonic. Source feedback remains at
the original slot position until a decoded commit reaches the bridge and the
target acknowledges that replay. Targets merge repeated keys. The migration
marker is then cleared and normal pgoutput replication continues. A failed run
retries from the retained position; keep its state and source slot together.

A quiet source may have already retired wal2json while its overlap marker is
still present. A later published transaction lets replay finish. Until then,
pgoutput retains the original WAL position and publication selection remains
frozen. This limitation will be addressed separately with message-based
boundaries. Do not manually advance the slot to clear the marker.

Automatic migration requires a dedicated, inactive wal2json slot with retained
history. Shared database-wide slots are not adopted or deleted. Ambiguous
truncated historical names need an explicit alias. Selected partition roots
require an unfiltered whole-tap FastSync instead of automatic migration.
LOG_BASED tables need a valid, non-deferrable primary key and matching replica
identity. Selected generated columns, ordinary inheritance parents and foreign
partitions are rejected. Resolve the reported incompatibility or use another
replication method before migration.

Check existing bookmarks before rollout
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stop the tap's schedule and wait for its run to finish. Back up its generated
configuration and ``state.json``. Compare the minimum integer ``lsn`` bookmark
across its selected LOG_BASED streams with the dedicated wal2json slot's
``confirmed_flush_lsn``. This read-only query lists those positions as integers:

.. code-block:: sql

   SELECT slot_name, active,
          confirmed_flush_lsn - '0/0'::pg_lsn AS confirmed_flush_lsn_integer
   FROM pg_replication_slots
   WHERE database = current_database() AND plugin = 'wal2json';

Previous releases could advance the slot past saved bookmarks during idle
keepalives. Such state can be produced by a successful old run. PipelineWise
rejects migration when a selected bookmark predates the slot's confirmed
position because it cannot prove that the missing interval reached the target.
Do not raise bookmarks or advance the slot to bypass this check.

Use an explicit, unfiltered whole-tap ``fast_sync`` to replace the old slot and
take fresh snapshots when this check fails. See :ref:`resync_postgres_slot_reset`.
Plan for the full reload; a restart alone cannot recover discarded history.
The upgrade tests exercise both accepted and rejected state produced by the
previous master tap, followed by migration or whole-tap recovery respectively.

Snapshot and update limitations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A configured Singer secondary or FastSync ``replica_host`` must be a physical
standby in recovery. A promoted or mistakenly configured primary is rejected
before reading the snapshot. Correct the endpoint, or remove the secondary
configuration to read from the primary.

Pgoutput can omit unchanged large values (TOAST). Ordinary updates preserve
them. A primary-key change that omits values needed to create the replacement
row stops replication without acknowledging that transaction. Use a full resync
to recover the row and avoid such key changes, or choose a replication method
that reads complete rows. A resync does not prevent the next such change.

Conditional Singer transformations require the transformed field and all
condition columns whenever any of them changes. An incomplete PATCH stops before
emitting that raw record or its checkpoint. Use unconditional masking or a
replication method that supplies complete rows. Fetching the current source row
would not reliably reconstruct its value at the WAL event. See :ref:`transformations`.

Publication membership
----------------------

New selected tables are added and fenced before their FastSync snapshot.
Persistent ``import_config`` removes deselected members tracked by PipelineWise,
after invalidating their logical bookmarks. Untracked DBA-added members are
preserved. Filtered runs retain other configured tables. Re-adding a removed
logical table requires a fresh snapshot. Removing the final LOG_BASED selection
also clears migration state and retires dedicated slots.

If discovery or publication cleanup fails during import, retry the same import.
PipelineWise keeps ``postgres_publication_pending.json`` beside the tap config
until reconciliation succeeds. Keep this file when retrying; it does not change
replication bookmarks or the migration boundary.

Publication preflight has an overall timeout as well as individual database
timeouts. Several slow steps can exhaust the overall timeout even when each
step remains within its limit. Retry the same command after resolving long-running
writers or locks; retained fence metadata makes this retry safe.

Do not change publication membership, table identity or options manually while
replication has retained history. A missing managed table or publication can
represent an unrecoverable gap; PipelineWise requires a whole-tap FastSync when
it detects this. Publication changes are blocked while migration is active.

.. _postgres_tap_rename:

Renaming a historical tap
-------------------------

If an old ID violates the new naming rules, stop the tap and back up generated
configuration and state. Change the YAML ``id`` and set ``previous_tap_id`` to
the exact old ID on the same target. Keep the source, target, selected streams,
mappings and transformations unchanged, then run ``import_config``. PipelineWise
validates the dedicated old slot and copies its state before migration. Keep
the alias and old generated files until migration has completed. Repeated
imports preserve progress rather than overwriting it with the old state.

.. code-block:: yaml

   id: "orders_pg"
   previous_tap_id: "Orders-PG"
   type: "tap-postgres"

A plain rename without this alias follows normal deletion cleanup and does not
preserve the old replication history.
