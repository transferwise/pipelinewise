.. _resync:

Resync and repair
=================

Resync deliberately recopies source data and can reset replication state. Use it
to recover an unavailable change-log position or repair known target drift, not
as the first response to a transient failure.


Choose an operation
-------------------

.. list-table::
   :header-rows: 1
   :widths: 24 28 26 22
   :width: 100%

   * - Operation
     - Use when
     - Target effect
     - State effect
   * - ``fast_sync``
     - Entire tables need rebuilding.
     - Full replacement or configured PartialSync.
     - Captures new bookmarks.
   * - ``partial_sync_table``
     - One deterministic range is wrong.
     - Merges the selected range.
     - Can capture current position when no end is supplied.
   * - ``reset_state``
     - A controlled MariaDB/MySQL switchover has an exact position mapping.
     - No rows copied.
     - Rewrites binlog bookmarks.


Preflight
---------

Before a resync:

1. identify the exact tables and failure boundary;
2. confirm the source retains every row needed for the rebuild;
3. estimate source scan, staging, target load, lock, and warehouse cost;
4. confirm available disk/object-store space;
5. record target row keys or a deterministic checksum;
6. back up ``state.json``; and
7. stop concurrent replication and downstream writes where target replacement
   requires it.


Full resync
-----------

.. code-block:: bash

   pipelinewise fast_sync \
     --tap <tap_id> \
     --target <target_id> \
     --tables <schema.table,schema.table>

The command requires a FullSync-capable route and fails rather than falling back
to Singer. ``--replication_method_only <method>`` filters by configured method.
``--force`` overrides ``allowed_resync_max_size`` after the operator accepts the
source and target impact. It does not override ``sync_start_from`` or change a
configured PartialSync into a full-table reload.

.. _resync_postgres_slot_reset:

PostgreSQL source-slot reset
''''''''''''''''''''''''''''

For a PostgreSQL tap containing LOG_BASED tables, this command prepares the
tap-specific pgoutput source slot once before any worker starts:

.. code-block:: bash

   pipelinewise fast_sync --tap <tap_id> --target <target_id>

The ``reset_state`` command is refused for PostgreSQL LOG_BASED taps because a
pgoutput slot cannot rewind to a fabricated bookmark. The unfiltered whole-tap
FastSync resets the slot and state together.

Omit ``--tables`` and leave ``--replication_method_only`` at its ``*`` default
so every selected table is rebuilt. The deprecated ``sync_tables`` alias has
the same behaviour. ``--force`` is not required for the reset: it only bypasses
the resync size limit. Taps without LOG_BASED tables do not reset a slot.

.. list-table:: PostgreSQL slot behaviour during replication
   :header-rows: 1

   * - Command or operation
     - Source slot
   * - Unfiltered ``fast_sync`` (with or without ``--force``)
     - Drop the canonical pgoutput slot and any dedicated wal2json slot, then
       create a fresh pgoutput slot.
   * - ``fast_sync --tables ...`` (even if every table is listed, or ``--force`` is supplied)
     - Retain.
   * - ``fast_sync --replication_method_only log_based`` (or another non-default filter, with or without ``--force``)
     - Retain.
   * - ``partial_sync_table`` or automatic initial sync of individual/new tables
     - Retain.

PipelineWise validates local configuration, preflights the FullSync size limit
unless ``--force`` is supplied, and checks the source connection and candidate
slots before changing state. It also prepares and validates the tap's pgoutput
publication and transaction fence before establishing any new WAL boundary. A
preflight size-limit or publication rejection leaves the slots and bookmarks
unchanged. An active slot, a slot for another database, or an unexpected output
plugin blocks the operation.

After these checks, PipelineWise saves a unique
``state.json.before-slot-reset-<id>.bak`` alongside the state file. It records
a durable reset intent and clears every tap bookmark before dropping
``ppw_slot_<tap_id>`` and any dedicated historical wal2json slot. It then creates
a fresh pgoutput slot. This explicit whole-tap resync replaces the old history
with new snapshots; it does not bridge or copy the old slot. It can recover
when the old slot's WAL is no longer usable. Potentially shared database-wide
slots remain untouched. PipelineWise also preserves an implicitly truncated
historical tap-specific slot because its 63-byte name does not prove ownership.
Use :ref:`postgres_tap_rename` with ``previous_tap_id`` when that slot contains
history which must be migrated.

After every worker succeeds, the reset intent is cleared and normal pgoutput
consumption can resume. This also works for selected partition roots, whose
automatic wal2json bridge is unsupported. A failed target load does not restore
the dropped wal2json slot; complete the whole-tap resync before ordinary runs.

Backups are retained across retries. No FullSync or configured PartialSync worker
starts until slot preparation succeeds. A failed or interrupted whole-tap reset
retains its durable intent; ordinary replication refuses to run until the
unfiltered ``fast_sync`` succeeds. This prevents an ordinary run from treating
an incomplete resync as a completed snapshot.

.. warning::

   Slot preparation is not atomic. A source error, lost response, or process
   interruption after state invalidation can leave the canonical slot unchanged,
   absent or replaced; bookmarks remain cleared. Keep scheduled
   replication stopped, resolve the error, then retry the unfiltered
   ``fast_sync`` and complete the whole-tap resync. A state backup cannot recover
   WAL discarded by a completed or uncertain slot drop. Never restore
   old LOG_BASED bookmarks after that outcome. Errors report the failed phase
   and backup path when available.
   Add ``--force`` only if the resync size limit needs to be bypassed.

When an old tap ID must change to meet pgoutput naming rules, use
:ref:`postgres_tap_rename` to preserve state and its migration source slot.
A plain import rename follows deletion cleanup and is not that migration path.

For managed Iceberg, a pending publication or conversion recovery stops the
whole-tap reset before source or state changes. Resume the corresponding filtered
FastSync or conversion command to finish recovery, then retry the unfiltered
``fast_sync``.

Separately, ``import_config`` removes a deleted PostgreSQL tap's canonical
pgoutput slot, tap-specific wal2json slot, and ``ppw_slot_<tap_id>`` publication
when it removes that tap's runtime configuration. It leaves the database-wide
historical slot unchanged. It validates each owned slot and verifies that the
current role can remove the publication before changing the source. The
publication must also carry valid PipelineWise management metadata. Otherwise,
cleanup preserves the publication and slots and requires manual review. This is
cleanup, not a resync. For an old tap ID that does not meet the current naming
rules, cleanup preserves every canonical and normalized historical slot candidate
and the publication. Those non-injective names may belong to another valid tap.



Configured PartialSync and replicas
'''''''''''''''''''''''''''''''''''

A table with ``sync_start_from`` uses PartialSync instead of FullSync. MariaDB,
MySQL, and PostgreSQL sources can use ``replica_host`` for the FastSync read while
ongoing LOG_BASED replication remains on the primary.

PostgreSQL FastSync waits up to 300 seconds for the replica to replay the
primary's publication and slot boundary before exporting. If the replica stays
behind, the export stops. Resolve replica lag and retry. See :ref:`tap-postgres`
for the corresponding Singer initial-load check.

``--force`` preserves that configured range and loading method. Existing rows
outside the range remain unless ``drop_target_table: true`` is configured.
A whole-tap PostgreSQL resync still resets the shared WAL position, so ensure
the configured ranges cover the changes that need rebuilding; retained rows
outside those ranges are not revalidated or refreshed.

For a managed Iceberg v3 target, an exactly compatible table uses
``INSERT OVERWRITE`` and retains its object identity. A new nullable column is
added before overwrite. Other mismatches require guarded replacement by the
table's current owning account role; database-role ownership is not supported.
Ownership is retained, but the new object resets history and can change its base
location; visible unsupported dependencies stop the operation first. See
:ref:`snowflake_iceberg`.


Partial repair
--------------

.. code-block:: bash

   pipelinewise partial_sync_table \
     --tap <tap_id> \
     --target <target_id> \
     --table <schema.table> \
     --column <column> \
     --start_value <inclusive_start> \
     --end_value <inclusive_end>

PartialSync is available only from MariaDB/MySQL or PostgreSQL to Snowflake. If
``--end_value`` is omitted, PipelineWise captures the current replication
position and updates state after the merge. See :ref:`partial_sync_cases` for
schema and delete behaviour.


Failure and validation
----------------------

If a managed-Iceberg resync fails, preserve the log, generated staging objects,
``iceberg-recovery-<hash>.json`` stream manifest,
``iceberg-fastsync-target-<hash>.json`` target pointer, target object names, and
state backup until the publication state is understood. Retry the same operation
from the same generated target runtime directory and with unchanged source,
target, staging, role, and transformation identity before editing state or
removing recovery evidence. Do not advance state past an unpublished table.

After success, verify exact primary keys or a deterministic reconciliation,
critical values, target grants, and the next Singer run. Keep the state backup
until ongoing replication advances normally.
