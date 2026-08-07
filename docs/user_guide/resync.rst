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
     - A controlled database switchover has an exact position mapping.
     - No rows copied.
     - Rewrites CDC bookmarks.


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
source and target impact.

A table with ``sync_start_from`` uses PartialSync instead of FullSync. MariaDB,
MySQL, and PostgreSQL sources can use ``replica_host`` for the FastSync read while
ongoing LOG_BASED replication remains on the primary.


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

If a resync fails, preserve the log, generated staging objects, target object
names, and state backup until the target publication state is understood. Do not
advance state past an unpublished table.

After success, verify exact primary keys or a deterministic reconciliation,
critical values, target grants, and the next Singer run. Keep the state backup
until ongoing replication advances normally.
