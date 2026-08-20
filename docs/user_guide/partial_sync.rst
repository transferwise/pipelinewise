.. _partial_sync_cases:

PartialSync behaviour
=====================

PartialSync exports a bounded source range, loads a temporary target table, and
merges that range into the existing target. It is available from MariaDB/MySQL or
PostgreSQL to Snowflake.


Range semantics
---------------

The start boundary is inclusive. An explicit end boundary is also inclusive.
Choose a stable, comparable column and a range that can be verified independently.

.. code-block:: bash

   pipelinewise partial_sync_table \
     --tap <tap_id> \
     --target snowflake \
     --table <schema.table> \
     --column <column> \
     --start_value <start> \
     --end_value <end>

Without an end value, PipelineWise captures a replication position at the start
and can hand ongoing replication over from that position after the merge.


Native-table merge outcomes
---------------------------

.. list-table::
   :header-rows: 1
   :widths: 28 36 36
   :width: 100%

   * - Condition
     - Rows inside the range
     - Rows outside the range
   * - Matching schemas
     - Existing rows update; new rows insert.
     - Unchanged.
   * - Column absent from target
     - The target column is created and receives source values.
     - Existing rows have no backfilled value.
   * - Column absent from source
     - Merged target values become ``NULL`` for that column.
     - Existing values remain unchanged.
   * - Compatible text column is narrower than ``VARCHAR(134217728)``
     - PipelineWise widens the target column before applying the merge.
     - Values are unchanged; the wider column definition applies to the table.
   * - ``hard_delete: true``
     - Target rows absent from the source range are deleted.
     - Unchanged.
   * - ``hard_delete: false``
     - Missing source rows are retained and marked in ``_SDC_DELETED_AT``.
     - Unchanged.

.. deprecated:: 0.79.0

   Soft delete (``hard_delete: false``) is scheduled for removal. New pipelines
   should use ``hard_delete: true``.

Snowflake commits schema changes independently from the merge transaction.
PipelineWise therefore widens compatible native text columns and adds missing
columns before starting DML. If the existing target type is not text, its width
cannot be verified, or the target role cannot alter it, PartialSync fails before
the merge and state advancement. Run a FullSync or alter the column to
``VARCHAR(134217728)`` with an authorized role, then retry PartialSync.


Managed Iceberg v3 outcomes
---------------------------

MariaDB/MySQL and PostgreSQL taps can select managed Iceberg v3 through
``target_table_format: iceberg``. This route requires a primary key,
``hard_delete: true``, and ``data_flattening_max_level: 0``.

.. list-table::
   :header-rows: 1
   :widths: 34 66
   :width: 100%

   * - Target state
     - Outcome
   * - Missing
     - Creates a managed Iceberg v3 table containing the selected range.
   * - Exactly compatible
     - Updates, inserts, and hard-deletes the range in one transaction.
   * - New nullable source column
     - Adds the column, then applies the range transaction.
   * - Other schema or primary-key mismatch
     - Fails before DML.
   * - Existing string column is not ``VARCHAR(134217728)``
     - Fails before DML; PipelineWise does not widen existing Iceberg columns.
   * - NULL or duplicate transformed staging key
     - Fails before new publication DML; an already-submitted recovery remains
       ambiguous and preserves its evidence.
   * - ``drop_target_table: true``
     - Replaces the table with only the selected range.

PipelineWise persists the resolved range before export. An interrupted retry
reuses and deterministically replays that range after an ambiguous commit; it
does not resolve a dynamic boundary again. State changes only after publication
and finalization succeed. See :ref:`snowflake_iceberg_recovery`.

After staging transformations, PipelineWise checks the canonical primary-key
projection for NULL components and duplicate composite-key groups before it
starts the range transaction. It does not choose or deduplicate conflicting
rows. An invalid staged attempt is kept for cleanup and re-export; if an attempt
was already submitted, its outcome remains ambiguous and requires manual
recovery because the transaction may have committed.


Visual examples
---------------

Normal merge:

.. image:: ../img/partial_sync_case_1.png

Target missing a source column:

.. image:: ../img/partial_sync_case_2.png

Source missing a target column:

.. image:: ../img/partial_sync_case_3.png

Soft-delete and hard-delete outcomes:

.. image:: ../img/partial_sync_case_4.png

.. image:: ../img/partial_sync_case_5.png

Combined schema and delete changes:

.. image:: ../img/partial_sync_case_all.png


Safety and validation
---------------------

- Stop overlapping writes or replication when the selected range can change
  during export and merge.
- Estimate the source range and target merge cost before running.
- Confirm the boundary query returns the intended rows; an empty range is a
  successful no-op.
- Ensure transformations and canonical casts preserve non-NULL, unique
  composite primary keys; PipelineWise rejects rather than deduplicates an
  invalid staging result.
- After completion, compare exact primary keys and critical values inside the
  range, then confirm the next normal Singer run advances state.
- On managed-Iceberg failure, retain staging objects, the
  ``iceberg-recovery-<hash>.json`` stream manifest, the
  ``iceberg-fastsync-target-<hash>.json`` target pointer, and state until target
  publication is understood. Do not mark the range repaired from row count alone.
