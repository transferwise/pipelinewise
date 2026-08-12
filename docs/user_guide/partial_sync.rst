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


Merge outcomes
--------------

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
   * - ``hard_delete: true``
     - Target rows absent from the source range are deleted.
     - Unchanged.
   * - ``hard_delete: false``
     - Missing source rows are retained and marked in ``_SDC_DELETED_AT``.
     - Unchanged.

.. deprecated:: 0.79.0

   Soft delete (``hard_delete: false``) is scheduled for removal. New pipelines
   should use ``hard_delete: true``.


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
- After completion, compare exact primary keys and critical values inside the
  range, then confirm the next normal Singer run advances state.
- On failure, retain staging objects and state until target publication is
  understood. Do not mark the range repaired from row count alone.
