.. _metadata_columns:

Metadata columns
================

PostgreSQL and Snowflake targets automatically add ``_SDC_`` columns that
describe ingestion time and source-delete events.


Columns
-------

.. list-table::
   :header-rows: 1
   :widths: 32 68
   :width: 100%

   * - Column
     - Meaning
   * - ``_SDC_EXTRACTED_AT``
     - Time the tap extracted the record.
   * - ``_SDC_BATCHED_AT``
     - Time the record entered a target load batch.
   * - ``_SDC_DELETED_AT``
     - Time a source delete event was received; ``NULL`` for active rows.


Source deletes
--------------

``_SDC_DELETED_AT`` is an internal deletion marker. PostgreSQL and Snowflake
targets physically remove marked rows before acknowledging Singer state.

Only LOG_BASED replication emits individual source-delete events. Key-based
incremental replication cannot detect deleted source rows. FullSync replaces the
target table, while PartialSync removes target rows that are missing from its
selected source range. Singer FULL_TABLE behaviour remains connector-specific.
