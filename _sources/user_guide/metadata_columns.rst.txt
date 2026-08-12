.. _metadata_columns:

Metadata columns and deletes
============================

Targets can add ``_SDC_`` columns that describe ingestion time and source-delete
events. Configure the behaviour per tap because pipelines sharing a target can
have different delete requirements.


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


Configuration outcomes
----------------------

.. list-table::
   :header-rows: 1
   :widths: 35 25 40
   :width: 100%

   * - Settings
     - Metadata
     - Delete behaviour
   * - ``hard_delete: true``
     - Enabled automatically
     - Physically removes a target row after a source delete event.
   * - ``hard_delete: false`` and ``add_metadata_columns: true``
     - Enabled
     - Retains the row and sets ``_SDC_DELETED_AT``.
   * - Both ``false``
     - Disabled
     - Retains the target row without marking the source delete.

.. code-block:: yaml

   add_metadata_columns: true
   hard_delete: true

``hard_delete`` defaults to ``true``. Only LOG_BASED replication emits
individual source-delete events; incremental replication cannot detect deletes.
Full-table and PartialSync publication semantics can remove rows as part of
replacing or reconciling a selected range.

.. deprecated:: 0.79.0

   Soft delete (``hard_delete: false``) is scheduled for removal. New pipelines
   should use hard delete and model retention in a controlled downstream layer.

Changing delete mode does not rewrite historical target rows. Plan a resync or
explicit target migration when existing data must adopt the new behaviour.
