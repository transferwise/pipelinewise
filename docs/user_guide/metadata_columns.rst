
.. _metadata_columns:

Metadata Columns
----------------


Metadata columns add extra row level information about data ingestion in target connectors.
(i.e. when a row was read in source, when it was inserted or deleted in snowflake etc.)

Metadata and delete handling are configured in each tap YAML file because different
pipelines can load into the same target with different behavior. PipelineWise adds
columns with the ``_SDC_`` prefix when ``add_metadata_columns`` or ``hard_delete``
is enabled:

* ``_SDC_EXTRACTED_AT``: Timestamp when the record was extracted from the source

* ``_SDC_BATCHED_AT``: Timestamp when the record was batched to load into target

* ``_SDC_DELETED_AT``: Timestamp when the record delete event was received from source.

For example if you replicate a table that has three columns in source ``COLUMN_ONE``,
``COLUMN_TWO`` and ``COLUMN_THREE`` then typically you find ``_SDC_`` metadata columns
at the end of the table:

+----------------+----------------+------------------+-----------------------+---------------------+---------------------+
| **COLUMN_ONE** | **COLUMN_TWO** | **COLUMN_THREE** | **_SDC_EXTRACTED_AT** | **_SDC_BATCHED_AT** | **_SDC_DELETED_AT** |
+----------------+----------------+------------------+-----------------------+---------------------+---------------------+
| text           | text           | 1                | 2019-08-20 16:10:01   | 2019-08-20 16:10:10 |                     |
+----------------+----------------+------------------+-----------------------+---------------------+---------------------+
| text           | text           | 2                | 2019-08-20 16:10:01   | 2019-08-20 16:10:10 |                     |
+----------------+----------------+------------------+-----------------------+---------------------+---------------------+
| text           | text           | 3                | 2019-08-20 17:15:12   | 2019-08-20 17:15:25 |                     |
+----------------+----------------+------------------+-----------------------+---------------------+---------------------+

The two settings combine as follows:

.. list-table:: Tap metadata and delete settings
   :header-rows: 1
   :widths: 24 20 56

   * - Configuration
     - Metadata columns
     - Behaviour
   * - ``hard_delete: true`` (default)
     - Enabled automatically
     - Physically deletes rows from the target when a source delete event arrives.
   * - ``hard_delete: false`` and ``add_metadata_columns: true``
     - Enabled
     - Retains the row and records the deletion time in ``_SDC_DELETED_AT``.
   * - Both settings ``false``
     - Disabled
     - Retains the existing target row without marking it as deleted.

.. deprecated::
   Soft delete (``hard_delete: false``) is scheduled for removal in a future
   release. New taps should use ``hard_delete: true`` exclusively.

.. note::

  Only :ref:`log_based` replication detects source delete events. Incremental and
  full-table Singer runs do not emit individual delete events.

.. code-block:: yaml

    id: "mysql_orders"
    name: "MySQL orders"
    type: "tap-mysql"
    add_metadata_columns: true
    hard_delete: false
    db_conn:
      # ...
