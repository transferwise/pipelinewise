
.. _metadata_columns:

Metadata Columns
----------------


Metadata columns add extra row level information about data ingestion in target connectors.
(i.e. when a row was read in source, when it was inserted or deleted in snowflake etc.)

Metadata columns are created automatically by adding extra columns to the tables with a
column prefix ``_SDC_``:

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

.. warning::

  Optionally, you can turn off creating metadata columns by creating
  ``add_metadata_columns: False`` to the :ref:`targets_list` YAML config file.
  Please note, if adding metadata columns option is turned off then deleted rows
  in source do not get deleted in target.

  Please note that **Hard Delete** mode is enabled by default for every target connector,
  which means that every record that was deleted in source will be deleted in the replicated
  target database as well. Please also note that Only :ref:`log_based` replication method
  detects delete row events.

  To turn off **Hard Delete** mode add ``hard_delete: False`` to the target :ref:`targets_list`
  YAML config file. In this case when a deleted row captured in source then
  ``_SDC_DELETED_AT`` column will only get flagged and not get deleted in the target.
  Please also note that Only :ref:`log_based` replication method detects delete row events.

