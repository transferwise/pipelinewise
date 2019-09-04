
.. _replication_methods:

Replication Methods
-------------------

Replication Methods define the approach PipelineWise takes (more precisley the `Singer.io <https://www.singer.io/>`_  taps)
when extracting data from a source during a replication job. Additionally, Replication Methods can also impact
how data is loaded into your destination and your overall row usage.

PipelineWise supports the following replication strategies to extract
data from data sources.

* :ref:`log_based`: It's replicating newly inserted, updated and also deleted records.

* :ref:`incremental`: The Tap saves it's progress via bookmarks. Only new or updated records are replicated during each sync.

* :ref:`full_table`: The Tap replicates all available records dating back to a start_date, defined in the tap config YAML, during every sync

* :ref:`fast_sync`: Same functionality as Full Table but optimised for data transfers between specific sources
  and targets and bypassing the Singer specification. Useful when initial syncing large tables with
  hundreds of millions of rows where singer components would usually be running for long hours or sometimes for days.


.. warning::

  **Important**: Replication Methods are one of the most important settings in PipelineWise.
  Defining a table’s Replication Method incorrectly can cause data discrepancies and latency.
  Before configuring the replication settings for a data pipeline, read through this  guide
  so you understand how PipelineWise will replicate your data.


.. _log_based:

Log Based
'''''''''

Log-based Replication is a replication method in which the we identify modifications
to records - including inserts, updates, and deletes - using a database’s binary log files.

.. warning::

  **Log Based** replication method is available **only for MySQL and PostgreSQL-backed** databases
  that support binary log replication and requires manual intervention when table structures change.


.. _incremental:

Key Based Incremental
'''''''''''''''''''''

Key-based Incremental Replication is a replication method in which the :ref:`taps_list` identify new and updated
data using a column called a Replication Key. A Replication Key is a ``timestamp``, ``date-time``, or ``integer``
column that exists in a source table.

When replicating a table using Key-based Incremental Replication, the following will happen:

1. During a replication job, PipelineWise stores the maximum value of a table’s Replication Key column.
2. During the next replication job, :ref:`taps_list` will compare saved value from the previous job to Replication Key column values in the source.
3. Any rows in the table with a Replication Key greater than or equal to the stored value are replicated.
4. PipelineWise stores the new maximum value from the table’s Replication Key column.
5. Repeat.

Let’s use a SQL query as an example:

.. code-block:: sql

    SELECT replication_key_column,
          column_you_selected_1,
          column_you_selected_2,
          [...]
      FROM schema.table
    WHERE replication_key_column >= 'last_saved_maximum_value'


If :ref:`log_based` Replication isn’t feasible or available for a data source, Key-based Incremental Replication
is the next best option.

.. warning::

  **Key Based Incremental** replication doesn't detect deletes in source.


.. _full_table:

Full Table
''''''''''

Full Table Replication is a replication method in which all rows in a table - including new, updated, and existing - are
replicated during every replication job.

If a table doesn't have a column suitable for :ref:`incremental` or if :ref:`log_based` is unavailable,
this method will be used to replicate data. 


.. _fast_sync:

Fast Sync
'''''''''

Fast Sync Replication is functionally identical to :ref:`full_table` replication but Fast Sync
bypassing the `Singer Specification <https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md>`_
for optimised performance. Primary use case of Fast Sync is initial sync or to resync large tables
with hundreds of millions of rows where singer components would usually run for long hours or
sometimes for days.

**Important**: Fast Sync is not a selectable replication method in the :ref:`yaml_configuration`.
PipelineWise detects automatically when Fast Sync gives better performance than the singer
components and uses it whenever it's possible. 

.. warning::

  **Fast Sync** is not a generic component and is **available only from some specific data sources to some specific targets**.
  Check :ref:`fast_sync` section for the supported components.


