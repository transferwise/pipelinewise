
.. _replication_methods:

Replication Methods
-------------------

Replication Methods define the approach Singer.io taps take when extracting data from a
source during a replication job. They also impact how data is loaded into your destination
and your overall row usage.

PipelineWise supports the following replication methods:

* :ref:`log_based`: Replicates newly inserted, updated, and deleted records using the database's change log.

* :ref:`incremental`: The tap saves its progress via bookmarks. Only new or updated records are replicated during each sync.

* :ref:`full_table`: The tap replicates all available records during every sync.


.. warning::

  **Important**: Replication Methods are one of the most important settings in PipelineWise.
  Defining a table’s Replication Method incorrectly can cause data discrepancies and latency.
  Before configuring the replication settings for a data pipeline, read through this guide
  so you understand how PipelineWise will replicate your data.


.. _log_based:

Log Based
'''''''''

Log-based Replication is a replication method in which we identify modifications
to records - including inserts, updates, and deletes - using a database’s binary log files.

.. warning::

  **Log Based** replication method is available **only for MySQL, PostgreSQL and MongoDB** source databases
  that support log replication.

.. note::
	When using **Log Based** replication method, table structures changes are detected automatically.

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

  **Key Based Incremental** replication from tables with long running transactions could lead to skipping rows in certain conditions.


.. _full_table:

Full Table
''''''''''

Full Table Replication is a replication method in which all rows in a table - including new, updated, and existing - are
replicated during every replication job.

If a table doesn't have a column suitable for :ref:`incremental` or if :ref:`log_based` is unavailable,
this method will be used to replicate data. 


.. seealso::

   PipelineWise also includes :ref:`fast_sync_main`, a performance optimization
   that bypasses Singer for bulk data transfers. FastSync is not a replication
   method — it is used automatically when conditions are met.

