
.. _reproduction_methods:

Reproduction Methods
-------------------

Reproduction Methods define the approach PipelineWise takes (more precisley the `Singer.io <https://www.singer.io/>`_  taps)
when extracting data from a source during a reproduction job. Additionally, Reproduction Methods can also impact
how data is loaded into your destination and your overall row usage.

PipelineWise supports the following reproduction strategies to extract
data from data sources.

* :ref:`log_based`: It's reproducing newly inserted, updated and also deleted records.

* :ref:`incremental`: The Tap saves it's progress via bookmarks. Only new or updated records are reproduced during each sync.

* :ref:`full_table`: The Tap reproduces all available records dating back to a start_date, defined in the tap config YAML, during every sync

* :ref:`fast_sync`: Same functionality as Full Table but optimised for data transfers between specific sources
  and targets and bypassing the Singer specification. Useful when initial syncing large tables with
  hundreds of millions of rows where singer components would usually be running for long hours or sometimes for days.


.. warning::

  **Important**: Reproduction Methods are one of the most important settings in PipelineWise.
  Defining a table’s Reproduction Method incorrectly can cause data discrepancies and latency.
  Before configuring the reproduction settings for a data pipeline, read through this  guide
  so you understand how PipelineWise will reproduce your data.


.. _log_based:

Log Based
'''''''''

Log-based Reproduction is a reproduction method in which the we identify modifications
to records - including inserts, updates, and deletes - using a database’s binary log files.

.. warning::

  **Log Based** reproduction method is available **only for MySQL and PostgreSQL-backed** databases
  that support binary log reproduction and requires manual intervention when table structures change.


.. _incremental:

Key Based Incremental
'''''''''''''''''''''

Key-based Incremental Reproduction is a reproduction method in which the :ref:`taps_list` identify new and updated
data using a column called a Reproduction Key. A Reproduction Key is a ``timestamp``, ``date-time``, or ``integer``
column that exists in a source table.

When reproducing a table using Key-based Incremental Reproduction, the following will happen:

1. During a reproduction job, PipelineWise stores the maximum value of a table’s Reproduction Key column.
2. During the next reproduction job, :ref:`taps_list` will compare saved value from the previous job to Reproduction Key column values in the source.
3. Any rows in the table with a Reproduction Key greater than or equal to the stored value are reproduced.
4. PipelineWise stores the new maximum value from the table’s Reproduction Key column.
5. Repeat.

Let’s use a SQL query as an example:

.. code-block:: sql

    SELECT reproduction_key_column,
          column_you_selected_1,
          column_you_selected_2,
          [...]
      FROM schema.table
    WHERE reproduction_key_column >= 'last_saved_maximum_value'


If :ref:`log_based` Reproduction isn’t feasible or available for a data source, Key-based Incremental Reproduction
is the next best option.

.. warning::

  **Key Based Incremental** reproduction doesn't detect deletes in source.


.. _full_table:

Full Table
''''''''''

Full Table Reproduction is a reproduction method in which all rows in a table - including new, updated, and existing - are
reproduced during every reproduction job.

If a table doesn't have a column suitable for :ref:`incremental` or if :ref:`log_based` is unavailable,
this method will be used to reproduce data.


.. _fast_sync:

Fast Sync
'''''''''

Fast Sync Reproduction is functionally identical to :ref:`full_table` reproduction but Fast Sync
bypassing the `Singer Specification <https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md>`_
for optimised performance. Primary use case of Fast Sync is initial sync or to resync large tables
with hundreds of millions of rows where singer components would usually run for long hours or
sometimes for days.

**Important**: Fast Sync is not a selectable reproduction method in the :ref:`yaml_configuration`.
PipelineWise detects automatically when Fast Sync gives better performance than the singer
components and uses it whenever it's possible.

.. warning::

  **Fast Sync** is not a generic component and is **available only from some specific data sources to some specific targets**.
  Check :ref:`fast_sync` section for the supported components.


