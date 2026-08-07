.. _replication_methods:

Replication methods
===================

A table's replication method controls how its tap finds records after the
initial load. FastSync is a transfer optimisation, not a replication method.


Choose a method
---------------

.. list-table::
   :header-rows: 1
   :widths: 22 24 18 18 18
   :width: 100%

   * - Method
     - Change boundary
     - Inserts
     - Updates
     - Deletes
   * - ``LOG_BASED``
     - Database change log
     - Yes
     - Yes
     - Yes
   * - ``INCREMENTAL``
     - Replication-key maximum
     - Yes
     - Only when key advances
     - No
   * - ``FULL_TABLE``
     - None; reads all rows
     - Yes
     - Yes
     - By table replacement or merge semantics

Prefer LOG_BASED for mutable database tables when the source supports it and log
retention can be operated safely. Use INCREMENTAL for append-oriented data with a
stable increasing key. Use FULL_TABLE only when complete rescans are acceptable.


.. _log_based:

Log-based replication
---------------------

LOG_BASED reads inserts, updates, and deletes from a database change log.
PipelineWise supports it for MariaDB/MySQL, PostgreSQL, and MongoDB connectors.
Experimental connector status still applies.

An initial table without a bookmark uses FullSync when the route supports it.
The same ``run_tap`` invocation then starts Singer for ongoing log consumption.
The source must retain change-log data until the target-acknowledged bookmark has
advanced beyond it.

.. warning::

   Losing a binlog, logical replication slot, WAL range, or change-stream token
   can make the saved bookmark unrecoverable. Restore the source log when
   possible; otherwise perform a deliberate resync.


.. _incremental:

Key-based incremental replication
---------------------------------

INCREMENTAL stores the maximum observed ``replication_key`` and selects rows
whose key is greater than or equal to that bookmark:

.. code-block:: sql

   SELECT <selected_columns>
     FROM <schema>.<table>
    WHERE <replication_key> >= <saved_maximum>;

The overlap permits replay of the boundary value; the target primary key must
deduplicate or merge repeated rows. Updates whose replication key does not
advance and source deletes are not detected. Backfilled or non-monotonic keys can
therefore leave permanent gaps.


.. _full_table:

Full-table replication
----------------------

FULL_TABLE reads every selected row on every run. Supported native routes use
FullSync; other routes use the Singer path. Account for a full source scan,
staging space, target replacement/merge work, and a longer recovery window.

Use :ref:`resync` when a one-off rebuild is required without permanently setting
the table to FULL_TABLE.
