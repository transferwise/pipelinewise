
.. _fast_sync_main:

FastSync
--------

**FastSync** is a performance optimization that bypasses the
`Singer Specification <https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md>`_
for bulk data operations. Instead of piping JSON between tap and target processes,
FastSync uses native database tools (``COPY``, staged files) to transfer data
directly — typically 10–100x faster than the standard Singer path.

FastSync is available only for specific tap-target combinations (see the table below).

.. warning::

  **Important**: FastSync is not a selectable replication method in the :ref:`yaml_configuration`.
  PipelineWise detects automatically when FastSync gives better performance than the Singer
  components and uses it whenever it's possible.


Supported tap-target combinations
''''''''''''''''''''''''''''''''''

FullSync and PartialSync support different tap-target combinations:

+----------------------------+----------------------------------+--------------+-----------------+
| **Tap**                    | **Target**                       | **FullSync** | **PartialSync** |
+----------------------------+----------------------------------+--------------+-----------------+
| :ref:`tap-mysql`           | **->** :ref:`target-snowflake`   | Yes          | Yes             |
+----------------------------+----------------------------------+--------------+-----------------+
| :ref:`tap-postgres`        | **->** :ref:`target-snowflake`   | Yes          | Yes             |
+----------------------------+----------------------------------+--------------+-----------------+
| :ref:`tap-mongodb`         | **->** :ref:`target-snowflake`   | Yes          | No              |
+----------------------------+----------------------------------+--------------+-----------------+
| :ref:`tap-mysql`           | **->** :ref:`target-postgres`    | Yes          | No              |
+----------------------------+----------------------------------+--------------+-----------------+
| :ref:`tap-postgres`        | **->** :ref:`target-postgres`    | Yes          | No              |
+----------------------------+----------------------------------+--------------+-----------------+
| :ref:`tap-mongodb`         | **->** :ref:`target-postgres`    | Yes          | No              |
+----------------------------+----------------------------------+--------------+-----------------+

.. note::

   During a normal :ref:`cli_run_tap` run, PipelineWise uses the standard
   Singer-based sync when FullSync is not supported for the tap-target combination.

   The explicit :ref:`cli_fast_sync` command behaves differently: it requires a
   supported FullSync combination and fails if one is not available. It never falls
   back to Singer.


Components
''''''''''

FastSync has two components that share the same underlying infrastructure:

**FullSync**

Exports the entire source table, stages the data, and replaces the target table.
This is the component used during initial syncs and when you explicitly run the
:ref:`cli_fast_sync` command.

**PartialSync**

Exports a filtered range of rows from the source table, stages the data, and
merges it with the existing target table (updating existing rows and inserting
new ones). This is the component used when you run the :ref:`cli_partial_sync_table`
command. See :ref:`partial_sync_cases` for details on how different scenarios are
handled.


When does PipelineWise use FastSync?
'''''''''''''''''''''''''''''''''''''

PipelineWise automatically selects the **FastSync** component for a table when one of the
following conditions are met:

* The replication method is ``FULL_TABLE`` (always treated as initial sync), **or**
* The replication method is ``INCREMENTAL`` but no replication key value has been
  recorded yet, **or**
* The replication method is ``LOG_BASED`` but no LSN, binlog position, GTID, or
  change stream token has been recorded yet.

For ``INCREMENTAL`` and ``LOG_BASED`` tables, FastSync handles only the initial load.
Once the initial sync completes, PipelineWise switches to the standard Singer path
for ongoing incremental or log-based replication.

When you run :ref:`cli_fast_sync` explicitly, FullSync is used unconditionally for all
supported tap-target combinations, regardless of bookmark state — unless a table has
``sync_start_from`` defined in its tap configuration (see :ref:`defined_partial_sync` below).

The **PartialSync** component is also used when you run :ref:`cli_partial_sync_table`
explicitly.


.. _defined_partial_sync:

Defined PartialSync (``sync_start_from``)
'''''''''''''''''''''''''''''''''''''''''

You can configure individual tables to always use PartialSync instead of FullSync
when :ref:`cli_fast_sync` is run. This is done by adding a ``sync_start_from`` block
to the table entry in your tap YAML configuration:

.. code-block:: yaml

    tables:
      - table_name: "my_table"
        replication_method: "LOG_BASED"
        sync_start_from:
          column: "updated_at"
          static_value: "2024-01-01"      # or use dynamic_value instead
          drop_target_table: false

When ``fast_sync`` encounters a table with ``sync_start_from``, it routes that table
through PartialSync automatically — applying a ``WHERE column >= value`` filter during
export and merging the result into the target rather than replacing it.

**Configuration keys:**

``column`` *(required)*
  The column to use for the range filter (``WHERE column >= value``).

``static_value`` *(one of static/dynamic required)*
  A fixed literal value that the sync always starts from.

``dynamic_value`` *(one of static/dynamic required)*
  A ``SELECT`` query that returns a single row with a single column. The query is
  evaluated at sync time against the source database, allowing the start value to
  be computed dynamically (e.g. ``SELECT MAX(updated_at) - INTERVAL '7 days' FROM my_table``).

``drop_target_table`` *(optional, default: false)*
  When ``true``, the target table is dropped and recreated before merging. This
  effectively converts the partial sync into a filtered full replacement.

.. note::

   Exactly one of ``static_value`` or ``dynamic_value`` must be provided, not both.

.. warning::

   Defined PartialSync via ``sync_start_from`` is currently supported only for
   :ref:`tap-mysql` and :ref:`tap-postgres` to :ref:`target-snowflake`.

See the :ref:`tap-mysql` and :ref:`tap-postgres` connector pages for full YAML examples.
