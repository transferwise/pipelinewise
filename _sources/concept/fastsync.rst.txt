.. _fast_sync_main:

FastSync
========

FastSync bypasses Singer JSON for supported bulk transfers and uses native
database export, staging, copy, and merge operations. It has two components:

.. list-table::
   :header-rows: 1
   :widths: 22 38 40
   :width: 100%

   * - Component
     - Selection
     - Target effect
   * - FullSync
     - Initial load, ``FULL_TABLE``, or explicit ``fast_sync``
     - Publishes a complete source-table copy.
   * - PartialSync
     - ``partial_sync_table`` or configured ``sync_start_from``
     - Merges a filtered source range into the existing target.

FastSync is not a replication method. ``LOG_BASED``, ``INCREMENTAL``, and
``FULL_TABLE`` remain the table's replication methods.


Supported routes
----------------

.. list-table::
   :header-rows: 1
   :widths: 28 28 22 22
   :width: 100%

   * - Source
     - Target
     - FullSync
     - PartialSync
   * - MariaDB / MySQL
     - Snowflake
     - Yes
     - Yes
   * - PostgreSQL
     - Snowflake
     - Yes
     - Yes
   * - MongoDB
     - Snowflake
     - Yes
     - No
   * - MariaDB / MySQL
     - PostgreSQL
     - Yes
     - No
   * - PostgreSQL
     - PostgreSQL
     - Yes
     - No
   * - MongoDB
     - PostgreSQL
     - Yes
     - No

Endpoint support status from :ref:`connector_support` still applies. A normal
``run_tap`` falls back to Singer when FullSync is unavailable. The explicit
``fast_sync`` command instead fails without loading data.


Automatic selection and handover
--------------------------------

During ``run_tap``, PipelineWise selects FullSync when:

- the table uses ``FULL_TABLE``;
- an ``INCREMENTAL`` table has no replication-key bookmark; or
- a ``LOG_BASED`` table has no LSN, binlog, GTID, or change-stream bookmark.

After a successful initial FullSync, PipelineWise writes the captured bookmark
and starts the Singer portion of the same ``run_tap`` invocation for incremental
or log-based tables. It does not wait for the next scheduled launch.

If FullSync fails, Singer does not advance that table past an incomplete initial
load. Restart the same command after correcting the failure.


Explicit FullSync
-----------------

``fast_sync`` resyncs every selected table regardless of its current bookmark:

.. code-block:: bash

   pipelinewise fast_sync \
     --tap <tap_id> \
     --target <target_id> \
     --tables <schema.table>

This operation can replace target data and reset replication bookmarks. Review
:ref:`resync` before running it against a large or actively written table.


.. _defined_partial_sync:

Configured PartialSync
----------------------

``sync_start_from`` makes explicit ``fast_sync`` use PartialSync for that table:

.. code-block:: yaml

   tables:
     - table_name: "orders"
       replication_method: "LOG_BASED"
       sync_start_from:
         column: "updated_at"
         static_value: "2024-01-01"
         drop_target_table: false

.. list-table:: Settings
   :header-rows: 1
   :widths: 28 22 50
   :width: 100%

   * - Setting
     - Required
     - Behaviour
   * - ``column``
     - Yes
     - Applies ``WHERE column >= value`` to the source export.
   * - ``static_value``
     - Exactly one value source
     - Uses the same literal boundary on every run.
   * - ``dynamic_value``
     - Exactly one value source
     - Runs a source query that must return one row and one column.
   * - ``drop_target_table``
     - No; default ``false``
     - Recreates the target before loading the filtered result.

Exactly one of ``static_value`` and ``dynamic_value`` is allowed. Configured
PartialSync is supported only from MariaDB/MySQL or PostgreSQL to Snowflake.

If a dynamic query returns no boundary, PipelineWise treats the partial range as
empty and completes successfully. Use a static boundary when an empty result
would hide a configuration or source-data problem.
