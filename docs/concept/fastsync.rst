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
   :widths: 23 22 25 15 15
   :width: 100%

   * - Source
     - Target
     - Table formats
     - FullSync
     - PartialSync
   * - MariaDB / MySQL
     - Snowflake
     - Native, managed Iceberg v3
     - Yes
     - Yes
   * - PostgreSQL
     - Snowflake
     - Native, managed Iceberg v3
     - Yes
     - Yes
   * - MongoDB
     - Snowflake
     - Native
     - Yes
     - No
   * - MariaDB / MySQL
     - PostgreSQL
     - Native
     - Yes
     - No
   * - PostgreSQL
     - PostgreSQL
     - Native
     - Yes
     - No
   * - MongoDB
     - PostgreSQL
     - Native
     - Yes
     - No

Endpoint support status from :ref:`connector_support` still applies. For a
route without a FastSync component, a normal ``run_tap`` falls back to Singer.
The explicit ``fast_sync`` command instead fails without loading data.


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


Snowflake Iceberg publication
-----------------------------

MariaDB/MySQL and PostgreSQL FastSync use native staging for managed Iceberg v3.
A missing table is created through explicit-schema CTAS. An exactly compatible
FullSync target uses ``INSERT OVERWRITE``; a compatible new nullable column is
added first. Other FullSync mismatches require guarded replacement. PartialSync
uses transactional range DML and requires a primary key.

The route requires explicit ``target_table_format: iceberg``,
``iceberg_version: 3``, ``data_flattening_max_level: 0``, and
``hard_delete: true``. Native remains the default. See
:ref:`snowflake_iceberg` for metadata limits, writer exclusion, and recovery.
After an eligible initial load, Singer continues LOG_BASED or INCREMENTAL
replication against the same managed-v3 table in the same run.
FastSync availability and its zero-flattening requirement are specific to these
routes. A compatible Singer-only source such as Salesforce can load managed v3
through ``target-snowflake`` without gaining a FastSync component; it retains
its normal flattening setting, still requires ``hard_delete: true``, and sends
``FULL_TABLE`` streams through Singer.


Snowflake string widths
-----------------------

MariaDB/MySQL and PostgreSQL FastSync declare string staging and new-target
columns as ``VARCHAR(134217728)`` for both native and managed Iceberg v3 routes.
This applies to character and text families, MariaDB/MySQL blob and enum types,
and the fallback for an otherwise unmapped source type. It avoids Snowflake's
narrower 16,777,216-character default for a bare ``VARCHAR``.

The declared limit does not remove Snowflake's 128 MB encoded-value limit, so a
value can reach the byte limit before the character limit when it contains
multi-byte characters. Values above Snowflake's maximum still fail while loading
the native staging table, before publication or state advancement.
See Snowflake's `string and binary data type reference
<https://docs.snowflake.com/en/sql-reference/data-types-text>`_.

An existing native PartialSync target whose compatible text column is narrower
is widened before the merge. Existing managed Iceberg v3 string columns must
already use the maximum width; see :ref:`snowflake_iceberg`.

Singer schema evolution has a different existing-native policy:
``target-snowflake`` uses the maximum width for new string columns but does not
widen or version a compatible existing native string column solely because of
its declared width. See :ref:`target-snowflake`.


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
