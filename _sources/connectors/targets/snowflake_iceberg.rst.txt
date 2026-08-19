.. _snowflake_iceberg:

Snowflake Iceberg tables
========================

The target-snowflake Singer connector can create managed Snowflake Iceberg v3
tables. This is connector groundwork for later FastSync support, not a complete
RDBMS-to-Iceberg route. Fresh RDBMS taps normally require FullSync before Singer,
and that path remains unavailable. The existing ``copy-native-to-iceberg``
utility can copy a native table separately.


Support
-------

.. list-table::
   :header-rows: 1
   :widths: 38 24 38
   :width: 100%

   * - Operation
     - Status
     - Behaviour
   * - Singer with explicit v3 tap configuration
     - Connector groundwork
     - When Singer runs without a FastSync selection, creates a managed Iceberg
       v3 table and stores objects and arrays as ``VARIANT``.
   * - Singer with target-level ``iceberg_create``
     - Deprecated compatibility
     - Preserves the existing Iceberg mapping, including objects and arrays as
       ``TEXT``.
   * - FastSync or PartialSync
     - Unavailable
     - Explicit tap-level Iceberg configuration fails before creating or
       changing a target table.
   * - Convert an existing native table
     - Available utility
     - Copies data into a companion table, then optionally promotes it.


Snowflake prerequisites
-----------------------

Explicit v3 creation sets ``CATALOG = 'SNOWFLAKE'`` and leaves
``EXTERNAL_VOLUME`` unset. Snowflake uses the effective schema, database, or
account default. If no different default is configured, Snowflake-managed
storage is used.

To use your own cloud storage, create an external volume and configure it as a
default on the schema, database, or account. For example:

.. code-block:: sql

   CREATE OR REPLACE EXTERNAL VOLUME <external_volume> ...;
   ALTER DATABASE <target_database>
     SET EXTERNAL_VOLUME = <external_volume>;

The target role needs the parent-object, warehouse, and ``CREATE ICEBERG TABLE``
privileges required by Snowflake. A custom external volume also requires its
applicable privileges. PipelineWise does not preflight these privileges; a
failed ``CREATE ICEBERG TABLE`` reports the Snowflake error. Follow Snowflake's
`managed Iceberg table documentation
<https://docs.snowflake.com/en/user-guide/tables-iceberg>`_ for the account-level
setup.


Create new Singer tables
------------------------

Set the desired format in the tap YAML. It applies to every selected table in
that tap:

.. code-block:: yaml

   target: "snowflake"
   target_table_format: iceberg
   iceberg_version: 3

When tap-level format and the legacy target flag are omitted, new tables are
native. ``target_table_format`` accepts only ``native`` or ``iceberg``. Iceberg
requires integer version ``3``; a version is invalid when the format is native
or omitted.

Explicit Iceberg v3 configuration is limited to ``tap-mysql`` (MariaDB/MySQL)
and ``tap-postgres`` with ``target-snowflake``. It requires
``data_flattening_max_level: 0`` and ``hard_delete: true``. The deprecated
``hard_delete: false`` behaviour is rejected for this route.

An explicit setting must match an existing table. PipelineWise does not convert
a native table, change an Iceberg table's catalog, or upgrade Iceberg v2. Nested
objects and arrays use ``VARIANT`` only with the explicit v3 configuration.
Keep both settings while replicating to a v3 table. Removing them restores the
legacy ``TEXT`` mapping; PipelineWise stops on existing ``VARIANT`` columns and
asks the operator to restore the explicit v3 configuration rather than changing
the columns automatically.

Fresh LOG_BASED and INCREMENTAL RDBMS taps normally select FullSync before
Singer. With explicit tap-level Iceberg configuration, that FullSync fails
before target mutation and does not fall back to Singer. ``fast_sync`` and
``partial_sync_table`` are also unavailable for that configuration in this
release.


Legacy target setting
---------------------

The deprecated target-level setting remains available during migration:

.. code-block:: yaml

   db_conn:
     iceberg_create: true

When tap-level format is omitted, PipelineWise preserves this setting's current
new-table and type-mapping behaviour. The legacy setting affects missing tables;
an existing physical table continues to win. When both settings are present,
they must agree. Only an explicit tap-level format must match an existing table;
neither setting converts it.


Convert a native table
----------------------

Before conversion:

1. stop PipelineWise replication and every other writer to the source table;
2. record row counts, primary-key coverage, ownership, grants, policies, tags,
   comments, constraints, defaults, clustering, and other metadata;
3. choose whether the native or Iceberg name should be primary after the copy;
4. use a role that can read every row and see unmasked values; and
5. keep writers stopped until the copied table and metadata are validated.

Writes during copy or cutover can be absent from the Iceberg result. Row-access
or masking policies applied to ``INSERT ... SELECT`` can copy an incomplete or
permanently masked result.

.. code-block:: bash

   copy-native-to-iceberg \
     --config <target-snowflake-config.json> \
     --fqtn <database.schema.table> \
     --eventual NATIVE

.. list-table:: Cutover modes
   :header-rows: 1
   :widths: 24 38 38
   :width: 100%

   * - Mode
     - Primary name after success
     - Companion
   * - ``NATIVE``
     - Original native ``<table>``
     - Copied ``<table>_ICEBERG``
   * - ``ICEBERG``
     - Promoted Iceberg ``<table>``
     - Original ``<table>_NATIVE``

The ``<table>_NATIVE`` and ``<table>_ICEBERG`` names are reserved. Rename
independently managed objects with those names before running the utility.


Copied data and metadata
------------------------

The utility copies rows, column names, compatible types, and primary-key column
order. It applies these type conversions:

.. list-table::
   :header-rows: 1
   :widths: 42 58
   :width: 100%

   * - Native type
     - Iceberg type
   * - ``TEXT``
     - ``VARCHAR``
   * - ``VARIANT``
     - ``TEXT`` representation
   * - ``TIMESTAMP_TZ``
     - ``TIMESTAMP_LTZ(6)``
   * - ``TIMESTAMP_NTZ``
     - ``TIMESTAMP_NTZ(6)``
   * - ``TIMESTAMP_LTZ``
     - ``TIMESTAMP_LTZ(6)``
   * - ``TIME``
     - ``TIME(6)``

Timezone offsets and precision finer than microseconds are not retained. The
utility does not copy ownership, grants, masking or row-access policies, tags,
comments, nullability, default or autoincrement expressions, clustering,
secondary constraints, or other table metadata. Reapply and validate required
metadata before consumers or replication resume.


Interrupted conversion recovery
-------------------------------

The utility fails safely when it cannot confirm a rename. A retry handles these
identifiable states:

- ``<table>`` is already Iceberg: return successfully.
- ``<table>`` is absent and both companions exist: promote the loaded Iceberg
  table for ``ICEBERG`` mode, or restore the native name for ``NATIVE`` mode.
- ``<table>`` is absent and only ``<table>_NATIVE`` exists: restore the native
  name before copying again.

For any other state, inspect all three object names and recover the intended
``<table>`` manually before retrying. The utility removes a stale
``<table>_ICEBERG`` before a new copy but never deletes ``<table>_NATIVE``.


Validation
----------

Before resuming writers, compare exact primary-key coverage, row counts, critical
values, and required metadata. Confirm downstream roles can query the promoted
table and that PipelineWise uses the Singer path. Retain the native companion
until the Iceberg cutover has been observed through at least one successful
replication and reconciliation cycle.
