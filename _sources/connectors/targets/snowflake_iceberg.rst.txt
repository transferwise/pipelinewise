.. _snowflake_iceberg:

Snowflake Iceberg tables
========================

PipelineWise can create new managed Snowflake Iceberg tables through the Singer
target path or copy an existing native table into an Iceberg table with the
``copy-native-to-iceberg`` utility.


Support
-------

.. list-table::
   :header-rows: 1
   :widths: 38 24 38
   :width: 100%

   * - Operation
     - Status
     - Behaviour
   * - Singer creates a new table
     - Available
     - ``iceberg_create: true`` creates a managed Iceberg table.
   * - FastSync or PartialSync
     - Unavailable
     - Fails rather than replacing or merging an Iceberg table.
   * - Convert an existing native table
     - Available utility
     - Copies data into a companion table, then optionally promotes it.


Snowflake prerequisites
-----------------------

Configure a default catalog and external volume on the target database:

.. code-block:: sql

   CREATE OR REPLACE EXTERNAL VOLUME <external_volume> ...;
   ALTER DATABASE <target_database> SET CATALOG = 'snowflake';
   ALTER DATABASE <target_database>
     SET EXTERNAL_VOLUME = <external_volume>;

The target role needs the parent-object, warehouse, catalog, external-volume,
and ``CREATE ICEBERG TABLE`` privileges required by Snowflake. Follow Snowflake's
`managed Iceberg table documentation
<https://docs.snowflake.com/en/user-guide/tables-iceberg>`_ for the account-level
setup.


Create new Iceberg tables
-------------------------

Set ``iceberg_create`` in the target YAML before the Singer target creates the
table:

.. code-block:: yaml

   db_conn:
     iceberg_create: true

The setting affects new tables only. It does not convert an existing native
table. Do not use ``fast_sync`` or ``partial_sync_table`` for a table that exists
as Iceberg.


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
