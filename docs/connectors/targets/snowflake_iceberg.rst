.. _snowflake_iceberg:

Snowflake Iceberg tables
========================

PipelineWise supports a Snowflake Iceberg table only when all conditions hold:

* The table is Snowflake-managed Iceberg version 3.
* ``ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED'`` is set explicitly at table
  level, so Snowflake uses copy-on-write.
* Every string column has physical ``CHARACTER_MAXIMUM_LENGTH`` 134,217,728.
* The table and every replicated column were created by PipelineWise FullSync,
  PartialSync, ``target-snowflake``, or the supported native-to-Iceberg converter.

PipelineWise rejects other Iceberg versions, external catalogs, and tables that
do not meet the copy-on-write or string-width requirement before writing.

Singer can load managed v3 from any source compatible with
``target-snowflake``. MariaDB, MySQL, and PostgreSQL also support FastSync
FullSync and PartialSync. Native Snowflake tables remain the default.


Support
-------

.. list-table:: Supported paths to managed Iceberg v3
   :header-rows: 1
   :widths: 31 23 23 23
   :width: 100%

   * - Source
     - Singer
     - FastSync FullSync
     - FastSync PartialSync
   * - MariaDB
     - Yes
     - Yes
     - Yes
   * - MySQL
     - Yes
     - Yes
     - Yes
   * - PostgreSQL
     - Yes
     - Yes
     - Yes
   * - Other compatible Singer sources, for example Salesforce
     - Yes
     - No
     - No

Connector support levels still apply and are listed in
:ref:`connector_support`. A source outside MariaDB, MySQL, and PostgreSQL can use
managed Iceberg v3 only when PipelineWise routes the selected streams through
Singer. Any Iceberg selection that would invoke another source's FastSync
component is rejected before mutation.


Snowflake prerequisites
-----------------------

PipelineWise creates explicit v3 tables with ``CATALOG = 'SNOWFLAKE'`` and no
``EXTERNAL_VOLUME`` clause. Snowflake therefore uses the effective schema,
database, or account default, or Snowflake-managed storage when no other default
is set. New tables use one day of data retention, a 16 MB target file size, and
automatic data compaction; these values are not configurable per tap.

PipelineWise sets the required table parameter on every managed v3 table it
creates or replaces. Snowflake ``UPDATE``, ``DELETE``, and ``MERGE`` therefore
use copy-on-write and do not create deletion vectors. PipelineWise does not use
the deprecated ``ENABLE_ICEBERG_MERGE_ON_READ`` parameter.

PipelineWise creates and adds managed-v3 string columns explicitly as
``VARCHAR(134217728)``. This is Snowflake's maximum declared character width;
the 128 MB encoded-value limit still applies, so multi-byte values can reach the
byte limit with fewer characters. Existing managed-v3 strings must have the same
physical width before PipelineWise writes them.

.. warning::

   PipelineWise must be the only writer. External reads are allowed, but an
   external writer can choose a write mode or delete representation outside the
   Snowflake parameter and break the copy-on-write contract.

To use your own cloud storage, create an external volume and configure it as a
default on the schema, database, or account. For example:

.. code-block:: sql

   CREATE OR REPLACE EXTERNAL VOLUME <external_volume> ...;
   ALTER DATABASE <target_database>
     SET EXTERNAL_VOLUME = <external_volume>;

The target role needs the parent-object, warehouse, staging, and table privileges
required by the selected operation, including ``CREATE ICEBERG TABLE``. A custom
external volume also requires its applicable privileges. PipelineWise reports
Snowflake privilege and storage errors rather than attempting to reconfigure the
account. See Snowflake's `managed Iceberg table documentation
<https://docs.snowflake.com/en/user-guide/tables-iceberg>`_.

Metadata preflight can inspect only objects visible to the target role. Use a role
that can see dependent streams, policies, tags, grants, and comments before an
operation that might replace a table.


Configure a tap
---------------

Set the desired format in any tap whose Singer output is compatible with
``target-snowflake``. The setting applies to every selected table in that tap:

.. code-block:: yaml

   target: "snowflake"
   target_table_format: iceberg
   iceberg_version: 3
   hard_delete: true

These tap-level settings are the only valid Iceberg selection. Omitting
``target_table_format`` or selecting ``native`` creates native tables. Target
configuration rejects these keys and the removed ``iceberg_create`` setting;
remove ``iceberg_create`` and configure each Iceberg tap before upgrading.

Every explicit managed Iceberg v3 tap requires ``hard_delete: true``. MariaDB,
MySQL, and PostgreSQL taps also require ``data_flattening_max_level: 0`` because
their initial load can use FastSync before handing the stream to Singer.
Singer-only sources retain their normal flattening setting; for example,
Salesforce's default level ``10`` is valid. A physical format mismatch fails
before mutation. PipelineWise does not upgrade or automatically convert tables;
use the manual command below during a controlled outage.

With flattening disabled, nested objects and arrays use ``VARIANT`` only with
explicit v3 configuration. A Singer-only tap with a non-zero flattening level
continues to apply its normal target flattening behavior. Keep
``target_table_format`` and ``iceberg_version`` for the tap's lifetime. Removing
them selects the native contract and fails on an existing Iceberg table rather
than converting it silently.

Explicit v3 maps integer Singer fields to ``NUMBER(38,0)`` and approximate
numeric fields to Iceberg ``DOUBLE``. The latter preserves 64-bit floating-point
range instead of narrowing to Iceberg ``FLOAT``. Native and fixed-point
``NUMBER(precision, scale)`` mappings are unchanged. PostgreSQL
``hstore`` maps to ``VARIANT`` on this route. For ``engine: mariadb``, a
``LONGTEXT`` column with MariaDB's exact generated ``JSON_VALID`` constraint is
treated as the ``JSON`` alias and maps to ``VARIANT``; ordinary ``LONGTEXT`` and
native routes remain strings. Object, array, string, number, Boolean, and null
JSON roots are carried as validated JSON text and restored as ``VARIANT``;
JSON null remains distinct from SQL ``NULL``.


Repair a PipelineWise-created managed v3 table
----------------------------------------------

PipelineWise does not adopt arbitrary externally created tables or columns.
Before upgrading a managed v3 table created by an earlier PipelineWise version,
or after a coordinated DBA repair to such a table, inspect its parameter and
column widths:

.. code-block:: sql

   SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR'
     IN TABLE <database>.<schema>.<table>;

   SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
     FROM <database>.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '<schema>'
      AND TABLE_NAME = '<table>'
    ORDER BY ORDINAL_POSITION;

PipelineWise requires value ``DISABLED`` at level ``TABLE``; inherited values do
not qualify. Every string row must also report ``CHARACTER_MAXIMUM_LENGTH``
134217728. Singer and FastSync reject the table before writing when either
contract fails. Set the parameter and widen each narrower string column before
starting PipelineWise:

.. code-block:: sql

   ALTER ICEBERG TABLE <database>.<schema>.<table>
     SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED';

   ALTER ICEBERG TABLE <database>.<schema>.<table>
     ALTER COLUMN <column> SET DATA TYPE VARCHAR(134217728);

PipelineWise does not widen existing Iceberg columns automatically. Use a role
authorized to repair the existing PipelineWise-created table, or recreate it
through PipelineWise FullSync or the supported native-to-Iceberg converter, then
retry the same operation. Do not introduce a new table or column through DBA DDL.

The change affects future Snowflake DML only; it does not rewrite the current
snapshot or remove deletion vectors already referenced there. If the current
snapshot must be deletion-vector-free, follow the change with a complete
FullSync or controlled full-table replacement. PartialSync is insufficient.
Historical snapshots and files can remain until retention cleanup. See
Snowflake's `ALTER ICEBERG TABLE reference
<https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table>`_.


FullSync publication
--------------------

FastSync always exports into a uniquely named native staging table. PipelineWise
then chooses one Iceberg publication method:

.. list-table:: FullSync publication
   :header-rows: 1
   :widths: 35 30 35
   :width: 100%

   * - Target state
     - Method
     - Effect
   * - Missing
     - Explicit-schema CTAS
     - Creates a managed Iceberg v3 table from staging.
   * - Exactly compatible
     - ``INSERT OVERWRITE``
     - Preserves the existing table object and its metadata.
   * - New nullable columns only
     - Add columns, then ``INSERT OVERWRITE``
     - Preserves the object and adds the compatible schema.
   * - Other compatible-format mismatch
     - Guarded replacement CTAS
     - Replaces the table only after metadata safety checks.

FullSync supports tables without a primary key. Where one exists, PipelineWise
preserves composite primary-key order and its Iceberg identifier fields, and
rejects NULL key values.
After a successful initial load, LOG_BASED and INCREMENTAL tables continue with
Singer in the same ``run_tap`` invocation, using the same managed-v3 contract.

A guarded replacement preserves explicit grants and table tags through Snowflake
copy clauses, then restores table and column comments. It fails before replacement
when visible dependent streams, secondary constraints, inbound foreign keys,
masking or row-access policies, direct column tags, defaults, identity columns,
or clustering are present. The current owning account role must run the
replacement, so ownership is retained; database-role ownership is not supported.
The new object resets table history and can change its base location. Account-wide
stream inspection is limited to dependencies visible to that role, so the role
must be able to see every relevant dependency.


PartialSync publication
-----------------------

A missing Iceberg target is created from the selected range. An existing
compatible target is updated through one transaction. PartialSync requires a
primary key and supports only ``hard_delete: true``.

Compatible nullable columns are added before the merge. Any other schema or key
mismatch fails before DML. Setting ``drop_target_table: true`` explicitly selects
replacement CTAS and leaves the table containing only the selected range.

Before normal or recovered publication, PipelineWise validates the canonical
post-transformation staging projection. Every composite primary-key component
must be non-NULL and every composite key group must be unique. A violation
blocks new publication DML; PipelineWise does not deduplicate staging or choose
a winning row. If an already-submitted attempt fails this recheck, its outcome
remains ambiguous and PipelineWise preserves the manifest and staging table for
manual recovery.

See :ref:`partial_sync_cases` for boundaries, delete behaviour, and operational
checks.


.. _snowflake_iceberg_recovery:

Concurrency and recovery
------------------------

PipelineWise holds a process lock for the complete Iceberg attempt and rejects a
changed target before publication. The lock coordinates only processes sharing
the generated runtime directory; enforce the sole-writer requirement across
other deployments and systems operationally.

Each FastSync attempt has a credential-free
``iceberg-recovery-<hash>.json`` stream manifest and
``iceberg-fastsync-target-<hash>.json`` target pointer in the generated target
runtime directory at ``$PIPELINEWISE_CONFIG_DIRECTORY/<target_id>/``. A stable
source-stream identity finds the attempt after target-mapping changes, while the
target pointer prevents another source from starting against the same physical
table. PipelineWise also holds a physical-target lock. Recovery is bound to the
tap ID, source route and table, source endpoint and extraction settings, target
mapping and account/user/role, staging configuration, and transformation
contract. Passwords, private keys, and raw transformation rules are not stored.

The manifest records ``prepared``, ``uploaded``, ``staging_created``, ``staged``,
``submitted``, ``published``, and ``finalized`` progress, including the saved
source boundary, staging identity, expected schema and content fingerprint,
publication identity, and completed finalization actions. PipelineWise advances
Singer state only after publication, metadata and grants, S3 cleanup, and staging
cleanup succeed. An interrupted additive change can leave a retry-safe nullable
column while target data and state remain unchanged.

If a connection fails after submission, new target data may be visible while
state remains unchanged. Restart the same command without editing state, removing
staging, or deleting either recovery file. Use the same generated runtime
directory, tap ID, route, source table, target mapping, account/user/role,
staging configuration, and transformation contract.

For CTAS and ``INSERT OVERWRITE``, PipelineWise uses a 60-second query-history
polling budget. Each lookup scans at most 10,000 visible completed queries in a
fixed window from five minutes before the persisted submission time through the
earlier of the current time or 24 hours after submission. Connector timeouts are
best-effort, so an individual Snowflake call can make the observed wall-clock
duration exceed the nominal budget. PipelineWise requires the exact attempt,
verifies the target, resumes unfinished finalization, and then performs the saved
state handover. Query-history visibility can lag. For an ambiguous PartialSync
commit, PipelineWise replays the persisted range transaction deterministically
instead of resolving its boundaries again. Retry the same command if evidence is
not yet visible and while Snowflake retains the Information Schema query history.
If the result remains ambiguous, PipelineWise preserves the evidence and fails
for operator inspection.


Convert a native table
----------------------

The PipelineWise-owned command builds and validates one managed Iceberg v3 copy
using an imported Snowflake target:

.. code-block:: bash

   pipelinewise copy_native_to_iceberg \
     --target <target_id> \
     --table <database.schema.table> \
     --eventual native \
     --iceberg-version 3

``--eventual`` defaults to ``native``. The command has no tap argument and only
accepts a table in the target's configured database.

.. list-table:: Conversion outcomes
   :header-rows: 1
   :widths: 24 38 38
   :width: 100%

   * - Mode
     - Primary name after success
     - Companion
   * - ``native``
     - Original native ``<table>``
     - Validated ``<table>_ICEBERG``
   * - ``iceberg``
     - Promoted Iceberg ``<table>``
     - Original ``<table>_NATIVE`` rollback point

The ``_NATIVE`` and ``_ICEBERG`` suffixes are reserved. Rename independently
managed objects with those names before conversion. The PipelineWise-owned
command is the only supported native-to-Iceberg converter; the old connector-local
``copy-native-to-iceberg`` executable has been removed.

.. warning::

   Complete or manually reconcile any interrupted connector-local conversion
   before upgrading. The PipelineWise-owned command cannot adopt the old
   unmanifested state when the primary name is absent and ``_NATIVE`` plus
   ``_ICEBERG`` objects remain. Reinstall or recreate the target-snowflake
   environment during deployment; a source-only update can leave a stale
   generated console wrapper that no longer has a Python implementation.


Conversion preflight and metadata
---------------------------------

Before conversion:

1. stop PipelineWise replication and every other writer to the table;
2. record source and target evidence plus required metadata;
3. use the current owning account role, with access to unmasked source rows and
   all relevant metadata;
4. confirm the role can create, alter, rename, grant, tag, and comment; and
5. for ``eventual=iceberg``, also stop every reader, including dashboards,
   transformations, and ad-hoc queries, before cutover; and
6. keep readers and writers stopped until the result is independently validated.

The command compares row count and ``HASH_AGG`` evidence before cutover. It
copies every row through the required type projection, including duplicate-key
and other representable flawed data, without filtering, deduplication, or
repair. It preserves supported types, nullability, primary-key order, table and
column comments, explicit non-ownership grants to account roles and qualified
database roles, grant options, and direct table tags. The current account role
must own the native source; database-role ownership is not supported.

Conversion fails before copying when the table has visible dependent streams,
policies, direct column tags, secondary constraints, inbound foreign keys,
defaults, identity columns, a clustering key, NULL primary-key values, or an
unsupported Iceberg type. Account-wide stream inspection is limited to objects
visible to the current role. Time and timestamp precision is normalized to
microseconds. Snowflake ``VARIANT`` remains ``VARIANT``.

Writes during the copy or cutover can be absent from the Iceberg result. The
local manifest lock does not replace the required external writer exclusion.
For ``eventual=iceberg``, promotion renames ``<table>`` to ``<table>_NATIVE``
and then renames ``<table>_ICEBERG`` to ``<table>``. These are independent
statements: the primary table name is absent between them, so concurrent readers
fail instead of seeing stale data. Rollback has the same availability gap in
reverse. Use ``eventual=native`` when a reader outage is not acceptable.


Interrupted conversion
----------------------

Retry from the same imported target runtime directory with the same table,
``--eventual`` value, target account/database/user, and account role. The command
reuses its durable manifest, verifies companion-table state and contents, and
resumes a committed rename. If validation or Iceberg promotion fails, PipelineWise
records rollback intent before renaming and restores the native table name across
retries. It never deletes the ``_NATIVE`` rollback table.

An interruption after the first promotion or rollback rename can leave the
primary name absent until recovery runs. Keep the reader-and-writer outage in
place and retry the identical command immediately. Do not resume readers merely
because writers are stopped.

If the primary and companion names cannot prove one safe state, the command fails.
Inspect all three tables and the manifest before changing anything manually.


Validation
----------

Before resuming readers or writers, compare exact primary-key coverage or a
deterministic checksum, critical values, physical format and version, ownership,
grants, tags, comments, and downstream access. For ``eventual=iceberg``, retain
``_NATIVE`` until at least one Singer or FastSync cycle and independent
reconciliation have succeeded.
