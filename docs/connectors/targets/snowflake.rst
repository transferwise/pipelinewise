.. _target-snowflake:

Snowflake target
================

``target-snowflake`` loads Singer records from compatible taps through staged
CSV files into native or managed Iceberg v3 tables. It also supports FastSync
for selected database sources.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Target
     - Status
     - FastSync
   * - Snowflake
     - Available
     - Native FullSync from MariaDB/MySQL, PostgreSQL, or MongoDB; native or
       managed Iceberg v3 FullSync and PartialSync from MariaDB/MySQL or
       PostgreSQL


Prerequisites
-------------

Create these Snowflake objects before importing a pipeline:

- a warehouse used for loading;
- a target role and user;
- an external S3 stage; and
- a named CSV file format.

The role needs warehouse usage, database usage, schema creation, and usage on the
stage and file format. Grant ownership or table privileges only where the target
must create, merge, alter, or replace tables.

Example stage and file format:

.. code-block:: sql

   CREATE STAGE <database>.<schema>.<stage>
     URL = 's3://<bucket>';

   CREATE FILE FORMAT <database>.<schema>.<file_format>
     TYPE = 'CSV'
     ESCAPE = '\\'
     FIELD_OPTIONALLY_ENCLOSED_BY = '"';

Use a storage integration, instance role, or AWS profile where possible. If the
stage uses client-side encryption, configure the same master key in PipelineWise.

``aws_profile`` falls back to ``AWS_PROFILE``. ``aws_access_key_id``,
``aws_secret_access_key``, and ``aws_session_token`` fall back to
``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, and ``AWS_SESSION_TOKEN``.
Configure the access-key ID and secret together; add the session token for
temporary credentials. With none configured, Boto3 uses its default credential
chain.


Configuration
-------------

.. code-block:: yaml

   id: "snowflake"
   name: "Analytics Snowflake"
   type: "target-snowflake"
   db_conn:
     account: "<ACCOUNT>"
     dbname: "<DATABASE>"
     user: "<USER>"
     private_key: "/run/secrets/snowflake-key.pem"
     warehouse: "<WAREHOUSE>"
     role: "<ROLE>"
     s3_bucket: "<STAGING_BUCKET>"
     s3_key_prefix: "pipelinewise/"
     stage: "<SCHEMA>.<STAGE>"
     file_format: "<SCHEMA>.<FILE_FORMAT>"

.. list-table:: Connector-specific settings
   :header-rows: 1
   :widths: 28 18 18 36
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``account`` / ``dbname``
     - Yes
     - —
     - Snowflake account and target database.
   * - ``user`` / ``private_key``
     - Yes
     - —
     - Key-pair authentication for the target role.
   * - ``warehouse``
     - Yes
     - —
     - Warehouse used for load and merge statements.
   * - ``role``
     - No
     - User default role
     - Pins Singer, FastSync, recovery, and conversion to one Snowflake role.
       Set it explicitly when ownership or metadata visibility matters.
   * - ``s3_bucket`` / ``s3_key_prefix``
     - Yes
     - —
     - Staging location used by target loads.
   * - ``aws_profile``
     - No
     - ``AWS_PROFILE``
     - Selects a named profile when no static key pair is configured.
   * - ``aws_access_key_id`` / ``aws_secret_access_key``
     - No
     - AWS environment
     - Supplies a static credential pair; encrypt both YAML values.
   * - ``aws_session_token``
     - With temporary keys
     - ``AWS_SESSION_TOKEN``
     - Completes temporary static credentials.
   * - ``s3_acl``
     - No
     - None
     - Applies a canned ACL to staged uploads. Leave unset for
       bucket-owner-enforced buckets.
   * - ``stage`` / ``file_format``
     - Yes
     - —
     - Pre-created Snowflake objects used by ``COPY`` and ``MERGE``.
   * - ``client_side_encryption_master_key``
     - No
     - None
     - Encrypts staged files using the stage's matching master key.
   * - ``max_parallelism``
     - No
     - ``16``
     - Caps automatic Singer stream-flush threads. Configure this in the target
       ``db_conn``; tap-level ``parallelism_max`` is currently ineffective.

Generate the full template with ``pipelinewise init``. Common target and tap-side
batch settings are documented in :ref:`yaml_configuration`.

Managed Iceberg selection is tap-level. Target YAML rejects the removed
``iceberg_create`` setting and tap format/version keys. See
:ref:`snowflake_iceberg`.


String columns
--------------

``target-snowflake`` declares every new string column as
``VARCHAR(134217728)`` for native and managed Iceberg v3 tables. This applies
both when Singer creates a table and when it adds a column during schema
evolution. Snowflake's 128 MB encoded-value limit still applies, so a multi-byte
value can reach the byte limit before the declared character limit.

For an existing native table, ``target-snowflake`` leaves a compatible string
column at its current width. It does not widen or version that column solely
because its declared width is narrower. Existing managed Iceberg v3 strings are
different: every string column must already have the exact maximum width before
PipelineWise writes the table. See :ref:`snowflake_iceberg`.


Publication and grants
----------------------

MariaDB/MySQL and PostgreSQL FastSync apply configured select roles only after a
table is published. They do not grant schema-wide access while an obfuscated
``_TEMP`` staging table may exist. When adding a role, sync each existing table
that needs the role or grant it explicitly. MongoDB FastSync retains its legacy
schema-wide grant behaviour.


Iceberg tables
--------------

Any compatible Singer tap can load managed Iceberg v3 when it explicitly
selects that format. MariaDB/MySQL and PostgreSQL additionally support FastSync
FullSync and PartialSync for managed v3. Native tables remain the default.
PipelineWise can also build or promote one Iceberg copy of an existing native
table with ``copy_native_to_iceberg``. See :ref:`snowflake_iceberg` for
configuration, publication methods, metadata limits, writer exclusion, and
recovery.
