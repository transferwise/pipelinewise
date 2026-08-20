.. _yaml_configuration:

YAML configuration
==================

PipelineWise projects define global settings, targets, and taps in YAML.
``import_config`` validates these files, performs discovery, and generates the
connector JSON and state layout. Generated files below ``~/.pipelinewise`` are
runtime artifacts, not configuration source.


Project files
-------------

.. list-table::
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - File
     - Quantity
     - Purpose
   * - ``config.yml``
     - One
     - Global alerts, resync limit, switchover file, and backend database.
   * - ``target_*.yml``
     - One per destination
     - Target identity and connection.
   * - ``tap_*.yml``
     - One per source pipeline
     - Source, target reference, tables, replication, and optional transforms.

Run ``pipelinewise init --name <project>`` to generate templates. Templates can
include legacy connectors; check :ref:`connector_support` before using one.


Global configuration
--------------------

.. code-block:: yaml

   alert_handlers:
     slack:
       token: "{{ env_var['SLACK_BOT_TOKEN'] }}"
       channel: "#pipeline-alerts"

   allowed_resync_max_size:
     table_mb: 50000

   switch_over_data_file: "switch_over_data.json"

   backend_db:
     host: "backend.example.com"
     port: 5432
     user: "pipelinewise"
     password: "{{ env_var['BACKEND_PASSWORD'] }}"
     dbname: "pipelinewise"
     sslmode: "verify-full"
     connect_timeout: 10
     ddl_user: "pipelinewise_ddl"
     ddl_password: "{{ env_var['BACKEND_DDL_PASSWORD'] }}"

.. list-table:: Global settings
   :header-rows: 1
   :widths: 31 17 17 35
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``alert_handlers``
     - No
     - None
     - Configures Slack or VictorOps failure delivery. See :ref:`alerts`.
   * - ``allowed_resync_max_size`` / ``table_mb``
     - No
     - No limit
     - Blocks oversized MariaDB/MySQL or PostgreSQL-to-Snowflake FullSync unless
       ``--force`` is used.
   * - ``switch_over_data_file``
     - No
     - None
     - Supplies state mapping used by ``reset_state``.
   * - ``backend_db``
     - For data-diff
     - Disabled
     - Enables persisted data-diff definitions, runs, and coverage.
   * - ``backend_db.connect_timeout``
     - No
     - ``10`` seconds
     - Limits application-role connection attempts. The migration connection
       does not currently inherit this timeout.

The backend is a control-plane database, not a replication target. Separate its
service, database, runtime role, DDL role, credentials, and storage from every
target. See :ref:`data_diff`.


Tap configuration
-----------------

.. code-block:: yaml

   id: "orders"
   name: "Orders PostgreSQL"
   type: "tap-postgres"
   owner: "data-platform@example.com"
   db_conn:
     host: "postgres.example.com"
     port: 5432
     user: "pipelinewise"
     password: "{{ env_var['POSTGRES_PASSWORD'] }}"
     dbname: "orders"
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   add_metadata_columns: true
   hard_delete: true
   schemas:
     - source_schema: "public"
       target_schema: "repl_orders"
       target_schema_select_permissions:
         - "analytics_reader"
       tables:
         - table_name: "orders"
           replication_method: "INCREMENTAL"
           replication_key: "updated_at"

.. list-table:: Common tap settings
   :header-rows: 1
   :widths: 28 18 18 36
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``id`` / ``name`` / ``type``
     - Yes
     - —
     - Identify the pipeline and source connector.
   * - ``owner``
     - Operationally required
     - None
     - Identifies the source-data owner.
   * - ``db_conn``
     - Yes
     - —
     - Connector-specific source connection. See :ref:`taps_list`.
   * - ``target``
     - Yes
     - —
     - Must match a target YAML ``id``.
   * - ``batch_size_rows``
     - No
     - ``20000``
     - Target batch size; schema accepts 1,000–5,000,000.
   * - ``stream_buffer_size``
     - No
     - ``0``
     - ``mbuffer`` memory in MB; ``0`` disables it, values 1–9 are rounded up
       to 10 MB, and the maximum is 2500 MB. See :ref:`stream_buffering`.
   * - ``batch_wait_limit_seconds``
     - No
     - None
     - Flushes a partial Snowflake batch after the limit.
   * - ``send_alert``
     - No
     - ``true``
     - Suppresses tap and data-diff alerts when ``false``.
   * - ``slack_alert_channel``
     - No
     - Global channel
     - Adds a tap-specific Slack destination; must start with ``#``.
   * - ``add_metadata_columns`` / ``hard_delete``
     - No
     - See metadata guide
     - Controls target metadata and delete handling. See
       :ref:`metadata_columns`.

.. list-table:: Target-loading settings inherited from the tap
   :header-rows: 1
   :widths: 31 18 51
   :width: 100%

   * - Setting
     - Default
     - Effect
   * - ``parallelism``
     - ``0``
     - Controls Singer target flush threads. ``0`` selects one thread per
       buffered stream up to the target's effective cap.
   * - ``parallelism_max``
     - Generated as ``4``
     - Currently ineffective: PipelineWise emits this name, but the PostgreSQL
       and Snowflake targets read ``max_parallelism`` instead.
   * - ``flush_all_streams``
     - ``false``
     - Flushes every buffered stream when one stream reaches its batch boundary.
   * - ``primary_key_required``
     - ``true``
     - Rejects streams without a target merge key when enabled.
   * - ``default_target_schema``
     - None
     - Supplies a target schema for taps without database-style mappings.
   * - ``default_target_schema_select_permissions``
     - None
     - Applies default target read roles when supported.
   * - ``data_flattening_max_level``
     - Tap-specific or ``0``
     - Expands nested objects into columns. ``0`` keeps flattening disabled;
       higher values can create wide and changing target schemas. Managed
       Iceberg FastSync requires ``0``; Singer-only Iceberg routes retain the
       tap's normal setting.
   * - ``target_table_format``
     - Omitted
     - Selects ``native`` or managed ``iceberg`` Snowflake tables for the
       Singer target and supported FastSync routes. An omitted value or
       ``native`` creates native tables; managed Iceberg creation requires
       explicit ``iceberg``. The setting applies to every table in the tap.
   * - ``iceberg_version``
     - None
     - Managed-Iceberg version discriminator. Its only supported value is integer
       ``3``; it is required with ``target_table_format: iceberg`` and invalid
       otherwise.
   * - ``validate_records``
     - ``false``
     - Validates Singer records against their emitted schema before loading.
   * - ``split_large_files``
     - ``false``
     - Splits FastSync export archives for Snowflake staging.
   * - ``split_file_chunk_size_mb``
     - ``1000``
     - Sets the split archive chunk size.
   * - ``split_file_max_chunks``
     - ``20``
     - Limits the number of generated chunks.
   * - ``archive_load_files``
     - ``false``
     - Retains Snowflake target load files in a separate S3 archive.
   * - ``archive_load_files_s3_bucket`` / ``archive_load_files_s3_prefix``
     - None
     - Selects the archive destination when load-file archiving is enabled.

.. important::

   Any tap whose Singer output is compatible with ``target-snowflake`` can
   select managed Iceberg v3. FastSync FullSync and PartialSync for managed v3
   remain limited to ``tap-mysql`` (MariaDB/MySQL) and ``tap-postgres``. Every
   explicit v3 route requires ``hard_delete: true``; those two FastSync-capable
   taps also require ``data_flattening_max_level: 0``. Singer-only sources retain
   their normal flattening setting, including Salesforce's default level ``10``.
   Native remains the default, and PipelineWise does not convert an existing
   table when the requested format conflicts. Every managed Iceberg version
   other than v3 is rejected before mutation. See :ref:`snowflake_iceberg`.

   Managed Iceberg selection is tap-level. Target YAML rejects these keys and
   the removed ``iceberg_create`` setting. Remove ``iceberg_create`` and
   configure each managed Iceberg tap before upgrading.

.. warning::

   The effective automatic Singer flush cap is currently 16, not 4. To change
   it, set ``max_parallelism`` inside the target's ``db_conn``. Do not rely on
   tap-level ``parallelism_max`` until the naming mismatch is fixed.


Schemas and tables
------------------

``schemas`` maps source schemas into target schemas. Every table needs a
``table_name``. Database tables also select a replication method:

.. list-table:: Table settings
   :header-rows: 1
   :widths: 28 20 52
   :width: 100%

   * - Setting
     - Required
     - Behaviour
   * - ``replication_method``
     - Database tables
     - ``LOG_BASED``, ``INCREMENTAL``, or ``FULL_TABLE``. See
       :ref:`replication_methods`.
   * - ``replication_key``
     - For ``INCREMENTAL``
     - Increasing source column used as the bookmark.
   * - ``sync_start_from``
     - No
     - Routes explicit FastSync through PartialSync. See
       :ref:`defined_partial_sync`.
   * - ``transformations``
     - No
     - Applies load-time field protection. See :ref:`transformations`.
   * - ``data_diff``
     - No
     - Configures independent aggregate reconciliation. See :ref:`data_diff`.

Connector-specific schema or table mappings, such as ``s3_csv_mapping``, are
documented on their connector page.


Target configuration
--------------------

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
     s3_bucket: "<BUCKET>"
     s3_key_prefix: "pipelinewise/"
     stage: "<SCHEMA>.<STAGE>"
     file_format: "<SCHEMA>.<FILE_FORMAT>"

``id``, ``name``, ``type``, and ``db_conn`` are required. Connection fields are
target-specific; see :ref:`targets_list`.


Secrets and validation
----------------------

Inject environment values with Jinja:

.. code-block:: yaml

   password: "{{ env_var['POSTGRES_PASSWORD'] }}"

Ansible Vault values are also supported; see :ref:`encrypting_passwords`.

After every change:

.. code-block:: bash

   pipelinewise validate --dir <project>
   pipelinewise import_config --dir <project>

``validate`` checks YAML and references but not connectivity. ``import_config``
discovers the source and replaces generated runtime configuration only after the
project passes validation.

.. warning::

   ``import_config`` treats a missing tap or target YAML file as a deletion. It
   removes that connector's generated directory, including every ``state.json``
   bookmark; removing a target removes all of its taps. Removing a PostgreSQL
   tap also drops its replication slot. Renaming an ``id`` has the same effect
   as deleting the old connector and adding a new one. Stop the pipeline, back
   up its generated state, and plan a full initial sync before removing or
   renaming imported configuration.
