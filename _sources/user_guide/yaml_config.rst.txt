
.. _yaml_configuration:

YAML Configuration
------------------

PipelineWise uses YAML files to define pipelines and global settings. These files are the
main input for generating the JSON configuration that the underlying Singer.io components
require. You never need to edit the JSON files directly.

A sample project with YAML templates for every supported connector can be generated with
the :ref:`cli_init` command. See :ref:`creating_pipelines` for a walkthrough.


Global Configuration (config.yml)
'''''''''''''''''''''''''''''''''

The ``config.yml`` file contains settings that apply to the entire PipelineWise installation.
It is created automatically when you run ``pipelinewise init``.

.. code-block:: yaml

    ---

    # Optional: Alert handlers for pipeline failure notifications
    alert_handlers:
      slack:
        token: "slack-bot-token"
        channel: "#alerts-channel"

    # Optional: Refuse large MySQL/PostgreSQL to Snowflake resyncs unless
    # pipelinewise fast_sync is run with --force
    allowed_resync_max_size:
      table_mb: 50000

    # Optional: Path to a JSON file containing database switchover data for reset_state
    switch_over_data_file: "switch_over_data.json"

    # Optional shared PostgreSQL control plane for operational modules
    # Do not point this at a PostgreSQL replication target.
    backend_db:
      host: "backend.example.com"
      port: 5432
      user: "pipelinewise"
      password: "<PASSWORD>"
      dbname: "pipelinewise"
      sslmode: "verify-full"

**Top-level keys:**

``alert_handlers``: Configure alert destinations for pipeline failures. See :ref:`alerts` for details.

``allowed_resync_max_size``: For MySQL or PostgreSQL taps targeting Snowflake,
prevent ``fast_sync`` from resyncing a non-partial table larger than ``table_mb``.
Use ``fast_sync --force`` to override the limit. See :ref:`resync`.

``switch_over_data_file``: Path to a JSON file with database switchover data, used by the ``reset_state`` CLI command.

``backend_db``: Shared PostgreSQL control-plane connection used by the internal
``pipelinewise.backend_db`` and ``pipelinewise.data_diff`` packages. It stores
immutable check revisions, audit evidence, and verified timestamp coverage.
This database is not a PostgreSQL replication target: keep its service,
database, role, credentials, and storage separate from every target.
See :ref:`data_diff`.


Tap Configuration (tap_*.yml)
'''''''''''''''''''''''''''''

Each tap YAML file defines a single data source pipeline. The general structure is:

.. code-block:: yaml

    ---

    # General Properties
    id: "my_tap_id"                        # Unique identifier of the tap
    name: "My Data Source"                 # Human-readable name
    type: "tap-mysql"                      # Must be a supported tap connector type
    owner: "somebody@example.com"          # Data owner contact
    #send_alert: False                     # Optional: Disable alerts for this tap
    #slack_alert_channel: "#tap-channel"   # Optional: Send tap-specific alerts to this channel

    # Source connection details
    db_conn:
      host: "<HOST>"
      port: 3306
      user: "<USER>"
      password: "<PASSWORD>"               # Plain string or vault encrypted
      dbname: "<DB_NAME>"

    # Destination
    target: "snowflake"                    # Must match the id in a target YAML file
    batch_size_rows: 20000                 # Batch size for stream load performance
    stream_buffer_size: 0                  # In-memory buffer size (MB) for async pipes
    #batch_wait_limit_seconds: 3600        # Optional: Max seconds to wait for batch_size_rows
    #add_metadata_columns: true            # Optional: Add _SDC_ ingestion metadata columns
    #hard_delete: false                    # Optional: Retain and flag source-deleted rows
                                           # before flushing a partial batch (Snowflake target only)

    # Optional defaults for every table-level data-diff block in this tap
    data_diff_defaults:
      frequency: "0 */6 * * *"
      window_start: "-15h"
      window_end: "-3h"
      statement_timeout: "20min"

    # Schema mapping
    schemas:
      - source_schema: "my_schema"
        target_schema: "my_target_schema"
        target_schema_select_permissions:   # Optional: Grant SELECT to these roles
          - grp_analytics

        tables:
          - table_name: "my_table"
            replication_method: "LOG_BASED"  # Preferred where the source supports it

            # Optional independent source-to-target aggregate checks
            data_diff:
              checks:
                - schema_compatibility
                - row_count
                - null_key_count
                - duplicate_key_count
                - row_checksum
              key_column: "id"
              timestamp_column: "updated_at"
              compare_columns: ["status", "currency"]
              frequency: "0 */12 * * *"   # Override the tap-level default

**Key sections:**

``id``: Unique identifier used in CLI commands (``--tap <id>``).

``name``: Human-readable name shown in status and alert output.

``owner``: Contact responsible for the source data.

``type``: The connector type. Must match a supported tap. See :ref:`taps_list`.

``db_conn``: Source database connection details. Fields vary by connector type — see the
individual connector pages for the full list of supported parameters.

``target``: Must match the ``id`` field in one of your target YAML files.

``send_alert``: Optional Boolean. Set to ``false`` to suppress failure alerts for
this tap. Alerts are enabled by default when a global handler is configured.

``slack_alert_channel``: Optional tap-specific Slack channel. The channel name must
start with ``#``.

``batch_size_rows``: Number of rows per batch sent to the target. Valid values
are 1,000 to 5,000,000. Higher values can improve throughput but use more memory.

``stream_buffer_size``: Size in MB of the in-memory buffer between the tap and target
processes. Valid values are 0 to 2,500. Set to ``0`` to disable asynchronous buffering.

``batch_wait_limit_seconds``: Maximum time in seconds to wait for ``batch_size_rows``
before flushing a partial batch. Currently available only for the Snowflake target.

``add_metadata_columns`` and ``hard_delete``: Control ingestion metadata and source
delete handling for this pipeline. ``hard_delete`` defaults to ``true`` and enables
metadata columns automatically. See :ref:`metadata_columns` for all combinations.

``data_diff_defaults``: Optional timing defaults consumed by the PipelineWise
data-diff subsystem for every table-level ``data_diff`` block in this tap. A
table can override any individual default.
``frequency`` and ``window_start`` are required after resolving the tap defaults
and table overrides.
All check types in a table's ``data_diff`` block run on the same resolved cadence.
See :ref:`data_diff` for scheduling and verified timestamp coverage semantics.

``schemas``: Maps source schemas and tables to target schemas. Each table entry specifies
a ``replication_method`` (see :ref:`replication_methods`) and optionally a ``replication_key``
for incremental replication.

For connector-specific ``db_conn`` parameters and advanced options, see the individual
connector documentation:

* :ref:`tap-mysql`
* :ref:`tap-postgres`
* :ref:`tap-mongodb`
* :ref:`tap-s3-csv`
* :ref:`tap-kafka`
* :ref:`tap-snowflake`
* :ref:`tap-zendesk`
* :ref:`tap-jira`
* :ref:`tap-github`
* :ref:`tap-slack`
* :ref:`tap-mixpanel`
* :ref:`tap-salesforce`
* :ref:`tap-twilio`


Target Configuration (target_*.yml)
''''''''''''''''''''''''''''''''''''

Each target YAML file defines a single data destination. The general structure is:

.. code-block:: yaml

    ---

    id: "snowflake"                        # Unique identifier of the target
    name: "My Snowflake DW"               # Human-readable name
    type: "target-snowflake"              # Must be a supported target connector type

    db_conn:
      # Connection details vary by target type

``id``: Unique identifier referenced by tap YAML files in their ``target`` field.

``type``: The connector type. See :ref:`targets_list`.

``db_conn``: Target database connection details. See the individual target connector pages:

* :ref:`target-snowflake`
* :ref:`target-postgres`
* :ref:`target-s3-csv`


Environment Variables
'''''''''''''''''''''

Sensitive values can be injected into YAML files using Jinja2 syntax:

.. code-block:: yaml

    password: "{{ env_var['MY_PASSWORD'] }}"

See :ref:`passing_environment_variables_via_jinja` for details.

Alternatively, credentials can be encrypted using Ansible Vault. See :ref:`encrypting_passwords`.
