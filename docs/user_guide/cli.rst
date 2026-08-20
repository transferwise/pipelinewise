.. _command_line_interface:

Command-line interface
======================

PipelineWise commands operate on a project directory or an imported tap-target
pair. Commands exit non-zero on validation, discovery, replication, or
reconciliation failure; automation must retain that exit status and the run log.


Command summary
---------------

.. list-table::
   :header-rows: 1
   :widths: 29 43 28
   :width: 100%

   * - Command
     - Purpose
     - Changes state or data
   * - ``init``
     - Generate a project and connector templates.
     - Creates local files.
   * - ``validate``
     - Validate project YAML and references.
     - No.
   * - ``import_config``
     - Discover sources and generate runtime configuration.
     - Replaces generated config; persists data-diff definitions.
   * - ``status``
     - List imported pipelines and last results.
     - No.
   * - ``test_tap_connection``
     - Test source connectivity.
     - No target load.
   * - ``discover_tap``
     - Refresh or inspect source catalog metadata.
     - Runs source discovery.
   * - ``run_tap``
     - Run normal initial and ongoing replication.
     - Loads target data and advances acknowledged state.
   * - ``stop_tap``
     - Terminate a managed running pipeline.
     - Stops processes; may leave replayable work.
   * - ``fast_sync``
     - FullSync or configured PartialSync selected tables.
     - Rebuilds/merges data and resets bookmarks.
   * - ``partial_sync_table``
     - Repair one bounded source range.
     - Merges target data; can update state.
   * - ``copy_native_to_iceberg``
     - Build or promote one managed Iceberg v3 copy.
     - Creates companion tables; can rename the live table.
   * - ``reset_state``
     - Move CDC state after a controlled switchover.
     - Changes bookmarks without copying data.
   * - ``encrypt_string``
     - Produce an Ansible Vault YAML value.
     - No pipeline state.
   * - ``list_data_diff_checks``
     - Inspect persisted data-diff definitions and coverage.
     - No.
   * - ``run_data_diff_checks``
     - Run due data-diff checks.
     - Persists attempts and coverage.
   * - ``rerun_data_diff_check``
     - Re-run one failed immutable window.
     - Persists a remediation attempt.


Project lifecycle
-----------------

.. _cli_init:

``init``
''''''''

.. code-block:: bash

   pipelinewise init --name <project>

Creates sample YAML for available, experimental, and some legacy connectors.
Review :ref:`connector_support` before enabling a template.


.. _cli_validate:

``validate``
''''''''''''

.. code-block:: bash

   pipelinewise validate --dir <project>

Checks YAML syntax, required fields, connector types, schema mapping, and tap
target references. It does not connect to a source or target.


.. _cli_import_config:
.. _cli_import:

``import_config``
'''''''''''''''''''

.. code-block:: bash

   pipelinewise import_config --dir <project>

Useful options:

.. list-table::
   :header-rows: 1
   :widths: 28 72
   :width: 100%

   * - Option
     - Behaviour
   * - ``--taps <id,id>``
     - Imports only the named tap IDs and their targets.
   * - ``--secret <file>``
     - Reads the Ansible Vault password needed by encrypted YAML values.

The command validates, connects to sources, performs discovery, and writes
generated files below ``~/.pipelinewise``. Data-diff definitions are versioned
only after connector generation and discovery succeed. ``import`` remains a
deprecated alias.

.. warning::

   Removing or renaming a tap or target in project YAML makes
   ``import_config`` delete its generated runtime directory and saved state.
   Removing a PostgreSQL tap also drops its replication slot. Back up state and
   plan a new initial sync before importing that change.


Inspect and test
----------------

.. _cli_status:

``status``
''''''''''

.. code-block:: bash

   pipelinewise status

Shows imported tap-target pairs, enabled state, current status, and last run
result. A successful status row does not prove source-to-target equality.


.. _cli_test_tap_connection:

``test_tap_connection``
'''''''''''''''''''''''

.. code-block:: bash

   pipelinewise test_tap_connection --tap <tap_id> --target <target_id>

Tests the configured source connection. It does not test the target or load
records.


.. _cli_discover_tap:

``discover_tap``
''''''''''''''''

.. code-block:: bash

   pipelinewise discover_tap --tap <tap_id> --target <target_id>

Runs source discovery and writes catalog metadata. Use it to diagnose missing
schemas, tables, fields, or permissions. ``import_config`` runs discovery
automatically.


Replication
-----------

.. _cli_run_tap:

``run_tap``
'''''''''''

.. code-block:: bash

   pipelinewise run_tap --tap <tap_id> --target <target_id>

Runs eligible initial FastSync work, then Singer replication in the same
invocation. ``--extra_log`` mirrors connector output to the PipelineWise logger.
See :ref:`running_pipelines` for preflight and recovery.


.. _cli_stop_tap:

``stop_tap``
''''''''''''

.. code-block:: bash

   pipelinewise stop_tap --tap <tap_id> --target <target_id>

Signals the process tree associated with the tap-target PID file. The target is
given an opportunity to finish data already received. Restart without editing
state after an unexpected interruption.


.. _cli_fast_sync:

``fast_sync``
'''''''''''''

.. code-block:: bash

   pipelinewise fast_sync \
     --tap <tap_id> \
     --target <target_id> \
     --tables <schema.table,schema.table>

.. list-table:: Important options
   :header-rows: 1
   :widths: 34 66
   :width: 100%

   * - Option
     - Behaviour
   * - ``--tables``
     - Limits work to comma-separated source ``schema.table`` names.
   * - ``--force``
     - Overrides ``allowed_resync_max_size``.
   * - ``--replication_method_only``
     - Selects tables with ``full_table``, ``incremental``, or ``log_based``.

The command fails when FullSync is unavailable for the route. It never falls
back to Singer. A table with ``sync_start_from`` uses PartialSync. ``sync_tables``
remains a deprecated alias.


.. _cli_partial_sync_table:

``partial_sync_table``
''''''''''''''''''''''

.. code-block:: bash

   pipelinewise partial_sync_table \
     --tap <tap_id> \
     --target <target_id> \
     --table <schema.table> \
     --column <column> \
     --start_value <inclusive_start> \
     --end_value <inclusive_end>

``--end_value`` is optional. When absent, PipelineWise captures the current
replication position and can update state after the merge. PartialSync is
available only from MariaDB/MySQL or PostgreSQL to Snowflake. See
:ref:`partial_sync_cases`.


.. _cli_reset_state:

``reset_state``
'''''''''''''''

.. code-block:: bash

   pipelinewise reset_state --tap <tap_id> --target <target_id>

Use this only after a controlled MariaDB/MySQL or PostgreSQL switchover whose old
and new replication positions are known. The command changes state without
copying rows; an incorrect mapping can skip data permanently.

For MariaDB/MySQL, ``switch_over_data_file`` in ``config.yml`` points to JSON
that maps the new host to the old/new identifiers, hosts, timestamp, and binlog
positions. Back up state and verify target continuity after the first run.


Target maintenance
------------------

.. _cli_copy_native_to_iceberg:

``copy_native_to_iceberg``
''''''''''''''''''''''''''

.. code-block:: bash

   pipelinewise copy_native_to_iceberg \
     --target <target_id> \
     --table <database.schema.table> \
     --eventual native \
     --iceberg-version 3

Creates and validates a managed Iceberg v3 copy using an imported
``target-snowflake`` configuration. ``--iceberg-version`` is required and accepts
only ``3``. ``--eventual native`` is the default and leaves the original table
live with an ``_ICEBERG`` companion. Selecting ``--eventual iceberg`` renames the
original to ``_NATIVE`` and promotes the Iceberg table with a second statement.

Stop PipelineWise and every other writer to the table before either mode, then
keep them stopped until data and metadata are validated. ``--eventual iceberg``
also requires a controlled reader outage: stop dashboards, transformations,
ad-hoc queries, and other readers before cutover. The primary table name is
temporarily absent between the two promotion statements and between the two
rollback statements. An interruption can leave it absent until the identical
command completes recovery.

Retry from the same imported target runtime directory with the same table,
``--eventual`` value, target account/database/user, and account role before
resuming readers or writers. Do not delete its recovery manifest or companion
tables. See :ref:`snowflake_iceberg` for preflight, unsupported metadata, and
rollback behaviour.


Data-diff
---------

.. _cli_list_data_diff_checks:

``list_data_diff_checks``
'''''''''''''''''''''''''

.. code-block:: bash

   pipelinewise list_data_diff_checks --target <target_id> --tap <tap_id>

Options include ``--output-format table|json`` and ``--include-versioned``.
``--tap`` requires ``--target``.


.. _cli_run_data_diff_checks:

``run_data_diff_checks``
''''''''''''''''''''''''

.. code-block:: bash

   pipelinewise run_data_diff_checks --target <target_id> --tap <tap_id>
   pipelinewise run_data_diff_checks --all

``--check`` selects a check name, logical key, or version ID. ``--force`` creates
another attempt for the current UTC slot when a terminal attempt already exists.


.. _cli_rerun_data_diff_check:

``rerun_data_diff_check``
'''''''''''''''''''''''''

.. code-block:: bash

   pipelinewise rerun_data_diff_check \
     --run-id <uuid> \
     --remediation-ref <ticket_or_incident>

Both options are required. The original attempt remains immutable. See
:ref:`data_diff` for scheduling, coverage, and remediation semantics.


Secrets
-------

.. _cli_encrypt_string:

``encrypt_string``
''''''''''''''''''

.. code-block:: bash

   pipelinewise encrypt_string \
     --secret <vault-password-file> \
     --string <value>

The command prints an Ansible Vault YAML value. Avoid shared shell history and
process inspection when supplying sensitive command-line values. See
:ref:`encrypting_passwords`.


Common options and environment
------------------------------

.. list-table::
   :header-rows: 1
   :widths: 28 72
   :width: 100%

   * - Option
     - Behaviour
   * - ``--log <file>``
     - Writes PipelineWise CLI logs to a file.
   * - ``--extra_log``
     - Copies Singer and FastSync subprocess output to standard output.
   * - ``--debug``
     - Enables debug logging on standard output.
   * - ``--profiler`` / ``-p``
     - Writes cProfile output below the configured profiling directory.
   * - ``--version``
     - Prints installed component versions.

``PIPELINEWISE_HOME`` selects the installation root containing connector virtual
environments. It defaults to ``~/pipelinewise``.

``PIPELINEWISE_CONFIG_DIRECTORY`` selects the runtime configuration, state, and
log directory. It defaults to ``~/.pipelinewise``.
