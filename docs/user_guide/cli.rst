
.. _command_line_interface:

Command Line Interface
======================

PipelineWise's command line interface allows for a number of operation types on a pipeline.


.. code-block:: bash

    usage: pipelinewise [-h]
                        {init,run_tap,stop_tap,discover_tap,status,test_tap_connection,
                         fast_sync,sync_tables,partial_sync_table,import_config,import,
                         validate,encrypt_string,reset_state,list_data_diff_checks,
                         run_data_diff_checks,rerun_data_diff_check}


Positional Arguments
--------------------

:subcommand: The operation to perform. ``sync_tables`` is a deprecated alias for
             ``fast_sync``; ``import`` is a deprecated alias for ``import_config``.


Sub-commands:
-------------

.. _cli_init:

init
""""

Initialise and create a sample project. The project will contain sample YAML
configuration for every supported tap and target connectors.

Positional Arguments
''''''''''''''''''''

:--name: name of the project



.. _cli_run_tap:

run_tap
"""""""

Run a specific pipeline

:--target: Target connector id

:--tap: Tap connector id

:--extra_log: Optional: Copy logging into PipelineWise logger to see tap run logs at runtime in STDOUT


.. _cli_stop_tap:

stop_tap
""""""""

Stop a running pipeline. PipelineWise sends a termination signal to the running tap
process. The target process will finish processing any remaining data in its buffer
before exiting. Data that has already been loaded into the target is not affected.

:--target: Target connector id

:--tap: Tap connector id


.. _cli_discover_tap:

discover_tap
""""""""""""

Run a specific tap in discovery mode. Discovery mode connects to the data source
and collects information (schemas, tables, columns, data types) that is required
for running the tap. Discovery is also run automatically as part of the
:ref:`cli_import_config` command, so you typically only need to run this manually when
debugging connection or schema issues.

:--target: Target connector id

:--tap: Tap connector id


.. _cli_status:

status
""""""

Prints a status summary table of every imported pipeline with their tap and target.


.. _cli_test_tap_connection:

test_tap_connection
"""""""""""""""""""

Test the tap connection. It will connect to the data source that is defined in the tap
and will return success if it's available.

:--target: Target connector id

:--tap: Tap connector id


.. _cli_fast_sync:

fast_sync
"""""""""

Sync or resync one or more tables from a specific datasource. It performs an initial
sync and resets the table bookmarks to their new location.

This command uses the **FullSync** component of :ref:`fast_sync_main`, bypassing
Singer for performance. It requires a supported FullSync tap-target combination.
If the combination is unsupported, the command fails without syncing any data; it
does not fall back to Singer.

.. note::

   The legacy command name ``sync_tables`` is accepted as a backward-compatible alias.

:--target: Target connector id

:--tap: Tap connector id

:--tables: Optional: Comma separated list of tables to sync from the data source.

:--force: Optional: Ignore the configured table-size limit and perform the sync.

:--replication_method_only: Optional: Sync only tables using the specified replication
                            method (``full_table``, ``incremental`` or ``log_based``).


.. _cli_partial_sync_table:

partial_sync_table
""""""""""""""""""

Partially resync a specific range of rows from a single table. This is useful for
repairing a specific segment of data without resyncing the entire table.

This command uses the **PartialSync** component of :ref:`fast_sync_main`. It is
available only from MySQL or PostgreSQL taps to a Snowflake target. If the
tap-target combination is unsupported, the command fails; it does not fall back
to FullSync or Singer.

See :ref:`partial_sync_cases` for details on how partial sync handles different
scenarios.

:--target: Target connector id

:--tap: Tap connector id

:--table: Source schema and table name (e.g. ``schema_name.table_name``)

:--column: Column name to use for the range filter

:--start_value: Lower boundary of the range (inclusive)

:--end_value: Optional upper boundary of the range (inclusive). When omitted, PipelineWise
              captures the current replication position and updates state after the partial sync.


.. _cli_import_config:
.. _cli_import:

import_config
"""""""""""""

Import a project directory into PipelineWise. It will create every JSON file required for
the tap and target connectors in ``~/.pipelinewise``. ``import`` is retained as a
deprecated alias.

:--dir: relative path to the project directory to import

:--taps: Optional: Comma separated list of tap id's to create.

:--secret: Optional: Path to the Ansible Vault password file needed to decrypt values
           in the project YAML files.

Data-diff definitions are versioned in the
backend database after successful discovery. See :ref:`data_diff`.


.. _cli_list_data_diff_checks:

list_data_diff_checks
"""""""""""""""""""""

List persisted data-diff definition revisions and their PostgreSQL scheduling
and verified timestamp coverage state. This reads the backend database rather
than reconstructing output from YAML.

:--target: Optional target id filter.

:--tap: Optional tap id filter. Requires ``--target``.

:--output-format: ``table`` (default) or ``json``.

:--include-versioned: Include superseded definition revisions.


.. _cli_run_data_diff_checks:

run_data_diff_checks
""""""""""""""""""""

Run active data-diff definitions manually. Definitions are versioned and
persisted by :ref:`cli_import_config`; this command does not synchronize YAML.
Use it for operator-triggered execution rather than continuous scheduling.

:--target: Optional target id filter.

:--tap: Optional tap id filter. Requires ``--target``.

:--check: Optional data-diff check-name filter.

:--force: Create another attempt for the current UTC frequency slot when a
          terminal attempt already exists.


.. _cli_rerun_data_diff_check:

rerun_data_diff_check
"""""""""""""""""""""

Re-execute the exact immutable definition and UTC window of a failed or errored
run. The original attempt remains unchanged and the new attempt is linked to it
for remediation reporting.

:--run-id: UUID of the failed or errored data-diff run.

:--remediation-ref: Required incident, change, or repair reference.


.. _cli_validate:

validate
""""""""

Validates a project directory with YAML tap and target files. This checks that:

* All YAML files are syntactically valid
* Required fields (``id``, ``type``, ``db_conn``, ``target``, ``schemas``) are present
* Tap ``target`` references match an existing target YAML file
* Connector types are supported
* Schema mappings are correctly structured

Validation does **not** test connectivity to data sources or targets.

:--dir: relative path to the project directory with YAML taps and targets.


.. _cli_encrypt_string:

encrypt_string
""""""""""""""

Encrypt a value for use in a PipelineWise YAML file. The command prints an Ansible
Vault value that can be copied into the YAML configuration. See
:ref:`encrypting_passwords` for the complete workflow.

:--secret: Path to the file containing the Ansible Vault password.

:--string: The value to encrypt.

.. code-block:: bash

    $ pipelinewise encrypt_string --secret vault-password.txt --string "value to encrypt"


.. _cli_reset_state:

reset_state
"""""""""""

Reset the state file for log-based replication tables after a database switchover
or failover. This updates the CDC bookmarks (binlog position for MySQL, LSN for
PostgreSQL) to point to the new primary server. Unlike ``fast_sync``, this does
not resync any data — it only updates the replication position.

This command works only for PostgreSQL and MySQL databases.

:--target: Target connector id

:--tap: Tap connector id


For MySQL, the command requires a JSON file containing the switchover data in the
following format:

.. code-block:: JSON

   {
     "new_url_of_the_source_database": {
       "old_identifier": "old_identifier_of_source_db",
       "new_identifier": "new_identifier_of_source_db",
       "old_host": "old_url_of_the_source_database",
       "new_host": "new_url_of_the_source_database",
       "engine": "mariadb",
       "switchover_utc_timestamp": "2025-04-03 12:13:14+00:00",
       "old_binlog_filename": "old_mysql-bin.000001",
       "old_binlog_position": 1,
       "new_binlog_filename": "new_mysql-bin.000002",
       "new_binlog_position": 500
     }
   }


.. attention::

   The filename for switchover data can be added in the `config.yml`:

   .. code-block:: yaml

       switch_over_data_file: "switch_over_data.json"


Common options
--------------

The following options are accepted by all commands, although they are mainly useful
for commands that run or inspect a pipeline:

:--log: Write PipelineWise CLI logs to the specified file.

:--extra_log: Copy Singer and FastSync subprocess logs to standard output.

:--debug: Enable debug-level logging on standard output.

:--profiler, -p: Enable Python profiling. Results are written below
                 ``<config-directory>/profiling``.

:--version: Print the installed PipelineWise version and exit.



Environment Variables
---------------------

`PIPELINEWISE_HOME`
"""""""""""""""""""

Configures the PipelineWise installation root. PipelineWise expects connector virtual
environments below ``${PIPELINEWISE_HOME}/.virtualenvs``. The default is
``~/pipelinewise``.

`PIPELINEWISE_CONFIG_DIRECTORY`
"""""""""""""""""""""""""""""""

Overrides the default directory (``~/.pipelinewise``) at which PipelineWise stores
configuration files, state files, and logs generated by ``pipelinewise import_config``.
