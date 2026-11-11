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
   * - ``tap_info``
     - Returns some information about the taps and source tables.
     - No.
   * - ``test_tap_connection``
     - Test source connectivity.
     - No target load.
   * - ``discover_tap``
     - Refresh or inspect source catalog metadata.
     - Runs source discovery.
   * - ``run_tap``
     - Run normal initial and ongoing replication.
     - Loads target data and advances acknowledged state. With
       ``native_to_iceberg_conversion`` enabled, can first convert and rename
       native tables.
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
     - Build or promote one managed Iceberg v3 copy, or report feasibility.
     - Creates companion tables; can rename the live table. ``--preflight-only``
       changes nothing.
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
     - Retry a failed window with its original settings and bounds.
     - Saves a new attempt.


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
target references. PostgreSQL/MySQL-to-Snowflake routes also validate source-side
transformation syntax and reject transformed INCREMENTAL keys. It does not
connect to a source or target or verify live column types.


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

The command validates config, discovers source tables, and writes runtime files
below ``~/.pipelinewise``. It updates each tap's data-diff definitions after
connector generation and discovery succeed. Failed taps keep their definitions.
An explicitly selected tap missing from YAML has its definitions deactivated.

The summary counts pending initial scans for current checks on successfully
imported taps. It includes ``DEFERRED`` scans. It excludes scans already
started, retries, schema-only checks, and rolling-window checks.
The count is unavailable if the backend update fails.

A tap or backend failure appears in the summary and exits non-zero. ``import``
is a deprecated alias.

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


``tap_info``
''''''''''''

Run ``pipelinewise tap_info --tap <tap_id>`` to get information about the tap
and its selected tables, including table sizes, indexes, and primary keys.
This command is available only for MySQL, PostgreSQL, and YugabyteDB taps.
Only ``--tap`` is required. If the tap ID exists under multiple targets, use
``--target <target_id>`` to select one.

``size_bytes`` is ``null`` for views and foreign tables. For YugabyteDB it is
also ``null`` for colocated tables. YugabyteDB measures size differently from
PostgreSQL; see :ref:`YugabyteDB resync size limit <yugabyte_resync_size_limit>`.


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
See :ref:`running_pipelines` for preflight and recovery. When the tap enables
``native_to_iceberg_conversion``, the command first converts the tap's native
Snowflake tables and stops before replication if it cannot; see
:ref:`snowflake_iceberg_auto_conversion`.


.. _cli_stop_tap:

``stop_tap``
''''''''''''

.. code-block:: bash

   pipelinewise stop_tap --tap <tap_id> --target <target_id>

Sends SIGTERM to the PipelineWise process in the tap-target PID file. That
process stops its child processes, marks its run log ``.terminated``, and
exits. The target is given an opportunity to finish data already received. If
the run is still alive after 25 seconds, ``stop_tap`` kills its process tree.
Restart without editing state after an unexpected interruption.

Sending SIGTERM or SIGINT to the PipelineWise process has the same effect.


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
     - Overrides ``allowed_resync_max_size`` only. Does not override
       ``sync_start_from`` or bypass slot-reset safety checks.
   * - ``--replication_method_only``
     - Selects tables with ``full_table``, ``incremental``, or ``log_based``.

The command fails when FullSync is unavailable for the route. It never falls
back to Singer. A table with ``sync_start_from`` uses PartialSync. ``sync_tables``
remains a deprecated alias. Supplying ``--tables`` or setting
``--replication_method_only`` to anything other than ``*`` makes the resync
filtered and retains the PostgreSQL or YugabyteDB replication slot, even if
``--tables`` lists every table. An unfiltered whole-tap PostgreSQL or YugabyteDB
run with LOG_BASED tables resets its tap-specific slot once before workers start,
with or without ``--force``.
See :ref:`resync_postgres_slot_reset` for safety checks, backups, pending-Iceberg
guards, and non-atomic reset recovery.


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
available only from MariaDB/MySQL, PostgreSQL, or YugabyteDB to Snowflake. See
:ref:`partial_sync_cases`.


.. _cli_reset_state:

``reset_state``
'''''''''''''''

.. code-block:: bash

   pipelinewise reset_state --tap <tap_id> --target <target_id> [--boundary_lsn <X/Y>]

Use this only after a controlled MariaDB/MySQL or PostgreSQL switchover whose old
and new replication positions are known. The command changes state without
copying rows; an incorrect mapping can skip data permanently.

The command takes the tap lock. It fails if the tap is running.

For PostgreSQL, ``--boundary_lsn`` is required, in ``X/Y`` form. It is the
switchover boundary on the new primary: the value of ``pg_current_wal_lsn()`` on
the new primary, read after it has applied every change from the old primary and
before any service user is unlocked. The ``restart_lsn_mappings`` in the
switchover event is not the boundary. The command fails, and leaves state
unchanged, unless:

* the boundary is not ahead of the new primary's current WAL position;
* the tap's replication slot on the new primary is finished and belongs to the
  tap's database and ``wal2json``; and
* the slot starts (``confirmed_flush_lsn``) at or before the boundary.

On success, LOG_BASED bookmarks are set to ``lsn=1``, so the tap streams from the
slot's start.

For MariaDB/MySQL, ``switch_over_data_file`` in ``config.yml`` points to JSON
that maps the new host to the old/new identifiers, hosts, timestamp, and binlog
positions. Back up state and verify target continuity after the first run.

The command does not check that the tap read everything from the old host before
the switchover. Confirm that before running it. See
:ref:`troubleshooting_source_identity` for its error messages.

.. _cli_source_identity:

Source identity
"""""""""""""""

Tap state for PostgreSQL and MariaDB/MySQL taps with LOG_BASED tables records the
source it belongs to under ``source_identity``:

* PostgreSQL: the configured ``host``, ``system_identifier`` and the current
  timeline. A promoted replica keeps the system identifier but starts a new
  timeline.
* MySQL: ``server_uuid``.
* MariaDB: the configured ``host`` and ``server_id``, because MariaDB has no
  ``server_uuid``. ``server_id`` tells apart two servers behind the same host
  name.

``run_tap`` and ``fast_sync`` refuse to start when the connected source does not
match the state, because the stored log positions do not apply to another
source. tap-postgres repeats the PostgreSQL check on its replication connection
before it streams. After a switchover, run ``reset_state``, which records the new
identity. An unfiltered PostgreSQL ``fast_sync`` that resets the slot also
records the new identity. Restoring a state backup from another source fails
this check. A change of the configured ``host`` also fails it, even when the
server is the same. Run ``reset_state`` with the tap's current position as the
boundary in that case.

A state without an identity gets the connected source's identity on its next
run. The identity is read under the tap lock before the first bookmark is taken,
also for a new tap, and FastSync records that value once its workers have
created the state. A tap config rewritten to another host during the run cannot
relabel positions taken from the old one. When the state
already holds log positions, PipelineWise logs a warning, because nothing proves
they came from this source. That is the case for every existing tap after the
upgrade to 0.103.0.

The check does not tell apart two PostgreSQL servers with the same system
identifier, timeline and host, such as sibling replicas behind one repointed
DNS name.


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
resuming readers or writers. After an interrupted cutover, a retry restores the
native table name before it validates anything. A rerun after a completed
``--eventual iceberg`` conversion changes nothing. Do not delete its recovery
manifest or companion tables. See :ref:`snowflake_iceberg` for preflight,
unsupported metadata, and rollback behaviour.

The command and its report run on the target's warehouse by default.
``--conversion-warehouse <warehouse>`` runs them on another warehouse, which the
target role must be able to use. For a ``--tap`` report, the tap's
``conversion_warehouse`` applies when you give no option. Other commands reject
``--conversion-warehouse``.

Check feasibility first with ``--preflight-only``:

.. code-block:: bash

   pipelinewise copy_native_to_iceberg \
     --target <target_id> \
     --tap <tap_id> \
     --preflight-only \
     --iceberg-version 3 \
     --output-format table

The report changes nothing. It takes no lock, writes no manifest, and runs no
DDL, so it can run beside the tap. ``--tap`` reports every table that the tap's
selected streams write; ``--table`` reports one table, checked against the
catalogs of the imported taps that write it. Each row shows the table's state,
the action conversion would take, Snowflake's row and byte counts, and its
blockers: the first metadata refusal, or every conversion blocker when there is
no refusal. ``--output-format json`` prints only the report on stdout,
with a top-level ``ready`` flag; log messages go to stderr. The command exits 0
whenever it produces a report, so check ``ready`` or the final line. Other
commands reject ``--preflight-only``, and this command accepts ``--tap`` only
with ``--preflight-only``. See :ref:`snowflake_iceberg_conversion_report`.


Data-diff
---------

.. _cli_list_data_diff_checks:

``list_data_diff_checks``
'''''''''''''''''''''''''

.. code-block:: bash

   pipelinewise list_data_diff_checks --target <target_id> --tap <tap_id>

Options include ``--output-format table|json`` and ``--include-versioned``.
``--tap`` requires ``--target``.

- ``Full scan`` (JSON: ``initial_full_scan``): the configured scan mode.
- ``Initial scan pending`` (``historical_scan_pending``): awaiting the first
  historical comparison. Includes ``DEFERRED`` scans, but excludes running or
  failed scans. A pending scan has no verified coverage.
- ``Verified start`` (``verified_start``): the start of verified coverage.


.. _cli_run_data_diff_checks:

``run_data_diff_checks``
''''''''''''''''''''''''

.. code-block:: bash

   pipelinewise run_data_diff_checks --target <target_id> --tap <tap_id>
   pipelinewise run_data_diff_checks --all

``--check`` selects a check name, logical key, or version ID. ``--force`` reruns
the current UTC slot after its previous attempt finishes. It cannot replace a
running attempt.

``FAIL`` and ``ERROR`` windows retry at the next cron interval. See
:ref:`data_diff_retries` for saved boundaries and retry limits.
``DEFERRED`` means neither side has history before the cutoff. See
:ref:`data_diff_initial_scan` for empty-table handling.

Skipped checks show a reason and any known slot status and window. Unknown
bounds are blank. Skipping a check does not verify data.


.. _cli_rerun_data_diff_check:

``rerun_data_diff_check``
'''''''''''''''''''''''''

.. code-block:: bash

   pipelinewise rerun_data_diff_check \
     --run-id <uuid> \
     --remediation-ref <ticket_or_incident>

Both options are required. The original attempt stays in history. See
:ref:`data_diff_retries` for retry timing and saved window boundaries.


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
