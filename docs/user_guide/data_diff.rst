.. _data_diff:

Data-diff checks
================

Data-diff compares source tables with their PostgreSQL or Snowflake replicas.
Define checks in the tap YAML. The backend stores each definition revision and
its results.


Supported routes
----------------

.. list-table::
    :header-rows: 1
    :widths: 40 40

    * - Source
      - Target
    * - PostgreSQL
      - PostgreSQL
    * - PostgreSQL
      - Snowflake
    * - MySQL / MariaDB
      - PostgreSQL
    * - MySQL / MariaDB
      - Snowflake

Snowflake supports native and PipelineWise-managed Iceberg v3 tables for all
checks, including the initial historical scan.


Check types
-----------

.. list-table:: Available
    :header-rows: 1
    :widths: 22 40 22 16
    :width: 100%

    * - Check
      - What it tests
      - Pass condition
      - Database impact
    * - ``schema_compatibility``
      - Selected columns exist with compatible types on both sides
      - All selected columns exist and have compatible types
      - Metadata only
    * - ``row_count``
      - ``COUNT(*)`` in the window
      - Source equals target
      - Aggregate scan
    * - ``distinct_key_count``
      - ``COUNT(DISTINCT key)`` in the window
      - Source equals target
      - Aggregate scan
    * - ``null_key_count``
      - Rows with NULL key
      - Both sides have zero
      - Aggregate scan
    * - ``duplicate_key_count``
      - ``COUNT(key) - COUNT(DISTINCT key)``
      - Both sides have zero
      - Aggregate scan
    * - ``min_key`` / ``max_key``
      - Key boundary values
      - Source equals target
      - Aggregate scan

.. list-table:: Experimental
    :header-rows: 1
    :widths: 22 40 22 16
    :width: 100%

    * - Check
      - What it tests
      - Pass condition
      - Database impact
    * - ``row_checksum``
      - Single hash over all rows' key + timestamp + compare_columns
      - Checksums match
      - Heavier scan

.. attention::

   - ``row_checksum`` is probabilistic. A mismatch identifies a window, not the
     individual rows that differ.
   - JSON/VARIANT and floating-point checksum columns are unsupported. They
     produce ``ERROR``, not ``FAIL``. Cross-database ordering and numeric
     representations can differ.
   - Exact numeric checksums use the wider source/target scale to detect lost
     precision. Both columns need known scales.
   - Missing or incompatible checksum columns also produce ``ERROR``. Other
     check types in the run still execute.


Check configuration
-------------------

Configure the :ref:`data_diff_backend` once for the PipelineWise installation
before importing check definitions.

Tap YAML
''''''''

The ``data_diff`` block goes inside ``schemas[].tables[]`` in the tap YAML:

.. code-block:: yaml

    data_diff_defaults:
      frequency: "0 */6 * * *"
      window_start: "-15h"
      window_end: "-3h"
      statement_timeout: "20min"

    schemas:
      - source_schema: "public"
        target_schema: "repl_payments"
        tables:
          - table_name: "transfers"
            replication_method: "LOG_BASED"
            data_diff:
              checks:
                - schema_compatibility
                - row_count
                - row_checksum
              key_column: "transfer_id"
              timestamp_column: "updated_at"
              compare_columns:
                - "status"
                - "currency"
              # Override: this table is checked twice a day rather than four times
              window_start: "-16h"
              window_end: "-4h"
              frequency: "0 */12 * * *"

Every table must resolve ``key_column``, ``timestamp_column``, ``checks``,
``frequency``, and ``window_start``. These can be set directly on the table or
inherited from ``data_diff_defaults`` — the table value wins when both exist.

Field reference:

- ``schema_version`` — Optional. Only ``1`` is accepted
- ``frequency`` — Cron schedule in UTC
- ``window_start`` — Negative offset from the scheduled time
- ``window_end`` — Window end offset. Must be later than ``window_start`` and
  no later than the scheduled time. Default ``"0s"`` (the scheduled time)
- ``initial_full_scan`` — Check shared history on the first data run when
  ``true``. Default ``false`` uses the configured rolling window
- ``statement_timeout`` — Per-query timeout. Default ``"5min"``
- ``key_column`` — Scalar key for integrity and range checks
- ``timestamp_column`` — Column that defines the comparison window boundaries
- ``compare_columns`` — Required when ``row_checksum`` is selected. Must not have
  PipelineWise transformations

Durations use ``s``, ``min``, ``h``, ``d``, and ``w``. Units can be combined.
For example, ``"-1d6h"`` means 30 hours before the scheduled time.

For MySQL/MariaDB sources, ``db_conn.engine`` overrides server detection. Set it
explicitly if a proxy hides the server identity. See :ref:`tap-mysql` for
connection settings.

Choosing a frequency and window
'''''''''''''''''''''''''''''''

Use the example settings as a starting point. Each data check scans both tables,
so frequent checks add source load.

Leave enough time for replication to catch up before ``window_end``. For example,
``"-3h"`` excludes the latest three hours. Use an earlier cutoff for slower taps.
Otherwise, replication lag can cause a ``FAIL``.

.. important::

   **The window must cover at least the time between checks.** A three-hour window
   checked every six hours leaves three hours unverified. Coverage stays
   ``BLOCKED`` at that gap even if every check passes.

   The tap defaults above check a 12-hour window every six hours. This overlap
   covers one missed slot. The table override runs every 12 hours, with no overlap.

Allow enough ``statement_timeout`` for the scan, especially with ``row_checksum``.
The example uses ``"20min"``. The default is ``"5min"``. A timeout records
``ERROR`` and blocks coverage.


.. _data_diff_initial_scan:

Initial historical scan
'''''''''''''''''''''''

Set ``initial_full_scan: true`` on the table or in ``data_diff_defaults`` to
check shared history on the first data run. PipelineWise reads
``MIN(timestamp_column)`` on both sides, using only timestamps before the
``window_end`` cutoff. The comparison includes the later minimum and excludes
the cutoff.

For example, if source history starts on January 1 and target history starts on
January 5, comparison starts on January 5. Older rows are excluded.
Historical and rolling data checks exclude NULL timestamps.

- Neither side has timestamps before the cutoff: record ``DEFERRED`` with no
  verified coverage. Try again at the next cron slot with its new cutoff.
- Only one side has timestamps before the cutoff: record ``ERROR`` and name the
  missing side.
- Schema or comparison errors still produce ``ERROR``, even if both sides are
  empty.

Later scheduled windows use the configured rolling range. Without the setting,
the first run uses that range too. Schema-only checks do not scan history.

An initial scan may read most of the table, even with an index. Schedule it
off-peak and allow enough ``statement_timeout``.

Changing the timeout, schedule, or window creates a new definition revision.
With ``initial_full_scan: true``, it starts another historical scan. Earlier runs
keep their settings, including the retry timeout. Use ``--include-versioned`` to
list earlier definitions. Backend reports retain their runs and coverage.

Checks without the setting keep their existing configuration hash and rolling
windows. New and never-run revisions scan history only when explicitly enabled.


CLI commands
------------

.. code-block:: bash

    # Import/validate
    pipelinewise validate --dir ./pipelinewise-config
    pipelinewise import_config --dir ./pipelinewise-config

    # List persisted definitions
    pipelinewise list_data_diff_checks --target snowflake --tap payments
    pipelinewise list_data_diff_checks --output-format json
    pipelinewise list_data_diff_checks --include-versioned

    # Run checks (oldest unobserved slot first, max 24 catch-up windows)
    pipelinewise run_data_diff_checks --target snowflake --tap payments
    pipelinewise run_data_diff_checks --all
    pipelinewise run_data_diff_checks --target snowflake --tap payments --force

    # Remediate a specific failed run; both arguments are required
    pipelinewise rerun_data_diff_check --run-id <uuid> --remediation-ref <ticket>

``import_config`` versions changed definitions and deactivates removed ones.
It keeps unchanged definitions and reports pending initial scans. See
:ref:`cli_import_config` for import failures and the pending count.

``list_data_diff_checks`` shows the scan mode, pending initial scans, and verified
starts. See :ref:`cli_list_data_diff_checks` for table and JSON fields.

``--force`` reruns the current slot. ``rerun_data_diff_check`` retries a specific
earlier run. See :ref:`data_diff_retries` for timing and limits.

Failed checks exit non-zero and include a reason. See :ref:`data_diff_alerts`.
Skipped results explain why the check did not run and show any known status and
window. A skipped result does not verify data. Unknown bounds remain blank.


.. _data_diff_alerts:

Alerts
------

Data-diff uses the tap's replication alert settings. Set
``alert_handlers.slack.data_diff_channel`` to use a separate global Slack channel.
The tap-specific channel still receives a copy. Without this option, data-diff
uses the global replication channel. See :ref:`alerts` for configuration,
suppression, and limitations.

One alert per check per invocation
''''''''''''''''''''''''''''''''''''''''

Each check with a ``FAIL`` or ``ERROR`` sends at most one alert per command
invocation, to each configured destination. ``FAIL`` means a comparison found
differences. ``ERROR`` means the check could not complete. Slack shows a short
heading, aligned details, and a next action. For example:

.. code-block:: text

    :exclamation: FAIL data-diff payments/public.transfers — target has 20 fewer rows

    alert_utc_time    : 2026-10-09 09:30:05
    run_id            : 2bd3e725-38fc-48c1-b565-b4f20e5bc7dd
    attempt           : 3 (RETRY)
    scheduled_utc     : 2026-10-09 09:00:00
    source_table      : payments.public.transfers
    target_table      : ANALYTICS.PAYMENTS.TRANSFERS
    timestamp_column  : updated_at
    window_start_utc  : 2026-10-09 08:00:00 (inclusive)
    window_end_utc    : 2026-10-09 09:00:00 (exclusive)
    query_seconds     : source=2.40, target=1.80

    Check                 Source    Target    Difference
    row_count             100000     99980           -20
    distinct_key_count    100000     99980           -20

    Difference = target - source

    Next action: Check replication lag and investigate the mismatch.
    Failed windows retry automatically on scheduled runs. To verify sooner,
    use rerun_data_diff_check with --run-id and --remediation-ref.

If several attempts fail, the alert shows the number of failed windows and
attempts, with ``FAIL`` and ``ERROR`` totals. It shows the last processed
``ERROR`` as the representative failure, or the last ``FAIL`` when no attempt
has an ``ERROR``. For example:

.. code-block:: text

    failed_windows    : 25
    failed_attempts   : 25
    failure_statuses  : ERROR=25
    shown_window      : representative failure

The run ID, window, reason, counts, and query durations describe that one
representative failure. They are not totals across windows. Every attempt and
its results remain in the backend history.

Slack puts the details and count table in a code block. Times use UTC. Window
bounds include the start and exclude the end. An unresolved historical start is
labelled as an initial scan. Bounds keep fractional seconds when present.
Optional fields are omitted when unavailable.
When timestamp column names differ, the alert names both source and target
columns. Query durations describe the shared metric query and appear once per
database.

Alerts can include ``row_count`` and ``distinct_key_count`` values and their
exact differences. They exclude key values, checksums, schema result objects,
and row samples. Error details preserve line breaks; long details are shortened.

A missing-index error recommends adding a source index starting with the
timestamp column. Other errors recommend resolving the reported error.
Failed windows for current scheduled checks retry automatically on scheduled
runs. Use
``pipelinewise rerun_data_diff_check --run-id <uuid> --remediation-ref <ticket>``
to verify a saved failed attempt sooner. This also applies to failed manual
reruns of current definitions. Inactive historical definitions do not retry
automatically. When no run was saved, the alert directs you to the next scheduled
execution instead of a saved-window rerun.

``SKIPPED``, ``DEFERRED``, and ``PASS`` send no alert. One invocation can process
24 catch-up windows and 24 retries per check, but sends at most one failure alert
per check to each destination. The alert limit resets at the next invocation;
it does not suppress recurring failures. Automatic retries, saved results,
coverage, and the command's non-zero failure exit status are unchanged.

Coverage and remediation
------------------------

``verified_end`` marks how far successful windows cover an unbroken range for
one definition revision:

.. code-block:: text

    [10:00, 11:00) PASS  → verified_end = 11:00
    [11:00, 12:00) FAIL  → stays 11:00 (blocked)
    [12:00, 13:00) PASS  → stays 11:00
    rerun [11:00, 12:00) PASS → advances to 13:00

A later pass cannot cross an earlier failed window or gap. Each definition
revision has separate coverage. Coverage starts at the saved historical minimum,
or at ``window_start`` when ``initial_full_scan: false``.

The historical start is saved before comparison. Retries, forced reruns, and
remediation keep that interval even if the oldest available rows change.
If the start could not be found, it remains NULL and the failure blocks coverage.
A retry finds the start using the original cutoff, then saves it for later
attempts. A successful retry clears that historical failure.


.. _data_diff_retries:

Retries and manual reruns
'''''''''''''''''''''''''

At the next cron interval, PipelineWise retries failed windows alongside new
scheduled windows:

- Retry unresolved ``FAIL`` and ``ERROR`` windows for current definitions.
  Keep the original definition and saved bounds.
- Retry each window at most once per interval. If an attempt spans intervals,
  wait until the first interval after it finishes.
- Retry up to 24 failed windows per check per invocation. Further eligible
  windows wait for the next invocation.
- Leave ``PASS`` windows unchanged. A ``DEFERRED`` retry keeps the original
  failed window blocked and eligible for a later retry.

``--force`` reruns the current slot immediately. It cannot replace a running
attempt. To retry an earlier window or superseded definition, use its run ID:

.. code-block:: bash

    pipelinewise rerun_data_diff_check \
      --run-id "2bd3e725-38fc-48c1-b565-b4f20e5bc7dd" \
      --remediation-ref "AP-1234"

The original run stays in history. The rerun gets the next attempt number and
links to that run. A pass replaces the slot's failed outcome. The watermark then
advances through contiguous successful windows.

The backend retains every run attempt and coverage transition. See
:ref:`data_diff_backend` for the schema, persistence model, and reporting queries.


Interrupted runs
''''''''''''''''

``SIGTERM`` and ``SIGINT`` record ``ERROR`` before exit. After a hard kill, the
next invocation expires stale ``RUNNING`` attempts before scheduling. Both cases
follow the normal retry rules.

Expired workers cannot write results or preflight evidence. Their CLI result is
``SKIPPED`` with the recorded status and reason. If saving one check fails,
PipelineWise reports the error and continues with other checks.


Source safety
'''''''''''''

- All scheduling and window boundaries are UTC.
- Source queries use read-only transactions with timeouts.
- The backend stores aggregates, not full source rows. ``min_key`` and
  ``max_key`` retain actual key values. Restrict access to these results.
- ``row_checksum`` adds CPU work. Monitor source load during rollout.

Preflight
'''''''''

Before each data check, the source table must have a usable index whose first
column is ``timestamp_column``. This applies to every table size and to initial
full scans. Without that index, preflight returns ``BLOCKED`` and the data
comparison does not run. Schema-only checks do not require an index.

Checks that combine schema and data comparisons keep completed schema results.
If preflight blocks the data comparison, the overall run is ``ERROR``. No data
queries run, and verified coverage does not advance.

Accepted indexes are:

- PostgreSQL: a plain, valid, ready B-tree index. Partial, expression, hash,
  BRIN, and still-building indexes do not satisfy this preflight policy.
- MySQL/MariaDB: a BTREE index with no prefix length on the timestamp column.
  ``INVISIBLE`` and ``IGNORED`` indexes do not satisfy the policy.

.. note::

   Preflight checks index metadata. A query may still use a full table scan.
   Keep rolling windows narrow and set ``statement_timeout`` to limit each
   query's duration.

For a blocked check, ask the DBA to add or enable an accepted index. PipelineWise
does not create indexes. Inspect ``dd_preflight_log`` for the verdict and index
findings.

Before upgrading, review the latest index findings in ``dd_preflight_log`` for
the selected data checks. Add missing qualifying indexes, including on small
tables whose older preflight verdict was ``PASS`` under the row-count exemption.
Missing indexes continue to block each new window and its retries until fixed.
