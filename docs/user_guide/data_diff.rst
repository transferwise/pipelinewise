.. _data_diff:

Data-diff checks
================

Data-diff performs bounded aggregate reconciliation between source tables and
their PostgreSQL or Snowflake replicas. Checks are defined in the tap YAML and
persisted as immutable versioned definitions in the backend database.


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

Snowflake targets support both native tables and PipelineWise-managed Iceberg v3
tables, including the initial historical comparison.


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
      - All columns resolve
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

   - ``row_checksum`` is **probabilistic** — a mismatch identifies a window to investigate,
     but does not expose which individual rows differ
   - **JSON** and **VARIANT** columns have non-deterministic key ordering across
     databases, making consistent hashing impossible
   - **FLOAT** columns can differ due to IEEE 754 precision between database
     engines (e.g., ``0.1 + 0.2`` producing different representations)
   - Exact numeric columns are compared at the **wider** of the source and target
     scales, so a target that truncated precision fails rather than passing
   - Incompatible column types are recorded as ``ERROR`` (not ``FAIL``);
     other check types in the same run still execute normally


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

**Field reference:**

- ``schema_version`` — Optional compatibility marker; the only accepted value is
  ``1``. The current normalizer does not change behaviour based on this field.
- ``frequency`` — Crontab expression of when to fire the check
- ``window_start`` — Negative offset from fire time for the window start
- ``window_end`` — Negative offset for the window end. Must be closer to fire time
  than ``window_start``. Default ``"0s"`` (fire time)
- ``initial_full_scan`` — Whether the first data-check run of a new revision
  compares shared history before ``window_end``. Default ``true``. Set ``false``
  to use the configured rolling window from the first run
- ``statement_timeout`` — Per-query timeout. Default ``"5min"``
- ``key_column`` — Scalar key for integrity and range checks
- ``timestamp_column`` — Column that defines the comparison window boundaries
- ``compare_columns`` — Required when ``row_checksum`` is selected. Must not have
  PipelineWise transformations

Durations compose the units ``s``, ``min``, ``h``, ``d``, ``w``: ``"-15h"`` is 15
hours before fire time, and ``"1d6h"`` is valid too.

For ``tap-mysql`` sources, data-diff uses ``db_conn.engine`` when it is set. If
it is omitted, data-diff infers MariaDB or MySQL from the connected server's
handshake. Singer, FullSync, and PartialSync use the same fallback for all
source-specific behaviour, including session defaults, GTID handling, and
managed Iceberg v3 JSON aliases. Set ``engine`` explicitly for proxies that hide
the server identity.

Choosing a frequency and window
'''''''''''''''''''''''''''''''

Check infrequently, over a window that has already settled — the values above are
the recommended starting point. Every check is an aggregate scan of both the
source and the target, so frequency is a direct cost to the source database.

By default, the first data-check run of each new revision reads
``MIN(timestamp_column)`` on both source and target, considering only timestamps
before the configured ``window_end`` cutoff. It compares from the later minimum,
inclusive, to the cutoff, exclusive. For example, source history starting on
January 1 and target history starting on January 5 are compared from January 5.
Older rows on either side are intentionally excluded. NULL timestamps are excluded
from both historical and rolling checks. If neither side has a non-NULL timestamp
before the cutoff, the run is ``DEFERRED`` without verified coverage. The next
cron slot tries historical discovery again with its new cutoff. If only one side
has settled timestamps, the run records ``ERROR`` and identifies the missing side.
Detected schema or comparison errors still produce ``ERROR`` when both sides
have no settled timestamps.

Set ``initial_full_scan: false`` in ``data_diff_defaults`` or on a table to use
the configured rolling window from the first run. New scheduled windows after the
initial scan use that rolling range. The historical scan may read most of the
source table even with a timestamp index; schedule it off-peak and allow enough
``statement_timeout``.
Changing the timeout, schedule, or window creates a new definition revision and
another initial scan. Previous runs and coverage remain available through
``--include-versioned`` and backend history reports. Retries and remediation use
the original revision's timeout; increasing the YAML timeout does not change an
older run.

``window_end`` sets how long replication has to settle: too close to fire time and
uncleared lag is reported as drift, producing a ``FAIL`` that is retried at the
next cron interval. Raise it for taps that routinely lag further behind.

.. important::

   **The window must be at least as wide as the cadence.** Windows are positioned
   relative to fire time, so a narrower window leaves time no check ever examines,
   and those gaps block coverage permanently.

   Firing every 6 hours over a 3-hour window checks ``[09:00, 12:00)`` then
   ``[15:00, 18:00)``: the 3 hours between are never verified, and coverage reports
   ``BLOCKED`` even though every check passed. The defaults are 12 hours wide on a
   6-hour cadence, so windows overlap and a skipped slot cannot open a gap — each row
   is checked twice, which is the cheaper mistake.

Give ``statement_timeout`` room to match the window. The examples use ``"20min"``
rather than the ``"5min"`` default: a timeout is recorded as ``ERROR``, which
blocks coverage exactly as a real mismatch does. Raise it further for tables where
``row_checksum`` is selected.


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

``import_config`` creates a new definition revision when config changes and
deactivates removed ones; unchanged definitions are skipped. ``--force`` creates
another attempt for the current slot, while ``rerun_data_diff_check`` repairs a
historical one — see `Coverage and remediation`_.

Definitions are reconciled independently for taps whose discovery succeeds. If
another selected tap fails discovery, successful taps are still reconciled and
the failed tap's existing definitions remain unchanged. Definitions for an
explicitly selected tap absent from the project YAML are deactivated. Discovery
and backend reconciliation failures retain the import summary and a non-zero
exit so automation can report them.

The import summary counts initial scans awaiting a first historical comparison
among current checks for successfully imported taps. The count includes checks
whose discovery was ``DEFERRED``. It excludes disabled full scans, schema-only
checks, and scans already started. Failed scans are tracked as retries, not new
initial scans. A failed backend reconciliation reports the count as unavailable.

``list_data_diff_checks`` shows ``Full scan`` (the configured mode),
``Initial scan pending`` (awaiting that first comparison), and ``Verified start``
(the beginning of verified coverage). The JSON fields are ``initial_full_scan``,
``historical_scan_pending``, and ``verified_start``. A pending scan has no verified
coverage yet; a scan no longer pending may still have failed or be running.

A mismatch exits non-zero and sends an alert. See :ref:`data_diff_alerts`.
Results include the failure reason. Skipped checks show the existing slot status
when available and explain why they did not run. Known bounds identify the
existing or attempted window; they do not claim a new successful comparison.
Bounds remain blank when no window is known.


.. _data_diff_alerts:

Alerts
------

Data-diff reuses the tap's alert configuration, so whichever team owns the
replication owns its checks and their alerts. There is nothing separate to
configure:

* ``alert_handlers.slack.channel`` in ``config.yml`` receives every alert.
* A tap's ``slack_alert_channel`` also receives the alerts for that tap's checks.
* ``send_alert: False`` on a tap silences its checks along with its runs.

See :ref:`alerts` to configure the handlers. Alerts go to every configured handler,
so the :ref:`victorops_alert_handler` limitations apply here too.

One alert per failed attempt
''''''''''''''''''''''''''''

One invocation can evaluate several windows for the same table when it backfills
missed slots or retries failures. Each ``FAIL`` or ``ERROR`` produces its own alert
naming the check, the window, the reason, and the run ID:

.. code-block:: text

    data-diff FAIL snowflake/payments/public/transfers
      window  2026-07-29T10:00:00+00:00 → 2026-07-29T11:00:00+00:00
      run_id  2bd3e725-38fc-48c1-b565-b4f20e5bc7dd
      reason  row_count FAIL

``SKIPPED``, ``DEFERRED``, and ``PASS`` results are not alerted on. Failures are not
batched or deduplicated. An invocation can process up to 24 catch-up windows and
24 retries per check, and each failed attempt sends an alert. Choose a frequency
that allows replication to settle and keeps retry load manageable.

Coverage and remediation
------------------------

``verified_end`` is the end of the contiguous union of successful windows
for one definition revision:

.. code-block:: text

    [10:00, 11:00) PASS  → verified_end = 11:00
    [11:00, 12:00) FAIL  → stays 11:00 (blocked)
    [12:00, 13:00) PASS  → stays 11:00
    rerun [11:00, 12:00) PASS → advances to 13:00

A later pass cannot carry the watermark over an earlier gap. New definition
revisions start independent coverage. With ``initial_full_scan: true``, the first
run saves the later source/target minimum as its start before comparing data.
Automatic retries, forced reruns, and remediation retain that saved interval,
even if the oldest available rows later change. That run does not verify earlier
rows or NULL timestamps. If the historical run fails, its next successful retry
establishes historical coverage.
When failure happens before its start is known, the stored start and verified
interval remain NULL. The failed run still blocks coverage, even if later rolling
checks pass. A retry discovers the start using the original cutoff, then
preserves the resolved interval for subsequent attempts.
With ``initial_full_scan: false``, coverage starts at the configured
``window_start``. Existing revisions with recorded runs retain their normal
schedule after upgrade; never-run revisions use the new historical default.

At each new cron interval, PipelineWise checks the new rolling window and retries
unresolved ``FAIL`` and ``ERROR`` windows for current definitions. Each retry
retains the original window and definition and is recorded as a ``RETRY`` attempt.
A failed window is retried at most once per cron interval. If an attempt spans
several intervals, its next retry waits for the first interval after completion.
Each invocation retries up to 24 failed windows per check; further eligible
windows remain pending for the next invocation.

``PASS`` windows are not automatically rerun. Use ``--force`` to rerun the current
slot immediately, or remediation for a specific earlier window or superseded
definition. An initial ``DEFERRED`` scan tries discovery at the next cron slot
with its new cutoff. If a retry is ``DEFERRED``, its original failed window stays
blocked and eligible for a later retry.

An interrupted run is recorded as ``ERROR`` before the process exits, including on
``SIGTERM`` and ``SIGINT``. A worker killed outright cannot do that, so each
invocation retires stale ``RUNNING`` attempts before scheduling. These errors
follow the same retry schedule. If an expired worker finishes later, its result
is discarded and the recorded outcome is preserved. Its CLI result is ``SKIPPED``
with the recorded status and reason. A failure to save one check's result is
reported without stopping the remaining checks.

To retry a repaired window immediately, rerun its exact definition and time
boundaries:

.. code-block:: bash

    pipelinewise rerun_data_diff_check \
      --run-id "2bd3e725-38fc-48c1-b565-b4f20e5bc7dd" \
      --remediation-ref "AP-1234"

The original run remains immutable. The rerun gets the next attempt number,
``trigger_type = REMEDIATION``, and a ``rerun_of_run_id`` link. When it passes,
the effective attempt for that scheduled slot changes and the watermark advances.

The backend retains every run attempt and coverage transition. See
:ref:`data_diff_backend` for the schema, persistence model, and reporting queries.


Source safety
'''''''''''''

- All scheduling and window boundaries are UTC.
- Source queries use read-only transactions with timeouts.
- No source rows or business values are stored — only aggregate metrics.
- ``row_checksum`` adds CPU to the same aggregate scan; monitor during rollout.

Preflight
'''''''''

Before either aggregate query runs, a preflight returns ``BLOCKED`` when the source
table exceeds the safe row limit **and** has no usable index leading with the
timestamp column, since every window would then scan the whole table. A missing index
on a small table is reported but not blocked.

Usable means a plain, valid, ready btree index the optimizer is allowed to choose.
Partial, expression-based, still-building, hash and BRIN indexes cannot serve a
timestamp range, and a MySQL ``INVISIBLE`` or MariaDB ``IGNORED`` index is one the
planner refuses outright. All are recorded as evidence but none satisfies the check;
those leading with the timestamp column are named in the findings, so a disabled index
is distinguishable from a missing one.

Table size comes from catalog statistics, counting each partition once. A table with
no statistics is sized from its physical pages using a deliberately dense packing
estimate, so an unanalyzed large table blocks rather than slipping through. Running
``ANALYZE`` on the source replaces the estimate with a real count.

.. note::

   The preflight establishes that the table *can* be read by timestamp, not that the
   optimizer will choose to. A window wide enough to select most of the table is still
   planned as a sequential scan — correctly, since that is the cheaper plan for it.
   Keep windows narrow relative to table size, and treat ``statement_timeout`` as the
   real bound on source cost.

Each verdict is written to ``dd_preflight_log`` with the table size, the row limit, and
the index verdict it decided from, so a ``PASS`` stays auditable after the table or
the limit changes.
