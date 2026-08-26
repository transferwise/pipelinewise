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
- ``statement_timeout`` — Per-query timeout. Default ``"5min"``
- ``key_column`` — Scalar key for integrity and range checks
- ``timestamp_column`` — Column that defines the comparison window boundaries
- ``compare_columns`` — Required when ``row_checksum`` is selected. Must not have
  PipelineWise transformations

Durations compose the units ``s``, ``min``, ``h``, ``d``, ``w``: ``"-15h"`` is 15
hours before fire time, and ``"1d6h"`` is valid too.

Choosing a frequency and window
'''''''''''''''''''''''''''''''

Check infrequently, over a window that has already settled — the values above are
the recommended starting point. Every check is an aggregate scan of both the
source and the target, so frequency is a direct cost to the source database.
``window_end`` sets how long replication has to settle: too close to fire time and
uncleared lag is reported as drift, producing a ``FAIL`` that resolves itself on
the next run and trains people to ignore alerts. Raise it for taps that routinely
lag further behind.

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

A mismatch exits non-zero and sends an alert. See :ref:`data_diff_alerts`.


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

One alert per failed check per window
''''''''''''''''''''''''''''''''''''''

One invocation can evaluate several windows for the same table when it backfills
missed slots. Each ``FAIL`` or ``ERROR`` produces its own alert naming the check, the
window, and the run ID needed to remediate it:

.. code-block:: text

    data-diff FAIL snowflake/payments/public/transfers
      window  2026-07-29T10:00:00+00:00 → 2026-07-29T11:00:00+00:00
      run_id  2bd3e725-38fc-48c1-b565-b4f20e5bc7dd

``SKIPPED`` and ``PASS`` results are not alerted on. Failures are not batched or
deduplicated, so a stalled tap alerts on every failing check and every backfilled
window — up to 24 windows per check per invocation. Keeping ``frequency`` low is
what keeps the volume sane.

Coverage and remediation
------------------------

``verified_through`` is the end of the contiguous union of successful windows
for one definition revision:

.. code-block:: text

    [10:00, 11:00) PASS  → verified_through = 11:00
    [11:00, 12:00) FAIL  → stays 11:00 (blocked)
    [12:00, 13:00) PASS  → stays 11:00
    rerun [11:00, 12:00) PASS → advances to 13:00

A later pass cannot carry the watermark over an earlier gap. New definition revisions
start independent coverage.

An interrupted run is recorded as ``ERROR`` before the process exits, including on
``SIGTERM`` and ``SIGINT``, so its slot stays retryable. A worker killed outright
cannot do that, so every invocation first retires any attempt still ``RUNNING`` well
past its query budget, across every slot and remediation attempts too. That sweep
runs before scheduling because a ``RUNNING`` row makes its own slot look observed,
which would otherwise advance the scheduler past the slot needing recovery.

After repairing a failed window, rerun its exact definition and time boundaries:

.. code-block:: bash

    pipelinewise rerun_data_diff_check \
      --run-id "2bd3e725-38fc-48c1-b565-b4f20e5bc7dd" \
      --remediation-ref "AP-1234"

The original run remains immutable. The rerun gets the next attempt number,
``trigger = REMEDIATION``, and a ``rerun_of_run_id`` link. When it passes,
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

Each verdict is written to ``dd_preflights`` with the table size, the row limit, and
the index verdict it decided from, so a ``PASS`` stays auditable after the table or
the limit changes.
