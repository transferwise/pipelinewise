.. _data_diff_backend:

Data-diff backend database
==========================

Data-diff stores check definitions, run history, results, and coverage in
PostgreSQL. Use a separate database from any replication target.

See :ref:`data_diff` to define, schedule, and remediate data-diff checks.


Configuration
-------------

Add ``backend_db`` to ``config.yml``:

.. code-block:: yaml

    backend_db:
      host: "backend.example.com"
      port: 5432
      user: "pipelinewise"
      password: "<vault encrypted>"
      dbname: "pipelinewise"
      sslmode: "verify-full"
      ddl_user: "pipelinewise_ddl"
      ddl_password: "<vault encrypted>"

The backend can share a PostgreSQL server with a target, but not its database.
Use a separate service and storage for stronger isolation. Keep backend roles
and credentials separate from replication roles.

``backend_db`` enables data-diff. Without it, ``import_config`` warns and ignores
every ``data_diff`` block. A backend outage stops checks, not replication.
``import_config`` fails if it cannot save check definitions.

``ddl_user`` runs migrations and owns the schema. Grant the application ``user``
``CONNECT`` to the database. Migrations grant its table and sequence access.
They do not grant ``DELETE`` or schema-changing privileges to a separate role.
To share one role, set both credential pairs to that role.


Schema
------

.. mermaid:: ../../pipelinewise/backend_db/migrations/versions/004_schema.erd.mmd
   :align: center
   :caption: Data-diff backend schema after migration 004
   :zoom:

The first path stores definitions and run results. The second uses each slot's
latest outcome to update the watermark and its history:

.. code-block:: text

    dd_check_definitions
        ├── dd_preflight_log
        └── dd_run_attempts
                └── dd_run_results

    dd_run_attempts
        └── dd_run_slot_state
                └── dd_watermark_state
                        └── dd_watermark_events

This shows the processing flow. The ERD shows the exact foreign keys.

- ``dd_preflight_log`` records source index checks. Without a qualifying index,
  ``row_limit`` is 100,000 and ``table_rows`` contains the bounded count when
  available. Counts below that limit are exact; a count of 100,000 means at
  least that many rows.
  Both fields are NULL when an index makes counting unnecessary. Older records
  may contain estimates, and records written by 0.95.0 leave both fields NULL.
- ``dd_index_warning_state`` remembers delivered source-index warnings. Its
  identity uses the tap, source connection, table, and timestamp column. Check
  revisions and target changes do not repeat the warning.
- ``dd_run_attempts`` keeps every attempt.
- ``dd_run_slot_state`` keeps the highest attempt number with ``PASS``, ``FAIL``,
  or ``ERROR`` status per scheduled slot.
- ``dd_watermark_state`` holds the verified interval and furthest observed end.
  New chronological slots update it directly. Retries and older slots recalculate
  it from ``dd_run_slot_state``.
- ``dd_watermark_events`` keeps every watermark change.

Automatic retries use ``trigger_type = 'RETRY'``. They keep ``scheduled_for`` and
increment ``attempt``. A successful retry replaces the slot outcome and
recalculates coverage. Earlier attempts and results remain in history.
Manual reruns use ``trigger_type = 'REMEDIATION'``. ``rerun_of_run_id`` links them
to the original run.


Migrations 003 and 004
''''''''''''''''''''''

Back up the backend before upgrading. After upgrading, run
``pipelinewise import_config --dir <project>`` to apply migrations before
running checks. Data-diff execution does not apply migrations itself.

Migration 004 adds ``dd_index_warning_state`` so successful warning deliveries
remain remembered across restarts and configuration imports. The marker is saved
when any configured destination accepts the warning. Other destinations are
still attempted, and failures are logged. Failed destinations are not retried
after another destination accepts it. If all destinations fail, delivery can be
tried again. A crash between delivery and saving its marker can produce a
duplicate. No source tables or indexes are changed.

Migration 003 supports historical scans whose start is not yet known:

- ``window_start`` can be NULL until the historical start is found.
- An error with an unknown start records ``BLOCKED`` coverage. Both verified
  bounds are NULL, and ``blocking_run_id`` identifies the failure.
- ``DEFERRED`` means neither side had history before the cutoff. It changes no
  slot or watermark state. A deferred retry leaves its failed slot blocked.

Older versions cannot read this history. Downgrade to 002 is blocked while NULL
bounds or ``DEFERRED`` attempts remain. Later successful runs do not erase those
records. Keep the pre-upgrade backup if you may need to roll back.


Reporting queries
-----------------

Run these queries against the PipelineWise backend database.

Watermark per table:

.. code-block:: sql

    SELECT d.full_check_name,
           d.revision,
           d.source_database,
           d.source_schema,
           d.source_table,
           d.target_database,
           d.target_schema,
           d.target_table,
           COALESCE(w.verified_status, 'NOT_RUN') AS verified_status,
           w.verified_start,
           w.verified_end,
           w.furthest_observed_end,
           CURRENT_TIMESTAMP - w.verified_end AS verification_lag,
           w.blocking_run_id,
           w.last_evaluated_run_id,
           w.updated_at AS watermark_updated_at,
           w.reason
      FROM public.dd_check_definitions d
      LEFT JOIN public.dd_watermark_state w
        ON w.check_id = d.check_id
     WHERE d.is_current
     ORDER BY d.target_id, d.tap_id,
              d.source_schema, d.source_table;

Successful and failed check results per table per UTC day. Includes earlier
definition revisions, retries, and manual reruns. Counts each ``check_type``
result separately. Run-level errors without result rows have their own count:

.. code-block:: sql

    WITH outcomes AS (
        SELECT d.full_check_name,
               d.source_database,
               d.source_schema,
               d.source_table,
               d.target_database,
               d.target_schema,
               d.target_table,
               (COALESCE(a.finished_at, a.started_at)
                   AT TIME ZONE 'UTC')::date AS check_date,
               a.status AS run_status,
               r.run_id AS result_run_id,
               r.status AS result_status
          FROM public.dd_run_attempts a
          JOIN public.dd_check_definitions d
            ON d.check_id = a.check_id
          LEFT JOIN public.dd_run_results r
            ON r.run_id = a.run_id
         WHERE a.status IN ('PASS', 'FAIL', 'ERROR')
    )
    SELECT full_check_name,
           source_database,
           source_schema,
           source_table,
           target_database,
           target_schema,
           target_table,
           check_date,
           COUNT(*) FILTER (
               WHERE result_status = 'PASS'
           ) AS successful_check_results,
           COUNT(*) FILTER (
               WHERE result_status IN ('FAIL', 'ERROR')
           ) AS failed_check_results,
           COUNT(*) FILTER (
               WHERE result_run_id IS NULL
                 AND run_status = 'ERROR'
           ) AS run_level_errors
      FROM outcomes
     GROUP BY full_check_name,
              source_database, source_schema, source_table,
              target_database, target_schema, target_table,
              check_date
     ORDER BY check_date DESC, full_check_name;

Unresolved failures for current definitions only. The query uses each result's
error when available. Otherwise, it uses the combined reason in
``dd_run_attempts.error``:

.. code-block:: sql

    SELECT d.full_check_name,
           d.revision,
           d.source_schema,
           d.source_table,
           s.scheduled_for,
           s.window_start,
           s.window_end,
           s.run_id,
           a.attempt,
           a.trigger_type,
           COALESCE(r.check_type, '<run-level error>') AS failed_check,
           COALESCE(r.status, s.status) AS failure_status,
           COALESCE(r.error, a.error) AS error,
           COALESCE(s.run_id = w.blocking_run_id, FALSE) AS blocks_watermark,
           a.finished_at
      FROM public.dd_run_slot_state s
      JOIN public.dd_check_definitions d
        ON d.check_id = s.check_id
      JOIN public.dd_run_attempts a
        ON a.run_id = s.run_id
      LEFT JOIN public.dd_run_results r
        ON r.run_id = s.run_id
       AND r.status IN ('FAIL', 'ERROR')
      LEFT JOIN public.dd_watermark_state w
        ON w.check_id = s.check_id
     WHERE d.is_current
       AND s.status IN ('FAIL', 'ERROR')
     ORDER BY a.finished_at DESC,
              d.full_check_name,
              r.check_type;

Failed runs and their remediation attempts:

.. code-block:: sql

    SELECT checks.full_check_name, checks.revision,
           original.run_id AS failed_run_id,
           original.status AS failed_status,
           original.window_start, original.window_end,
           original.finished_at AS failed_at,
           remediation.run_id AS remediation_run_id,
           remediation.attempt AS remediation_attempt,
           remediation.status AS remediation_status,
           remediation.remediation_reference,
           remediation.finished_at AS remediation_finished_at,
           COALESCE(remediation.status = 'PASS', FALSE) AS recovered
      FROM public.dd_run_attempts original
      JOIN public.dd_check_definitions checks
        ON checks.check_id = original.check_id
      LEFT JOIN public.dd_run_attempts remediation
        ON remediation.rerun_of_run_id = original.run_id
     WHERE original.rerun_of_run_id IS NULL
       AND original.status IN ('FAIL', 'ERROR');
