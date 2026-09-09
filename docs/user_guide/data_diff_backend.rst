.. _data_diff_backend:

Data-diff backend database
==========================

Data-diff uses a PostgreSQL control-plane database to store check definitions,
execution history, results, and coverage. It is independent of source and target
connections and must not also serve as a replication target.

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

When PostgreSQL is also the replication target, give the backend its own service
or database.

``backend_db`` enables data-diff. Without it, ``import_config`` warns and ignores
every ``data_diff`` block. Replication never reads the backend, so an outage pauses
reconciliation only. However, ``import_config`` fails when it cannot persist check
definitions.

``ddl_user`` runs Alembic migrations and owns the schema. The application ``user``
can therefore hold DML grants only. The migration grants ``user`` what it needs, so
that role requires nothing beyond ``CONNECT``. Set ``ddl_user`` to the application
credentials when separate roles are not required.


Schema
------

.. mermaid:: ../../pipelinewise/backend_db/migrations/versions/002_schema.erd.mmd
   :align: center
   :caption: Data-diff backend schema after migration 002
   :zoom:

The schema has two related paths. The first records definitions and execution
evidence. The second selects one terminal attempt per scheduled slot, folds those
slot outcomes into the current watermark, and records every watermark transition:

.. code-block:: text

    dd_check_definitions
        ├── dd_preflight_log
        └── dd_run_attempts
                └── dd_run_results

    dd_run_attempts
        └── dd_run_slot_state
                └── dd_watermark_state
                        └── dd_watermark_events

The second path describes processing rather than foreign-key ownership; the ERD
above shows the exact database relationships.

PipelineWise keeps every attempt in ``dd_run_attempts`` and every watermark
transition in ``dd_watermark_events``. ``dd_run_slot_state`` materializes only
the highest terminal attempt for each scheduled slot, while
``dd_watermark_state`` stores the current verified interval and the furthest
observed window end. A new chronological slot updates that state directly. A
replacement or out-of-order slot recalculates it from the slot-state rows
without rescanning superseded attempts.


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

Successful and failed check results per table per UTC day. Historical definition
revisions and every attempt, including retries and remediation runs, are included.
The result counts are per ``check_type``; terminal run-level errors without
result rows are counted separately:

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

Current unresolved failures. Only current check-definition revisions are shown;
failures belonging to superseded revisions are intentionally excluded:

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
