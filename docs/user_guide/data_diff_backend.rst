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

.. mermaid:: ../../pipelinewise/backend_db/migrations/versions/001_schema.erd.mmd
   :align: center
   :caption: Data-diff backend schema after migration 001
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

PipelineWise keeps every attempt in ``dd_run_attempts`` and every coverage
transition in ``dd_watermark_events``. ``dd_run_slot_state`` materializes only
the highest terminal attempt for each scheduled slot, while
``dd_watermark_state`` stores the current watermark. A new chronological slot
updates that state directly. A replacement or out-of-order slot recalculates it
from the slot-state rows without rescanning superseded attempts.


Reporting queries
-----------------

Run these queries against the PipelineWise backend database.

Current coverage watermarks:

.. code-block:: sql

    SELECT state.check_id, checks.full_check_name,
           checks.revision, checks.source_schema, checks.source_table,
           checks.source_timestamp_column, state.coverage_start,
           state.verified_through, state.max_observed_end,
           state.coverage_status, state.blocking_run_id,
           state.evaluated_run_id, state.event_type,
           state.updated_at AS verified_at, state.reason
      FROM public.dd_watermark_state state
      JOIN public.dd_check_definitions checks
        ON checks.check_id = state.check_id;

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
