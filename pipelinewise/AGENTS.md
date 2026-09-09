# PipelineWise Implementation Instructions

Read root `AGENTS.md` first, then relevant connector, test, E2E, and docs guides.

## Map and boundaries

- `cli/__init__.py`: commands/aliases/dispatch; `cli/pipelinewise.py`:
  orchestration; `cli/commands.py`: Singer pipeline; `cli/config.py`: YAML
  validation and generated JSON under
  `$PIPELINEWISE_CONFIG_DIRECTORY/<target_id>/<tap_id>/` (default
  `~/.pipelinewise`); `cli/constants.py`: connector types/mappings;
  `cli/schemas/`: JSON Schemas; `cli/alert_handlers/`: Slack/VictorOps—extend
  `BaseAlertHandler`. `cli/fastsync_capabilities.py` is the sole format-aware
  FullSync/PartialSync policy: resolve every native/Iceberg direct route through
  its operation-specific immutable registry and derive, never duplicate,
  compatibility views.
- `fastsync/`: native bulk sync. FullSync replaces tables for initial loads,
  FULL_TABLE, and explicit `fast_sync`; PartialSync merges filtered ranges for
  `partial_sync_table`/`sync_start_from`. FullSync supports `tap-mysql`
  (MariaDB/MySQL), `tap-postgres`, and `tap-mongodb` to PostgreSQL/Snowflake;
  PartialSync supports `tap-mysql`/`tap-postgres` to Snowflake. S3 CSV remains
  Singer-only. Keep MySQL/PostgreSQL→Snowflake lifecycle in
  `commons/rdbms_to_snowflake.py` and `partialsync/rdbms_to_snowflake.py`;
  source modules only adapt source construction, mapping, and ordering. Put
  other shared primitives in `commons/`, imported by `partialsync/`, and align
  `docs/concept/fastsync.rst`.
- `backend_db/`: PostgreSQL connections, transactions, Alembic. Required
  `ddl_user`/`ddl_password` may equal app credentials. It cannot depend on
  data-diff or replication orchestration; an AST test enforces this.
- `data_diff/`: may use backend-db, never Singer/FastSync execution. Supports
  MySQL/MariaDB or PostgreSQL → PostgreSQL/Snowflake. Ownership: `adapters.py`
  dialects; `engine.py` execution; `repository.py` persistence; `runner.py`
  scheduling/remediation; `config.py`, `comparison.py`, `coverage.py` their
  named concerns; `runtime.py` generated connector JSON; `credentials.py`
  private keys. `import_config` persists definitions only after connector
  generation/discovery. Add database types at the adapter boundary and AST
  coverage for new dependency seams.

## Backend schema

- Primary keys use concise domain names (`check_id`, `run_id`, `preflight_id`);
  foreign keys reuse them. Prefix a role only for distinct meaning, e.g.
  `rerun_of_run_id`, `evaluated_run_id`, or `blocking_run_id`.
- Backend table/column/constraint/index identifiers must avoid PostgreSQL and
  Snowflake reserved or limited keywords. Prefer descriptive `is_current` and
  `trigger_type` even if quoting makes a keyword legal in one database.
- Data-diff suffixes encode lifecycle: `_definitions` versioned config,
  `_attempts` executions, `_results` execution detail, `_state` mutable
  materialized projections, `_events`/`_log` append-only history.
- `public` is fixed across Alembic, runtime, tests, ERDs, and docs. Changing it requires a forward migration plan and synchronized updates.
- Every `NNN_*.py` revision needs a matching `NNN_schema.erd.mmd` Mermaid ERD
  of the resulting `public` schema. Preserve old ERDs; show FKs on the tables diagram.
- Migration 001 shipped in `0.78.0`; its `0.82.0` schema finalization is an
  approved exception requiring coordinated manual updates to existing
  databases. It is immutable after `0.82.0`; all later changes need a new
  forward migration and matching ERD.
- History is append-oriented: insert preflight logs, results, and watermark
  events; control updates to definitions, run attempts, run-slot state, and
  watermark state. The database does not enforce immutability.

## Runtime and data-diff constraints

- PostgreSQL replication sources require 11.2 or later across Singer,
  FullSync, and PartialSync. Keep the Singer and FastSync connection gates
  aligned; only deleted-tap slot cleanup may bypass the floor. PostgreSQL
  targets, the backend, and data-diff connections are separate.
- Soft delete (`hard_delete: false`, `_SDC_DELETED_AT`) is deprecated. Preserve
  compatibility but add no features/docs, do not restore data-diff
  `exclude_soft_deleted`, and use `hard_delete: true` for new taps.
- Dev MySQL requires TLS (`ssl={'': True}`). PyMySQL interpolates bound SQL, so
  double literal tokens, e.g. `DATE_FORMAT(t, '%%Y')`.
- PostgreSQL `reltuples == 0` after ANALYZE-then-load does not prove emptiness;
  partitioned parents can duplicate child estimates. Sum leaf partitions.
- SIGTERM normally does not raise `SystemExit`; durable handling needs an
  installed signal handler, and injected `SystemExit` is not proof.
- Separate backend app roles receive schema/sequence access plus `SELECT`,
  `INSERT`, and `UPDATE`, but no `DELETE`/DDL. A shared app/DDL identity removes
  that separation intentionally.
- Source preflight checks estimates and timestamp-index shape—not exact counts
  or actual index use—and requires a statement timeout. Treat `min_key`/`max_key`
  values in `dd_run_results` as sensitive; avoid casual logging.

## Snowflake and Iceberg contract

- PipelineWise is the sole automated writer; external reads are allowed. DBA
  writes/DDL require a maintenance window, stopped affected replication, and no
  active recovery. Replicated tables/columns must originate via FullSync,
  PartialSync, target-snowflake, or the supported converter—never arbitrary
  external schemas or repair-added objects. Before resuming, exceptional repair
  must preserve v3, copy-on-write, width, metadata, and recovery invariants.
- Only tap-level `target_table_format: iceberg` plus integer
  `iceberg_version: 3` creates managed Iceberg; omitted/native creates native.
  Retain both through Singer
  handover/evolution. Route compatible Singer taps through target-snowflake and
  MySQL/PostgreSQL FastSync through the shared publisher; retain native
  `SWAP WITH`. All v3 taps need `hard_delete: true`; only FastSync-capable
  MySQL/PostgreSQL also need `data_flattening_max_level: 0` (keep Singer-only
  defaults such as Salesforce level 10).
- Carry `iceberg_version` through tap/generated config, publication/recovery,
  and conversion; reject non-v3 before mutation. Future versions need explicit
  branches/tests. Through executable hooks, `snowflake_iceberg_versions.py`
  owns format, canonical types, existing-table checks, semantic options, and
  copy-on-write level. Align its dependency-free fixture with target-snowflake
  CREATE/ADD/metadata/transport; new registry entries cannot inherit v3 implicitly.
- All managed-v3 creation, replacement, converter DDL, and pre-write paths need
  table-level `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED'`; never use deprecated
  `ENABLE_ICEBERG_MERGE_ON_READ`. Creation also needs
  `TARGET_FILE_SIZE = 'AUTO'` and
  `STORAGE_SERIALIZATION_POLICY = 'COMPATIBLE'`. Keep FastSync, conversion,
  target-snowflake, and the dependency-free contract fixture aligned.
- Map FastSync string-like/fallback MySQL, MariaDB, and PostgreSQL types to
  `VARCHAR(134217728)`; before DML, auto-widen compatible narrow native
  PartialSync targets. target-snowflake uses that width for new native/v3 Singer
  strings, preserves compatible existing native widths, and requires exact
  existing-v3 width without implicit widening.
- Key recovery by stable source stream, index active attempts by physical
  target, and hold both locks throughout; reject source, target, staging, role,
  transformation, or boundary drift. `RecoveryCoordinator` owns target runtime
  root, store, ordered locks, pointers, persistence, transitions, completion,
  and abort. Use typed payloads; legacy `context` is only a serialized
  compatibility projection. Reject invalid transitions. After retained-stage
  key validation, ambiguous PartialSync MERGE replay rotates submission
  identity, clears query evidence, durably returns to `staged`, then replans.
- Reuse `SnowflakeSqlClient` for authentication/query/transactions and
  `SnowflakeTableInspector` for discovery. Explicitly compose publication,
  finalization, conversion evidence, and
  `SnowflakeConversionFinalizationValidator`, kept in
  `snowflake_iceberg_conversion_recovery.py`; never restore mixin inheritance,
  dynamic binding, or duplicate catalog inspection.
- Advance state only after publication, metadata, grants, and cleanup. Persist
  registered finalization actions only as exact Boolean `true`; require grants,
  S3/staging cleanup, and replacement metadata restoration. Require a serialized
  dictionary `source_bookmark`, even empty, and reject malformed recovery.
  PartialSync needs a PK and rejects transformed-stage NULL or duplicate-key
  groups before publication.
- Content mismatches may expose counts/aggregate fingerprints, never source
  values/samples. Bounded query-history visibility/lookup failures are retryable
  ambiguity: preserve state/manifest/staging, instruct an unchanged retry, and
  reserve tracebacks for unexpected errors.
- Guarded replacement/conversion requires the owning account role; reject
  database-role ownership and unsafe dependencies/metadata. Conversion is
  target-only and fidelity-first: exclude external writers for every copy;
  `eventual=iceberg` also needs a reader/writer outage. Retain the native backup
  and recover through the manifest. Copy/validate the whole row multiset,
  including duplicate keys and representable flaws, without filtering, repair,
  or deduplication; fail before cutover if v3 cannot represent it exactly.
