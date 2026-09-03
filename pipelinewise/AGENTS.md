# PipelineWise Implementation Instructions

Read root `AGENTS.md` first; also use scoped connector, test, E2E, and docs guidance where relevant.

## Map and boundaries

- `cli/__init__.py`: commands, aliases, dispatch. `cli/pipelinewise.py`: orchestration. `cli/commands.py`: Singer pipeline. `cli/config.py`: YAML validation and generated JSON under `$PIPELINEWISE_CONFIG_DIRECTORY/<target_id>/<tap_id>/` (default `~/.pipelinewise`). `cli/constants.py`: connector types/mappings. `cli/fastsync_capabilities.py`: the sole format-aware FullSync/PartialSync capability policy; resolve every native and Iceberg direct route through its operation-specific immutable registry, and derive any compatibility views from it rather than duplicating route matrices. `cli/schemas/`: JSON Schemas. `cli/alert_handlers/`: Slack/VictorOps; extend `BaseAlertHandler`.
- `fastsync/`: native bulk sync. FullSync replaces tables for initial loads, FULL_TABLE, and explicit `fast_sync`; PartialSync merges filtered ranges for `partial_sync_table` and `sync_start_from`. FullSync supports `tap-mysql` (MariaDB/MySQL), `tap-postgres`, and `tap-mongodb` to PostgreSQL/Snowflake; PartialSync supports `tap-mysql` and `tap-postgres` to Snowflake. S3 CSV stays on Singer and outside FastSync. Keep the MySQL/PostgreSQL-to-Snowflake lifecycle in `commons/rdbms_to_snowflake.py` and `partialsync/rdbms_to_snowflake.py`; source modules are thin adapters for source construction, mapping, and ordering differences. Put other shared primitives in `commons/`, imported by `partialsync/`; keep `docs/concept/fastsync.rst` aligned.
- `backend_db/`: PostgreSQL connections, transactions, Alembic. `ddl_user`/`ddl_password` are required, though they may equal app credentials. It must not depend on data-diff or replication orchestration; an AST test enforces this boundary.
- `data_diff/`: may use backend-db, never Singer/FastSync execution. Supported routes are MySQL/MariaDB or PostgreSQL to PostgreSQL/Snowflake. `adapters.py` owns dialect behavior; `engine.py` execution; `repository.py` persistence; `runner.py` scheduling/remediation; `config.py`, `comparison.py`, and `coverage.py` own their named concerns; `runtime.py` generated connector JSON; `credentials.py` private keys. `import_config` persists definitions only after connector generation/discovery. Add database types at the adapter boundary and AST coverage for new dependency seams.

## Backend schema

- Primary keys use concise domain names such as `check_id`, `run_id`, and
  `preflight_id`. Foreign keys reuse the referenced primary-key name. Add a role
  prefix only when the relationship has a distinct meaning, such as
  `rerun_of_run_id`, `evaluated_run_id`, or `blocking_run_id`.
- Do not use PostgreSQL or Snowflake reserved or limited keywords as backend
  table, column, constraint, or index identifiers. Prefer descriptive names
  such as `is_current` and `trigger_type` even when quoting could make a keyword
  legal in one database.
- Data-diff table suffixes describe lifecycle: `_definitions` stores versioned
  configuration, `_attempts` stores executions, `_results` stores execution
  detail, `_state` stores mutable materialized projections, and `_events` or
  `_log` stores append-only history.
- `public` is fixed across Alembic, runtime, tests, ERDs, and docs. Changing it requires a forward migration plan and synchronized updates.
- Each `NNN_*.py` revision needs a matching `NNN_schema.erd.mmd` Mermaid ERD of the resulting `public` schema; preserve old ERDs and show foreign-key relationships on the tables diagram.
- The data-diff backend schema first went live in PipelineWise `0.82.0`.
  Migration 001 is immutable from that release onward; make every later schema
  change in a new forward migration with a matching ERD.
- History is append-oriented: preflight logs, results, and watermark events are
  inserts; definitions, run attempts, run-slot state, and watermark state have
  controlled updates. The database does not enforce immutability.

## Runtime and data-diff constraints

- Soft delete (`hard_delete: false`, `_SDC_DELETED_AT`) is deprecated. Preserve
  compatibility, but add no features/docs and do not restore data-diff
  `exclude_soft_deleted`; new taps use `hard_delete: true`.
- Dev MySQL requires TLS; use `ssl={'': True}`. PyMySQL interpolates bound SQL,
  so double literal tokens, e.g. `DATE_FORMAT(t, '%%Y')`.
- PostgreSQL `reltuples == 0` after ANALYZE-then-load does not prove emptiness;
  partitioned parents can duplicate child estimates. Sum leaf partitions.
- SIGTERM normally does not raise `SystemExit`; durable handling needs an
  installed signal handler, and injected `SystemExit` is not proof.
- Separate backend app roles receive schema/sequence access plus `SELECT`,
  `INSERT`, and `UPDATE`, but no `DELETE`/DDL. A shared app/DDL identity removes
  that separation intentionally.
- Source preflight checks estimates and timestamp-index shape, not exact counts
  or actual index use; bound execution with a statement timeout. Treat
  `min_key`/`max_key` values in `dd_run_results` as sensitive and avoid casual
  logging.

## Snowflake and Iceberg contract

- PipelineWise is the sole automated writer. External reads are allowed; DBA
  writes/DDL require a maintenance window with affected replication stopped and
  no active recovery. Every replicated table and column must originate from
  FullSync, PartialSync, target-snowflake, or the supported converter; never
  adopt arbitrary external schemas or add replicated objects during repair.
- Tap-level `target_table_format: iceberg` plus integer `iceberg_version: 3` is
  the sole managed-Iceberg creation contract; omitted/native creates native.
  Retain both settings for Singer handover and evolution. Route compatible
  Singer taps through target-snowflake and MySQL/PostgreSQL FastSync through the
  shared publisher; keep native `SWAP WITH`. Every v3 tap requires
  `hard_delete: true`; only FastSync-capable MySQL/PostgreSQL also require
  `data_flattening_max_level: 0` (preserve Singer-only defaults such as
  Salesforce level 10).
- Carry `iceberg_version` through tap/generated config, publication/recovery,
  and conversion. Only v3 is supported; reject all others before mutation.
  Future versions require explicit branches and tests.
- `snowflake_iceberg_versions.py` owns each version's format, canonical types,
  existing-table checks, semantic options, and copy-on-write level through
  executable hooks. Keep the dependency-free fixture aligned with
  target-snowflake CREATE, ADD, metadata, and transport behavior; never let a
  new registry entry inherit v3 implicitly.
- Managed v3 requires table-level
  `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED'` in creation, replacement, and
  converter DDL and before writes. Never use deprecated
  `ENABLE_ICEBERG_MERGE_ON_READ`.
- Map FastSync string-like/fallback MySQL, MariaDB, and PostgreSQL types to
  `VARCHAR(134217728)`. Auto-widen compatible narrow native PartialSync targets
  before DML. target-snowflake uses that width for new native/v3 Singer strings,
  leaves compatible existing native strings unchanged, and requires exact width
  on existing v3 strings without implicit widening.
- Exceptional DBA repair must preserve v3, copy-on-write, width, metadata, and
  recovery invariants before replication resumes.
- Key recovery by stable source stream, index the active attempt by physical
  target, and hold both locks throughout. Reject source, target, staging, role,
  transformation, or boundary drift.
- `RecoveryCoordinator` owns the target runtime root, store, ordered locks,
  pointers, persistence, transitions, completion, and abort. Use typed payloads;
  legacy `context` is only a serialized compatibility projection. Reject invalid
  transitions. After validating retained staging keys, ambiguous PartialSync
  MERGE replay rotates submission identity, clears query evidence, durably
  returns to `staged`, then replans.
- Reuse `SnowflakeSqlClient` for authentication/query/transactions and
  `SnowflakeTableInspector` for discovery. Compose publication, finalization,
  conversion-evidence, and `SnowflakeConversionFinalizationValidator`
  explicitly; keep the validator in
  `snowflake_iceberg_conversion_recovery.py`. Do not restore mixin inheritance,
  dynamic binding, or duplicate catalog inspection.
- Advance state only after publication, metadata, grants, and cleanup. Persist
  registered finalization actions only with exact Boolean `true`; require
  grants, S3 cleanup, staging cleanup, and replacement metadata restoration.
  Require a serialized dictionary `source_bookmark`, even empty, and reject
  malformed recovery. PartialSync requires a PK and rejects transformed-stage
  NULL or duplicate key groups before publication.
- Content mismatches may report counts and aggregate fingerprints, never source
  values/samples. Treat bounded query-history visibility/lookup failures as
  retryable ambiguity: preserve state, manifest, and staging; instruct an
  unchanged retry; reserve tracebacks for unexpected errors.
- Guarded replacement/conversion requires the owning account role; reject
  database-role ownership and unsafe dependencies or metadata.
- Conversion remains target-only and fidelity-first. Exclude external writers
  for every copy; `eventual=iceberg` also needs a reader/writer outage. Retain
  the native backup and recover through the manifest. Copy and validate the
  complete row multiset—including duplicate keys and representable flaws—without
  filtering, repair, or deduplication; fail before cutover if v3 cannot represent
  it exactly.
