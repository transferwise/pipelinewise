# PipelineWise Implementation Instructions

Read root `AGENTS.md` first. This file adds guidance under `pipelinewise/`; use the scoped connector, test, E2E, and docs files when those areas are involved.

## Map and boundaries

- `cli/__init__.py`: argparse commands/aliases/dispatch; `cli/pipelinewise.py`: orchestration (`run_tap`, FastSync, data-diff CLI); `cli/commands.py`: `tap | transform-field | mbuffer | target`; `cli/config.py`: YAML validation and generated JSON under `$PIPELINEWISE_CONFIG_DIRECTORY/<target_id>/<tap_id>/` (default `~/.pipelinewise`); `cli/constants.py`: connector types/mappings; `cli/schemas/`: JSON Schemas; `cli/alert_handlers/`: Slack/VictorOps (`BaseAlertHandler` for extensions).
- `fastsync/`: native bulk sync. FullSync replaces a table and serves supported initial loads, FULL_TABLE replication, and explicit `fast_sync`; PartialSync exports a filtered range and merges it, serving `partial_sync_table` and tables with `sync_start_from`. FullSync supports MySQL/PostgreSQL/MongoDB to Snowflake/PostgreSQL; PartialSync supports MySQL/PostgreSQL to Snowflake. S3 CSV uses Singer and is not in `FASTSYNC_PAIRS`. Shared primitives belong in `commons/`; `partialsync/` imports them. Keep `docs/concept/fastsync.rst` aligned with behavior, selection, CLI, and supported pairs.
- `backend_db/`: shared PostgreSQL connections, transactions, and Alembic migrations. `ddl_user` and `ddl_password` are required; they may match application credentials for an intentional single-role setup. It must not depend on data-diff or replication orchestration.
- `data_diff/`: may depend on backend-db, never on Singer/FastSync execution. Supported routes are MySQL/MariaDB or PostgreSQL to PostgreSQL or Snowflake. `adapters.py` owns dialect SQL, metadata, aggregate execution, and normalization; `engine.py` connections, catalog preflight, and check execution; `repository.py` persistence; `runner.py` scheduling, bounded backfill, execution, and remediation; `config.py` YAML semantics; `comparison.py` type compatibility and result evaluation; `coverage.py` watermarks; `runtime.py` the generated-connector-JSON boundary; `credentials.py` private-key conversion. Add database types at the adapter boundary.

## Backend schema

- PKs omit the shared `dd_` prefix (`dd_runs.run_id`). FKs normally reuse the referenced PK; `dd_runs.dd_check_id` is the existing exception. Role-qualified relationships use names such as `rerun_of_run_id`.
- `public` is deliberately fixed in Alembic's version-table config, migration SQL, and `DataDiffRepository`; do not make it configurable without a forward migration plan and synchronized runtime, test, ERD, and docs changes.
- Every `NNN_*.py` revision has a matching `NNN_schema.erd.excalidraw` representing the resulting `public` schema. Preserve old ERDs.
- Never rewrite a revision present in a release or run against a non-disposable database. Before first deployment, amend an unshipped revision only after explicit confirmation that all affected databases are disposable; rebuild an empty backend and update its ERD.
- ERD FK lines connect columns with 90-degree bends; show `*`, `1`, `1:1` for unique, or `0..1` for nullable; nullable FKs are dashed and required FKs solid.
- History is append-oriented: repository code inserts preflights, results, and coverage events, while checks, runs, effective attempts, and coverage state have controlled lifecycle updates. The database does not enforce immutability.

## Compatibility and traps

- Soft delete (`hard_delete: false`, `_SDC_DELETED_AT`) is deprecated and planned for removal. Preserve compatibility while supported but add no features/docs, use `hard_delete: true` for new taps, and do not restore data-diff `exclude_soft_deleted` support.
- Dev MySQL rejects non-TLS PyMySQL with an authentication-looking error; ad-hoc connections need `ssl={'': True}` like tap-mysql, FastSync, and E2E helpers.
- PyMySQL interpolates bound SQL; double literal format tokens, e.g. `DATE_FORMAT(t, '%%Y')`.
- PostgreSQL `reltuples` may be zero after ANALYZE-then-load, while partitioned parents can duplicate child estimates. Zero does not prove emptiness; sum leaf partitions only.
- SIGTERM normally exits without `SystemExit`. Termination-state persistence needs an installed signal handler; injecting `SystemExit` does not prove it.
- A distinct application backend role receives `SELECT`, `INSERT`, and `UPDATE`, plus required schema/sequence access; it has no `DELETE` or DDL grants. Cleanup and object deletion require the DDL role. Using one identity for both roles removes this separation.
- Source preflight evaluates catalog-derived row estimates and timestamp-index shape; it neither counts exact rows nor proves a window query will use that index. Statement timeout bounds query duration.
- Treat persisted `dd_results` values as data: `min_key` and `max_key` can contain source boundary values. Do not expose them casually in logs or diagnostics.
- Singer target-snowflake can preserve or create Iceberg tables, but FastSync/PartialSync use separate native DDL paths and do not inherit `iceberg_create`.
- Future row-level hashes must exclude target-only `_SDC_EXTRACTED_AT`, `_SDC_BATCHED_AT`, `_SDC_DELETED_AT`, and `_SDC_RECEIVED_AT`. Aggregate `row_checksum` avoids this because `compare_columns` is explicit.
