# PipelineWise Implementation Instructions

Read root `AGENTS.md` first. This file adds guidance under `pipelinewise/`; use the scoped connector, test, E2E, and docs files when those areas are involved.

## Map and boundaries

- `cli/__init__.py`: argparse commands/aliases/dispatch; `cli/pipelinewise.py`: orchestration (`run_tap`, FastSync, data-diff CLI); `cli/commands.py`: `tap | transform-field | mbuffer | target`; `cli/config.py`: YAML validation and generated JSON under `~/.pipelinewise/<target_id>/<tap_id>/`; `cli/constants.py`: connector types/mappings; `cli/schemas/`: JSON Schemas; `cli/alert_handlers/`: Slack/VictorOps (`BaseAlertHandler` for extensions).
- `fastsync/`: native bulk sync. FullSync replaces a table and serves supported initial loads, FULL_TABLE replication, and explicit `fast_sync`; PartialSync exports a filtered range and merges it, serving `partial_sync_table` and tables with `sync_start_from`. FullSync supports MySQL/PostgreSQL/MongoDB to Snowflake/PostgreSQL; PartialSync supports MySQL/PostgreSQL to Snowflake. S3 CSV uses Singer and is not in `FASTSYNC_PAIRS`. Shared primitives belong in `commons/`; `partialsync/` imports them. Keep `docs/concept/fastsync.rst` aligned with behavior, selection, CLI, and supported pairs.
- `backend_db/`: shared PostgreSQL connections, transactions, and Alembic migrations, including optional `ddl_user`/`ddl_password`. It must not depend on data-diff or replication orchestration.
- `data_diff/`: may depend on backend-db, never on Singer/FastSync execution. `adapters.py` owns database metadata/dialects/plans/checksum normalization; `engine.py` orchestration/preflight/connections; `repository.py` persistence; `runner.py` scheduling/backfill/loop; `config.py` YAML/semantics/`CheckDefinition`; `comparison.py` output and decisions; `coverage.py` verified-through watermarks; `runtime.py` the sole generated-connector-JSON boundary; `credentials.py` credential resolution. Add database types at the adapter boundary.

## Backend schema

- Key names omit the shared `dd_` prefix: `{singular_table_name}_id` (`dd_runs.run_id`); FKs reuse the referenced PK name; multiple FKs to one table add a role prefix (`rerun_of_run_id`).
- Every `NNN_*.py` revision has a matching `NNN_schema.erd.excalidraw` representing the resulting `public` schema. Preserve old ERDs.
- Never rewrite a shipped or non-disposable revision. Before initial release, amend an unshipped initial revision only after explicit confirmation that the system is not live and tests rebuild an empty disposable backend; update its ERD.
- ERD FK lines connect columns with 90-degree bends; show `*`, `1`, `1:1` for unique, or `0..1` for nullable; nullable FKs are dashed and required FKs solid.

## Compatibility and traps

- Soft delete (`hard_delete: false`, `_SDC_DELETED_AT`) is scheduled for removal. Preserve compatibility but add no features/docs, use `hard_delete: true` for new taps, and do not restore data-diff `exclude_soft_deleted` support.
- Dev MySQL rejects non-TLS PyMySQL with an authentication-looking error; ad-hoc connections need `ssl={'': True}` like tap-mysql, FastSync, and E2E helpers.
- PyMySQL interpolates bound SQL; double literal format tokens, e.g. `DATE_FORMAT(t, '%%Y')`.
- PostgreSQL `reltuples` may be zero after ANALYZE-then-load, while partitioned parents can duplicate child estimates. Zero does not prove emptiness; sum leaf partitions only.
- SIGTERM normally exits without `SystemExit`. Termination-state persistence needs an installed signal handler; injecting `SystemExit` does not prove it.
- The application backend user has DML only; schema cleanup/object deletion requires the DDL role.
- Future row-level hashes must exclude target-only `_SDC_EXTRACTED_AT`, `_SDC_BATCHED_AT`, `_SDC_DELETED_AT`, and `_SDC_RECEIVED_AT`. Aggregate `row_checksum` avoids this because `compare_columns` is explicit.
