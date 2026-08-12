# PipelineWise Implementation Instructions

Read root `AGENTS.md` first; also use scoped connector, test, E2E, and docs guidance where relevant.

## Map and boundaries

- `cli/__init__.py`: commands, aliases, dispatch. `cli/pipelinewise.py`: orchestration. `cli/commands.py`: Singer pipeline. `cli/config.py`: YAML validation and generated JSON under `$PIPELINEWISE_CONFIG_DIRECTORY/<target_id>/<tap_id>/` (default `~/.pipelinewise`). `cli/constants.py`: connector types/mappings. `cli/schemas/`: JSON Schemas. `cli/alert_handlers/`: Slack/VictorOps; extend `BaseAlertHandler`.
- `fastsync/`: native bulk sync. FullSync replaces tables for initial loads, FULL_TABLE, and explicit `fast_sync`; PartialSync merges filtered ranges for `partial_sync_table` and `sync_start_from`. FullSync supports `tap-mysql` (MariaDB/MySQL), `tap-postgres`, and `tap-mongodb` to PostgreSQL/Snowflake; PartialSync supports `tap-mysql` and `tap-postgres` to Snowflake. S3 CSV stays on Singer and outside `FASTSYNC_PAIRS`. Put shared primitives in `commons/`, imported by `partialsync/`; keep `docs/concept/fastsync.rst` aligned.
- `backend_db/`: PostgreSQL connections, transactions, Alembic. `ddl_user`/`ddl_password` are required, though they may equal app credentials. It must not depend on data-diff or replication orchestration; an AST test enforces this boundary.
- `data_diff/`: may use backend-db, never Singer/FastSync execution. Supported routes are MySQL/MariaDB or PostgreSQL to PostgreSQL/Snowflake. `adapters.py` owns dialect behavior; `engine.py` execution; `repository.py` persistence; `runner.py` scheduling/remediation; `config.py`, `comparison.py`, and `coverage.py` own their named concerns; `runtime.py` generated connector JSON; `credentials.py` private keys. `import_config` persists definitions only after connector generation/discovery. Add database types at the adapter boundary and AST coverage for new dependency seams.

## Backend schema

- PKs omit `dd_` (`dd_runs.run_id`); FKs normally reuse referenced PK names, except existing `dd_runs.dd_check_id`. Name role-qualified FKs clearly, e.g. `rerun_of_run_id`.
- `public` is fixed across Alembic, runtime, tests, ERDs, and docs. Changing it requires a forward migration plan and synchronized updates.
- Each `NNN_*.py` revision needs a matching `NNN_schema.erd.excalidraw` of the resulting `public` schema; preserve old ERDs. FK lines use right angles and `*`, `1`, `1:1`, or `0..1`; nullable is dashed, required solid.
- Never rewrite a released/applied migration. Before first deployment, amend an unshipped revision only after explicit confirmation that all databases are disposable; rebuild an empty backend and update its ERD.
- History is append-oriented: preflights, results, and coverage events are inserts; checks, runs, effective attempts, and coverage state have controlled updates. The database does not enforce immutability.

## Compatibility and traps

- Soft delete (`hard_delete: false`, `_SDC_DELETED_AT`) is deprecated: preserve current compatibility, but add no features/docs and do not restore data-diff `exclude_soft_deleted`; new taps use `hard_delete: true`.
- Dev MySQL rejects non-TLS PyMySQL with an authentication-looking error; use `ssl={'': True}`. PyMySQL interpolates bound SQL, so double literal tokens, e.g. `DATE_FORMAT(t, '%%Y')`.
- PostgreSQL `reltuples == 0` after ANALYZE-then-load does not prove emptiness; partitioned parents can duplicate child estimates. Sum leaf partitions only.
- SIGTERM normally does not raise `SystemExit`; persistence requires an installed signal handler, and injected `SystemExit` does not prove it.
- Separate backend app roles receive schema/sequence access plus `SELECT`, `INSERT`, `UPDATE`, but no `DELETE`/DDL. A shared app/DDL identity intentionally removes this separation.
- Source preflight checks catalog estimates and timestamp-index shape, not exact counts or actual index use; statement timeout bounds execution.
- Treat `dd_results.min_key`/`max_key` as sensitive source data; avoid casual logging.
- Singer target-snowflake can preserve/create Iceberg tables; native FastSync/PartialSync DDL does not inherit `iceberg_create`.
