# End-to-End and Dev-Project Instructions

Read root `AGENTS.md` first. This covers E2E, database-backed checks, connector routes, and `dev-project/`; connector source remains governed by its scoped file.

## Ready stack and result quality

Reuse a ready stack only when its environment and install inputs are unchanged. Editing `.env`, dependency files, connector selection, `entrypoint.sh`, Dockerfiles, or image inputs requires recreating or rebuilding the affected services without deleting data volumes; bind-mounted source updates immediately, but container environment and the anonymous dev-virtualenv volume do not. If E2E was requested and no stack exists, require the existing ignored `dev-project/.env`, then start it without replacing credentials or volumes:

```bash
docker compose -f dev-project/docker-compose.yml up -d
```

Before database/E2E commands, require both container state and a readiness marker from the current container start:

```bash
docker ps --format '{{.Names}}\t{{.Status}}'
started_at=$(docker inspect --format '{{.State.StartedAt}}' pipelinewise)
docker logs --since "$started_at" pipelinewise 2>&1 | grep -F "PipelineWise Dev environment is ready"
```

`Up` alone is insufficient while provisioning installs clients, PipelineWise, and the E2E connector set. The repository is bind-mounted at `/opt/pipelinewise`; startup is slow on Apple Silicon. Never copy `.env.template` over an existing `.env` or recreate volumes without explicit approval.

The local topology/commands match CI, but results are CI-equivalent only after fresh full provisioning with required credentials and no unplanned skips; persisted volumes/config can differ. E2E workflow setup and test steps run only when `ci_check_no_file_changes.sh python config` returns failure, so a green job can mean tests were skipped. Confirm the named `Run ... end-to-end tests` step executed and report pass/skip/fail counts for every group.

## Full E2E matrix

Each CI job provisions an independent Docker stack, but Snowflake and S3 remain shared external services. Local groups share and reset state, fixtures, and config. Run serially inside the ready container:

```bash
run_e2e() { docker exec -t pipelinewise pytest "$@" -vx --timer-top-n 10; }

run_e2e \
  tests/end_to_end/test_target_postgres.py \
  tests/end_to_end/data_diff/test_postgres_to_postgres.py \
  tests/end_to_end/data_diff/test_mysql_to_postgres.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_mariadb \
  tests/end_to_end/data_diff/test_mysql_to_snowflake.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_postgres \
  tests/end_to_end/data_diff/test_postgres_to_snowflake.py

run_e2e tests/end_to_end/target_snowflake/tap_mongodb
run_e2e tests/end_to_end/target_snowflake/tap_s3
```

Run all five groups only when a full suite is requested; otherwise run each group whose source, target, connector/helper, backend migration, FastSync, or data-diff path may be affected.

Current Snowflake E2E templates do not set `iceberg_create`; these groups prove native-table routes only. Do not claim Iceberg E2E coverage until an explicit Iceberg route/config exists.

## Credentials and destructive scope

- For local Snowflake E2E, put the key at `dev-project/snowflake.pem` and set `TARGET_SNOWFLAKE_PRIVATE_KEY=/opt/pipelinewise/.ssh/snowflake.pem` in `.env`; populate the other fields required by `tests/end_to_end/helpers/env.py`. All Snowflake groups require the target AWS/S3 staging fields, including `TARGET_SNOWFLAKE_AWS_ACCESS_KEY` and `TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY`. Use a dedicated test database and role: setup and teardown drop matching `PPW_E2E_<tap type>_%` and data-diff schemas. Never use production credentials.
- The tap-S3 route requires `TAP_S3_CSV_AWS_KEY`, `TAP_S3_CSV_AWS_SECRET_ACCESS_KEY`, and a dedicated `TAP_S3_CSV_BUCKET`; fixtures overwrite `ppw_e2e_tap_s3_csv/mock_data_{1,2}.csv`. A completely empty bucket must be pre-seeded with both keys because discovery currently precedes fixture upload.
- The archive route deletes `archive_folder/postgres_to_sf_archive_load_files/` in `TARGET_SNOWFLAKE_S3_BUCKET`. Reserve that prefix for E2E and do not run this route concurrently against the same bucket.
- Target-PostgreSQL includes an S3 route and can skip despite healthy local databases. Credential skips are not passes.

## Ownership and stack constraints

- `test_target_postgres.py`: Singer/FastSync to local PostgreSQL. Snowflake route directories cover MariaDB, PostgreSQL, MongoDB, and S3; their classes inherit `TargetSnowflake`, whose setup attempts to drop every matching stale `PPW_E2E_*` schema for that tap type and swallows cleanup errors.
- `data_diff/test_postgres_to_postgres.py`: failure, remediation, coverage, and empty-backend migration lifecycle. Other data-diff modules prove cross-dialect checksum agreement for MySQL-to-PostgreSQL, MySQL-to-Snowflake, and PostgreSQL-to-Snowflake.
- `pipelinewise` health-gates normal database dependencies with `condition: service_healthy`, but `pipelinewise-mysql-source-replica` has neither a healthcheck nor a `depends_on` entry even though startup/tests connect to it. Treat early replica failures as a possible startup race and verify readiness explicitly.
- Keep MongoDB healthcheck as bare `ping`; the PipelineWise container initializes `rs0` afterward, so requiring `rs.status()` deadlocks startup. Keep container `PATH` literal because Compose expands `${PATH}` from the macOS host.
- `entrypoint.sh` hardcodes `tap_mysql_db.sh`, `tap_postgres_db.sh`, `tap_mongodb.sh`, and `target_postgres.sh` on each PipelineWise-container start. New seed scripts run only after being wired there. Alembic runs when a successful `import_config` persists data-diff definitions, not on container startup or first connection.
- Docker `initdb.d` runs only for an empty volume. If a change requires recreating `pipelinewise-backend-data`, identify that exact volume and obtain permission before deletion; it destroys local backend state.

## Traps

- Run E2E inside the container; host database/connector environments differ.
- `assert_command_success` treats stderr as failure, while target-snowflake prints its version there; assert process exit status.
- Dev MySQL requires TLS; use the shared E2E helper for ad-hoc access.
- Fixture resets are stateful. After a cross-group failure, reset the affected fixture and rerun serially before diagnosing a regression.
- `test_target_postgres.py` uses `pytest-dependency`; a targeted dependent test skips unless its `validate` and `import_config` prerequisites are included.
