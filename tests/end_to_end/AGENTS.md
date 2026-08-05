# End-to-End and Dev-Project Instructions

Read root `AGENTS.md` first. This covers E2E, database-backed checks, connector routes, and `dev-project/`; connector source remains governed by its scoped file.

## Ready stack and result quality

Before database/E2E commands, require both container state and the application readiness marker:

```bash
docker ps --format '{{.Names}}\t{{.Status}}'
docker logs pipelinewise 2>&1 | grep -F "PipelineWise Dev environment is ready"
```

`Up` alone is insufficient while provisioning installs clients, PipelineWise, and connectors. Use an existing ready container; the repo is bind-mounted at `/opt/pipelinewise`. If none exists, tell the user before starting it. Startup is slow on Apple Silicon and needs `dev-project/.env`; never overwrite that ignored file because it may contain live credentials.

The local topology/commands match CI, but results are CI-equivalent only after fresh full provisioning with required credentials and no unplanned skips; persisted volumes/config can differ. Report pass/skip/fail counts for every group.

## Full E2E matrix

CI isolates groups; local groups share/reset state, fixtures, and config. Run serially inside the ready container:

```bash
docker exec -t pipelinewise pytest \
  tests/end_to_end/test_target_postgres.py \
  tests/end_to_end/data_diff/test_postgres_to_postgres.py \
  tests/end_to_end/data_diff/test_mysql_to_postgres.py \
  -vx --timer-top-n 10

docker exec -t pipelinewise pytest \
  tests/end_to_end/target_snowflake/tap_mariadb \
  tests/end_to_end/data_diff/test_mysql_to_snowflake.py \
  -vx --timer-top-n 10

docker exec -t pipelinewise pytest \
  tests/end_to_end/target_snowflake/tap_postgres \
  tests/end_to_end/data_diff/test_postgres_to_snowflake.py \
  -vx --timer-top-n 10

docker exec -t pipelinewise pytest \
  tests/end_to_end/target_snowflake/tap_mongodb \
  -vx --timer-top-n 10

docker exec -t pipelinewise pytest \
  tests/end_to_end/target_snowflake/tap_s3 \
  -vx --timer-top-n 10
```

Run all five groups only when a full suite is requested; otherwise run each group whose source, target, connector/helper, backend migration, FastSync, or data-diff path may be affected.

Snowflake groups require `TARGET_SNOWFLAKE_*` and the private key in `.env`; S3 also needs `TAP_S3_CSV_AWS_KEY`, `TAP_S3_CSV_AWS_SECRET_ACCESS_KEY`, and `TAP_S3_CSV_BUCKET`. Target-PostgreSQL includes an S3 route and can skip despite healthy local databases. Never report credential skips as passes.

## Ownership and stack constraints

- `test_target_postgres.py`: Singer/FastSync to local PostgreSQL. Snowflake route directories cover MariaDB, PostgreSQL, MongoDB, and S3; their classes inherit `TargetSnowflake`, whose setup clears stale per-tap `PPW_E2E_*` schemas.
- `data_diff/test_postgres_to_postgres.py`: failure, remediation, coverage, and empty-backend migration lifecycle. Other data-diff modules prove cross-dialect checksum agreement for MySQL-to-PostgreSQL, MySQL-to-Snowflake, and PostgreSQL-to-Snowflake.
- `pipelinewise` health-gates normal database dependencies with `condition: service_healthy`, but `pipelinewise-mysql-source-replica` has neither a healthcheck nor a `depends_on` entry even though startup/tests connect to it. Treat early replica failures as a possible startup race and verify readiness explicitly.
- Keep MongoDB healthcheck as bare `ping`; the PipelineWise container initializes `rs0` afterward, so requiring `rs.status()` deadlocks startup. Keep container `PATH` literal because Compose expands `${PATH}` from the macOS host.
- `entrypoint.sh` runs every `tests/db/*.sh` fixture seed on each PipelineWise-container start and installs source/connectors. It does not run Alembic; the application upgrades lazily on first backend connection.
- Docker `initdb.d` runs only for an empty volume. If a change requires recreating `pipelinewise-backend-data`, identify that exact volume and obtain permission before deletion; it destroys local backend state.

## Traps

- Run E2E inside the container; host database/connector environments differ.
- `assert_command_success` treats stderr as failure, while target-snowflake prints its version there; assert process exit status.
- Dev MySQL requires TLS; use the shared E2E helper for ad-hoc access.
- Fixture resets are stateful. After a cross-group failure, reset the affected fixture and rerun serially before diagnosing a regression.
