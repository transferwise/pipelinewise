# End-to-End and Dev-Project Instructions

Read root `AGENTS.md` first. This covers E2E, database-backed checks, connector routes, and `dev-project/`; connector source also follows its scoped file.

## Stack readiness and evidence

Use dev-project Docker for every supported database and E2E check; report host fallbacks and remaining gaps.

Reuse a ready stack only while environment/install inputs are unchanged. Changes to `.env`, dependencies, connector selection, `entrypoint.sh`, Dockerfiles, or images require recreating affected services without deleting volumes. Source updates are bind-mounted; container environment and virtualenv volumes are not. Never replace the ignored `.env` or recreate volumes without approval:

```bash
docker compose -f dev-project/docker-compose.yml up -d
```

Before database/E2E commands, inspect container state and require a readiness marker from the current start:

```bash
docker ps --format '{{.Names}}\t{{.Status}}'
started_at=$(docker inspect --format '{{.State.StartedAt}}' pipelinewise)
docker logs --since "$started_at" pipelinewise 2>&1 | grep -F "PipelineWise Dev environment is ready"
```

`Up` is insufficient while provisioning installs clients, PipelineWise, and connectors. Persisted volumes/config can differ from CI; only fresh provisioning with required credentials and no unplanned skips is CI-equivalent. A green workflow can contain skipped E2E jobs, so confirm each named test step ran and report pass/skip/fail counts.

## E2E matrix

These groups mirror `.github/workflows/e2e_tests.yml`; update both together. CI isolates jobs, but local groups share/reset fixtures, config, Snowflake, and S3, so run them serially:

```bash
run_e2e() { docker exec -t pipelinewise pytest "$@" -vx --timer-top-n 10; }

run_e2e \
  tests/end_to_end/test_target_postgres.py \
  tests/end_to_end/test_postgres_stream_buffer_recovery.py \
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

Run all five groups only for a full suite; otherwise run each affected group. Snowflake templates omit `iceberg_create`, so they prove native tables only; do not claim Iceberg coverage without an explicit route/config.

## Credentials and destructive scope

- Snowflake E2E requires a dedicated test database/role, `dev-project/snowflake.pem`, `TARGET_SNOWFLAKE_PRIVATE_KEY=/opt/pipelinewise/.ssh/snowflake.pem`, fields from `helpers/env.py`, and target staging credentials. Teardown drops only the run's unique schemas; never use production credentials.
- Tap-S3 requires dedicated `TAP_S3_CSV_AWS_KEY`, `TAP_S3_CSV_AWS_SECRET_ACCESS_KEY`, and `TAP_S3_CSV_BUCKET`. Fixtures overwrite `ppw_e2e_tap_s3_csv/mock_data_{1,2}.csv`; pre-seed both in an empty bucket because discovery precedes upload.
- The archive route deletes `archive_folder/postgres_to_sf_archive_load_files/` in `TARGET_SNOWFLAKE_S3_BUCKET`; reserve it for E2E and avoid concurrent runs.
- Target-PostgreSQL includes an S3 route and may skip despite healthy local databases; credential skips are not passes.

## Topology and traps

- Missing generated config is tolerated during cleanup; other cleanup failures surface.
- Normal databases are health-gated, but the MySQL replica has no healthcheck/`depends_on`; verify it explicitly before diagnosing early failures.
- Keep MongoDB healthcheck as bare `ping`: the PipelineWise container initializes `rs0`, so `rs.status()` deadlocks startup. Keep container `PATH` literal to prevent host Compose interpolation.
- `entrypoint.sh` explicitly runs `tap_mysql_db.sh`, `tap_postgres_db.sh`, `tap_mongodb.sh`, and `target_postgres.sh`; wire new seed scripts there. Alembic runs only when successful `import_config` persists data-diff definitions.
- Docker `initdb.d` runs only on empty volumes. Deleting `pipelinewise-backend-data` destroys local state; identify it exactly and obtain permission first.
- Use the shared TLS helper for dev MySQL. Check target-snowflake exit status rather than stderr.
- Fixture resets are stateful; after cross-group failures, reset and rerun serially before diagnosing a regression.
- `test_target_postgres.py` uses `pytest-dependency`; targeted dependent tests skip unless `validate` and `import_config` prerequisites are included.
