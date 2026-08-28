# End-to-End and Dev-Project Instructions

Read root `AGENTS.md`. This file covers E2E, database checks, connector routes,
and `dev-project/`; connector source also follows connector guidance.

## Stack readiness and evidence

Use dev-project Docker for supported database/E2E checks; report host fallbacks
and gaps.

Reuse a ready stack only while environment/install inputs are unchanged. Changes
to `.env`, dependencies, connector selection, `entrypoint.sh`, Dockerfiles, or
images require recreating affected services without deleting volumes. Source is
bind-mounted; container environment and virtualenv volumes are not. Never
replace ignored `.env` or recreate volumes without approval:

```bash
docker compose -f dev-project/docker-compose.yml up -d
```

Before database/E2E commands, inspect container state and require a readiness marker from the current start:

```bash
docker ps --format '{{.Names}}\t{{.Status}}'
started_at=$(docker inspect --format '{{.State.StartedAt}}' pipelinewise)
docker logs --since "$started_at" pipelinewise 2>&1 | grep -F "PipelineWise Dev environment is ready"
```

`Up` is insufficient during provisioning. Persisted volumes/config may differ
from CI; only fresh provisioning with required credentials and no unplanned
skips is CI-equivalent. Green workflows may skip E2E jobs: confirm each named
step ran and report pass/skip/fail counts.

## E2E matrix

These groups mirror `.github/workflows/e2e_tests.yml`; update both together. CI
runs the eight Snowflake groups concurrently on isolated runners. Local groups
share/reset fixtures and config, so run them serially:

```bash
run_e2e() { docker exec -t pipelinewise pytest "$@" -vx --timer-top-n 10; }

run_e2e \
  tests/end_to_end/test_target_postgres.py \
  tests/end_to_end/test_postgres_stream_buffer_recovery.py \
  tests/end_to_end/data_diff/test_postgres_to_postgres.py \
  tests/end_to_end/data_diff/test_mysql_to_postgres.py

run_e2e \
  tests/end_to_end/target_snowflake/test_native_to_iceberg_converter.py \
  tests/end_to_end/data_diff/test_mysql_to_snowflake.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_postgres/test_snowflake_iceberg_publisher.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_replica_to_sf.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_postgres/test_partial_sync_pg_to_sf.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_to_sf_with_custom_buffer_size.py \
  tests/end_to_end/data_diff/test_postgres_to_snowflake.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_mariadb/test_partial_sync_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_defined_partial_sync_pg_to_sf.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_resync_pg_to_sf_with_split_large_files.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_postgres/test_iceberg_v3_postgres_to_sf.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_replicate_pg_to_sf.py \
  tests/end_to_end/target_snowflake/tap_s3/test_replicate_s3_to_sf.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_mariadb/test_iceberg_v3_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_to_sf_soft_delete.py \
  tests/end_to_end/target_snowflake/tap_mongodb/test_replicate_mongodb_to_sf.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_mysql/test_iceberg_v3_mysql_to_sf.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_resync_pg_to_sf_table_size_check.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_resync_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_replicate_pg_to_sf_with_archive_load_files.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_mariadb/test_resync_mariadb_to_sf_table_size_check.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_defined_partial_sync_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_resync_mariadb_to_sf_with_split_large_files.py
```

Run all nine only for a full suite; otherwise run every affected group. MariaDB
and PostgreSQL cover native and explicit v3; genuine MySQL covers explicit v3.
Do not infer one format from the other. `SHOW PRIMARY KEYS` does not prove
Iceberg identifier fields; inspect raw metadata and compare
`identifier-field-ids` with current schema field IDs.

## Credentials and destructive scope

- Snowflake E2E requires a dedicated database/role,
  `dev-project/snowflake.pem`,
  `TARGET_SNOWFLAKE_PRIVATE_KEY=/opt/pipelinewise/.ssh/snowflake.pem`,
  `helpers/env.py` fields, and staging credentials. Teardown drops only unique
  run schemas; never use production credentials.
- CI sets `PIPELINEWISE_E2E_NAMESPACE` from the run, attempt, and shard. It must
  contain only letters, digits, underscores, and hyphens. The namespace scopes
  Snowflake staging, archive keys, and tap-S3 fixtures; local runs leave it unset.
- Tap-S3 needs dedicated `TAP_S3_CSV_AWS_KEY`,
  `TAP_S3_CSV_AWS_SECRET_ACCESS_KEY`, and `TAP_S3_CSV_BUCKET`. Setup uploads both
  namespaced fixture objects before discovery, and teardown removes them.
- The archive route removes its namespaced files after every test. Use only the
  dedicated `TARGET_SNOWFLAKE_S3_BUCKET`.
- Target-PostgreSQL includes S3 and may skip despite healthy databases;
  credential skips are not passes.

## Topology and traps

- Missing generated config is tolerated during cleanup; other cleanup failures surface.
- Normal databases are health-gated; the MariaDB replica has no
  healthcheck/`depends_on`, so verify it before diagnosing early failures. The
  Oracle MySQL 8 service proves MySQL-specific behavior.
- Keep MongoDB healthcheck as bare `ping`: the PipelineWise container initializes `rs0`, so `rs.status()` deadlocks startup. Keep container `PATH` literal to prevent host Compose interpolation.
- `entrypoint.sh` explicitly runs `tap_mysql_db.sh`, `tap_oracle_mysql_db.sh`,
  `tap_postgres_db.sh`, `tap_mongodb.sh`, and `target_postgres.sh`; wire new seed
  scripts there. Alembic runs only after successful `import_config` persists
  data-diff definitions.
- Docker `initdb.d` runs only on empty volumes. Deleting `pipelinewise-backend-data` destroys local state; identify it exactly and obtain permission first.
- Use the shared TLS helper for dev MySQL. Check target-snowflake exit status rather than stderr.
- Fixture resets are stateful; after cross-group failures, reset and rerun
  serially before diagnosing a regression.
- `test_target_postgres.py` uses `pytest-dependency`; targeted dependent tests
  skip unless `validate` and `import_config` prerequisites are included.
