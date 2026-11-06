# End-to-End and Dev-Project Instructions

Read root `AGENTS.md`. This guide covers E2E, database checks, connector routes,
and `dev-project/`; connector-source work also follows connector guidance.

## Execution and CI policy

- Run local E2E only when the user explicitly asks. Prefer GitHub E2E when a PR
  exists; a command recipe is not authorisation to run it locally.
- Poll CI status about once a minute. Do not stream test logs or inspect logs
  from successful jobs; read failed-job logs to diagnose failures. Confirm the
  intended named steps ran, not just that an aggregate is green. Report actual
  counts when available and identify skipped/unavailable coverage.
- Target under 15 minutes per shard. Rebalance from measured durations after
  relevant changes or failures; preserve coverage and isolation. Do not repeat
  successful runs without a new reason.
- `.github/workflows/e2e_tests.yml` is authoritative for `e2e_tests_01`–`22`.
  [README.md](README.md) preserves the corresponding local recipes; update it
  with matrix changes. Run all twenty-two only for a full-suite request; otherwise
  select every affected group within the authorised scope.
- CI's twenty-one Snowflake groups use isolated runners. Local groups share/reset
  fixtures and must run serially. Do not reset another session's fixtures or
  recreate its services; coordinate or isolate work first.

## Stack readiness and evidence

Use dev-project Docker for supported database/E2E checks; report host fallbacks/gaps.

Reuse a ready stack only while environment/install inputs are unchanged.
Changes to `.env`, dependencies, connector selection, `entrypoint.sh`,
Dockerfiles, or images require recreating affected services without deleting
volumes. Source is bind-mounted; container environment and virtualenv volumes
are not. Never replace ignored `.env` or recreate volumes without approval:

```bash
docker compose -f dev-project/docker-compose.yml up -d
```

Before database/E2E commands, inspect container state and require the current
start's marker:

```bash
docker ps --format '{{.Names}}\t{{.Status}}'
started_at=$(docker inspect --format '{{.State.StartedAt}}' pipelinewise)
docker logs --since "$started_at" pipelinewise 2>&1 | grep -F "PipelineWise Dev environment is ready"
```

`Up` is insufficient during provisioning. Persisted volumes/config may differ
from CI; only fresh provisioning with required credentials and no unplanned
skips is CI-equivalent. Green workflows may skip E2E jobs: confirm every named
step ran and report pass/skip/fail counts.

## Fidelity coverage

Do not infer native/v3 behaviour from the other format. `SHOW PRIMARY KEYS`
does not prove Iceberg identifier fields; compare raw metadata
`identifier-field-ids` with the current schema field IDs.

Verify exact UTF-8 bytes, SQL NULL versus empty strings, and literal escapes
through native and explicit-v3 FullSync/PartialSync for MariaDB, MySQL, and
PostgreSQL. Reuse `target_snowflake/multiline_values.py`; do not normalize values
in assertions. Connector integration separately covers Singer CSV loading.

Native multiline coverage runs in the PostgreSQL partial, MySQL Iceberg and
MariaDB native CI groups; the matrix contract enforces exact-once coverage.

## Credentials and destructive scope

- Snowflake E2E requires a dedicated database/role, `dev-project/snowflake.pem`,
  `TARGET_SNOWFLAKE_PRIVATE_KEY=/opt/pipelinewise/.ssh/snowflake.pem`,
  `helpers/env.py` fields, and staging credentials. Teardown drops only unique
  run schemas; never use production credentials.
- CI derives `PIPELINEWISE_E2E_NAMESPACE` from run, attempt, and shard. It allows
  only letters, digits, underscores, and hyphens and scopes Snowflake staging,
  archive keys, and tap-S3 fixtures; local runs leave it unset.
- Tap-S3 needs dedicated `TAP_S3_CSV_AWS_KEY`,
  `TAP_S3_CSV_AWS_SECRET_ACCESS_KEY`, and `TAP_S3_CSV_BUCKET`. Setup uploads both
  namespaced fixture objects before discovery, and teardown removes them.
- The archive route removes namespaced files after every test. Use only the
  dedicated `TARGET_SNOWFLAKE_S3_BUCKET`.
- Target-PostgreSQL includes S3 and may skip despite healthy databases;
  credential skips are not passes.

## Topology and traps

- Cleanup tolerates missing generated config; other failures surface.
- Normal databases are health-gated; the MariaDB replica has no
  healthcheck/`depends_on`, so verify it before diagnosing early failures. The
  Oracle MySQL 8 service proves MySQL-specific behavior.
- Keep MongoDB healthcheck as bare `ping`: PipelineWise initializes `rs0`, so
  `rs.status()` deadlocks startup. Keep container `PATH` literal to prevent host
  Compose interpolation.
- `entrypoint.sh` explicitly runs `tap_mysql_db.sh`, `tap_oracle_mysql_db.sh`,
  `tap_postgres_db.sh`, `tap_yugabyte_db.sh`, `tap_mongodb.sh`, and
  `target_postgres.sh`; wire new seed scripts there. Alembic runs only after
  successful `import_config` persists data-diff definitions.
- tap-yugabyte reuses tap-postgres's YSQL-compatible schema/row-count helpers in
  `assertions.py` (`sql_get_columns_postgres`,
  `sql_dynamic_row_count_postgres`) via its own `run_query_tap_yugabyte` entry in
  `_map_tap_to_target_functions`; LOG_BASED coverage lives in
  `test_target_postgres.py::test_replicate_yugabyte_to_pg` alongside
  `test_replicate_pg_to_pg`, so both share group one's serial fixtures — a DML
  value inserted by one test (e.g. `city.id = 4080`) must not collide with a
  fixture the other reuses across reruns. `test_yugabyte_recovery.py` and
  `test_partial_sync_repair_yugabyte_to_sf.py` use their own taps and slots;
  teardown waits out YugabyteDB's active-slot window before dropping them.
- Docker `initdb.d` runs only on empty volumes. Deleting
  `pipelinewise-backend-data` destroys local state; identify it exactly and get
  permission first.
- Use the shared TLS helper for dev MySQL. Check target-snowflake exit status rather than stderr.
- Fixture resets are stateful; after cross-group failures, reset and rerun
  serially before diagnosing a regression.
- `test_target_postgres.py` uses `pytest-dependency`; targeted dependent tests
  skip unless `validate` and `import_config` prerequisites are included.
