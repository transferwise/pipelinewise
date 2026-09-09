# Singer Connector Instructions

Read root `AGENTS.md` and relevant implementation, test, E2E, and docs guides.

## Environments and CI

These are vendored sources, not submodules, and root lint/unit gates exclude
them. Prefer the ready `pipelinewise` container; report host fallbacks.

Connector CI installs all connectors and runs Python 3.12 units for tap-mysql
(`make unit_test_cov`, 47%), tap-postgres (`make unit_test_cov`, 58%), and
target-snowflake (`make unit_test`, 67%). It excludes integration; behavior
changes need local connector tests and an available E2E route.

Root `make connectors -e pw_connector=<name>` creates runtime
`.virtualenvs/<name>/`; connector Makefiles often test in `./venv/`. Never mix
PipelineWise, runtime-connector, connector-test, host, or container interpreters.

## Validation

The owning Makefile is authoritative. Run available environment, Pylint, unit,
integration, and coverage targets without lowering thresholds; integration may
need containers or credentials.

Legacy connector Pylint configurations may not be green with the installed
Pylint. Preserve or improve the master score and finding set, and report the
baseline configuration errors and findings rather than claiming the gate passed.

- Most use `venv`, `pylint`, `unit_test`, and `integration_test`; inspect the
  Makefile for variants.
- PostgreSQL also requires `integration_test_cov` >=63 and `total_cov` >=85;
  MySQL uses Pytest for unit and integration tests.
- Without Makefiles: GitHub uses `tests/`, Zendesk uses Nose, and
  transform-field uses direct suites and Singer E2E. Jira is an external pin
  without local source/tests; Salesforce has a Makefile but no tests. GitHub,
  Jira, and Zendesk lack repository E2E.

Report unavailable or skipped integration/E2E coverage as unverified.

### Target-snowflake integration tests in dev-project

Run credentialed target-snowflake integration only in the ready `pipelinewise`
container and never beside another Snowflake group. The suite drops
`TARGET_SNOWFLAKE_SCHEMA`; dedicate it. After changing `dev-project/.env`,
recreate the CLI container and await its current-start readiness marker:

```bash
docker compose -f dev-project/docker-compose.yml up -d --force-recreate --no-deps pipelinewise
docker logs --follow pipelinewise
```

Require `PipelineWise Dev environment is ready in Docker container(s).`. The
CSV suite needs standard Snowflake/S3 variables,
`TARGET_SNOWFLAKE_SCHEMA`, and `TARGET_SNOWFLAKE_FILE_FORMAT_CSV` (which may
reuse `TARGET_SNOWFLAKE_FILE_FORMAT`); ensure the private key is readable.

Run the supported 46-test subset with plaintext upload explicitly selected:

```bash
docker exec -t -e CLIENT_SIDE_ENCRYPTION_MASTER_KEY= pipelinewise bash -lc '
  cd /opt/pipelinewise/singer-connectors/target-snowflake
  . ./venv/bin/activate
  pytest tests/integration -vvx \
    -k "not test_parquet and not test_table_stage and \
        (not test_loading_tables_with_client_side_encryption or wrong_master_key)"
'
```

This excludes Parquet, mixed CSV/Parquet table-stage, and successful client-side
encryption while retaining wrong-key rejection. Expect 46 passes, zero skips;
anything else is non-green. Full `make integration_test` separately requires
Parquet and a real client-side encryption master key.

## Versioning and upstream

- Connector source ships with PipelineWise. Unless a standalone release is
  explicit, do not bump connector versions or add versioned connector
  changelogs. Put release-visible changes in the current root release; include
  test/CI/fixture changes only when release-relevant. Jira remains an external pin.
- These are upstream-derived copies. Coordinate non-trivial divergence
  upstream; keep local fixes narrow, comments limited to why divergence is
  needed, and avoid broad formatting.
- PostgreSQL sources require version 11.2 or later for every Singer replication
  method and PipelineWise FullSync/PartialSync. Keep their version checks aligned;
  this source minimum does not constrain target-postgres or the PipelineWise
  backend database.

## Snowflake traps

The Snowflake/Iceberg contract in `pipelinewise/AGENTS.md` is authoritative for
creation settings, versions, types, writer/DBA boundaries, and conversion
invariants. Connector-specific rules follow:

- Uppercase and double-quote identifiers. With
  `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`, DDL/DML case must match. Compare
  generated types with Snowflake canonical reports; aliases can cause false
  replacement.
- Connector version prints to stderr; E2E must check exit status because
  `assert_command_success` treats stderr as failure.
- The connector serves all compatible v3 Singer sources; core serves only
  MariaDB/MySQL/PostgreSQL FastSync. Align discovery/type/version contracts
  without importing core or reducing version to Boolean.
- `target_snowflake/managed_iceberg.py` owns per-version configuration,
  discovery, validation, DDL, compatibility, and pure column planning. `DbSync`
  in `target_snowflake/db_sync.py` executes native/managed plans; add no other
  Iceberg policy layer. Keep the dependency-free fixture aligned with core.
- Keep all managed-v3 DDL/type/version settings and the dependency-free fixture
  in exact core parity. CREATE/ADD emits v3 binary as `BINARY(67108864)`.
- Conversion stays in the PipelineWise command; do not restore a connector
  executable or duplicate its type, metadata, or recovery policy.
- Detect MariaDB JSON aliases only from the exact generated `JSON_VALID`
  constraint and explicit v3. Advertise object, array, string, number, Boolean,
  and null roots. Carry non-SQL-null values as validated serialized JSON so
  `PARSE_JSON` restores roots and keeps JSON null distinct from SQL NULL;
  preserve ordinary `LONGTEXT` and native mappings.
