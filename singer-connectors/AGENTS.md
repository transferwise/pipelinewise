# Singer Connector Instructions

Read root `AGENTS.md` and any relevant implementation, test, E2E, or docs guide.

## Environments and CI

This is vendored source, not submodules; root lint/unit gates do not inspect it.
Prefer the ready `pipelinewise` container and report host fallbacks.

Connector CI installs all connectors and runs Python 3.12 unit gates for
tap-mysql (`make unit_test_cov`, 47%), tap-postgres (`make unit_test_cov`, 58%),
and target-snowflake (`make unit_test`, 67%). It does not run integration;
behavior changes need local connector tests and an available E2E route.

Root `make connectors -e pw_connector=<name>` creates runtime
`.virtualenvs/<name>/`; connector Makefiles often use `./venv/` for tests. Never
mix PipelineWise, runtime-connector, connector-test, host, or container
interpreters.

## Validation

The owning Makefile is authoritative. Run its environment, Pylint, unit,
integration, and coverage targets where present; never lower thresholds.
Integration may need containers or credentials.

- Most use `venv`, `pylint`, `unit_test`, and `integration_test`; inspect the
  Makefile for variants.
- PostgreSQL also requires `integration_test_cov` >=63 and `total_cov` >=85; MySQL uses Pytest for unit and integration tests.
- No Makefile: GitHub (`tests/`), Zendesk (Nose), and transform-field (direct suites and Singer E2E). Jira is an external pin without local source/tests; Salesforce has a Makefile but no tests. GitHub, Jira, and Zendesk lack repository E2E.

Report unavailable or skipped integration/E2E coverage as unverified.

### Target-snowflake integration tests in dev-project

Run credentialed target-snowflake integration only in the ready `pipelinewise`
container, never beside another Snowflake group. The suite drops
`TARGET_SNOWFLAKE_SCHEMA`; dedicate that schema. After changing
`dev-project/.env`, recreate the CLI container and wait for its current-start
readiness marker:

```bash
docker compose -f dev-project/docker-compose.yml up -d --force-recreate --no-deps pipelinewise
docker logs --follow pipelinewise
```

Require `PipelineWise Dev environment is ready in Docker container(s).`. The
CSV suite needs standard Snowflake/S3 variables plus
`TARGET_SNOWFLAKE_SCHEMA` and `TARGET_SNOWFLAKE_FILE_FORMAT_CSV` (which may
reuse `TARGET_SNOWFLAKE_FILE_FORMAT`); verify the private key is readable.

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
encryption, but retains wrong-key rejection. Expect 46 passes and zero skips;
anything else is non-green. Full `make integration_test` separately requires
Parquet and a real client-side encryption master key.

## Versioning and upstream

- Connector source ships with PipelineWise. Unless a standalone release is
  explicit, do not bump connector versions or add versioned connector
  changelogs. Put release-visible changes under the current root release;
  include test/CI/fixture changes only when release-relevant. Jira remains an
  external pin.
- These are upstream-derived copies. Coordinate non-trivial divergence upstream; keep local fixes narrow and comments limited to why divergence is needed. Avoid broad formatting.

## Snowflake traps

- Uppercase and double-quote identifiers; with
  `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`, DDL/DML case must match. Compare
  generated types with Snowflake's canonical reports because aliases can cause
  false replacement.
- Connector version prints to stderr; E2E must check exit status because
  `assert_command_success` treats stderr as failure.
- Only tap-level `target_table_format: iceberg` plus integer
  `iceberg_version: 3` creates managed Iceberg; omitted/native creates native.
  Retain both settings through Singer handover/evolution and reject every other
  version before mutation.
- The connector serves all compatible v3 Singer sources; core serves only
  MariaDB/MySQL/PostgreSQL FastSync. Every v3 route needs `hard_delete: true`;
  preserve Singer-only defaults such as Salesforce flattening level 10. Keep
  discovery/type/version contracts aligned without importing core or reducing
  the version to a Boolean; future versions need explicit branches and tests.
- `target_snowflake/managed_iceberg.py` owns per-version configuration,
  discovery, validation, DDL, compatibility, and pure column planning. `DbSync`
  in `target_snowflake/db_sync.py` executes its native/managed plan; do not add
  another Iceberg policy layer. Keep the dependency-free fixture aligned with
  the isolated core.
- Managed v3 requires table-level
  `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED'` in CREATE and before writes;
  keep core parity and never use deprecated `ENABLE_ICEBERG_MERGE_ON_READ`.
- Create managed v3 with `TARGET_FILE_SIZE = 'AUTO'` and
  `STORAGE_SERIALIZATION_POLICY = 'COMPATIBLE'`; keep the dependency-free
  fixture and core FastSync/conversion contract aligned.
- Emit new native/v3 strings as `VARCHAR(134217728)`. Keep compatible existing
  native widths; require exact v3 width and never widen existing Iceberg
  implicitly. Emit v3 binary explicitly as `BINARY(67108864)` in CREATE/ADD.
- PipelineWise is the sole automated writer; external reads are allowed. DBA
  writes/DDL require a maintenance window with replication stopped and no
  recovery, followed by v3 revalidation. Replicated tables/columns must come
  from FullSync, PartialSync, target-snowflake, or the supported converter;
  never adopt arbitrary external schemas.
- Conversion stays in the PipelineWise command; do not restore a connector
  executable or duplicate its type, metadata, or recovery policy.
- Detect MariaDB JSON aliases only from the exact generated `JSON_VALID`
  constraint and explicit v3. Advertise object, array, string, number, Boolean,
  and null roots. Carry non-SQL-null values as validated serialized JSON so
  `PARSE_JSON` restores the root and JSON null remains distinct from SQL NULL;
  preserve ordinary `LONGTEXT` and native mappings.
