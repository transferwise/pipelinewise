# Singer Connector Instructions

Read root `AGENTS.md`, the core contracts in `pipelinewise/AGENTS.md` and the
relevant test, E2E and docs guides. These are vendored sources, not submodules.

## Environments and CI

Root Ruff scope applies; root units do not collect connectors. Connector CI
installs all connectors and runs these Python 3.12 unit gates:

| Connector | Make target | Coverage floor |
| --- | --- | --- |
| tap-mysql | `unit_test_cov` | 47% |
| tap-postgres | `unit_test_cov` | 58% |
| tap-yugabyte | `unit_test_cov` | 65% |
| target-postgres | `unit_test` | 44% |
| target-snowflake | `unit_test` | 90% |

The shared Singer job runs MongoDB source-logging regressions in a separate,
database-free environment; other MongoDB units remain local. E2E job 01 runs SQL
source/PostgreSQL-target decimal integrations; jobs 15, 16 and 20 run Snowflake
decimal/runtime integrations via `scripts/test_decimal_connectors.sh`. Inspect
the actual workflow commands for other integration coverage: a test file or a
green aggregate does not prove collection. Behaviour changes need the owning
connector tests and an appropriate E2E route, subject to the root local-E2E rule.

Root `make connectors -e pw_connector=<name>` creates runtime
`.virtualenvs/<name>/`; connector Makefiles often test in `./venv/`. Keep these
separate from PipelineWise and host environments as required by the root guide.
Yugabyte bulk executables use isolated `.virtualenvs/fastsync-yugabyte/` with
the forked driver; keep stock psycopg2 in the main/backend/data-diff environment.

## Validation

The owning Makefile's `lint` target uses its venv's Ruff from the repository root,
so root `pyproject.toml` applies. The five CI-tested connectors lint source,
CI unit suites and shared helpers; other targets lint source only. Follow root
line-length, quotes, lambda-assignment, complexity and narrow-ignore rules. Add
no connector-local lint configuration, other linter or automatic formatter.
Units and integration tests provide behavioural evidence, not lint.

Run available environment, lint, unit, integration, and coverage targets
without lowering thresholds; integration may need containers or credentials.

- Most use `venv`, `lint`, `unit_test`, and `integration_test`; inspect the
  Makefile for variants.
- PostgreSQL also requires `integration_test_cov` >=63 and `total_cov` >=85;
  MySQL uses Pytest for unit and integration tests.
- Without Makefiles: GitHub uses `tests/`, Zendesk uses Nose, and
  transform-field uses direct suites and Singer E2E. Jira is an external pin
  without local source/tests; Salesforce has a Makefile but no tests. GitHub,
  Jira, and Zendesk lack repository E2E.

Use distinct `COVERAGE_FILE` paths for overlapping runs; report unit, integration
and combined coverage separately. Combining suites does not satisfy either
individual gate; unavailable/skipped integrations remain unverified. Report
actual counts, not a historical expected total.

Credentialed target-snowflake integrations require the ready container and an
isolated `TARGET_SNOWFLAKE_SCHEMA`, which they drop. Never overlap Snowflake
groups or another session's fixtures. The complete environment, recreation,
plaintext-upload and encryption-exclusion recipe is in
`tests/end_to_end/README.md`; root shared-session and E2E restrictions apply.

## Versioning and upstream

- Connector source ships with PipelineWise. Unless a standalone release is
  explicit, do not bump connector versions or add versioned connector
  changelogs. Put release-visible changes in the current root release; include
  test/CI/fixture changes only when release-relevant. Jira remains an external pin.
- The shared vendored Singer package has its own compatibility version. When its
  API changes, bump that version and every local consumer pin together. Check a
  clean wheel/sdist installation when packaging is affected; editable installs
  alone cannot expose an older cached wheel satisfying an unchanged pin.
- These are upstream-derived copies. Coordinate non-trivial divergence
  upstream; keep local fixes narrow, comments limited to why divergence is
  needed, and avoid broad formatting.
- Use the core **PostgreSQL 14+** source floor across Singer/FullSync/PartialSync
  and keep connection gates aligned. Targets and backend DBs have separate
  requirements.
- PostgreSQL discovery's `row-count` uses `reltuples`, which can be stale or zero
  after ANALYZE-then-load. Zero does not prove emptiness. Sum leaf estimates for
  partitioned totals without adding parent estimates.
- Follow core delete/state and decimal contracts. Keep `_SDC_DELETED_AT` through
  schemas/transport and apply deletes before acknowledging state. Use explicit
  decimal metadata and exact string transport; a generic JSON `number` from an
  API tap remains approximate without that contract.
- MySQL binlog checkpoints must retain transaction/table-map boundaries and
  each stream's acknowledged history. Preserve complete GTID sets and replay
  fences; only new complete-history captures may set `gtid_complete: true`.
  Test interrupted large transactions on both MySQL and MariaDB.

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
- Keep target-snowflake's quote-aware CSV writer and named-format validation in
  lockstep. Preserve SQL NULL, empty strings, controls, Unicode, punctuation,
  and literal escapes; require the documented comma/LF, escape, enclosure,
  whitespace, empty-field, multiline, UTF-8, header, and empty `NULL_IF` options.
- Conversion stays in the PipelineWise command; do not restore a connector
  executable or duplicate its type, metadata, or recovery policy. Core
  conversion preflight predicts level-0 TEXT/VARIANT columns, their names, and
  which top-level objects a flattening tap splits; change the fixture's
  `singer_text_variant_classes`, `level_zero_column_names` and `top_level_splits`
  with any flattening, naming, or type-mapping change so both suites keep
  executing the same cases.
- Detect MariaDB JSON aliases only from the exact generated `JSON_VALID`
  constraint and explicit v3. Advertise object, array, string, number, Boolean,
  and null roots. Carry non-SQL-null values as validated serialized JSON so
  `PARSE_JSON` restores roots and keeps JSON null distinct from SQL NULL;
  preserve ordinary `LONGTEXT` and native mappings.
