# Singer Connector Instructions

Read root `AGENTS.md` first and scoped implementation, test, E2E, and docs guidance where relevant.

## Environments and CI

This is vendored source, not submodules; root lint/unit gates do not inspect it. Prefer the ready `pipelinewise` container when it can install the connector's dependencies; report any host fallback.

Connector CI installs every connector and separately runs Python 3.12 unit gates for tap-mysql (`make unit_test_cov`, 47%), tap-postgres (`make unit_test_cov`, 58%), and target-snowflake (`make unit_test`, 67%). It does not run integration suites; behavioral changes need local connector tests and an available E2E route.

Root `make connectors -e pw_connector=<name>` creates runtime `.virtualenvs/<name>/`; connector Makefiles often create `./venv/` for tests. Never mix PipelineWise, runtime-connector, connector-test, host, or container interpreters.

## Validation

The owning Makefile is authoritative. Run its environment, Pylint, unit, integration, and coverage targets where present; never lower thresholds. Integration may require containers or external credentials.

- Most use `venv`, `pylint`, `unit_test`, and `integration_test`; inspect the Makefile for different target names.
- PostgreSQL also requires `integration_test_cov` >=63 and `total_cov` >=85; MySQL uses Pytest for unit and integration tests.
- No Makefile: GitHub (`tests/`), Zendesk (Nose), and transform-field (direct suites and Singer E2E). Jira is an external pin without local source/tests; Salesforce has a Makefile but no tests. GitHub, Jira, and Zendesk lack repository E2E.

Report unavailable or skipped integration/E2E coverage as unverified.

## Versioning and upstream

- Connector source ships with PipelineWise. Unless a standalone release is explicit, do not bump connector versions or add versioned connector changelog entries. Record release-visible changes under the current root release using its atomic-bullet rule; include test/CI/fixture changes only when release-relevant. Jira remains an external pin.
- These are upstream-derived copies. Coordinate non-trivial divergence upstream; keep local fixes narrow and comments limited to why divergence is needed. Avoid broad formatting.

## Snowflake traps

- Uppercase and double-quote identifiers; with `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`, DDL/DML case must match.
- Compare generated types with Snowflake's canonical reported types; aliases can round-trip differently and trigger false column replacement.
- Connector version prints to stderr; E2E must check exit status because `assert_command_success` treats stderr as failure.
- With legacy `iceberg_create`, the existing physical format wins and the flag applies only to missing tables. Explicit tap-level formats must match. Keep `target_table_format: iceberg` and `iceberg_version: 3` for the tap's lifetime; removing both restores legacy TEXT mapping and stops on existing VARIANT columns. FastSync/PartialSync reject explicit Iceberg until supported.
