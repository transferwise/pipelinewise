# Singer Connector Instructions

Read root `AGENTS.md` first. This file covers `singer-connectors/`; use scoped E2E, implementation, test, and docs guidance when those areas are involved.

## CI and environments

This is tracked vendored source, not submodules. Root Ruff gates inspect `tap-mysql`, `tap-postgres`, and `target-snowflake`; root unit tests do not inspect connector code. Connector CI installs every connector, then runs `make venv`, `make lint`, and `make unit_test_cov` for tap-mysql (47% minimum), tap-postgres (58%), and target-snowflake (67%) on Python 3.12. Behavioral changes still need connector-local tests plus an E2E route when one exists; otherwise report E2E unavailable.

Root `make connectors -e pw_connector=<name>` creates runtime `.virtualenvs/<name>/`, consuming `pre_requirements.txt`, `requirements.txt`, then `setup.py` when present; it does not create the connector's test environment. Connector-local Makefiles usually create `./venv/` with test extras. Never mix PipelineWise, runtime-connector, local-test, or another connector's interpreter.

## Local validation

Read the connector's files and Makefile before choosing targets. For `tap-mysql`, `tap-postgres`, and `target-snowflake`, run the local environment/install, Ruff lint, unit, and applicable integration targets from its directory. Other connectors retain their local lint tools. Do not change a threshold to obtain a pass. Integration may need local containers, external credentials, or both—inspect its Makefile, Compose files, and environment first. Connector CI does not run integration suites.

- The Ruff-managed connectors use `venv`, `lint`, `format`, `unit_test`, and `integration_test`. Other common targets include `pylint`, but Kafka uses `virtual_env` and containers; MongoDB uses `setup`, `test`, `test_cov`; Twilio uses `test`; S3 CSV uses plural `unit_tests`/`integration_tests`.
- PostgreSQL additionally gates `integration_test_cov` at 63 and `total_cov` at 85. MySQL uses Pytest for both unit and integration tests. Mixpanel, Slack, Twilio, and tap-snowflake test targets have no fail threshold. Other declared thresholds range from 30 to 87; the Makefile is authoritative.
- Mixpanel, Slack, and Salesforce have no integration target.
- GitHub, Jira, Zendesk, and transform-field have no Makefile. GitHub has `tests/`; Zendesk has singular `test/` and uses Nose; transform-field has `[test]` extras plus direct `tests/unit/` and `tests/integration/` suites and also receives Singer E2E coverage. Jira is only the external pin in `tap-jira/requirements.txt`, with no local source/tests. Salesforce has source and Makefile but no tests. None of GitHub, Jira, or Zendesk has a repository E2E route.

## Versioning and upstream

Connector source in this directory ships with PipelineWise; standalone connector packages are released only when explicitly planned. Without such a plan, do not bump `setup.py` versions or add versioned connector changelog entries. Record release-visible runtime and dependency changes under the current PipelineWise release in the root `CHANGELOG.md`, without an old/new connector version. Jira remains an external version pin in `tap-jira/requirements.txt`. Test-only, CI, or fixture changes need a root changelog entry only when release-relevant.

These are upstream-derived copies. Coordinate non-trivial divergence upstream; keep local fixes narrow and comments limited to why divergence is necessary. Do not run broad formatting or `pre-commit run --all-files`.

## Snowflake traps

- Identifiers are uppercased and double-quoted; `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`, so DDL/DML case must agree.
- Type-change detection compares generated types with Snowflake's canonical reported types. An alias can round-trip differently, falsely triggering live-column rename/replacement.
- The connector prints its version to stderr; E2E must check process exit status because `assert_command_success` treats stderr as failure.
- Iceberg is decided per table at runtime: existing type from `SHOW TERSE ICEBERG TABLES` wins; `iceberg_create` applies only to new tables. FastSync/PartialSync do not make this decision.
