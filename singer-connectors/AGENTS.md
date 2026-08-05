# Singer Connector Instructions

Read root `AGENTS.md` first. This file covers `singer-connectors/`; use scoped E2E, implementation, test, and docs guidance when those areas are involved.

## CI and environments

This is tracked vendored source, not submodules. Connector CI only installs everything through `make pipelinewise_no_test_extras all_connectors`; repository lint/unit gates do not inspect it. A behavioral change therefore needs connector-local tests plus an E2E route when one exists. If no route exists, run the available local checks and report E2E as unavailable.

`make connectors -e pw_connector=<name>` creates isolated `.virtualenvs/<name>/`, consuming `pre_requirements.txt`, `requirements.txt`, then `setup.py` when present. Never test with the PipelineWise, root, or another connector interpreter. Declare direct top-level dependencies instead of relying on transitive installation.

## Local validation

Read the connector's files and Makefile before choosing targets. Where present, run its environment/install, Pylint, unit, and credentialed integration targets from its directory; do not change a coverage threshold to obtain a pass.

- Common targets are `venv`, `pylint`, `unit_test`, and `integration_test`, but Kafka uses `virtual_env` and containers; MongoDB uses `setup`, `test`, `test_cov`; Twilio uses `test`; S3 CSV uses plural `unit_tests`/`integration_tests`.
- PostgreSQL gates coverage in separate `unit_test_cov` (58), `integration_test_cov` (63), and `total_cov` (85) targets. MySQL uses Nose with 47. Mixpanel, Slack, Twilio, and tap-snowflake test targets have no fail threshold. Other declared thresholds range from 30 to 87; the Makefile is authoritative.
- Mixpanel, Slack, and Salesforce have no integration target. Integration tests require live credentials and install CI does not run them.
- GitHub, Jira, Zendesk, and transform-field have no Makefile. GitHub has `tests/`; Zendesk has singular `test/` and uses Nose; transform-field is validated by installation and indirectly by Singer E2E routes. Jira is only the external pin in `tap-jira/requirements.txt`, with no local source/tests. Salesforce has source and Makefile but no tests. None of GitHub, Jira, or Zendesk has a repository E2E route.
- Local Pylint configs exist only at `tap-mongodb/pylintrc`, `tap-mysql/.pylintrc`, `tap-postgres/.pylintrc`, `tap-s3-csv/.pylintrc`, `tap-snowflake/pylintrc`, `target-snowflake/pylintrc`, and `transform-field/.pylintrc`.

## Versioning and upstream

Connector versions normally live in their own `setup.py`; Jira is the external version pin in `tap-jira/requirements.txt`. A behavioral change requires the applicable version bump, a connector `CHANGELOG.md` entry when that file exists, and a root `CHANGELOG.md` entry naming old/new package versions with the change nested below. Jira and Salesforce have no connector changelog, so use the root changelog only.

Example:

```text
- `pipelinewise-target-snowflake` from `2.5.1` to `2.5.2`
    - Support creating new Iceberg tables for pure Singer replications
```

These are upstream-derived copies. Coordinate non-trivial divergence upstream; keep local fixes narrow and comments limited to why divergence is necessary. Do not run broad formatting or `pre-commit run --all-files`.

## Snowflake traps

- Identifiers are uppercased and double-quoted; `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`, so DDL/DML case must agree.
- Type-change detection compares generated types with Snowflake's canonical reported types. An alias can round-trip differently, falsely triggering live-column rename/replacement.
- The connector prints its version to stderr; E2E must check process exit status because `assert_command_success` treats stderr as failure.
- Iceberg is decided per table at runtime: existing type from `SHOW TERSE ICEBERG TABLES` wins; `iceberg_create` applies only to new tables. FastSync/PartialSync do not make this decision.
