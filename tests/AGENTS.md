# Test Suite Instructions

Read root `AGENTS.md` first. This covers `tests/`; also read the E2E file for database-backed, connector-route, or `dev-project/` work and the connector file for connector-local tests.

## Unit gate and boundaries

From the repository root:

```bash
. .virtualenvs/pipelinewise/bin/activate
pytest --cov=pipelinewise --cov-fail-under=77 -v tests/units
```

- Target `tests/units`, the only credential/container-free suite and the only one behind the CI coverage gate. Nested selection below `tests/units/data_diff/` or `backend_db/` can break imports; use root plus `-k`. Never run bare `pytest tests/`, which also collects credentialed E2E.
- CI overrides `.coveragerc` with 77. Ruff/Pylint also inspect tests.
- E2E needs the dev container and, for Snowflake/S3, live credentials; run it inside the container.
- `dev-project/entrypoint.sh` executes all `tests/db/*.sh` seed scripts on every PipelineWise-container start. Only Docker image `initdb.d` scripts depend on an empty volume.
- Connector tests are separate and uncollected here: usually `tests/`, Zendesk singular `test/`; Jira and Salesforce have none. Follow `singer-connectors/AGENTS.md`.

## Proof and reporting

- Generated-SQL assertions prove the string, not engine acceptance; state when real-engine execution is unverified.
- Mirror implementation paths (`pipelinewise/data_diff/coverage.py` -> `tests/units/data_diff/test_coverage.py`).
- Cover routing guards for documented unsupported cases, not only supported shapes. Prefer behavior assertions, except where exact emitted SQL is the behavior.
- Module-boundary tests required by root guidance are mandatory.
- Report pass/skip/fail counts per command group. Credential skips are not passes, and a partly skipped matrix is not complete.
