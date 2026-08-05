# Test Suite Instructions

Read root `AGENTS.md` first. This covers `tests/`; also read the E2E file for database-backed, connector-route, or `dev-project/` work and the connector file for connector-local tests.

## Unit gate and boundaries

- `tests/units` is the credential/container-free root PipelineWise CI coverage gate; run the exact command in root `AGENTS.md`. Connector CI has separate tap-mysql and tap-postgres gates.
- Selecting paths below `tests/units/data_diff/` or `tests/units/backend_db/` can break imports; collect from `tests/units` and narrow with `-k`.
- Never run bare `pytest tests/`; it also collects credentialed E2E. CI overrides `.coveragerc` with 77, and Ruff inspects root tests.
- Connector tests are separate and uncollected here; follow `singer-connectors/AGENTS.md`. Database-backed and connector-route tests follow `tests/end_to_end/AGENTS.md`.

## Proof and reporting

- Generated-SQL assertions prove the string, not engine acceptance; state when real-engine execution is unverified.
- Mirror implementation paths (`pipelinewise/data_diff/coverage.py` -> `tests/units/data_diff/test_coverage.py`).
- Cover routing guards for documented unsupported cases, not only supported shapes. Prefer behavior assertions, except where exact emitted SQL is the behavior.
- When changing dependency seams, add or adjust import/AST boundary coverage; only the backend-db to data-diff prohibition is currently enforced directly.
- Report pass/skip/fail counts per command group. Credential skips are not passes, and a partly skipped matrix is not complete.
