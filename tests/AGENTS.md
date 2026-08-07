# Test Suite Instructions

Read root `AGENTS.md` first; also read E2E guidance for database/connector/dev-project work and connector guidance for connector-local tests.

## Boundaries

- Run root unit tests in the ready `pipelinewise` container when possible; report host fallback. `tests/units` is the credential/container-free root CI coverage gate; use the exact root command.
- Collect nested data-diff/backend-db selections from `tests/units` and narrow with `-k`; direct nested paths can break imports.
- Never run bare `pytest tests/`: it collects credentialed E2E. CI coverage is 77, regardless of `.coveragerc`.
- Connector tests are separate; database-backed and route tests follow their scoped files.

## Proof

- Generated SQL assertions prove text, not database acceptance; identify missing real-engine verification.
- Mirror implementation paths in unit tests and cover unsupported routing guards, not only happy paths. Prefer behavior assertions unless exact SQL is the behavior.
- Add import/AST coverage when changing dependency seams; currently only backend-db -> data-diff is directly enforced.
- Report pass/skip/fail counts per command group. Skips and partial matrices are not complete.
