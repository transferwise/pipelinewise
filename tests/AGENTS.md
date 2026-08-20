# Test Suite Instructions

Read root `AGENTS.md`; database/dev-project work also uses E2E guidance, and
connector-local tests use connector guidance.

## Boundaries

- Prefer root units in the ready `pipelinewise` container; report host fallback.
  `tests/units` is the credential/container-free root CI gate; use the exact
  root command and its fixed 77% threshold, regardless of `.coveragerc`.
- Collect nested data-diff/backend-db selections from `tests/units` and narrow
  with `-k`; direct nested paths can break imports.
- Never run bare `pytest tests/`; it collects credentialed E2E.
- Connector tests are separate; database-backed and route tests follow their scoped files.

## Proof

- Generated SQL assertions prove text, not database acceptance; identify missing
  real-engine proof.
- Mirror implementation paths and cover unsupported-route guards, not only happy
  paths. Prefer behavior assertions unless exact SQL is the behavior.
- Add import/AST coverage when changing dependency seams; currently only backend-db -> data-diff is directly enforced.
- Report pass/skip/fail counts per command group. Skips and partial matrices are not complete.
