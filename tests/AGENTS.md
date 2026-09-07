# Test Suite Instructions

Read root `AGENTS.md`; database/dev-project work also follows E2E guidance and
connector-local tests follow connector guidance.

## Boundaries

- Prefer root units in the ready `pipelinewise` container and report host
  fallback. `tests/units` is the credential/container-free root CI gate; use
  the exact root command and fixed 77% threshold regardless of `.coveragerc`.
- Collect nested data-diff/backend-db selections from `tests/units` and narrow
  with `-k`; direct nested paths can break imports. Never run bare
  `pytest tests/`, which collects credentialed E2E.
- Connector tests are separate; database-backed and route tests follow their
  scoped guides.

## Proof

- Generated-SQL assertions prove text, not database acceptance; identify absent
  real-engine proof.
- Mirror implementation paths and unsupported-route guards, not only happy
  paths. Prefer behavior assertions unless exact SQL is the behavior.
- Add import/AST coverage for dependency-seam changes; only backend-db →
  data-diff is currently enforced directly.
- Report pass/skip/fail counts per command group; skips and partial matrices are
  incomplete verification.
