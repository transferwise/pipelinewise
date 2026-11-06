# Test Suite Instructions

Read root `AGENTS.md`. Database/dev-project work follows
`tests/end_to_end/AGENTS.md`; connector tests follow `singer-connectors/AGENTS.md`.

## Boundaries

- `tests/units` is the credential/container-free root gate. Use the root command
  and its fixed 77% floor regardless of `.coveragerc`, in the ready container or
  a declared host fallback. Collect nested data-diff/backend-db units from
  `tests/units` with `-k`; direct nested paths can break imports.
- Never run bare `pytest tests/`: it collects credentialed E2E. Connector suites
  are separate. Local E2E requires an explicit user request; reviews and shared
  sessions follow the root validation/isolation rules.

## Test design and evidence

- Test observable behaviour and failure risks, not private implementation shape
  or coverage percentages. Cover unsupported-route guards as well as successful
  paths; prefer behaviour assertions unless exact SQL is the contract. Add a
  regression for the actual trigger, including boundary types and state.
- For recovery changes, exercise persisted state, target acknowledgement,
  complete transaction boundaries and interruption/restart. For concurrency,
  notification or rollback guarantees, use real sessions/engine tests where
  needed; in-memory mocks alone do not prove durability or locking.
- Generated SQL proves text, not engine acceptance. Distinguish unit/fake,
  integration and E2E evidence; identify missing real-engine verification.
- Add import/AST tests for dependency seams; only backend-db → data-diff is
  currently enforced directly.
- Ensure new tests are collected by their intended CI command/matrix and not
  conditionally skipped. A file existing or a green aggregate is insufficient;
  inspect named steps while following the CI status/log policy.
- Keep coverage gates and purposeful exclusions. Do not lower thresholds, mix
  coverage files or add superficial tests to make a percentage pass. Report
  actual pass/skip/fail and intentional deselection counts per group; unavailable
  checks and partial matrices remain incomplete verification.
