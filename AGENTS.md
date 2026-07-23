# AI Coding Agent Instructions

## Purpose and routing

PipelineWise is a Python 3.12 Singer.io ELT framework for replicating taps to analytics warehouses. Write for senior engineers: emphasize operational precision, edge cases, and rationale.

Read every scoped file touched by the task:

- `pipelinewise/AGENTS.md`: orchestration, FastSync, backend migrations, data-diff.
- `singer-connectors/AGENTS.md`: vendored tap and target source.
- `tests/AGENTS.md`: unit tests and suite boundaries.
- `tests/end_to_end/AGENTS.md`: databases, connectors, E2E, `dev-project/`.
- `docs/AGENTS.md`: documentation.

Scoped files apply even when they were not auto-loaded from the current directory; multi-area changes follow all relevant files. More specific guidance adds to this file. Explicit user instructions win.

## Architecture

- **Framework:** Singer JSON messages over stdout/stdin; YAML definitions and generated JSON config, state, and catalogs.
- **CLI:** argparse in `pipelinewise/cli/__init__.py`, dispatching to `PipelineWise`. Deprecated `sync_tables` maps to `fast_sync`; canonical `import_config` and deprecated `import` map to `import_project`.
- **Singer path:** `tap | transform | target` for ongoing INCREMENTAL and LOG_BASED replication.
- **FastSync:** native full or filtered bulk transfer, not a replication method. See `pipelinewise/AGENTS.md` and `docs/concept/fastsync.rst`.

## Environment

- `make pipelinewise` installs PipelineWise, backend-db, and data-diff into canonical `.virtualenvs/pipelinewise/`; activate it before host validation.
- A local `.venv/` is non-canonical and may be stale. Connectors use separate environments under `.virtualenvs/`.
- Run host commands below from the repository root.

## Validation

### Python gates

Run the four CI lint commands verbatim and in order for implementation changes:

```bash
. .virtualenvs/pipelinewise/bin/activate
ruff check pipelinewise tests
pylint pipelinewise tests
flake8 pipelinewise --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 pipelinewise --count --max-complexity=15 --max-line-length=120 --statistics
```

Ruff and Pylint inspect `pipelinewise tests`; Flake8 inspects `pipelinewise`. Do not substitute bare Flake8, widen paths, add flags, or format in place of a gate.

Run the full unit gate from the root:

```bash
pytest --cov=pipelinewise --cov-fail-under=77 -v tests/units
```

- Nested paths below `tests/units/data_diff/` or `tests/units/backend_db/` can break imports; narrow with `-k`, for example `pytest tests/units -k "data_diff"`.
- Never run bare `pytest tests/`; it collects credentialed E2E tests. CI's coverage threshold is 77, not `.coveragerc`'s lower value.
- See `tests/AGENTS.md` for suite boundaries and proof requirements.

### Configuration

The dev config references environment variables. Source the existing file before validating:

```bash
set -a
. dev-project/.env
set +a
.virtualenvs/pipelinewise/bin/pipelinewise validate --dir dev-project/pipelinewise-config
```

CI creates `.env` from `.env.template`; locally, never overwrite an existing `.env` because it may contain real credentials. Validate after changes to implementation, schemas, example config, or connector config.

### Scoped checks

- Database, connector, migration, FastSync, data-diff, or E2E work: follow `tests/end_to_end/AGENTS.md` and report each relevant group.
- Connector source: follow `singer-connectors/AGENTS.md`; repository lint and unit gates do not inspect it.
- Docs: follow `docs/AGENTS.md`; warnings fail the build.
- Always run `git diff --check`.

## Connector CI boundary

`singer-connectors/` is tracked vendored source, not submodules. CI checks that all connectors install via `make pipelinewise_no_test_extras all_connectors`; it does not run connector tests. Behavioral changes need connector-local tests and an E2E route when one exists. Coordinate non-trivial divergence with upstream. Details are in `singer-connectors/AGENTS.md`.

## Module boundaries

- `pipelinewise.backend_db` must not import data-diff or replication orchestration.
- `pipelinewise.data_diff` may import backend-db, but not Singer or FastSync execution.
- Data-diff reads generated connector JSON only through its runtime loader.
- `import_config` persists and versions data-diff definitions only after connector generation and discovery succeed.
- Boundary tests enforce these directions.

## Style and safety

- Python: 120 characters, complexity 15, four spaces, Google docstrings, single quotes where consistent; `snake_case` functions/variables/JSON keys and `PascalCase` classes.
- FastSync uppercases Snowflake identifiers. Scope Pylint disables to a line or function, never a module.
- Comments explain non-obvious constraints and consequences in at most two lines; do not restate code, narrate edits, argue choices, or write walkthroughs.
- Never run `pre-commit run --all-files`; it mutates files broadly and is not a CI gate.
- Do not reformat or lint-fix unrelated files. Preserve user changes in dirty worktrees.
- Never commit secrets, `.tfvars`, private keys, or populated environment files.
- Use `import_config` in docs, examples, tests, and comments; `import` is only a deprecated alias.

## Git and completion

- Branch from `master`; use `AP-NNNN-short-description` and `[AP-NNNN]` commit subjects when a ticket exists.
- Keep diffs task-scoped; incidental cleanup hinders review and rollback.

Before completion:

1. Implementation changes pass all four lint gates and the full unit gate.
2. Configuration validation passes when applicable.
3. Relevant E2E groups run per `tests/end_to_end/AGENTS.md`; report pass/skip/fail counts and never call a skipped suite complete.
4. User-facing behavior/config changes update and validate docs.
5. `git diff --check` passes and `git status` contains only expected files.
6. Report failed, skipped, or unavailable checks with output or blocker; never call partial verification complete.
