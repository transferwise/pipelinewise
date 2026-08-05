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

Each `CLAUDE.md` is a relative symlink to the adjacent `AGENTS.md`. Edit the `AGENTS.md` and preserve the symlink.

## Architecture

- **Framework:** Singer JSON messages over stdout/stdin; YAML definitions and generated JSON config, state, and catalogs.
- **CLI:** argparse in `pipelinewise/cli/__init__.py`, dispatching to `PipelineWise`. Deprecated `sync_tables` maps to `fast_sync`; canonical `import_config` and deprecated `import` map to `import_project`.
- **Singer path:** `tap | [transform-field] | [mbuffer] | target`; handles INCREMENTAL/LOG_BASED and FULL_TABLE streams not selected for FastSync.
- **FastSync:** native full or filtered bulk transfer, not a replication method. See `pipelinewise/AGENTS.md` and `docs/concept/fastsync.rst`.

## Environment

- `make pipelinewise` creates `.virtualenvs/pipelinewise/` and installs the editable root package with test extras; activate it before host validation.
- Do not use a repository `.venv/`. Root connector installs use `.virtualenvs/<name>/`; connector-local Makefiles may create `venv/`. Never mix interpreters.
- Run host commands below from the repository root.

## Validation

### Python gates

Run the two CI style gates verbatim and in order for implementation changes:

```bash
. .virtualenvs/pipelinewise/bin/activate
ruff format --check .
ruff check .
```

Ruff formats and lints PipelineWise plus `tap-mysql`, `tap-postgres`, and `target-snowflake` with 120-character lines and complexity 15. Other connectors remain excluded and use their local tooling. Apply formatting with `ruff format <changed paths>`; do not use formatting as a substitute for either check gate.

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
- Connector source: follow `singer-connectors/AGENTS.md`. Root Ruff gates inspect `tap-mysql`, `tap-postgres`, and `target-snowflake`; connector CI also runs their local Ruff and unit coverage gates on Python 3.12. Root unit tests do not inspect connector code.
- Docs: follow `docs/AGENTS.md`; warnings fail the build.
- Always run `git diff --check`.

## Module boundaries

- `pipelinewise.backend_db` must not import data-diff or replication orchestration.
- `pipelinewise.data_diff` may import backend-db, but not Singer or FastSync execution.
- Data-diff reads generated connector JSON only through its runtime loader.
- `import_config` persists and versions data-diff definitions only after connector generation and discovery succeed.
- An AST test enforces the backend-db to data-diff prohibition; add equivalent coverage when changing the other boundaries.

## Style and safety

- Python: Ruff's default formatter with 120-character lines, complexity 15, four spaces, Google docstrings, `snake_case` functions/variables/JSON keys, and `PascalCase` classes.
- FastSync uppercases Snowflake identifiers. Scope Ruff `noqa` directives to the narrowest line or function; prefer symbolic rule codes with a short reason for non-obvious exceptions.
- Comments explain non-obvious constraints and consequences in at most two lines; do not restate code, narrate edits, argue choices, or write walkthroughs.
- Never run `pre-commit run --all-files`; it mutates files broadly and is not a CI gate.
- Do not reformat or lint-fix unrelated files. Preserve user changes in dirty worktrees.
- Never commit secrets, `.tfvars`, private keys, or populated environment files.
- When adding a third-party import, declare it in the owning package's `setup.py`; do not rely on transitive installation.
- Use `import_config` in docs, examples, tests, and comments; `import` is only a deprecated alias.

## Git and completion

- Branch from `master`; use `AP-NNNN-short-description` and `[AP-NNNN]` commit subjects when a ticket exists.
- Keep diffs task-scoped; incidental cleanup hinders review and rollback.

Before completion:

1. Implementation changes pass both Ruff gates and the full unit gate.
2. Configuration validation passes when applicable.
3. Relevant E2E groups run per `tests/end_to_end/AGENTS.md`; report pass/skip/fail counts and never call a skipped suite complete.
4. User-facing behavior/config changes update and validate docs.
5. Release-visible connector changes update the root changelog per `singer-connectors/AGENTS.md`.
6. `git diff --check` passes and `git status` contains only expected files.
7. Report failed, skipped, or unavailable checks with output or blocker; never call partial verification complete.
