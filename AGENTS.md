# AI Coding Agent Instructions

## Scope

PipelineWise is a Python 3.12 Singer ELT framework. Write for senior engineers; prioritize operational precision, edge cases, and rationale.

Read every scoped file relevant to the task:

- `pipelinewise/AGENTS.md`: orchestration, FastSync, backend migrations, data-diff.
- `singer-connectors/AGENTS.md`: vendored taps and targets.
- `tests/AGENTS.md`: unit tests and suite boundaries.
- `tests/end_to_end/AGENTS.md`: databases, connectors, E2E, `dev-project/`.
- `docs/AGENTS.md`: documentation.

More-specific guidance adds to this file; explicit user instructions win. Each `CLAUDE.md` symlinks to its adjacent `AGENTS.md`; edit the latter and preserve the link.

## Architecture

- Singer JSON flows as `tap | [transform-field] | [mbuffer] | target`; YAML becomes generated config, state, and catalog JSON.
- `pipelinewise/cli/__init__.py` defines argparse and dispatches to `PipelineWise`. Canonical `fast_sync` and `import_config` retain deprecated aliases `sync_tables` and `import`.
- FastSync is a native full/filtered bulk-transfer optimization, not a replication method. Singer handles INCREMENTAL/LOG_BASED and FULL_TABLE streams not selected for FastSync.

## Environment

- Prefer the `dev-project` Docker stack for supported development and verification. Its Linux runtime best approximates production; use the host only to manage Docker or when the container cannot run a check. Report host-only verification and its compatibility gap.
- The repo is mounted at `/opt/pipelinewise`; run commands there in the `pipelinewise` container. `PIPELINEWISE_HOME` keeps its environments under `dev-project/.virtualenvs/`, which is on `PATH`.
- `make pipelinewise` installs the editable root package with test extras. Connector runtime environments use `.virtualenvs/<name>/`; connector-local Makefiles may use `venv/`. Never mix interpreters or reuse environments across host and container. Never use repository `.venv/`.

## Validation

For implementation changes, run the four lint gates verbatim and in order, then the unit gate, from the repo root—preferably in the ready container:

```bash
ruff check pipelinewise tests
pylint pipelinewise tests
flake8 pipelinewise --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 pipelinewise --count --max-complexity=15 --max-line-length=120 --statistics
pytest --cov=pipelinewise --cov-fail-under=77 -v tests/units
```

On an unavoidable host run, first activate `.virtualenvs/pipelinewise/`. Do not alter paths or flags: Ruff/Pylint inspect `pipelinewise tests`; Flake8 inspects `pipelinewise`. Never run bare `pytest tests/` because it collects credentialed E2E. For nested data-diff/backend-db tests, collect from `tests/units` and narrow with `-k` to avoid import failures.

For config-affecting changes, validate in Docker, where Compose loads `dev-project/.env`:

```bash
pipelinewise validate --dir dev-project/pipelinewise-config
```

For a host fallback, source the existing `.env` without overwriting it:

```bash
set -a
. dev-project/.env
set +a
.virtualenvs/pipelinewise/bin/pipelinewise validate --dir dev-project/pipelinewise-config
```

Validate after implementation, schema, example-config, or connector-config changes.

Also follow scoped checks:

- Database, migration, FastSync, data-diff, connector-route, or E2E: `tests/end_to_end/AGENTS.md`.
- Connector source: `singer-connectors/AGENTS.md`; root gates do not inspect it.
- Docs: `docs/AGENTS.md`; warnings fail.

## Style and safety

- Python: 120 columns, complexity 15, four spaces, Google docstrings, consistent single quotes, `snake_case` names/JSON keys, `PascalCase` classes.
- Snowflake FastSync identifiers are uppercase. Scope Pylint disables to a line/function, never a module.
- Comments should explain a non-obvious constraint or consequence in at most two lines; do not restate code, narrate edits, argue choices, or add walkthroughs.
- Do not run `pre-commit run --all-files`, reformat unrelated files, or broaden lint fixes. Preserve dirty-worktree changes.
- Declare third-party imports in the owning `setup.py`; never rely on transitive installs.
- Never commit secrets, `.tfvars`, private keys, or populated environment files.
- Use `import_config` in docs, examples, tests, and comments; `import` is deprecated.

## Git and completion

Branch from `master` and keep diffs task-scoped.
Create every commit with a cryptographic signature using `git commit -S`; never
create or push an unsigned commit.

Keep CHANGELOG bullets concise, outcome-focused, and atomic: one independently
reviewable behavior per bullet. Start with an action verb, name the affected
component, and state the operational result. Include implementation details only
when needed to explain behavior or risk. Use headings to group related changes.
Use semantic versioning for root releases: patch for fixes only, minor for backward-compatible
features, and major for breaking changes. Keep `setup.py` aligned with the top CHANGELOG release.

Before completion:

1. Run all applicable lint, unit, config, scoped E2E, connector, and docs checks.
2. Update validated docs for user-facing behavior/config changes and the root changelog for release-visible connector changes.
3. Report pass/skip/fail counts per group; skips, failures, and unavailable checks are not passing verification.
4. Ensure `git diff --check` passes and `git status` contains only expected files.
