# AI Coding Agent Instructions

## Scope

PipelineWise is a Python 3.12 Singer ELT framework. Write for senior engineers;
prioritize operational precision, edge cases, and rationale. Read each relevant
guide:

- `pipelinewise/AGENTS.md`: orchestration, FastSync, backend migrations, data-diff.
- `singer-connectors/AGENTS.md`: vendored taps and targets.
- `tests/AGENTS.md`: unit tests and suite boundaries.
- `tests/end_to_end/AGENTS.md`: databases, connectors, E2E, `dev-project/`.
- `docs/AGENTS.md`: documentation.

Scoped guidance adds to this file; explicit user instructions win. Each
`CLAUDE.md` symlinks to its adjacent `AGENTS.md`; edit the latter and preserve
the link.

## Architecture

- Singer JSON flows as `tap | [transform-field] | [mbuffer] | target`; YAML becomes generated config, state, and catalog JSON.
- `pipelinewise/cli/__init__.py` defines argparse and dispatches to `PipelineWise`. Canonical `fast_sync` and `import_config` retain deprecated aliases `sync_tables` and `import`.
- FastSync is a native full/filtered bulk-transfer optimization, not a replication method. Singer handles INCREMENTAL/LOG_BASED and FULL_TABLE streams not selected for FastSync.

## Environment

- Prefer the `dev-project` Docker stack; its Linux runtime best matches
  production. Use the host only to manage Docker or when a check cannot run in
  the container, and report the compatibility gap.
- The repo is mounted at `/opt/pipelinewise` in the `pipelinewise` container.
  `PIPELINEWISE_HOME` environments live under `dev-project/.virtualenvs/` and
  are on `PATH`.
- `make pipelinewise` installs the editable root plus test extras. Connector
  runtimes use `.virtualenvs/<name>/`; connector Makefiles may use `venv/`.
  Never mix host, container, root, runtime-connector, or connector-test
  interpreters, and never use repository `.venv/`.

## Validation

For implementation changes, run these root gates verbatim and in order,
preferably in the ready container:

```bash
ruff check pipelinewise tests
pylint pipelinewise tests
flake8 pipelinewise --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 pipelinewise --count --max-complexity=15 --max-line-length=120 --statistics
pytest --cov=pipelinewise --cov-fail-under=77 -v tests/units
```

For an unavoidable host run, activate `.virtualenvs/pipelinewise/`. Do not alter
paths or flags: Ruff/Pylint inspect `pipelinewise tests`; Flake8 inspects
`pipelinewise`. Never run bare `pytest tests/`; it collects credentialed E2E.
Collect nested data-diff/backend-db selections from `tests/units` and narrow
with `-k` to avoid import failures.

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

Run this after implementation, schema, example-config, or connector-config
changes.

Also follow scoped checks:

- Database, migration, FastSync, data-diff, connector-route, or E2E: `tests/end_to_end/AGENTS.md`.
- Connector source: `singer-connectors/AGENTS.md`; root gates do not inspect it.
- Docs: `docs/AGENTS.md`; warnings fail.

## Style and safety

- Python: 120 columns, complexity 15, four spaces, Google docstrings, consistent
  single quotes, `snake_case` names/JSON keys, and `PascalCase` classes.
- Uppercase Snowflake FastSync identifiers. Scope Pylint disables to a
  line/function, never a module.
- Comments explain a non-obvious constraint or consequence in at most two
  lines; do not restate code, narrate edits, argue choices, or add walkthroughs.
- Preserve dirty-worktree changes. Do not run `pre-commit run --all-files`,
  reformat unrelated files, or broaden lint fixes.
- Declare third-party imports in the owning `setup.py`; never rely on
  transitive installs.
- Never commit secrets, `.tfvars`, private keys, or populated environment
  files.
- Use `import_config` in docs, examples, tests, and comments; `import` is
  deprecated.

## Git and completion

- Branch from `master`; keep diffs task-scoped. Sign every commit with
  `git commit -S`; never create or push an unsigned commit.
- CHANGELOG bullets are atomic and outcome-focused: start with an action verb,
  name the component and operational result, and include implementation detail
  only to explain risk. Group related bullets under headings.
- Before creating a PR, compare the complete branch diff with the current
  release CHANGELOG entry. Do not create the PR while the CHANGELOG omits,
  misstates, or claims changes that are not present in the diff.
- PipelineWise stays in `0.x` and must never publish `1.0.0` or above. Use a
  patch for compatible fixes and the next `0.x` minor for features or intentional
  compatibility changes. Document material operator impact and keep `setup.py`
  aligned with the top CHANGELOG release.

Before completion:

1. Run all applicable lint, unit, config, scoped E2E, connector, and docs checks.
2. Update validated docs for user-facing behavior/config changes and the root changelog for release-visible connector changes.
3. Report pass/skip/fail counts per group; skips, failures, and unavailable checks are not passing verification.
4. Ensure `git diff --check` passes and `git status` contains only expected files.
