# AI Coding Agent Instructions

## Scope

PipelineWise is a Python 3.12 Singer ELT framework. Write for senior engineers
with operational precision, edge cases, and rationale. Root rules apply
repo-wide; scoped rules add to them, and explicit user instructions win. Read
the guides relevant to the change:

- `pipelinewise/AGENTS.md`: orchestration, FastSync, backend migrations, data-diff.
- `singer-connectors/AGENTS.md`: vendored taps and targets.
- `tests/AGENTS.md`: units and suite boundaries.
- `tests/end_to_end/AGENTS.md`: databases, connector routes, E2E, `dev-project/`.
- `docs/AGENTS.md`: documentation.

Every `CLAUDE.md` symlinks to its adjacent `AGENTS.md`; edit the latter and
preserve the link.

## Architecture

- Singer JSON flows as `tap | [transform-field] | [mbuffer] | target`; YAML
  generates config, state, and catalog JSON.
- `pipelinewise/cli/__init__.py` defines argparse and dispatches to
  `PipelineWise`. Canonical `fast_sync` and `import_config` retain deprecated
  aliases `sync_tables` and `import`.
- FastSync optimizes native full/filtered bulk transfer; it is not a replication
  method. Singer handles INCREMENTAL/LOG_BASED and non-FastSync FULL_TABLE streams.
- Treat UTC as the canonical convention for replication and data-diff timestamps
  and windows. Interpret source timezones correctly before normalizing; never
  rely on the host or container timezone for correctness.

## Environment

- Default to the `dev-project` Docker stack, whose Linux runtime best matches
  production. Use the host only to manage Docker or when the container cannot
  run a check; report that fallback and compatibility gap.
- The repo mounts at `/opt/pipelinewise` in `pipelinewise`.
  `PIPELINEWISE_HOME` environments are on `PATH` under
  `dev-project/.virtualenvs/`.
- `make pipelinewise` installs the editable root and test extras. Connector
  runtimes use `.virtualenvs/<name>/`; connector Makefiles may use `venv/`.
  Never mix host/container, root, runtime-connector, or connector-test
  interpreters; never use repository `.venv/`.

## Validation

For implementation changes, run these root gates verbatim, in order, preferably
in the ready container:

```bash
ruff check .
pytest --cov=pipelinewise --cov-fail-under=77 -v tests/units
```

An unavoidable host run must activate `.virtualenvs/pipelinewise/`. Keep paths
and flags exact: Ruff checks all repository Python except connector tests outside
existing GitHub connector CI, including integration suites, legacy tests, and
spikes. This includes data-diff, root E2E, vendored connector source, and the
tap-mysql, tap-postgres, tap-yugabyte, and target-snowflake unit suites run by
connector CI. Never run bare `pytest tests/` because it collects credentialed
E2E. Collect
nested data-diff/backend-db tests from `tests/units`, narrowing with `-k` to
avoid import failures.

After implementation, schema, example-config, or connector-config changes,
validate in Docker (Compose loads `dev-project/.env`):

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

Also follow these scoped checks:

- Database, migration, FastSync, data-diff, connector-route, or E2E:
  `tests/end_to_end/AGENTS.md`.
- Connector source: `singer-connectors/AGENTS.md`; the root Ruff gate checks
  source and GitHub-tested suites, while connector-local targets provide install
  and test coverage.
- Docs: `docs/AGENTS.md`; warnings fail.

## Style and safety

- Python: 120 columns, complexity 15, four spaces, Google docstrings,
  consistent single quotes, `snake_case` names/JSON keys, `PascalCase` classes.
- Ruff retains error, warning, quote, line-length, and complexity gates plus the
  `PLE` family. Preview rules require explicit selection; `E301`–`E306` enforce
  blank-line spacing. The Ruff-only migration intentionally did not enable `PLC`,
  `PLR`, or `PLW` wholesale over legacy code; broaden rules only with a
  scoped cleanup and regression plan.
- Every new or modified Python file must remain in scope for `ruff check .` and
  pass the repository `pyproject.toml` policy. Do not add or invoke another
  Python linter or formatter.
- Uppercase Snowflake FastSync identifiers. Scope new Ruff ignores to the
  narrowest line or function and rule. Do not add connector-wide or
  directory-wide rule exemptions or broaden unrelated inline suppressions.
- Comments explain a non-obvious constraint or consequence in at most two
  lines; do not restate code, narrate edits, argue choices, or add walkthroughs.
- Preserve dirty-worktree changes. Never run `pre-commit run --all-files`,
  reformat unrelated files, or broaden lint fixes.
- Declare third-party imports in the owning `setup.py`; do not rely on
  transitive installs.
- Never commit secrets, `.tfvars`, private keys, or populated environment files.
- Use `import_config` in docs, examples, tests, and comments; `import` is deprecated.

## Reviews

When reviewing changes between the current state and `master`, present concerns
as a numbered list, ordered from most to least severe. Label each concern's
severity; if there are no concerns, say so explicitly.

## Git and completion

- Branch from `master`; keep diffs task-scoped. Sign every commit with
  `git commit -S`; never create or push an unsigned commit.
- CHANGELOG bullets are atomic and outcome-focused: start with an action verb,
  name the component and operational result, group related bullets, and include
  implementation detail only to explain risk.
- Before creating a PR or pushing to an open PR, compare the complete branch
  diff with the current CHANGELOG entry; do not proceed if the entry omits or
  misstates the diff or claims absent changes. Set that release to today's date
  (never `TBD` or an earlier date) and link its CHANGELOG entry directly in the
  PR body.
- PipelineWise remains `0.x`; never publish `1.0.0` or above. Use a patch for
  compatible fixes and the next `0.x` minor for features or intentional
  compatibility changes. Document material operator impact and align `setup.py`
  with the top CHANGELOG release.

Before completion:

1. Run every applicable lint, unit, config, scoped E2E, connector, and docs check.
2. Update validated docs for user-facing behavior/config changes and the root
   changelog for release-visible connector changes.
3. Report pass/skip/fail counts by group; skips, failures, and unavailable checks
   are not passing verification.
4. Require clean `git diff --check` and only expected `git status` entries.
