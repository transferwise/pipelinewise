# AI Coding Agent Instructions

## Scope and shared work

PipelineWise is a Python 3.12 Singer ELT framework. Write for senior engineers
with operational precision, edge cases and rationale. Explicit user instructions
win; root rules apply repo-wide and scoped guides add to them:

- `pipelinewise/AGENTS.md`: orchestration, FastSync, backend and data-diff.
- `singer-connectors/AGENTS.md`: vendored taps and targets.
- `tests/AGENTS.md`: test boundaries and evidence.
- `tests/end_to_end/AGENTS.md`: databases, E2E, CI and `dev-project/`.
- `docs/AGENTS.md`: documentation.

Edit `AGENTS.md`, preserving each adjacent `CLAUDE.md` symlink. Preserve dirty
work. While another session works here, do not switch branches, reset files,
rebuild environments, restart shared services or reset fixtures; coordinate or
isolate disruptive work. Read-only reviews do not trigger implementation gates.
If told not to run anything, limit work to file reads and text searches.

## Architecture and environment

- Singer JSON flows as `tap | [transform-field] | [mbuffer] | target`; YAML
  generates config, state and catalogs. `pipelinewise/cli/__init__.py` dispatches to
  `PipelineWise`. Use `fast_sync` and `import_config`; `sync_tables` and `import`
  remain deprecated aliases, not names for new docs, tests or comments.
- FastSync optimizes full/filtered bulk transfer; it is not a replication method.
  Singer handles INCREMENTAL, LOG_BASED and non-FastSync FULL_TABLE streams.
- Normalize timestamps/windows to UTC after interpreting source timezones;
  never depend on the host or container timezone.
- Prefer dev-project Docker for production compatibility. Use the host to
  manage Docker or when the container cannot run a check; report fallback gaps.
  The repo mounts at `/opt/pipelinewise` in `pipelinewise`; environments under
  `dev-project/.virtualenvs/` are on `PIPELINEWISE_HOME`'s PATH.
- `make pipelinewise` installs the editable root and test extras. Connector
  runtimes use `.virtualenvs/<name>/`; connector Makefiles may use `venv/`.
  Never mix host/container, root, runtime-connector or connector-test interpreters,
  or use repository `.venv/`.

## Validation

For implementation changes, run these root gates verbatim and in order in the
ready container; a host fallback must activate `.virtualenvs/pipelinewise/`:

```bash
ruff check .
pytest --cov=pipelinewise --cov-fail-under=77 -v tests/units
```

Ruff follows root `pyproject.toml`: source, data-diff, root E2E and the tap-mysql,
tap-postgres, tap-yugabyte, target-postgres and target-snowflake CI unit suites
are included. Connector tests outside existing GitHub CI, including integration,
legacy tests and spikes, are excluded. Keep changed Python within this policy;
do not escape checks through exclusions or broad ignores. Never run bare
`pytest tests/`, which collects credentialed E2E. Collect nested data-diff and
backend units from `tests/units`, narrowing with `-k` to avoid import failures.

After implementation, schema, example-config or connector-config changes:

```bash
pipelinewise validate --dir dev-project/pipelinewise-config
```

Compose loads `dev-project/.env`. A host fallback must source, not overwrite it:

```bash
set -a
. dev-project/.env
set +a
.virtualenvs/pipelinewise/bin/pipelinewise validate --dir dev-project/pipelinewise-config
```

Follow scoped connector and docs checks. Run local E2E only when explicitly
requested; prefer PR CI and follow `tests/end_to_end/AGENTS.md`. Reviews use
bounded, non-disruptive verification within scope. Guidance-only edits need
text, link, symlink and diff checks, not project execution. Do not repeat passed
checks without changed inputs, a new failure or an unresolved concern.

## Style and diagnostics

- Python: 120 columns, complexity 15, four spaces, Google docstrings, single
  quotes, `snake_case` names/JSON keys and `PascalCase` classes. Declare imports
  in the owning `setup.py`; do not rely on transitive installs.
- Use only the repository's Ruff lint tooling; add no other linter or formatter.
  Keep error, warning, quote, line-length, complexity and `PLE` gates; preview
  rules require explicit
  selection and `E301`–`E306` cover spacing. Do not enable `PLC`, `PLR` or `PLW`
  wholesale over legacy code without scoped cleanup and a regression plan.
- Keep new ignores specific to the narrowest line/function and rule. Do not
  add connector/directory exemptions, broaden unrelated suppressions, reformat
  unrelated files or run `pre-commit run --all-files`.
- Comments explain non-obvious constraints/consequences in at most two lines;
  omit restated code, edit narratives, arguments and walkthroughs.
- Log source hostnames consistently at INFO once per run/session through shared
  helpers, never for every row, array or hstore value. Reuse shared redaction;
  never log usernames, passwords, keys, tokens or raw source values, including
  through exceptions. Metadata and aggregate evidence may support diagnosis.
- Log column versioning at INFO with the same format across Singer and native/
  Iceberg PartialSync. Uppercase and quote Snowflake FastSync identifiers.
- Never commit secrets, `.tfvars`, private keys or populated environment files.

## Reviews

Keep reviews read-only unless fixes are requested. Verify the reviewed revision
and base; recheck before carrying findings forward. Number findings by severity,
highest first, with a concrete trigger, impact and code reference. Distinguish
introduced defects, inherited limitations and operational risks. Separate
unverified concerns and validation gaps from confirmed defects; missing tests
alone do not establish a Medium issue. If there are no findings, say so.

## Git, releases and completion

- Branch from `master`, keep scope narrow and sign every commit with
  `git commit -S`; never create or push an unsigned commit.
- Exclude internal Jira names/keys (such as `AP-...`) from commit messages, PR
  titles and bodies, even if the branch has one. Add no AI/model `Co-authored-by`
  or "Generated with" attribution.
- Write docs/CHANGELOG in plain language: short sentences, one idea each,
  behaviour/default/operator action first. CHANGELOG bullets start with verbs
  and cover user-visible outcomes and upgrade steps. Link detailed rules;
  retain exact limits, boundaries and recovery requirements when shortening.
- Before PR creation/push, reconcile the whole branch diff with its CHANGELOG
  entry; stop if it omits/misstates changes or claims absent behaviour. Date
  that release today, never `TBD` or an earlier date, and link its entry in the PR.
- Remain below `1.0.0`: patches for compatible fixes, the next `0.x` minor for
  features/intentional compatibility changes. Align `setup.py` and the top
  CHANGELOG release and document material operator impact.
- Finish applicable authorised lint, unit, config, connector, docs and CI checks;
  update user-facing docs and release-visible connector CHANGELOG entries.
  Report actual pass/skip/fail counts by group and unavailable checks; skips
  and partial matrices are not passes. Require clean scoped `git diff --check`
  and inspect `git status`, distinguishing your changes from pre-existing work.
