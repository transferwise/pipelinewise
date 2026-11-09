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

## Agent roles

- Prefer GPT-6 Astra for planning, architecture decisions and orchestration
  when a task warrants multiple agents.
- Prefer GPT-6.1 Sol for scoped implementation, testing and independent code
  review. Use a separate reviewer from the implementer.
- Give independent reviewers a fresh conversation context (`fork_turns: "none"`
  where available), requirements, the proposal or diff, and relevant source
  references. Let them form their own conclusions without the author's transcript.
- Delegate bounded tasks with context, file ownership and acceptance criteria;
  coordinate edits to shared files. Keep small tasks with one agent.
- These are preferences. Explicit user choices and model availability take
  precedence; disclose any substitute.

## Architecture and environment

- For consequential architecture or behavior changes, keep a short design record
  beside the relevant code or docs: intended behavior, decisions and rationale,
  source references, affected components, and failure-case acceptance tests.
  Link durable design records from scoped guidance; small fixes need no record.
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
- Distinguish missing values from valid `0`, `False`, empty strings and empty
  collections. Use key membership for presence and `is None` for null checks
  when truthiness would discard a meaningful value.
- Prefer straightforward control flow, named intermediate values and early
  returns where they clarify decisions. Extract helpers around meaningful
  responsibilities, not merely to shorten functions or satisfy complexity gates.
- Make mutation and I/O explicit in function contracts. Prefer pure helpers for
  validation, conversion and planning; callers should know when inputs change
  or a function writes files or accesses a database.
- Add precise type hints to new interfaces using Python 3.12 notation such as
  `dict[str, str]` and `X | None`. Use structured types for substantial shared
  payloads; avoid unrelated annotation rewrites.
- Catch expected exceptions near the operation and preserve useful, sanitized
  context. Reserve broad catches for deliberate orchestration boundaries; make
  resource cleanup explicit through context managers or `finally`.
- Use only the repository's Ruff lint tooling; add no other linter or formatter.
  Keep error, warning, quote, line-length, complexity and `PLE` gates; preview
  rules require explicit
  selection and `E301`–`E306` cover spacing. Do not enable `PLC`, `PLR` or `PLW`
  wholesale over legacy code without scoped cleanup and a regression plan.
- Keep new ignores specific to the narrowest line/function and rule. Do not
  add connector/directory exemptions, broaden unrelated suppressions, reformat
  unrelated files or run `pre-commit run --all-files`.
- Prefer short comments explaining why. Allow longer explanations for subtle
  correctness invariants; put extensive procedures in linked documentation.
  Omit restated code, edit narratives, arguments and walkthroughs.
- Log source hostnames consistently at INFO once per run/session through shared
  helpers, never for every row, array or hstore value. Reuse shared redaction;
  never log usernames, passwords, keys, tokens or raw source values, including
  through exceptions. Metadata and aggregate evidence may support diagnosis.
- Log column versioning at INFO with the same format across Singer and native/
  Iceberg PartialSync. Uppercase and quote Snowflake FastSync identifiers.
- Never commit secrets, `.tfvars`, private keys or populated environment files.

## Reviews

Keep reviews read-only unless fixes are requested. Start with a brief explanation
of the current behavior and proposed change; identify unclear assumptions before
critiquing the implementation. Verify the reviewed revision and base; recheck
before carrying findings forward. Number findings by severity,
highest first, with a concrete trigger, impact and code reference. Distinguish
introduced defects, inherited limitations and operational risks. Separate
unverified concerns and validation gaps from confirmed defects; missing tests
alone do not establish a Medium issue. If there are no findings, say so.

## Git, releases and completion

- Branch from `master`, keep scope narrow and sign every commit with
  `git commit -S`; never create or push an unsigned commit.
- Exclude internal Jira names/keys (such as `AP-...`) from commit messages, PR
  titles and bodies, even if the branch has one, except bot-required links in
  `Context` below. Add no AI/model `Co-authored-by` or "Generated with" attribution.
- Follow [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) and
  [Wise's description rules](https://github.com/transferwise/wise-github-bot/wiki/good-description).
  Always include the exact level-two heading `## Context` with more than 70
  characters explaining the problem and operational consequence, even if a link
  would satisfy the bot. Retain the template's Changes, Type of change,
  Compatibility and operations, Validation, and Checklist sections; check only
  verified items and explain anything not applicable.
- Check PR labels and `.github/tw-rules.yaml` before publishing: `change:impactful`
  requires a non-empty Jira ticket or Confluence link in Context;
  `alwaysRequiresJiraTicket: true` requires a Jira ticket, not just Confluence.
  Use the `https://transferwise.atlassian.net/browse/...` or `/wiki/...` URLs;
  a ticket counts only if the bot can fetch its description. Recheck the rendered
  description after creating or updating the PR.
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
