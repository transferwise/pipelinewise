# Contributing to PipelineWise

PipelineWise accepts focused bug fixes, tests, documentation, and connector
improvements. Discuss large features or compatibility changes in a GitHub issue
before implementation so support, migration, and operational requirements are
clear.

## Before changing code

1. Fork the repository and branch from `master`.
2. Read the root `AGENTS.md` and the scoped file for the package you will change.
3. Inspect the existing worktree and preserve unrelated changes.
4. Reproduce the problem with the smallest deterministic test.
5. Use the `dev-project` Docker environment wherever possible; its Linux,
   database, and connector layout best approximates production.

Do not include credentials, populated environment files, private keys, source
records, or production identifiers in a pull request.

## Implementation expectations

- Keep changes within the owning package and avoid unrelated formatting.
- Add regression coverage for observable behaviour, failure boundaries, state,
  cleanup, and retries.
- Preserve target-bounded Singer acknowledgement and replay safety.
- Update connector configuration, samples, schemas, and documentation together.
- Treat FastSync as a native transfer optimisation, not a replication method.
- Add a changelog entry for release-visible behaviour or dependency changes.

New connectors begin as Experimental. Promotion to Available requires maintained
ownership, dependency and CI coverage, deterministic interruption recovery, and a
documented operated route.

## Verification

Run checks in the ready `pipelinewise` container. The root implementation gates
are:

```bash
ruff check pipelinewise tests
pylint pipelinewise tests
flake8 pipelinewise --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 pipelinewise --count --max-complexity=15 --max-line-length=120 --statistics
pytest --cov=pipelinewise --cov-fail-under=77 -v tests/units
```

Never run bare `pytest tests/`; it collects credentialed end-to-end tests.

Connector source has its own Makefile, lint, unit, coverage, integration, and E2E
requirements under `singer-connectors/AGENTS.md`. Root gates do not inspect it.

For documentation changes, run:

```bash
cd docs
make check
```

Always run `git diff --check`. Report exact commands and pass, fail, and skip
counts; an unavailable or skipped integration is not a passing result.

## Pull requests

A pull request should contain:

- the problem and operational consequence;
- the chosen behaviour and compatibility impact;
- tests that fail without the change;
- migration, rollback, or recovery instructions where relevant;
- documentation and changelog updates; and
- exact validation evidence.

Keep each pull request small enough to review and revert independently.

## Bug reports

[Open an issue](https://github.com/transferwise/pipelinewise/issues/new) with:

- PipelineWise and connector versions;
- source, target, and replication method;
- minimal configuration with secrets removed;
- exact reproduction steps;
- expected and actual results;
- the complete relevant log excerpt; and
- whether state or target data changed.

Do not attach production data. Replace sensitive values with deterministic test
fixtures that preserve the failing type, size, or boundary.

## License

Contributions are licensed under Apache License 2.0. Packaged connector licenses
can differ; review the [license inventory](docs/project/licenses.rst) before adding
or redistributing a component.
