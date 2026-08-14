# Contributing to PipelineWise

PipelineWise welcomes focused bug fixes, tests, documentation, and connector
improvements. Discuss large features, breaking changes, and new connectors in a
GitHub issue before implementation so the compatibility and operational
requirements are clear.

## Before changing code

1. Fork the repository and create a focused branch from `master`.
2. Sign every commit using `git commit -S`; unsigned commits are not accepted.
3. Inspect the relevant implementation and tests before proposing a change.
4. Reproduce defects with the smallest deterministic test you can.
5. Use the [`dev-project`](dev-project/README.md) Docker environment wherever
   possible. Its Linux runtime, databases, and connector layout best approximate
   production.
6. If you use an AI coding agent, ensure it follows the root and applicable
   scoped `AGENTS.md` files.

Never commit credentials, populated environment files, private keys, production
identifiers, or source records. Replace sensitive values with deterministic test
fixtures that preserve the relevant type, size, or boundary.

## Implementation expectations

- Keep the change within the owning package and avoid unrelated formatting.
- Add regression tests for changed behaviour. Cover relevant state, failure,
  cleanup, retry, and replay boundaries.
- Update configuration schemas, samples, and documentation with their behaviour.
- Declare dependencies in the owning `setup.py`; do not rely on transitive
  installations.
- Add a concise changelog entry for release-visible behaviour or dependency
  changes. Keep each independently reviewable change in its own bullet.
- Only Maintainers can correlate all changelog entries and convert into a Release

New connectors begin as Experimental. Maintainers promote a connector to
Available only after its ownership, dependencies, compatibility, CI coverage,
recovery behaviour, and operated routes are documented.

## Verification

Run applicable checks in the ready `pipelinewise` container. The root
implementation gates are:

```bash
ruff check pipelinewise tests
pylint pipelinewise tests
flake8 pipelinewise --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 pipelinewise --count --max-complexity=15 --max-line-length=120 --statistics
pytest --cov=pipelinewise --cov-fail-under=77 -v tests/units
```

Never run bare `pytest tests/`; it also collects credentialed end-to-end tests.

Changes under `singer-connectors/` require the owning connector's install, lint,
unit, coverage, and applicable integration targets. Root checks do not inspect
vendored connector source. Run relevant database and end-to-end routes serially
using the [`dev-project`](dev-project/README.md) environment.

After configuration schema or sample changes, run:

```bash
pipelinewise validate --dir dev-project/pipelinewise-config
```

For documentation changes, run:

```bash
cd docs
make check
```

Always run `git diff --check`. In the pull request, list exact commands and their
pass, fail, and skip counts. Treat skipped, unavailable, or credential-dependent
checks as explicit coverage gaps rather than passing verification.

## Pull requests

Keep each pull request focused enough to review and revert independently. Explain:

- the problem and its operational consequence;
- the chosen behaviour and any compatibility impact;
- the tests that demonstrate behavioural changes;
- migration, deployment, rollback, and recovery considerations where relevant;
- documentation and changelog changes; and
- exact validation results, including skipped or unavailable checks.

Documentation-only and maintenance changes may not need behavioural tests. State
why a requirement is not applicable instead of marking unperformed work complete.

## Bug and security reports

[Open a public issue](https://github.com/transferwise/pipelinewise/issues/new) for
non-sensitive defects and include:

- PipelineWise and connector versions;
- source, target, and replication method;
- minimal configuration with secrets removed;
- exact reproduction steps;
- expected and actual results;
- the relevant sanitized log excerpt; and
- whether state or target data changed.

Do not disclose suspected vulnerabilities, credentials, or sensitive production
details in a public issue. Use [GitHub private vulnerability
reporting](https://github.com/transferwise/pipelinewise/security/advisories/new)
to contact the maintainers securely.

## License

Contributions are licensed under Apache License 2.0. Packaged connector licenses
can differ; review the [license inventory](docs/project/licenses.rst) before adding
or redistributing a component.
