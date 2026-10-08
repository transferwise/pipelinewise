# PipelineWise Singer library

This vendored package retains the `pipelinewise-singer-python` distribution and
`singer` import names. The source baseline is the released 3.0.2 package; the
upstream 3.0.0 checkout supplies its existing tests and Apache 2.0 license.
PipelineWise owns subsequent changes and release notes.

Root and local connector installers explicitly install this package into each
isolated environment. Jira remains an external connector using its own upstream
`singer-python` dependency.

From this directory, `make venv lint unit_test` installs and verifies the library.
Use the development Docker container for testing.

For an isolated core development environment, install both local packages from
the repository root in one resolver operation:

```bash
python -m pip install -e singer-connectors/singer-python -e '.[test]'
```

The root source distribution includes this library, so the corresponding
non-editable command also works from an extracted source archive. Build and
install both packages when producing wheels. The root pins the unique local
version `3.0.2+pipelinewise.0.94.1`; installing it alone without the bundled
package or its wheel fails dependency resolution instead of loading the
published Singer implementation.

SQL decimals use a string value with an explicit schema marker:

```json
{"type": ["null", "string"], "format": "singer.decimal", "decimal": {"precision": 20, "scale": 4}}
```

`singer.decimal_support` preserves and validates these values without converting
them to floating point. Unmarked number schemas and legacy decimal-formatted
strings retain their existing behavior.

The original project is [PipelineWise Singer Python](https://github.com/transferwise/pipelinewise-singer-python).
See [LICENSE](LICENSE) for the retained Apache 2.0 license.
