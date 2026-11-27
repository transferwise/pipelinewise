# Definition sync from generated config

## Intended behaviour

A data-diff check belongs to one installation at a time, while several installations share one backend. The
installation that imports project YAML keeps the checks of its own taps. An installation that generates runtime config
without `import_config`, such as a Kubernetes operator, keeps the checks of the taps it runs. A tap can move between
them and keep its definitions, revisions and coverage.

## Decisions

- **Definitions come from a generated file, not YAML.** `import_config` writes `<target>/<tap>/data_diff.json` with only
  the keys `extract_check_definitions` reads. `sync_data_diff_checks` rebuilds definitions from it with the same
  function, so both paths produce equal `CheckDefinition`s and `config_hash`es. Keys missing from the config stay
  missing, so validation fails the same way on both paths. The file carries no credentials or connection details.
- **Sync is scoped to one tap.** `sync_data_diff_checks` calls `sync_definitions(selected_taps=[tap])`. It can create,
  revise and deactivate only that tap's definitions.
- **A missing input is an error.** Treating it as "no checks" would deactivate every definition of the tap after a
  mounting or naming mistake. An explicit input with no checked tables deactivates them on purpose.
- **Imports exclude the other installation's taps explicitly.** `import_config --taps '*'` deactivates every current
  definition whose tap is missing from its YAML, and a moved tap looks exactly like a deleted one.
  `--exclude-data-diff-taps` passes the existing `excluded_taps` scope, so those taps' definitions are neither created
  nor deactivated. The importing installation's operator supplies the list; PipelineWise cannot tell a move from a
  deletion.
- **Secret lookups render through a seam.** `RuntimeConnectorConfigLoader` takes a `render` callable. The CLI passes
  `utils.render_env_var_lookups`, so `data_diff` gains no dependency on the CLI package.

## Affected components

`data_diff/config.py` (`definition_input`, `definitions_from_input`), `data_diff/runtime.py`
(`load_definition_input`, the loader's `render`), `cli/config.py` (writes and removes the file), `cli/pipelinewise.py`
(`sync_data_diff_checks`, import exclusion, rendering for runs and reruns), and `cli/__init__.py` (command, option and
validation).

## Failure-case acceptance tests

- A round trip through the saved JSON yields equal definitions and hashes, including inherited tap defaults
  (`tests/units/data_diff/test_config.py`).
- Transformed comparison columns and a missing source `dbname` fail the same way on both paths.
- A missing, mismatched or malformed input fails before the backend is opened (`test_runtime.py`, `test_cli.py`).
- An empty input deactivates only the selected tap. Excluded taps reach `sync_definitions` as `excluded_taps`.
- Runs and reruns render secret lookups; a missing variable stops the check instead of connecting without a secret.

## References

`sync_definitions` scope rules (`data_diff/repository.py`), `import_project` (`cli/pipelinewise.py`), and the user guide
section [Sharing a backend between installations](../../docs/user_guide/data_diff.rst).
