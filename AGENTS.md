# AI Coding Agent Instructions

## Purpose
PipelineWise is an ELT data pipeline framework built on the Singer.io specification. It replicates data from various sources (taps) to analytics data warehouses (targets) with minimal load-time transformations.

## Architecture
- **Language:** Python 3.12
- **Framework:** Singer.io (taps and targets communicate via JSON over stdout/stdin)
- **Config format:** YAML (tap/target definitions), JSON (runtime configs, state, catalog)
- **CLI:** argparse-based, entry point at `pipelinewise/cli/__init__.py`. Commands map to methods on `PipelineWise` class via `getattr`. Alias mechanism: command names can be remapped before dispatch (e.g. deprecated `sync_tables` → `fast_sync`; canonical `import_config` and its deprecated `import` alias → `import_project`).

### Key Components
- `pipelinewise/cli/pipelinewise.py` — Main orchestration engine (~2000 lines). `run_tap()` is the core pipeline execution method. `fast_sync()` is the entry point for the `fast_sync` CLI command (aliased from `sync_tables`).
- `pipelinewise/cli/commands.py` — Builds shell commands: `tap | transform-field | mbuffer | target` pipeline.
- `pipelinewise/cli/config.py` — Loads/validates YAML configs, generates runtime JSON files at `~/.pipelinewise/<target_id>/<tap_id>/`.
- `pipelinewise/cli/constants.py` — Connector type enums and mappings.
- `pipelinewise/fastsync/` — Optimized native database-to-database sync (10-100x faster than Singer for full loads).
- `pipelinewise/cli/alert_handlers/` — Slack and VictorOps alerting. Extend by subclassing `BaseAlertHandler`.
- `pipelinewise/cli/schemas/` — JSON Schema files for validating tap/target configs.

### Sync Paths
1. **Singer** — Standard replication via `tap | transform | target` piped processes. Used for ongoing INCREMENTAL and LOG_BASED replication.
2. **FastSync** — Performance optimization that bypasses Singer for bulk data operations using native database tools. Not a replication method — it is an optimization engine with two components:
   - **FullSync** — Exports entire tables and replaces the target. Used automatically for initial syncs and explicitly via the `fast_sync` CLI command. Used for FULL_TABLE replication.
   - **PartialSync** — Exports a filtered range of rows and merges with the target. Used explicitly via the `partial_sync_table` CLI command, or automatically when `fast_sync` encounters tables with `sync_start_from` in the tap config.
   - FastSync infrastructure lives at `pipelinewise/fastsync/`, with shared connectors in `pipelinewise/fastsync/commons/`. PartialSync lives at `pipelinewise/fastsync/partialsync/` and imports from `commons/`.
   - Supported pairs: MySQL/PG/S3-CSV/MongoDB → Snowflake/PG. Defined PartialSync (`sync_start_from`) only: MySQL/PG → Snowflake.

### Supported Connectors
- **Taps:** MySQL, PostgreSQL, MongoDB, Kafka, S3 CSV, Snowflake, Salesforce, Zendesk, Jira, Google Analytics, Oracle, GitHub, Slack, Shopify, Twilio, Zuora, Mixpanel
- **Targets:** Snowflake, PostgreSQL, S3 CSV

## Development Environment
- **Python:** 3.12 required (`python_requires='==3.12.*'`)
- **Setup:** `pip install -e ".[test]"` from repo root
- **Connectors:** Installed separately via `make` targets into `.virtualenvs/` directory

## Build & Test
- **Unit tests:** `pytest tests/` (use `.venv/bin/pytest` if system Python lacks dependencies)
- **Lint:** `flake8`, `pylint`, `ruff`
- **Format check:** `pre-commit run --all-files`
- **Single test:** `pytest tests/path/to/test.py -v`
- **Coverage:** `pytest --cov=pipelinewise tests/`

## Repository Map
- `pipelinewise/cli/` — CLI and orchestration logic
- `pipelinewise/fastsync/` — Optimized sync implementations
- `pipelinewise/fastsync/commons/` — Shared FastSync tap/target connectors (used by both FullSync and PartialSync)
- `pipelinewise/fastsync/partialsync/` — PartialSync implementations (imports from `commons/`)
- `singer-connectors/` — Git submodule references to Singer tap/target repos
- `tests/` — Unit and integration tests
- `docs/` — Documentation
- `dev-project/` — Example project for local development

## Code Style
- Follows PEP 8 with `ruff` and `flake8` enforcement.
- Use `snake_case` for functions/variables, `PascalCase` for classes.
- JSON configs use `snake_case` keys.
- Snowflake identifiers are uppercased in FastSync connectors.

## Documentation Style
- Format: reStructuredText (RST) using Sphinx.
- Heading conventions: `=` for page title (H1), `-` for concept-level pages (H1), `'` for subsections (H3), `"` for sub-subsections (H4). Concept pages (`docs/concept/`) use `-` for titles; user guide pages (`docs/user_guide/`) use `=` for titles.
- RST labels: use `.. _label_name:` before headings for cross-referencing with `:ref:`label_name``.
- Code examples: use `.. code-block:: yaml` (or `bash`, `sql`, etc.), never plain `.. code-block::`.
- Admonitions: `.. warning::`, `.. note::`, `.. tip::`, `.. attention::`, `.. seealso::`.
- When renaming CLI commands, update both the command reference in `cli.rst` and all cross-references (`:ref:` labels, code examples, inline mentions) across all docs.
- Use `import_config` in documentation, examples, tests, and comments. `import` is retained only as a deprecated CLI alias.

## Git & PR Policy
- Run `pytest` and linting before committing.
- Do not commit secrets, `.tfvars`, or private keys.
- **Keep documentation up to date**: When adding or changing features, update the corresponding docs in `docs/` and this `AGENTS.md` file.

  **When to update docs:**
  - Adding/removing a CLI command → update `docs/user_guide/cli.rst`
  - Adding/removing a connector → update `docs/connectors/taps/` or `docs/connectors/targets/`, the visual gallery in `docs/connectors/taps.rst` or `docs/connectors/targets.rst`, the license table in `docs/project/licenses.rst`, and the connector table in `docs/installation_guide/installation.rst`
  - Changing YAML config parameters → update `docs/user_guide/yaml_config.rst` and the relevant connector page
  - Changing replication behavior → update `docs/concept/replication_methods.rst` and/or `docs/concept/fastsync.rst`
  - Changing FastSync components, auto-selection logic, or supported pairs → update `docs/concept/fastsync.rst`
  - Changing resync behavior or `sync_start_from` → update `docs/concept/fastsync.rst`, `docs/user_guide/resync.rst`
  - Changing alert handlers → update `docs/user_guide/alerts.rst`
  - Adding known errors or operational tips → update `docs/user_guide/troubleshooting.rst`
  - Adding a new docs page → add it to the appropriate toctree in `docs/index.rst`
  - Changing architecture or build/test commands → update this `AGENTS.md` file

  **Key doc files:**
  - `docs/user_guide/yaml_config.rst` — YAML configuration reference (config.yml, tap, and target YAML structure)
  - `docs/user_guide/cli.rst` — CLI command reference (all commands, arguments, and environment variables)
  - `docs/user_guide/resync.rst` — Resync guide: full resync (`fast_sync`) and partial resync (`partial_sync_table`)
  - `docs/user_guide/partial_sync.rst` — Visual guide to partial sync edge cases (column diffs, hard/soft delete)
  - `docs/user_guide/troubleshooting.rst` — Troubleshooting guide (common errors, replication tips, diagnostics)
  - `docs/user_guide/alerts.rst` — Slack and VictorOps alert configuration
  - `docs/concept/fastsync.rst` — FastSync optimization: components (FullSync/PartialSync), auto-selection criteria, defined PartialSync (`sync_start_from`), supported tap-target combinations
  - `docs/concept/replication_methods.rst` — Singer replication method definitions (LOG_BASED, INCREMENTAL, FULL_TABLE). FastSync is NOT listed here — it has its own page.
  - `docs/installation_guide/installation.rst` — Installation guide and connector table
  - `docs/project/licenses.rst` — Connector license table
  - `docs/index.rst` — Table of contents (add new pages to the appropriate toctree)
  - `AGENTS.md` — AI agent instructions (this file)
