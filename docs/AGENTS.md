# Documentation Instructions

Read root `AGENTS.md` first. This covers `docs/`; consult scoped implementation, connector, test, and E2E files for the behavior documented.

## Build and sources

`docs/Makefile` uses `SPHINXOPTS = -W`; publishing is clean, so any warning fails. The root `test` extra does not install Sphinx: use a docs environment containing the editable root package, `sphinx`, and `sphinx-rtd-theme`. `scripts/publish_docs.sh` shows those dependencies but mutates Git state and is not a local validation command. After changing RST, Sphinx config, or docs assets run:

```bash
cd docs && make clean html
```

Check prose against implementation, CLI help, JSON Schemas, example YAML, and runtime—not assumptions or only the diff. Use `.virtualenvs/pipelinewise/bin/pipelinewise --help` and command help. Use canonical `import_config` (`import` is deprecated); describe FastSync as FullSync/PartialSync optimization, not a Singer replication method; parse embedded YAML after config-example edits.

## Content style

- Write public documentation for PipelineWise operators and integration engineers; keep Wise-internal hosts, credentials, and runbooks out of this repository.
- Use a concise, authoritative, pragmatic, mildly operations-first engineering tone.
- Lead with supported behavior, prerequisites, and defaults; then explain operational impact, failure modes, diagnosis, and recovery where relevant.
- Distinguish defaults from examples and current support from future intent. Do not document planned behavior as available.
- Avoid repetition within and across pages, but repeat essential context where a section must stand alone or repetition prevents an operational mistake.

## RST

- Preserve each file's established heading hierarchy and do not normalize adornments opportunistically. For a new page, use `=` for the title, `-` for first-level sections, then `'` and `"`, unless its owning section consistently uses another hierarchy.
- Put `.. _label_name:` immediately before its heading and reference it with ``:ref:`label_name` ``.
- Declare every code-block language (`yaml`, `bash`, `sql`, etc.); use Sphinx admonitions.
- Add new pages to their owning toctree, including a nested connector index when applicable; strict builds reject orphans.

## Update map

| Change | Required documentation |
| --- | --- |
| CLI add/remove/rename | `user_guide/cli.rst`: arguments, environment, references, examples, inline mentions. |
| Connector add/remove | Connector page; tap/target gallery; `project/licenses.rst`; `installation_guide/installation.rst` table/guidance. |
| Connector behavior/capabilities/limitations | Owning connector page; a concept page for cross-cutting behavior. |
| YAML parameters | `user_guide/yaml_config.rst` and relevant connector page. |
| Singer replication | `concept/replication_methods.rst`; `concept/singer.rst` when relevant. |
| FastSync components/selection/pairs | `concept/fastsync.rst`; keep FastSync out of Singer replication-method lists. |
| Resync/`sync_start_from` | `concept/fastsync.rst`, `user_guide/resync.rst`. |
| Data-diff config/checks/coverage/remediation | `user_guide/data_diff.rst`. |
| PartialSync edge cases | `user_guide/partial_sync.rst`, including affected visuals. |
| Alert handlers | `user_guide/alerts.rst`. |
| Operational diagnostic/known failure | `user_guide/troubleshooting.rst`. |

Backend schema changes also follow migration/ERD history in `pipelinewise/AGENTS.md`.

## Scope

Update docs required by behavior, not unrelated prose. Avoid broad formatters/pre-commit; preserve RST wrapping/indentation. If durable architecture, CI gates, or workflow commands change, update their owning root/scoped `AGENTS.md`.
