# Documentation Instructions

Read root `AGENTS.md` first. This covers `docs/`; consult scoped implementation, connector, test, and E2E files for the behavior documented.

## Build and sources

`docs/Makefile` uses `SPHINXOPTS = -W`; publishing is clean, so any warning fails. After every docs change run:

```bash
cd docs && make clean html
```

Check prose against implementation, CLI help, JSON Schemas, example YAML, and runtime—not assumptions or only the diff. Use `.virtualenvs/pipelinewise/bin/pipelinewise --help` and command help. Use canonical `import_config` (`import` is deprecated); describe FastSync as FullSync/PartialSync optimization, not a Singer replication method; parse embedded YAML after config-example edits.

## Content style

- Use a concise, authoritative, pragmatic, mildly operations-first engineering tone.
- Avoid repetition within and across pages, but repeat essential context where a section must stand alone or repetition prevents an operational mistake.

## RST

- `concept/` titles use `-`; `user_guide/` titles use `=`; subsections use `'`, then `"`.
- Put `.. _label_name:` immediately before its heading and reference it with ``:ref:`label_name` ``.
- Declare every code-block language (`yaml`, `bash`, `sql`, etc.); use Sphinx admonitions.
- Add new pages to the correct `docs/index.rst` toctree; strict builds reject orphans.

## Update map

| Change | Required documentation |
| --- | --- |
| CLI add/remove/rename | `user_guide/cli.rst`: arguments, environment, references, examples, inline mentions. |
| Connector add/remove | Connector page; tap/target gallery; `project/licenses.rst`; `installation_guide/installation.rst` table/guidance. |
| YAML parameters | `user_guide/yaml_config.rst` and relevant connector page. |
| Singer replication | `concept/replication_methods.rst`; `concept/singer.rst` when relevant. |
| FastSync components/selection/pairs | `concept/fastsync.rst`; keep FastSync out of Singer replication-method lists. |
| Resync/`sync_start_from` | `concept/fastsync.rst`, `user_guide/resync.rst`. |
| Data-diff config/checks/coverage/remediation | `user_guide/data_diff.rst`. |
| PartialSync edge cases | `user_guide/partial_sync.rst`, including affected visuals. |
| Alert handlers | `user_guide/alerts.rst`. |
| Operational diagnostic/known failure | `user_guide/troubleshooting.rst`. |
| New page | Page plus appropriate `index.rst` toctree entry. |

Backend schema changes also follow migration/ERD history in `pipelinewise/AGENTS.md`.

## Scope

Update docs required by behavior, not unrelated prose. Avoid broad formatters/pre-commit; preserve RST wrapping/indentation. If durable architecture, CI gates, or workflow commands change, update their owning root/scoped `AGENTS.md`.
