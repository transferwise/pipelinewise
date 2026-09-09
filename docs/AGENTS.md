# Documentation Instructions

Read root `AGENTS.md` and the guide for the documented behavior.

## Build and sources

`docs/Makefile` sets `SPHINXOPTS = -W`, so warnings fail. The root test extra
omits Sphinx; the ready container needs the editable package, `sphinx`,
`sphinx-rtd-theme`, and `sphinxcontrib-mermaid`. Never validate with
`scripts/publish_docs.sh`, which mutates Git.

After changing RST, Sphinx config, or assets:

```bash
cd docs && make check
```

``make check`` validates embedded YAML plus implementation-backed CLI, config,
and packaged-connector references before a strict clean HTML build. Use
``make clean html`` only to isolate Sphinx failures.

Verify prose against implementation, CLI help, schemas, example YAML, and
runtime—not assumptions or only the diff. Use `import_config` (`import` is
deprecated) and describe FastSync as a FullSync/PartialSync optimization, not
Singer replication.

## Content

- Write concise, authoritative, operations-first public docs for operators and
  integration engineers; exclude Wise-only hosts, credentials, and runbooks.
- Order: support, prerequisites/defaults, operational impact, failures,
  diagnosis, recovery.
- Separate defaults from examples and current support from future intent;
  repeat only for standalone use or safety.

## RST

- Preserve heading levels. New pages use `=`, `-`, `'`, then `"` unless their
  section differs.
- Put `.. _label:` directly before its heading; link with ``:ref:`label` ``.
- Declare every code-block language and use Sphinx admonitions.
- Add pages to the owning toctree, including nested connector indexes; strict
  builds reject orphans.

## Update map

| Change | Required docs |
| --- | --- |
| CLI add/remove/rename | `user_guide/cli.rst`: arguments, environment, references, examples, mentions |
| Connector add/remove | Connector page, galleries, `project/licenses.rst`, `installation_guide/installation.rst` |
| Connector behavior/capability/limit | Connector page; concept page if cross-cutting |
| YAML parameters | `user_guide/yaml_config.rst` and connector page |
| Singer replication | `concept/replication_methods.rst`; `concept/singer.rst` if relevant |
| FastSync components/selection/pairs | `concept/fastsync.rst`; never list FastSync as a replication method |
| Resync/`sync_start_from` | `concept/fastsync.rst`, `user_guide/resync.rst` |
| Data-diff checks/config/coverage/remediation | `user_guide/data_diff.rst` |
| Data-diff backend config/schema/reporting | `user_guide/data_diff_backend.rst` |
| PartialSync edge cases | `user_guide/partial_sync.rst`, including visuals |
| Alert handlers | `user_guide/alerts.rst` |
| Operational diagnostics | `user_guide/troubleshooting.rst` |

Backend schema changes also require migration/ERD updates per
`pipelinewise/AGENTS.md`. Update scoped guidance only for durable architecture,
CI, or workflow changes; otherwise keep docs edits task-scoped.
