.. _contribution:

Contributing
============

PipelineWise vendors connector source under ``singer-connectors`` and integrates
it with YAML generation, discovery, state handling, tests, and CI. A Singer-
compatible executable is necessary but not sufficient for PipelineWise support.


Development environment
-----------------------

Use the repository's ``dev-project`` Docker environment whenever possible. It
provides the Linux runtime, databases, and connector layout closest to production.
Read the root and scoped ``AGENTS.md`` files before changing code or tests.

Keep changes limited to the owning package. Root lint and unit tests do not inspect
vendored connector source, so run the connector's own Makefile targets as well.


Add or update a source connector
--------------------------------

The source must:

1. accept ``--config`` and a selected ``--catalog`` or ``--properties`` file;
2. accept optional ``--state`` and emit ordered Singer ``SCHEMA``, ``RECORD``,
   and ``STATE`` messages;
3. implement discovery and stable stream identifiers;
4. live in ``singer-connectors/<tap-name>`` with explicit dependencies;
5. be registered in connector constants, tap properties, schemas, samples, and
   the root Makefile when it is packaged;
6. preserve target-bounded state and restart semantics;
7. include connector-local unit/integration coverage and route E2E where
   available; and
8. document status, prerequisites, replication methods, configuration, limits,
   failure, and recovery.


Add or update a target connector
--------------------------------

The target must:

1. accept ``--config`` and consume Singer messages from standard input;
2. emit acknowledged Singer state only after corresponding records are durable;
3. handle duplicate replay around the acknowledgement boundary;
4. live in ``singer-connectors/<target-name>`` with explicit dependencies;
5. be registered in target schemas, samples, constants, packaging, and CI;
6. test schema evolution, nulls, empty strings, deletes, large values, and batch
   flush boundaries; and
7. document target privileges, publication semantics, recovery, and limitations.


Support status
--------------

Newly packaged connectors start as Experimental unless maintainers explicitly
accept production support. Promotion to Available requires documented ownership,
maintained dependencies, CI coverage, deterministic recovery tests, and an
operated source-to-target route. Update :ref:`connector_support`, connector
inventories, installation packaging, and :ref:`licenses` together.


Validation
----------

Before opening a pull request:

- run the root lint and unit gates from ``AGENTS.md``;
- run the owning connector's install, lint, unit, coverage, and applicable
  integration targets;
- run the narrowest relevant E2E route serially;
- run ``pipelinewise validate`` after sample or schema changes;
- run ``cd docs && make check`` after documentation changes; and
- run ``git diff --check``.

Report exact commands and pass, fail, and skip counts. A skipped or unavailable
integration is an explicit coverage gap, not a passing result.
