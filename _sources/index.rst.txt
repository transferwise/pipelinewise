PipelineWise
============

PipelineWise is a Python framework for configuring, running, and operating
Singer-based ELT pipelines. It moves source data into analytical destinations
with log-based, incremental, or full-table replication and optional native bulk
transfer through :ref:`fast_sync_main`.

.. important:: Project scope

   Available sources are MariaDB and PostgreSQL; available targets are
   PostgreSQL and Snowflake. Other packaged connectors, including the Snowflake
   source, are experimental. ``pipelinewise init`` also generates some legacy
   templates that are not packaged. Review :ref:`connector_support` before
   selecting a source and target.

   Release ``v0.64.1`` is the last version from before the connector set was
   reduced. It is a historical reference, not a recommendation to deploy an
   older release.


Start here
----------

1. :ref:`installation_guide` — install PipelineWise, preferably with Docker.
2. :ref:`creating_pipelines` — configure an available source-to-target route.
3. :ref:`running_pipelines` — validate, import, run, and inspect a pipeline.
4. :ref:`troubleshooting` — diagnose failures and recover safely.


Core capabilities
-----------------

.. list-table::
   :header-rows: 1
   :widths: 28 72
   :width: 100%

   * - Capability
     - Behaviour
   * - Singer replication
     - Log-based change capture, key-based incremental loads, and full-table
       snapshots.
   * - FastSync
     - Native FullSync and PartialSync transfers for supported database routes.
   * - Schema evolution
     - Detects source schema changes and applies compatible target changes.
   * - Load-time protection
     - Masks, hashes, or removes sensitive values before target loading.
   * - Data-diff
     - Performs bounded aggregate reconciliation with auditable coverage and
       remediation.
   * - Configuration as code
     - Generates connector JSON, catalogs, and state from version-controlled YAML.


Documentation
-------------

.. toctree::
   :maxdepth: 2
   :caption: Installation

   installation_guide/installation
   installation_guide/creating_pipelines
   installation_guide/running_pipelines

.. toctree::
   :maxdepth: 2
   :caption: Concepts

   concept/singer
   concept/replication_methods
   concept/fastsync
   concept/linux_pipes

.. toctree::
   :maxdepth: 2
   :caption: Operations

   user_guide/yaml_config
   user_guide/encrypting_passwords
   user_guide/cli
   user_guide/data_diff
   user_guide/scheduling
   user_guide/multi_server_cluster
   user_guide/logging
   user_guide/alerts
   user_guide/resync
   user_guide/partial_sync
   user_guide/schema_changes
   user_guide/transformations
   user_guide/metadata_columns
   user_guide/troubleshooting

.. toctree::
   :maxdepth: 2
   :caption: Connectors
   :titlesonly:

   Connector overview <connectors/index>
   Sources <connectors/taps>
   Targets <connectors/targets>

.. toctree::
   :maxdepth: 2
   :caption: Project

   project/contribution
   project/about
   project/licenses
