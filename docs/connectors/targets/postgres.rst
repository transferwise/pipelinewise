.. _target-postgres:

PostgreSQL target
=================

``target-postgres`` loads Singer streams into PostgreSQL and manages compatible
target schema changes.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Target
     - Status
     - Native transfer
   * - PostgreSQL
     - Available
     - FullSync from MariaDB/MySQL, PostgreSQL, or MongoDB; no PartialSync


Prerequisites
-------------

The target user needs to connect to the database and create or alter schemas,
tables, and indexes used by its pipelines. Grant only the target schemas it owns;
do not reuse the :ref:`data_diff` backend database or role as a replication
target.


Configuration
-------------

.. code-block:: yaml

   id: "postgres_dwh"
   name: "PostgreSQL warehouse"
   type: "target-postgres"
   db_conn:
     host: "<HOST>"
     port: 5432
     user: "<USER>"
     password: "{{ env_var['TARGET_POSTGRES_PASSWORD'] }}"
     dbname: "analytics"
     ssl: "true"

.. list-table:: Connection settings
   :header-rows: 1
   :widths: 24 18 18 40
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``host``
     - Yes
     - —
     - PostgreSQL server hostname.
   * - ``port``
     - Yes
     - —
     - PostgreSQL server port.
   * - ``user`` / ``password``
     - Yes
     - —
     - Target role credentials.
   * - ``dbname``
     - Yes
     - —
     - Database that receives target schemas.
   * - ``ssl``
     - No
     - Connector default
     - Uses ``sslmode=require`` when enabled.
   * - ``max_parallelism``
     - No
     - ``16``
     - Caps automatic Singer stream-flush threads. Configure this in the target
       ``db_conn``; tap-level ``parallelism_max`` is currently ineffective.

Target schema names and grants are configured in the tap YAML. See
:ref:`yaml_configuration` and generate the full template with
``pipelinewise init``.


Operational notes
-----------------

- Size transactions and ``batch_size_rows`` for available memory and WAL volume.
- The target must acknowledge Singer state only after the corresponding records
  are durable; PipelineWise persists that acknowledgement for source recovery.
- Schema evolution can add or version columns. See :ref:`schema_changes` before
  granting downstream consumers direct access.
