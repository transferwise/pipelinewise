.. _target-postgres:

PostgreSQL target
=================

``target-postgres`` loads Singer streams into PostgreSQL and manages compatible
target schema changes.

MariaDB/MySQL and PostgreSQL source decimals use ``NUMERIC(p,s)`` with their
supported dimensions. Unconstrained PostgreSQL ``NUMERIC`` remains unconstrained.
Older targets use unconstrained ``NUMERIC`` for unsupported scale declarations.
Existing ``REAL`` or ``DOUBLE PRECISION`` columns created by the legacy decimal
mapping remain in place, including primary keys. Singer stages those values with
the existing target type. Finite values above ``1.7976931348623157e308`` for
``DOUBLE PRECISION`` or ``3.4028234663852886e38`` for ``REAL`` clamp to that
limit with the same sign. Magnitudes at or below ``2^-1075`` for
``DOUBLE PRECISION`` or ``2^-150`` for ``REAL`` become zero. Finite values round
to the nearest representable value, with exact ties rounded to even. ``NaN`` and
infinities remain unchanged. New decimal columns use the exact numeric mapping. Singer
groups retained floating-point decimal keys by their loaded target value.
Colliding changes follow source event order within each batch. Distinct source
keys that round or clamp to that value cannot remain distinct; FullSync the
table to use the exact numeric key mapping.
For MariaDB/MySQL tables, Singer keeps a nonempty legacy primary-key subset
until FullSync adopts the complete source key. Singer also keeps an existing
``YEAR`` key on its legacy text type. Retained text BLOB keys use uppercase
hexadecimal to match historical FastSync rows. Use FullSync to change these
legacy key mappings. Other exact numeric key type changes still require FullSync.
See :ref:`exact_decimal_mapping` for column versioning and key restrictions.

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
tables, and indexes used by its pipelines. Grant access only to its target schemas.
Use a separate database and role for the :ref:`data_diff_backend`.


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
- Source-delete markers always physically remove rows before state is
  acknowledged. Metadata columns are enabled automatically; see
  :ref:`metadata_columns` for deletion processing.
- Schema evolution can add or version columns. See :ref:`schema_changes` before
  granting downstream consumers direct access.
