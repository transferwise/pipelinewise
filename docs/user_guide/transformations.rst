.. _transformations:

Load-time transformations
=========================

PipelineWise can transform selected fields between the tap and target. Use this
boundary to prevent sensitive source values from reaching the warehouse or
external staging. Perform joins, aggregations, and analytical modelling after
ingestion.


Transformation types
--------------------

.. list-table:: Available
   :header-rows: 1
   :widths: 34 28 38
   :width: 100%

   * - Type
     - Compatible input
     - Output
   * - ``SET-NULL``
     - Any supported field
     - ``NULL``.
   * - ``HASH``
     - String
     - SHA-256 representation.
   * - ``HASH-SKIP-FIRST-n``
     - String; ``n`` is 1–9
     - First ``n`` characters plus a hash of the remainder.
   * - ``MASK-DATE``
     - Date or timestamp
     - Date masked to 1 January.
   * - ``MASK-NUMBER``
     - Numeric
     - Zero.
   * - ``MASK-HIDDEN``
     - String
     - ``hidden``.
   * - ``MASK-STRING-SKIP-ENDS-n``
     - String; ``n`` is 1–9
     - Preserves the first and last ``n`` characters and masks the middle.

For ``MASK-STRING-SKIP-ENDS-n``, values no longer than ``2 * n`` are fully
masked.


Configuration
-------------

.. code-block:: yaml

   tables:
     - table_name: "users"
       replication_method: "LOG_BASED"
       transformations:
         - column: "email"
           type: "HASH"
         - column: "phone"
           type: "MASK-STRING-SKIP-ENDS-2"


Nested JSON fields
------------------

``field_paths`` applies a transformation to selected paths inside the JSON
object named by ``column``. A condition can similarly inspect
``when[].field_path``.

.. warning::

   PipelineWise rejects ``field_paths`` and ``when[].field_path`` for every
   connector pair that has a FastSync component: MariaDB/MySQL, PostgreSQL, or
   MongoDB sources targeting PostgreSQL or Snowflake. This route-level check
   applies even when the configured table would use Singer replication. On
   those routes, transform only top-level columns or omit the nested condition.


.. _conditional_transformations:

Conditions
----------

``when`` applies a transformation only when every condition matches:

.. code-block:: yaml

   transformations:
     - column: "value"
       type: "SET-NULL"
       when:
         - column: "field_name"
           regex_match: "password|secret|token"
         - column: "environment"
           equals: "production"

Conditions can inspect a top-level column. Nested ``field_path`` conditions are
subject to the FastSync-route restriction above. Multiple entries use logical
AND.


.. _transformation_validation:

Validation and rollout
----------------------

``import_config`` checks transformation names, field existence, and type
compatibility. Runtime validation repeats the check so a later source-schema
change fails instead of emitting an unprotected value.

Transformation changes affect newly extracted records only. To update historical
target values, perform a controlled resync and verify that temporary files,
staging tables, logs, and rejected records do not expose the original value.

Test deterministic output, ``NULL`` handling, nested paths, conditional misses,
and schema changes before relying on a transformation as a security boundary.
