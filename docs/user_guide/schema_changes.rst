.. _schema_changes:

Schema changes
==============

Singer taps emit schema messages and targets apply compatible changes before
loading affected records. Target behaviour preserves existing data rather than
silently coercing or deleting it.


Change outcomes
---------------

.. list-table::
   :header-rows: 1
   :widths: 27 35 38
   :width: 100%

   * - Source change
     - Target action
     - Operational consequence
   * - Add column
     - Add a compatible target column.
     - Historical rows normally contain ``NULL`` until explicitly backfilled.
   * - Drop column
     - Retain the target column.
     - Historical values remain queryable; remove it manually only after
       downstream review.
   * - Change data type
     - Rename the old target column with a timestamp suffix and create a new
       column using the new type.
     - Old and new values are split across columns until a resync or downstream
       migration.

Target-specific type mapping and experimental connector behaviour still apply.
Test changes that narrow precision, alter timezone semantics, or change nested
JSON shape before production rollout.


.. _versioning_columns:

Column versioning
-----------------

If ``COLUMN_THREE`` changes from ``INTEGER`` to ``VARCHAR``, the target keeps the
old data in a name such as ``COLUMN_THREE_20260809_1520`` and writes subsequent
records to a new ``COLUMN_THREE`` column.

PipelineWise does not convert historical values into the new type. Queries that
need one logical field must handle both versions until the table is deliberately
resynced or migrated.


Operational procedure
---------------------

Before a planned source change:

1. inspect target type mapping and downstream contracts;
2. test discovery and one representative load;
3. decide whether historical values require conversion;
4. notify consumers of versioned or retained columns; and
5. retain enough source log to recover if the first changed record fails.

After the change, verify the new target schema and representative values. Use
:ref:`resync` only when its source and target cost is acceptable; a resync is not
required merely because an old target column remains.
