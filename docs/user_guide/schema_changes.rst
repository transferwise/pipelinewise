
.. _schema_changes:

Schema Changes
--------------

Taps detect schema changes in source databases and target connectors alter the
destination tables automatically. Based on the schema change type PipelineWise
performs different actions in the destination tables:

* **When new column added**: target connectors **add the new column** to the destination
  table with the same name using a compatible data type.

* **When column dropped**: target connectors **DO NOT drop columns**.
  Old column remains in the table in case you need to do historical analysis on
  the column. If the old column is not needed in the destination table then you can
  perform a manual ``ALTER TABLE ... DROP COLUMN ...`` statement in the target database
  or alternatively you can :ref:`resync` the table.

* **When column data type changed**: target connectors **versioning the column**.


.. _versioning_columns:

Versioning columns
''''''''''''''''''

Target connectors are versioning columns **when data type change detected** in the source
table. Versioning columns means that the old column with the old datatype is
renamed by adding a timestamp to column name and a new column with the new data
type will be added to the table.

For example if the data type of ``COLUMN_THREE`` changes from ``INTEGER`` to ``VARCHAR``
PipelineWise will replicate data in this order:

1. Before changing data type ``COLUMN_THREE`` is ``INTEGER`` just like in in source table:

+----------------+----------------+------------------+
| **COLUMN_ONE** | **COLUMN_TWO** | **COLUMN_THREE** |
|                |                |   (INTEGER)      |
+----------------+----------------+------------------+
| text           | text           | 1                | 
+----------------+----------------+------------------+
| text           | text           | 2                | 
+----------------+----------------+------------------+
| text           | text           | 3                | 
+----------------+----------------+------------------+

2. After the data type change ``COLUMN_THREE_20190812_1520`` remains ``INTEGER`` with
the old data and a new ``COLUMN_TREE`` column created with ``VARCHAR`` type that keeps
data only after the change.

+----------------+----------------+--------------------------------+------------------+
| **COLUMN_ONE** | **COLUMN_TWO** | **COLUMN_THREE_20190812_1520** | **COLUMN_THREE** |
|                |                |                   (INTEGER)    |    (VARCHAR)     |
+----------------+----------------+--------------------------------+------------------+
| text           | text           | 111                            |                  |
+----------------+----------------+--------------------------------+------------------+
| text           | text           | 222                            |                  |
+----------------+----------------+--------------------------------+------------------+
| text           | text           | 333                            |                  |
+----------------+----------------+--------------------------------+------------------+
| text           | text           |                                | 444-ABC          |
+----------------+----------------+--------------------------------+------------------+
| text           | text           |                                | 555-DEF          | 
+----------------+----------------+--------------------------------+------------------+

.. warning::

  Please note the ``NULL`` values in ``COLUMN_THREE_20190812`` and ``COLUMN_THREE`` tables.
  **Historical values are not converted to the new data types!**
  If you need the actual representation of the table after data type changes then
  you need to :ref:`resync` the table.

