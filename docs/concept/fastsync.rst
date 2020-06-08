
.. _fast_sync_main:

Fast Sync
---------

**Fast Sync** is one of the :ref:`replication_methods` that is functionally identical to :ref:`full_table`
replication but Fast Sync is bypassing the `Singer Specification <https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md>`_
for optimised performance. Primary use case of Fast Sync is initial sync or to resync large tables
with hundreds of millions of rows where singer component would usually be running for long hours or
sometimes for days.

.. warning::

  **Important**: Fast Sync is not a selectable replication method in the :ref:`yaml_configuration`.
  PipelineWise detects automatically when Fast Sync gives better performance than the singer
  components and uses it whenever it's possible.

Fast Sync exists only between the following tap and target components:

+---------------------------------------------------------------+
| **Fast Sync supported Tap to Target combinations**            |
+----------------------------+----------------------------------+
| **Tap**                    | **Target**                       |
+----------------------------+----------------------------------+
| :ref:`tap-mysql`           | **->** :ref:`target-snowflake`   |
+----------------------------+----------------------------------+
| :ref:`tap-postgres`        | **->** :ref:`target-snowflake`   |
+----------------------------+----------------------------------+
| :ref:`tap-s3-csv`          | **->** :ref:`target-snowflake`   |
+----------------------------+----------------------------------+
| :ref:`tap-mongodb`         | **->** :ref:`target-snowflake`   |
+----------------------------+----------------------------------+
| :ref:`tap-mysql`           | **->** :ref:`target-redshift`    |
+----------------------------+----------------------------------+
| :ref:`tap-postgres`        | **->** :ref:`target-redshift`    |
+----------------------------+----------------------------------+
| :ref:`tap-s3-csv`          | **->** :ref:`target-redshift`    |
+----------------------------+----------------------------------+
| :ref:`tap-mysql`           | **->** :ref:`target-postgres`    |
+----------------------------+----------------------------------+
| :ref:`tap-postgres`        | **->** :ref:`target-postgres`    |
+----------------------------+----------------------------------+
| :ref:`tap-s3-csv`          | **->** :ref:`target-postgres`    |
+----------------------------+----------------------------------+
| :ref:`tap-mongodb`         | **->** :ref:`target-postgres`   |
+----------------------------+----------------------------------+

