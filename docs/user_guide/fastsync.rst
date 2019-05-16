
.. _fast_sync_main:

Fast Sync
---------

Fast Sync is a :ref:`replication_methods` that is functionally identical to :ref:`full_table`
replication but Fast Sync bypassing the `Singer Specification <https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md>`_
for optimised performance. Primary use case of Fast Sync is initial sync or resync large tables
with hundreds of millions of rows where singer components usually running for long hours or
sometimes for days.

.. warning::

  **Important**: Fast Sync is not a selectable replication method in the :ref:`yaml_configuration`.
  PipelineWise detects automatically when Fast Sync gives better performance than the singer
  components and uses it whenever it's possible.

Fast Sync exists only between the following tap and target components:

==================== ==================
Tap                  Target
==================== ==================
tap-mysql            target-snowflake
tap-postgres         target-snowflake
==================== ==================

