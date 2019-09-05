
.. _resync:

Resync Tables
-------------

Sometimes you will need to resync tables in ad-hoc bases. For example when
binlog position deleted in MySQL or when a database migrated to another server
and previous CDC bookmarks are not transferable, etc.

If you want to resync every table from a specific tap then use the ``sync_tables``
command and specify the tap and target ids:

.. code-block:: bash

    $ pipelinewise sync_tables --target <target_id> --tap <tap_id>


PipelineWise will update the bookmark(s) in the internal state files automatically
and at the next normal run it will load only the changes since the resync.

If you want to resync only a list of specific tables then
add the ``--tables`` argument:

.. code-block:: bash

    $ pipelinewise sync_tables --target <target_id> --tap <tap_id> --tables schema.table_one,schema.table_two

.. warning::

  The value of the optional ``--tables`` argument needs to be a comma separated
  list of table names using the ``<schema_name>.<table_name>`` format. Schema and
  table names have to be the names in the source database.

