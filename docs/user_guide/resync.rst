
.. _resync:

Resync Tables
-------------

Sometimes you will need to resync tables in ad-hoc bases. For example when
binlog position deleted in MySQL or when a database migrated to another server
and previous CDC bookmarks are not transferable, etc.

1. **Full resync**

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

.. warning::

  Based on the tap setting, tables can be fully synced or partial synced if they are defined
  as partial synced.
  Currently this option is available only for :ref:`tap-mysql` and :ref:`tap-postgres` to Snowflake.

.. attention::

  There is an option for :ref:`tap-mysql` and :ref:`tap-postgres` to :ref:`target-snowflake` in main pipelinewise
  config file for ignoring resync in a case the size of a table in the tap is greater than the defined value.
  this setting is optional and even yet you can force the resync by using ``--force`` argument.

    $ pipelinewise sync_tables --target <target_id> --tap <tap_id> --force

  this setting can be added in the `config.yml` for checking the table size:

  .. code-block:: yaml

     allowed_resync_max_size:
       table_mb: <integer/float>


.. attention::

  There is an option to chose tables for re-sync which has a specific replication method by ``--replication_method_only <name of replication method>``

   $ pipelinewise sync_tables --target <target_id> --tap <tap_id> --replication_method_only log_based



2. **Partial resync**

If you want to partial resync a table from a specific tap then use the ``partial_sync_table`` command
and specify the tap and target ids and table, column ,start_value and end_value(optional)

.. code-block:: bash

    $ pipelinewise partial_sync_table --target <target_id> --tap <tap_id> --table schema.table --column column_name --start_value start_value_from_column --end_value end_value_from_column

**note** if there is no end_value, the internal state file will be updated with the replication value (gtid, wal, etc) that was captured at the start of the partial sync

.. warning::

  The value of the ``--table`` argument needs to be in the ``<schema_name>.<table_name>`` format. Schema and
  table name have to be the names in the source database.

More description about different cases of partial resync can be found here :ref:`partial_sync_cases`

