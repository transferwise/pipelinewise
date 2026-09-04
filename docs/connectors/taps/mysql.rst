.. _tap-mysql:

MariaDB and MySQL source
========================

``tap-mysql`` extracts relational tables with full-table, key-based incremental,
or binlog-based replication. MariaDB and MySQL share the connector but have
different support status.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Bulk transfer
   * - MariaDB
     - Available
     - FullSync to PostgreSQL or Snowflake; PartialSync to Snowflake, including
       managed Iceberg v3
   * - MySQL
     - Experimental
     - FullSync to PostgreSQL or Snowflake; PartialSync to Snowflake, including
       managed Iceberg v3


Prerequisites
-------------

The runtime user needs ``SELECT`` on every replicated table and access to
``INFORMATION_SCHEMA``. LOG_BASED replication additionally requires
``REPLICATION CLIENT`` and ``REPLICATION SLAVE``.

Configure the source before selecting LOG_BASED:

.. code-block:: ini

   [mysqld]
   log_bin=mysql-binlog
   binlog_format=ROW
   binlog_row_image=FULL

Retain binlogs longer than the maximum expected outage. If PipelineWise's saved
position is purged, the affected tables require a resync.


Configuration
-------------

.. code-block:: yaml

   id: "orders"
   name: "Orders MariaDB"
   type: "tap-mysql"
   owner: "data-platform@example.com"
   db_conn:
     host: "<HOST>"
     port: 3306
     user: "<USER>"
     password: "{{ env_var['MARIADB_PASSWORD'] }}"
     dbname: "orders"
     engine: "mariadb"
     use_gtid: true
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   schemas:
     - source_schema: "orders"
       target_schema: "repl_orders"
       tables:
         - table_name: "payments"
           replication_method: "LOG_BASED"

.. list-table:: Connector-specific settings
   :header-rows: 1
   :widths: 24 20 18 38
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``engine``
     - For MariaDB GTID
     - ``mysql``
     - Selects MariaDB or MySQL source-specific semantics.
   * - ``use_gtid``
     - No
     - ``false``
     - Stores a GTID bookmark instead of a filename and position.
   * - ``replica_host``
     - No
     - Primary host
     - Offloads FastSync reads; LOG_BASED continues from the primary.
   * - ``filter_dbs``
     - No
     - All visible schemas
     - Limits discovery to a comma-separated schema list.
   * - ``export_batch_rows``
     - No
     - ``50000``
     - Controls rows written per FastSync export batch.
   * - ``session_sqls``
     - No
     - Connector defaults
     - Sets session variables after connecting.
   * - ``fastsync_parallelism``
     - No
     - CPU count
     - Controls concurrent FastSync table exports.

Common tap settings are documented in :ref:`yaml_configuration`. Generate the
full template with ``pipelinewise init``.


Operational notes
-----------------

- ``binlog_row_image`` must remain ``FULL``; sparse row images can omit values
  required to reconstruct a target row.
- The connector interprets ``TINYINT`` as Boolean. Values outside ``0`` and ``1``
  can fail when the target column is Boolean.
- After an initial FastSync, LOG_BASED or INCREMENTAL replication continues from
  the captured bookmark in the Singer portion of the same run.
- Snowflake Singer, FullSync, and PartialSync can target managed Iceberg v3 with
  explicit tap-level configuration. See :ref:`snowflake_iceberg`.
- On an explicit v3 route with ``engine: mariadb``, MariaDB's generated
  ``JSON_VALID`` constraint identifies its ``JSON``-alias ``LONGTEXT`` columns
  for ``VARIANT`` loading. Plain ``LONGTEXT`` and native routes remain strings.
  Object, array, string, number, Boolean, and null JSON roots are carried as
  validated JSON text and restored as ``VARIANT``. JSON null remains distinct
  from SQL ``NULL``.
- Use :ref:`troubleshooting` for missing-binlog and packet-size failures.
