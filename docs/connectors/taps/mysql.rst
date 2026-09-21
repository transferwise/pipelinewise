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
   * - ``charset``
     - No
     - ``utf8mb4``
     - FastSync connection encoding; Singer connections always use ``utf8mb4``.
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
- MySQL partial-JSON events and MySQL/MariaDB compressed binlog events are not
  supported by the bundled decoder. Keep ``binlog_row_value_options`` empty,
  ``binlog_transaction_compression`` disabled, and MariaDB ``log_bin_compress``
  disabled where these variables exist.
  Previously logged unsupported events still require a resync.
- GTID checkpoints advance only after the transaction has been emitted and
  retain all source UUIDs or MariaDB domains. Keep the upgraded connector when
  resuming these complete-set bookmarks; older versions cannot reliably parse
  multi-source history. XA transactions are not supported.
- Existing MySQL/MariaDB GTID bookmarks without ``gtid_complete: true`` require
  a one-time FullSync before resuming. Older taps retained only a latest
  transaction or partial source history; even a range-shaped bookmark can omit
  previous-primary UUIDs or MariaDB domains. New snapshots set this marker only
  after capturing complete history. Do not add it manually or invent GTID ranges.
  Rejection does not modify the saved state or automatically resync the target.
  The startup error lists all selected streams with missing or incomplete legacy
  GTIDs so the required resync can be planned together.
- File/position checkpoints also wait for safe transaction boundaries. An
  identifiable unsafe legacy bookmark inside row events is rejected before decoding;
  resync the affected tables instead of manually advancing the bookmark.
  MariaDB can infer a GTID from a saved file/position only at a verified
  transaction boundary; ambiguous positions require FullSync instead.
  Not every historical omission can be detected from a saved position; resync
  affected tables when upgrading a tap suspected of dropping rows.
- A lost binlog connection in file/position mode stops the run. Retry normally
  to resume from durable state; the decoder cannot safely reconnect using its
  last packet position inside a transaction. GTID mode retains safe reconnects.
  Separate table-metadata connections can still retry transient disconnects.
- ``TRUNCATE`` on a selected table stops binlog replication because it has no
  per-row delete images. FullSync that table to capture the resulting contents
  before resuming Singer.
- The connector interprets ``TINYINT(1)`` as Boolean; other display widths are
  integers. Snowflake bulk mappings use floating-point ``DECIMAL`` and Boolean
  ``BIT`` values, so they do not preserve arbitrary decimal precision or
  multi-bit bitsets. MySQL ``TIME`` values outside a 24-hour clock cannot be
  represented by Snowflake ``TIME``.
- The bundled decoder does not distinguish SQL ``NULL`` from JSON literal
  ``null`` in native MySQL JSON binlog values. Do not rely on Singer preserving
  that distinction; this limitation does not apply to MariaDB's JSON text alias.
- After an initial FastSync, LOG_BASED or INCREMENTAL replication continues from
  the captured bookmark in the Singer portion of the same run.
- Replica FullSync captures the primary's applied binlog coordinates, not the
  receiver's potentially newer position. Singer replays changes not yet present
  in the replica snapshot. Multi-channel replicas require an unambiguous source
  and are rejected rather than choosing an arbitrary channel.
- Snowflake Singer loading, FullSync, and PartialSync preserve line breaks,
  tabs, CSV punctuation, literal backslash sequences, and supplementary Unicode
  in string values. Singer connections use ``utf8mb4``. FastSync defaults to
  ``utf8mb4`` connections and uses an ``utf8mb4`` text projection; an explicitly
  narrower FastSync connection charset can still limit representable characters.
  FastSync removes NUL characters.
- Finish pending managed-Iceberg FastSync recovery before upgrading from the
  previous default charset, or retain explicit ``charset: utf8`` while finishing
  recovery. Recovery rejects a changed source encoding; use ``utf8mb4`` for
  subsequent runs after clearing the pending attempt.
- Snowflake Singer, FullSync, and PartialSync can target managed Iceberg v3 with
  explicit tap-level configuration. See :ref:`snowflake_iceberg`.
- On an explicit v3 route with ``engine: mariadb``, MariaDB's generated
  ``JSON_VALID`` constraint identifies its ``JSON``-alias ``LONGTEXT`` columns
  for ``VARIANT`` loading. Plain ``LONGTEXT`` and native routes remain strings.
  Object, array, string, number, Boolean, and null JSON roots are carried as
  validated JSON text and restored as ``VARIANT``. JSON null remains distinct
  from SQL ``NULL``.
- Use :ref:`troubleshooting` for missing-binlog and packet-size failures.
- Primary-key value changes are replicated as deletion of the previous key and
  insertion of the new key. After changing the primary-key definition itself,
  refresh the catalog and resync the table; ordinary binlog replication does not
  automatically detect every key-definition change.
- Fixes prevent new omissions but do not repair rows already lost or corrupted.
  Use :ref:`resync` to reload affected tables or an appropriate PartialSync range.
