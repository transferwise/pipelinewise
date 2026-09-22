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
     - No
     - Detected from the connected server
     - Overrides automatic MariaDB or MySQL detection for source-specific
       behaviour.
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
     - Server-specific connector defaults
     - Runs after the connector defaults and can extend or override them.
       Defaults set UTC, ``wait_timeout=28800``, ``net_read_timeout=3600``, and
       ``innodb_lock_wait_timeout=3600``. MariaDB also sets
       ``max_statement_time=0``.
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
- Singer, FullSync, and PartialSync use an explicit ``engine`` value when
  present and otherwise detect the connected server. The resolved engine is
  used consistently for session defaults, GTID handling, binlog status, and
  managed Iceberg v3 JSON aliases. MariaDB sessions set
  ``max_statement_time=0``; MySQL sessions do not.
- MySQL partial-JSON events and MySQL/MariaDB compressed binlog events are not
  supported by the bundled decoder. Keep ``binlog_row_value_options`` empty,
  ``binlog_transaction_compression`` disabled, and MariaDB ``log_bin_compress``
  disabled where these variables exist.
  Previously logged unsupported events still require a resync.
- GTID checkpoints advance only after the transaction has been emitted and
  retain all source UUIDs or MariaDB domains. Keep the upgraded connector when
  resuming these complete-set bookmarks; older versions cannot reliably parse
  multi-source history. XA transactions are not supported.
- Legacy GTID bookmarks without ``gtid_complete: true`` are upgraded when their
  file/position coordinates remain available. This does not require FastSync.
  GTID-only bookmarks cannot be recovered and fail without changing state. Do
  not add the marker manually or invent GTID ranges.
- File/position checkpoints also wait for safe transaction boundaries. An
  unsafe legacy bookmark replays from the nearest proven boundary and skips rows
  already acknowledged by each stream. State advances only after target
  acknowledgement. Recovery fails without changing state if the retained binlog
  cannot prove a boundary.
  Proving the boundary scans that retained binlog from its beginning. Large
  binlogs can take time and temporarily increase source read load.
  MariaDB can infer a GTID from a saved file/position only at a verified
  transaction boundary.
  Not every historical omission can be detected from a saved position; resync
  affected tables when upgrading a tap suspected of dropping rows.
- MariaDB 11.4 zero ``End_log_pos`` values are supported. Do not enable
  ``binlog_legacy_event_pos`` for PipelineWise.
- PipelineWise retries a lost file/position connection twice from
  target-acknowledged state. Retries are at-least-once and remain in the run's
  single terminal log. Standalone ``tap-mysql`` exits for its supervisor to
  restart; GTID and metadata connections keep their safe reconnect behavior.
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
- Finish pending managed-Iceberg attempts with the PipelineWise version and
  configuration that created them before upgrading. Matching the previous
  charset alone is insufficient: recovery identity also includes engine and
  session settings, and older manifests lack the saved engine needed for
  re-export. See :ref:`snowflake_iceberg_recovery` for recovery guidance.
- Managed-Iceberg recovery that re-exports source data requires the same resolved
  MySQL or MariaDB engine recorded in the manifest. Recovery from completed
  staging does not reconnect to the source or recheck its engine. Resolve
  pending recovery before replacing or repointing the source server.
- Snowflake Singer, FullSync, and PartialSync can target managed Iceberg v3 with
  explicit tap-level configuration. See :ref:`snowflake_iceberg`.
- On a v3 route, MariaDB's generated ``JSON_VALID`` constraint identifies its
  ``JSON``-alias ``LONGTEXT`` columns for ``VARIANT`` loading. Plain
  ``LONGTEXT`` and native routes remain strings.
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
