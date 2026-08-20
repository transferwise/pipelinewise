
.. _troubleshooting:

Troubleshooting
===============

Start with the exact failing log line, connector versions, last acknowledged
state, and source/target errors from the same UTC interval. Do not resync or edit
state until the unavailable boundary has been identified.


Symptom index
-------------

.. list-table::
   :header-rows: 1
   :widths: 42 28 30
   :width: 100%

   * - Symptom
     - Area
     - First check
   * - :ref:`Lost connection <troubleshooting_mysql_lost_connection>` or
       :ref:`max_allowed_packet <troubleshooting_mysql_max_allowed_packet>`
     - MariaDB / MySQL
     - Session timeouts and packet limits.
   * - :ref:`Missing <troubleshooting_mysql_missing_binlog>` or
       :ref:`corrupt binlog <troubleshooting_mysql_bogus_log_event>`
     - MariaDB / MySQL LOG_BASED
     - Source retention and saved binlog position.
   * - :ref:`wal_level <troubleshooting_postgres_wal_level>` or
       :ref:`missing slot <troubleshooting_postgres_missing_slot>`
     - PostgreSQL LOG_BASED
     - Primary configuration and slot existence.
   * - :ref:`PGRES_COPY_BOTH <troubleshooting_postgres_pgres_copy_both>`
     - PostgreSQL LOG_BASED
     - Source logs, network, timeout, and target backpressure.
   * - :ref:`LOG_BASED throughput falls behind without errors
       <troubleshooting_postgres_logical_decoding_spill>`
     - PostgreSQL LOG_BASED
     - Logical-decoding disk spill and ``logical_decoding_work_mem``.
   * - :ref:`Recovery conflict <troubleshooting_postgres_recovery_conflict>`
     - PostgreSQL replica reads
     - Standby replay delay settings.
   * - :ref:`Table not found <troubleshooting_fastsync_table_not_found>` or
       :ref:`zero discovered tables <troubleshooting_discovery_zero_tables>`
     - Discovery / FastSync
     - Source name and replication-user permissions.
   * - :ref:`Iceberg publication remains ambiguous
       <troubleshooting_iceberg_publication>`
     - Snowflake FastSync
     - Recovery manifest, query tag, and Snowflake query history.
   * - :ref:`Snowflake VARCHAR width is incompatible
       <troubleshooting_snowflake_varchar_width>`
     - Snowflake FastSync / Singer
     - Physical column width and target-role DDL privileges.
   * - :ref:`Native-to-Iceberg primary name is absent
       <troubleshooting_iceberg_conversion_missing_primary>`
     - Snowflake conversion
     - Conversion manifest and ``_NATIVE`` / ``_ICEBERG`` companions.
   * - :ref:`Stale running log <troubleshooting_logging>`
     - Process lifecycle
     - PID file and complete process tree.


Replication checks
------------------

See :ref:`replication_methods` for a detailed explanation of each replication method.

.. warning::

    MySQL, MariaDB, and PostgreSQL tables using ``INCREMENTAL`` or ``LOG_BASED``
    replication must have a primary key by default. ``FULL_TABLE`` replication does
    not have this requirement. PipelineWise validates this during import and reports
    the affected stream before a run starts.

.. _troubleshooting_when_to_resync:

When to resync
''''''''''''''

* For **INCREMENTAL** replication you almost never need to resync, unless there
  was data corruption or a large number of old rows were updated that need to
  be replicated to the target.
* For **LOG_BASED** MariaDB/MySQL, you need to resync if the source binlog is no
  longer available.
* For **LOG_BASED** PostgreSQL, you need to resync if the replication slot is no
  longer available.

.. _troubleshooting_incremental_missing_updates:

INCREMENTAL method and missing updates
''''''''''''''''''''''''''''''''''''''

When using incremental replication, PipelineWise will miss new or updated records if
the ``replication_key`` column is being back-filled (i.e. older rows are receiving
new values for the replication key). Consider using ``LOG_BASED`` replication if
you need to capture all changes.

.. _troubleshooting_replica_host_resyncs:

Using replica_host for large table resyncs (1 TB+)
''''''''''''''''''''''''''''''''''''''''''''''''''

Both MySQL and PostgreSQL taps support setting a ``replica_host`` key. When set,
the initial FastSync will be done on the replica, reducing pressure on the primary
database. Once the resync has completed, the tap will pick up the replication slot
it created on the primary to continue replication.

You can use this effectively with a partial sync (``sync_start_from``) to resync
large tables and/or switch them to log-based replication.

.. _troubleshooting_transformation_changes:

Changes to transformations
''''''''''''''''''''''''''

Changes to transformations will only be applied to newly extracted data. If you need
the transformation applied to existing data in the target, you will need to resync
the affected tables.

.. _troubleshooting_resync_size_limit:

Resync table size limit
'''''''''''''''''''''''

For MySQL and PostgreSQL taps, ``fast_sync`` checks the size of non-partial-sync
tables and will refuse to proceed if any table exceeds the configured
``allowed_resync_max_size`` limit (see :ref:`resync`). Use ``fast_sync --force``
to override this check.


MariaDB / MySQL Errors
''''''''''''''''''''''

.. _troubleshooting_mysql_lost_connection:

Lost connection to MySQL server (Errno 104, Connection reset by peer)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

*Log message:*

.. code-block:: text

    pymysql.err.OperationalError: (2013, 'Lost connection to MySQL server
    during query ([Errno 104] Connection reset by peer)')

*Why it happens:*
Server session defaults are not conducive to PipelineWise type workloads.

*How to fix:*
Add the following ``session_sqls`` block to your ``tap.yml``:

.. code-block:: yaml

    dbname: "your_database"
    session_sqls:
      - SET SESSION max_statement_time=0
      - SET SESSION net_write_timeout=3600
      - SET SESSION time_zone="+0:00"
      - SET SESSION wait_timeout=28800
      - SET SESSION net_read_timeout=3600
      - SET SESSION innodb_lock_wait_timeout=3600

.. _troubleshooting_mysql_utf8mb3:

Unknown encoding: utf8mb3
"""""""""""""""""""""""""

*How to fix:*
The tap cannot decode ``utf8mb3`` data. Identify the affected table or column, then
work with your database administrator to test and convert its character set to a
supported encoding such as ``utf8mb4``. A character-set conversion changes source
data and should be tested before it is applied in production.

.. _troubleshooting_mysql_bogus_log_event:

Bogus data in log event
"""""""""""""""""""""""

*Why it happens:*
The source server (OS or MySQL service) restarted unexpectedly, corrupting the
current binlog position stored in PipelineWise state.

*How to fix:*
Run a full resync with
``pipelinewise fast_sync --tap <tap_id> --target <target_id>``. See :ref:`resync`
for the impact and available table-selection options.

.. _troubleshooting_mysql_max_allowed_packet:

Log event exceeded max_allowed_packet
"""""""""""""""""""""""""""""""""""""

*Log message:*

.. code-block:: text

    log event entry exceeded max_allowed_packet; Increase max_allowed_packet on master

*How to fix:*
Increase ``max_allowed_packet`` on the source server. If the problem persists after
increasing the value, run a FastSync to rebuild the affected tables.

.. _troubleshooting_mysql_missing_binlog:

Missing binlog
""""""""""""""

*Log message:*

.. code-block:: text

    Exception: Unable to replicate binlog stream because the following
    binary log(s) no longer exist: mysql-bin.000119

*Why it happens:*
The binary log files have been purged from the source server due to the binlog
retention period.

*How to fix:*

1. Resync the affected tables. If the error recurs, the retention period may be
   too low. You can check the current binlog files by running ``SHOW BINARY LOGS``
   on the source.
2. Ask your database administrator to increase the binlog retention period. You can
   check the current setting with:

   .. code-block:: sql

       SHOW GLOBAL VARIABLES LIKE 'expire_logs_days';

3. As a last resort, change the replication method from ``LOG_BASED`` to another method.

.. _troubleshooting_mysql_state_position:

Changing the binlog position in state.json
""""""""""""""""""""""""""""""""""""""""""

Prefer :ref:`cli_reset_state` for a controlled failover with an exact position
mapping. If no supported mapping is available and manual recovery is unavoidable,
stop the tap and back up the state first. Never edit a state file while its tap is
running.

.. code-block:: bash

    $ pipelinewise stop_tap --tap <tap_id> --target <target_id>
    $ cp state.json state.json.backup

The following command previews a binlog filename change without modifying the file:

.. code-block:: bash

    $ sed -E 's/(mysql-bin-changelog\.)[0-9]+/\1#####/g' state.json

Replace ``#####`` with the new binlog number. ``-E`` works with BSD/macOS and
modern GNU ``sed``. To update both the filename and ``log_pos``, write the result
to a new file rather than editing the state in place:

.. code-block:: bash

    $ sed -E \
        -e 's/(mysql-bin-changelog\.)[0-9]+/\1#####/g' \
        -e 's|("log_pos": )[0-9]+|\1#####|g' \
        state.json > state.json.new

Replace each ``#####`` with the intended value. Validate the new JSON before
replacing the state file:

.. code-block:: bash

    $ python -m json.tool state.json.new > /dev/null
    $ mv state.json.new state.json

Keep ``state.json.backup`` until the next run succeeds. A wrong binlog filename or
position can skip or duplicate data; use a full resync instead when the correct
position is uncertain.


PostgreSQL Errors
'''''''''''''''''

.. _troubleshooting_postgres_wal_level:

requires wal_level >= logical
"""""""""""""""""""""""""""""

*Log message:*

.. code-block:: text

    {SCHEMA_NAME}.{TABLE_NAME}:logical decoding requires wal_level >= logical

*Why it happens:*
The PostgreSQL source database does not have logical replication enabled.

*How to fix:*
Set ``wal_level`` to ``logical`` on the source database. Note that changing the WAL
level requires restarting the database instance. If the tap owner cannot tolerate a
restart, consider changing the replication method to something other than ``LOG_BASED``.

.. _troubleshooting_postgres_pgres_copy_both:

PGRES_COPY_BOTH and no message from the libpq
"""""""""""""""""""""""""""""""""""""""""""""

*Log message:*

.. code-block:: text

    logger_name=tap_postgres log_level=CRITICAL message=error with status
    PGRES_COPY_BOTH and no message from the libpq

*Why it happens:*
This libpq message says that the replication COPY connection ended without a more
specific client-side error; it does not identify the cause. Possible causes include
a PostgreSQL restart or termination, a network interruption,
``wal_sender_timeout`` expiry, or delayed feedback while the target is blocked.
Compare the PipelineWise, source PostgreSQL, and network logs from the same UTC
interval before changing timeouts or state.

*How to fix:*

* **If the source or network terminated the connection,** correct that cause and
  restart the same tap without advancing state.

* **For PostgreSQL versions before 12, consider upgrading.** PostgreSQL 12 and
  later support the session-level ``wal_sender_timeout`` that PipelineWise sets.

* **If the PostgreSQL log reports a sender timeout,** check the effective value:

  .. code-block:: sql

      SELECT name, setting, unit FROM pg_settings WHERE name = 'wal_sender_timeout';

* **If target backpressure is confirmed, enable or increase
  ``stream_buffer_size``.** This buffer decouples reading from loading. It cannot
  compensate indefinitely for a blocked target or an unsuitable timeout, so fix
  those causes first.

.. _troubleshooting_postgres_logical_decoding_spill:

LOG_BASED replication is slow or falling behind
"""""""""""""""""""""""""""""""""""""""""""""""

*Why it happens:*
On PostgreSQL 13 and later, ``logical_decoding_work_mem`` limits the memory used
by each logical replication connection. When decoded changes exceed the limit,
PostgreSQL writes them to local disk. A value that is too low for the source
workload can therefore cause frequent disk spill and substantially reduce
LOG_BASED throughput.

Each active logical replication connection has its own buffer. A busy source, large
or concurrent transactions, or several PipelineWise replications consuming slots
on the same PostgreSQL cluster can increase both spill I/O and total memory demand.

*How to diagnose:*
Check the configured value. PostgreSQL versions before 13 return no row because
they do not provide this setting:

.. code-block:: sql

    SELECT name, setting, unit, source
    FROM pg_settings
    WHERE name = 'logical_decoding_work_mem';

On PostgreSQL 14 and later, compare the following counters over the period when
replication is slow. Increasing ``spill_count`` or ``spill_bytes`` confirms that
logical decoding is writing changes to disk; a large historical total alone does
not prove a current bottleneck.

.. code-block:: sql

    SELECT slot_name,
           spill_txns,
           spill_count,
           pg_size_pretty(spill_bytes) AS spill_bytes
    FROM pg_stat_replication_slots
    ORDER BY spill_bytes DESC;

Check slot activity and acknowledgement separately. ``confirmed_flush_lsn`` is
the position acknowledged by the consumer, while ``restart_lsn`` is the oldest
WAL that may still be required. The calculated value approximates WAL retained
for each slot:

.. code-block:: sql

    SELECT slot_name,
           active,
           confirmed_flush_lsn,
           restart_lsn,
           pg_size_pretty(
               pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
           ) AS retained_wal
    FROM pg_replication_slots
    WHERE slot_type = 'logical'
    ORDER BY slot_name;

Sample these values over the same interval as the spill counters. An inactive
slot identifies a disconnected consumer. A stationary ``confirmed_flush_lsn``
shows that acknowledgement is not advancing, but does not by itself distinguish
source decoding from target or network backpressure.

See the PostgreSQL documentation for
`logical_decoding_work_mem <https://www.postgresql.org/docs/current/runtime-config-resource.html#GUC-LOGICAL-DECODING-WORK-MEM>`_
and
`logical replication slot statistics <https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-SLOTS-VIEW>`_.

*How to fix:*
Work with the source database administrator to increase the setting incrementally
for the PipelineWise replication role and database. There is no universal value;
test against the transaction sizes and concurrency of the source. For example:

.. code-block:: sql

    ALTER ROLE <replication_user> IN DATABASE <source_database>
    SET logical_decoding_work_mem = '<tested_value>';

Restart the tap after changing a role or database default so its replication
connection receives the new value. Budget memory for every concurrent logical
replication connection and monitor database memory and disk I/O while tuning.
Increasing this setting reduces decoding spill; it does not fix target, network,
or ``stream_buffer_size`` backpressure. Replication lag alone does not require a
resync.

.. _troubleshooting_postgres_recovery_conflict:

Canceling statement due to conflict with recovery
"""""""""""""""""""""""""""""""""""""""""""""""""

*Log message:*

.. code-block:: text

    logger_name=tap_postgres log_level=CRITICAL message=terminating connection due to
    conflict with recovery
    DETAIL: User query might have needed to see row versions that must be removed.
    HINT: In a moment you should be able to reconnect to the database and repeat your command.
    SSL connection has been closed unexpectedly

*Why it happens:*
The replica has been set up with restrictive replay settings. You can check the current
values with:

.. code-block:: sql

    SELECT name, setting, unit
    FROM pg_settings
    WHERE name IN ('max_standby_archive_delay', 'max_standby_streaming_delay');

*How to fix:*
Increase ``max_standby_streaming_delay`` on the replica.

.. _troubleshooting_postgres_connection_closed:

Connection already closed
"""""""""""""""""""""""""

*Log message:*

.. code-block:: text

    logger_name=tap_postgres log_level=CRITICAL message=connection already closed
    psycopg2.OperationalError: SSL connection has been closed unexpectedly

*Why it happens:*
The source server is likely configured to kill idle connections.

*How to fix:*
Review the idle connection timeout settings on the source PostgreSQL server.

.. _troubleshooting_postgres_recovery_in_progress:

recovery is in progress
"""""""""""""""""""""""

*Log message:*

.. code-block:: text

    logger_name=tap_postgres log_level=CRITICAL message=recovery is in progress
    HINT: WAL control functions cannot be executed during recovery.

*Why it happens:*
The tap is reading INCREMENTAL or FULL_TABLE replication from a replica. (LOG_BASED from a replica is not possible)

*How to fix:*
Point the tap at the primary server, or increase `max_standby_streaming_delay`.

.. _troubleshooting_postgres_missing_slot:

Unable to find replication slot
"""""""""""""""""""""""""""""""

*How to fix:*
Run ``pipelinewise fast_sync --tap <tap_id> --target <target_id>`` to resync the
tap and recreate the replication slot. See :ref:`resync` before proceeding.

.. warning::

    Triggering a resync of a PostgreSQL tap using ``fast_sync`` will drop the
    replication slot.


FastSync Errors
'''''''''''''''

.. _troubleshooting_snowflake_varchar_width:

Snowflake VARCHAR width is incompatible
"""""""""""""""""""""""""""""""""""""""

Snowflake FastSync stages MariaDB/MySQL and PostgreSQL string-like and fallback
types as ``VARCHAR(134217728)``. Managed Iceberg v3 also requires every existing
string column to have ``CHARACTER_MAXIMUM_LENGTH`` 134217728.

For a managed Iceberg v3 target, widen the reported column with ``ALTER ICEBERG
TABLE ... ALTER COLUMN ... SET DATA TYPE VARCHAR(134217728)`` or recreate the
table, then retry. PipelineWise does not change existing Iceberg widths.

For a native PartialSync target, PipelineWise widens a compatible narrow text
column before the merge. If that DDL fails, use a role authorized to alter the
table or widen it manually, then retry. The failed attempt does not run the merge
or advance state. A non-text target type requires a FullSync or an explicit,
reviewed schema migration.

``VARCHAR(134217728)`` remains subject to Snowflake's 128 MB encoded-value limit.
If the maximum-width column still rejects a value, measure its encoded byte size
and reduce, split, or exclude it at the source.

.. _troubleshooting_iceberg_publication:

Iceberg publication remains ambiguous
""""""""""""""""""""""""""""""""""""""""

A Snowflake connection can fail after a publication statement commits but before
the client receives its response. PipelineWise keeps the source state unchanged
and records the attempt in an ``iceberg-recovery-<hash>.json`` stream manifest
and ``iceberg-fastsync-target-<hash>.json`` target pointer under the generated
target runtime directory at ``$PIPELINEWISE_CONFIG_DIRECTORY/<target_id>/``.

Stop other writers to the target table and retry the same command without editing
state, dropping staging objects, or deleting either recovery file. Use the same
generated target runtime directory, tap/source identity, target mapping and role,
staging configuration, and transformations. For CTAS or ``INSERT OVERWRITE``,
PipelineWise uses a 60-second query-history polling budget. Each lookup scans at
most 10,000 visible completed queries from five minutes before the persisted
submission time through the earlier of the current time or 24 hours after
submission. Snowflake connector timeouts are best-effort, so a call can make the
observed wall-clock duration exceed the nominal budget. PipelineWise requires the
exact query tag, verifies the target, and resumes finalization. For PartialSync,
it replays the persisted range transaction deterministically. Query-history
visibility can lag, so a later retry may be required while Snowflake retains the
Information Schema query history.

Query-history visibility timeouts and lookup failures are reported as retryable
publication ambiguity. Their failure message confirms that the recovery files
and staging table were preserved and instructs the operator to retry the same
FastSync command unchanged; PipelineWise does not automatically republish or
advance state.

If reconciliation remains ambiguous, preserve the stream manifest, target
pointer, staging table, S3 keys, query tag, target identity, and logs. Inspect
them with a role that can see all relevant table metadata and query history
before changing target objects. See :ref:`snowflake_iceberg_recovery`.

.. _troubleshooting_iceberg_conversion_missing_primary:

Native-to-Iceberg primary name is absent
""""""""""""""""""""""""""""""""""""""""

``copy_native_to_iceberg --eventual iceberg`` uses one statement to rename the
native table to ``<table>_NATIVE`` and another to promote
``<table>_ICEBERG``. Rollback likewise uses two statements. The primary name is
absent between them, and an interruption can extend that reader outage until
recovery runs.

Keep every reader and writer stopped. Retry the identical command from the same
imported target runtime directory with the same table, ``--eventual`` value,
target identity, and account role. Do not rename or drop companion tables and do
not delete or edit the recovery manifest. If PipelineWise cannot prove one safe
table state, inspect the primary, ``_NATIVE``, ``_ICEBERG``, and manifest before
making a manual change. See :ref:`snowflake_iceberg`.

.. _troubleshooting_fastsync_table_not_found:

Table not found
"""""""""""""""

*Log message:*

.. code-block:: text

    CRITICAL: {SCHEMA_NAME}.TABLE_NAME table not found.

*Why it happens:*
FastSync cannot find the table in the source database. Common causes:

1. Wrong ``source_schema`` name in the tap definition.
2. The table does not exist in the source database.
3. The replication user does not have ``SELECT`` privilege on the table.

*How to fix:*

1. Check if the given source schema exists and is correct.
2. Verify that the table exists in the source. If it does not exist, remove
   it from the tap YAML file.
3. Verify user privileges by running:

   .. code-block:: sql

       SELECT * FROM information_schema.columns
       WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{TABLE_NAME}';

   If this returns no results, the replication user lacks ``SELECT`` privilege.

.. _troubleshooting_discovery_zero_tables:

Failure to import tap (0 tables discovered)
"""""""""""""""""""""""""""""""""""""""""""

*Log message:*

.. code-block:: text

    CRITICAL 0 tables were discovered across the entire cluster

*Why it happens:*

1. The source database does not exist.
2. The replication user does not have enough privileges to ``SELECT``.

*How to fix:*

1. Verify that the source database exists and the tap configuration points to
   the correct host and database name.
2. Test that the replication user can run a ``SELECT`` query on one of the tables
   in the source database.


.. _troubleshooting_logging:

Logging and Diagnostics
'''''''''''''''''''''''

Use ``pipelinewise status`` to get an overview of all configured pipelines and
their last run status. Use ``pipelinewise test_tap_connection`` to verify
connectivity to a source before running a full sync.

Runtime files are written below ``PIPELINEWISE_CONFIG_DIRECTORY``, which defaults
to ``~/.pipelinewise``. Tap logs are stored under
``<target_id>/<tap_id>/log/`` and use ``.running``, ``.success``, or ``.failed``
suffixes. See :ref:`logging` for details.

To follow the progress of a running sync:

.. code-block:: bash

    $ pipelinewise_config_dir="${PIPELINEWISE_CONFIG_DIRECTORY:-$HOME/.pipelinewise}"
    $ tail -f "$pipelinewise_config_dir/<target_id>/<tap_id>/log/"*running

You can also check the temporary files being written during a sync:

.. code-block:: bash

    $ ls -lah "$pipelinewise_config_dir/tmp/"*<tap_id>*


Verify recovery
---------------

After applying a repair:

1. restart the same tap-target pair without advancing state again;
2. confirm the command and ``pipelinewise status`` report success;
3. verify exact primary keys or a deterministic checksum in the affected range;
4. confirm the source bookmark or replication-slot acknowledgement advances;
5. retain the failed log and state backup until another normal run succeeds; and
6. remove temporary or staging objects only after their publication state is
   understood.
