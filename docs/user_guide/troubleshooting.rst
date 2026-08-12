
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
   * - ``Lost connection`` or ``max_allowed_packet``
     - MariaDB / MySQL
     - Session timeouts and packet limits.
   * - Missing or corrupt binlog
     - MariaDB / MySQL LOG_BASED
     - Source retention and saved binlog position.
   * - ``wal_level >= logical`` or missing slot
     - PostgreSQL LOG_BASED
     - Primary configuration and slot existence.
   * - ``PGRES_COPY_BOTH``
     - PostgreSQL LOG_BASED
     - ``wal_sender_timeout`` and target backpressure.
   * - Recovery conflict
     - PostgreSQL replica reads
     - Standby replay delay settings.
   * - Table not found or zero discovered tables
     - Discovery / FastSync
     - Source name and replication-user permissions.
   * - Stale ``.running`` log
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

**When to resync**

* For **INCREMENTAL** replication you almost never need to resync, unless there
  was data corruption or a large number of old rows were updated that need to
  be replicated to the target.
* For **LOG_BASED** MariaDB/MySQL, you need to resync if the source binlog is no
  longer available.
* For **LOG_BASED** PostgreSQL, you need to resync if the replication slot is no
  longer available.

**INCREMENTAL method and missing updates**

When using incremental replication, PipelineWise will miss new or updated records if
the ``replication_key`` column is being back-filled (i.e. older rows are receiving
new values for the replication key). Consider using ``LOG_BASED`` replication if
you need to capture all changes.

**Using replica_host for large table resyncs (1 TB+)**

Both MySQL and PostgreSQL taps support setting a ``replica_host`` key. When set,
the initial FastSync will be done on the replica, reducing pressure on the primary
database. Once the resync has completed, the tap will pick up the replication slot
it created on the primary to continue replication.

You can use this effectively with a partial sync (``sync_start_from``) to resync
large tables and/or switch them to log-based replication.

**Changes to transformations**

Changes to transformations will only be applied to newly extracted data. If you need
the transformation applied to existing data in the target, you will need to resync
the affected tables.

**Resync table size limit**

For MySQL and PostgreSQL taps, ``fast_sync`` checks the size of non-partial-sync
tables and will refuse to proceed if any table exceeds the configured
``allowed_resync_max_size`` limit (see :ref:`resync`). Use ``fast_sync --force``
to override this check.


MariaDB / MySQL Errors
''''''''''''''''''''''

**Lost connection to MySQL server (Errno 104, Connection reset by peer)**

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

**Unknown encoding: utf8mb3**

*How to fix:*
The tap cannot decode ``utf8mb3`` data. Identify the affected table or column, then
work with your database administrator to test and convert its character set to a
supported encoding such as ``utf8mb4``. A character-set conversion changes source
data and should be tested before it is applied in production.

**Bogus data in log event**

*Why it happens:*
The source server (OS or MySQL service) restarted unexpectedly, corrupting the
current binlog position stored in PipelineWise state.

*How to fix:*
Run a full resync with
``pipelinewise fast_sync --tap <tap_id> --target <target_id>``. See :ref:`resync`
for the impact and available table-selection options.

**Log event exceeded max_allowed_packet**

*Log message:*

.. code-block:: text

    log event entry exceeded max_allowed_packet; Increase max_allowed_packet on master

*How to fix:*
Increase ``max_allowed_packet`` on the source server. If the problem persists after
increasing the value, run a FastSync to rebuild the affected tables.

**Missing binlog**

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

**Changing the binlog position in state.json**

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

**requires wal_level >= logical**

*Log message:*

.. code-block:: text

    {SCHEMA_NAME}.{TABLE_NAME}:logical decoding requires wal_level >= logical

*Why it happens:*
The PostgreSQL source database does not have logical replication enabled.

*How to fix:*
Set ``wal_level`` to ``logical`` on the source database. Note that changing the WAL
level requires restarting the database instance. If the tap owner cannot tolerate a
restart, consider changing the replication method to something other than ``LOG_BASED``.

**PGRES_COPY_BOTH and no message from the libpq**

*Log message:*

.. code-block:: text

    logger_name=tap_postgres log_level=CRITICAL message=error with status
    PGRES_COPY_BOTH and no message from the libpq

*Why it happens:*
PostgreSQL kills the replication connection because it thinks the client has died.
This usually happens when it takes a long time to load data into the target.

*How to fix:*

* **Option 1: Upgrade to PostgreSQL 12 or above.** PG12 and above supports session-level
  ``wal_sender_timeout`` and PipelineWise takes advantage of it.

* **Option 2: Increase the ``wal_sender_timeout`` value on the source.**
  A ``wal_sender_timeout`` value of less than 5 minutes will often result in this error.
  Check the current value with:

  .. code-block:: sql

      SELECT name, setting, unit FROM pg_settings WHERE name = 'wal_sender_timeout';

* **Option 3: Enable or increase the tap ``stream_buffer_size``.**
  The ``stream_buffer_size`` is a buffer allowing synchronous reading and loading of data.
  Note that a small ``wal_sender_timeout`` will cause even a very large ``stream_buffer_size``
  to run out of space, so fix ``wal_sender_timeout`` first.

**Canceling statement due to conflict with recovery**

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

**Connection already closed**

*Log message:*

.. code-block:: text

    logger_name=tap_postgres log_level=CRITICAL message=connection already closed
    psycopg2.OperationalError: SSL connection has been closed unexpectedly

*Why it happens:*
The source server is likely configured to kill idle connections.

*How to fix:*
Review the idle connection timeout settings on the source PostgreSQL server.

**recovery is in progress**

*Log message:*

.. code-block:: text

    logger_name=tap_postgres log_level=CRITICAL message=recovery is in progress
    HINT: WAL control functions cannot be executed during recovery.

*Why it happens:*
The tap is configured with a mix of incremental and log-based replication methods,
and the connection points to a replica. Log-based replication cannot run against a
replica.

*How to fix:*
Point the tap at the primary server, or change the log-based tables to use a different
replication method.

**Unable to find replication slot**

*How to fix:*
Run ``pipelinewise fast_sync --tap <tap_id> --target <target_id>`` to resync the
tap and recreate the replication slot. See :ref:`resync` before proceeding.

.. warning::

    Triggering a resync of a PostgreSQL tap using ``fast_sync`` will drop the
    replication slot.


FastSync Errors
'''''''''''''''

**Table not found**

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

**Failure to import tap (0 tables discovered)**

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


Logging and Diagnostics
'''''''''''''''''''''''

Use ``pipelinewise status`` to get an overview of all configured pipelines and
their last run status. Use ``pipelinewise test_tap_connection`` to verify
connectivity to a source before running a full sync.

Log files are written to ``~/.pipelinewise/<target_id>/<tap_id>/log/`` with a
``.success`` or ``.failed`` suffix indicating the outcome of each run.
See :ref:`logging` for details.

To follow the progress of a running sync:

.. code-block:: bash

    $ tail -f ~/.pipelinewise/<target_id>/<tap_id>/log/*running

You can also check the temporary files being written during a sync:

.. code-block:: bash

    $ ls -lah ~/.pipelinewise/tmp/*<tap_id>*


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
