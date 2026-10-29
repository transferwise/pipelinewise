.. _tap-yugabyte:

YugabyteDB source
=================

``tap-yugabyte`` extracts tables with full-table, key-based incremental, or
wal2json logical replication, speaking YugabyteDB's PostgreSQL-compatible YSQL
wire protocol.

.. list-table:: Support
   :header-rows: 1
   :widths: 28 24 48
   :width: 100%

   * - Source
     - Status
     - Bulk transfer
   * - YugabyteDB
     - Experimental
     - FullSync to PostgreSQL or Snowflake; PartialSync to Snowflake

.. note::

   YugabyteDB does not support managed Iceberg targets yet.

LOG_BASED works only with PostgreSQL and Snowflake native targets. Only
FastSync creates the replication slot, and other targets, such as S3 CSV, have
no YugabyteDB FastSync route. PipelineWise rejects LOG_BASED tables on those
targets. Use FULL_TABLE or INCREMENTAL there.


Prerequisites
-------------

YSQL speaks the PostgreSQL wire protocol on port **5433**, not 5432.

The runtime user needs ``CONNECT`` on the database, ``USAGE`` on each source
schema, and ``SELECT`` on replicated tables. Grant default privileges if future
tables must be discovered automatically. Discovery reads
``information_schema``, which is privilege-filtered: a table the user cannot
read is silently absent from the catalog rather than reported as an error.

Tables without ``replication_method`` use LOG_BASED, as with tap-postgres.
LOG_BASED replication also requires:

- port ``5433``, the YSQL default (see the warning below);
- the `wal2json <https://github.com/eulerto/wal2json>`_ output plugin, which
  ships pre-packaged with YugabyteDB;
- the ``REPLICATION`` role attribute, to create, consume, and drop the
  replication slot;
- ``SET`` on the ``yb_read_time`` parameter. FastSync uses it to pin the
  initial snapshot to the slot's start point. Without it, the first sync fails
  with ``permission denied to set parameter "yb_read_time"``; and
- a replica identity other than ``NOTHING`` on every table. PostgreSQL targets
  need ``DEFAULT`` or ``FULL``. See :ref:`yugabyte_replica_identity`.

.. warning::

   LOG_BASED replication works only on port 5433. The current YugabyteDB
   psycopg2 drivers, ``psycopg2-yugabytedb-binary`` 2.9.3.5 and
   2.9.3.5.post1, ignore the configured ``port`` on replication connections
   and always connect to 5433. PipelineWise rejects LOG_BASED tables on any
   other port when it loads the configuration, and the tap stops before it
   reads any data. If the cluster is reachable only on another port, set
   FULL_TABLE or INCREMENTAL on every table.

The following grants are enough for every replication method, FastSync, and
data-diff. Run the first block as a superuser and the second as the owner of
each source schema:

.. code-block:: sql

   CREATE ROLE pipelinewise LOGIN REPLICATION PASSWORD '<password>';
   GRANT CONNECT ON DATABASE <database> TO pipelinewise;
   GRANT SET ON PARAMETER yb_read_time TO pipelinewise;

   GRANT USAGE ON SCHEMA <schema> TO pipelinewise;
   GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO pipelinewise;
   ALTER DEFAULT PRIVILEGES FOR ROLE <owner> IN SCHEMA <schema>
   GRANT SELECT ON TABLES TO pipelinewise;

Omit ``REPLICATION`` and the ``yb_read_time`` grant only when every table sets
FULL_TABLE or INCREMENTAL. The user does not need superuser, object
ownership, or write privileges.

.. warning::

   Table privileges do not limit logical replication. The slot streams
   changes from every table in the database, including tables the user cannot
   ``SELECT``. The tap discards unselected tables only after receiving them.
   Replicate from a database that holds only data the pipeline may read.

Only FastSync creates the replication slot. It creates a ``wal2json`` slot
with LSN type ``HYBRID_TIME`` on the first sync. Slot names follow
``pipelinewise_<dbname>_<tap_id>``.

PipelineWise drops the slot when the tap's configuration is removed. By default,
YugabyteDB reports a slot as active for five minutes after its last consumer
disconnects. PipelineWise retries a "slot is active" error for up to five and a
half minutes to wait out that window.

FastSync and the tap fail if the slot's plugin is not ``wal2json`` or its
``yb_lsn_type`` is not ``HYBRID_TIME``. A slot created without the
``'HYBRID_TIME'`` argument gets YugabyteDB's default ``SEQUENCE`` type, which
would stream no changes. Drop the slot and rerun the unfiltered ``fast_sync``
to recreate it.

YugabyteDB LSNs are HybridTime values, not comparable byte offsets, so they
are only meaningful relative to their own slot.

An unfiltered ``fast_sync`` of a tap with LOG_BASED tables resets the slot.
It backs up and clears every bookmark, then drops and recreates the slot once
before workers start. ``--tables``, a non-default ``--replication_method_only``,
automatic initial loads, and standalone PartialSync keep the slot. See
:ref:`resync_postgres_slot_reset`.


Configuration
-------------

.. code-block:: yaml

   id: "orders"
   name: "Orders YugabyteDB"
   type: "tap-yugabyte"
   owner: "data-platform@example.com"
   db_conn:
     host: "<HOST>"
     port: 5433
     user: "<USER>"
     password: "{{ env_var['YUGABYTE_PASSWORD'] }}"
     dbname: "orders"
   target: "snowflake"
   batch_size_rows: 20000
   stream_buffer_size: 0
   schemas:
     - source_schema: "public"
       target_schema: "repl_orders"
       tables:
         - table_name: "payments"
           replication_method: "LOG_BASED"

.. list-table:: Connector-specific settings
   :header-rows: 1
   :widths: 27 18 18 37
   :width: 100%

   * - Setting
     - Required
     - Default
     - Effect
   * - ``filter_schemas``
     - No
     - All visible schemas
     - Limits discovery to a comma-separated schema list.
   * - ``max_run_seconds``
     - No
     - ``43200``
     - Stops a logical replication run after this duration.
   * - ``logical_poll_total_seconds``
     - No
     - ``300``
     - Stops a logical replication run after this many seconds without a
       WAL message. ``0`` disables the idle stop.
   * - ``break_at_end_lsn``
     - No
     - ``true``
     - Stops at the first WAL message past the HybridTime boundary captured
       before streaming.
   * - ``ssl``
     - No
     - Connector default
     - Uses YSQL ``sslmode=require`` when set to ``"true"``.
   * - ``load_balance``
     - No
     - Off
     - Enables YugabyteDB's client-side load balancing across cluster nodes
       for this connection. ``"true"`` distributes connections uniformly;
       ``"any"`` is required together with ``topology_keys`` for
       topology-aware placement. Accepts ``"true"``, ``"false"``, ``"any"``,
       ``"only-rr"``, ``"only-primary"``, ``"prefer-rr"``, and
       ``"prefer-primary"``; any other value fails at connection time.
   * - ``topology_keys``
     - No
     - None
     - Comma-separated ``cloud.region.zone[:preference]`` list restricting
       ``load_balance: "any"`` to the given placements. Ignored unless
       ``load_balance`` is set.
   * - ``itersize``
     - No
     - ``20000``
     - Server-side cursor fetch size for non-FastSync scans.
   * - ``keyset_buckets``
     - No
     - ``3``
     - Number of hash buckets in this tap's keyset scans, from ``2`` to
       ``1024``. It must match the bucket count of every table's
       `Required indexes`_. Changing it means rebuilding those indexes.
   * - ``limit``
     - No
     - Unlimited
     - Bounds rows returned by an incremental query.
   * - ``debug_lsn``
     - No
     - ``false``
     - Adds the current LSN as an automatic stream property when set to
       ``"true"``.
   * - ``fastsync_parallelism``
     - No
     - CPU count
     - Controls concurrent FastSync table exports.

``load_balance`` and ``topology_keys`` apply to every replication connection
opened for this tap, including FastSync FullSync/PartialSync bulk exports and
LOG_BASED streaming, since both connect through YugabyteDB's
native-load-balancing psycopg2 driver.

Four features run inside PipelineWise itself, which uses the stock psycopg2
driver. They ignore both options and connect directly to the configured
``host``:

- The replication-slot cleanup that runs when a tap's configuration is removed
  and ``pipelinewise import_config`` drops its slot. Keep that host reachable,
  or drop the slot manually if it is not.
- The slot reset that runs before an unfiltered ``fast_sync``.
- ``pipelinewise tap_info``.
- :ref:`data_diff` checks.

Common tap settings are documented in :ref:`yaml_configuration`.

Decimals
--------

``NUMERIC(p,s)`` and ``DECIMAL(p,s)`` keep their precision and scale on
Snowflake native and PostgreSQL targets. This applies to every replication
method and to FullSync and PartialSync. Values and decimal bookmarks travel as
exact strings, so no digits are lost to floating point.

Numeric primary keys stay fixed-point on Snowflake when their declaration fits.
YugabyteDB rejects ``NaN`` in decimal columns, so these keys never need the
text mapping that PostgreSQL keys use. See :ref:`exact_decimal_mapping` for the
fallback used by wider or unconstrained declarations.

Arrays are discovered as array columns and are synced. FullSync loads them
into Snowflake ``VARIANT`` or PostgreSQL ``JSONB``. Enums are discovered as
strings and are synced. Composite types and other user-defined types are
discovered with inclusion ``unsupported`` and excluded from the sync. ``hstore`` is
available on YugabyteDB and is exported as-is; it is not mapped to
Snowflake ``VARIANT``, unlike tap-postgres's explicit Iceberg v3 route.


Required indexes
-----------------

**The tap never creates indexes.** YugabyteDB shards a primary key by hash
unless it was declared ``ASC``/``DESC``. A hash-sharded key has no order to scan
along, so every ordered, resumable read by the tap needs an index to supply one.
Without it the tap still returns correct rows, but it reads the whole table on
every run and every resume. A ``FULL_TABLE`` scan logs ``Falling back to a
non-resumable full scan``. An ``INCREMENTAL`` read gives no sign in the query
plan or the log.

Which tables need one depends on the target:

- On PostgreSQL and Snowflake targets, FastSync copies ``FULL_TABLE`` tables
  and ``LOG_BASED`` initial snapshots with a plain ``COPY``. They need no
  index. The change stream needs none either.
- ``INCREMENTAL`` tables, PartialSync, and data-diff need the replication-key
  index on every target.
- On a target without FastSync, such as S3 CSV, the tap scans ``FULL_TABLE``
  tables itself. They need the ``<table>_pw_keyset`` index.

There are four, and they are one shape: the **primary key** hashed into ``N``
buckets ``ASC``, then the ordering column, then the **primary key** again,
``UNIQUE``, split one bucket per tablet.

.. list-table::
   :header-rows: 1
   :widths: 26 30 44

   * - Shape
     - Index
     - Enables
   * - ``bucket, PK``
     - ``<table>_pw_keyset``
     - ``FULL_TABLE`` scanned by the tap (targets without FastSync), with
       parallel buckets and resume
   * - ``bucket, PK`` monotonic
     - *the same index*
     - ``INCREMENTAL`` where the replication key is the primary key
   * - ``bucket, created_at, PK``
     - ``<table>_<column>_pw_keyset``
     - ``INCREMENTAL`` (new rows only), PartialSync, data-diff on that column
   * - ``bucket, updated_at, PK``
     - ``<table>_<column>_pw_keyset``
     - ``INCREMENTAL`` (inserts and updates), PartialSync, data-diff on that column

``singer-connectors/tap-yugabyte/INDEXES.md`` has the exact DDL, the rules to
follow, and what each mistake costs.

In a colocated database, the index has no ``SPLIT AT VALUES`` and shares the
colocation tablet. YugabyteDB rejects split options on colocated indexes. For a
partitioned table, create the index on the parent table and select the parent.

Check a tap config against a live source before running it. The
``tap-yugabyte-index-check`` command installs with the connector:

.. code-block:: bash

    tap-yugabyte-index-check tap_yugabyte.yml \
      --host <host> --port 5433 --user <user> --dbname <db>

It prints ``OK``, ``ACTION`` (with the exact ``CREATE UNIQUE INDEX`` to run) or
``BLOCK`` per table, and exits non-zero unless every table is ``OK``. It reads
``keyset_buckets`` and ``limit`` from the YAML's ``db_conn``. ``--buckets`` and
``--limit`` override them. A table without ``replication_method`` is checked as
``LOG_BASED``, the PipelineWise default for this tap.

The check assumes a PostgreSQL or Snowflake target. Pass
``--target-type target-s3-csv`` for an S3 CSV target. Then ``FULL_TABLE``
tables need ``<table>_pw_keyset``, and ``LOG_BASED`` tables are blocked. A YAML
with ``target_table_format: iceberg`` blocks every table.


Replication behaviour
----------------------

``INCREMENTAL`` requires ``replication_key``. The bookmark is compared
inclusively (``>=``), so the boundary row is re-emitted on every run and the
target must deduplicate on the primary key. Deletes are not captured.

On PostgreSQL and Snowflake targets, FastSync copies every ``FULL_TABLE`` table
on every run. On a target without FastSync, the tap scans the table itself. It
resumes an interrupted scan with primary-key keyset pagination, because
YugabyteDB has no row version like PostgreSQL's ``xmin``. The scan needs the
``<table>_pw_keyset`` index; see `Required indexes`_. Tables without a declared
primary key fall back to a plain, non-resumable full scan.

FastSync loads a ``LOG_BASED`` table's initial snapshot. It pins the export to
the replication slot's restart point with ``SET yb_read_time``, and saves that
point as the table's bookmark. The snapshot and the change stream therefore
meet exactly. A PartialSync with an end value saves no replication position, so
its export reads current data instead. Views and materialized views are
rejected for ``LOG_BASED``.

An update that changes a primary key streams as a delete of the old key and an
insert of the new key. The target removes the row under the old key.

``LOG_BASED`` does not replicate ``TRUNCATE``. After a ``TRUNCATE``, resync the
table:

.. code-block:: bash

   pipelinewise fast_sync --tap <tap_id> --target <target_id> --tables <schema.table>

A filtered ``fast_sync`` like this one also repairs a damaged target table. It
keeps the slot and exports the table as it was at the slot's restart point. It
saves that point as the table's bookmark. The next run replays every later
change, including deletes and key changes, so the target ends up matching the
source. The export reads history at the restart point. YugabyteDB keeps that
history while the slot exists, even past
``timestamp_history_retention_interval_sec``. A slot that nothing consumes for
``cdc_intent_retention_ms`` (default eight hours) expires. Then run the
unfiltered ``fast_sync`` to create a new slot.

.. _yugabyte_large_initial_loads:

Large initial loads
'''''''''''''''''''

Nothing reads the replication slot while FastSync runs. Streaming starts only
after every table in the run is loaded. If that takes longer than
``cdc_intent_retention_ms`` (default eight hours), the slot expires before the
tap can use it. You then have to run the unfiltered ``fast_sync`` again.

A table's copy cannot resume. If it fails, FastSync copies that table again
from the start. Each table is copied by one statement, so more workers do not
speed up one large table. ``fastsync_parallelism`` only loads more tables at
once.

Before loading tables of hundreds of gigabytes:

1. Add the largest tables a few at a time. Add one batch to the tap YAML, run
   ``import_config`` and ``run_tap``, and wait until the tap streams. The next
   batch then copies from a slot the tap reads on every run, so the slot stays
   current.
2. If one batch can still take longer than eight hours, ask the DBA to raise
   both retention flags on every YB-TServer for the load:

   - ``cdc_intent_retention_ms``, for example ``172800000`` for 48 hours; and
   - ``cdc_wal_retention_time_secs``, for example ``172800``.

   ``cdc_wal_retention_time_secs`` multiplied by 1000 must be at least
   ``cdc_intent_retention_ms``. Both flags can be changed without a restart.
3. Watch tablet disk while the flags are raised. Every table in the database
   keeps its write-ahead log and intents for the whole window, not only the
   selected tables.
4. Set both flags back to their previous values once the tap streams. A
   scheduled tap reads the slot on every run.
5. For tables with ``sync_start_from``, create the index on that column. The
   first copy then reads only the selected rows. See `Required indexes`_.
6. Give the PipelineWise host enough temporary disk for the largest export.
   Set ``split_large_files`` on the Snowflake target to write it as several
   files.

.. _yugabyte_replica_identity:

Replica identity
''''''''''''''''

A table's replica identity decides what YugabyteDB streams for an update.
YugabyteDB fixes it for each table when the replication slot is created.

.. list-table::
   :header-rows: 1

   * - Replica identity
     - What an update streams
   * - ``CHANGE`` (YugabyteDB default)
     - The primary key and the changed columns only.
   * - ``DEFAULT`` or ``FULL``
     - The whole new row.
   * - ``NOTHING``
     - Nothing. Deletes also lose their key values, so the tap refuses to start.

Snowflake targets keep the current value of every column an update leaves out.
PostgreSQL targets would write ``NULL`` into those columns. So for a PostgreSQL
target, every ``LOG_BASED`` table needs ``REPLICA IDENTITY DEFAULT`` or
``FULL``. PipelineWise checks this before FastSync creates or resets the slot,
and the tap checks it again at the start of every run. Either check stops with
the names of the tables to fix. Set ``DEFAULT`` before the first ``fast_sync``.

Prefer ``DEFAULT``. With ``FULL``, a transaction that inserts a row and then
updates it can stop the change stream for every table, with the error
``Failed to get the beforeimage``.

Changing the replica identity of a replicated table has no effect until the slot
is recreated. The checks read the table's current setting, so they cannot see
the identity an existing slot holds. Stop replication, run ``ALTER TABLE
<schema.table> REPLICA IDENTITY DEFAULT``, then run the unfiltered
``fast_sync``.


.. _yugabyte_resync_size_limit:

Resync size limit
-----------------

``allowed_resync_max_size.table_mb`` blocks a FullSync to Snowflake when a
selected table is larger than the limit. ``--force`` bypasses it. The limit
does not apply to PostgreSQL targets, the same as for PostgreSQL sources.

PipelineWise reads the size with ``pg_table_size`` plus ``pg_indexes_size``.
A partitioned table includes every partition, at every level. A selected
partition reports its own size. YugabyteDB stores data
differently from PostgreSQL, so the same functions measure different things:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Aspect
     - PostgreSQL
     - YugabyteDB
   * - What is counted
     - On-disk table, TOAST, and index storage. Only large values are
       compressed.
     - Compressed data files (SST) plus the write-ahead log (WAL), for the
       table and its secondary indexes.
   * - Copies counted
     - The single server's copy.
     - One copy, read from the leader replicas. The replication factor does not
       change it.
   * - Stability
     - Stable. Roughly tracks the CSV that FastSync exports.
     - Changes with recent writes, flushes, and WAL clean-up.

What to expect
''''''''''''''

A YugabyteDB table's size moves as it is written, flushed, and cleaned up.
Compared with the CSV that FastSync exports, expect these readings:

.. list-table::
   :header-rows: 1
   :widths: 32 26 42

   * - Table state
     - Size reads
     - Why
   * - Just after a large load
     - Up to 2 times the export
     - The WAL still holds a copy of every write.
   * - Settled, data compresses poorly
     - About 1.3 times the export
     - Compressed data plus the WAL each tablet always keeps.
   * - Settled, data compresses well
     - Can be well below the export
     - Repetitive data can compress about 3.5 to 1.

A table settles once its data is flushed and about 15 minutes have passed
(``log_min_seconds_to_retain``). Each tablet still keeps at least two 64 MB WAL
segments by default (``log_min_segments_to_retain`` and
``log_segment_size_mb``). So allow up to 128 MB per tablet on top of the data.

Readings to double-check
''''''''''''''''''''''''

- **0 or close to 0.** A table loaded in the last minute can read 0, because
  YugabyteDB refreshes sizes by heartbeat. Secondary indexes built after a bulk
  load read close to 0 until they are flushed. Check again a few minutes later.
- **Higher than expected.** Tables in a CDC stream keep WAL for longer
  (``cdc_wal_retention_time_secs``).
- **No size.** Colocated tables report none, and views have none. The check
  blocks these tables, and any table it cannot find. Use ``--force`` to resync
  them.

Setting the limit
'''''''''''''''''

1. Run ``pipelinewise tap_info --tap <tap_id>`` and read each table's
   ``size_bytes``. This is the figure the check uses. It is ``null`` for
   colocated tables and views.
2. Set ``table_mb`` to the largest table you are willing to resync.
3. Add headroom. A table can read up to twice its export size just after a
   large load.


Acknowledgement and recovery
-----------------------------

Consuming WAL does not by itself advance the slot's safe flush position.
The tap sends feedback only up to the minimum target-acknowledged LSN stored in
``state.json``. Missing, unreadable, invalid, or regressing state
retains the previous safe LSN.

The feedback is two below that LSN. A bookmark is a transaction's commit record
LSN, and YugabyteDB can give the next transaction that same LSN. Feedback at the
bookmark itself would confirm that next transaction before the target has it.
The lower feedback confirms only earlier transactions. YugabyteDB also confirms
the position a run starts streaming from, so each run starts from the same lower
LSN. After a restart, the last acknowledged transaction replays, and the target
applies it again by primary key.

YugabyteDB does not stream ``pg_logical_emit_message`` records, so an idle
source sends nothing that can end a run. A run stops after
``logical_poll_total_seconds`` (default 300) without a WAL message and keeps
its bookmarks. Commits on unselected tables in the same database count as
WAL messages. With ``0``, an idle run lasts until ``max_run_seconds``.

On an idle source the slot does not move. The next run that receives a WAL
message first acknowledges the committed bookmark from its input state.

After an unexpected termination, restart the same tap without advancing
state. Unacknowledged changes remain replayable while the slot exists.
Checkpoints advance only after the tap consumes a transaction's commit record.
A completed transaction
can checkpoint even if no later message arrives. If a transaction is interrupted,
the tap retains the last consumed commit, or each stream's starting checkpoint
when no commit was consumed, without regressing below a newer target acknowledgement.
Restarting can therefore replay rows that the target already received.
If a WAL message contains invalid JSON, UTF-8, or an invalid wal2json row message
shape, the tap fails at that message and retains the last complete transaction
checkpoint. Its error identifies the LSN, replication slot, and tap without
printing the row payload. Row messages require a schema, table, and nonempty
columns (inserts and updates) or identity (deletes); each entry requires a name
and a value, which may be null. Investigate the source or output plugin, then
restart with the same state and slot after correcting the cause; do not manually
advance the bookmark past the failed transaction.

Resync only when the slot is unavailable. Run the unfiltered ``fast_sync``
so every LOG_BASED table gets a bookmark from the new slot. Monitor
``ysql_cdc_active_replication_slot_window_ms`` (default five minutes) when
tearing down a tap: dropping a slot immediately after the last consumer
disconnects can transiently fail with "slot is active." See
:ref:`stream_buffering` and :ref:`troubleshooting`.
