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


Prerequisites
-------------

YSQL speaks the PostgreSQL wire protocol on port **5433**, not 5432. Pointing the
tap at 5432 usually reaches a co-located PostgreSQL instance instead of the
cluster.

The runtime user needs ``CONNECT`` on the database, ``USAGE`` on each source
schema, and ``SELECT`` on replicated tables. Grant default privileges if future
tables must be discovered automatically. Discovery reads
``information_schema``, which is privilege-filtered: a table the user cannot
read is silently absent from the catalog rather than reported as an error.

LOG_BASED replication also requires:

- port ``5433``, the YSQL default (see the warning below);
- the `wal2json <https://github.com/eulerto/wal2json>`_ output plugin, which
  ships pre-packaged with YugabyteDB;
- the ``REPLICATION`` role attribute, to create, consume, and drop the
  replication slot; and
- ``SET`` on the ``yb_read_time`` parameter. The initial snapshot pins this
  setting to the slot's start point. Without it, the first sync fails with
  ``permission denied to set parameter "yb_read_time"``.

.. warning::

   LOG_BASED replication works only on port 5433. The current YugabyteDB
   psycopg2 drivers, ``psycopg2-yugabytedb-binary`` 2.9.3.5 and
   2.9.3.5.post1, ignore the configured ``port`` on replication connections
   and always connect to 5433. PipelineWise rejects LOG_BASED tables on any
   other port when it loads the configuration, and the tap stops before it
   reads any data. If the cluster is reachable only on another port, use
   FULL_TABLE or INCREMENTAL.

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

Omit ``REPLICATION`` and the ``yb_read_time`` grant when no table uses
LOG_BASED. The user does not need superuser, object ownership, or write
privileges. Revoke the ``yb_read_time`` grant before dropping the role.

.. warning::

   Table privileges do not limit logical replication. The slot streams
   changes from every table in the database, including tables the user cannot
   ``SELECT``. The tap discards unselected tables only after receiving them.
   Replicate from a database that holds only data the pipeline may read.

FastSync owns the replication slot's full lifecycle: it creates a
``wal2json`` slot with LSN type ``HYBRID_TIME`` on first sync and drops it
when the tap is removed, retrying a transient "slot is active" error for up
to five minutes to tolerate YugabyteDB's post-disconnect active-slot window.
Slot names follow ``pipelinewise_<dbname>_<tap_id>``.

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

Three features run inside PipelineWise itself, which uses the stock psycopg2
driver. They ignore both options and connect directly to the configured
``host``:

- The replication-slot cleanup that runs when a tap's configuration is removed
  and ``pipelinewise import_config`` drops its slot. Keep that host reachable,
  or drop the slot manually if it is not.
- The slot reset that runs before an unfiltered ``fast_sync``.
- :ref:`data_diff` checks.

Common tap settings are documented in :ref:`yaml_configuration`. Generate the
full template with ``pipelinewise init``.

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

**Every table this tap reads needs a prerequisite index, and the tap will not
create it.** YugabyteDB shards a primary key by hash unless it was declared
``ASC``/``DESC``, and a hash-sharded key has no order to scan along, so every
ordered, resumable read needs an index to supply one. Without it the tap still
returns correct rows -- it reads the whole table to do it, on every run and
every resume, and neither the query plan nor the tap log says so.

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
     - ``FULL_TABLE``, parallel export, resume, ``LOG_BASED`` initial snapshot
   * - ``bucket, PK`` monotonic
     - *the same index*
     - the above, plus ``INCREMENTAL`` where the replication key is the primary key
   * - ``bucket, created_at, PK``
     - ``<table>_<column>_pw_keyset``
     - ``INCREMENTAL`` (new rows only), PartialSync, data-diff on that column
   * - ``bucket, updated_at, PK``
     - ``<table>_<column>_pw_keyset``
     - ``INCREMENTAL`` (inserts and updates), PartialSync, data-diff on that column

The full contract -- exact DDL, the rules that must not be varied, what each
mistake costs, and the feature matrix -- is in
``singer-connectors/tap-yugabyte/INDEXES.md``.

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
``FULL_TABLE``, the PipelineWise default for this tap.


Replication behaviour
----------------------

``INCREMENTAL`` requires ``replication_key``. The bookmark is compared
inclusively (``>=``), so the boundary row is re-emitted on every run and the
target must deduplicate on the primary key. Deletes are not captured.

``FULL_TABLE`` resumes an interrupted sync with parameterized primary-key
keyset pagination, since YugabyteDB's distributed MVCC exposes no
cluster-wide monotonic row version like PostgreSQL's ``xmin``. Tables without
a declared primary key fall back to a plain, non-resumable full scan.

``LOG_BASED`` bootstraps its initial scan from a snapshot pinned to the
replication slot's own restart boundary (``SET yb_read_time``), so the
snapshot and the streaming start position are provably consistent. A
PartialSync with an end value saves no replication position, so its export
reads current data instead. An interrupted bootstrap resumes from a ``bootstrap_in_progress`` bookmark
rather than an ``xmin`` sentinel. Views and materialized views are rejected
for ``LOG_BASED``.

``LOG_BASED`` does not replicate ``TRUNCATE``. An update that changes a primary
key adds a row under the new key, and the row under the old key stays in the
target. After either change, resync the table:

.. code-block:: bash

   pipelinewise fast_sync --tap <tap_id> --target <target_id> --tables <schema.table>


.. _yugabyte_resync_size_limit:

Resync size limit
-----------------

``allowed_resync_max_size.table_mb`` blocks a FullSync to Snowflake when a
selected table is larger than the limit. ``--force`` bypasses it. The limit
does not apply to PostgreSQL targets, the same as for PostgreSQL sources.

PipelineWise reads the size with ``pg_table_size`` plus ``pg_indexes_size``,
summed over a partitioned table's partitions. YugabyteDB stores data
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

The YugabyteDB figure changes over a table's life. In a test with about
540 MB of exported CSV and data that compressed poorly:

- **Right after a large load**, it read about twice the export size. The WAL
  still held a copy of every write, next to the data files.
- **Once settled**, it read about 1.3 times the export size. YugabyteDB removes
  old WAL once data is flushed and about 15 minutes have passed
  (``log_min_seconds_to_retain``). Each tablet keeps at least two WAL segments
  of 64 MB by default (``log_min_segments_to_retain`` and
  ``log_segment_size_mb``), so up to 128 MB per tablet remains.
- **Data that compresses well reads lower.** A settled table reads about its
  compressed data plus that WAL floor. Repetitive data compressed about 3.5 to
  1 in testing, so it can read well below its export size.

Some readings are low or missing:

- A table loaded in the last minute can read 0. YugabyteDB refreshes sizes by
  heartbeat.
- Secondary indexes built after a bulk load can read close to 0 until they are
  flushed.
- Tables in a CDC stream can keep WAL for longer (``cdc_wal_retention_time_secs``),
  so they read higher.
- Colocated tables report no size, and views have none. The check blocks these
  tables. Use ``--force`` to resync them.

Set ``table_mb`` to the largest table you are willing to resync, with
headroom. Expect a table to read up to twice its export size just after a
large load. To see the figure the check uses, run
``SELECT pg_table_size('schema.table') + pg_indexes_size('schema.table')``.
For a partitioned table, add up the results for its partitions.


Acknowledgement and recovery
-----------------------------

Consuming WAL does not by itself advance the slot's safe flush position.
PipelineWise sends feedback only up to the minimum target-acknowledged LSN
stored in ``state.json``. Missing, unreadable, invalid, or regressing state
retains the previous safe LSN.

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
