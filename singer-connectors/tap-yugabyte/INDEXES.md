# Required indexes for tap-yugabyte

The tap never creates indexes. Create the ones a table needs before you add the
table to a tap config.

Without them the tap still returns correct rows. It reads the whole table to do
it, on every run and every resume.

## When you need which index

Which indexes a table needs depends on the target and the replication method.

**PostgreSQL and Snowflake targets** have a FastSync route.

- FastSync copies every `FULL_TABLE` table on every run. It also loads every
  `LOG_BASED` initial snapshot. It uses a plain `COPY (SELECT ...)` and reads no
  keyset index.
- The `LOG_BASED` change stream reads no index.
- So `FULL_TABLE` and `LOG_BASED` tables need no index from this document.

**Targets without FastSync**, such as S3 CSV, have no FastSync route.

- The tap scans `FULL_TABLE` tables itself. Each table needs
  `<table>_pw_keyset` (shape 1).
- PipelineWise rejects `LOG_BASED` tables on these targets. Only FastSync
  creates the replication slot.

**On every target:**

- `INCREMENTAL` runs through the tap after the initial load. It needs the
  replication-key index: shape 3 or 4, or shape 2 when the key is the primary
  key.
- PartialSync (Snowflake targets) and data-diff need the index on their
  timestamp column: shape 3 or 4.

YugabyteDB does not support managed Iceberg targets yet.

| Table | Index |
|---|---|
| `FULL_TABLE` or `LOG_BASED`, PostgreSQL or Snowflake target | none |
| `FULL_TABLE`, target without FastSync | shape 1, `<table>_pw_keyset` |
| `INCREMENTAL` on the primary key | shape 2, which is the same index as shape 1 |
| `INCREMENTAL` on a timestamp | shape 3 or 4 for that column |
| PartialSync or data-diff on a timestamp | shape 3 or 4 for that column |
| No primary key | none possible. The tap scans it in one non-resumable pass. The preflight reports `BLOCK` for tap-scanned methods |

If you run the tap standalone with `LOG_BASED`, its own initial scan uses the
`FULL_TABLE` path and needs shape 1.

### Why the tap needs an index at all

The tap pages by **bucket**, not by the primary key. It splits the table into N
groups of `yb_hash_code(<pk>) % N`. Each group goes to one worker.

That expression is not stored on the table. No table can be read in its order,
so every ordered, resumable read the tap performs needs an index to supply the
order.

This holds whatever the sharding. A hash-sharded primary key has no order to
scan along. A range-sharded key has an order on the key, but the tap pages on
the bucket.

---

## One shape

Every index below has the same shape:

```sql
CREATE UNIQUE INDEX <name> ON <schema>.<table>
  (((yb_hash_code(<primary key columns>) % N)) ASC,   -- the bucket
   <ordering column>,                                 -- omitted for shapes 1 and 2
   <primary key columns> ASC)                         -- always last
  SPLIT AT VALUES ((1), (2), ... (N-1));
```

`N` is `keyset_buckets` in the tap config. The default is 3.

Do not vary these rules. Each one fails quietly if you do.

| Rule | If you deviate |
|---|---|
| The bucket hashes the **primary key**, never the ordering column | Shapes 1 and 2: a worker resumes from its primary-key bookmark, so the bucket must be computable from it. Shapes 3 and 4: see below |
| `ASC` on the bucket, never `HASH` | `SPLIT AT VALUES` is rejected. Buckets land on tablets at random and workers contend |
| The primary key is the **last** thing in the index | The order is no longer total. With `UNIQUE`, `CREATE INDEX` fails instead |
| `UNIQUE` | Costs nothing because the primary key trails. It turns a missing trailing primary key into a failed `CREATE INDEX` |
| `SPLIT AT VALUES` with N−1 boundaries | The index gets one tablet. All N buckets share it and the workers contend |
| One N for the index and the tap config | The scan names bucket values the index does not have. See [What it costs to get wrong](#what-it-costs-to-get-wrong) |

### Why shapes 3 and 4 hash the primary key

`INCREMENTAL` reads every bucket, so either column would work for reads. Writes
are different. `now()` returns the transaction start time. Every row written in
one transaction gets the same timestamp, and one timestamp hashes to one bucket:

```sql
-- 9,000 rows inserted in ONE transaction, counted per bucket
bucket = yb_hash_code(id)         -> 2960 / 2955 / 3085
bucket = yb_hash_code(updated_at) -> 9000 /    0 /    0
```

A bulk load or batch job would put its whole batch on one tablet. Primary keys
are distinct, so they spread evenly. One expression and one N also cover every
index on the table.

---

## The four indexes

### 1 · `bucket, PK`

```sql
CREATE UNIQUE INDEX <table>_pw_keyset ON <schema>.<table>
  (((yb_hash_code(<pk>) % 3)) ASC, <pk> ASC)
  SPLIT AT VALUES ((1), (2));
```

The primary key does not need to be monotonic, sequential or numeric. A `uuid`,
a `text` or a composite `(tenant_id, id)` all work.

Required when the tap scans a table with `FULL_TABLE`. That happens on a target
without FastSync, or when you run the tap standalone. The sharding of the
primary key makes no difference.

It enables:

- **Parallel export**: N workers, one per bucket, each on its own connection.
- **Resume**: each bucket bookmarks its own position. An interrupted run
  restarts each bucket from the last row that reached the target.

A composite primary key resumes with one statement per key prefix, so every
resume seeks directly to its bookmark.

Without a usable index, the tap logs a warning and falls back to one
non-parallel, non-resumable full scan.

### 2 · `bucket, PK` where the primary key is monotonic

Shape 2 is the same index as shape 1. Do not create a second one.

The difference is a property of your data. It lets you use `INCREMENTAL` with
the primary key as `replication_key`. See
[INCREMENTAL caveats](#incremental-caveats) before you do.

### 3 · `bucket, created_at, PK`

```sql
CREATE UNIQUE INDEX <table>_created_at_pw_keyset ON <schema>.<table>
  (((yb_hash_code(<pk>) % 3)) ASC, created_at ASC, <pk> ASC)
  SPLIT AT VALUES ((1), (2));
```

Any column name works. The index name follows the column:
`<table>_<column>_pw_keyset`.

It enables:

- `INCREMENTAL` on that column. It sees **new rows only**.
- PartialSync bounded by that column.
- data-diff with that column as `timestamp_column`.

A creation timestamp never changes after insert. So updates to existing rows are
invisible to `INCREMENTAL`.

### 4 · `bucket, updated_at, PK`

```sql
CREATE UNIQUE INDEX <table>_updated_at_pw_keyset ON <schema>.<table>
  (((yb_hash_code(<pk>) % 3)) ASC, updated_at ASC, <pk> ASC)
  SPLIT AT VALUES ((1), (2));
```

The DDL is identical to shape 3. The difference is what the column records.

It enables:

- `INCREMENTAL` on that column. It sees **inserts and updates**.
- PartialSync bounded by that column.
- data-diff with that column as `timestamp_column`.

A column named `updated_at` does not maintain itself. With only
`DEFAULT now()`, it is set on insert and never moves again. Every later update
is missed. Only a row-level `UPDATE` trigger keeps it current:

```sql
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END $$ LANGUAGE plpgsql;

CREATE TRIGGER <table>_touch BEFORE UPDATE ON <schema>.<table>
  FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
```

The preflight reports whether the table has such a trigger. It cannot tell which
column the trigger sets. Confirm it fires on every write path.

Neither shape 3 nor shape 4 sees **deletes**. Only `LOG_BASED` does.

---

## Feature matrix

"Tap-scanned" means a target without FastSync, or the tap run standalone.

| | **1** `bucket, PK` | **2** `bucket, PK` monotonic | **3** `bucket, created_at, PK` | **4** `bucket, updated_at, PK` |
|---|:---:|:---:|:---:|:---:|
| Index to create | `<t>_pw_keyset` | *same index as 1* | `<t>_<col>_pw_keyset` | `<t>_<col>_pw_keyset` |
| `FULL_TABLE` through FastSync | *no index needed* | | | |
| `LOG_BASED` initial snapshot and change stream | *no index needed* | | | |
| Tap-scanned `FULL_TABLE`, parallel and resumable | ✅ | ✅ | — | — |
| `INCREMENTAL` | — | ✅ key = PK | ✅ key = that column | ✅ key = that column |
| ⮑ sees inserts | — | ✅ | ✅ | ✅ |
| ⮑ sees updates | — | ❌ | ❌ | ✅ *trigger required* |
| ⮑ sees deletes | — | ❌ | ❌ | ❌ |
| PartialSync | — | ✅ on the PK | ✅ | ✅ |
| data-diff `timestamp_column` | — | — | ✅ | ✅ |

---

## INCREMENTAL caveats

The preflight checks these for every `INCREMENTAL` table. The tap does not check
them at runtime.

### Nullable keys

A `NULL` key is never written to the bookmark. Rows with a `NULL` key re-sync on
every run. The preflight reports a nullable replication key as `BLOCK`.

A non-unique key on a table with no primary key is also `BLOCK`. Nothing breaks
ties, so paging is not deterministic.

### Sequence cache

YugabyteDB hands out sequence values in per-connection blocks. The default
block is `100`. One session can commit ids 1–100 while another commits
101–200.

A run that bookmarks 150 never sees id 40 if it commits a moment later. The
row is lost to `INCREMENTAL`.

The tap has no lag window to reread those rows. Lower the sequence `CACHE` to 1,
or use `LOG_BASED`. The preflight warns when the cache is larger than 1. It also
warns when the column uses a sequence through `DEFAULT nextval(...)` without
owning it, because the sequence can then change with no change to the column.

### Long write transactions

A timestamp set by `now()` records when the transaction started, not when it
committed. If a run advances the bookmark past that time before the transaction
commits, the rows are lost to `INCREMENTAL`.

Measured: a transaction open for 4.26 s had all 500 of its rows missed.

The tap has no lag window. If your writers hold long transactions, use
`LOG_BASED`.

### Resuming inside a tie group

`INCREMENTAL` state holds one value, `replication_key_value`. A run reads
`WHERE key >= <bookmark>`, so a resume rereads every row that shares the
bookmarked value. The target deduplicates on the primary key, so this is not a
correctness problem.

It becomes one when a tie group is larger than one run can drain. Each run reads
`limit` rows that all carry the same value. The bookmark does not move, and the
next run issues the same statement. The sync never advances.

Large tie groups are common after a bulk load, because one transaction stamps
every row with the same `now()`.

**Before the run**, the preflight estimates the largest tie group from
`pg_stats`. It reads no table data. With `limit` set, a group of `limit` rows or
more is a certain stall, so it reports `BLOCK`:

```
BLOCK  rt.bulkload  [INCREMENTAL on updated_at]
       - 39,995 of 40,050 rows (100%) share one updated_at value, estimated from
         most_common_vals -- at or above the configured limit of 10,000. ...
```

Without `limit`, a run drains to the end and always advances. The preflight then
reports large groups as a risk note. A column with no statistics is reported as
unknown. Run `ANALYZE` and check again.

**During the run**, the tap logs a warning when a run hits `limit` and the
bookmark ends where it started:

```
INCREMENTAL sync of rt-bulkload MADE NO PROGRESS and cannot make any: all 10000
rows it emitted carry the same updated_at ('2026-09-22T14:21:22.858584+00:00'),
the run stopped on LIMIT 10000, and the bookmark ends where it started. ...
```

To fix it, raise `limit` above the group size, choose a key with higher
cardinality, or use `LOG_BASED`.

---

## Choosing N

`N` is `keyset_buckets`, from 2 to 1024. The default is 3.

N fixes the index expression, the tablet count and the maximum parallelism of a
tap-scanned `FULL_TABLE`. A worker owns exactly one bucket. Changing N means
rebuilding every index.

If you change N while a `FULL_TABLE` scan is part-way through, the tap discards
its per-bucket bookmarks with a warning. The table restarts from the beginning.

Consider these limits:

- **Upper bound.** Merging held at every N tested up to 512. Building an index
  with 1024 tablets did not finish in 20 minutes.
- **`INCREMENTAL` memory grows with N.** It reads all N buckets through one
  cursor. Measured on a three-column table: 161 kB at N = 3, 1.5 MB at N = 32,
  6 MB at N = 128.
- **Wide tables need a larger N.** Peak memory per `FULL_TABLE` worker scales
  with rows in the bucket times row width. Measured on a 50-column table at
  ~4.8 kB per row: 50 MB per worker at N = 3, 13 MB at N = 12. A wide table with
  500k rows in one bucket would use about 2.5 GB.
- **Skew removes parallelism.** Ordinary keys spread evenly. A pathological key
  set can put most rows in one bucket. The sync is then as slow as that worker.
  Nothing reports it. To check:

```sql
SELECT (yb_hash_code(<pk>) % <N>) AS bucket, count(*) FROM <table> GROUP BY 1 ORDER BY 1;
```

---

## Colocated and partitioned tables

In a colocated database every index shares the colocation tablet. YugabyteDB
rejects `SPLIT AT VALUES` there. Omit the split clause. The preflight prints the
DDL without it and accepts the single tablet.

For a partitioned table, create the index on the parent and select the parent.

## Create indexes outside a transaction block

YugabyteDB cannot build an index concurrently inside a transaction:

```
NOTICE:  making create index for table "t" nonconcurrent
DETAIL:  Create index in transaction block cannot be concurrent.
```

When this rule was ignored, `pg_index` reported the index valid and ready. The
session that built it saw its rows. A new session saw none, through the index or
the table. The preflight reported `OK`.

Run each `CREATE UNIQUE INDEX` as its own statement. Do not wrap it in
`BEGIN`/`COMMIT` or a migration that batches DDL into one transaction.
YugabyteDB serialises index backfills within a database anyway, so batching
saves nothing.

## Writes during a tap-scanned FULL_TABLE

This applies only when the tap scans `FULL_TABLE` itself. The tap does not pin
its scan to one point in time.

An uninterrupted scan with no retries is close to a consistent image. The
per-bucket statements start within a millisecond of each other. Even so:

- Rows deleted during the scan are still delivered as live rows.
- A row whose primary key changes during the scan arrives once, under its old
  key. The target never receives the new key.

A resumed scan, or one where any bucket retried, reads at more than one point in
time. Read restarts and leader elections make retries normal. Across that
boundary, a row whose primary key moved is delivered twice or not at all. The
sync still reports success. The log shows only the retry warnings.

If a table's primary key is ever updated, do not use tap-scanned `FULL_TABLE`.
Use a PostgreSQL or Snowflake target, where FastSync copies the table.

---

## What it costs to get wrong

Measured on YugabyteDB 2026.1.1.1 with 3 tablets and 3 buckets.

| Mistake | Effect |
|---|---|
| No `<table>_pw_keyset`, or the wrong shape, on a tap-scanned `FULL_TABLE` | The tap logs `Falling back to a non-resumable full scan` and reads the table in one non-parallel pass. An interruption restarts the table |
| No replication-key index on `INCREMENTAL` | Every run reads and sorts the whole table before it emits a row. Nothing in the log says so. For scale: an unindexed ordered read from the midpoint of 50,000 rows scanned all 50,000 and sorted 3,112 kB to return 25,000. The index read 8,391 entries and streamed |
| `keyset_buckets` differs from the index's N | `FULL_TABLE`: the tap logs a warning and falls back as above. `INCREMENTAL`: a full table scan on every run, with no warning |
| `SPLIT AT VALUES` omitted | One tablet instead of N. The preflight reports it |
| Bucket declared `HASH` instead of `ASC` | `SPLIT AT VALUES` is rejected |
| Primary key omitted from the end of the index | `CREATE UNIQUE INDEX` fails with a duplicate-key error. Without `UNIQUE` it succeeds, and scans become a blocking sort instead of a stream |
| Index created inside a transaction block | Other sessions may not see the data. See [above](#create-indexes-outside-a-transaction-block) |

## Reading a plan by hand

The tap sets these on its own scan connections. Set them too if you capture a
plan to compare:

```sql
SET yb_max_merge_scan_streams = <max(N, 8)>;  -- must be at least N
SET enable_seqscan = off;
```

`yb_max_merge_scan_streams` defaults to `0`. Below N the planner sorts instead of
merging the bucket streams.

Run `ANALYZE` on the table first. With no statistics YugabyteDB underestimates
the row count. A small estimate can produce an index scan plus an in-memory
`Sort` even when the index is correct. Do not read that `Sort` as an index
problem.

---

## Checking a config before you run it

`tap-yugabyte-index-check` installs with the connector. Run it against the tap
YAML and the live source:

```bash
tap-yugabyte-index-check tap_yugabyte.yml \
  --host <host> --port 5433 --user <user> --dbname <db> \
  [--buckets N] [--limit N] [--target-type TYPE]
```

The password comes from `--password` or `PGPASSWORD`.

- `--buckets` and `--limit` default to `keyset_buckets` and `limit` under the
  YAML's `db_conn`. `limit` makes the tie-group check exact. See
  [Resuming inside a tie group](#resuming-inside-a-tie-group).
- `--target-type` is `target-postgres`, `target-snowflake` or `target-s3-csv`.
  Without it, the check assumes a PostgreSQL or Snowflake target.
- A table without `replication_method` is checked as `LOG_BASED`, the
  PipelineWise default for this tap.

For each table it prints one of:

- `OK`: nothing to do. The index exists with the right shape, bucket count,
  uniqueness and tablet count, or the table needs no index.
- `ACTION`: the exact DDL to run. If a wrong index exists, it prints the
  `DROP INDEX` first.
- `BLOCK`: the configuration cannot work, with the reason.

What it reports per route:

| Table | PostgreSQL or Snowflake (default) | `--target-type target-s3-csv` |
|---|---|---|
| `FULL_TABLE` | `OK`, no index needed | needs `<table>_pw_keyset` |
| `LOG_BASED` | `OK`, no index needed. It notes the replica identity requirement | `BLOCK` |
| `INCREMENTAL` | needs the replication-key index | needs the replication-key index |
| any, with `target_table_format: iceberg` | `BLOCK` | `BLOCK` |

The `LOG_BASED` replica identity is not checked here. The tap and FastSync check
it at run time. It must not be `NOTHING`. PostgreSQL targets need `DEFAULT` or
`FULL`.

The exit status is 0 only when every table is `OK`.
