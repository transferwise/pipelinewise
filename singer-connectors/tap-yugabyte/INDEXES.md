# Indexes for tap-yugabyte

The tap never creates indexes. Create the ones a table needs before you add it to
a tap config. Run [`tap-yugabyte-index-check`](#check-a-config-before-you-run-it)
to see exactly which ones are missing.

Without the right index the tap still returns correct rows, but it reads the whole
table on every run and every resume.

## Which index a table needs

| Table                                           | Target                           | Index                                                                                                      |
|-------------------------------------------------|----------------------------------|------------------------------------------------------------------------------------------------------------|
| `FULL_TABLE` or `LOG_BASED`                     | PostgreSQL or Snowflake          | None. FastSync copies the table with a plain `COPY`.                                                       |
| `FULL_TABLE`                                    | Without FastSync, such as S3 CSV | `<table>_pw_keyset` (shape 1)                                                                              |
| `LOG_BASED`                                     | Without FastSync                 | Not supported. Only FastSync creates the replication slot.                                                 |
| `INCREMENTAL` on the primary key                | Any                              | `<table>_pw_keyset` (shape 2, the same index as shape 1)                                                   |
| `INCREMENTAL` on a timestamp                    | Any                              | `<table>_<column>_pw_keyset` (shape 3 or 4)                                                                |
| PartialSync or `sync_start_from` on a timestamp | Snowflake                        | `<table>_<column>_pw_keyset` for that column                                                               |
| data-diff `timestamp_column`                    | Any                              | `<table>_<column>_pw_keyset`, or a plain `ASC`/`DESC` index on the column                                  |
| No primary key                                  | Any                              | None possible. `FULL_TABLE` reads it in one pass that cannot resume. The index check blocks `INCREMENTAL`. |

YugabyteDB does not support managed Iceberg targets yet.

## The index shape

Every index here has the same shape:

```sql
CREATE UNIQUE INDEX <name> ON <schema>.<table>
  (((yb_hash_code(<primary key columns>) % N)) ASC,   -- the bucket
   <ordering column> ASC,                             -- omitted for shapes 1 and 2
   <primary key columns> ASC)                         -- always last
  SPLIT AT VALUES ((1), (2), ... (N-1));
```

`N` is `keyset_buckets` in the tap's `db_conn`, from 2 to 1024. The default is 3.

The tap splits a table into N buckets of `yb_hash_code(<primary key>) % N` and
reads each bucket in order. No table stores that order, so the index supplies it.
This is true whether the primary key is hash- or range-sharded.

Keep to these rules. Most mistakes raise no error; the tap falls back to reading
the whole table.

| Rule                                                             | Why                                                                                                                                                                             |
|------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| The bucket hashes the **primary key**, never the ordering column | Each bucket's bookmark is a primary key, so the bucket must be computable from it. Hashing a timestamp would also put a whole batch, which shares one `now()`, into one bucket. |
| The bucket is `ASC`, never `HASH`                                | `SPLIT AT VALUES` is rejected on a `HASH` key.                                                                                                                                  |
| The primary key comes **last**                                   | It makes the order total.                                                                                                                                                       |
| `UNIQUE`                                                         | Costs nothing, and makes a missing trailing primary key fail at `CREATE INDEX`.                                                                                                 |
| `SPLIT AT VALUES` with N−1 boundaries                            | Puts each bucket on its own tablet. Without it all buckets share one.                                                                                                           |
| The same N in the index and the tap config                       | Otherwise the index cannot serve the tap's scan.                                                                                                                                |

### Shapes 1 and 2: `bucket, PK`

```sql
CREATE UNIQUE INDEX <table>_pw_keyset ON <schema>.<table>
  (((yb_hash_code(<pk>) % 3)) ASC, <pk> ASC)
  SPLIT AT VALUES ((1), (2));
```

Shapes 1 and 2 are the same index. Create it once.

- **Shape 1**: `FULL_TABLE` when the tap scans the table itself, on a target
  without FastSync. It gives N parallel workers, and each bucket resumes from its
  last row.
- **Shape 2**: `INCREMENTAL` when the replication key is the primary key, such
  as a `bigserial` id. Read [INCREMENTAL risks](#incremental-risks) first.

The primary key can be any type `yb_hash_code` accepts, including `uuid`,
`text`, and composite keys. The index check blocks a key it cannot hash.

### Shape 3: `bucket, created_at, PK`

```sql
CREATE UNIQUE INDEX <table>_created_at_pw_keyset ON <schema>.<table>
  (((yb_hash_code(<pk>) % 3)) ASC, created_at ASC, <pk> ASC)
  SPLIT AT VALUES ((1), (2));
```

Name it `<table>_<column>_pw_keyset`. Use it for `INCREMENTAL`, PartialSync, and
data-diff on that column. A creation time never changes, so `INCREMENTAL` sees new
rows only.

### Shape 4: `bucket, updated_at, PK`

The DDL is the same as shape 3, on the update-time column. `INCREMENTAL` then
sees inserts and updates, but only if the application sets the column on every
update. A `DEFAULT now()` sets it on insert only, so updates that do not set it
are never read.

If the column's name looks like an update time, such as `updated_at` or
`modified_at`, the index check warns when the table has no update trigger. If
the application sets the column on every update, you can ignore that warning.

`INCREMENTAL` never sees deletes. Use `LOG_BASED` for those.

## Building the indexes

- **Run each `CREATE UNIQUE INDEX` on its own.** Do not wrap it in
  `BEGIN`/`COMMIT` or in a migration that batches DDL into one transaction.
  YugabyteDB then builds it non-concurrently, and other sessions may not see its
  rows.
- **Colocated database:** omit `SPLIT AT VALUES`. YugabyteDB rejects it there,
  and all buckets share the colocation tablet.
- **Partitioned table:** create the index on the parent, and select the parent
  in the tap config.
- **Changing N** means rebuilding every index with the new N. If a `FULL_TABLE`
  scan is part-way through, the tap discards its bucket bookmarks with a warning
  and restarts that table.

## Choosing N

N is the number of parallel workers for a tap-scanned `FULL_TABLE`, and the
number of tablets in each index.

- Match N to the cluster. Each bucket is one tablet, and YugabyteDB spreads
  tablet leaders across the nodes that can hold them. Pick N as a multiple of
  that node count, not of the replication factor, so every node gets the same
  number of leaders. For example, use 3, 6, or 9 on a 3-node cluster. The
  default of 3 suits a 3-node cluster.
- Raise N for more parallel workers on a large tap-scanned `FULL_TABLE`. Each
  worker streams its bucket `itersize` rows at a time (default 20,000). Lower
  `itersize` for very wide rows, not N.
- Keep N moderate for `INCREMENTAL`. It merges all N buckets in one query, so a
  very large N adds work to every run.
- Stay at or below 512. Building an index with more tablets than that can take a
  very long time.

Ordinary keys spread evenly across buckets. To check a table:

```sql
SELECT (yb_hash_code(<pk>) % <N>) AS bucket, count(*) FROM <table> GROUP BY 1 ORDER BY 1;
```

## INCREMENTAL risks

The index check reports these for every `INCREMENTAL` table. The tap does not
check them at run time, and it has no lag window to reread rows it passed.

| Risk                                                       | What happens                                                                                                     | What to do                                         |
|------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|----------------------------------------------------|
| Nullable replication key                                   | Rows with `NULL` are never bookmarked and re-sync on every run. The check reports `BLOCK`.                       | Make the column `NOT NULL`.                        |
| No primary key                                             | Nothing to bucket on, and nothing to order rows that share a value. The check reports `BLOCK`.                   | Add a primary key.                                 |
| Sequence with `CACHE` above 1 (YugabyteDB defaults to 100) | Sessions commit ids out of order. A run can bookmark 150 before id 40 commits, and id 40 is never read.          | Set the sequence `CACHE` to 1, or use `LOG_BASED`. |
| Long write transactions on a `now()` timestamp             | `now()` is the transaction's start time. Rows can commit with a timestamp below the bookmark and are never read. | Use `LOG_BASED`.                                   |
| Large groups of rows with the same key value               | See below.                                                                                                       |                                                    |

### Resuming inside a tie group

The bookmark is one value, and each run reads `WHERE key >= <bookmark>`. So each
run rereads every row that shares the bookmarked value. The target deduplicates
by primary key, so this is safe.

It stops progress when a group of equal values is larger than `limit`. Each run
then reads `limit` rows with the same value, the bookmark does not move, and the
next run does the same. Bulk loads cause this, because one transaction stamps
every row with the same `now()`.

- The index check estimates the largest group from table statistics. With
  `limit` set, a group of at least `limit` rows is `BLOCK`. Without `limit`, it is
  a warning, because a run reads to the end and always advances. Run `ANALYZE`
  first if the check reports the statistics as missing.
- At run time, the tap logs `MADE NO PROGRESS` when a run stops on `limit` and
  the bookmark has not moved.
- To fix it, raise `limit` above the group size, choose a key with more distinct
  values, or use `LOG_BASED`.

## Writes during a tap-scanned FULL_TABLE

This applies only when the tap scans `FULL_TABLE` itself, on a target without
FastSync. That scan is not pinned to one point in time.

- Rows deleted during the scan can still arrive as live rows.
- A row whose primary key changes during the scan arrives under its old key.
- After a resume or a bucket retry, such a row can arrive twice or not at all.
  The sync still reports success.

If a table's primary key can change, do not use tap-scanned `FULL_TABLE`. Use a
PostgreSQL or Snowflake target, where FastSync copies the table at one point in
time.

## What goes wrong without the right index

| Mistake                                                     | Effect                                                                                                                        |
|-------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| No usable `<table>_pw_keyset` on a tap-scanned `FULL_TABLE` | The tap logs `Falling back to a non-resumable full scan` and reads the table in one pass. An interruption restarts the table. |
| No replication-key index on `INCREMENTAL`                   | Every run reads and sorts the whole table before it emits a row. Nothing in the log says so.                                  |
| `keyset_buckets` differs from the index's N                 | `FULL_TABLE` falls back as above. `INCREMENTAL` reads the whole table, with no warning.                                       |
| `SPLIT AT VALUES` omitted                                   | One tablet instead of N. The index check reports it.                                                                          |
| Bucket declared `HASH`                                      | `SPLIT AT VALUES` is rejected.                                                                                                |
| Primary key missing from the end                            | `CREATE UNIQUE INDEX` fails with a duplicate-key error when rows share a value.                                               |
| Index built inside a transaction block                      | Other sessions may not see its rows.                                                                                          |

To compare a plan by hand, use the settings the tap uses, and run `ANALYZE` on
the table first. Without statistics a correct index can still show a `Sort`.

```sql
SET yb_max_merge_scan_streams = <max(N, 8)>;
SET enable_seqscan = off;
```

## Check a config before you run it

`tap-yugabyte-index-check` installs with the connector. It reads a tap YAML and
checks every table against the live source:

```bash
tap-yugabyte-index-check tap_yugabyte.yml \
  --host <host> --port 5433 --user <user> --dbname <db> \
  [--buckets N] [--limit N] [--target-type TYPE]
```

- The password comes from `--password` or `PGPASSWORD`.
- `--buckets` and `--limit` default to `keyset_buckets` and `limit` in the YAML's
  `db_conn`. With `limit` known, the tie-group check is exact.
- `--target-type` is `target-postgres`, `target-snowflake`, or `target-s3-csv`.
  Without it, the check assumes PostgreSQL or Snowflake.
- A table without `replication_method` is checked as `LOG_BASED`, the
  PipelineWise default.

It prints one line per table:

- `OK`: nothing to do.
- `ACTION`: the exact DDL to run. If a wrong index exists, it prints the
  `DROP INDEX` first.
- `BLOCK`: the configuration cannot work, with the reason.

| Table                                    | PostgreSQL or Snowflake (default) | `--target-type target-s3-csv`   |
|------------------------------------------|-----------------------------------|---------------------------------|
| `FULL_TABLE`                             | `OK`, no index needed             | Needs `<table>_pw_keyset`       |
| `LOG_BASED`                              | `OK`, no index needed             | `BLOCK`                         |
| `INCREMENTAL`                            | Needs the replication-key index   | Needs the replication-key index |
| Any, with `target_table_format: iceberg` | `BLOCK`                           | `BLOCK`                         |

The command exits 0 only when every table is `OK`.

The check does not test replica identity. The tap and FastSync check it at run
time: it must not be `NOTHING`, and PostgreSQL targets need `DEFAULT` or `FULL`.
