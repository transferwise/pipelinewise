# Required indexes for tap-yugabyte

**These indexes are a prerequisite, not a tuning suggestion.** Create them before
a table is added to a tap config. Without them the tap still returns correct
rows — it just reads the whole table to do it, on every run and every resume, and
neither the query plan nor the tap log says that happened.

The reason is that YugabyteDB shards a primary key by hash unless it was declared
`ASC`/`DESC`, and a hash-sharded key has no order to scan along. Every ordered,
resumable read the tap performs therefore needs an index to supply an order the
table itself does not have.

---

## One shape, four uses

Every index below is the same shape. Learn it once:

```sql
CREATE UNIQUE INDEX <name> ON <schema>.<table>
  (((yb_hash_code(<primary key columns>) % N)) ASC,   -- the bucket
   <ordering column>,                                 -- omitted for shapes 1 and 2
   <primary key columns> ASC)                         -- always last
  SPLIT AT VALUES ((1), (2), ... (N-1));
```

Six rules. None of them are stylistic; each one fails in its own way if you
deviate, and the failures are quiet.

| Rule | If you deviate |
|---|---|
| The bucket hashes the **primary key**, always — never the ordering column | For shapes 1 and 2 this is a hard requirement: a parallel worker owns one bucket and resumes on its primary-key cursor, so the bucket must be computable from that cursor. For shapes 3 and 4 it is a choice, and the reason is below |
| `ASC` on the bucket, never `HASH` | `SPLIT AT VALUES` is rejected, so bucket→tablet placement is whatever the hash space gives you, and workers contend |
| The primary key is the **last** thing in the index | The order stops being total, so the sequence of rows sharing one replication-key value is unspecified between runs. Combined with `UNIQUE` below, omitting it makes the `CREATE` fail outright rather than degrade quietly |
| `UNIQUE` | Free, because the primary key trails — and that is the point: it turns a missing trailing primary key into a failed `CREATE INDEX` instead of a silently non-total order. (It is *not* what lets the index answer without touching the table; that is coverage, and a non-unique copy gives the same `Index Only Scan` with `Heap Fetches: 0`) |
| `SPLIT AT VALUES` with N−1 boundaries | A range-sharded index gets **one** tablet. All N buckets land on it, the parallelism buys nothing, and the workers contend on a single tablet |
| One N everywhere — index, tap config, every table | The scan names bucket values the index does not have. Full table scan |

### Why the bucket hashes the primary key even in shapes 3 and 4

`INCREMENTAL` names every bucket (`IN (0, 1, ... N-1)`) rather than targeting
one, so the bucket never has to be derivable from the cursor and either column
would work. The read plans are identical. What differs is where a batch lands:

```sql
-- 9,000 rows inserted in ONE transaction, counted per bucket
bucket = yb_hash_code(id)         -> 2960 / 2955 / 3085
bucket = yb_hash_code(updated_at) -> 9000 /    0 /    0
```

`now()` is **transaction-start** time, so every row written in one transaction
carries the identical timestamp, and one timestamp hashes to one bucket. A bulk
load, a backfill or a batch job puts its entire batch on a single tablet — the
thing the bucketing exists to prevent. Primary keys are distinct by construction
and do not do this.

This is *not* a write-volume difference. The index entry is a delete plus an
insert either way, because the replication key is part of the index key in both
designs: 3,000 storage write requests for 1,000 updated rows, measured on both.

Hashing the primary key also means one expression and one bucket count cover
every index on a table, rather than one per indexed column.

`N` is `keyset_buckets` in the tap config. **Default 3.** It fixes the index
expression, the tablet count, and the maximum useful parallelism, because a
worker owns exactly one bucket. Changing it means rebuilding every index.

---

## The four indexes

### 1 · `bucket, PK` — the primary key, whatever it is

```sql
CREATE UNIQUE INDEX <table>_pw_keyset ON <schema>.<table>
  (((yb_hash_code(<pk>) % 3)) ASC, <pk> ASC)
  SPLIT AT VALUES ((1), (2));
```

The primary key does **not** need to be monotonic, sequential, or numeric. A
`uuid`, a `text`, a composite `(tenant_id, id)` — all page identically. Keyset
paging wants a total order, which every primary key has by definition.

**Required for every table synced by `FULL_TABLE` or `LOG_BASED`.**

Enables:
- `FULL_TABLE` — the whole table, in key order
- **Parallel export** — N workers, one per bucket, each on its own connection
- **Resume** — each bucket bookmarks its own position; a kill mid-table restarts
  from the last row that reached the target, not from the beginning
- `LOG_BASED` **initial snapshot** — the bootstrap stage runs the same code path,
  and resumes the same way after an interruption

### 2 · `bucket, PK` — where the primary key is also monotonic

```sql
-- identical DDL to shape 1. The same index. Nothing extra to create.
CREATE UNIQUE INDEX <table>_pw_keyset ON <schema>.<table>
  (((yb_hash_code(<pk>) % 3)) ASC, <pk> ASC)
  SPLIT AT VALUES ((1), (2));
```

Shape 2 is shape 1 plus a property of your **data**, not of the index. It unlocks
one extra feature: `INCREMENTAL` with `replication_key` set to the primary key
itself.

The tap will not take your word for it. It checks, and blocks on:
- the column is **nullable** — a NULL is never written to the bookmark, so those
  rows re-sync on every run, forever
- the column is **not unique and there is no primary key** to break ties

and warns on:
- a **sequence with `CACHE > 1`** (YugabyteDB's default is `100`). Values are
  handed out in per-connection blocks, so one session commits ids 1–100 while
  another commits 101–200. A run that bookmarks 150 will never see id 40
  committed a moment later. Lower the cache, carry a lag window at least that
  wide, or use `LOG_BASED`
- a sequence attached by `DEFAULT nextval(...)` rather than owned — it can be
  dropped, repointed or reset with no change to the column

### 3 · `bucket, created_at, PK` — a creation timestamp

```sql
CREATE UNIQUE INDEX <table>_created_at_pw_keyset ON <schema>.<table>
  (((yb_hash_code(<pk>) % 3)) ASC, created_at ASC, <pk> ASC)
  SPLIT AT VALUES ((1), (2));
```

Any column name works — `created_at`, `createdat`, `created`, `inserted_at`,
`ts_insert`, `event_time`. The tap does not gate on the name; it reads what the
column actually is. The index name follows the column: `<table>_<column>_pw_keyset`.

Enables:
- `INCREMENTAL` on that column — **new rows only**
- **PartialSync** — a bounded export, `WHERE created_at BETWEEN ... AND ...`

Does **not** see updates. A creation timestamp never moves after insert, so every
change to an existing row is invisible. That is not a defect in the index; it is
what the column means.

### 4 · `bucket, updated_at, PK` — a modification timestamp

```sql
CREATE UNIQUE INDEX <table>_updated_at_pw_keyset ON <schema>.<table>
  (((yb_hash_code(<pk>) % 3)) ASC, updated_at ASC, <pk> ASC)
  SPLIT AT VALUES ((1), (2));
```

Identical DDL to shape 3. The difference is what the column records.

Enables:
- `INCREMENTAL` on that column — **inserts and updates**
- **PartialSync**

**A column named for modification time is a claim, not a mechanism.** An
`updated_at` carrying only `DEFAULT now()` is set on insert and never moves
again — it is shape 3 wearing shape 4's name, and every update after the first is
silently missed. Only a row-level `UPDATE` trigger actually maintains one:

```sql
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END $$ LANGUAGE plpgsql;

CREATE TRIGGER <table>_touch BEFORE UPDATE ON <schema>.<table>
  FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
```

The tap reports whether such a trigger exists. It cannot confirm which column the
trigger touches — that is inside the function body — so the owner still has to
confirm it fires on every write path.

Neither shape 3 nor shape 4 sees **deletes**. Only `LOG_BASED` does.

---

## Feature matrix

| | **1** `bucket, PK` | **2** `bucket, PK` monotonic | **3** `bucket, created_at, PK` | **4** `bucket, updated_at, PK` |
|---|:---:|:---:|:---:|:---:|
| Index to create | `<t>_pw_keyset` | *same index as 1* | `<t>_<col>_pw_keyset` | `<t>_<col>_pw_keyset` |
| `FULL_TABLE` | ✅ | ✅ | — | — |
| ⮑ parallel, N workers | ✅ | ✅ | — | — |
| ⮑ resume mid-table | ✅ | ✅ | — | — |
| `LOG_BASED` initial snapshot | ✅ | ✅ | — | — |
| `LOG_BASED` change stream | *no index needed* | | | |
| `INCREMENTAL` | ❌ | ✅ key = PK | ✅ key = that column | ✅ key = that column |
| ⮑ sees inserts | — | ✅ | ✅ | ✅ |
| ⮑ sees updates | — | ❌ | ❌ | ✅ *trigger required* |
| ⮑ sees deletes | — | ❌ | ❌ | ❌ |
| ⮑ resumes inside a tie | — | ✅ *(key is unique)* | ❌ *see below* | ❌ *see below* |
| PartialSync bounded export | ❌ | ✅ on the PK | ✅ | ✅ |

### Resuming inside a tie group

The index makes the order total — the primary key trails, so rows sharing a
timestamp have a fixed sequence. **The tap cannot use that**, and shapes 3 and 4
are marked ❌ above because of it.

Singer's `INCREMENTAL` state carries one scalar, `replication_key_value`, and the
tap rejects any other bookmark key outright. So a resume can only say
`WHERE key >= <last timestamp seen>` — it cannot say "and after this primary
key". Every row sharing the last timestamp is therefore re-read. Measured on a
10,000-row tie group, interrupting at different points:

| interrupted after | rows re-emitted on resume |
|---|---|
| 5 rows into the group | 5 |
| 3,000 rows into the group | 3,000 |
| the whole group | 10,000 |

Re-emission equals how far into the group the run got, so the worst case is the
size of the group. This is at-least-once by design and the target deduplicates on
the primary key, so it is not a correctness problem — but there is a failure mode
worth knowing:

**A tie group larger than one run can drain will never advance the bookmark.**
The resume restarts the group every time. If a single timestamp covers more rows
than a run completes, the sync makes no progress at all.

This is a reason to prefer a replication key with high cardinality. A bulk load
stamps every row in one transaction with the same `now()`, so a table loaded in
large batches can carry very large tie groups. `LOG_BASED` has none of this.

### Which do I need?

- Table is `FULL_TABLE` or `LOG_BASED` → **shape 1**.
- Table is `INCREMENTAL` on the primary key → **shape 2**, which *is* shape 1.
  Do not create a second index.
- Table is `INCREMENTAL` on a timestamp → **shape 3 or 4** for that column.
  Shape 1 is not required unless the table is also `FULL_TABLE`/`LOG_BASED`.
- Table is `LOG_BASED` *and* PartialSync'd on a timestamp → **1 + (3 or 4)**.
- Table has **no primary key** → nothing here applies. It cannot be paged,
  parallelised or resumed; it gets one unresumable sequential pass. The
  preflight reports this as `BLOCK`.

---

## Session settings the tap sets for itself

You do not need to set these; they are listed so a plan you capture by hand
matches the one the tap gets.

```sql
SET yb_max_merge_scan_streams = <max(N, 8)>;  -- merge the N per-bucket streams
SET enable_seqscan = off;                     -- on the scan connections only
```

`yb_max_merge_scan_streams` must be at least N or the streams are not merged and
the result is sorted instead. `enable_seqscan = off` is what actually gets the
streaming plan: the index hint alone does not, because the cost model prefers a
sequential scan and an external merge sort — cheaper in wall clock at moderate
sizes, and it spills to disk instead of streaming. It is set on connections that
run nothing but these scans, and it does not forbid a sequential scan where no
index can serve.

Every scan also names every bucket explicitly:

```sql
WHERE (yb_hash_code(<pk>) % N) IN (0, 1, ... N-1)
```

This is required for **ordering**, not for bounding. YugabyteDB will bound a
range on a trailing column under an unbounded leading one — so PartialSync's
`WHERE created_at >= ...` works without it — but `ORDER BY` will not stream:
without the predicate a full ordered drain of 50,000 rows plans as a sequential
scan and a 1,672 kB external merge sort instead of a 3-stream merge.

---

## What it costs to get wrong

Measured on the review test bed (50,000 rows, 3 tablets, 3 buckets,
YugabyteDB 2026.1.1.1).

| Mistake | Effect |
|---|---|
| No index, hash PK, resume at the halfway point | Sequential scan of all 50,000 rows and a blocking 3,112 kB quicksort, to return 25,000. Nothing is emitted until the whole table has been read and sorted. With the index: 8,391 index rows, 81 kB, streaming |
| `SPLIT AT VALUES` omitted | 1 tablet instead of 3. Reported by the preflight |
| Bucket declared `HASH` instead of `ASC` | `SPLIT AT VALUES` rejected outright |
| Primary key omitted from the trailing columns | `CREATE UNIQUE INDEX` fails with a duplicate-key error, which is the intended outcome. Drop `UNIQUE` as well and it succeeds, leaving a non-total order — the drain then plans as a blocking external merge sort (2,432 kB measured) instead of a 3-stream merge. It does **not** change tie re-reading: that comes from the bookmark, not the index, and was measured identical with and without the trailing key |
| `ORDER BY` written as a row constructor — `ORDER BY (a, b)` | Opaque to the planner: a blocking sort where the index could have supplied the order. 48 MB spill against 81 kB streaming |
| Capturing a plan on a table that has never been `ANALYZE`d | Not a mistake in the index — but see the note below before reading a `Sort` as one |
| Keyset resume written as a row constructor — `WHERE (a, b) > (%s, %s)` | Reads `Index Cond`, so the plan looks correct, but every remaining index entry in the bucket is read and dropped. The tap emits an expanded form instead — but see the composite-key limitation below, which is not yet fixed |
| `N` in the config ≠ `N` in the index | Full table scan, silently. The tap validates this against the live index and refuses |

---

## Known limitation: resuming a COMPOSITE primary key

**Single-column primary keys are unaffected.** `(id) > (%s)` collapses to
`id > %s`, which becomes a real `Index Cond` and seeks straight to the cursor.

For a composite key the tap emits an expanded comparison rather than a row
constructor, because a row constructor gets no pushdown at all under a bucketed
index. But the expansion only gets the *leading* column into the `Index Cond`;
the rest becomes a storage filter:

```
Index Cond: (((yb_hash_code(tenant, id) % 3) = 0) AND (tenant >= 1))
Storage Index Filter: ((tenant > 1) OR (id > 5994))
Storage Index Rows Scanned: 1991        -- to return 5
```

So a resume costs **the size of the cursor's leading-value group**, not the
number of rows returned. How bad that is depends entirely on the cardinality of
the first key column:

| leading column | rows sharing the cursor's value, per bucket | index rows scanned to return 5 |
|---|---|---|
| high cardinality | 33 | 22 |
| low cardinality (worst case: constant) | 1,991 — the whole bucket | 1,991 |

An earlier version of this document quoted the high-cardinality number as though
it were the general case. It is not: it was an artifact of the test data. With a
low-cardinality leading column — `tenant_id`, `region`, a status or type code,
anything with few distinct values — every resume re-reads the whole bucket, which
is the behaviour the expansion was meant to avoid.

**What to do about it for now:** prefer a composite key whose *first* column is
selective. If your leading column has few distinct values, expect resumes to cost
a full bucket scan, and prefer fewer, larger buckets so that fewer resumes happen.
A fix that seeks properly is under investigation.

## Choosing N

`N` is `keyset_buckets`, default 3. Three things bound it, measured on a
single-TServer cluster:

**Merging holds to at least 512.** Verified at N = 3, 8, 16, 32, 64, 128, 256 and
512: `Merge Streams` equals N every time, tablets equal N, and `SPLIT AT VALUES`
emits N−1 boundaries. N = 1024 was not reached — not a merge limit
(`yb_max_merge_scan_streams` maxes at 1024) but an index-build one: creating 1024
tablets did not finish in 20 minutes.

**The session setting is load-bearing and the server default is wrong for it.**
`yb_max_merge_scan_streams` boots at `0`. On a 16-bucket index it flips exactly
at N:

| setting | plan | index rows to return 1 |
|---|---|---|
| 0 (server default) | `Sort` | 3,000 |
| 15 (N−1) | `Sort` | 3,000 |
| **16 (= N)** | **merge, 16 streams** | **16** |

187× and a blocking sort, on the wrong side of a single integer. The tap sets
`max(N, 8)`, which is always ≥ N.

**For wide tables, choose N against row width, not just parallelism.** Peak
memory per worker scales with *rows in the bucket × row width*, and narrow tables
hide this completely:

| table | rows in bucket | peak memory | per row |
|---|---|---|---|
| 3 columns, 500k rows | 166,477 | **65 kB** | 0.0004 kB |
| 50 columns, ~4.8 kB/row | 9,863 | **50,675 kB** | 5.14 kB |
| same, N raised 3 → 12 | 2,449 | **12,886 kB** | 5.26 kB |

It is not a fetch-batch effect — varying `yb_fetch_row_limit` across 1024/256/64
moved peak memory by under 1%. A wide table with 500k rows in one bucket would
report around 2.5 GB. Raising N cuts it proportionally, because it cuts rows per
bucket.

**Skew erases parallelism without breaking anything.** Because the bucket hashes
the primary key, ordinary keys spread evenly — but a pathological key set can
land 99% of rows in one bucket, and then the sync is the slowest worker:
measured, a 20,000/100/100 split caps speedup at 1.01× where an even 500k table
reaches 2.32×. Correctness is unaffected and nothing reports the distribution.
To check a table yourself:

```sql
SELECT (yb_hash_code(<pk>) % <N>) AS bucket, count(*) FROM <table> GROUP BY 1 ORDER BY 1;
```

## `ANALYZE` before you read a plan

A `Sort` in the plan usually means the bucket predicate or the index is wrong.
There is one case where it does not, and it is easy to hit: **a table with no
statistics.**

With no `reltuples`, YugabyteDB underestimates the row count badly — measured at
roughly 1/66 of reality — and once the estimate falls under about 20 rows the
cost model prefers an index scan plus a quicksort over the merge. The index is
still the right one and the scan still reads only the rows it returns; the sort
is a bounded in-memory sort of the result, not an external merge. Same query,
same settings, same table:

```
                 est     actual   plan
before ANALYZE    15      1,000   Sort Method: quicksort  Memory: 87kB
before ANALYZE   150     10,000   Merge Streams: 3
after  ANALYZE 1,000      1,000   Merge Streams: 3
after  ANALYZE   100        100   Merge Streams: 3
```

So: between a bulk load and the first auto-analyze, an incremental run with a
small delta will plan with a sort. Run `ANALYZE` on the table before capturing a
plan to check it against this document, or you will be reading a statistics
problem as an index problem.

## Create these indexes outside a transaction block

YugabyteDB cannot build an index concurrently inside a transaction, and says so:

```
NOTICE:  making create index for table "t" nonconcurrent
DETAIL:  Create index in transaction block cannot be concurrent.
HINT:   Consider running it outside of a transaction block.
```

Observed on this cluster when the rule is ignored: `pg_index` reports
`indisvalid = true, indisready = true`, the session that ran the transaction sees
its rows, and **a fresh session sees none** — neither through the index nor
through the table. The preflight reports `OK`, because as far as the catalog is
concerned the index is perfect.

Run each `CREATE UNIQUE INDEX` as its own statement, not wrapped in
`BEGIN`/`COMMIT`, and not inside a migration that batches DDL into one
transaction. If you have to serialise a batch of these, note that YugabyteDB
serialises index backfills within a database anyway, so batching buys nothing.

## Checking a config before you run it

```bash
singer-connectors/tap-yugabyte/tools/yb_index_check.py tap_yugabyte.yml \
  --host <host> --port 5433 --user <user> --dbname <db> [--buckets N]
```

It reads the tap YAML the service owner already maintains, checks every table
against the live source, and prints one of:

- `OK` — the index exists and its shape, bucket count, uniqueness and tablet
  count all match
- `ACTION` — with the exact `CREATE UNIQUE INDEX` to run. The DDL it prints is
  what turns that line into `OK`
- `BLOCK` — the configuration cannot work, with the reason

Exit status is 0 only when every table is `OK`.
