# Required indexes for tap-yugabyte

**These indexes are a prerequisite, not a tuning suggestion.** Create them before
a table is added to a tap config. Without them the tap still returns correct
rows — it just reads the whole table to do it, on every run and every resume, and
neither the query plan nor the tap log says that happened.

The reason is that the tap pages by **bucket**, not by the primary key: it
partitions the table into N groups of `yb_hash_code(<pk>) % N` and gives each one
to a worker. That expression is not stored on the table, so no table can order by
it, and every ordered, resumable read the tap performs needs an index to supply
the order.

**This holds whatever the sharding.** YugabyteDB shards a primary key by hash
unless it was declared `ASC`/`DESC`, and a hash-sharded key has no order to scan
along at all — but a range-sharded one is no help either, because the order it
has is on the key and the tap pages on the bucket. Shape 1 is required for every
table with a primary key.

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

`INCREMENTAL` reads every bucket — one `UNION ALL` branch each — rather than
targeting one, so the bucket never has to be derivable from the cursor and either
column would work. The read plans are identical. What differs is where a batch
lands:

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

**Required for every table synced by `FULL_TABLE` or `LOG_BASED`** — including one
whose primary key is range-sharded, because the tap pages on the bucket rather
than on the key and nothing in it reads a range-sharded key in key order.
Without this index `full_table.sync_table` falls through to a single
non-resumable, non-parallel pass.

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

#### Both ends now say when this is happening

No index fixes it, so the tap **detects and reports** it instead. Nothing here
changes the state shape or the predicate.

**Before the run** — the preflight measures the largest tie group for every
`INCREMENTAL` replication key. It reads `pg_stats` rather than the table, so it
costs a catalog lookup and no scan: `most_common_freqs` × `reltuples` is the
size of the largest group, measured within 0.02% on a 40,000-row group. Tell it
the tap's row cap with `--limit N` and the verdict is exact rather than a guess:

```
BLOCK  rt.bulkload  [INCREMENTAL on updated_at]
       - 39,995 of 40,050 rows (100%) share one updated_at value, estimated from
         most_common_vals -- at or above the configured limit of 10,000. A run
         that starts on that value reads 10,000 rows all carrying it, writes the
         bookmark it already had, and the next run issues the identical
         statement: the sync cannot advance past this value, ever.
```

A run reads `WHERE key >= <bookmark> ORDER BY key, pk LIMIT <limit>`, so a group
of `limit` rows or more is a livelock by arithmetic, not by luck — hence `BLOCK`.
Without `--limit` there is no row cap, a run that completes drains to the end and
always advances, and the failure needs a run to be *killed* inside the group; the
group is then reported as a risk note rather than as a verdict. A column with no
statistics is reported as unknown, not as fine — run `ANALYZE` and re-check.

**During the run** — `incremental.py` warns when a run emitted rows and the
bookmark ends exactly where it started:

```
INCREMENTAL sync of rt-bulkload MADE NO PROGRESS and cannot make any: all 10000
rows it emitted carry the same updated_at ('2026-09-22T14:21:22.858584+00:00'),
the run stopped on LIMIT 10000, and the bookmark ends where it started.
```

It needs all of: rows emitted, a bookmark that already existed, that bookmark
unchanged, and the `LIMIT` reached. Dropping the last one makes a one-row table
warn on every run — it re-reads its single row forever and the bookmark cannot
move, because nothing lies beyond it. That is caught up, not stuck.

### Which do I need?

- Table is `FULL_TABLE` or `LOG_BASED` → **shape 1**, whether the primary key is
  hash- or range-sharded. Sharding changes nothing: the tap pages on the bucket.
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
WHERE (yb_hash_code(<pk>) % N) IN (0, 1, ... N-1)     -- FULL_TABLE's max-key probe
WHERE (yb_hash_code(<pk>) % N) = <b>                  -- one branch per bucket
```

This is required for **ordering**, not for bounding. YugabyteDB will bound a
range on a trailing column under an unbounded leading one — so PartialSync's
`WHERE created_at >= ...` works without it — but `ORDER BY` will not stream:
without the predicate a full ordered drain of 50,000 rows plans as a sequential
scan and a 1,672 kB external merge sort instead of a 3-stream merge.

The `IN`-list form is only safe on a **plain** cursor, which is why `INCREMENTAL`
uses the branch form — see the next section.

---

## The merge scan does not survive a server-side cursor

**`ORDER BY` over a multi-bucket `IN` list is not ordered when the statement is
read through a named cursor.** The merge is performed by the storage layer, and
`DECLARE CURSOR` does not perform it: the N per-bucket streams arrive
**concatenated** — each ascending internally, the whole not ascending.

`EXPLAIN` reports `Merge Streams: 3` in **both** cases. The plans are
byte-identical. Nothing about any plan distinguishes them; only comparing the
rows that come back does. Measured on `rt.healthy`, 40,000 rows, one session,
identical settings, only the cursor varying:

| cursor | rows | `ORDER BY` violations | bucket changes along the stream |
|---|---|---|---|
| plain | 40,000 | 0 | 26,712 — interleaved, so merged |
| named | 40,000 | **2** | **2** — concatenated, N−1 for N buckets |

Two violations sounds trivial. It is not: with a `LIMIT` the run takes the first
*n* rows of the concatenation — essentially one bucket — bookmarks that bucket's
high-water mark, and **permanently skips every row in the other buckets below
it**, while reporting success and emitting a full batch on schedule. Driving the
real `incremental.sync_table` against `rt.healthy` with `limit: 10000` until it
reported caught up:

| statement | runs | rows emitted | distinct rows | missing forever |
|---|---|---|---|---|
| `IN`-list + storage merge | 3 | 19,634 | 19,631 | **20,369** |
| `UNION ALL` + outer `ORDER BY` | 6 | 40,005 | 40,000 | **0** |

### What `INCREMENTAL` emits instead

```sql
SELECT <columns> FROM (
  (SELECT * FROM <table> WHERE (yb_hash_code(<pk>) % N) = 0 [AND <key> >= <bookmark>]
     ORDER BY <key> ASC, <pk> ASC)
  UNION ALL
  ... one branch per bucket ...
) yb_speedup_trick ORDER BY <key> ASC, <pk> ASC [LIMIT n];
```

**The outer `ORDER BY` is load-bearing and must never be omitted.** It is what
makes the shape correct rather than lucky: the planner is *obliged* to satisfy
it, so it emits `Merge Append` where the branches already supply the order and a
`Sort` where they cannot. Correctness never rests on `Append` emitting its
children in branch order, which neither SQL nor PostgreSQL guarantees. Drop it
and you get a bare `Append` and the same 2 violations.

`Merge Append` is a plan node rather than a storage-layer behaviour, so a named
cursor honours it. Measured through the tap's own named cursor: 0 violations,
40,000 rows.

| shape | plan | peak memory | violations, named cursor |
|---|---|---|---|
| `UNION ALL`, ordered branches, outer `ORDER BY` | `Merge Append` | **161 kB** | **0** |
| `UNION ALL`, unordered branches, outer `ORDER BY` | `Sort` | 5,624 kB | 0 |
| `UNION ALL`, ordered branches, **no** outer `ORDER BY` | `Append` | 185 kB | **2** |
| `IN`-list + storage merge | `Merge Streams: 3` | 263 kB | **2** |

Each branch states its bucket as an equality and carries the bookmark, so both
reach the same `Index Cond` — which is what terminates early under a `LIMIT`:

```
Limit
  -> Merge Append
     -> Index Only Scan using healthy_updated_at_pw_keyset
          Index Cond: (((yb_hash_code(id) % 3)) = 0
                       AND (updated_at >= '...'::timestamp with time zone))
          Storage Index Rows Scanned: 4096      -- per branch, for LIMIT 10000
```

12,288 index rows read to return 10,000, against 40,000 for a full drain.

**No index hint is emitted on this path, and one must not be added.** Measured:

- a hint nested inside the subquery **is** read — verified by A/B on the exact
  `yb_speedup_trick` shape: unhinted plans an `Index Only Scan`, and adding
  `/*+ SeqScan(healthy) */` inside the subquery turns it into a `Seq Scan`. So
  the hint this statement used to carry was doing something; it just was not
  doing anything useful. `full_table` puts its hint first in the statement, where
  it *is* read
- one leading hint naming the bare table reaches exactly **one** branch, because
  every branch writes the same relation name. Probed with `SeqScan(healthy)`: one
  branch sequentially scanned, two still on the index
- N leading hints against per-branch aliases do reach every branch, and make the
  plan **worse** — `IndexScan` forces a plain `Index Scan` in place of the `Index
  Only Scan` the planner picks unaided, adding a heap fetch per row: 80,000
  storage rows scanned against 40,000 on a full drain, 24,576 against 12,288
  under `LIMIT 10000`
- unhinted is already the right plan everywhere it was measured — `Merge Append`
  over N `Index Only Scan`s with `Heap Fetches: 0` — on an `ANALYZE`d table and
  on one that has never been `ANALYZE`d, with the session settings above and with
  none at all

The session settings stay, but this statement no longer depends on them: the
ordering is in the plan now, not in the storage layer.

**Tables with no primary key, or no bucket count, are untouched.** Nothing to
hash means no discriminator and no branches, so the statement is the plain single
ordered `SELECT` it has always been — and one stream is in order under any
cursor, because nothing is being merged.

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
| An ordered multi-bucket `IN`-list scan read through a **named** cursor | The storage-level merge is not performed and the buckets arrive concatenated. Both plans read `Merge Streams: 3`, so no plan shows it. With a `LIMIT`, 20,369 of 40,000 rows were skipped permanently while every run reported success — see "The merge scan does not survive a server-side cursor" |
| The outer `ORDER BY` dropped from the `UNION ALL` | A bare `Append`, whose child order nothing guarantees — the same 2 violations and the same silent loss. It is the `ORDER BY` that obliges the planner, not the branch order |
| A `LIMIT` moved *inside* the `UNION ALL` branches | Caps each bucket separately instead of terminating the merge early, so rows past a bucket's cap are dropped from the run and the bookmark advances past them |

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

**`INCREMENTAL`'s `Merge Append` holds one open branch per bucket, so its own
memory scales with N.** Confirmed on `rt.healthy`, 40,000 rows, full ordered
drain through the tap's named cursor — `Merge Append` at every N, no point at
which it degraded to a `Sort`, and 0 `ORDER BY` violations every time:

| N | 3 | 8 | 16 | 32 | 64 | 128 |
|---|---|---|---|---|---|---|
| plan | `Merge Append` | `Merge Append` | `Merge Append` | `Merge Append` | `Merge Append` | `Merge Append` |
| peak memory | 161 kB | 361 kB | 745 kB | 1,521 kB | 3,065 kB | 6,145 kB |

Roughly 47 kB per bucket on a three-column table, on top of a fixed ~20 kB. It
pulls the opposite way from the per-worker figure below, which *falls* as N
rises: `FULL_TABLE` gives one bucket to each of N workers, while `INCREMENTAL`
reads all N through one cursor. 128 was the largest confirmed here.

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

## What a sync sees when the table is being written to

Measured, not inferred: the scan was frozen at a known key and the writes landed
while it was stopped there.

**An uninterrupted, retry-free `FULL_TABLE` sync is a consistent image, and that
is better than it sounds.** The per-bucket statements start within 0–1 ms of each
other, and a row reaches the target only if it existed, under a key inside that
statement's range, at the instant the statement ran. No concurrent write reaches
the sync at all — not above the bound, not below it, not even in a region the
scan has not got to yet. With a writer running at 28 rows/s through a 9.4 s sync,
**7 of 262 rows** arrived, all committed before the cursors were declared.

Two consequences that follow from the same fact:

- **Rows deleted during the scan are still delivered, as live rows.** 120 of 120,
  measured. `ACTIVATE_VERSION` cannot remove them: they carry the current version.
- **A row whose primary key changes mid-scan is emitted once, under its old key.**
  The target then holds a key the source no longer has, and never receives the
  key it does.

### Where it stops being consistent

**A resumed sync is not one image, it is two.** So is a sync in which any bucket
retried — and `retry_read` exists because leader elections and read restarts are
expected, not rare. Across that boundary a row whose primary key moves is either
delivered twice or not at all, depending on which side of the cursor each key
falls:

| interrupted at | rows whose PK moved | duplicated | lost |
|---|---|---|---|
| 25% drained | 600 | **17.8%** | **17.7%** |
| midpoint | — | **25%** | **25%** |

A single bucket retry inside one otherwise-normal sync produced 60 duplicated and
60 lost, and **the sync reported success** — 400,060 records, no error, three
warning lines the only trace.

The composite resume ladder adds one more window of its own: a row that moves
from a later rung's range into an earlier one while rung 1 is still draining is
lost, where a single-statement resume would have caught it. That window is rung 1's
*entire drain time* — 2,168 ms measured — and it is longest exactly where the
ladder matters most, because a low-cardinality leading column makes rung 1 large.

### The fix, and it is a config key

`snapshot_hybrid_time` pins every reader to one instant and removes all of it.
Measured on the same resume:

| | late inserts | PK moves duplicated | PK moves lost | distinct keys delivered |
|---|---|---|---|---|
| unpinned | 60/60 | 60 | 60 | 400,060 |
| **pinned** | **0/60** | **0** | **0** | **400,000** |

Get the value from `yb_get_current_hybrid_time_lsn()`. `SET yb_read_time` is
superuser-only, so a least-privilege tap user needs the `SECURITY DEFINER`
procedure named by `yb_read_time_proc` — **which this branch references but does
not create.**

**If a table's primary key is ever updated, do not run `FULL_TABLE` against it
unpinned.** Use `snapshot_hybrid_time`, or use `LOG_BASED`.

### INCREMENTAL and long write transactions

`now()` is transaction-start time, so a row committed by a long transaction
carries a timestamp from when that transaction *began*. If a sync advances the
bookmark past it in the meantime, the row is permanently missed. Measured: a
transaction open for 4.26 s had **500 of 500 rows** missed for ever. Rewinding
2.1 s recovered none of them; rewinding 5.3 s recovered all 500.

The lag window has to exceed your longest write transaction. There is no safe
fixed constant.

## Checking a config before you run it

```bash
singer-connectors/tap-yugabyte/tools/yb_index_check.py tap_yugabyte.yml \
  --host <host> --port 5433 --user <user> --dbname <db> [--buckets N] [--limit N]
```

Pass `--limit` whatever the tap config's `limit` is. It is what caps the rows one
`INCREMENTAL` run reads, and it is the difference between the tie-group check
guessing and knowing — see "Both ends now say when this is happening" above.

It reads the tap YAML the service owner already maintains, checks every table
against the live source, and prints one of:

- `OK` — the index exists and its shape, bucket count, uniqueness and tablet
  count all match
- `ACTION` — with the exact `CREATE UNIQUE INDEX` to run. The DDL it prints is
  what turns that line into `OK`
- `BLOCK` — the configuration cannot work, with the reason

Exit status is 0 only when every table is `OK`.
