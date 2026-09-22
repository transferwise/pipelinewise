"""Bucketed keyset scanning: index prerequisite, snapshot pinning, key discovery.

A YugabyteDB primary key is HASH-sharded unless it was declared ASC/DESC, and a
hash-sharded key has no order to scan along. That makes the obvious resumable
full-table scan -- order by the key, remember where you stopped -- degrade into a
full table scan on every resume.

The fix is a prerequisite index that supplies an order the key itself does not
have: ((yb_hash_code(<pk>) % N) HASH, <pk> ASC). The leading column takes only N
values and every scan predicate against it is an equality, which is the one
access pattern hash sharding serves; the key columns trail it ASC, so each bucket
is an ordered range that a cursor can walk and resume inside.

N is the only knob. It fixes the index expression, the number of buckets, and
therefore the maximum useful parallelism, because a worker owns exactly one
bucket. Because N appears in both the index and every query, it is validated
against the live index rather than trusted from config.
"""

import re

import psycopg2

BUCKETS_DEFAULT = 3
INDEX_SUFFIX = '_pw_keyset'

# Name fragments that hint at what a temporal column records. They rank
# candidates; they never decide whether one is a candidate at all. A column
# called `ts_insert` or `event_time` is just as likely to be the right watermark
# as one called `created_at`, and gating on the name loses it silently.
#
# The split matters more than the hints do. A modification time advances when a
# row is updated, so INCREMENTAL sees the update; a creation time never moves
# again after insert, so every update after the first is invisible. Preferring
# creation columns -- which reads naturally -- is exactly backwards.
# A name suggesting modification time is a claim, not a mechanism. A column
# called `updated_at` carrying only DEFAULT now() is set on insert and never
# again -- it is a creation column wearing the wrong name, and nothing about the
# name says otherwise. Only a row-level UPDATE trigger actually maintains one;
# anything else is the application remembering, on every write path, forever.
#
# So neither kind is ranked above the other. They fail differently: a creation
# column misses every update, and a modification column misses every update the
# application forgot to stamp. Both are guesses that LOG_BASED does not need.
_MODIFIED_NAME_HINTS = (
    'updated_at', 'updated', 'modified_at', 'modified', 'last_modified',
    'changed_at', 'changed', 'update_time', 'mtime',
)
_CREATED_NAME_HINTS = (
    'created_at', 'created', 'inserted_at', 'inserted',
    'create_time', 'created_on', 'creation_date', 'ctime',
)


def _name_rank(lowered, hints):
    """Position of the first matching hint, or None when nothing matches."""
    for position, hint in enumerate(hints):
        if hint in lowered:
            return position
    return None


def quoted(columns):
    return [f'"{c}"' for c in columns]


def bucket_expr(pk_columns, buckets, escape_percent=False):
    """Bucket discriminator. Rendered identically in the index and in every scan;
    any divergence silently costs the index and falls back to a full scan.

    psycopg2 treats `%` as the start of a placeholder in any statement it is given
    parameters for -- including an empty sequence -- so the modulo operator has to
    be doubled in those, and left alone in statements executed without parameters
    (the index DDL, the max-key probe). Passing the wrong one does not produce a
    slow query, it produces an IndexError or a malformed statement.
    """
    modulo = '%%' if escape_percent else '%'
    return f"(yb_hash_code({', '.join(quoted(pk_columns))}) {modulo} {buckets})"


def order_by_sql(pk_columns, direction='ASC'):
    """Per-column ORDER BY. A ROW() expression here is opaque to the planner and
    forces a blocking sort even when the index could have supplied the order."""
    return ', '.join(f'{c} {direction}' for c in quoted(pk_columns))


def keyset_predicate(columns, after=True):
    """Lexicographic comparison of `columns` against a bound, expanded into the
    conjunctive form the index can actually bound.

    The natural way to write this is a row constructor -- `(a, b) > (%s, %s)`.
    PostgreSQL optimises that into a range when the columns lead the index;
    YugabyteDB does not when something else leads it, and the bucket
    discriminator always does. The plan still reads `Index Cond`, so nothing
    about it says the scan degraded -- but every remaining index entry in the
    bucket is read and dropped. Measured on 50k rows with a two-column key:
    9,221 index rows scanned to return 5, against 22 for the form below. Three
    columns: 9,221 against 177.

    The expansion states the leading column as a plain range first, which is
    what gives the scan a start position, and leaves the rest as a filter the
    storage layer evaluates over the few rows that remain:

        a >= %s AND (a > %s OR b >= %s) AND (a > %s OR b > %s OR c > %s)

    A single-column key needs none of this -- `(a) > (%s)` already collapses to
    `a > %s` -- and it is emitted as the plain comparison.

    Returns (sql, parameter_order), where parameter_order lists the index into
    the caller's value sequence for each placeholder, since values repeat.
    """
    strict, last = ('>', '>') if after else ('<', '<=')
    loose = '>=' if after else '<='
    cols = quoted(columns)
    if len(cols) == 1:
        return f'{cols[0]} {last} %s', [0]

    clauses, order = [], []
    for i in range(len(cols)):
        terms = []
        for j in range(i):
            terms.append(f'{cols[j]} {strict} %s')
            order.append(j)
        final = last if i == len(cols) - 1 else loose
        terms.append(f'{cols[i]} {final} %s')
        order.append(i)
        clauses.append(terms[0] if len(terms) == 1 else f'({" OR ".join(terms)})')
    return ' AND '.join(clauses), order


def keyset_params(values, order):
    """Lay the caller's values out in the order keyset_predicate's placeholders
    consume them."""
    return [values[i] for i in order]


def bucket_in_sql(pk_columns, buckets, escape_percent=False):
    """`(<bucket expr>) IN (0, 1, ... N-1)` -- every bucket, as one predicate.

    YugabyteDB turns this into a single index condition over all N values and,
    with yb_max_merge_scan_streams >= N, merges the N per-bucket sorted streams
    into one ordered result. That replaces the UNION ALL the scan used to need,
    and it works against the expression index directly -- the source table needs
    no generated column and no schema change, only the index.

    It has to be a literal IN list: the planner does not derive one from a CHECK
    constraint, and BETWEEN degrades to a storage filter over the whole index.
    """
    values = ', '.join(str(b) for b in range(buckets))
    return f'{bucket_expr(pk_columns, buckets, escape_percent)} IN ({values})'


def index_hint(table_name):
    """Pin the scan to the keyset index.

    The merge plan is a cost decision, and the cost model does not price what
    this scan is for: at moderate sizes it prefers a sequential scan and an
    external merge sort, which is faster in wall clock and spills to disk
    instead of streaming. Measured on 50k rows it chose a 2.9MB spill over the
    index. The tap wants the streaming plan every time, not the cheaper one,
    so the choice is stated rather than left to the estimate.
    """
    return f'/*+ IndexScan({table_name} {index_name(table_name)}) */'


def merge_scan_available(cur):
    """Whether this server build has the merge-scan setting at all.

    Checked rather than assumed: older YugabyteDB has no such parameter, and SET
    on one that does not exist raises rather than being ignored. The scan has a
    correct form either way, so this picks between them instead of failing.
    """
    cur.execute("SELECT count(*) FROM pg_settings WHERE name = 'yb_max_merge_scan_streams'")
    return cur.fetchone()[0] > 0


def max_pk_values_sql(fq_table_name, table_name, pk_columns, buckets, merge_scan):
    """Largest key tuple in the table, in whichever form this server can serve.

    With merge scan, one index condition over every bucket and the streams merged
    into order. Without it, a branch per bucket: each carries its own LIMIT, so
    each reads exactly one index entry and the outer sort orders N rows. The
    branching form is wordier but needs nothing set -- and the IN-list form
    without the setting reads the whole index, which is the failure worth
    avoiding since nothing about the plan says it happened.
    """
    cols = ', '.join(quoted(pk_columns))
    desc = order_by_sql(pk_columns, 'DESC')
    hint = index_hint(table_name)
    if merge_scan:
        return (f'{hint} SELECT {cols} FROM {fq_table_name} '
                f'WHERE {bucket_in_sql(pk_columns, buckets)} '
                f'ORDER BY {desc} LIMIT 1')
    branches = '\nUNION ALL\n'.join(
        f'  ({hint} SELECT {cols} FROM {fq_table_name} '
        f'WHERE {bucket_expr(pk_columns, buckets)} = {bucket} '
        f'ORDER BY {desc} LIMIT 1)'
        for bucket in range(buckets)
    )
    return f'SELECT {cols} FROM (\n{branches}\n) bucket_maxima ORDER BY {desc} LIMIT 1'


def scan_settings_sql(buckets):
    """Session settings a bucketed keyset scan needs, in order.

    yb_max_merge_scan_streams lets the scan merge the per-bucket streams instead
    of sorting the whole result, and must be at least the bucket count.

    enable_seqscan is what actually makes the planner take it. The index hint
    alone does not: measured, the hinted query still chose a sequential scan and
    a 2.9MB external merge sort, because that is cheaper in wall clock and the
    cost model does not price streaming or spilling. Deprioritising the
    sequential scan is the only thing that reliably gets the streaming plan.
    It is a session setting on a connection that runs nothing but these scans,
    and it does not forbid a sequential scan -- a table with no usable index
    still gets one.
    """
    return [
        f'SET yb_max_merge_scan_streams = {max(buckets, 8)}',
        'SET enable_seqscan = off',
    ]


def index_name(table_name):
    return f'{table_name}{INDEX_SUFFIX}'


def split_at_values(buckets):
    """Tablet boundaries placing exactly one bucket per tablet.

    A range-sharded index gets a single tablet unless boundaries are given, and
    one tablet is the thing the bucketing exists to avoid. Splitting at every
    bucket value but the first gives a deterministic bucket-to-tablet mapping:
    bucket k is tablet k, so the workers do not contend and no bucket shares a
    tablet with another. Hash sharding cannot promise that -- it maps N values
    into the hash space and wherever they land is wherever they land.
    """
    return ', '.join(f'({b})' for b in range(1, buckets))


def index_ddl(fq_table_name, table_name, pk_columns, buckets, tablets=None):
    """DDL for the prerequisite index.

    The bucket column is range-sharded, not hashed, so the tablet boundaries can
    be stated: see split_at_values. The key columns trail it ASC, so each bucket
    is an ordered range a cursor can walk and resume inside. Uniqueness is free --
    the bucket is a function of the key -- and it lets the index back a keyset
    cursor without a recheck against the base table.
    """
    return (
        f'CREATE UNIQUE INDEX {index_name(table_name)} ON {fq_table_name} '
        f'(({bucket_expr(pk_columns, buckets)}) ASC, {order_by_sql(pk_columns)}) '
        f'SPLIT AT VALUES ({split_at_values(buckets)})'
    )


_INDEX_SHAPE_SQL = """
SELECT pg_get_indexdef(i.oid), x.indisunique, p.num_tablets
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_index x ON x.indrelid = c.oid
JOIN pg_class i ON i.oid = x.indexrelid
CROSS JOIN LATERAL yb_table_properties(i.oid) p
WHERE n.nspname = %s AND c.relname = %s AND i.relname = %s
"""


def describe_index(cur, schema_name, table_name, wanted_name):
    """What a keyset index on this table actually is, read from the catalog.

    Returns None when it does not exist. The key list comes from the index
    definition text because the leading column is an expression, which pg_index
    stores as a parse tree and reports as attnum 0; the rest of the shape --
    uniqueness, tablet count -- is read properly.
    """
    cur.execute(_INDEX_SHAPE_SQL, (schema_name, table_name, wanted_name))
    row = cur.fetchone()
    if row is None:
        return None
    indexdef, is_unique, num_tablets = row
    # the key list is the parenthesised group following `USING lsm `
    body = indexdef[indexdef.index('USING lsm (') + len('USING lsm ('):]
    body = body[:body.rindex(')')]
    depth, current, parts = 0, [], []
    for char in body:
        if char == ',' and depth == 0:
            parts.append(''.join(current).strip())
            current = []
            continue
        if char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
        current.append(char)
    parts.append(''.join(current).strip())
    trailing = [part.rsplit(' ', 1)[0].strip('"') for part in parts[1:]]
    return {
        'name': wanted_name,
        'definition': indexdef,
        'unique': is_unique,
        'buckets': parse_index_buckets(indexdef),
        'bucket_columns': _bucket_columns(parts[0]),
        'trailing': trailing,
        'tablets': num_tablets,
    }


def _bucket_columns(leading):
    """Column names inside the leading yb_hash_code(...) expression."""
    match = re.search(r'yb_hash_code\(([^)]*)\)', leading or '')
    if not match:
        return []
    return [c.strip().strip('"') for c in match.group(1).split(',') if c.strip()]


def check_index(cur, schema_name, table_name, wanted_name, pk_columns,
                trailing, buckets):
    """Confirm one index matches the shape the tap requires, or say what is wrong.

    Every index the tap needs is the same shape -- the primary key hashed into N
    buckets, ASC, then the ordering columns, ending in the primary key, UNIQUE,
    one tablet per bucket. This checks all of it, because each part fails
    differently and none of them fails loudly:

    wrong bucket count -- the scan predicate names buckets the index does not
    have, so the index is unusable and the scan is a full table scan;
    bucketed on the wrong column -- same, and it also moves index entries between
    tablets when the replication key changes;
    missing trailing primary key -- the order is not total, so a resume re-reads
    every row sharing the last value seen, and the index cannot answer alone;
    not unique -- a duplicate key would be accepted and then emitted twice;
    one tablet -- the bucketing bought nothing and all N workers contend on it.
    """
    found = describe_index(cur, schema_name, table_name, wanted_name)
    if found is None:
        return None, [f'{wanted_name} does not exist.']

    problems = []
    if found['buckets'] != buckets:
        problems.append(
            f"{wanted_name} is built with {found['buckets']} buckets but the tap "
            f'is configured for {buckets}. Every scan names {buckets} bucket '
            f'values, so the index cannot serve it -- rebuild the index, or set '
            f'keyset_buckets: {found["buckets"]}.')
    if found['bucket_columns'] != list(pk_columns):
        problems.append(
            f"{wanted_name} hashes {', '.join(found['bucket_columns']) or 'nothing'} "
            f"but the scan hashes {', '.join(pk_columns)}.")
    if found['trailing'] != list(trailing):
        problems.append(
            f"{wanted_name} orders by {', '.join(found['trailing'])} but the scan "
            f"orders by {', '.join(trailing)}.")
    if not found['unique']:
        problems.append(f'{wanted_name} is not UNIQUE.')
    if found['tablets'] is not None and found['tablets'] < buckets:
        problems.append(
            f"{wanted_name} has {found['tablets']} tablet(s) for {buckets} buckets. "
            f'Without SPLIT AT VALUES the whole index is one tablet and the '
            f'buckets share it.')
    return found, problems


def parse_index_buckets(indexdef):
    """Recover the bucket count from a live index definition, or None if the
    index is not one of ours."""
    match = re.search(r'yb_hash_code\([^)]*\)\s*%\s*(\d+)', indexdef or '')
    return int(match.group(1)) if match else None


def validate_index(cur, schema_name, table_name, pk_columns, buckets):
    """Confirm the prerequisite index exists and agrees with the configured
    bucket count.

    A mismatch is reported rather than tolerated: the scans would still return
    correct rows, but each would silently become a full table scan, which is the
    failure mode hardest to notice from the outside.
    """
    cur.execute(
        'SELECT pg_get_indexdef(i.oid) '
        'FROM pg_class c '
        'JOIN pg_namespace n ON n.oid = c.relnamespace '
        'JOIN pg_index x ON x.indrelid = c.oid '
        'JOIN pg_class i ON i.oid = x.indexrelid '
        'WHERE n.nspname = %s AND c.relname = %s AND i.relname = %s',
        (schema_name, table_name, index_name(table_name)),
    )
    row = cur.fetchone()
    fq_table_name = f'"{schema_name}"."{table_name}"'
    if row is None:
        return False, (
            f'Parallel keyset sync of {schema_name}.{table_name} requires a bucket '
            f'index. Create it with:\n  '
            f'{index_ddl(fq_table_name, table_name, pk_columns, buckets, buckets)};'
        )
    found = parse_index_buckets(row[0])
    if found != buckets:
        return False, (
            f'{index_name(table_name)} is built with {found} buckets but the tap is '
            f'configured for {buckets}. Rebuild the index, or set '
            f'keyset_buckets: {found}.'
        )
    return True, None


def pin_snapshot(conn, hybrid_time, security_definer_proc=None):
    """Pin this connection to a hybrid-time snapshot so every worker reads the
    same instant.

    `yb_read_time` is superuser-only and is rejected inside an explicit
    transaction block, so it is issued as its own statement with autocommit on.
    A least-privilege tap user instead calls a SECURITY DEFINER procedure, which
    is the pattern YugabyteDB documents for this case. The session is read-only
    for as long as it stays pinned.
    """
    if hybrid_time is None:
        return
    previous_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            if security_definer_proc:
                cur.execute(f'CALL {security_definer_proc}(%s)', (f'{hybrid_time} ht',))
            else:
                cur.execute(f"SET yb_read_time TO '{hybrid_time} ht'")
    finally:
        conn.autocommit = previous_autocommit


REPLICATION_KEY_CANDIDATES_SQL = """
SELECT a.attname,
       format_type(a.atttypid, a.atttypmod)                    AS data_type,
       a.attnotnull                                            AS not_null,
       a.attidentity IN ('a', 'd')                             AS is_identity,
       COALESCE(pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%%', false)
                                                               AS is_sequence_default,
       COALESCE(x.indisprimary, false)                         AS is_primary_key,
       COALESCE(u.indisunique, false)                          AS is_unique
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
LEFT JOIN pg_index x ON x.indrelid = c.oid AND x.indisprimary
                    AND a.attnum = ANY (x.indkey)
LEFT JOIN pg_index u ON u.indrelid = c.oid AND u.indisunique
                    AND u.indnatts = 1 AND a.attnum = u.indkey[0]
WHERE n.nspname = %s AND c.relname = %s
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""


def discover_replication_key_candidates(cur, schema_name, table_name):
    """Rank columns that could serve as an INCREMENTAL replication key.

    Two shapes qualify: a sequence-backed integer (identity column or a
    nextval() default) and a creation timestamp. Both are returned with the
    caveats that apply to them, because on YugabyteDB neither is safe on its own
    -- see `replication_key_warnings`.

    Returns dicts ordered best-first, each with `column`, `kind`, `unique`, and
    `tiebreaker_required`.
    """
    cur.execute(REPLICATION_KEY_CANDIDATES_SQL, (schema_name, table_name))
    candidates = []
    for (name, data_type, not_null, is_identity, is_sequence_default,
         is_primary_key, is_unique) in cur.fetchall():
        lowered = name.lower()
        unique = bool(is_primary_key or is_unique)
        if is_identity or is_sequence_default:
            candidates.append({
                'column': name, 'kind': 'sequence', 'data_type': data_type,
                'unique': unique, 'not_null': bool(not_null),
                'tiebreaker_required': not unique,
                # a unique candidate beats a non-unique one of the same shape: it
                # needs no tiebreaker and can page on its own
                'rank': (0, 0 if unique else 1, 0 if is_identity else 1),
            })
        elif data_type.startswith(('timestamp', 'date', 'time')):
            # every temporal column is a candidate; the name only orders them.
            # A timestamp is never unique on its own either -- two rows written in
            # the same transaction carry the same value exactly.
            modified_rank = _name_rank(lowered, _MODIFIED_NAME_HINTS)
            created_rank = _name_rank(lowered, _CREATED_NAME_HINTS)
            if modified_rank is not None:
                kind, group, hint_rank = 'modified_timestamp', 1, modified_rank
            elif created_rank is not None:
                kind, group, hint_rank = 'created_timestamp', 1, created_rank
            else:
                kind, group, hint_rank = 'unknown_timestamp', 2, 0
            candidates.append({
                'column': name, 'kind': kind, 'data_type': data_type,
                'unique': False, 'not_null': bool(not_null),
                'tiebreaker_required': True,
                'rank': (group, 1, hint_rank),
            })
    candidates.sort(key=lambda c: c['rank'])
    for candidate in candidates:
        del candidate['rank']
    return candidates


def replication_key_warnings(candidate, pk_columns, sequence_cache_minval=None):
    """Explain why a candidate is not, by itself, a safe INCREMENTAL watermark.

    A watermark is only sound when key order matches commit order. On
    YugabyteDB it does not, for either candidate shape:

    Sequence-backed columns are handed out in per-connection blocks
    (ysql_sequence_cache_method=connection, ysql_sequence_cache_minval=100 by
    default), so one session can be committing ids 1..100 while another commits
    101..200. A run that bookmarks max(id)=150 will never see id=40 committed a
    moment later.

    Creation timestamps carry the transaction's start time, so a long-running
    transaction commits rows stamped before a watermark that has already moved
    past them -- and clock skew between nodes widens the same gap.

    Neither is fixed by a tiebreaker. A tiebreaker only makes the ordering
    total, which matters for deterministic paging; it does not make key order
    agree with commit order. That needs either a lag window, or LOG_BASED.
    """
    warnings = []
    if candidate['tiebreaker_required']:
        tiebreaker = ', '.join(pk_columns) if pk_columns else 'the primary key'
        warnings.append(
            f"{candidate['column']} is not unique, so paging by it alone can "
            f'repeat or skip rows that share a value; it needs a tiebreaker '
            f'({tiebreaker}) to give a total order.'
        )
    if not candidate['not_null']:
        warnings.append(
            f"{candidate['column']} is nullable, and NULL is never written to the "
            f'bookmark, so NULL-keyed rows re-sync on every run.'
        )
    if candidate['kind'] == 'sequence':
        cache = sequence_cache_minval if sequence_cache_minval is not None else 100
        warnings.append(
            f"{candidate['column']} is sequence-backed, and YugabyteDB caches "
            f'{cache} values per connection, so commit order does not follow id '
            f'order. Rows committed after the bookmark can carry ids far below '
            f'it and be missed permanently. Use LOG_BASED, or lower '
            f'ysql_sequence_cache_minval, or carry a lag window of at least '
            f'{cache} x (concurrent writers).'
        )
    if candidate['kind'] == 'created_timestamp':
        warnings.append(
            f"{candidate['column']} records transaction start time, so a long "
            f'transaction commits rows stamped earlier than an advanced '
            f'bookmark. Carry a lag window wider than the longest write '
            f'transaction, or use LOG_BASED.'
        )
    return warnings


# --------------------------------------------------------------- eligibility

STRATEGY_PK_RANGE = 'pk_range'
STRATEGY_BUCKET_INDEX = 'bucket_index'
STRATEGY_PLAIN_SCAN = 'plain_scan'

_SHARDING_SQL = """
SELECT c.relkind, p.num_hash_key_columns, p.num_tablets
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL yb_table_properties(c.oid) p
WHERE n.nspname = %s AND c.relname = %s
"""

# An index can serve ordered range access on `column` when that column leads it
# and is stored in sorted order. A HASH leading column cannot: it is ordered by
# hash, which has nothing to do with the column's own ordering.
# The join on indkey[0] is what restricts this to indexes the column LEADS --
# and it also excludes every expression index, because an expression key is
# stored as attnum 0 and matches no column. A bucket index is therefore never
# returned here: it leads with the discriminator, not with the column.
_RANGE_INDEX_SQL = """
SELECT i.relname,
       x.indoption[0]                               AS leading_indoption,
       x.indpred IS NOT NULL                        AS is_partial,
       pg_get_expr(x.indpred, x.indrelid)           AS predicate
FROM pg_index x
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_class c ON c.oid = x.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = x.indkey[0]
WHERE n.nspname = %s AND c.relname = %s AND a.attname = %s
"""


_KEYSET_INDEX_SQL = """
SELECT i.relname, pg_get_indexdef(i.oid)
FROM pg_index x
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_class c ON c.oid = x.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s
  AND x.indkey[0] = 0
  AND pg_get_indexdef(i.oid) LIKE '%%yb_hash_code%%'
"""


def _hashable(cur, type_name):
    """Whether yb_hash_code accepts this type.

    Probed against a NULL literal rather than a type allow-list: it costs no
    storage read, it is authoritative for the server actually being talked to,
    and the set of accepted types is not something the tap should be tracking.
    The rejection is an error, so inside a transaction the probe is wrapped in a
    savepoint to keep it from poisoning the surrounding work. Under autocommit
    each statement is already its own transaction and SAVEPOINT is itself an
    error, so the guard is skipped rather than assumed either way.
    """
    in_transaction = not getattr(cur.connection, 'autocommit', False)
    if in_transaction:
        cur.execute('SAVEPOINT yb_hash_probe')
    try:
        cur.execute(f'SELECT yb_hash_code(NULL::{type_name})')
        cur.fetchone()
        if in_transaction:
            cur.execute('RELEASE SAVEPOINT yb_hash_probe')
        return True
    except psycopg2.Error:
        if in_transaction:
            cur.execute('ROLLBACK TO SAVEPOINT yb_hash_probe')
        return False


# pg_index.indoption carries one bitmask per key column. Bits 0 and 1 are
# PostgreSQL's DESC and NULLS FIRST; YugabyteDB adds bit 2 for a hash-sharded
# column. Observed on this server: ASC 0, NULLS FIRST 2, DESC 3 (DESC implies
# NULLS FIRST), HASH 4 -- and `(a, b) HASH, c ASC, d DESC` reads `4 4 0 3`,
# agreeing with yb_table_properties' num_hash_key_columns of 2.
INDOPTION_HASH = 4


def _column_is_ordered(indoption):
    """True when an index key column is stored sorted rather than hashed.

    Read from the catalog rather than from pg_get_indexdef's text. The text is a
    rendering meant for people: it is not a stable interface, a change to how it
    spells a modifier would be silent here, and matching a column name inside it
    invites confusing a name with a prefix of another. The bitmask is the thing
    the planner itself uses.
    """
    return (indoption & INDOPTION_HASH) == 0


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-return-statements
def plan_keyset_strategy(cur, schema_name, table_name, pk_columns, pk_types, buckets):
    """Decide how this table can be scanned, and what creating the index would need.

    Three outcomes, in descending order of preference:

    `pk_range`    the primary key is already range-sharded, so it is directly
                  scannable in key order and no extra index is wanted -- adding
                  one would cost writes for nothing.
    `bucket_index` the key is hash-sharded, so ordered access needs the bucket
                  index; `index_ddl` is the statement that would create it.
    `plain_scan`  nothing can give ordered access, so the sync is a single
                  non-resumable pass. `blockers` says why.
    """
    cur.execute(_SHARDING_SQL, (schema_name, table_name))
    row = cur.fetchone()
    if row is None:
        return {'strategy': STRATEGY_PLAIN_SCAN, 'index_ddl': None,
                'blockers': [f'{schema_name}.{table_name} was not found']}
    relkind, num_hash_key_columns, num_tablets = row[0], row[1], row[2]

    if relkind in ('v', 'm'):
        return {'strategy': STRATEGY_PLAIN_SCAN, 'index_ddl': None,
                'blockers': ['a view has no primary key and cannot be indexed']}

    if not pk_columns:
        return {'strategy': STRATEGY_PLAIN_SCAN, 'index_ddl': None,
                'blockers': ['the table has no primary key to page on']}

    if num_hash_key_columns == 0:
        # already ordered by the key itself; the bucket index would be pure overhead
        return {'strategy': STRATEGY_PK_RANGE, 'index_ddl': None, 'blockers': []}

    unhashable = [c for c, t in zip(pk_columns, pk_types) if not _hashable(cur, t)]
    if unhashable:
        return {
            'strategy': STRATEGY_PLAIN_SCAN, 'index_ddl': None,
            'blockers': [
                f'yb_hash_code does not accept {", ".join(unhashable)}, so the '
                f'bucket discriminator cannot be computed'
            ],
        }

    fq_table_name = f'"{schema_name}"."{table_name}"'
    return {
        'strategy': STRATEGY_BUCKET_INDEX,
        'index_ddl': index_ddl(fq_table_name, table_name, pk_columns, buckets,
                               tablets=num_tablets or buckets),
        'blockers': [],
    }


def plan_partial_sync(cur, schema_name, table_name, boundary_column, start_value=None):
    """Decide whether a PartialSync boundary can be served by an index.

    PartialSync bounds an arbitrary configured column, not the primary key, so
    the keyset index built for full-table sync does nothing for it. What it needs
    is an index that column leads in sorted order.

    When the boundary has a fixed lower bound, a partial index over just that
    range is the cheaper answer: it indexes the rows being synced instead of the
    whole table. It only applies while queries stay inside its predicate -- a
    boundary that later moves below `start_value` silently stops using it -- so
    it is offered, never created implicitly.
    """
    cur.execute(_RANGE_INDEX_SQL, (schema_name, table_name, boundary_column))
    usable, partial = [], []
    for name, leading_indoption, is_partial, predicate in cur.fetchall():
        if not _column_is_ordered(leading_indoption):
            continue
        (partial if is_partial else usable).append((name, predicate))

    if usable:
        return {'indexed': True, 'index': usable[0][0], 'partial_indexes': partial,
                'suggestion': None, 'via_bucket_index': None, 'note': None}

    # A bucket index does not lead with the boundary column -- the discriminator
    # does -- so no column-name lookup finds it. It can still serve the range,
    # but only if the query names every bucket, which lets the index bound the
    # trailing key and the merge scan put the streams back in order. That is a
    # change to the PartialSync predicate, not something an index alone provides.
    cur.execute(_KEYSET_INDEX_SQL, (schema_name, table_name))
    for name, indexdef in cur.fetchall():
        keys = indexdef[indexdef.index('(') + 1:].split(',')
        if len(keys) > 1 and keys[1].strip().startswith(f'{boundary_column} '):
            return {
                'indexed': False, 'index': None, 'partial_indexes': partial,
                'suggestion': None,
                'via_bucket_index': name,
                'note': (
                    f'{name} can bound this range if the PartialSync predicate also '
                    f'names every bucket, as the full-table scan does. Without that '
                    f'the boundary is a full scan.'
                ),
            }

    fq_table_name = f'"{schema_name}"."{table_name}"'
    suggestion = (
        f'CREATE INDEX {table_name}_pw_partial ON {fq_table_name} '
        f'("{boundary_column}" ASC)'
    )
    if start_value is not None:
        suggestion += f" WHERE \"{boundary_column}\" >= '{start_value}'"
    return {'indexed': False, 'index': None, 'partial_indexes': partial,
            'suggestion': suggestion, 'via_bucket_index': None, 'note': None}


# ------------------------------------------------- monotonic key eligibility

# pg_get_serial_sequence only reports a sequence the column OWNS -- the
# dependency serial/bigserial/IDENTITY sets up. A sequence attached by hand,
# `DEFAULT nextval('some_seq')`, drives the column just as much but is invisible
# to it, so the raw default is carried too and parsed as a fallback. Missing it
# would drop the cache warning on exactly the columns that need it.
_KEY_FACTS_SQL = """
SELECT a.attnotnull,
       COALESCE(u.indisunique, false)                          AS is_unique,
       pg_get_serial_sequence(%s, a.attname)                   AS owned_sequence,
       format_type(a.atttypid, a.atttypmod)                    AS data_type,
       a.attidentity IN ('a', 'd')                             AS is_identity,
       pg_get_expr(d.adbin, d.adrelid)                         AS column_default
FROM pg_attribute a
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_index u ON u.indrelid = c.oid AND u.indisunique
                    AND u.indnatts = 1 AND a.attnum = u.indkey[0]
WHERE n.nspname = %s AND c.relname = %s AND a.attname = %s
  AND a.attnum > 0 AND NOT a.attisdropped
"""


_UPDATE_TRIGGER_SQL = """
SELECT t.tgname
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s
  AND NOT t.tgisinternal
  AND (t.tgtype & 16) <> 0     -- fires on UPDATE
  AND (t.tgtype & 1) <> 0      -- FOR EACH ROW
"""


def _has_update_trigger(cur, schema_name, table_name):
    """Whether any row-level UPDATE trigger exists on the table.

    Necessary for a modification timestamp to maintain itself, and not
    sufficient: which column a trigger touches is inside its function body, and
    reading that is guesswork. The absence is the useful half -- with no such
    trigger, a column named for modification time is maintained by the
    application or not at all.
    """
    cur.execute(_UPDATE_TRIGGER_SQL, (schema_name, table_name))
    return [row[0] for row in cur.fetchall()]


def _sequence_from_default(column_default):
    """Sequence named by a `nextval('...')` default, when the column does not own it."""
    if not column_default:
        return None
    match = re.search(r"nextval\('([^']+)'", column_default)
    return match.group(1) if match else None


def require_monotonic_key(cur, schema_name, table_name, column, pk_columns=()):
    """Check whether `column` can serve as an INCREMENTAL watermark.

    A watermark is sound only when every row committed after a run carries a key
    above that run's bookmark. Three separable things have to hold, and they fail
    for different reasons:

    NOT NULL       a NULL key is never written to the bookmark, so those rows are
                   re-read on every run forever. Hard failure.
    total order    ties make paging non-deterministic at the boundary. A unique
                   column has this already; anything else needs the primary key
                   appended as a tiebreaker.
    commit order   the one that actually loses data, and the one nothing in the
                   schema records. YugabyteDB hands out sequence values in
                   per-connection blocks, so one session can be committing ids
                   1..100 while another commits 101..200: a run bookmarking 150
                   never sees id 40 committed a moment later. The block size is
                   the sequence's own CACHE, read here rather than assumed.

    Note this is not needed for FULL_TABLE. Keyset paging wants a total order,
    which every primary key has by definition, and monotonicity buys it nothing --
    a UUID key pages exactly as well.
    """
    cur.execute(_KEY_FACTS_SQL,
                (f'{schema_name}.{table_name}', schema_name, table_name, column))
    row = cur.fetchone()
    if row is None:
        return {'usable': False, 'column': column, 'tiebreaker': None,
                'hard_failures': [f'{column} does not exist on {schema_name}.{table_name}'],
                'risks': []}
    not_null, is_unique, owned_sequence, data_type, is_identity, column_default = row
    sequence_name = owned_sequence or _sequence_from_default(column_default)

    hard_failures, risks = [], []
    if not not_null:
        hard_failures.append(
            f'{column} is nullable. NULL is never written to the bookmark, so '
            f'NULL-keyed rows re-sync on every run.'
        )

    tiebreaker = None
    if not is_unique:
        tiebreaker = [c for c in pk_columns if c != column] or None
        if tiebreaker is None:
            hard_failures.append(
                f'{column} is not unique and the table has no primary key to break '
                f'ties with, so paging cannot be made deterministic.'
            )

    if sequence_name:
        cur.execute('SELECT seqcache FROM pg_sequence WHERE seqrelid = %s::regclass',
                    (sequence_name,))
        cached = cur.fetchone()
        cache = cached[0] if cached else None
        if not owned_sequence:
            risks.append(
                f'{column} draws from {sequence_name} through a plain default rather '
                f'than owning it, so the sequence can be dropped, repointed or reset '
                f'without any change to the column.'
            )
        if cache and cache > 1:
            risks.append(
                f'{column} draws from {sequence_name}, which caches {cache} values '
                f'per connection. Commit order does not follow key order across '
                f'{cache} x (concurrent writers). Lower the cache, carry a lag '
                f'window at least that wide, or use LOG_BASED.'
            )
    elif data_type.startswith(('timestamp', 'date', 'time')):
        risks.append(
            f'{column} is a timestamp, so it records transaction start time. A long '
            f'write transaction commits rows stamped before an already-advanced '
            f'bookmark. Carry a lag window wider than the longest write transaction, '
            f'or use LOG_BASED.'
        )
        if _name_rank(column.lower(), _MODIFIED_NAME_HINTS) is not None:
            triggers = _has_update_trigger(cur, schema_name, table_name)
            if not triggers:
                risks.append(
                    f'{column} is named for modification time but the table has no '
                    f'row-level UPDATE trigger, so nothing in the database maintains '
                    f'it. If it carries only a DEFAULT it is set on insert and never '
                    f'moves again, and updates are invisible despite the name.'
                )
            else:
                risks.append(
                    f'{column} may be maintained by {", ".join(triggers)}, but which '
                    f'column a trigger touches is not recorded anywhere -- confirm it '
                    f'sets {column} on every update path.'
                )
    else:
        risks.append(
            f'{column} is neither sequence-backed nor a timestamp, so nothing '
            f'guarantees it increases at all. Confirm the application only ever '
            f'assigns increasing values.'
        )

    return {'usable': not hard_failures, 'column': column, 'tiebreaker': tiebreaker,
            'hard_failures': hard_failures, 'risks': risks,
            'is_identity': is_identity, 'sequence': sequence_name}


# --------------------------------------------- replication-key keyset index

def replication_key_index_name(table_name, replication_key):
    return f'{table_name}_{replication_key}{INDEX_SUFFIX}'


def index_for_replication_key(table_name, replication_key, pk_columns):
    """Name of the index an INCREMENTAL scan should use.

    When the replication key IS the primary key -- a bigserial id used as the
    watermark -- the primary-key keyset index is already `(bucket, id)`, which is
    exactly what a replication-key index would be. Naming a separate one asks for
    an index nobody created; a hint that names a missing index is not an error,
    it is silently dropped, and the scan falls back to whatever the cost model
    prefers. Which, on a full drain, is a sequential scan and an external sort.
    """
    if list(pk_columns) == [replication_key]:
        return index_name(table_name)
    return replication_key_index_name(table_name, replication_key)


def replication_key_index_ddl(fq_table_name, table_name, replication_key,
                              pk_columns, buckets):
    """Bucketed index for an INCREMENTAL replication key.

    A plain (key ASC) index serves the watermark query, but a replication key is
    almost always a timestamp or a sequence -- values that only ever increase --
    so every insert lands at the tail of a range-sharded index and one tablet
    takes the whole write load. Bucketing spreads the tail across N tablets while
    keeping each bucket ordered, exactly as it does for the primary key.

    Three details make this the same shape as the primary-key index rather than a
    second idea:

    The discriminator hashes the PRIMARY KEY, not the replication key. Either
    works -- INCREMENTAL names every bucket rather than targeting one, so the
    bucket never has to be computable from the cursor, and both give the same
    plan. What separates them is where a batch lands.

    `now()` is transaction-start time, so every row written in one transaction
    carries the identical timestamp, and one timestamp hashes to one bucket. A
    9,000-row insert in a single transaction distributed 2960/2955/3085 across
    three buckets when hashed on the key, and 9000/0/0 when hashed on the
    timestamp -- the whole batch on one tablet, which is what the bucketing
    exists to prevent. Primary keys are distinct by construction and do not do
    this.

    (It is not a write-volume difference: the entry is a delete plus an insert
    either way, because the replication key is part of the index key in both
    designs. Measured at 3,000 storage writes for 1,000 updated rows on both.)

    Hashing the primary key also means one expression and one bucket count cover
    every index on the table.

    The primary key trails the replication key. That makes the order total, so a
    cursor can resume inside a group of rows sharing a timestamp instead of
    re-reading the whole group, and it makes the index UNIQUE -- which lets it
    answer from the index alone.

    The scan must name every bucket. Bounding a range does not need it
    (YugabyteDB bounds a trailing column under an unbounded leading one), but
    ORDER BY does: without the predicate the plan is a sort, and on a full drain
    a sequential scan and an external merge. incremental.py emits it.
    """
    if list(pk_columns) == [replication_key]:
        # the primary-key index already is this index -- see
        # index_for_replication_key, which is what the scan hints. Emitting the
        # replication-key form here names an index nothing uses and repeats the
        # key column, `(bucket ASC, "id" ASC, "id" ASC)`, which the server
        # accepts: a service owner running it builds a second copy of an index
        # they already have, and nothing ever reads it.
        return index_ddl(fq_table_name, table_name, pk_columns, buckets)

    trailing = ', '.join(f'{c} ASC' for c in quoted(pk_columns))
    return (
        f'CREATE UNIQUE INDEX '
        f'{replication_key_index_name(table_name, replication_key)} '
        f'ON {fq_table_name} '
        f'(({bucket_expr(pk_columns, buckets)}) ASC, "{replication_key}" ASC, '
        f'{trailing}) '
        f'SPLIT AT VALUES ({split_at_values(buckets)})'
    )


def replication_key_hint(table_name, replication_key, pk_columns=()):
    index = index_for_replication_key(table_name, replication_key, pk_columns)
    return f'/*+ IndexScan({table_name} {index}) */'
