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


def tuple_sql(pk_columns):
    """Row constructor for keyset comparison. Correct and index-usable in a
    WHERE clause -- unlike in ORDER BY, where it defeats the index."""
    return f"({', '.join(quoted(pk_columns))})"


def placeholders(pk_columns):
    return f"({', '.join(['%s'] * len(pk_columns))})"


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


def merge_scan_guc_sql(buckets):
    """Session setting that lets an index scan merge the per-bucket streams.

    Without it the planner has no way to produce ordered output from a
    multi-valued leading column and falls back to a sort over the whole result,
    even when the index condition is already right.
    """
    return f'SET yb_max_merge_scan_streams = {max(buckets, 8)}'


def index_name(table_name):
    return f'{table_name}{INDEX_SUFFIX}'


def index_ddl(fq_table_name, table_name, pk_columns, buckets, tablets=None):
    """DDL for the prerequisite index.

    Uniqueness is free -- the bucket is a function of the key, so (bucket, key)
    is unique exactly when the key is -- and it lets the index back a keyset
    cursor without a recheck against the base table.
    """
    split = f' SPLIT INTO {tablets} TABLETS' if tablets else ''
    return (
        f'CREATE UNIQUE INDEX {index_name(table_name)} ON {fq_table_name} '
        f'(({bucket_expr(pk_columns, buckets)}) HASH, {order_by_sql(pk_columns)})'
        f'{split}'
    )


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
                kind, group, hint_rank = 'created_timestamp', 2, created_rank
            else:
                kind, group, hint_rank = 'unknown_timestamp', 3, 0
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
_RANGE_INDEX_SQL = """
SELECT i.relname,
       pg_get_indexdef(i.oid)                       AS indexdef,
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
    Runs inside a savepoint because the rejection is an error, which would
    otherwise poison the surrounding transaction.
    """
    cur.execute('SAVEPOINT yb_hash_probe')
    try:
        cur.execute(f'SELECT yb_hash_code(NULL::{type_name})')
        cur.fetchone()
        cur.execute('RELEASE SAVEPOINT yb_hash_probe')
        return True
    except psycopg2.Error:
        cur.execute('ROLLBACK TO SAVEPOINT yb_hash_probe')
        return False


def _leading_column_is_ordered(indexdef, column):
    """True when `column` leads the index in sorted, not hashed, order."""
    inside = indexdef[indexdef.index('(') + 1:]
    leading = inside.split(',')[0].strip()
    return leading.startswith(f'{column} ') and 'HASH' not in leading


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
    for name, indexdef, is_partial, predicate in cur.fetchall():
        if not _leading_column_is_ordered(indexdef, boundary_column):
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
    elif data_type.startswith(('timestamp', 'date')):
        risks.append(
            f'{column} is a timestamp, so it records transaction start time. A long '
            f'write transaction commits rows stamped before an already-advanced '
            f'bookmark. Carry a lag window wider than the longest write transaction, '
            f'or use LOG_BASED.'
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
