"""INCREMENTAL replication against YugabyteDB.

One ordered statement per attempt, read through a named server-side cursor,
resuming from the Singer bookmark. The scan is retried on a transient source
error the way full_table's is, and the replacement statement is rebuilt from the
bookmark as it stands at that moment rather than from the one the run started
on.

WHAT `limit` MEANS ONCE RETRIES EXIST. `limit` caps ONE statement, so a run that
retries can emit more than `limit` rows in total -- up to `max_attempts * limit`
in the worst case, realistically about twice it, since a fault mid-scan means
the failed attempt emitted fewer than `limit` and the successful one resumes
past them. That is deliberate, and it is the cheaper of the two options.

Carrying a remaining-rows budget across attempts instead -- issuing `LIMIT
limit - rows_already_emitted` -- breaks two things for nothing. It makes the
cap the fault budget rather than the batch budget, so a fault costs throughput
on top of the backoff it already costs. And it silently disables the stalled
bookmark warning: a run that failed after `limit - 1` rows would retry with
`LIMIT 1`, emit one row, and `rows_saved < run_limit` would read that as "the
run drained everything there was" on a table where it drained nothing. The
degenerate case is worse still -- an attempt that died after the last row but
before the cursor closed would retry with `LIMIT 0` and emit nothing at all.

Overshooting the cap costs a longer run. Nothing downstream assumes a run
covers at most `limit` rows: the bookmark advances per row, the resume
predicate is `>=`, and the target upserts by primary key, so extra rows are
extra progress. `limit` bounds the work one statement does, which is what
bounds the memory and the time a single scan holds a cursor open; it was never
a transactional quantity.
"""

import copy
import time
import psycopg2
import psycopg2.extras
import singer

from singer import utils
from functools import partial
from singer import metrics

import tap_yugabyte.db as yb_db
import tap_yugabyte.keyset as keyset
from tap_yugabyte.retry import retry_read


LOGGER = singer.get_logger('tap_yugabyte')

UPDATE_BOOKMARK_PERIOD = 10000


# pylint: disable=invalid-name,missing-function-docstring
def fetch_max_replication_key(conn_config, replication_key, schema_name, table_name):
    with yb_db.open_connection(conn_config) as conn:
        with conn.cursor() as cur:
            max_key_sql = f"""
                SELECT max({yb_db.prepare_columns_sql(replication_key)})
                FROM {yb_db.fully_qualified_table_name(schema_name, table_name)}"""

            LOGGER.info("determine max replication key value: %s", max_key_sql)
            cur.execute(max_key_sql)
            max_key = cur.fetchone()[0]

            LOGGER.info("max replication key value: %s", max_key)
            return max_key


# pylint: disable=too-many-locals,too-many-statements
def sync_table(conn_info, stream, state, desired_columns, md_map):
    """Read the rows at or beyond the bookmark, restarting the scan on a
    transient source error.

    The scan is wrapped the way full_table's is, and for the same reason: a
    tablet leader election or a terminated backend ends the statement without
    saying anything about whether the query was wrong. Before this, a real
    `pg_terminate_backend` mid-scan aborted the run outright after 160,000 rows
    with an InterfaceError and zero retry attempts; only the Singer bookmark
    already on disk saved the next run.

    What makes re-running safe is the same property the bookmark already has:
    every emitted row advances `replication_key_value`, the resume predicate is
    `>=`, and the order is (replication key, primary key), so a replacement
    statement built from the current bookmark covers exactly the rows the run
    has not delivered -- plus the tie group it stopped inside, which it re-reads
    by design and the target upserts away.
    """
    time_extracted = utils.now()

    stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    if stream_version is None:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    schema_name = md_map.get(()).get('schema-name')

    # materialised rather than left a lazy map, because the statement is now
    # rebuilt once per attempt. `map` is an iterator: the first build consumes
    # it and every later one joins an empty sequence, emitting `SELECT  FROM
    # (...)`. That is SQLSTATE 42601, which the retry policy correctly calls
    # permanent -- so the first retry would have turned a recoverable fault into
    # a hard syntax failure. The column list is fixed for the whole run.
    escaped_columns = list(map(partial(yb_db.prepare_columns_for_select_sql, md_map=md_map),
                               desired_columns))

    activate_version_message = singer.ActivateVersionMessage(
        stream=yb_db.calculate_destination_stream_name(stream, md_map),
        version=stream_version)

    singer.write_message(activate_version_message)

    replication_key = md_map.get((), {}).get('replication-key')
    # the bookmark THIS RUN started on, captured once. _warn_if_bookmark_stalled
    # compares against it and it must stay the run's starting point rather than
    # the attempt's: a retry resuming from a bookmark an earlier attempt already
    # advanced has made progress, and comparing attempt-start to run-end would
    # report that as a stall.
    replication_key_value = singer.get_bookmark(state, stream['tap_stream_id'], 'replication_key_value')
    replication_key_sql_datatype = md_map.get(('properties', replication_key)).get('sql-datatype')

    hstore_available = yb_db.hstore_available(conn_info)
    buckets = conn_info.get('keyset_buckets', keyset.BUCKETS_DEFAULT)
    pk_columns = md_map.get((), {}).get('table-key-properties', [])

    # the reader survives across attempts so an error that left the session
    # usable does not pay for a reconnect; see scan().
    reader = {'conn': None}
    rows_saved = 0

    def _prepare(conn):
        """Once-per-connection setup: nothing here is undone by a rollback."""
        # Client side character encoding defaults to the value in postgresql.conf under client_encoding.
        # The server / db can also have its own configured encoding.
        with conn.cursor() as cur:
            cur.execute("show server_encoding")
            LOGGER.info("Current Server Encoding: %s", cur.fetchone()[0])
            cur.execute("show client_encoding")
            LOGGER.info("Current Client Encoding: %s", cur.fetchone()[0])

        if hstore_available:
            LOGGER.info("hstore is available")
            # a client-side type registration derived from a catalog lookup, so
            # it belongs to the connection and not to the transaction
            psycopg2.extras.register_hstore(conn)
        else:
            LOGGER.info("hstore is UNavailable")

    def _apply_scan_settings(conn):
        """The same settings full_table's scans need, re-issued every attempt.

        They must run on a plain cursor: the extraction below uses a named
        server-side cursor, which cannot carry SET.

        EVERY ATTEMPT, not only on a fresh connection. A plain `SET` is
        transactional -- a failed attempt's `with conn` rolls back, and the
        settings roll back with it (measured on this cluster: enable_seqscan
        off -> on and yb_max_merge_scan_streams 8 -> 0 across one ROLLBACK).
        A reconnect loses them for the more obvious reason. Either way the
        replacement statement would be correct and slow, which is the failure
        mode nothing in the result reveals.

        The extraction no longer DEPENDS on them. It used to lean on the
        storage-level merge scan, which yb_max_merge_scan_streams enables
        and which a named cursor silently does not perform -- see
        _get_select_sql. The statement is a UNION ALL under an outer ORDER
        BY now, so the ordering is a plan node (`Merge Append`) that the
        cursor honours, and the plan was measured identical with these
        settings and with none at all: Merge Append over N Index Only Scans,
        Heap Fetches 0, on an ANALYZEd table and on one never ANALYZEd.
        They stay because they are cheap, because this connection runs
        nothing else, and because enable_seqscan = off is still the thing
        that keeps a cost estimate from preferring a sequential scan on a
        table shape nobody has measured yet.
        """
        with conn.cursor() as setup:
            if keyset.merge_scan_available(setup):
                for setting in keyset.scan_settings_sql(buckets):
                    setup.execute(setting)

    with metrics.record_counter(None) as counter:

        def scan():
            nonlocal state, rows_saved

            # REBUILT HERE, from the bookmark as it stands now, rather than once
            # before the cursor is opened. Every row already emitted has moved
            # the bookmark past itself, so the replacement statement covers
            # exactly the remainder; hoisting this out of the retried operation
            # would replay the run from the position it started at, re-emitting
            # everything the failed attempt had already delivered.
            # full_table._scan_bucket rebuilds its ladder in the same place for
            # the same reason.
            select_sql = _get_select_sql({"escaped_columns": escaped_columns,
                                          "replication_key": replication_key,
                                          "replication_key_sql_datatype": replication_key_sql_datatype,
                                          "replication_key_value": singer.get_bookmark(
                                              state, stream['tap_stream_id'],
                                              'replication_key_value'),
                                          "schema_name": schema_name,
                                          "table_name": stream['table_name'],
                                          # per ATTEMPT, not per run -- see the
                                          # module docstring on LIMIT and retries
                                          "limit": conn_info['limit'],
                                          # same default as full_table: absent from
                                          # config this used to be None, which emits
                                          # no bucket predicate and no hint at all
                                          "keyset_buckets": buckets,
                                          "pk_columns": pk_columns,
                                          })

            # A failed attempt may have lost the connection, not just the
            # statement: a terminated backend or a tablet leader election
            # leaves it closed, and psycopg2 then raises InterfaceError
            # ('connection already closed') on the first use of the dead
            # cursor. That error carries no SQLSTATE, so the policy reads it as
            # transient and re-runs an operation that cannot ever succeed --
            # the exact shape of the FastSync export bug fixed alongside this.
            # Reconnecting here rather than in on_retry keeps a failure to
            # reconnect inside the retry loop, where it is itself retried;
            # raising out of on_retry would escape the loop entirely.
            #
            # Only a connection psycopg2 has actually marked closed is
            # replaced, so an error that leaves the session usable -- a
            # catalog-version bump, say -- retries on the same connection.
            fresh = reader['conn'] is None or getattr(reader['conn'], 'closed', 0)
            if fresh:
                if reader['conn'] is not None:
                    LOGGER.info('Source connection is closed; reopening it for '
                                'this attempt')
                    try:
                        reader['conn'].close()
                    except psycopg2.Error:
                        LOGGER.warning('Could not close the lost source connection')
                reader['conn'] = yb_db.open_connection(conn_info)

            # `with conn` is a transaction, not the connection's lifetime: on
            # the way out of a failed attempt it rolls back, which is what
            # leaves a surviving session clean enough to reuse.
            with reader['conn'] as conn:
                if fresh:
                    _prepare(conn)
                _apply_scan_settings(conn)

                # reset per attempt. _warn_if_bookmark_stalled asks whether the
                # LIMIT truncated the statement that actually COMPLETED; a
                # running total across attempts would answer a different
                # question and warn on a run that merely retried its way past
                # the cap. See that function's docstring.
                rows_saved = 0

                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='pipelinewise') as cur:
                    cur.itersize = yb_db.CURSOR_ITER_SIZE
                    LOGGER.info("Beginning new incremental replication sync %s", stream_version)
                    LOGGER.info('select statement: %s with itersize %s', select_sql, cur.itersize)
                    cur.execute(select_sql)

                    for rec in cur:
                        record_message = yb_db.selected_row_to_singer_message(stream,
                                                                               rec,
                                                                               stream_version,
                                                                               desired_columns,
                                                                               time_extracted,
                                                                               md_map)

                        singer.write_message(record_message)
                        rows_saved += 1

                        #Picking a replication_key with NULL values will result in it ALWAYS been synced which is not great
                        #event worse would be allowing the NULL value to enter into the state
                        if record_message.record[replication_key] is not None:
                            state = singer.write_bookmark(state,
                                                          stream['tap_stream_id'],
                                                          'replication_key_value',
                                                          record_message.record[replication_key])

                        if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                        counter.increment()

        retry_read(scan, f"incremental scan of {stream['tap_stream_id']}")

    _warn_if_bookmark_stalled(stream, state, replication_key,
                              replication_key_value, rows_saved,
                              conn_info['limit'])
    return state


def _warn_if_bookmark_stalled(stream, state, replication_key, started_at,
                              rows_saved, run_limit):
    """Say so when a run emitted rows and the bookmark did not move at all.

    Singer INCREMENTAL state carries one scalar, `replication_key_value`, and the
    resume predicate is `>=`, so a run advances only by reaching a value LARGER
    than the one it started on -- which means draining the whole group of rows
    sharing that starting value first. When the group holds at least `limit`
    rows, the run reads `limit` rows all carrying it, writes back the bookmark it
    already had, and the next run issues a byte-identical statement. The sync
    never progresses, and nothing said so: every run looks healthy, emitting a
    full batch of records on schedule. See INDEXES.md, "Resuming inside a tie
    group".

    This detects and reports. It does not fix: fixing means a composite bookmark,
    which is not a shape Singer INCREMENTAL state has.

    Three conditions have to hold together, and dropping any one of them makes
    this fire on a healthy sync:

    rows were emitted       an empty run moves no bookmark and is simply "no new
                            data".
    there WAS a bookmark    a first run has none, so it cannot fail to advance
                            one; it writes the group's value and the run after it
                            is the one that can stall.
    the bookmark is unchanged
                            a run that ends inside a tie having made progress has
                            moved it. Re-reading the tail of the last group on
                            the next run is at-least-once by design, not this.
    the LIMIT truncated it  without this a single-row table warns on every run:
                            it re-reads its one row, the bookmark cannot move
                            because nothing lies beyond it, and the sync is just
                            caught up. A run returning fewer rows than its cap
                            drained everything there was, and with no `limit`
                            configured a run always drains to the end.

    Bookmark is compared against bookmark, not against record values: the
    bookmark is whatever `write_bookmark` stored -- a string for a timestamp, an
    int for a sequence -- and comparing a live column value against the
    JSON-decoded state value would differ by type and silently never match.

    WHAT A RETRY DOES TO EACH ARGUMENT. A run is now a sequence of statements,
    of which only the last completes, so the two counts it is fed are
    deliberately taken from different scopes.

    `started_at` is the RUN's opening bookmark. A retry that resumes from a
    bookmark an earlier attempt advanced has made progress, and measuring from
    the attempt would call that a stall. The cost is that a run which advances
    and THEN stalls does not warn -- the next run starts inside the stalled
    group and warns then, so detection is delayed by one run, never lost. That
    is also exactly what this did before retries existed.

    `rows_saved` is the SUCCESSFUL ATTEMPT's count, not the run's total. The
    question this asks is whether the LIMIT truncated the statement that
    actually finished, and only that statement's own row count answers it. A
    running total would warn on a run whose attempts summed past the cap while
    the final statement drained the table short of it -- a caught-up sync, the
    case the `rows_saved < run_limit` guard exists to excuse. Taken per attempt
    the four conditions keep meaning exactly what they meant before: a group
    bigger than one statement's cap still warns (the final attempt re-reads it
    from `>=` and is truncated again), and a group smaller than the cap still
    does not.
    """
    if not rows_saved or started_at is None or not run_limit:
        return
    if rows_saved < run_limit:
        return
    ended_at = singer.get_bookmark(state, stream['tap_stream_id'],
                                   'replication_key_value')
    if ended_at != started_at:
        return

    LOGGER.warning(
        'INCREMENTAL sync of %s MADE NO PROGRESS and cannot make any: all %s rows '
        'it emitted carry the same %s (%r), the run stopped on LIMIT %s, and the '
        'bookmark ends where it started. The bookmark is one scalar and the resume '
        'predicate is >=, so the next run reads exactly these rows again -- this '
        'sync will never advance past %r. At least %s rows share that value, and a '
        'run reads %s. Raise limit above the size of that group, choose a '
        'replication key with higher cardinality, or replicate this table with '
        'LOG_BASED.',
        stream['tap_stream_id'], rows_saved, replication_key, started_at,
        run_limit, started_at, rows_saved, run_limit)


def _get_select_sql(params):
    """Build the extraction query.

    Bucketed, this is a `UNION ALL` of one ordered branch per bucket under an
    OUTER `ORDER BY`. THAT OUTER `ORDER BY` IS LOAD-BEARING AND MUST NEVER BE
    OMITTED. It is what makes this correct rather than lucky: the planner is
    obliged to satisfy it, so it emits `Merge Append` where the branches already
    supply the order and a `Sort` where they cannot -- correctness never depends
    on `Append` happening to emit its children in branch order. Drop it and the
    shape is a bare `Append`, which measures exactly as badly as what it
    replaced.

    WHAT IT REPLACED, AND WHY. This used to be one statement naming every bucket,
    `WHERE (yb_hash_code(id) % N) IN (0 ... N-1) ORDER BY key, pk`, relying on
    YugabyteDB's storage-level merge scan to supply the order. sync_table reads
    that through a NAMED server-side cursor, and UNDER A CURSOR YUGABYTEDB DOES
    NOT PERFORM THE MERGE: the N per-bucket streams come back concatenated, each
    ascending internally and the whole not ascending. Measured on rt.healthy,
    40,000 rows, same session, same settings, only the cursor varying:

        cursor  rows    ORDER BY violations  bucket changes along the stream
        plain   40,000  0                    26,712   (interleaved -- merged)
        named   40,000  2                    2        (concatenated -- N-1)

    `EXPLAIN` reports `Merge Streams: 3` in BOTH cases; the plans are
    byte-identical. The defect is invisible from any plan, and only comparing
    returned rows shows it.

    It lost data silently. With a `LIMIT` the run takes the first n rows of the
    concatenation -- essentially one bucket -- bookmarks that bucket's high-water
    mark, and permanently skips every row in the other buckets below it while
    reporting success. Measured against rt.healthy by driving sync_table to
    "caught up" and collecting every id it emitted: 19,631 of 40,000 rows with
    limit 10,000, 15,572 with limit 3,000. With `Merge Append`, both: 40,000 of
    40,000, nothing missing.

    The bucket predicate is still per-bucket rather than absent, because the
    index is led by the discriminator and an equality against it is the access
    pattern hash sharding serves. The bookmark rides inside each branch so both
    reach the same `Index Cond`, which is what terminates early under the
    `LIMIT`: with `LIMIT 10000` over three buckets each branch reads 4,096 index
    rows rather than its share of the table.

    No index hint is emitted; see keyset.bucket_branches_sql, which measured
    every place one could go.

    The subquery, labelled yb_speedup_trick as in tap-postgres, keeps the
    column-expression projection from defeating the index scan.
    """
    escaped_columns = params['escaped_columns']
    replication_key = yb_db.prepare_columns_sql(params['replication_key'])
    replication_key_sql_datatype = params['replication_key_sql_datatype']
    replication_key_value = params['replication_key_value']
    schema_name = params['schema_name']
    table_name = params['table_name']
    pk_columns = params.get('pk_columns') or []
    # the index is only bucketed when there is a primary key to hash
    buckets = params.get('keyset_buckets') if pk_columns else None

    limit_statement = f'LIMIT {params["limit"]}' if params['limit'] else ''

    bookmark_predicate = None
    if replication_key_value:
        # cast to the column's own discovered type: a timestamptz value compared
        # against a timestamp column is accepted as an index condition and then
        # rechecked against every row, which looks identical in the plan
        bookmark_predicate = (
            f"{replication_key} >= '{replication_key_value}'"
            f'::{replication_key_sql_datatype}'
        )

    # the primary key trails the replication key in the index and in the order, so
    # rows sharing a replication-key value come back in the same sequence on every
    # run. The bookmark is still the replication key alone and the predicate is
    # still >=, so a resume re-reads that group -- deterministically, rather than
    # in whatever order the storage layer happened to return.
    # dedupe: when the replication key IS the primary key this would otherwise
    # emit `ORDER BY "id" ASC, "id" ASC`
    ordering = list(dict.fromkeys([params['replication_key']] + list(pk_columns)))
    order_by = keyset.order_by_sql(ordering)

    fq_table_name = yb_db.fully_qualified_table_name(schema_name, table_name)

    # No primary key, or no bucket count: there is nothing to hash, so there are
    # no branches and no discriminator to name. One plain ordered statement, the
    # same one this has always emitted -- a single stream is in order under any
    # cursor, because nothing is being merged. The stray leading space before
    # SELECT is deliberate: it is where the (now removed) hint used to be
    # interpolated, and keeping it makes this path byte-identical to the previous
    # statement rather than merely equivalent to it.
    if not buckets:
        where_statement = (f'WHERE {bookmark_predicate}'
                           if bookmark_predicate else '')
        return f"""
    SELECT {','.join(escaped_columns)}
    FROM (
         SELECT *
        FROM {fq_table_name}
        {where_statement}
        ORDER BY {order_by} {limit_statement}
    ) yb_speedup_trick;"""

    # the branches interpolate the bookmark as a literal and the statement is
    # executed with no parameters, so the modulo is NOT doubled. Handing psycopg2
    # a doubled `%%` here does not produce a slow query, it produces a malformed
    # statement; handing it a single `%` in a statement that DOES carry
    # parameters raises IndexError. See keyset.bucket_expr.
    branches = keyset.bucket_branches_sql(fq_table_name, pk_columns, buckets,
                                          order_by, bookmark_predicate)

    # the outer ORDER BY is the whole point -- see the docstring
    return f"""
    SELECT {','.join(escaped_columns)}
    FROM (
{branches}
    ) yb_speedup_trick ORDER BY {order_by} {limit_statement};"""
