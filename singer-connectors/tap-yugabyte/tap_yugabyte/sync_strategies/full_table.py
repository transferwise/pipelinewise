"""Full-table replication against YugabyteDB.

The resumable path scans the table one hash bucket at a time, under a
prerequisite index that supplies an order the primary key does not have (see
tap_yugabyte.keyset). Buckets have no ordering relationship to one another and a
full-table sync reads every one of them to completion, so they are scanned
concurrently, one connection per bucket, rather than serialised behind a single
cursor.

Every statement here is a SELECT and every bucket bookmarks its own position, so
a failed scan is restarted rather than repaired: the replacement statements are
rebuilt from the bucket's current bookmark and pick up where the stream stopped.

A resumed bucket is more than one statement. A composite key only seeks when the
equality prefix of the bookmark is stated as an equality, so the resume is issued
as one statement per prefix, in ascending order, sequentially -- see
keyset.keyset_branches and _scan_bucket.

One consequence is worth stating plainly. The rungs share a connection and a
transaction but not a snapshot: READ COMMITTED gives each statement its own, and
that is live here rather than mapped away (measured on this cluster -- a value
committed by another session between two statements of one transaction was
visible to the second). So a resumed bucket reads at several instants where it
used to read at one. It is the same inconsistency the sync already carries
between buckets and across retries, not a new kind, and `snapshot_hybrid_time`
removes it here exactly as it does there by pinning every statement on the
connection to one hybrid time.
"""

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import psycopg2
import psycopg2.extras
import singer
from singer import metrics, utils

import tap_yugabyte.db as yb_db
import tap_yugabyte.keyset as keyset
from tap_yugabyte.retry import retry_read

LOGGER = singer.get_logger('tap_yugabyte')

UPDATE_BOOKMARK_PERIOD = 1000


def _escaped_columns(desired_columns, md_map):
    return list(map(partial(yb_db.prepare_columns_for_select_sql, md_map=md_map),
                    desired_columns))


def _open_reader(conn_info, hybrid_time=None, security_definer_proc=None):
    """A connection ready to stream rows: hstore registered if present, and
    pinned to the sync's snapshot so every worker reads the same instant."""
    conn = yb_db.open_connection(conn_info)
    if yb_db.hstore_available(conn_info):
        psycopg2.extras.register_hstore(conn)
    keyset.pin_snapshot(conn, hybrid_time, security_definer_proc)
    return conn


# ---------------------------------------------------------------- views


def sync_view(conn_info, stream, state, desired_columns, md_map):
    """Views have no primary key and no bucket index, so this scan cannot resume;
    a failure restarts it from the beginning."""
    time_extracted = utils.now()
    first_run = singer.get_bookmark(state, stream['tap_stream_id'], 'version') is None
    nascent_stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state, stream['tap_stream_id'], 'version',
                                  nascent_stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    schema_name = md_map.get(()).get('schema-name')
    escaped_columns = map(yb_db.prepare_columns_sql, desired_columns)
    fq_table_name = yb_db.fully_qualified_table_name(schema_name, stream['table_name'])

    activate_version_message = singer.ActivateVersionMessage(
        stream=yb_db.calculate_destination_stream_name(stream, md_map),
        version=nascent_stream_version)
    if first_run:
        singer.write_message(activate_version_message)

    select_sql = f"SELECT {','.join(escaped_columns)} FROM {fq_table_name}"

    def scan():
        with metrics.record_counter(None) as counter:
            with yb_db.open_connection(conn_info) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor,
                                 name='stitch_cursor') as cur:
                    cur.itersize = yb_db.CURSOR_ITER_SIZE
                    LOGGER.info('select %s with itersize %s', select_sql, cur.itersize)
                    cur.execute(select_sql)
                    rows_saved = 0
                    for rec in cur:
                        singer.write_message(yb_db.selected_row_to_singer_message(
                            stream, rec, nascent_stream_version, desired_columns,
                            time_extracted, md_map))
                        rows_saved += 1
                        if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                            singer.write_message(
                                singer.StateMessage(value=copy.deepcopy(state)))
                        counter.increment()

    retry_read(scan, f"view scan of {stream['tap_stream_id']}")
    singer.write_message(activate_version_message)
    return state


# ------------------------------------------------------- no primary key


def _sync_table_without_pk(conn_info, stream, state, desired_columns, md_map):
    """Plain, non-resumable full scan for tables with no usable primary key."""
    time_extracted = utils.now()
    tap_stream_id = stream['tap_stream_id']
    schema_name = md_map.get(()).get('schema-name')
    fq_table_name = yb_db.fully_qualified_table_name(schema_name, stream['table_name'])

    first_run = singer.get_bookmark(state, tap_stream_id, 'version') is None
    nascent_stream_version = int(time.time() * 1000)
    state = singer.write_bookmark(state, tap_stream_id, 'version', nascent_stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    activate_version_message = singer.ActivateVersionMessage(
        stream=yb_db.calculate_destination_stream_name(stream, md_map),
        version=nascent_stream_version)
    if first_run:
        singer.write_message(activate_version_message)

    LOGGER.info('Table %s has no primary key: syncing with a non-resumable full scan',
                tap_stream_id)
    select_sql = (f"SELECT {','.join(_escaped_columns(desired_columns, md_map))} "
                  f'FROM {fq_table_name}')

    def scan():
        with metrics.record_counter(None) as counter:
            with yb_db.open_connection(conn_info) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor,
                                 name='stitch_cursor') as cur:
                    cur.itersize = yb_db.CURSOR_ITER_SIZE
                    LOGGER.info('select %s with itersize %s', select_sql, cur.itersize)
                    cur.execute(select_sql)
                    rows_saved = 0
                    for rec in cur:
                        singer.write_message(yb_db.selected_row_to_singer_message(
                            stream, rec, nascent_stream_version, desired_columns,
                            time_extracted, md_map))
                        rows_saved += 1
                        if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                            singer.write_message(
                                singer.StateMessage(value=copy.deepcopy(state)))
                        counter.increment()

    retry_read(scan, f'full scan of {tap_stream_id}')
    singer.write_message(activate_version_message)
    return state


# ------------------------------------------------- bucketed keyset scan


def _fetch_max_pk_values(conn_info, fq_table_name, table_name, pk_columns, buckets):
    """Snapshot the largest primary-key tuple, bounding the scan against inserts
    that land while it runs.

    Reads N index entries, one per bucket, in whichever form this server can
    serve -- see keyset.max_pk_values_sql. Ordering by the key columns
    individually keeps it on the index either way; a ROW() expression here would
    sort the table to return a single row.
    """
    def probe():
        # pinned like the bucket scans are: on a pinned run this bound was read
        # at wall clock while the scans read an earlier instant, so the two came
        # from different moments. The direction was safe -- a bound at or above
        # what the scans can see only widens it -- but a bound and a scan that
        # disagree about when "now" is are hard to reason about, and the fix is
        # to open the probe the same way every other reader is opened.
        with _open_reader(conn_info, conn_info.get('snapshot_hybrid_time'),
                          conn_info.get('yb_read_time_proc')) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                merge_scan = keyset.merge_scan_available(cur)
                if merge_scan:
                    for setting in keyset.scan_settings_sql(buckets):
                        cur.execute(setting)
                select_sql = keyset.max_pk_values_sql(
                    fq_table_name, table_name, pk_columns, buckets, merge_scan)
                LOGGER.info('select %s', select_sql)
                cur.execute(select_sql)
                row = cur.fetchone()
                return None if row is None else [row[i] for i in range(len(pk_columns))]

    return retry_read(probe, f'max primary key probe on {fq_table_name}')


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
def _scan_bucket(conn_info, stream, state, desired_columns, md_map, pk_columns,
                 buckets, bucket, fq_table_name, table_name, max_pk_values,
                 stream_version, time_extracted, emit, read_bookmark, counter):
    """Read one bucket to completion on its own connection.

    A fresh bucket is one statement. A RESUMED bucket is a short ordered
    sequence of them -- one per equality prefix of the bookmarked key, most
    specific first -- because that is the only shape that makes a composite key
    seek. See keyset.keyset_branches for why every single-statement form was
    rejected; a single-column key still yields exactly one statement.

    The sequence is rebuilt from the bucket's bookmark on every attempt, so a
    retry after a read restart or a lost connection resumes from the last row
    that reached the target instead of replaying the bucket.
    """
    escaped = _escaped_columns(desired_columns, md_map)
    pk_indices = [desired_columns.index(pk) for pk in pk_columns]
    # this statement is always executed with a parameter sequence, so the modulo
    # has to survive psycopg2's placeholder interpolation
    bucket_sql = keyset.bucket_expr(pk_columns, buckets, escape_percent=True)
    hybrid_time = conn_info.get('snapshot_hybrid_time')
    proc = conn_info.get('yb_read_time_proc')

    def statements(last_pk_values):
        """The statements this attempt has to run, in the order it must run them.

        Every one of them carries the bucket equality and, when the scan is
        bounded, the upper bound -- which stays the conjunctive expansion,
        because it is a stop condition rather than a seek (see
        keyset.keyset_predicate).

        Only the lower bound is laddered, and only when there is one: with no
        bookmark there is nothing to resume past, so the list is the single
        unbounded-below statement this path has always issued.
        """
        upper_sql, upper_params = None, []
        if max_pk_values:
            sql, order = keyset.keyset_predicate(pk_columns, after=False)
            upper_sql = sql
            upper_params = keyset.keyset_params(max_pk_values, order)

        if last_pk_values:
            lower_bounds = [(sql, keyset.keyset_params(last_pk_values, order))
                            for sql, order in keyset.keyset_branches(pk_columns)]
        else:
            lower_bounds = [(None, [])]

        built = []
        for lower_sql, lower_params in lower_bounds:
            predicates = [f'{bucket_sql} = {bucket}']
            params = []
            if lower_sql is not None:
                predicates.append(lower_sql)
                params.extend(lower_params)
            if upper_sql is not None:
                predicates.append(upper_sql)
                params.extend(upper_params)
            built.append((
                f'{keyset.index_hint(table_name)} '
                f"SELECT {','.join(escaped)} FROM {fq_table_name} "
                f'WHERE {" AND ".join(predicates)} '
                f'ORDER BY {keyset.order_by_sql(pk_columns)}',
                params))
        return built

    def scan():
        # rebuilt here, not once outside: on a retry the bookmark has advanced
        # past everything this attempt already emitted, and the ladder built
        # from it covers exactly the remainder. Hoisting this out of the retry
        # would replay from the position the scan started at.
        branches = statements(read_bookmark(bucket))

        with _open_reader(conn_info, hybrid_time, proc) as conn:
            with conn.cursor() as setup:
                if keyset.merge_scan_available(setup):
                    for setting in keyset.scan_settings_sql(buckets):
                        setup.execute(setting)
            # one counter across the whole sequence: the flush period is a
            # property of the bucket's stream, not of an individual statement
            rows_saved = 0
            for branch, (select_sql, params) in enumerate(branches):
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor,
                                 name=f'stitch_cursor_bucket_{bucket}_{branch}') as cur:
                    cur.itersize = yb_db.CURSOR_ITER_SIZE
                    LOGGER.info(
                        'bucket %s branch %s/%s: select %s with itersize %s, params %s',
                        bucket, branch + 1, len(branches), select_sql,
                        cur.itersize, params)
                    cur.execute(select_sql, params)
                    for rec in cur:
                        message = yb_db.selected_row_to_singer_message(
                            stream, rec, stream_version, desired_columns,
                            time_extracted, md_map)
                        rows_saved += 1
                        emit(message, bucket, [rec[i] for i in pk_indices],
                             flush=rows_saved % UPDATE_BOOKMARK_PERIOD == 0)
                        counter.increment()

    # ONE retry_read around the whole sequence, never one per statement.
    #
    # Why this is safe. The rungs are disjoint, consecutive and ascending, so
    # the rows this attempt emitted are exactly the keys in (old bookmark,
    # current bookmark] -- whichever rung it died on, and whether or not it
    # died mid-rung. `emit` advances the bookmark on every row, so rebuilding
    # the ladder from it covers exactly the complement: nothing replayed,
    # nothing skipped. Rungs the attempt already drained rebuild as empty
    # probes rather than being skipped, which costs an index seek each and
    # keeps the structure from having to remember how far it got.
    #
    # Why per-statement retry is not. It would rebuild a full ladder from a
    # bookmark that has already passed the earlier rungs, so a failure inside
    # `a = X AND b > Y` would come back with `a > X` attached -- and the outer
    # loop would then run `a > X` again. Every row after the current leading
    # value, twice.
    retry_read(scan, f"bucket {bucket} of {stream['tap_stream_id']}")
    return bucket


# pylint: disable=too-many-locals,too-many-statements
def _sync_table_with_pk(conn_info, stream, state, desired_columns, md_map, pk_columns):
    """Resumable full-table scan, one concurrent worker per hash bucket."""
    time_extracted = utils.now()
    tap_stream_id = stream['tap_stream_id']
    schema_name = md_map.get(()).get('schema-name')
    table_name = stream['table_name']
    fq_table_name = yb_db.fully_qualified_table_name(schema_name, table_name)
    buckets = conn_info.get('keyset_buckets', keyset.BUCKETS_DEFAULT)

    first_run = singer.get_bookmark(state, tap_stream_id, 'version') is None
    max_pk_values = singer.get_bookmark(state, tap_stream_id, 'max_pk_values')

    # a max_pk_values bookmark means a previous run was interrupted mid-scan;
    # reuse its stream version instead of minting a new one
    if max_pk_values is None:
        nascent_stream_version = int(time.time() * 1000)
    else:
        nascent_stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    state = singer.write_bookmark(state, tap_stream_id, 'version', nascent_stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    if max_pk_values is None:
        max_pk_values = _fetch_max_pk_values(conn_info, fq_table_name, table_name,
                                             pk_columns, buckets)
        state = singer.write_bookmark(state, tap_stream_id, 'max_pk_values', max_pk_values)
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    activate_version_message = singer.ActivateVersionMessage(
        stream=yb_db.calculate_destination_stream_name(stream, md_map),
        version=nascent_stream_version)
    if first_run:
        singer.write_message(activate_version_message)

    if max_pk_values is None:
        # table is empty: nothing to bound the scan against
        singer.write_message(activate_version_message)
        return state

    write_lock = threading.Lock()

    # Before bucketing, last_pk_fetched was a single position for one sequential
    # cursor; it says nothing about where any individual bucket got to, so a state
    # file written by an older tap is discarded rather than misread. The scan
    # restarts within the same max_pk_values bound, and the target upserts by
    # primary key, so re-reading rows it already has costs time and nothing else.
    legacy_bookmark = singer.get_bookmark(state, tap_stream_id, 'last_pk_fetched')
    if legacy_bookmark is not None and not isinstance(legacy_bookmark, dict):
        LOGGER.info(
            'Discarding pre-bucketing last_pk_fetched bookmark %s for %s; the '
            'bounded scan restarts and the target deduplicates by primary key',
            legacy_bookmark, tap_stream_id)
        state = singer.write_bookmark(state, tap_stream_id, 'last_pk_fetched', {})

    # Every per-bucket bookmark is a statement about `yb_hash_code(pk) % N`, and
    # under a different N it is a statement about partitions that do not exist:
    # `completed_buckets` retires a bucket number whose rows are now a different
    # set, and `last_pk_fetched` lower-bounds a bucket it never scanned. Both
    # skip rows, both report success, and neither is detectable from the bookmark
    # itself -- nothing in `{"2": [2389]}` says which N produced it. Measured on
    # a 3,000-row table, resuming an interrupted N=8 scan at N=2: 1,965 rows
    # delivered, every bookmark cleared, ACTIVATE_VERSION emitted, 1,035 rows
    # silently missing.
    #
    # validate_index does not cover this. It compares the config against the
    # live index, so the mismatched cases (config moved, index did not, or the
    # reverse) are caught before any row moves -- and the case that loses rows is
    # the one where the operator did exactly what the docs say and rebuilt both.
    #
    # An ABSENT keyset_buckets counts as a change: a state written before this
    # bookmark existed carries per-bucket positions whose N cannot be recovered,
    # so the only safe reading is that it is unknown. Discarding restarts the
    # scan inside the same max_pk_values bound -- the bound and the stream
    # version are N-independent and are deliberately kept -- and the target
    # upserts by primary key, so it costs time and nothing else.
    bookmarked_buckets = singer.get_bookmark(state, tap_stream_id, 'keyset_buckets')
    if bookmarked_buckets != buckets and (
            singer.get_bookmark(state, tap_stream_id, 'last_pk_fetched')
            or singer.get_bookmark(state, tap_stream_id, 'completed_buckets')):
        LOGGER.warning(
            'Discarding per-bucket resume bookmarks for %s: they were written '
            'for keyset_buckets=%s and this run is configured for %s, so every '
            'bucket number in them names a different set of rows. The bounded '
            'scan restarts and the target deduplicates by primary key.',
            tap_stream_id, bookmarked_buckets, buckets)
        state = singer.write_bookmark(state, tap_stream_id, 'last_pk_fetched', {})
        state = singer.write_bookmark(state, tap_stream_id, 'completed_buckets', None)
    state = singer.write_bookmark(state, tap_stream_id, 'keyset_buckets', buckets)

    def read_bookmark(bucket):
        with write_lock:
            fetched = singer.get_bookmark(state, tap_stream_id, 'last_pk_fetched') or {}
            return fetched.get(str(bucket))

    def emit(message, bucket, last_pk, flush):
        # stdout is one pipe and the bookmark map is shared, so the write and the
        # bookmark update are the only part of this that must not be concurrent
        with write_lock:
            singer.write_message(message)
            fetched = dict(
                singer.get_bookmark(state, tap_stream_id, 'last_pk_fetched') or {})
            fetched[str(bucket)] = last_pk
            singer.write_bookmark(state, tap_stream_id, 'last_pk_fetched', fetched)
            if flush:
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    completed = set(singer.get_bookmark(state, tap_stream_id, 'completed_buckets') or [])
    pending = [b for b in range(buckets) if b not in completed]
    LOGGER.info('Full table replication %s of %s: %s of %s buckets pending',
                nascent_stream_version, tap_stream_id, len(pending), buckets)

    with metrics.record_counter(None) as counter:
        with ThreadPoolExecutor(max_workers=min(buckets, len(pending) or 1)) as pool:
            futures = [
                pool.submit(_scan_bucket, conn_info, stream, state, desired_columns,
                            md_map, pk_columns, buckets, bucket, fq_table_name,
                            table_name, max_pk_values, nascent_stream_version,
                            time_extracted, emit, read_bookmark, counter)
                for bucket in pending
            ]
            for future in futures:
                # a bucket that finished keeps its bookmark, so a failure here
                # costs only the buckets still in flight
                done_bucket = future.result()
                with write_lock:
                    state = singer.write_bookmark(
                        state, tap_stream_id, 'completed_buckets',
                        sorted(completed | {done_bucket}))
                    completed.add(done_bucket)

    # the scan completed: discard the resume bookmarks, they only matter mid-scan
    state = singer.write_bookmark(state, tap_stream_id, 'max_pk_values', None)
    state = singer.write_bookmark(state, tap_stream_id, 'last_pk_fetched', None)
    state = singer.write_bookmark(state, tap_stream_id, 'completed_buckets', None)
    state = singer.write_bookmark(state, tap_stream_id, 'keyset_buckets', None)

    singer.write_message(activate_version_message)
    return state


def sync_table(conn_info, stream, state, desired_columns, md_map):
    """Full-table sync entry point.

    The bucketed scan needs its prerequisite index; without it every bucket
    predicate would be a full table scan, so the tap falls back to a single plain
    scan and says why rather than silently doing N times the work.
    """
    pk_columns = md_map.get((), {}).get('table-key-properties', [])
    if not pk_columns:
        return _sync_table_without_pk(conn_info, stream, state, desired_columns, md_map)

    schema_name = md_map.get(()).get('schema-name')
    buckets = conn_info.get('keyset_buckets', keyset.BUCKETS_DEFAULT)
    with yb_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            usable, reason = keyset.validate_index(
                cur, schema_name, stream['table_name'], pk_columns, buckets)

    if not usable:
        LOGGER.warning(
            'Falling back to a non-resumable full scan for %s: %s',
            stream['tap_stream_id'], reason)
        return _sync_table_without_pk(conn_info, stream, state, desired_columns, md_map)

    return _sync_table_with_pk(conn_info, stream, state, desired_columns, md_map,
                               pk_columns)
