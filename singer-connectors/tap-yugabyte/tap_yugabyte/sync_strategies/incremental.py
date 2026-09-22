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


# pylint: disable=too-many-locals
def sync_table(conn_info, stream, state, desired_columns, md_map):
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

    escaped_columns = map(partial(yb_db.prepare_columns_for_select_sql, md_map=md_map), desired_columns)

    activate_version_message = singer.ActivateVersionMessage(
        stream=yb_db.calculate_destination_stream_name(stream, md_map),
        version=stream_version)

    singer.write_message(activate_version_message)

    replication_key = md_map.get((), {}).get('replication-key')
    replication_key_value = singer.get_bookmark(state, stream['tap_stream_id'], 'replication_key_value')
    replication_key_sql_datatype = md_map.get(('properties', replication_key)).get('sql-datatype')

    hstore_available = yb_db.hstore_available(conn_info)
    with metrics.record_counter(None) as counter:
        with yb_db.open_connection(conn_info) as conn:

            # Client side character encoding defaults to the value in postgresql.conf under client_encoding.
            # The server / db can also have its own configured encoding.
            with conn.cursor() as cur:
                cur.execute("show server_encoding")
                LOGGER.info("Current Server Encoding: %s", cur.fetchone()[0])
                cur.execute("show client_encoding")
                LOGGER.info("Current Client Encoding: %s", cur.fetchone()[0])

            if hstore_available:
                LOGGER.info("hstore is available")
                psycopg2.extras.register_hstore(conn)
            else:
                LOGGER.info("hstore is UNavailable")

            # the same settings full_table's scans need. They must run on a plain
            # cursor: the extraction below uses a named server-side cursor, which
            # cannot carry SET.
            #
            # The extraction no longer DEPENDS on them. It used to lean on the
            # storage-level merge scan, which yb_max_merge_scan_streams enables
            # and which a named cursor silently does not perform -- see
            # _get_select_sql. The statement is a UNION ALL under an outer ORDER
            # BY now, so the ordering is a plan node (`Merge Append`) that the
            # cursor honours, and the plan was measured identical with these
            # settings and with none at all: Merge Append over N Index Only Scans,
            # Heap Fetches 0, on an ANALYZEd table and on one never ANALYZEd.
            # They stay because they are cheap, because this connection runs
            # nothing else, and because enable_seqscan = off is still the thing
            # that keeps a cost estimate from preferring a sequential scan on a
            # table shape nobody has measured yet.
            buckets = conn_info.get('keyset_buckets', keyset.BUCKETS_DEFAULT)
            with conn.cursor() as setup:
                if keyset.merge_scan_available(setup):
                    for setting in keyset.scan_settings_sql(buckets):
                        setup.execute(setting)

            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='pipelinewise') as cur:
                cur.itersize = yb_db.CURSOR_ITER_SIZE
                LOGGER.info("Beginning new incremental replication sync %s", stream_version)
                select_sql = _get_select_sql({"escaped_columns": escaped_columns,
                                              "replication_key": replication_key,
                                              "replication_key_sql_datatype": replication_key_sql_datatype,
                                              "replication_key_value": replication_key_value,
                                              "schema_name": schema_name,
                                              "table_name": stream['table_name'],
                                              "limit": conn_info['limit'],
                                              # same default as full_table: absent from
                                              # config this used to be None, which emits
                                              # no bucket predicate and no hint at all
                                              "keyset_buckets": buckets,
                                              "pk_columns": md_map.get((), {}).get(
                                                  'table-key-properties', []),
                                              })
                LOGGER.info('select statement: %s with itersize %s', select_sql, cur.itersize)
                cur.execute(select_sql)

                rows_saved = 0

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
