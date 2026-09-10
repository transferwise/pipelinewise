import copy
import time
import psycopg2
import psycopg2.extras
import singer

from functools import partial
from singer import utils
from singer import metrics

import tap_yugabyte.db as yb_db

LOGGER = singer.get_logger('tap_yugabyte')

UPDATE_BOOKMARK_PERIOD = 1000


# pylint: disable=invalid-name,missing-function-docstring,too-many-locals,duplicate-code
def sync_view(conn_info, stream, state, desired_columns, md_map):
    time_extracted = utils.now()

    # before writing the table version to state, check if we had one to begin with
    first_run = singer.get_bookmark(state, stream['tap_stream_id'], 'version') is None
    nascent_stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  nascent_stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    schema_name = md_map.get(()).get('schema-name')

    escaped_columns = map(yb_db.prepare_columns_sql, desired_columns)

    activate_version_message = singer.ActivateVersionMessage(
        stream=yb_db.calculate_destination_stream_name(stream, md_map),
        version=nascent_stream_version)

    if first_run:
        singer.write_message(activate_version_message)

    with metrics.record_counter(None) as counter:
        with yb_db.open_connection(conn_info) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
                cur.itersize = yb_db.CURSOR_ITER_SIZE
                select_sql = f"SELECT {','.join(escaped_columns)} FROM " \
                             f"{yb_db.fully_qualified_table_name(schema_name,stream['table_name'])}"

                LOGGER.info("select %s with itersize %s", select_sql, cur.itersize)
                cur.execute(select_sql)

                rows_saved = 0
                for rec in cur:
                    record_message = yb_db.selected_row_to_singer_message(stream,
                                                                           rec,
                                                                           nascent_stream_version,
                                                                           desired_columns,
                                                                           time_extracted,
                                                                           md_map)
                    singer.write_message(record_message)
                    rows_saved += 1
                    if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                    counter.increment()

    # always send the activate version whether first run or subsequent
    singer.write_message(activate_version_message)

    return state


def _quoted_pk_columns(pk_columns):
    """Quote each primary key column for use inside a tuple comparison."""
    return [f'"{yb_db.canonicalize_identifier(c)}"' for c in pk_columns]


def _pk_tuple_sql(pk_columns):
    """Build the `(col1, col2, ...)` tuple expression used for keyset comparisons."""
    return f"({', '.join(_quoted_pk_columns(pk_columns))})"


def _placeholders(pk_columns):
    """Build a parameterized `(%s, %s, ...)` tuple matching the number of key columns."""
    return f"({', '.join(['%s'] * len(pk_columns))})"


def _fetch_max_pk_values(conn_info, fq_table_name, pk_columns):
    """Snapshot the current maximum primary key tuple, bounding a resumable full scan."""
    pk_tuple_sql = _pk_tuple_sql(pk_columns)
    select_sql = f"SELECT {', '.join(_quoted_pk_columns(pk_columns))} FROM {fq_table_name} " \
                 f"ORDER BY {pk_tuple_sql} DESC LIMIT 1"

    with yb_db.open_connection(conn_info) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            LOGGER.info("select %s", select_sql)
            cur.execute(select_sql)
            row = cur.fetchone()
            if row is None:
                return None
            # the query selects exactly pk_columns, in order, so positional indexing is safe
            return [row[i] for i in range(len(pk_columns))]


# pylint: disable=too-many-statements,too-many-locals,duplicate-code
# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def _sync_table_with_pk(conn_info, stream, state, desired_columns, md_map, pk_columns):
    """Resumable full-table scan using primary-key keyset pagination.

    Bookmarks `max_pk_values` (captured once, bounding the scan against concurrent
    inserts) and `last_pk_fetched` (advanced per row), so an interrupted sync resumes
    with `WHERE pk_tuple > last_pk_fetched AND pk_tuple <= max_pk_values`, independent
    of the underlying storage engine's row-versioning internals.
    """
    time_extracted = utils.now()
    tap_stream_id = stream['tap_stream_id']
    schema_name = md_map.get(()).get('schema-name')
    fq_table_name = yb_db.fully_qualified_table_name(schema_name, stream['table_name'])

    first_run = singer.get_bookmark(state, tap_stream_id, 'version') is None
    max_pk_values = singer.get_bookmark(state, tap_stream_id, 'max_pk_values')

    # a max_pk_values bookmark indicates a previous run was interrupted mid-scan;
    # reuse its stream version instead of minting a new one
    if max_pk_values is None:
        nascent_stream_version = int(time.time() * 1000)
    else:
        nascent_stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    state = singer.write_bookmark(state, tap_stream_id, 'version', nascent_stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    if max_pk_values is None:
        max_pk_values = _fetch_max_pk_values(conn_info, fq_table_name, pk_columns)
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

    escaped_columns = list(map(partial(yb_db.prepare_columns_for_select_sql, md_map=md_map), desired_columns))
    pk_tuple_sql = _pk_tuple_sql(pk_columns)
    placeholders = _placeholders(pk_columns)

    last_pk_fetched = singer.get_bookmark(state, tap_stream_id, 'last_pk_fetched')
    if last_pk_fetched:
        LOGGER.info("Resuming Full Table replication %s from last_pk_fetched %s",
                    nascent_stream_version, last_pk_fetched)
        where_clause = f"WHERE {pk_tuple_sql} > {placeholders} AND {pk_tuple_sql} <= {placeholders}"
        params = list(last_pk_fetched) + list(max_pk_values)
    else:
        LOGGER.info("Beginning new Full Table replication %s", nascent_stream_version)
        where_clause = f"WHERE {pk_tuple_sql} <= {placeholders}"
        params = list(max_pk_values)

    select_sql = f"SELECT {','.join(escaped_columns)} FROM {fq_table_name} " \
                 f"{where_clause} ORDER BY {pk_tuple_sql} ASC"

    # desired_columns includes every pk column (they're always 'automatic'), so
    # positional indexing into each fetched row is safe and avoids relying on
    # DictCursor's key-based access, which real rows support but plain rows don't
    pk_indices = [desired_columns.index(pk) for pk in pk_columns]

    hstore_available = yb_db.hstore_available(conn_info)
    with metrics.record_counter(None) as counter:
        with yb_db.open_connection(conn_info) as conn:
            if hstore_available:
                LOGGER.info("hstore is available")
                psycopg2.extras.register_hstore(conn)
            else:
                LOGGER.info("hstore is UNavailable")

            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
                cur.itersize = yb_db.CURSOR_ITER_SIZE

                LOGGER.info("select %s with itersize %s, params %s", select_sql, cur.itersize, params)
                cur.execute(select_sql, params)

                rows_saved = 0
                for rec in cur:
                    record_message = yb_db.selected_row_to_singer_message(stream,
                                                                           rec,
                                                                           nascent_stream_version,
                                                                           desired_columns,
                                                                           time_extracted,
                                                                           md_map)
                    singer.write_message(record_message)
                    last_pk_fetched = [rec[i] for i in pk_indices]
                    state = singer.write_bookmark(state, tap_stream_id, 'last_pk_fetched', last_pk_fetched)
                    rows_saved += 1
                    if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                    counter.increment()

    # the scan completed: discard the resume bookmarks, they only matter mid-scan
    state = singer.write_bookmark(state, tap_stream_id, 'max_pk_values', None)
    state = singer.write_bookmark(state, tap_stream_id, 'last_pk_fetched', None)

    # always send the activate version whether first run or subsequent
    singer.write_message(activate_version_message)

    return state


# pylint: disable=too-many-locals,duplicate-code
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

    escaped_columns = list(map(partial(yb_db.prepare_columns_for_select_sql, md_map=md_map), desired_columns))

    activate_version_message = singer.ActivateVersionMessage(
        stream=yb_db.calculate_destination_stream_name(stream, md_map),
        version=nascent_stream_version)

    if first_run:
        singer.write_message(activate_version_message)

    LOGGER.info("Table %s has no primary key: syncing with a non-resumable full scan", tap_stream_id)

    with metrics.record_counter(None) as counter:
        with yb_db.open_connection(conn_info) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
                cur.itersize = yb_db.CURSOR_ITER_SIZE
                select_sql = f"SELECT {','.join(escaped_columns)} FROM {fq_table_name}"

                LOGGER.info("select %s with itersize %s", select_sql, cur.itersize)
                cur.execute(select_sql)

                rows_saved = 0
                for rec in cur:
                    record_message = yb_db.selected_row_to_singer_message(stream,
                                                                           rec,
                                                                           nascent_stream_version,
                                                                           desired_columns,
                                                                           time_extracted,
                                                                           md_map)
                    singer.write_message(record_message)
                    rows_saved += 1
                    if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                    counter.increment()

    singer.write_message(activate_version_message)

    return state


def sync_table(conn_info, stream, state, desired_columns, md_map):
    """Full-table sync entry point: resumable PK-keyset scan, or a plain scan without a PK."""
    pk_columns = md_map.get((), {}).get('table-key-properties', [])
    if pk_columns:
        return _sync_table_with_pk(conn_info, stream, state, desired_columns, md_map, pk_columns)
    return _sync_table_without_pk(conn_info, stream, state, desired_columns, md_map)
