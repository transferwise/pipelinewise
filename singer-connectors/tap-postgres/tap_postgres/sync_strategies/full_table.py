import copy
import time
import psycopg2.extras
import singer

from functools import partial
from singer import utils
from singer import metrics

import tap_postgres.db as post_db

LOGGER = singer.get_logger('tap_postgres')

UPDATE_BOOKMARK_PERIOD = 1000


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

    escaped_columns = map(post_db.prepare_columns_sql, desired_columns)

    activate_version_message = singer.ActivateVersionMessage(
        stream=post_db.calculate_destination_stream_name(stream, md_map),
        version=nascent_stream_version)

    if first_run:
        singer.write_message(activate_version_message)

    with metrics.record_counter(None) as counter:
        with post_db.open_connection(conn_info) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
                cur.itersize = post_db.CURSOR_ITER_SIZE
                select_sql = f"SELECT {','.join(escaped_columns)} FROM " \
                             f"{post_db.fully_qualified_table_name(schema_name,stream['table_name'])}"

                LOGGER.info("select %s with itersize %s", select_sql, cur.itersize)
                cur.execute(select_sql)

                rows_saved = 0
                for rec in cur:
                    record_message = post_db.selected_row_to_singer_message(stream,
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


def sync_table(conn_info, stream, state, desired_columns, md_map, snapshot_lsn=None):
    # Keep the replay check and snapshot on one connection when a secondary
    # endpoint can route successive connections to different replicas.
    from tap_postgres.sync_strategies.logical_replication import wait_for_replica_replay

    conn = post_db.open_connection(conn_info)
    try:
        if snapshot_lsn is not None:
            wait_for_replica_replay(conn_info, snapshot_lsn, connection=conn)
            state = singer.write_bookmark(state, stream['tap_stream_id'], 'lsn', snapshot_lsn)
        return _sync_table(conn_info, conn, stream, state, desired_columns, md_map)
    finally:
        conn.close()


def _prepare_snapshot_state(stream_id, state):
    """Keep an interrupted snapshot's version and mark it before emitting state."""
    first_run = singer.get_bookmark(state, stream_id, 'version') is None
    # Keep the version and CDC boundary when replaying an interrupted snapshot.
    # XIDs can wrap or be frozen between runs, so resume from the first row.
    if singer.get_bookmark(state, stream_id, 'xmin') is None:
        version = int(time.time() * 1000)
    else:
        version = singer.get_bookmark(state, stream_id, 'version')

    state = singer.write_bookmark(state, stream_id, 'version', version)
    if singer.get_bookmark(state, stream_id, 'xmin') is None:
        # Mark the snapshot before its first checkpoint; otherwise a restart
        # after that checkpoint could mistake it for completed CDC bootstrap.
        state = singer.write_bookmark(state, stream_id, 'xmin', 1)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state, version, first_run


def _sync_table(conn_info, conn, stream, state, desired_columns, md_map):
    time_extracted = utils.now()
    state, nascent_stream_version, first_run = _prepare_snapshot_state(stream['tap_stream_id'], state)
    schema_name = md_map.get(()).get('schema-name')

    escaped_columns = map(partial(post_db.prepare_columns_for_select_sql, md_map=md_map), desired_columns)

    activate_version_message = singer.ActivateVersionMessage(
        stream=post_db.calculate_destination_stream_name(stream, md_map),
        version=nascent_stream_version)

    if first_run:
        singer.write_message(activate_version_message)

    hstore_available = post_db.hstore_available(conn_info, connection=conn)
    with metrics.record_counter(None) as counter:

        # Client side character encoding defaults to the value in postgresql.conf under client_encoding.
        # The server / db can also have its own configred encoding.
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

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
            cur.itersize = post_db.CURSOR_ITER_SIZE

            fq_table_name = post_db.fully_qualified_table_name(schema_name, stream['table_name'])
            LOGGER.info("Beginning or restarting Full Table replication %s", nascent_stream_version)
            select_sql = f"""SELECT {','.join(escaped_columns)}, xmin::text::bigint
                              FROM {fq_table_name}"""

            LOGGER.info("select %s with itersize %s", select_sql, cur.itersize)
            cur.execute(select_sql)

            rows_saved = 0
            for rec in cur:
                xmin = rec['xmin']
                rec = rec[:-1]
                record_message = post_db.selected_row_to_singer_message(stream,
                                                                        rec,
                                                                        nascent_stream_version,
                                                                        desired_columns,
                                                                        time_extracted,
                                                                        md_map)
                singer.write_message(record_message)
                state = singer.write_bookmark(state, stream['tap_stream_id'], 'xmin', xmin)
                rows_saved += 1
                if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                counter.increment()
    # once we have completed the full table replication, discard the xmin bookmark.
    # the xmin bookmark only comes into play when a full table replication is interrupted
    state = singer.write_bookmark(state, stream['tap_stream_id'], 'xmin', None)

    # always send the activate version whether first run or subsequent
    singer.write_message(activate_version_message)

    return state
