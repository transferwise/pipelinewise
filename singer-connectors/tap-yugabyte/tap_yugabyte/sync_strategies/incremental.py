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
                                              "keyset_buckets": conn_info.get('keyset_buckets'),
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

    return state


def _get_select_sql(params):
    """Build the extraction query.

    The bucket predicate names every bucket so the planner can use an index led
    by the discriminator; without it that index is unusable and the query reads
    the whole table. The hint and the session settings are there because the
    merge plan is otherwise a cost decision, and the cost model prefers a
    sequential scan and an external merge sort -- faster in wall clock, and it
    spills instead of streaming.

    The subquery, labelled yb_speedup_trick as in tap-postgres, keeps the
    column-expression projection from defeating the index scan.
    """
    escaped_columns = params['escaped_columns']
    replication_key = yb_db.prepare_columns_sql(params['replication_key'])
    replication_key_sql_datatype = params['replication_key_sql_datatype']
    replication_key_value = params['replication_key_value']
    schema_name = params['schema_name']
    table_name = params['table_name']
    buckets = params.get('keyset_buckets')

    limit_statement = f'LIMIT {params["limit"]}' if params['limit'] else ''

    predicates = []
    if buckets:
        predicates.append(keyset.bucket_in_sql([params['replication_key']], buckets))
    if replication_key_value:
        # cast to the column's own discovered type: a timestamptz value compared
        # against a timestamp column is accepted as an index condition and then
        # rechecked against every row, which looks identical in the plan
        predicates.append(
            f"{replication_key} >= '{replication_key_value}'"
            f'::{replication_key_sql_datatype}'
        )
    where_statement = f'WHERE {" AND ".join(predicates)}' if predicates else ''
    hint = (keyset.replication_key_hint(table_name, params['replication_key'])
            if buckets else '')

    select_sql = f"""
    SELECT {','.join(escaped_columns)}
    FROM (
        {hint} SELECT *
        FROM {yb_db.fully_qualified_table_name(schema_name, table_name)}
        {where_statement}
        ORDER BY {replication_key} ASC {limit_statement}
    ) yb_speedup_trick;"""

    return select_sql
