#!/usr/bin/env python3
# pylint: disable=duplicate-code,too-many-locals,simplifiable-if-expression

import copy
import singer
from singer import metadata

import tap_snowflake.sync_strategies.common as common
from tap_snowflake.connection import SnowflakeConnection

LOGGER = singer.get_logger()

BOOKMARK_KEYS = {'last_pk_fetched', 'max_pk_values', 'version', 'initial_full_table_complete'}

def get_max_pk_values(cursor, catalog_entry):
    database_name = common.get_database_name(catalog_entry)
    escaped_db = common.escape(database_name)
    escaped_table = common.escape(catalog_entry.table)

    key_properties = common.get_key_properties(catalog_entry)
    escaped_columns = [common.escape(c) for c in key_properties]

    sql = """SELECT {}
               FROM {}.{}
              ORDER BY {}
              LIMIT 1
    """

    select_column_clause = ", ".join(escaped_columns)
    order_column_clause = ", ".join([pk + " DESC" for pk in escaped_columns])

    cursor.execute(sql.format(select_column_clause,
                           escaped_db,
                           escaped_table,
                           order_column_clause))
    result = cursor.fetchone()

    if result:
        max_pk_values = dict(zip(key_properties, result))
    else:
        max_pk_values = {}

    return max_pk_values

def generate_pk_clause(catalog_entry, state):
    key_properties = common.get_key_properties(catalog_entry)
    escaped_columns = [common.escape(c) for c in key_properties]

    where_clause = " AND ".join([pk + " > `{}`" for pk in escaped_columns])
    order_by_clause = ", ".join(['`{}`, ' for pk in escaped_columns])

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    last_pk_fetched = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'last_pk_fetched')

    if last_pk_fetched:
        pk_comparisons = ["({} > {} AND {} <= {})".format(common.escape(pk),
                                                          last_pk_fetched[pk],
                                                          common.escape(pk),
                                                          max_pk_values[pk])
                          for pk in key_properties]
    else:
        pk_comparisons = ["{} <= {}".format(common.escape(pk), max_pk_values[pk])
                          for pk in key_properties]

    sql = " WHERE {} ORDER BY {} ASC".format(" AND ".join(pk_comparisons),
                                             ", ".join(escaped_columns))

    return sql



def sync_table(snowflake_conn, catalog_entry, state, columns, stream_version):
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    bookmark = state.get('bookmarks', {}).get(catalog_entry.tap_stream_id, {})
    version_exists = True if 'version' in bookmark else False

    initial_full_table_complete = singer.get_bookmark(state,
                                                      catalog_entry.tap_stream_id,
                                                      'initial_full_table_complete')

    state_version = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'version')

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete and not (version_exists and state_version is None):
        singer.write_message(activate_version_message)

    with snowflake_conn.connect_with_backoff() as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns)
            params = {}

            common.sync_query(cur,
                              catalog_entry,
                              state,
                              select_sql,
                              columns,
                              stream_version,
                              params)

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched')

    singer.write_message(activate_version_message)
