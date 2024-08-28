#!/usr/bin/env python3
# pylint: disable=missing-function-docstring

import pendulum
import singer
from singer import metadata

from tap_mysql.connection import connect_with_backoff
from tap_mysql.sync_strategies import common


BOOKMARK_KEYS = {'replication_key', 'replication_key_value', 'version'}


def sync_table(mysql_conn, catalog_entry, state, columns):
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    replication_key_metadata = stream_metadata.get('replication-key')
    replication_key_state = singer.get_bookmark(state,
                                                catalog_entry.tap_stream_id,
                                                'replication_key')

    replication_key_value = None

    if replication_key_metadata == replication_key_state:
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key_value')
    else:
        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'replication_key',
                                      replication_key_metadata)
        state = singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value')

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'version',
                                  stream_version)

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    singer.write_message(activate_version_message)

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns)
            params = {}

            if replication_key_value is not None:
                if catalog_entry.schema.properties[replication_key_metadata].format == 'date-time':
                    replication_key_value = pendulum.parse(replication_key_value)

                select_sql += f" WHERE `{replication_key_metadata}` >= %(replication_key_value)s " \
                              f"ORDER BY `{replication_key_metadata}` ASC"

                params['replication_key_value'] = replication_key_value
            elif replication_key_metadata is not None:
                select_sql += f' ORDER BY `{replication_key_metadata}` ASC'

            common.sync_query(cur,
                              catalog_entry,
                              state,
                              select_sql,
                              columns,
                              stream_version,
                              params)
