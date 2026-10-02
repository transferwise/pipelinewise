"""Verify PostgreSQL primary-key discovery excludes covering index columns."""

import unittest

from singer import metadata

from tap_postgres import discovery_utils
from ..utils import get_test_connection


class TestPrimaryKeyIncludeDiscovery(unittest.TestCase):
    def test_covering_column_is_not_a_replication_key(self):
        connection = get_test_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute('DROP TABLE IF EXISTS public.pgoutput_include_key_test')
                cursor.execute(
                    'CREATE TABLE public.pgoutput_include_key_test '
                    '(id integer NOT NULL, description text, PRIMARY KEY (id) INCLUDE (description))'
                )
            connection.autocommit = False
            streams = discovery_utils.discover_db(connection, tables=['pgoutput_include_key_test'])
            stream = next(stream for stream in streams if stream['table_name'] == 'pgoutput_include_key_test')
            md_map = metadata.to_map(stream['metadata'])

            self.assertEqual(md_map[()]['table-key-properties'], ['id'])
            self.assertEqual(md_map[('properties', 'description')]['inclusion'], 'available')
            self.assertEqual(stream['schema']['properties']['description']['type'], ['null', 'string'])
            connection.rollback()
        finally:
            connection.rollback()
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute('DROP TABLE IF EXISTS public.pgoutput_include_key_test')
            connection.close()
