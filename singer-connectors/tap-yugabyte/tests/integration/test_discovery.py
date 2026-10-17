import unittest
from unittest.mock import patch

from singer import metadata

import tap_yugabyte

from tests.utils import ensure_test_table, get_test_connection_config


class TestDiscovery(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        ensure_test_table({
            'name': self.table_name,
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': '"character-varying_name"', 'type': 'character varying'},
                {'name': '"varchar-name"', 'type': 'varchar(28)'},
                {'name': 'char_name', 'type': 'char(10)'},
                {'name': '"text-name"', 'type': 'text'},
            ],
        })

    @patch('tap_yugabyte.dump_catalog')
    def test_discovery_produces_expected_catalog_for_table(self, _dump_catalog):
        conn_config = get_test_connection_config()

        streams = tap_yugabyte.do_discovery(conn_config)

        our_streams = [s for s in streams if s['tap_stream_id'] == f'public-{self.table_name}']
        self.assertEqual(1, len(our_streams))

        stream = our_streams[0]
        self.assertEqual(self.table_name, stream['table_name'])

        self.assertEqual({
            'type': 'object',
            'properties': {
                'id': {'type': ['integer'], 'maximum': 2147483647, 'minimum': -2147483648},
                'character-varying_name': {'type': ['null', 'string']},
                'varchar-name': {'type': ['null', 'string'], 'maxLength': 28},
                'char_name': {'type': ['null', 'string'], 'maxLength': 10},
                'text-name': {'type': ['null', 'string']},
            },
            'definitions': tap_yugabyte.discovery_utils.BASE_RECURSIVE_SCHEMAS,
        }, stream['schema'])

        md_map = metadata.to_map(stream['metadata'])
        self.assertEqual({
            'table-key-properties': ['id'],
            'database-name': conn_config['dbname'],
            'schema-name': 'public',
            'is-view': False,
            'row-count': 0,
        }, md_map[()])
        self.assertEqual({'inclusion': 'automatic',
                          'sql-datatype': 'integer',
                          'selected-by-default': True}, md_map[('properties', 'id')])
        self.assertEqual({'inclusion': 'available',
                          'sql-datatype': 'character varying',
                          'selected-by-default': True}, md_map[('properties', 'varchar-name')])
