import unittest
from unittest.mock import patch

from singer import metadata

import tap_yugabyte

from tests.utils import ensure_test_table, get_test_connection, get_test_connection_config


class Unsupported(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DROP TYPE IF EXISTS person_composite CASCADE')
                cur.execute('CREATE TYPE person_composite AS (age int, name text)')

        ensure_test_table({'columns': [{'name': 'interval_col', 'type': 'INTERVAL'},
                                       {'name': 'bit_string_col', 'type': 'bit(5)'},
                                       {'name': 'bytea_col', 'type': 'bytea'},
                                       {'name': 'point_col', 'type': 'point'},
                                       {'name': 'line_col', 'type': 'line'},
                                       {'name': 'lseg_col', 'type': 'lseg'},
                                       {'name': 'box_col', 'type': 'box'},
                                       {'name': 'polygon_col', 'type': 'polygon'},
                                       {'name': 'circle_col', 'type': 'circle'},
                                       {'name': 'xml_col', 'type': 'xml'},
                                       {'name': 'composite_col', 'type': 'person_composite'},
                                       {'name': 'int_range_col', 'type': 'int4range'}],
                           'name': self.table_name})

    @patch('tap_yugabyte.dump_catalog')
    def test_catalog(self, _dump_catalog):
        conn_config = get_test_connection_config()
        streams = tap_yugabyte.do_discovery(conn_config)
        chicken_streams = [s for s in streams if s['tap_stream_id'] == f'public-{self.table_name}']

        self.assertEqual(1, len(chicken_streams))
        stream_dict = chicken_streams[0]
        stream_dict['metadata'].sort(key=lambda md: md['breadcrumb'])

        def unsupported(sql_datatype):
            return {'sql-datatype': sql_datatype, 'selected-by-default': False, 'inclusion': 'unsupported'}

        self.assertEqual({(): {'is-view': False, 'table-key-properties': [], 'row-count': 0,
                               'schema-name': 'public', 'database-name': conn_config['dbname']},
                          ('properties', 'bytea_col'): unsupported('bytea'),
                          ('properties', 'bit_string_col'): unsupported('bit(5)'),
                          ('properties', 'line_col'): unsupported('line'),
                          ('properties', 'xml_col'): unsupported('xml'),
                          ('properties', 'int_range_col'): unsupported('int4range'),
                          ('properties', 'circle_col'): unsupported('circle'),
                          ('properties', 'polygon_col'): unsupported('polygon'),
                          ('properties', 'box_col'): unsupported('box'),
                          ('properties', 'lseg_col'): unsupported('lseg'),
                          ('properties', 'composite_col'): unsupported('person_composite'),
                          ('properties', 'interval_col'): unsupported('interval'),
                          ('properties', 'point_col'): unsupported('point')},
                         metadata.to_map(stream_dict['metadata']))
