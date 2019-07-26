import unittest
import tap_postgres
import psycopg2
import psycopg2.extras
import os
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, insert_record, get_test_connection_config
import decimal
import math
import pytz
import strict_rfc3339
import copy

LOGGER = get_logger()

def do_not_dump_catalog(catalog):
    pass

tap_postgres.dump_catalog = do_not_dump_catalog

class Unsupported(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        with get_test_connection() as conn:
            cur = conn.cursor()
            table_spec = {"columns": [{"name": "interval_col",   "type": "INTERVAL"},
                                      {"name": "bit_string_col", "type": "bit(5)"},
                                      {"name": "bytea_col",      "type": "bytea"},
                                      {"name": "point_col",      "type": "point"},
                                      {"name": "line_col",      "type": "line"},
                                      {"name": "lseg_col",      "type": "lseg"},
                                      {"name": "box_col",      "type": "box"},
                                      {"name": "polygon_col",      "type": "polygon"},
                                      {"name": "circle_col",      "type": "circle"},
                                      {"name": "xml_col",      "type": "xml"},
                                      {"name": "composite_col",      "type": "person_composite"},
                                      {"name": "int_range_col",      "type": "int4range"},
            ],
                          "name": Unsupported.table_name}
            with get_test_connection() as conn:
                cur = conn.cursor()
                cur.execute("""     DROP TYPE IF EXISTS person_composite CASCADE """)
                cur.execute("""     CREATE TYPE person_composite AS (age int, name text) """)

            ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()
        streams = tap_postgres.do_discovery(conn_config)
        chicken_streams = [s for s in streams if s['tap_stream_id'] == "postgres-public-CHICKEN TIMES"]

        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]
        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {():                                   {'is-view': False, 'table-key-properties': [], 'row-count': 0, 'schema-name': 'public', 'database-name': 'postgres'},
                          ('properties', 'bytea_col'):          {'sql-datatype': 'bytea', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'bit_string_col'):     {'sql-datatype': 'bit(5)', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'line_col'):           {'sql-datatype': 'line', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'xml_col'):            {'sql-datatype': 'xml', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'int_range_col'):      {'sql-datatype': 'int4range', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'circle_col'):         {'sql-datatype': 'circle', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'polygon_col'):        {'sql-datatype': 'polygon', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'box_col'):            {'sql-datatype': 'box', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'lseg_col'):           {'sql-datatype': 'lseg', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'composite_col'):      {'sql-datatype': 'person_composite', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'interval_col'):       {'sql-datatype': 'interval', 'selected-by-default': False, 'inclusion': 'unsupported'},
                          ('properties', 'point_col'):          {'sql-datatype': 'point', 'selected-by-default': False, 'inclusion': 'unsupported'}}
        )


if __name__== "__main__":
    test1 = Unsupported()
    test1.setUp()
    test1.test_catalog()
