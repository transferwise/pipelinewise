import unittest
import tap_postgres

from singer import metadata


from tap_postgres.discovery_utils import BASE_RECURSIVE_SCHEMAS
from tap_postgres import stream_utils

try:
	from tests.utils import get_test_connection, ensure_test_table, get_test_connection_config
except ImportError:
	from utils import get_test_connection, ensure_test_table, get_test_connection_config


def do_not_dump_catalog(catalog):
	pass


tap_postgres.dump_catalog = do_not_dump_catalog


class TestInit(unittest.TestCase):
	maxDiff = None
	table_name = 'CHICKEN TIMES'

	def setUp(self):
		table_spec = {"columns": [{"name": "id", "type": "integer", "primary_key": True, "serial": True},
								  {"name": '"character-varying_name"', "type": "character varying"},
								  {"name": '"varchar-name"', "type": "varchar(28)"},
								  {"name": 'char_name', "type": "char(10)"},
								  {"name": '"text-name"', "type": "text"}],
					  "name": self.table_name}

		ensure_test_table(table_spec)

	def test_refresh_streams_schema(self):
		conn_config = get_test_connection_config()

		streams = [
			{
				'table_name': self.table_name,
				'stream': self.table_name,
				'tap_stream_id': f'public-{self.table_name}',
				'schema': [],
				'metadata': [
					{
						'breadcrumb': [],
						'metadata': {
							'replication-method': 'LOG_BASED',
							'table-key-properties': ['some_id'],
							'row-count': 1000,
						}
					}
				]
			}
		]

		stream_utils.refresh_streams_schema(conn_config, streams)

		self.assertEqual(len(streams), 1)
		self.assertEqual(self.table_name, streams[0].get('table_name'))
		self.assertEqual(self.table_name, streams[0].get('stream'))

		streams[0]['metadata'].sort(key=lambda md: md['breadcrumb'])

		self.assertEqual(metadata.to_map(streams[0]['metadata']), {
			(): {'table-key-properties': ['id'],
				 'database-name': 'postgres',
				 'schema-name': 'public',
				 'is-view': False,
				 'row-count': 0,
				 'replication-method': 'LOG_BASED'
				 },
			('properties', 'character-varying_name'): {'inclusion': 'available',
													   'sql-datatype': 'character varying',
													   'selected-by-default': True},
			('properties', 'id'): {'inclusion': 'automatic',
								   'sql-datatype': 'integer',
								   'selected-by-default': True},
			('properties', 'varchar-name'): {'inclusion': 'available',
											 'sql-datatype': 'character varying',
											 'selected-by-default': True},
			('properties', 'text-name'): {'inclusion': 'available',
										  'sql-datatype': 'text',
										  'selected-by-default': True},
			('properties', 'char_name'): {'selected-by-default': True,
										  'inclusion': 'available',
										  'sql-datatype': 'character'}})

		self.assertEqual({'properties': {'id': {'type': ['integer'],
												'maximum': 2147483647,
												'minimum': -2147483648},
										 'character-varying_name': {'type': ['null', 'string']},
										 'varchar-name': {'type': ['null', 'string'], 'maxLength': 28},
										 'char_name': {'type': ['null', 'string'], 'maxLength': 10},
										 'text-name': {'type': ['null', 'string']}},
						  'type': 'object',
						  'definitions': BASE_RECURSIVE_SCHEMAS}, streams[0].get('schema'))
