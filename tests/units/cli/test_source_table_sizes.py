"""Source size reads use bound names and source connections."""
from unittest import TestCase, mock

from pipelinewise.cli import source_table_sizes


class SourceTableSizesTestCase(TestCase):

    def test_mysql_connection_uses_source_host_and_tls(self):
        config = {'host': 'source', 'user': 'reader', 'password': 'test', 'replica_host': 'replica'}
        with mock.patch.object(source_table_sizes.pymysql, 'connect') as connect:
            connection = source_table_sizes.open_source_connection('tap-mysql', config)
        self.assertIs(connection, connect.return_value)
        self.assertEqual(connect.call_args.kwargs['host'], 'source')
        self.assertEqual(connect.call_args.kwargs['ssl'], {'': True})

    def test_postgres_connection_prioritizes_primary(self):
        config = {'host': 'source'}
        with mock.patch.object(source_table_sizes.FastSyncTapPostgres, 'get_connection') as connect:
            connection = source_table_sizes.open_source_connection('tap-postgres', config)
        self.assertIs(connection, connect.return_value)
        connect.assert_called_once_with(config, prioritize_primary=True)

    def test_sizes_are_bytes_and_names_are_bound(self):
        for tap_type, rows in [
            ('tap-mysql', [(8192,), (None,)]),
            ('tap-postgres', [(16384, 3), (None, 1)]),
        ]:
            with self.subTest(tap_type=tap_type):
                cursor = mock.Mock()
                cursor.fetchone.side_effect = rows
                schema, table = 'Mixed Schema.with.dot', "Mixed Table'quoted"
                size = source_table_sizes.read_source_table_size(cursor, tap_type, schema, table)
                self.assertEqual(size, rows[0][0])
                self.assertEqual(cursor.execute.call_args.args[1], (schema, table))
                self.assertNotIn(table, cursor.execute.call_args.args[0])
                self.assertIsNone(source_table_sizes.read_source_table_size(cursor, tap_type, schema, 'view'))

    def test_missing_tables_fail(self):
        for tap_type, result in [('tap-mysql', None), ('tap-postgres', (0, 0))]:
            with self.subTest(tap_type=tap_type):
                cursor = mock.Mock()
                cursor.fetchone.return_value = result
                with self.assertRaisesRegex(ValueError, 'Cannot read source table size'):
                    source_table_sizes.read_source_table_size(cursor, tap_type, 'schema', 'missing')
