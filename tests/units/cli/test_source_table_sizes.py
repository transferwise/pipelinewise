"""Source size reads stay parameterized and release connections on failure."""
from unittest import TestCase, mock

from pipelinewise.cli import source_table_sizes


class SourceTableSizesTestCase(TestCase):

    def test_mysql_sizes_are_bytes_and_names_are_bound(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [(8192,), (None,)]
        config = {'host': 'source', 'user': 'reader', 'password': 'test', 'replica_host': 'replica'}
        tables = [('schema.with.dot', "table'quoted"), ('schema.with.dot', 'view')]
        with mock.patch.object(source_table_sizes.pymysql, 'connect', return_value=connection) as connect:
            sizes = source_table_sizes.get_source_table_sizes('tap-mysql', config, tables)

        self.assertEqual(sizes, {tables[0]: 8192, tables[1]: None})
        self.assertEqual(connect.call_args.kwargs['host'], 'source')
        self.assertEqual(connect.call_args.kwargs['ssl'], {'': True})
        self.assertEqual(cursor.execute.call_args_list[0].args[1], tables[0])
        self.assertNotIn("table'quoted", cursor.execute.call_args_list[0].args[0])
        connection.close.assert_called_once()

    def test_postgres_sizes_keep_schema_and_table_separate(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [(16384, 3), (None, 1)]
        config = {'host': 'source'}
        tables = [('Mixed Schema', 'Mixed "Table'), ('public', 'foreign_table')]
        with mock.patch.object(
            source_table_sizes.FastSyncTapPostgres, 'get_connection', return_value=connection
        ) as connect:
            sizes = source_table_sizes.get_source_table_sizes('tap-postgres', config, tables)

        self.assertEqual(sizes, {tables[0]: 16384, tables[1]: None})
        connect.assert_called_once_with(config, prioritize_primary=True)
        self.assertEqual(cursor.execute.call_args_list[0].args[1], tables[0])
        connection.close.assert_called_once()

    def test_missing_tables_fail_and_close_connection(self):
        for tap_type, result in [('tap-mysql', None), ('tap-postgres', (0, 0))]:
            with self.subTest(tap_type=tap_type):
                connection = mock.MagicMock()
                connection.cursor.return_value.__enter__.return_value.fetchone.return_value = result
                with (
                    mock.patch.object(source_table_sizes.pymysql, 'connect', return_value=connection),
                    mock.patch.object(
                        source_table_sizes.FastSyncTapPostgres, 'get_connection', return_value=connection
                    ),
                ):
                    with self.assertRaisesRegex(ValueError, 'Cannot read source table size'):
                        source_table_sizes.get_source_table_sizes(
                            tap_type, {'host': 'source', 'user': 'reader', 'password': 'test'}, [('schema', 'missing')]
                        )
                connection.close.assert_called_once()

    def test_query_failure_closes_connection(self):
        connection = mock.MagicMock()
        connection.cursor.return_value.__enter__.return_value.execute.side_effect = RuntimeError('permission denied')
        with mock.patch.object(source_table_sizes.FastSyncTapPostgres, 'get_connection', return_value=connection):
            with self.assertRaisesRegex(RuntimeError, 'permission denied'):
                source_table_sizes.get_source_table_sizes('tap-postgres', {}, [('public', 'orders')])
        connection.close.assert_called_once()

    def test_empty_selection_does_not_connect(self):
        with mock.patch.object(source_table_sizes.pymysql, 'connect') as connect:
            self.assertEqual(source_table_sizes.get_source_table_sizes('tap-mysql', {}, []), {})
        connect.assert_not_called()
