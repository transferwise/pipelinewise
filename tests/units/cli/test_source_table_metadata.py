"""Index and key metadata preserves ordered definitions across source dialects."""
from unittest import TestCase, mock

from pipelinewise.cli import source_table_metadata as metadata


class SourceTableMetadataTestCase(TestCase):

    def test_mysql_indexes_preserve_order_prefixes_and_expressions(self):
        rows = [
            {'key_name': 'composite', 'seq_in_index': 2, 'index_type': 'BTREE', 'non_unique': 0,
             'column_name': 'email', 'sub_part': 20, 'collation': 'D'},
            {'key_name': 'composite', 'seq_in_index': 1, 'index_type': 'BTREE', 'non_unique': 0,
             'column_name': 'tenant', 'sub_part': None, 'collation': 'A'},
            {'key_name': 'functional', 'seq_in_index': 1, 'index_type': 'BTREE', 'non_unique': 1,
             'column_name': None, 'expression': 'lower(email)', 'sub_part': None, 'collation': 'A'},
        ]
        with mock.patch.object(metadata, '_query_rows', return_value=rows) as query:
            indexes = metadata._mysql_indexes(mock.Mock(), 'schema`quoted', 'table`quoted')
        self.assertEqual(query.call_args.args[1], 'SHOW INDEX FROM `table``quoted` FROM `schema``quoted`')
        self.assertTrue(indexes[0]['is_unique'])
        self.assertEqual([column['name'] for column in indexes[0]['columns']], ['tenant', 'email'])
        self.assertEqual(indexes[0]['columns'][1]['prefix_length'], 20)
        self.assertEqual(indexes[0]['columns'][1]['order'], 'DESC')
        self.assertEqual(indexes[1]['columns'][0]['expression'], 'lower(email)')
        self.assertIsNone(indexes[0]['is_valid'])

    def test_postgres_expression_and_include_columns_keep_predicate(self):
        base = {'name': 'partial_idx', 'type': 'btree', 'is_unique': True, 'is_primary': False,
                'is_valid': False, 'definition': 'CREATE UNIQUE INDEX partial_idx ...', 'predicate': '(active = true)'}
        rows = [
            {**base, 'column_name': None, 'column_definition': 'lower(email)', 'sort_order': 'DESC',
             'is_included': False},
            {**base, 'column_name': 'payload', 'column_definition': 'payload', 'sort_order': None,
             'is_included': True},
        ]
        with mock.patch.object(metadata, '_query_rows', return_value=rows) as query:
            indexes = metadata._postgres_indexes(mock.Mock(), 'Mixed Schema', 'Order')
        self.assertEqual(query.call_args.args[2], ('Mixed Schema', 'Order'))
        self.assertEqual(len(indexes), 1)
        self.assertFalse(indexes[0]['is_valid'])
        self.assertEqual(indexes[0]['predicate'], '(active = true)')
        self.assertEqual(indexes[0]['columns'][0]['expression'], 'lower(email)')
        self.assertTrue(indexes[0]['columns'][1]['is_included'])

    def test_mysql_composite_keys_keep_references_in_order(self):
        rows = [
            {'name': 'PRIMARY', 'kind': 'PRIMARY KEY', 'column_name': 'tenant'},
            {'name': 'PRIMARY', 'kind': 'PRIMARY KEY', 'column_name': 'id'},
            {'name': 'unique_email', 'kind': 'UNIQUE', 'column_name': 'email'},
        ]
        rows += [
            {'name': 'parent_fk', 'kind': 'FOREIGN KEY', 'column_name': column,
             'referenced_schema': 'other', 'referenced_table': 'parent', 'referenced_column': referenced,
             'on_update': 'RESTRICT', 'on_delete': 'CASCADE'}
            for column, referenced in [('tenant', 'parent_tenant'), ('parent_id', 'id')]
        ]
        with mock.patch.object(metadata, '_query_rows', return_value=rows) as query:
            keys = metadata._mysql_keys(mock.Mock(), 'schema', "quoted'table")
        self.assertEqual(query.call_args.args[2], ('schema', "quoted'table"))
        self.assertEqual(keys['primary_key']['columns'], ['tenant', 'id'])
        self.assertEqual(keys['unique_constraints'][0]['columns'], ['email'])
        self.assertEqual(keys['foreign_keys'][0]['columns'], ['tenant', 'parent_id'])
        self.assertEqual(keys['foreign_keys'][0]['referenced_columns'], ['parent_tenant', 'id'])
        self.assertEqual(keys['foreign_keys'][0]['on_delete'], 'CASCADE')

    def test_postgres_keys_distinguish_constraints_and_actions(self):
        flags = {'is_deferrable': True, 'initially_deferred': False, 'is_validated': True}
        rows = [
            {'name': 'pk', 'kind': 'p', 'columns': ['tenant', 'id'], **flags},
            {'name': 'uq', 'kind': 'u', 'columns': ['email'], **flags},
            {'name': 'fk', 'kind': 'f', 'columns': ['tenant', 'parent_id'],
             'referenced_schema': 'other', 'referenced_table': 'parent', 'referenced_columns': ['tenant', 'id'],
             'on_update': 'a', 'on_delete': 'n', **flags},
        ]
        with mock.patch.object(metadata, '_query_rows', return_value=rows):
            keys = metadata._postgres_keys(mock.Mock(), 'public', 'orders')
        self.assertEqual(keys['primary_key']['columns'], ['tenant', 'id'])
        self.assertEqual(keys['unique_constraints'][0]['name'], 'uq')
        self.assertEqual(keys['foreign_keys'][0]['on_update'], 'NO ACTION')
        self.assertEqual(keys['foreign_keys'][0]['on_delete'], 'SET NULL')
        self.assertTrue(keys['foreign_keys'][0]['is_deferrable'])

    def test_no_keys_or_indexes_have_explicit_empty_values(self):
        with mock.patch.object(metadata, '_query_rows', return_value=[]):
            for reader in [metadata._mysql_keys, metadata._postgres_keys]:
                self.assertEqual(reader(mock.Mock(), 'schema', 'table'), {
                    'primary_key': None, 'unique_constraints': [], 'foreign_keys': [],
                })
            self.assertEqual(metadata._mysql_indexes(mock.Mock(), 'schema', 'table'), [])
            self.assertEqual(metadata._postgres_indexes(mock.Mock(), 'schema', 'table'), [])

    def test_details_share_one_connection_and_close_on_metadata_error(self):
        connection = mock.MagicMock()
        with (
            mock.patch.object(metadata, 'open_source_connection', return_value=connection) as connect,
            mock.patch.object(metadata, 'read_source_table_size', return_value=4096),
            mock.patch.object(metadata, '_postgres_indexes', side_effect=RuntimeError('metadata access denied')),
        ):
            with self.assertRaisesRegex(RuntimeError, 'metadata access denied'):
                metadata.get_source_table_details('tap-postgres', {}, [('schema', 'table')])
        connect.assert_called_once_with('tap-postgres', {})
        connection.close.assert_called_once()

    def test_mysql_view_skips_show_index(self):
        connection = mock.MagicMock()
        with (
            mock.patch.object(metadata, 'open_source_connection', return_value=connection),
            mock.patch.object(metadata, 'read_source_table_size', return_value=None),
            mock.patch.object(metadata, '_mysql_indexes') as indexes,
            mock.patch.object(metadata, '_mysql_keys', return_value=metadata._keys_result()),
        ):
            details = metadata.get_source_table_details('tap-mysql', {}, [('schema', 'view')])
        self.assertEqual(details[('schema', 'view')]['indexes'], [])
        self.assertIsNone(details[('schema', 'view')]['size_bytes'])
        indexes.assert_not_called()
        connection.close.assert_called_once()

    def test_empty_selection_does_not_connect(self):
        with mock.patch.object(metadata, 'open_source_connection') as connect:
            self.assertEqual(metadata.get_source_table_details('tap-mysql', {}, []), {})
        connect.assert_not_called()
