from unittest import TestCase, mock
from tempfile import TemporaryDirectory

from pipelinewise.fastsync.partialsync.utils import (
    load_into_snowflake, upload_to_s3, update_state_file,
    diff_source_target_columns, validate_boundary_value, get_sync_tables, quote_tag_to_char)
from pipelinewise.cli.errors import InvalidConfigException

from tests.units.partialsync.utils import PartialSync2SFArgs
from tests.units.partialsync.resources.test_partial_sync_utils.sample_sf_columns import SAMPLE_OUTPUT_FROM_SF


class PartialSyncUtilsTestCase(TestCase):
    """Test case for partial sync utils"""

    def test_upload_to_s3(self):
        """Test _upload_to_s3 method"""
        with TemporaryDirectory() as temp_test_dir:
            test_file_part = f'{temp_test_dir}/foo.gz1'
            test_s3_key = 'foo_s3_key'
            with open(test_file_part, 'w', encoding='utf8') as file_to_test:
                file_to_test.write('bar')

            mocked_snowflake = mock.MagicMock()
            mocked_upload_to_s3 = mocked_snowflake.upload_to_s3
            mocked_upload_to_s3.return_value = test_s3_key

            # pylint: disable=protected-access
            actual_return = upload_to_s3(mocked_snowflake, [test_file_part], temp_test_dir)
            self.assertTupleEqual(([test_s3_key], test_s3_key), actual_return)
            mocked_upload_to_s3.assert_called_with(test_file_part, tmp_dir=temp_test_dir)

    # pylint: disable=no-self-use
    def test_load_into_snowflake_hard_delete(self):
        """Test load_into_snowflake method with hard delete"""
        snowflake = mock.MagicMock()
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
            'temp': 'FOO_TEMP'
        }
        args = PartialSync2SFArgs(
            temp_test_dir='temp_test_dir', start_value='20', end_value='30'
        )
        columns_diff = {
            'added_columns': ['FOO_ADDED_COLUMN'],
            'source_columns': {'FOO_SOURCE_COLUMN': 'FOO_TYPE'}
        }
        primary_keys = ['FOO_PRIMARY']
        s3_key_pattern = 'FOO_PATTERN'
        size_bytes = 3
        where_clause_sql = 'test'
        load_into_snowflake(target, args, columns_diff, primary_keys, s3_key_pattern, size_bytes,
                            where_clause_sql)

        snowflake.assert_has_calls([
            mock.call.copy_to_table(s3_key_pattern, target['schema'], args.table, size_bytes, is_temporary=True),
            mock.call.obfuscate_columns(target['schema'], args.table),
            mock.call.add_columns(target['schema'], target['table'], columns_diff['added_columns']),
            mock.call.merge_tables(
                target['schema'], target['temp'], target['table'],
                ['FOO_SOURCE_COLUMN', '_SDC_EXTRACTED_AT', '_SDC_BATCHED_AT', '_SDC_DELETED_AT'], primary_keys),
            mock.call.partial_hard_delete(target['schema'], target['table'], where_clause_sql),
            mock.call.drop_table(target['schema'], target['temp'])
        ])

    # pylint: disable=no-self-use
    def test_load_into_snowflake_soft_delete(self):
        """Test load_into_snowflake method with soft delete"""
        snowflake = mock.MagicMock()
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
            'temp': 'FOO_TEMP'
        }
        args = PartialSync2SFArgs(
            temp_test_dir='temp_test_dir', start_value='20', end_value='30', hard_delete=False
        )
        columns_diff = {
            'added_columns': ['FOO_ADDED_COLUMN'],
            'source_columns': {'FOO_SOURCE_COLUMN': 'FOO_TYPE'}
        }
        primary_keys = ['FOO_PRIMARY']
        s3_key_pattern = 'FOO_PATTERN'
        size_bytes = 3
        where_clause_sql = 'test'
        load_into_snowflake(target, args, columns_diff, primary_keys, s3_key_pattern, size_bytes,
                            where_clause_sql)

        snowflake.assert_has_calls([
            mock.call.copy_to_table(s3_key_pattern, target['schema'], args.table, size_bytes, is_temporary=True),
            mock.call.obfuscate_columns(target['schema'], args.table),
            mock.call.add_columns(target['schema'], target['table'], columns_diff['added_columns']),
            mock.call.merge_tables(target['schema'], target['temp'], target['table'],
                                   ['FOO_SOURCE_COLUMN', '_SDC_EXTRACTED_AT', '_SDC_BATCHED_AT', '_SDC_DELETED_AT'],
                                   primary_keys),
            mock.call.drop_table(target['schema'], target['temp'])
        ])

    def test_load_into_snowflake_drop_target_table_enabled(self):
        """Test load_into_snowflake if drop_target_table is enabled"""
        snowflake = mock.MagicMock()
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
            'temp': 'FOO_TEMP'
        }
        args = PartialSync2SFArgs(
            temp_test_dir='temp_test_dir', start_value='20', end_value='30', hard_delete=False, drop_target_table=True
        )
        columns_diff = {
            'added_columns': ['FOO_ADDED_COLUMN'],
            'source_columns': {'FOO_SOURCE_COLUMN': 'FOO_TYPE'}
        }
        primary_keys = ['FOO_PRIMARY']
        s3_key_pattern = 'FOO_PATTERN'
        size_bytes = 3
        where_clause_sql = 'test'
        load_into_snowflake(target, args, columns_diff, primary_keys, s3_key_pattern, size_bytes,
                            where_clause_sql)

        snowflake.assert_has_calls([
            mock.call.copy_to_table(s3_key_pattern, target['schema'], args.table, size_bytes, is_temporary=True),
            mock.call.obfuscate_columns(target['schema'], args.table),
            mock.call.add_columns(target['schema'], target['table'], columns_diff['added_columns']),
            mock.call.swap_tables(target['schema'], target['table']),
        ])

    # pylint: disable=no-self-use
    def test_update_state_file(self):
        """Test state file updating with and without end value"""
        bookmark = {'foo': 2}
        test_end_values = (None, 'bar')

        for end_value in test_end_values:
            with self.subTest(endvalue=end_value):
                with mock.patch('pipelinewise.fastsync.commons.utils.save_state_file') as mocked_save_state_file:
                    args = PartialSync2SFArgs(
                        temp_test_dir='foo_temp', table='FOO', start_value='20', end_value=end_value, state='foo_state'
                    )
                    update_state_file(args, bookmark)
                if end_value:
                    mocked_save_state_file.assert_not_called()
                else:
                    mocked_save_state_file.assert_called_with(args.state, args.table, bookmark)

    def test_find_diff_columns(self):
        """Test find_diff_columns method works as expected"""
        sample_source_columns = [
            '"FOO_COLUMN_0" NUMBER', '"FOO_COLUMN_1" NUMBER', '"FOO_COLUMN_3" VARCHAR', '"FOO_COLUMN_5" VARCHAR'
        ]
        schema = 'FOO_SCHEMA'
        table = 'BAR_TABLE'
        mocked_snowflake = mock.MagicMock()
        mocked_snowflake.query.return_value = SAMPLE_OUTPUT_FROM_SF
        sample_target_sf = {
            'sf_object': mocked_snowflake,
            'schema': schema,
            'table': table
        }

        expected_output = {
            'added_columns': {'"FOO_COLUMN_0"': 'NUMBER',
                              '"FOO_COLUMN_5"': 'VARCHAR'},
            'removed_columns': {
                '"FOO_COLUMN_2"': 'TEXT',
                '"FOO_COLUMN_4"': 'NUMBER',
                '"_SDC_FOO_BAR"': 'TIMESTAMP_NTZ'
            },
            'source_columns': {
                '"FOO_COLUMN_0"': 'NUMBER',
                '"FOO_COLUMN_1"': 'NUMBER',
                '"FOO_COLUMN_3"': 'VARCHAR',
                '"FOO_COLUMN_5"': 'VARCHAR'
            },
            'target_columns': ['FOO_COLUMN_1', 'FOO_COLUMN_2',
                               'FOO_COLUMN_3', 'FOO_COLUMN_4',
                               '_SDC_EXTRACTED_AT', '_SDC_BATCHED_AT', '_SDC_DELETED_AT', '_SDC_FOO_BAR'],
        }
        actual_output = diff_source_target_columns(target_sf=sample_target_sf, source_columns=sample_source_columns)
        self.assertDictEqual(actual_output, expected_output)

    def test_valiodate_boundary_value_return_none_if_value_is_none(self):
        """Test if validate_boundary_value method returns none with none as input"""
        query_object = mock.MagicMock()
        self.assertIsNone(validate_boundary_value(query_object, None))

    def test_validate_static_boundary_value_works_as_expected(self):
        """Testing validate_boundary_value method for stati values"""
        valid_values = ('<S>foo', '<S>123', '<S>2022-12-11 12:11:13',
                        '<S>2022-12-11', '<S>foo123', '<S>24.5', '<S>ABCD-FH11-24')

        query_object = mock.MagicMock()
        for test_value in valid_values:
            self.assertEqual(test_value[3:], validate_boundary_value(query_object, test_value))

    def test_validate_static_boundary_value_raises_exception_if_invalid_value(self):
        """Test if exception is raised on invalid static values"""
        invalid_values = ('<S>;', '<S>foo bar', '<S>(foo)', '<S>foo;bar',
                          '<S>foo%', '<S>1 2 3', '<S>foo,bar', '<S>[foo]', '<S>*', '<S>%')
        query_object = mock.MagicMock()

        for test_value in invalid_values:
            self.assertRaises(InvalidConfigException, validate_boundary_value, query_object, test_value)

    def test_validate_dynamic_boundary_value_works_as_expected(self):
        """Testing validate_boundary_value method for dynamic values"""
        test_cases = [('<D>select get_foo();', [['foo', ]]),
                      ("<D>SELECT NOW() - INTERVAL '1 day';", [['2023-01-01 00:00:00', ]]),
                      ("<D>SELECT max('inserted_time');", [['foo', ]]),
                      ('<D>select bar();', [{'bar()': 5}])
                      ]
        query_object = mock.MagicMock()
        for dynamic_value, query_return_from_source in test_cases:
            query_object.return_value = query_return_from_source
            if isinstance(query_return_from_source[0], dict):
                expected_value = list(query_return_from_source[0].values())[0]
            else:
                expected_value = query_return_from_source[0][0]
            self.assertEqual(expected_value, validate_boundary_value(query_object, dynamic_value))
            query_object.assert_called_with(dynamic_value[3:])

    def test_validate_dynamic_boindary_value_raise_exception_if_invalid_value(self):
        """Test if exception is raised on invalid static values"""
        invalid_values = ('<D>foo;bar;', '<D>foo;bar', '<D>delete from foo;',
                          '<D>select * from foo;DELETE foo;', '<D>update foo set bar=baz',
                          '<D>INSERT into foo (bar) values (baz);', '<D>foo')

        query_object = mock.MagicMock()
        query_object.return_value = [['foo', ]]

        for test_value in invalid_values:
            self.assertRaises(InvalidConfigException, validate_boundary_value, query_object, test_value)

    def test_validate_dynamic_value_raise_exception_if_return_has_than_one_column_or_row(self):
        """Test if validate_boundary_value raise exception for dynamic values
         which return more than one row or column"""
        query_object = mock.MagicMock()
        query_returns = (
            [['foo', 'bar']],
            [{'foo': 1, 'bar': 2}],
            [['foo'], ['bar']],
            [{'foo': 1}, {'bar': 2}]
        )
        for test_value in query_returns:
            query_object.return_value = test_value
            test_query = '<D>SELECT * FROM baz'
            self.assertRaises(InvalidConfigException, validate_boundary_value, query_object, test_query)

    def test_validate_dynamic_value_returns_null_if_dynamic_value_returns_nothing(self):
        """Test if validate_boundary_value returns NULL if dynamic value returns nothing"""
        query_object = mock.MagicMock()
        query_object.return_value = []
        test_query = '<D>SELECT id FROM foo WHERE id=1;'
        self.assertEqual('NULL', validate_boundary_value(query_object, test_query))

    def test_get_sync_tables(self):
        """Test if get_sync_tables wotks as expected"""
        mocked_args = mock.MagicMock()
        mocked_args.table = 'foo_table,bar_table,baz_table'
        mocked_args.column = 'foo_column,bar_column,baz_column'
        mocked_args.start_value = 'foo_start,bar_start,baz_start'
        mocked_args.end_value = 'foo_end,bar_end,baz_end'
        mocked_args.drop_target_table = 'True,False,True'

        expected_output = {
            'foo_table': {
                'column': 'foo_column',
                'drop_target_table': True,
                'start_value': 'foo_start',
                'end_value': 'foo_end'
            },
            'bar_table': {
                'column': 'bar_column',
                'drop_target_table': False,
                'start_value': 'bar_start',
                'end_value': 'bar_end'
            },
            'baz_table': {
                'column': 'baz_column',
                'drop_target_table': True,
                'start_value': 'baz_start',
                'end_value': 'baz_end'
            },
        }
        actual_output = get_sync_tables(mocked_args)
        self.assertDictEqual(expected_output, actual_output)

    def test_quote_tag_to_char(self):
        """Test if the method works as expected and replaces quote tags with quote character"""
        input_string = 'foo <<quote>>bar<<quote>> baz'
        expected_string = "foo 'bar' baz"
        self.assertEqual(expected_string, quote_tag_to_char(input_string))
