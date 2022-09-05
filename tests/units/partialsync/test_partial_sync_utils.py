from unittest import TestCase, mock
from tempfile import TemporaryDirectory

from pipelinewise.fastsync.partialsync.utils import load_into_snowflake, upload_to_s3, update_state_file
from pipelinewise.fastsync.partialsync.utils import diff_source_target_columns


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
        """Test load_into_snowflake method"""
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
            mock.call.partial_hard_delete(target["schema"], target["table"], where_clause_sql),
            mock.call.drop_table(target["schema"], target["temp"])
        ])

    # pylint: disable=no-self-use
    def test_load_into_snowflake_soft_delete(self):
        """Test load_into_snowflake method"""
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
            mock.call.drop_table(target["schema"], target["temp"])
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
