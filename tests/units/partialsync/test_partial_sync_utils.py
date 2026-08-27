import json
import os

from unittest import TestCase, mock
from tempfile import TemporaryDirectory

from pipelinewise.fastsync.partialsync.utils import (
    DYNAMIC_BOUNDARY_NOT_READY, delete_s3_objects, load_into_snowflake, upload_to_s3,
    update_state_file, diff_source_target_columns, validate_boundary_value, get_sync_tables,
    NativePartialSyncCompatibilityError, quote_tag_to_char)
from pipelinewise.cli.errors import InvalidConfigException
from pipelinewise.fastsync.commons.snowflake_types import SNOWFLAKE_MAX_VARCHAR
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
    TABLE_FORMAT_MISSING,
    TableCompatibilityError,
)

from tests.units.partialsync.utils import PartialSync2SFArgs
from tests.units.partialsync.resources.test_partial_sync_utils.sample_sf_columns import SAMPLE_OUTPUT_FROM_SF


def _snowflake_column(name, data_type, length=None):
    type_metadata = {'type': data_type, 'nullable': True}
    if length is not None:
        type_metadata['length'] = length
    return {
        'column_name': name,
        'data_type': json.dumps(type_metadata),
    }


class PartialSyncUtilsTestCase(TestCase):  # pylint: disable=too-many-public-methods
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
            self.assertFalse(os.path.exists(test_file_part))

    def test_upload_to_s3_preserves_local_file_if_upload_fails(self):
        """A failed upload must leave its local part available for diagnosis or retry."""
        with TemporaryDirectory() as temp_test_dir:
            test_file_part = f'{temp_test_dir}/foo.gz1'
            with open(test_file_part, 'w', encoding='utf8') as file_to_test:
                file_to_test.write('bar')

            mocked_snowflake = mock.MagicMock()
            mocked_snowflake.upload_to_s3.side_effect = RuntimeError('upload failed')

            with self.assertRaisesRegex(RuntimeError, 'upload failed'):
                upload_to_s3(mocked_snowflake, [test_file_part], temp_test_dir)

            mocked_snowflake.upload_to_s3.assert_called_once_with(
                test_file_part, tmp_dir=temp_test_dir
            )
            self.assertTrue(os.path.exists(test_file_part))

    def test_upload_to_s3_cleans_prior_parts_and_preserves_locals_on_later_failure(self):
        """A later upload failure removes completed remote parts without deleting local inputs."""
        with TemporaryDirectory() as temp_test_dir:
            file_parts = [
                f'{temp_test_dir}/foo.part0',
                f'{temp_test_dir}/foo.part1',
            ]
            for file_part in file_parts:
                with open(file_part, 'w', encoding='utf8') as file_to_test:
                    file_to_test.write('bar')

            snowflake = mock.MagicMock()
            snowflake.connection_config = {'s3_bucket': 'test-bucket'}
            setattr(
                snowflake,
                '_get_s3_key',
                mock.Mock(
                    side_effect=['staging/foo.part0', 'staging/foo.part1']
                ),
            )
            snowflake.upload_to_s3.side_effect = [
                'staging/foo.part0',
                RuntimeError('second upload failed'),
            ]

            with self.assertRaisesRegex(RuntimeError, 'second upload failed'):
                upload_to_s3(snowflake, file_parts, temp_test_dir)

            self.assertEqual(snowflake.upload_to_s3.call_args_list, [
                mock.call(file_parts[0], tmp_dir=temp_test_dir),
                mock.call(file_parts[1], tmp_dir=temp_test_dir),
            ])
            self.assertEqual(snowflake.s3.delete_object.call_args_list, [
                mock.call(Bucket='test-bucket', Key='staging/foo.part0'),
                mock.call(Bucket='test-bucket', Key='staging/foo.part1'),
            ])
            self.assertTrue(all(os.path.exists(file_part) for file_part in file_parts))

    def test_upload_to_s3_preserves_cleanup_debt_on_rollback_failure(self):
        """The caller receives uploaded keys when immediate rollback is exhausted."""
        with TemporaryDirectory() as temp_test_dir:
            file_parts = [
                f'{temp_test_dir}/foo.part0',
                f'{temp_test_dir}/foo.part1',
            ]
            for file_part in file_parts:
                with open(file_part, 'w', encoding='utf8') as file_to_test:
                    file_to_test.write('bar')

            snowflake = mock.MagicMock()
            snowflake.connection_config = {'s3_bucket': 'test-bucket'}
            setattr(
                snowflake,
                '_get_s3_key',
                mock.Mock(
                    side_effect=['staging/foo.part0', 'staging/foo.part1']
                ),
            )
            snowflake.upload_to_s3.side_effect = [
                'staging/foo.part0',
                RuntimeError('second upload failed'),
            ]
            snowflake.s3.delete_object.side_effect = RuntimeError('delete failed')

            with self.assertRaisesRegex(
                RuntimeError, 'staging upload rollback failed'
            ) as raised:
                upload_to_s3(snowflake, file_parts, temp_test_dir)

            self.assertEqual(
                raised.exception.s3_keys,
                ['staging/foo.part0', 'staging/foo.part1'],
            )
            self.assertEqual(snowflake.s3.delete_object.call_count, 6)
            self.assertTrue(all(os.path.exists(path) for path in file_parts))

    def test_upload_to_s3_cleans_remote_parts_if_local_cleanup_fails(self):
        """A local unlink failure cannot leave the completed multipart upload behind."""
        with TemporaryDirectory() as temp_test_dir:
            file_parts = [
                f'{temp_test_dir}/foo.part0',
                f'{temp_test_dir}/foo.part1',
            ]
            for file_part in file_parts:
                with open(file_part, 'w', encoding='utf8') as file_to_test:
                    file_to_test.write('bar')

            snowflake = mock.MagicMock()
            snowflake.connection_config = {'s3_bucket': 'test-bucket'}
            snowflake.upload_to_s3.side_effect = [
                'staging/foo.part0',
                'staging/foo.part1',
            ]

            with mock.patch(
                'pipelinewise.fastsync.commons.utils.os.remove',
                side_effect=PermissionError('local cleanup failed'),
            ), self.assertRaisesRegex(PermissionError, 'local cleanup failed'):
                upload_to_s3(snowflake, file_parts, temp_test_dir)

            self.assertEqual(snowflake.upload_to_s3.call_args_list, [
                mock.call(file_parts[0], tmp_dir=temp_test_dir),
                mock.call(file_parts[1], tmp_dir=temp_test_dir),
            ])
            self.assertEqual(snowflake.s3.delete_object.call_args_list, [
                mock.call(Bucket='test-bucket', Key='staging/foo.part0'),
                mock.call(Bucket='test-bucket', Key='staging/foo.part1'),
            ])
            self.assertTrue(all(os.path.exists(file_part) for file_part in file_parts))

    def test_delete_s3_objects_retries_and_removes_every_object(self):
        """A transient deletion failure is retried before later keys are removed."""
        snowflake = mock.MagicMock()
        snowflake.s3.delete_object.side_effect = [
            RuntimeError('first delete failed'),
            None,
            None,
        ]

        with self.assertLogs('pipelinewise.fastsync.commons.utils', level='WARNING') as logs:
            delete_s3_objects(
                snowflake,
                ['staging/first.csv.gz', 'staging/second.csv.gz'],
                'test-bucket',
            )

        self.assertEqual(snowflake.s3.delete_object.call_args_list, [
            mock.call(Bucket='test-bucket', Key='staging/first.csv.gz'),
            mock.call(Bucket='test-bucket', Key='staging/first.csv.gz'),
            mock.call(Bucket='test-bucket', Key='staging/second.csv.gz'),
        ])
        self.assertIn(
            's3://test-bucket/staging/first.csv.gz: first delete failed',
            logs.output[0],
        )

    def test_delete_s3_objects_reports_exhausted_cleanup(self):
        """State callers receive a failure after all delete retries are exhausted."""
        snowflake = mock.MagicMock()
        snowflake.s3.delete_object.side_effect = RuntimeError('delete denied')

        with self.assertRaisesRegex(RuntimeError, 'failed after 3 attempts'):
            delete_s3_objects(
                snowflake,
                ['staging/first.csv.gz'],
                'test-bucket',
            )

        self.assertEqual(snowflake.s3.delete_object.call_count, 3)

    def test_load_into_snowflake_hard_delete(self):
        """Staging completes before the live target is published atomically."""
        snowflake = mock.MagicMock()
        snowflake.query.return_value = []
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
            'temp': 'FOO_TEMP',
            'publication_status': {'attempted': False},
        }
        args = PartialSync2SFArgs(
            temp_test_dir='temp_test_dir', start_value='20', end_value='30'
        )
        source_columns = ['"FOO_SOURCE_COLUMN" FOO_TYPE']
        primary_keys = ['FOO_PRIMARY']
        s3_key_pattern = 'FOO_PATTERN'
        size_bytes = 3
        where_clause_sql = 'test'
        with mock.patch(
            'pipelinewise.fastsync.partialsync.utils.iceberg_routes.'
            'require_native_target_format'
        ) as native_format_guard:
            load_into_snowflake(
                target,
                args,
                source_columns,
                primary_keys,
                s3_key_pattern,
                size_bytes,
                where_clause_sql,
            )

        native_format_guard.assert_called_once_with(
            snowflake,
            args,
            target['schema'],
            args.table,
            allow_missing=False,
        )

        self.assertEqual(snowflake.method_calls, [
            mock.call.copy_to_table(s3_key_pattern, target['schema'], args.table, size_bytes, is_temporary=True),
            mock.call.obfuscate_columns(target['schema'], args.table),
            mock.call.create_table(
                target_schema=target['schema'], table_name=target['table'], columns=source_columns,
                primary_key=primary_keys, is_temporary=False, sort_columns=False, allow_replace_table=False,
                normalize_primary_keys='if_created',
            ),
            mock.call.query('SHOW COLUMNS IN TABLE FOO_SCHEMA."FOO_TABLE"'),
            mock.call.add_columns(target['schema'], target['table'], {'"FOO_SOURCE_COLUMN"': 'FOO_TYPE'}),
            mock.call.publish_partial_sync(
                target['schema'], target['temp'], target['table'],
                ['"FOO_SOURCE_COLUMN"', '_SDC_EXTRACTED_AT', '_SDC_BATCHED_AT', '_SDC_DELETED_AT'],
                primary_keys, where_clause_sql, hard_delete=True,
            ),
            mock.call.drop_table(
                target['schema'],
                target['table'],
                is_temporary=True,
                max_attempts=3,
            )
        ])

    def test_load_into_snowflake_soft_delete(self):
        """Test load_into_snowflake method with soft delete"""
        snowflake = mock.MagicMock()
        snowflake.query.return_value = []
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
            'temp': 'FOO_TEMP',
            'publication_status': {'attempted': False},
        }
        args = PartialSync2SFArgs(
            temp_test_dir='temp_test_dir', start_value='20', end_value='30', hard_delete=False
        )
        source_columns = ['"FOO_SOURCE_COLUMN" FOO_TYPE']
        primary_keys = ['FOO_PRIMARY']
        s3_key_pattern = 'FOO_PATTERN'
        size_bytes = 3
        where_clause_sql = 'test'
        with mock.patch(
            'pipelinewise.fastsync.partialsync.utils.iceberg_routes.'
            'require_native_target_format'
        ) as native_format_guard:
            load_into_snowflake(
                target,
                args,
                source_columns,
                primary_keys,
                s3_key_pattern,
                size_bytes,
                where_clause_sql,
            )

        native_format_guard.assert_called_once_with(
            snowflake,
            args,
            target['schema'],
            args.table,
            allow_missing=False,
        )

        self.assertEqual(snowflake.method_calls, [
            mock.call.copy_to_table(s3_key_pattern, target['schema'], args.table, size_bytes, is_temporary=True),
            mock.call.obfuscate_columns(target['schema'], args.table),
            mock.call.create_table(
                target_schema=target['schema'], table_name=target['table'], columns=source_columns,
                primary_key=primary_keys, is_temporary=False, sort_columns=False, allow_replace_table=False,
                normalize_primary_keys='if_created',
            ),
            mock.call.query('SHOW COLUMNS IN TABLE FOO_SCHEMA."FOO_TABLE"'),
            mock.call.add_columns(target['schema'], target['table'], {'"FOO_SOURCE_COLUMN"': 'FOO_TYPE'}),
            mock.call.publish_partial_sync(
                target['schema'], target['temp'], target['table'],
                ['"FOO_SOURCE_COLUMN"', '_SDC_EXTRACTED_AT', '_SDC_BATCHED_AT', '_SDC_DELETED_AT'],
                primary_keys, where_clause_sql, hard_delete=False,
            ),
            mock.call.drop_table(
                target['schema'],
                target['table'],
                is_temporary=True,
                max_attempts=3,
            )
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
        source_columns = ['"FOO_SOURCE_COLUMN" FOO_TYPE']
        primary_keys = ['FOO_PRIMARY']
        s3_key_pattern = 'FOO_PATTERN'
        size_bytes = 3
        where_clause_sql = 'test'
        timeline = []
        snowflake.copy_to_table.side_effect = lambda *_args, **_kwargs: timeline.append('copy')
        snowflake.obfuscate_columns.side_effect = (
            lambda *_args, **_kwargs: timeline.append('obfuscate')
        )
        snowflake.swap_tables.side_effect = lambda *_args, **_kwargs: timeline.append('swap')
        with mock.patch(
            'pipelinewise.fastsync.partialsync.utils.common_utils.'
            'apply_snowflake_table_grants',
            side_effect=lambda *_args, **_kwargs: timeline.append('staging grant'),
        ) as grant_mock, mock.patch(
            'pipelinewise.fastsync.partialsync.utils.iceberg_routes.'
            'require_native_target_format'
        ) as native_format_guard:
            load_into_snowflake(
                target,
                args,
                source_columns,
                primary_keys,
                s3_key_pattern,
                size_bytes,
                where_clause_sql,
            )

        native_format_guard.assert_called_once_with(
            snowflake,
            args,
            target['schema'],
            args.table,
            allow_missing=False,
        )
        grant_mock.assert_called_once_with(
            snowflake,
            args.target,
            target['schema'],
            args.table,
            is_temporary=True,
        )
        self.assertEqual(
            timeline,
            ['copy', 'obfuscate', 'staging grant', 'swap'],
        )

        self.assertEqual(snowflake.method_calls, [
            mock.call.copy_to_table(s3_key_pattern, target['schema'], args.table, size_bytes, is_temporary=True),
            mock.call.obfuscate_columns(target['schema'], args.table),
            mock.call.create_table(
                target_schema=target['schema'], table_name=target['table'], columns=source_columns,
                primary_key=primary_keys, is_temporary=False, sort_columns=False, allow_replace_table=False,
                normalize_primary_keys=False,
            ),
            mock.call.swap_tables(target['schema'], target['table']),
        ])

    def test_load_into_snowflake_stops_if_copy_fails(self):
        """A staging load failure must not mutate or publish the target."""
        snowflake = mock.MagicMock()
        snowflake.copy_to_table.side_effect = RuntimeError('copy failed')
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
            'temp': 'FOO_TEMP',
            'publication_status': {'attempted': False},
        }
        args = PartialSync2SFArgs(
            temp_test_dir='temp_test_dir', start_value='20', end_value='30'
        )
        source_columns = ['"FOO_SOURCE_COLUMN" FOO_TYPE']
        with self.assertRaisesRegex(RuntimeError, 'copy failed'):
            load_into_snowflake(
                target, args, source_columns, ['FOO_PRIMARY'], 'FOO_PATTERN', 3, 'test'
            )

        self.assertFalse(target['publication_status']['attempted'])
        self.assertEqual(snowflake.method_calls, [
            mock.call.copy_to_table('FOO_PATTERN', 'FOO_SCHEMA', args.table, 3, is_temporary=True)
        ])

    def test_load_into_snowflake_stops_if_created_target_is_not_native(self):
        """A missing or Iceberg post-create result stops before publication."""
        for table_format in (
            TABLE_FORMAT_MISSING,
            TABLE_FORMAT_MANAGED_ICEBERG_V3,
        ):
            with self.subTest(table_format=table_format):
                snowflake = mock.MagicMock()
                target = {
                    'sf_object': snowflake,
                    'schema': 'FOO_SCHEMA',
                    'table': 'FOO_TABLE',
                    'temp': 'FOO_TEMP',
                    'publication_status': {'attempted': False},
                }
                args = PartialSync2SFArgs(
                    temp_test_dir='temp_test_dir',
                    start_value='20',
                    end_value='30',
                )
                format_error = TableCompatibilityError(
                    f'native target format check found {table_format}'
                )

                with mock.patch(
                    'pipelinewise.fastsync.partialsync.utils.iceberg_routes.'
                    'require_native_target_format',
                    side_effect=format_error,
                ) as native_format_guard, self.assertRaisesRegex(
                    TableCompatibilityError, f'found {table_format}'
                ):
                    load_into_snowflake(
                        target,
                        args,
                        ['"FOO_SOURCE_COLUMN" FOO_TYPE'],
                        ['FOO_PRIMARY'],
                        'FOO_PATTERN',
                        3,
                        'test',
                    )

                native_format_guard.assert_called_once_with(
                    snowflake,
                    args,
                    target['schema'],
                    args.table,
                    allow_missing=False,
                )
                self.assertFalse(target['publication_status']['attempted'])
                snowflake.add_columns.assert_not_called()
                snowflake.swap_tables.assert_not_called()
                snowflake.publish_partial_sync.assert_not_called()
                self.assertEqual(snowflake.method_calls, [
                    mock.call.copy_to_table(
                        'FOO_PATTERN',
                        'FOO_SCHEMA',
                        args.table,
                        3,
                        is_temporary=True,
                    ),
                    mock.call.obfuscate_columns('FOO_SCHEMA', args.table),
                    mock.call.create_table(
                        target_schema='FOO_SCHEMA',
                        table_name='FOO_TABLE',
                        columns=['"FOO_SOURCE_COLUMN" FOO_TYPE'],
                        primary_key=['FOO_PRIMARY'],
                        is_temporary=False,
                        sort_columns=False,
                        allow_replace_table=False,
                        normalize_primary_keys='if_created',
                    ),
                ])

    def test_load_into_snowflake_stops_if_publication_fails(self):
        """A publication failure must preserve staging and propagate to the caller."""
        snowflake = mock.MagicMock()
        snowflake.query.return_value = []
        snowflake.publish_partial_sync.side_effect = RuntimeError('publication failed')
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
            'temp': 'FOO_TEMP',
            'publication_status': {'attempted': False},
        }
        args = PartialSync2SFArgs(
            temp_test_dir='temp_test_dir', start_value='20', end_value='30'
        )
        source_columns = ['"FOO_SOURCE_COLUMN" FOO_TYPE']
        merge_columns = [
            '"FOO_SOURCE_COLUMN"', '_SDC_EXTRACTED_AT', '_SDC_BATCHED_AT', '_SDC_DELETED_AT'
        ]
        with mock.patch(
            'pipelinewise.fastsync.partialsync.utils.iceberg_routes.'
            'require_native_target_format'
        ) as native_format_guard, self.assertRaisesRegex(
            RuntimeError, 'publication failed'
        ):
            load_into_snowflake(
                target, args, source_columns, ['FOO_PRIMARY'], 'FOO_PATTERN', 3, 'test'
            )

        native_format_guard.assert_called_once_with(
            snowflake,
            args,
            target['schema'],
            args.table,
            allow_missing=False,
        )

        self.assertTrue(target['publication_status']['attempted'])
        self.assertEqual(snowflake.method_calls, [
            mock.call.copy_to_table('FOO_PATTERN', 'FOO_SCHEMA', args.table, 3, is_temporary=True),
            mock.call.obfuscate_columns('FOO_SCHEMA', args.table),
            mock.call.create_table(
                target_schema='FOO_SCHEMA', table_name='FOO_TABLE', columns=source_columns,
                primary_key=['FOO_PRIMARY'], is_temporary=False, sort_columns=False, allow_replace_table=False,
                normalize_primary_keys='if_created',
            ),
            mock.call.query('SHOW COLUMNS IN TABLE FOO_SCHEMA."FOO_TABLE"'),
            mock.call.add_columns('FOO_SCHEMA', 'FOO_TABLE', {'"FOO_SOURCE_COLUMN"': 'FOO_TYPE'}),
            mock.call.publish_partial_sync(
                'FOO_SCHEMA', 'FOO_TEMP', 'FOO_TABLE', merge_columns, ['FOO_PRIMARY'], 'test', hard_delete=True,
            ),
        ])

    def test_load_into_snowflake_widens_native_text_before_publication(self):
        """A compatible narrow target is widened before additive DDL and MERGE."""
        snowflake = mock.MagicMock()
        snowflake.query.return_value = [
            _snowflake_column('BODY TEXT', 'TEXT', 16777216),
        ]
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'TABLE WITH SPACE',
            'temp': 'FOO_TEMP',
            'publication_status': {'attempted': False},
        }
        args = PartialSync2SFArgs(
            temp_test_dir='temp_test_dir', start_value='20', end_value='30'
        )
        source_columns = [
            f'"BODY TEXT" {SNOWFLAKE_MAX_VARCHAR}',
            f'"NEW BODY" {SNOWFLAKE_MAX_VARCHAR}',
        ]

        with mock.patch(
            'pipelinewise.fastsync.partialsync.utils.iceberg_routes.'
            'require_native_target_format'
        ):
            load_into_snowflake(
                target,
                args,
                source_columns,
                ['"BODY TEXT"'],
                'FOO_PATTERN',
                3,
                ' WHERE 1=1',
            )

        snowflake.widen_varchar_columns.assert_called_once_with(
            'FOO_SCHEMA', 'TABLE WITH SPACE', ['BODY TEXT']
        )
        snowflake.add_columns.assert_called_once_with(
            'FOO_SCHEMA',
            'TABLE WITH SPACE',
            {'"NEW BODY"': SNOWFLAKE_MAX_VARCHAR},
        )
        self.assertLess(
            snowflake.method_calls.index(
                mock.call.widen_varchar_columns(
                    'FOO_SCHEMA', 'TABLE WITH SPACE', ['BODY TEXT']
                )
            ),
            next(
                index
                for index, method_call in enumerate(snowflake.method_calls)
                if method_call[0] == 'publish_partial_sync'
            ),
        )
        self.assertTrue(target['publication_status']['attempted'])

    def test_load_into_snowflake_reports_widening_failure_before_publication(self):
        """A Snowflake DDL error remains actionable and cannot advance publication."""
        snowflake = mock.MagicMock()
        snowflake.query.return_value = [
            _snowflake_column('BODY', 'TEXT', 16777216),
        ]
        snowflake.widen_varchar_columns.side_effect = RuntimeError(
            'insufficient privileges'
        )
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
            'temp': 'FOO_TEMP',
            'publication_status': {'attempted': False},
        }
        args = PartialSync2SFArgs(
            temp_test_dir='temp_test_dir', start_value='20', end_value='30'
        )

        with mock.patch(
            'pipelinewise.fastsync.partialsync.utils.iceberg_routes.'
            'require_native_target_format'
        ), self.assertRaisesRegex(
            NativePartialSyncCompatibilityError,
            'insufficient privileges.*MERGE and state advancement did not run',
        ):
            load_into_snowflake(
                target,
                args,
                [f'"BODY" {SNOWFLAKE_MAX_VARCHAR}'],
                ['"BODY"'],
                'FOO_PATTERN',
                3,
                ' WHERE 1=1',
            )

        self.assertFalse(target['publication_status']['attempted'])
        snowflake.add_columns.assert_not_called()
        snowflake.publish_partial_sync.assert_not_called()

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

    @mock.patch(
        'pipelinewise.fastsync.commons.utils.save_state_file',
        side_effect=RuntimeError('state save failed'),
    )
    def test_update_state_file_propagates_save_failure(self, _mocked_save_state_file):
        """A failed shared-state write must fail the table sync."""
        args = PartialSync2SFArgs(
            temp_test_dir='foo_temp', end_value=None, state='foo_state'
        )

        with self.assertRaisesRegex(RuntimeError, 'state save failed'):
            update_state_file(args, {'foo': 2})

    def test_update_state_file_preserves_multiple_table_bookmarks(self):
        """Serialized workers retain every bookmark in their shared state file."""
        with TemporaryDirectory() as temp_directory:
            state_path = f'{temp_directory}/state.json'
            args = PartialSync2SFArgs(
                temp_test_dir=temp_directory, end_value=None, state=state_path
            )

            for table, bookmark in (
                ('schema.first_table', {'position': 1}),
                ('schema.second_table', {'position': 2}),
            ):
                args.table = table
                update_state_file(args, bookmark)

            with open(state_path, encoding='utf8') as state_file:
                state = json.load(state_file)

        self.assertEqual(
            state['bookmarks'],
            {
                'schema-first_table': {'position': 1},
                'schema-second_table': {'position': 2},
            },
        )

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
            'varchar_columns_to_widen': [],
        }
        actual_output = diff_source_target_columns(target_sf=sample_target_sf, source_columns=sample_source_columns)
        self.assertDictEqual(actual_output, expected_output)

    def test_varchar_width_diff_covers_missing_wide_and_narrow_columns(self):
        """Only existing compatible text columns below the source width need DDL."""
        snowflake = mock.MagicMock()
        snowflake.query.return_value = [
            _snowflake_column('NARROW BODY', 'TEXT', 16777216),
            _snowflake_column('WIDE_BODY', 'TEXT', 134217728),
            _snowflake_column('COUNT', 'FIXED'),
        ]
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
        }

        result = diff_source_target_columns(
            target,
            [
                f'"NARROW BODY" {SNOWFLAKE_MAX_VARCHAR}',
                f'"WIDE_BODY" {SNOWFLAKE_MAX_VARCHAR}',
                f'"MISSING_BODY" {SNOWFLAKE_MAX_VARCHAR}',
                '"COUNT" NUMBER',
            ],
        )

        self.assertEqual(result['varchar_columns_to_widen'], ['NARROW BODY'])
        self.assertEqual(
            result['added_columns'],
            {'"MISSING_BODY"': SNOWFLAKE_MAX_VARCHAR},
        )

    def test_varchar_width_diff_rejects_existing_non_text_column(self):
        """Automatic widening never converts an incompatible target data type."""
        snowflake = mock.MagicMock()
        snowflake.query.return_value = [_snowflake_column('BODY', 'FIXED')]
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
        }

        with self.assertRaisesRegex(
            NativePartialSyncCompatibilityError,
            r'existing target FOO_SCHEMA\."FOO_TABLE" has type FIXED.*FullSync',
        ):
            diff_source_target_columns(
                target,
                [f'"BODY" {SNOWFLAKE_MAX_VARCHAR}'],
            )

    def test_varchar_width_diff_rejects_missing_length_metadata(self):
        """An unprovable existing string width fails closed before target DML."""
        snowflake = mock.MagicMock()
        snowflake.query.return_value = [_snowflake_column('BODY', 'TEXT')]
        target = {
            'sf_object': snowflake,
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
        }

        with self.assertRaisesRegex(
            NativePartialSyncCompatibilityError,
            'cannot verify CHARACTER_MAXIMUM_LENGTH.*Widen.*manually',
        ):
            diff_source_target_columns(
                target,
                [f'"BODY" {SNOWFLAKE_MAX_VARCHAR}'],
            )

    def test_varchar_width_diff_accepts_snowflake_text_type_aliases(self):
        """Snowflake text aliases are all eligible for monotonic widening."""
        target = {
            'schema': 'FOO_SCHEMA',
            'table': 'FOO_TABLE',
        }

        for target_type in ('CHAR', 'CHARACTER', 'CHARACTER VARYING', 'STRING', 'TEXT', 'VARCHAR'):
            with self.subTest(target_type=target_type):
                snowflake = mock.MagicMock()
                snowflake.query.return_value = [
                    _snowflake_column('BODY', target_type, 16777216)
                ]
                target['sf_object'] = snowflake

                result = diff_source_target_columns(
                    target,
                    [f'"BODY" {SNOWFLAKE_MAX_VARCHAR}'],
                )

                self.assertEqual(result['varchar_columns_to_widen'], ['BODY'])

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

    def test_validate_dynamic_value_raise_excp_if_return_more_than_one_column_or_row(self):
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

    def test_validate_dynamic_value_returns_missing_boundary_sentinel(self):
        """No dynamic scalar is distinct from a valid empty source range."""
        query_object = mock.MagicMock()
        test_query = '<D>SELECT id FROM foo WHERE id=1;'
        for query_result in ([], [(None,)], [{'id': None}]):
            with self.subTest(query_result=query_result):
                query_object.return_value = query_result
                self.assertIs(
                    DYNAMIC_BOUNDARY_NOT_READY,
                    validate_boundary_value(query_object, test_query),
                )

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
