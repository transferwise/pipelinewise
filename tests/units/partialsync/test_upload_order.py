from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from pipelinewise.fastsync.partialsync import mysql_to_snowflake
from pipelinewise.fastsync.partialsync import postgres_to_snowflake
from pipelinewise.fastsync.commons.partial_sync_boundary import (
    PartialSyncBoundary,
)
from tests.units.partialsync.utils import PartialSync2SFArgs


class PartialSyncUploadOrderTestCase(TestCase):
    """Composition tests for local and remote PartialSync staging lifecycle."""

    # pylint: disable=too-many-locals
    def _run_route(
        self,
        sync_module,
        tap_class_name,
        **scenario,
    ):
        publication_error = scenario.get('publication_error')
        staging_error = scenario.get('staging_error')
        cleanup_error = scenario.get('cleanup_error')
        grant_error = scenario.get('grant_error')
        table_start_value = scenario.get('table_start_value', '<S>1')
        table_end_value = scenario.get('table_end_value')
        drop_target_table = scenario.get('drop_target_table', False)
        source_query_result = scenario.get('source_query_result', ((0,),))
        with TemporaryDirectory() as temp_directory:
            file_part = Path(temp_directory, 'part.csv.gz')
            file_part.write_text('data', encoding='utf8')
            args = PartialSync2SFArgs(
                temp_test_dir=temp_directory,
                end_value=table_end_value,
            )
            table = ('foo', {
                'column': 'foo_column',
                'start_value': table_start_value,
                'end_value': table_end_value,
                'drop_target_table': drop_target_table,
            })
            timeline = []

            source = mock.MagicMock()
            source.query.return_value = source_query_result
            source.export_source_table_data.return_value = [str(file_part)]
            source.map_column_types_to_target.return_value = {
                'columns': ['"ID" NUMBER'],
                'primary_key': ['"ID"'],
                'source_column_names': ['foo_column'],
            }
            snowflake = mock.MagicMock()

            def getsize(path):
                timeline.append('getsize')
                self.assertTrue(Path(path).exists())
                return Path(path).stat().st_size

            def upload(path, tmp_dir=None):
                timeline.append('upload')
                self.assertTrue(Path(path).exists())
                self.assertEqual(tmp_dir, temp_directory)
                return 'staging/part.csv.gz'

            def publish(*load_args):
                timeline.append('publish')
                if staging_error:
                    raise staging_error
                load_args[0]['publication_status']['attempted'] = True
                self.assertFalse(file_part.exists())
                if publication_error:
                    raise publication_error

            def delete_object(**kwargs):
                timeline.append('remote_delete')
                self.assertEqual(kwargs, {
                    'Bucket': args.target['s3_bucket'],
                    'Key': 'staging/part.csv.gz',
                })
                if cleanup_error:
                    raise cleanup_error

            def save_state(*_args):
                timeline.append('state')

            def apply_grants(*_args, **_kwargs):
                timeline.append('grants')
                if grant_error:
                    raise grant_error

            snowflake.upload_to_s3.side_effect = upload
            snowflake.s3.delete_object.side_effect = delete_object
            with mock.patch.object(
                sync_module, tap_class_name, return_value=source
            ), mock.patch.object(
                sync_module, 'FastSyncTargetSnowflake', return_value=snowflake
            ), mock.patch(
                'pipelinewise.fastsync.partialsync.rdbms_to_snowflake.os.path.getsize',
                side_effect=getsize,
            ), mock.patch.object(
                sync_module.utils, 'load_into_snowflake', side_effect=publish
            ) as load_into_snowflake, mock.patch.object(
                sync_module.utils.common_utils,
                'save_state_file',
                side_effect=save_state,
            ) as save_state_file, mock.patch.object(
                sync_module.common_utils, 'get_bookmark_for_table', return_value='bookmark'
            ) as get_bookmark, mock.patch.object(
                sync_module.common_utils, 'get_target_schema', return_value='foo_schema'
            ) as get_target_schema, mock.patch.object(
                sync_module.common_utils,
                'apply_snowflake_table_grants',
                side_effect=apply_grants,
            ) as apply_grants_mock, mock.patch.object(
                sync_module.iceberg_routes,
                'require_native_target_format',
            ) as native_format_guard:
                result = sync_module.partial_sync_table(table, args)

            return {
                'args': args,
                'file_exists': file_part.exists(),
                'load': load_into_snowflake,
                'result': result,
                'source': source,
                'snowflake': snowflake,
                'state': save_state_file,
                'grants': apply_grants_mock,
                'bookmark': get_bookmark,
                'target_schema': get_target_schema,
                'native_format_guard': native_format_guard,
                'timeline': timeline,
            }

    def _assert_successful_lifecycle(self, sync_module, tap_class_name):
        actual = self._run_route(sync_module, tap_class_name)

        self.assertIs(actual['result'], True)
        self.assertEqual(
            actual['timeline'],
            ['getsize', 'upload', 'publish', 'grants', 'remote_delete', 'state'],
        )
        self.assertFalse(actual['file_exists'])
        self.assertEqual(actual['load'].call_args.args[5], 4)
        actual['snowflake'].s3.delete_object.assert_called_once_with(
            Bucket=actual['args'].target['s3_bucket'],
            Key='staging/part.csv.gz',
        )
        actual['state'].assert_called_once_with(
            actual['args'].state,
            'foo',
            'bookmark',
        )

    def _assert_publication_failure_cleans_remote_object(self, sync_module, tap_class_name):
        actual = self._run_route(
            sync_module,
            tap_class_name,
            publication_error=RuntimeError('publication failed'),
        )

        self.assertEqual(actual['result'], 'foo: publication failed')
        self.assertEqual(
            actual['timeline'],
            ['getsize', 'upload', 'publish', 'grants', 'remote_delete'],
        )
        actual['snowflake'].s3.delete_object.assert_called_once_with(
            Bucket=actual['args'].target['s3_bucket'],
            Key='staging/part.csv.gz',
        )
        actual['snowflake'].drop_table.assert_called_once_with(
            'foo_schema',
            'foo',
            is_temporary=True,
            max_attempts=3,
        )
        actual['state'].assert_not_called()

    def _assert_staging_failure_does_not_repair_grants(
            self, sync_module, tap_class_name):
        actual = self._run_route(
            sync_module,
            tap_class_name,
            staging_error=RuntimeError('staging failed'),
        )

        self.assertEqual(actual['result'], 'foo: staging failed')
        self.assertEqual(
            actual['timeline'],
            ['getsize', 'upload', 'publish', 'remote_delete'],
        )
        actual['grants'].assert_not_called()
        actual['state'].assert_not_called()

    def _assert_cleanup_failure_withholds_state(self, sync_module, tap_class_name):
        with self.assertLogs('pipelinewise.fastsync.commons.utils', level='WARNING') as logs:
            actual = self._run_route(
                sync_module,
                tap_class_name,
                cleanup_error=RuntimeError('cleanup failed'),
            )

        self.assertIn(
            'PartialSync staging cleanup after successful publication failed after 3 attempts',
            actual['result'],
        )
        self.assertIn('staging cleanup failed', actual['result'])
        self.assertEqual(
            actual['timeline'],
            ['getsize', 'upload', 'publish']
            + ['grants']
            + ['remote_delete'] * 6,
        )
        actual['state'].assert_not_called()
        self.assertIn('cleanup failed', logs.output[0])

    def _assert_grant_failure_withholds_state(self, sync_module, tap_class_name):
        actual = self._run_route(
            sync_module,
            tap_class_name,
            grant_error=RuntimeError('grant failed'),
        )

        self.assertEqual(actual['result'], 'foo: grant failed')
        self.assertEqual(
            actual['timeline'],
            ['getsize', 'upload', 'publish']
            + ['grants'] * 3
            + ['remote_delete'],
        )
        actual['state'].assert_not_called()
        self.assertEqual(actual['grants'].call_count, 3)
        self.assertTrue(all(
            grant_call == mock.call(
                actual['snowflake'],
                actual['args'].target,
                'foo_schema',
                'foo',
                is_temporary=False,
            )
            for grant_call in actual['grants'].call_args_list
        ))

    def _assert_dynamic_zero_end_boundary_is_retained(self, sync_module, tap_class_name):
        actual = self._run_route(
            sync_module,
            tap_class_name,
            table_end_value='<D>SELECT 0',
        )
        expected_where_clause = (
            ' WHERE "FOO_COLUMN" >= \'1\' AND "FOO_COLUMN" <= \'0\''
        )

        self.assertIs(actual['result'], True)
        self.assertEqual(actual['load'].call_args.args[6], expected_where_clause)
        actual['state'].assert_not_called()
        runtime_args = actual['source'].export_source_table_data.call_args.args[0]
        self.assertIsNot(runtime_args, actual['args'])
        self.assertNotEqual(runtime_args.table, actual['args'].table)
        actual['source'].export_source_table_data.assert_called_once_with(
            runtime_args,
            actual['args'].target.get('tap_id'),
            boundary=PartialSyncBoundary('foo_column', '1', 0),
        )

    def _assert_empty_dynamic_boundary_is_successful_noop(self, sync_module, tap_class_name):
        for boundary in ('start', 'end'):
            for query_result in ([], [(None,)], [{'next_boundary': None}]):
                with self.subTest(boundary=boundary, query_result=query_result):
                    kwargs = {
                        'source_query_result': query_result,
                        f'table_{boundary}_value': '<D>SELECT next_boundary',
                    }
                    actual = self._run_route(sync_module, tap_class_name, **kwargs)

                    self.assertIs(actual['result'], True)
                    self.assertEqual(actual['timeline'], [])
                    if tap_class_name == 'FastSyncTapMySql':
                        actual['source'].open_connections.assert_called_once_with()
                        actual['source'].close_connections.assert_called_once_with(silent=True)
                    else:
                        actual['source'].open_connection.assert_called_once_with()
                        actual['source'].close_connection.assert_called_once_with()
                    actual['bookmark'].assert_not_called()
                    actual['source'].map_column_types_to_target.assert_not_called()
                    actual['source'].export_source_table_data.assert_not_called()
                    actual['target_schema'].assert_not_called()
                    actual['native_format_guard'].assert_not_called()
                    actual['snowflake'].create_schema.assert_not_called()
                    actual['snowflake'].create_table.assert_not_called()
                    actual['snowflake'].upload_to_s3.assert_not_called()
                    actual['load'].assert_not_called()
                    actual['grants'].assert_not_called()
                    actual['snowflake'].s3.delete_object.assert_not_called()
                    actual['state'].assert_not_called()

    def _run_main_with_results(self, sync_module, sync_results):
        args = PartialSync2SFArgs(temp_test_dir='FOO_DIR')
        args.table = 'first,second'
        args.column = 'id,id'
        args.start_value = '1,1'
        args.end_value = '2,2'
        sync_tables = {'first': {}, 'second': {}}
        pool = mock.MagicMock()
        worker_pool = pool.return_value.__enter__.return_value
        worker_pool.map.return_value = sync_results

        exit_code = None
        with mock.patch.object(
            sync_module.utils,
            'parse_args_for_partial_sync',
            return_value=args,
        ), mock.patch.object(
            sync_module.utils,
            'get_sync_tables',
            return_value=sync_tables,
        ), mock.patch.object(
            sync_module.common_utils,
            'get_pool_size',
            return_value=2,
        ), mock.patch.object(
            sync_module.multiprocessing,
            'Pool',
            pool,
        ), self.assertLogs('pipelinewise') as logs:
            try:
                sync_module.main_impl()
            except SystemExit as exc:
                exit_code = exc.code

        worker_pool.map.assert_called_once()
        self.assertEqual(
            worker_pool.map.call_args.args[1],
            sync_tables.items(),
        )
        summary = next(log for log in logs.output if 'SUMMARY' in log)
        return exit_code, summary

    def test_partial_sync_summary_treats_only_true_as_success(self):
        """Both source routes accept successful no-ops without hiding failures."""
        scenarios = (
            (
                [True, True],
                'Exceptions during table sync   : []',
                None,
            ),
            (
                [True, 'second: failed'],
                "Exceptions during table sync   : ['second: failed']",
                1,
            ),
            (
                [True, False],
                'Exceptions during table sync   : [False]',
                1,
            ),
        )
        for sync_module in (mysql_to_snowflake, postgres_to_snowflake):
            for results, failure_log, expected_exit in scenarios:
                with self.subTest(
                    route=sync_module.__name__,
                    results=results,
                ):
                    exit_code, summary = self._run_main_with_results(
                        sync_module,
                        results,
                    )
                    self.assertEqual(exit_code, expected_exit)
                    self.assertIn(failure_log, summary)

    def test_mariadb_partial_sync_staging_lifecycle_order(self):
        """MariaDB cleans local then remote staging around successful publication."""
        self._assert_successful_lifecycle(
            mysql_to_snowflake, 'FastSyncTapMySql'
        )

    def test_postgres_partial_sync_staging_lifecycle_order(self):
        """PostgreSQL cleans local then remote staging around successful publication."""
        self._assert_successful_lifecycle(
            postgres_to_snowflake, 'FastSyncTapPostgres'
        )

    def test_mariadb_publication_failure_cleans_remote_object(self):
        """MariaDB removes uploaded staging data when publication fails."""
        self._assert_publication_failure_cleans_remote_object(
            mysql_to_snowflake, 'FastSyncTapMySql'
        )

    def test_postgres_publication_failure_cleans_remote_object(self):
        """PostgreSQL removes uploaded staging data when publication fails."""
        self._assert_publication_failure_cleans_remote_object(
            postgres_to_snowflake, 'FastSyncTapPostgres'
        )

    def test_mariadb_staging_failure_does_not_repair_live_grants(self):
        """A MariaDB staging failure is not treated as live publication."""
        self._assert_staging_failure_does_not_repair_grants(
            mysql_to_snowflake, 'FastSyncTapMySql'
        )

    def test_postgres_staging_failure_does_not_repair_live_grants(self):
        """A PostgreSQL staging failure is not treated as live publication."""
        self._assert_staging_failure_does_not_repair_grants(
            postgres_to_snowflake, 'FastSyncTapPostgres'
        )

    def test_mariadb_cleanup_failure_withholds_state(self):
        """MariaDB cannot advance state while staging cleanup is unresolved."""
        self._assert_cleanup_failure_withholds_state(
            mysql_to_snowflake, 'FastSyncTapMySql'
        )

    def test_postgres_cleanup_failure_withholds_state(self):
        """PostgreSQL cannot advance state while staging cleanup is unresolved."""
        self._assert_cleanup_failure_withholds_state(
            postgres_to_snowflake, 'FastSyncTapPostgres'
        )

    def test_mariadb_grant_failure_withholds_state(self):
        """MariaDB cannot advance state when published access is not repaired."""
        self._assert_grant_failure_withholds_state(
            mysql_to_snowflake, 'FastSyncTapMySql'
        )

    def test_postgres_grant_failure_withholds_state(self):
        """PostgreSQL cannot advance state when published access is not repaired."""
        self._assert_grant_failure_withholds_state(
            postgres_to_snowflake, 'FastSyncTapPostgres'
        )

    def test_mariadb_dynamic_zero_end_boundary_is_retained(self):
        """MariaDB keeps a dynamic numeric zero as the requested upper bound."""
        self._assert_dynamic_zero_end_boundary_is_retained(
            mysql_to_snowflake, 'FastSyncTapMySql'
        )

    def test_postgres_dynamic_zero_end_boundary_is_retained(self):
        """PostgreSQL keeps a dynamic numeric zero as the requested upper bound."""
        self._assert_dynamic_zero_end_boundary_is_retained(
            postgres_to_snowflake, 'FastSyncTapPostgres'
        )

    def test_omitted_drop_target_setting_keeps_native_partial_sync_additive(self):
        """An optional None setting retains the historical False default."""
        actual = self._run_route(
            postgres_to_snowflake,
            'FastSyncTapPostgres',
            drop_target_table=None,
        )

        self.assertIs(actual['result'], True)
        self.assertIs(actual['load'].call_args.args[1].drop_target_table, False)

    def test_mariadb_empty_dynamic_boundary_is_successful_noop(self):
        """MariaDB treats a missing dynamic boundary as side-effect-free success."""
        self._assert_empty_dynamic_boundary_is_successful_noop(
            mysql_to_snowflake, 'FastSyncTapMySql'
        )

    def test_postgres_empty_dynamic_boundary_is_successful_noop(self):
        """PostgreSQL treats a missing dynamic boundary as side-effect-free success."""
        self._assert_empty_dynamic_boundary_is_successful_noop(
            postgres_to_snowflake, 'FastSyncTapPostgres'
        )
