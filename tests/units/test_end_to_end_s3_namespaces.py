"""Tests for remote-object isolation between concurrent E2E jobs."""

from pathlib import Path
from unittest import TestCase, mock

from tests.end_to_end.helpers import env as env_module
from tests.end_to_end.helpers.env import E2EEnv
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres
from tests.end_to_end.target_snowflake.tap_postgres import (
    test_replicate_pg_to_sf_with_archive_load_files as archive_module,
)
from tests.end_to_end.target_snowflake.tap_s3 import TapS3
from tests.end_to_end.test_target_postgres import (
    TestTargetPostgres as TargetPostgresCase,
)


class EndToEndS3NamespaceTestCase(TestCase):
    """Validate local defaults and optional CI namespace isolation."""

    @staticmethod
    def _load_environment(environment):
        e2e = object.__new__(E2EEnv)
        e2e.sf_schema_postfix = '_generated'
        with mock.patch.object(env_module, 'load_dotenv'), mock.patch.object(
            E2EEnv, '_is_env_connector_configured', return_value=True
        ), mock.patch.dict(env_module.os.environ, environment, clear=True):
            e2e._load_env()  # pylint: disable=protected-access
        return e2e

    def test_keeps_default_s3_paths_without_an_e2e_namespace(self):
        """Local E2E runs retain the established shared fixture paths."""
        e2e = self._load_environment({
            'TARGET_SNOWFLAKE_S3_KEY_PREFIX': 'staging/base/',
        })

        self.assertEqual(
            e2e.get_conn_env_var('TARGET_SNOWFLAKE', 'S3_KEY_PREFIX'),
            'staging/base/',
        )
        self.assertEqual(
            e2e.get_conn_env_var(
                'TARGET_SNOWFLAKE',
                'ARCHIVE_LOAD_FILES_S3_PREFIX',
            ),
            'archive_folder',
        )
        self.assertEqual(
            e2e.get_conn_env_var('TAP_S3_CSV', 'KEY_PREFIX'),
            'ppw_e2e_tap_s3_csv',
        )

    def test_namespaces_each_remote_e2e_s3_path(self):
        """Concurrent CI shards receive disjoint staging and fixture paths."""
        e2e = self._load_environment({
            'PIPELINEWISE_E2E_NAMESPACE': 'run_42_pg-shard',
            'TARGET_SNOWFLAKE_S3_KEY_PREFIX': 'staging/base/',
        })

        self.assertEqual(
            e2e.get_conn_env_var('TARGET_SNOWFLAKE', 'S3_KEY_PREFIX'),
            'staging/base/run_42_pg-shard/',
        )
        self.assertEqual(
            e2e.get_conn_env_var(
                'TARGET_SNOWFLAKE',
                'ARCHIVE_LOAD_FILES_S3_PREFIX',
            ),
            'archive_folder/run_42_pg-shard',
        )
        self.assertEqual(
            e2e.get_conn_env_var('TAP_S3_CSV', 'KEY_PREFIX'),
            'ppw_e2e_tap_s3_csv/run_42_pg-shard',
        )

    def test_rejects_an_unsafe_e2e_namespace(self):
        """A namespace cannot escape the one S3 path segment allocated to it."""
        with self.assertRaisesRegex(
            ValueError,
            'PIPELINEWISE_E2E_NAMESPACE must contain only',
        ):
            self._load_environment({
                'PIPELINEWISE_E2E_NAMESPACE': '../another-run',
            })

    def test_s3_fixture_setup_uses_the_rendered_key_prefix(self):
        """Fixture uploads use the namespaced prefix rendered into tap config."""
        e2e = mock.Mock(spec=E2EEnv)
        values = {
            ('TAP_S3_CSV', 'BUCKET'): 'source-bucket',
            ('TAP_S3_CSV', 'AWS_KEY'): 'access-key',
            ('TAP_S3_CSV', 'AWS_SECRET_ACCESS_KEY'): 'secret-key',
            ('TAP_S3_CSV', 'KEY_PREFIX'): 'ppw_e2e_tap_s3_csv/run_42_s3',
        }
        e2e.get_conn_env_var.side_effect = (
            lambda connector, key: values[(connector, key)]
        )
        s3_client = mock.Mock()

        with mock.patch.object(
            env_module.boto3,
            'client',
            return_value=s3_client,
        ):
            E2EEnv.setup_tap_s3_csv(e2e)

        self.assertEqual(
            [call.args[2] for call in s3_client.upload_file.call_args_list],
            [
                'ppw_e2e_tap_s3_csv/run_42_s3/mock_data_1.csv',
                'ppw_e2e_tap_s3_csv/run_42_s3/mock_data_2.csv',
            ],
        )

    def test_s3_fixture_cleanup_deletes_only_the_namespaced_fixture_keys(self):
        """CI cleanup removes the two exact keys uploaded by that run."""
        e2e = mock.Mock(spec=E2EEnv)
        e2e.e2e_namespace = 'run_42_s3'
        values = {
            ('TAP_S3_CSV', 'BUCKET'): 'source-bucket',
            ('TAP_S3_CSV', 'AWS_KEY'): 'access-key',
            ('TAP_S3_CSV', 'AWS_SECRET_ACCESS_KEY'): 'secret-key',
            ('TAP_S3_CSV', 'KEY_PREFIX'): 'ppw_e2e_tap_s3_csv/run_42_s3',
        }
        e2e.get_conn_env_var.side_effect = (
            lambda connector, key: values[(connector, key)]
        )
        s3_client = mock.Mock()

        with mock.patch.object(
            env_module.boto3,
            'client',
            return_value=s3_client,
        ):
            E2EEnv.cleanup_tap_s3_csv(e2e)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='source-bucket',
            Delete={
                'Objects': [
                    {
                        'Key': (
                            'ppw_e2e_tap_s3_csv/run_42_s3/mock_data_1.csv'
                        )
                    },
                    {
                        'Key': (
                            'ppw_e2e_tap_s3_csv/run_42_s3/mock_data_2.csv'
                        )
                    },
                ],
                'Quiet': True,
            },
        )

    def test_s3_fixture_cleanup_retains_legacy_local_fixture_keys(self):
        """An unset namespace keeps the fixed objects used by older branches."""
        e2e = mock.Mock(spec=E2EEnv)
        e2e.e2e_namespace = ''

        with mock.patch.object(env_module.boto3, 'client') as boto_client:
            E2EEnv.cleanup_tap_s3_csv(e2e)

        boto_client.assert_not_called()

    def test_tap_s3_upload_precedes_discovery_and_cleans_up_after_failure(self):
        """A fresh namespace exists for discovery and is cleaned on failure."""
        events = []

        def fail_import():
            events.append('import')
            raise RuntimeError('discovery failed')

        e2e = mock.Mock(spec=E2EEnv)
        e2e.env = {'TAP_S3_CSV': {'is_configured': True}}
        e2e.sf_schema_postfix_is_override = False
        e2e.setup_tap_s3_csv.side_effect = lambda: events.append('upload')
        e2e.cleanup_tap_s3_csv.side_effect = lambda: events.append('cleanup')
        target = TapS3(methodName='runTest')
        target.get_e2e_env = mock.Mock(return_value=e2e)
        target.remove_dir_from_config_dir = mock.Mock()
        target.check_validate_taps = mock.Mock(
            side_effect=lambda: events.append('validate')
        )
        target.check_import_config = mock.Mock(side_effect=fail_import)

        with mock.patch.object(
            target,
            'check_snowflake_credentials_provided',
        ), mock.patch.object(target, '_current_run_schemas', return_value=[]):
            with self.assertRaisesRegex(RuntimeError, 'discovery failed'):
                target.setUp(tap_id='s3_csv_to_sf', target_id='snowflake')

            self.assertEqual(events, ['upload', 'validate', 'import'])
            target.doCleanups()
            self.assertEqual(events, ['upload', 'validate', 'import', 'cleanup'])

    def test_remote_s3_templates_use_the_rendered_prefixes(self):
        """Tap discovery and archive config consume the derived namespace."""
        test_project = Path(__file__).resolve().parents[1] / 'end_to_end' / 'test-project'
        for target in ('pg', 'sf'):
            with self.subTest(target=target):
                template = Path(
                    test_project,
                    f'tap_s3_csv_to_{target}.yml.template',
                ).read_text(encoding='utf-8')
                self.assertIn(
                    '^${TAP_S3_CSV_KEY_PREFIX}/mock_data_1.csv$',
                    template,
                )
                self.assertIn(
                    '^${TAP_S3_CSV_KEY_PREFIX}/mock_data_2.csv$',
                    template,
                )

        archive_template = Path(
            test_project,
            'tap_postgres_to_sf_archive_load_files.yml.template',
        ).read_text(encoding='utf-8')
        self.assertIn(
            'archive_load_files_s3_prefix: '
            '"${TARGET_SNOWFLAKE_ARCHIVE_LOAD_FILES_S3_PREFIX}"',
            archive_template,
        )

    def test_archive_helpers_use_the_rendered_s3_prefix(self):
        """Archive cleanup and lookup stay inside the current CI namespace."""
        target = archive_module.TestReplicatePGToSFWithArchiveLoadFiles(
            methodName='runTest'
        )
        target.s3_bucket = 'staging-bucket'
        target.archive_s3_prefix = 'archive_folder/run_42_pg'
        target.s3_client = mock.Mock()
        target.s3_client.list_objects.side_effect = [
            {'Contents': [{'Key': 'archive_folder/run_42_pg/dangling.csv.gz'}]},
            {'Contents': [{'Key': 'archive_folder/run_42_pg/current.csv.gz'}]},
        ]

        target.delete_dangling_files_from_archive()
        result = target.get_files_from_s3_for_table('city')

        self.assertEqual(
            target.s3_client.list_objects.call_args_list,
            [
                mock.call(
                    Bucket='staging-bucket',
                    Prefix=(
                        'archive_folder/run_42_pg/'
                        'postgres_to_sf_archive_load_files/'
                    ),
                ),
                mock.call(
                    Bucket='staging-bucket',
                    Prefix=(
                        'archive_folder/run_42_pg/'
                        'postgres_to_sf_archive_load_files/city'
                    ),
                ),
            ],
        )
        target.s3_client.delete_object.assert_called_once_with(
            Bucket='staging-bucket',
            Key='archive_folder/run_42_pg/dangling.csv.gz',
        )
        self.assertEqual(
            result,
            [{'Key': 'archive_folder/run_42_pg/current.csv.gz'}],
        )

    def test_archive_test_registers_namespaced_cleanup(self):
        """Unittest cleanup removes archive files even after a test failure."""
        target = archive_module.TestReplicatePGToSFWithArchiveLoadFiles(
            methodName='runTest'
        )
        e2e = mock.Mock(spec=E2EEnv)
        e2e.e2e_namespace = 'run_42_pg'
        values = {
            ('TARGET_SNOWFLAKE', 'S3_BUCKET'): 'staging-bucket',
            (
                'TARGET_SNOWFLAKE',
                'ARCHIVE_LOAD_FILES_S3_PREFIX',
            ): 'archive_folder/run_42_pg',
        }
        e2e.get_conn_env_var.side_effect = (
            lambda connector, key: values[(connector, key)]
        )
        cleanup = mock.Mock()
        target.e2e_env = e2e

        with mock.patch.object(TapPostgres, 'setUp'), mock.patch.object(
            target,
            'delete_dangling_files_from_archive',
            cleanup,
        ):
            target.setUp()
            target.doCleanups()
            cleanup.assert_called_once_with()

    def test_archive_test_retains_legacy_local_archive_files(self):
        """An unset namespace retains the archive behavior used by local runs."""
        target = archive_module.TestReplicatePGToSFWithArchiveLoadFiles(
            methodName='runTest'
        )
        e2e = mock.Mock(spec=E2EEnv)
        e2e.e2e_namespace = ''
        values = {
            ('TARGET_SNOWFLAKE', 'S3_BUCKET'): 'staging-bucket',
            (
                'TARGET_SNOWFLAKE',
                'ARCHIVE_LOAD_FILES_S3_PREFIX',
            ): 'archive_folder',
        }
        e2e.get_conn_env_var.side_effect = (
            lambda connector, key: values[(connector, key)]
        )
        cleanup = mock.Mock()
        target.e2e_env = e2e

        with mock.patch.object(TapPostgres, 'setUp'), mock.patch.object(
            target,
            'delete_dangling_files_from_archive',
            cleanup,
        ):
            target.setUp()
            target.doCleanups()
            cleanup.assert_not_called()

    def test_target_postgres_cleans_s3_fixtures_at_class_teardown(self):
        """Dependent pytest methods retain fixtures until the class completes."""
        e2e = mock.Mock(spec=E2EEnv)
        e2e.env = {'TAP_S3_CSV': {'is_configured': True}}
        TargetPostgresCase.tap_s3_cleanup_env = e2e

        TargetPostgresCase.teardown_class()

        e2e.cleanup_tap_s3_csv.assert_called_once_with()
        self.assertIsNone(TargetPostgresCase.tap_s3_cleanup_env)
