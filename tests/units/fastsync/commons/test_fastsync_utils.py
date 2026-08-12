import argparse
import fcntl
import multiprocessing
import os
import pytest

from tempfile import TemporaryDirectory
from unittest import TestCase, mock
from unittest.mock import patch

from pipelinewise.fastsync.commons import utils
from pipelinewise.fastsync.commons.utils import NotSelectedTableException

RESOURCES_DIR = '{}/resources'.format(os.path.dirname(__file__))


def _save_state_worker(state_path, table, bookmark, ready, finished):
    ready.set()
    utils.save_state_file(state_path, table, bookmark)
    finished.set()


# pylint: disable=missing-function-docstring,invalid-name,too-few-public-methods
class MySqlMock:
    """
    MySQL mock
    """

    def fetch_current_log_pos(self):
        return {'log_file': 'mysqld-bin.000001', 'log_pos': '123456', 'version': 1}

    # pylint: disable=unused-argument
    def fetch_current_incremental_key_pos(self, table, replication_key):
        return {
            'replication_key': replication_key,
            'replication_key_value': 123456,
            'version': 1,
        }


class PostgresMock:
    """
    Postgres mock
    """

    def fetch_current_log_pos(self):
        return {'lsn': '16/B374D848', 'version': 1}

    # pylint: disable=unused-argument
    def fetch_current_incremental_key_pos(self, table, replication_key):
        return {
            'replication_key': replication_key,
            'replication_key_value': 123456,
            'version': 1,
        }


class S3CsvMock:
    """
    S3 CSV mock
    """

    # pylint: disable=unused-argument
    def fetch_current_incremental_key_pos(self, table, replication_key):
        return {'modified_since': '2019-11-15T07:39:44.171098'}


class TestFastSyncUtils(TestCase):  # pylint: disable=too-many-public-methods
    """
    Unit tests for fastsync common functions
    """

    def test_save_state_file_logs_completed_stream_write(self):
        """The completion marker is emitted only after atomic persistence succeeds."""
        with TemporaryDirectory() as temp_directory, self.assertLogs(
            utils.LOGGER, level='INFO'
        ) as logs:
            utils.save_state_file(
                f'{temp_directory}/state.json',
                'public.table',
                {'lsn': '16/B374D848'},
            )

        self.assertIn(
            'INFO:pipelinewise.fastsync.commons.utils:'
            'FastSync state updated for stream: public-table',
            logs.output,
        )

    @patch('pipelinewise.fastsync.commons.utils.save_dict_to_json')
    def test_save_state_file_does_not_log_completed_write_on_failure(self, save_mock):
        """A failed atomic replace cannot produce a successful state marker."""
        save_mock.side_effect = OSError('state write failed')
        with TemporaryDirectory() as temp_directory, patch.object(
            utils.LOGGER, 'info'
        ) as log_mock, self.assertRaisesRegex(OSError, 'state write failed'):
            utils.save_state_file(
                f'{temp_directory}/state.json',
                'public.table',
                {'lsn': '16/B374D848'},
            )

        log_mock.assert_not_called()

    def test_save_state_file_serializes_independent_processes(self):
        """Concurrent writers must retain every table bookmark."""
        context = multiprocessing.get_context('spawn')
        processes = []

        with TemporaryDirectory() as temp_directory:
            state_path = f'{temp_directory}/state.json'
            ready_events = [context.Event(), context.Event()]
            finished_events = [context.Event(), context.Event()]
            lock_path = f'{os.path.realpath(state_path)}.lock'

            try:
                with open(lock_path, 'a', encoding='utf-8') as lock_file:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                    for index in range(2):
                        process = context.Process(
                            target=_save_state_worker,
                            args=(
                                state_path,
                                f'schema.table_{index}',
                                {'position': index},
                                ready_events[index],
                                finished_events[index],
                            ),
                        )
                        process.start()
                        processes.append(process)

                    for ready in ready_events:
                        self.assertTrue(ready.wait(timeout=10))
                    self.assertFalse(
                        any(finished.wait(timeout=0.5) for finished in finished_events)
                    )
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

                for process in processes:
                    process.join(timeout=10)
                    self.assertEqual(process.exitcode, 0)

                self.assertEqual(
                    utils.load_json(state_path)['bookmarks'],
                    {
                        'schema-table_0': {'position': 0},
                        'schema-table_1': {'position': 1},
                    },
                )
            finally:
                for process in processes:
                    if process.is_alive():
                        process.terminate()
                    process.join(timeout=10)

    def test_upload_files_to_s3_uploads_all_parts_before_local_cleanup(self):
        """Every remote upload completes before any local source part is removed."""
        with TemporaryDirectory() as temp_directory:
            file_parts = [
                os.path.join(temp_directory, 'export.part0'),
                os.path.join(temp_directory, 'export.part1'),
            ]
            for file_part in file_parts:
                with open(file_part, 'w', encoding='utf8') as export_file:
                    export_file.write('data')

            snowflake = mock.MagicMock()

            def upload(file_part, tmp_dir=None):
                self.assertEqual(tmp_dir, temp_directory)
                self.assertTrue(all(os.path.exists(path) for path in file_parts))
                return f'loads/{os.path.basename(file_part)}'

            snowflake.upload_to_s3.side_effect = upload

            s3_keys, s3_pattern = utils.upload_files_to_s3(
                snowflake, file_parts, temp_directory, 'staging-bucket'
            )

            self.assertEqual(
                s3_keys,
                ['loads/export.part0', 'loads/export.part1'],
            )
            self.assertEqual(s3_pattern, 'loads/export')
            self.assertTrue(all(not os.path.exists(path) for path in file_parts))

    def test_upload_files_to_s3_rolls_back_remote_parts_on_local_cleanup_failure(self):
        """A local cleanup failure removes every successfully uploaded remote part."""
        file_parts = ['/tmp/export.part0', '/tmp/export.part1']
        snowflake = mock.MagicMock()
        snowflake.upload_to_s3.side_effect = [
            'loads/export.part0',
            'loads/export.part1',
        ]

        with patch(
            'pipelinewise.fastsync.commons.utils.os.remove',
            side_effect=PermissionError('local cleanup failed'),
        ), self.assertRaisesRegex(PermissionError, 'local cleanup failed'):
            utils.upload_files_to_s3(
                snowflake, file_parts, '/tmp', 'staging-bucket'
            )

        self.assertEqual(snowflake.s3.delete_object.call_args_list, [
            mock.call(Bucket='staging-bucket', Key='loads/export.part0'),
            mock.call(Bucket='staging-bucket', Key='loads/export.part1'),
        ])

    def test_run_post_publication_actions_reports_all_failures(self):
        """Every post-publication action runs and each failure is identified."""
        actions_run = []

        def fail_state():
            actions_run.append('state')
            raise RuntimeError('state failed')

        def fail_grants():
            actions_run.append('grants')
            raise RuntimeError('grant failed')

        with self.assertRaisesRegex(
            RuntimeError,
            'Post-publication actions failed: state persistence: state failed; '
            'grant application: grant failed',
        ):
            utils.run_post_publication_actions([
                ('state persistence', fail_state),
                ('grant application', fail_grants),
            ])

        self.assertEqual(actions_run, ['state', 'grants'])

    def test_apply_snowflake_grants_attempts_usage_and_select(self):
        """A failed usage grant cannot prevent the independent select grant."""
        snowflake = mock.MagicMock()
        snowflake.grant_usage_on_schema.side_effect = RuntimeError(
            'usage grant failed'
        )
        target_config = {
            'default_target_schema_select_permissions': ['reporting_role']
        }

        with self.assertRaisesRegex(
            RuntimeError,
            'schema usage grant: Privilege grants failed: '
            'reporting_role: usage grant failed',
        ):
            utils.apply_snowflake_table_grants(
                snowflake,
                target_config,
                'TARGET_SCHEMA',
                'source.table',
            )

        snowflake.grant_usage_on_schema.assert_called_once_with(
            'TARGET_SCHEMA', 'reporting_role', False
        )
        snowflake.grant_select_on_table.assert_called_once_with(
            'TARGET_SCHEMA',
            'source.table',
            'reporting_role',
            is_temporary=False,
            to_group=False,
        )
        snowflake.grant_select_on_schema.assert_not_called()

    def test_apply_snowflake_grants_attempts_every_role_and_staging_table(self):
        """One invalid role cannot skip later roles or expose the raw live table."""
        snowflake = mock.MagicMock()

        def fail_first_usage(_schema, role, *_args, **_kwargs):
            if role == 'bad_role':
                raise RuntimeError('unknown role')

        def fail_first_select(_schema, _table, role, *_args, **_kwargs):
            if role == 'bad_role':
                raise RuntimeError('unknown role')

        snowflake.grant_usage_on_schema.side_effect = fail_first_usage
        snowflake.grant_select_on_table.side_effect = fail_first_select
        target_config = {
            'default_target_schema_select_permissions': [
                'bad_role',
                'reporting_role',
            ]
        }

        with self.assertRaisesRegex(
            RuntimeError,
            'schema usage grant: .*bad_role.*live table select grant: .*bad_role',
        ):
            utils.apply_snowflake_table_grants(
                snowflake,
                target_config,
                'TARGET_SCHEMA',
                'source.table',
                is_temporary=True,
            )

        self.assertEqual(snowflake.grant_usage_on_schema.call_args_list, [
            mock.call('TARGET_SCHEMA', 'bad_role', False),
            mock.call('TARGET_SCHEMA', 'reporting_role', False),
        ])
        self.assertEqual(snowflake.grant_select_on_table.call_args_list, [
            mock.call(
                'TARGET_SCHEMA',
                'source.table',
                'bad_role',
                is_temporary=True,
                to_group=False,
            ),
            mock.call(
                'TARGET_SCHEMA',
                'source.table',
                'reporting_role',
                is_temporary=True,
                to_group=False,
            ),
        ])

    def test_fullsync_finalizer_attempts_cleanup_and_grants_before_failing(self):
        """Every post-publication prerequisite runs despite sibling failures."""
        snowflake = mock.MagicMock()
        timeline = []

        def fail(name, message):
            def side_effect(*_args, **_kwargs):
                timeline.append(name)
                raise RuntimeError(message)

            return side_effect

        snowflake.s3.delete_object.side_effect = fail('S3 cleanup', 'S3 delete failed')
        snowflake.drop_table.side_effect = fail('table cleanup', 'table drop failed')
        snowflake.grant_usage_on_schema.side_effect = fail(
            'usage grant', 'usage grant failed'
        )
        snowflake.grant_select_on_table.side_effect = (
            lambda *_args, **_kwargs: timeline.append('select grant')
        )
        target_config = {
            'default_target_schema_select_permissions': ['reporting_role']
        }

        with self.assertRaisesRegex(
            RuntimeError,
            'grant application: .*S3 staging cleanup: .*Snowflake staging cleanup:',
        ):
            utils.finalize_snowflake_fullsync(
                snowflake,
                ['staging/part.csv.gz'],
                'staging-bucket',
                target_config,
                'TARGET_SCHEMA',
                'source.table',
            )

        self.assertEqual(snowflake.s3.delete_object.call_count, 3)
        snowflake.drop_table.assert_called_once_with(
            'TARGET_SCHEMA',
            'source.table',
            is_temporary=True,
            max_attempts=3,
        )
        snowflake.grant_usage_on_schema.assert_called_once_with(
            'TARGET_SCHEMA', 'reporting_role', False
        )
        snowflake.grant_select_on_table.assert_called_once_with(
            'TARGET_SCHEMA',
            'source.table',
            'reporting_role',
            is_temporary=False,
            to_group=False,
        )
        snowflake.grant_select_on_schema.assert_not_called()
        self.assertEqual(
            timeline,
            [
                'usage grant',
                'select grant',
                'S3 cleanup',
                'S3 cleanup',
                'S3 cleanup',
                'table cleanup',
            ],
        )

    def test_fullsync_finalizer_preserves_publication_and_cleanup_failures(self):
        """A cleanup failure cannot replace the original ambiguous SWAP error."""
        snowflake = mock.MagicMock()
        snowflake.grant_usage_on_schema.side_effect = RuntimeError(
            'grant failed'
        )
        target_config = {
            'default_target_schema_select_permissions': ['reporting_role']
        }

        with self.assertRaisesRegex(
            RuntimeError,
            'swap failed; post-publication finalization failed: .*grant failed',
        ):
            utils.finalize_snowflake_fullsync(
                snowflake,
                ['staging/part.csv.gz'],
                'staging-bucket',
                target_config,
                'TARGET_SCHEMA',
                'source.table',
                publication_error=RuntimeError('swap failed'),
            )

        snowflake.s3.delete_object.assert_called_once_with(
            Bucket='staging-bucket', Key='staging/part.csv.gz'
        )
        snowflake.drop_table.assert_called_once()

    def test_tablename_to_dict(self):
        """Test identifying schema and table names from fully qualified table names"""

        # Format: <CATALOG>.<SCHEMA>.<TABLE>
        assert utils.tablename_to_dict('my_catalog.my_schema.my_table') == {
            'catalog_name': 'my_catalog',
            'schema_name': 'my_schema',
            'table_name': 'my_table',
            'temp_table_name': 'my_table_temp',
        }

        # Format: <SCHEMA>.<TABLE>
        assert utils.tablename_to_dict('my_schema.my_table') == {
            'catalog_name': None,
            'schema_name': 'my_schema',
            'table_name': 'my_table',
            'temp_table_name': 'my_table_temp',
        }

        # Format: <TABLE>
        assert utils.tablename_to_dict('my_table') == {
            'catalog_name': None,
            'schema_name': None,
            'table_name': 'my_table',
            'temp_table_name': 'my_table_temp',
        }

        # Format: <CATALOG>.<SCHEMA>.<TABLE>.<SOMETHING>
        assert utils.tablename_to_dict('my_catalog.my_schema.my_table.foo') == {
            'catalog_name': 'my_catalog',
            'schema_name': 'my_schema',
            'table_name': 'my_table_foo',
            'temp_table_name': 'my_table_foo_temp',
        }

        # Format: <CATALOG>.<SCHEMA>.<TABLE>.<SOMETHING>
        # Custom separator
        assert utils.tablename_to_dict(
            'my_catalog-my_schema-my_table-foo', separator='-'
        ) == {
            'catalog_name': 'my_catalog',
            'schema_name': 'my_schema',
            'table_name': 'my_table_foo',
            'temp_table_name': 'my_table_foo_temp',
        }

    def test_get_tables_from_properties(self):
        """Test getting selected tables from tap properties JSON"""
        # Load MySQL and Postgres properties JSON
        mysql_properties = utils.load_json(
            '{}/properties_mysql.json'.format(RESOURCES_DIR)
        )
        postgres_properties = utils.load_json(
            '{}/properties_postgres.json'.format(RESOURCES_DIR)
        )

        # Get list of selected tables
        # MySQL and Postgres schemas defined at different keys. get_tables_from_properties function
        # should detect and extract correctly
        mysql_tables = utils.get_tables_from_properties(mysql_properties)
        postgres_tables = utils.get_tables_from_properties(postgres_properties)

        # MySQL schema
        assert mysql_tables == {
            'mysql_source_db.address',
            'mysql_source_db.order',
            'mysql_source_db.weight_unit',
        }

        assert postgres_tables == {'public.city', 'public.country'}

    def test_get_tables_from_properties_for_s3_csv(self):
        properties = utils.load_json('{}/properties_s3_csv.json'.format(RESOURCES_DIR))

        s3_csv_tables = utils.get_tables_from_properties(properties)

        # MySQL schema
        assert s3_csv_tables == {
            'applications',
            'candidate_survey_questions',
            'interviews',
        }

    def test_get_bookmark_for_table_mysql(self):
        """Test bookmark extractors for MySQL taps"""
        # Load MySQL and Postgres properties JSON
        mysql_properties = utils.load_json(
            '{}/properties_mysql.json'.format(RESOURCES_DIR)
        )

        # MySQL: mysql_source_db.order is LOG_BASED
        assert utils.get_bookmark_for_table(
            'mysql_source_db.order', mysql_properties, MySqlMock()
        ) == {'log_file': 'mysqld-bin.000001', 'log_pos': '123456', 'version': 1}

        # MySQL: mysql_source_db.address is INCREMENTAL
        assert utils.get_bookmark_for_table(
            'mysql_source_db.address', mysql_properties, MySqlMock()
        ) == {
            'replication_key': 'date_updated',
            'replication_key_value': 123456,
            'version': 1,
        }

        # MySQL mysql_source_db.foo not exists
        assert (
            utils.get_bookmark_for_table(
                'mysql_source_db.foo', mysql_properties, MySqlMock()
            )
            == {}
        )

    def test_get_bookmark_for_table_postgresl(self):
        """Test bookmark extractors for Postgres taps"""
        # Load Postgres properties JSON
        postgres_properties = utils.load_json(
            '{}/properties_postgres.json'.format(RESOURCES_DIR)
        )

        # Postgres: public.countrylanguage is LOG_BASED
        assert utils.get_bookmark_for_table(
            'public.countrylanguage', postgres_properties, PostgresMock()
        ) == {'lsn': '16/B374D848', 'version': 1}

        # Postgres: postgres_source_db.public.city is INCREMENTAL
        assert utils.get_bookmark_for_table(
            'public.city',
            postgres_properties,
            PostgresMock(),
            dbname='postgres_source_db',
        ) == {'replication_key': 'id', 'replication_key_value': 123456, 'version': 1}

        # Postgres: postgres_source_db.public.foo not exists
        assert (
            utils.get_bookmark_for_table(
                'public.foo',
                postgres_properties,
                PostgresMock(),
                dbname='postgres_source_db',
            )
            == {}
        )

    def test_get_bookmark_for_table_tap_s3_csv(self):
        """Test bookmark extractors for S3 CSV taps"""
        # Load properties JSON
        properties = utils.load_json('{}/properties_s3_csv.json'.format(RESOURCES_DIR))

        # applications is INCREMENTAL
        assert utils.get_bookmark_for_table(
            'applications', properties, S3CsvMock()
        ) == {
            'modified_since': '2019-11-15T07:39:44.171098',
        }

        # candidate_survey_questions is Full table
        assert (
            utils.get_bookmark_for_table(
                'candidate_survey_questions', properties, S3CsvMock()
            )
            == {}
        )

        # foo not exists
        assert utils.get_bookmark_for_table('foo', properties, S3CsvMock()) == {}

    def test_get_target_schema(self):
        """Test target schema extractor from target config"""
        # No default_target_schema and schema_mapping should raise exception
        with pytest.raises(Exception):
            invalid_target_config = {}
            utils.get_target_schema(invalid_target_config, 'foo.foo')

        # Empty default_target_schema should raise exception
        with pytest.raises(Exception):
            target_config_with_default = {'default_target_schema': ''}
            utils.get_target_schema(target_config_with_default, 'foo.foo')

        # Default_target_schema should define the target_schema
        target_config_with_default = {'default_target_schema': 'target_schema'}
        assert (
            utils.get_target_schema(target_config_with_default, 'foo.foo')
            == 'target_schema'
        )

        # Empty schema_mapping should raise exception
        with pytest.raises(Exception):
            target_config_with_empty_schema_mapping = {'schema_mapping': {}}
            utils.get_target_schema(target_config_with_empty_schema_mapping, 'foo.foo')

        # Missing schema in schema_mapping should raise exception
        with pytest.raises(Exception):
            target_config_with_missing_schema_mapping = {
                'schema_mapping': {'foo2': {'target_schema': 'foo2'}}
            }
            utils.get_target_schema(
                target_config_with_missing_schema_mapping, 'foo.foo'
            )

        # Target schema should be extracted from schema_mapping
        target_config_with_schema_mapping = {
            'schema_mapping': {'foo': {'target_schema': 'foo'}}
        }
        assert (
            utils.get_target_schema(target_config_with_schema_mapping, 'foo.foo')
            == 'foo'
        )

        # If target schema exist in schema_mapping then should not use the default_target_schema
        target_config = {
            'default_target_schema': 'target_schema',
            'schema_mapping': {'foo': {'target_schema': 'foo'}},
        }
        assert utils.get_target_schema(target_config, 'foo.foo') == 'foo'

        # If target schema not exist in schema_mapping then should return the default_target_schema
        target_config = {
            'default_target_schema': 'target_schema',
            'schema_mapping': {'foo2': {'target_schema': 'foo2'}},
        }
        assert utils.get_target_schema(target_config, 'foo.foo') == 'target_schema'

    def test_get_grantees(self):
        """Test grantees extractor from target config"""
        # No default_target_schema_select_permissions and schema_mapping should return empty list
        target_config_with_empty_grantees = {}
        assert utils.get_grantees(target_config_with_empty_grantees, 'foo.foo') == []

        # Empty default_target_schema_select_permissions should return empty list
        target_config_with_default_empty = {
            'default_target_schema_select_permissions': ''
        }
        assert utils.get_grantees(target_config_with_default_empty, 'foo.foo') == []

        # default_target_schema_select_permissions as string should return list
        target_config_with_default_as_string = {
            'default_target_schema_select_permissions': 'grantee'
        }
        assert utils.get_grantees(target_config_with_default_as_string, 'foo.foo') == [
            'grantee'
        ]

        # default_target_schema_select_permissions as list should return list
        target_config_with_default_as_list = {
            'default_target_schema_select_permissions': ['grantee1']
        }
        assert utils.get_grantees(target_config_with_default_as_list, 'foo.foo') == [
            'grantee1'
        ]

        # default_target_schema_select_permissions as list should return list
        target_config_with_default_as_list = {
            'default_target_schema_select_permissions': ['grantee1', 'grantee2']
        }
        assert utils.get_grantees(target_config_with_default_as_list, 'foo.foo') == [
            'grantee1',
            'grantee2',
        ]

        # Empty schema_mapping should return empty list
        target_config_with_empty_schema_mapping = {'schema_mapping': {}}
        assert (
            utils.get_grantees(target_config_with_empty_schema_mapping, 'foo.foo') == []
        )

        # Missing schema in schema_mapping should return empty list
        target_config_with_missing_schema_mapping = {
            'schema_mapping': {'foo2': {'target_schema_select_permissions': 'grantee'}}
        }
        assert (
            utils.get_grantees(target_config_with_missing_schema_mapping, 'foo.foo')
            == []
        )

        # Grantees as string should be extracted from schema_mapping
        target_config_with_missing_schema_mapping = {
            'schema_mapping': {'foo': {'target_schema_select_permissions': 'grantee'}}
        }
        assert utils.get_grantees(
            target_config_with_missing_schema_mapping, 'foo.foo'
        ) == ['grantee']

        # Grantees as list should be extracted from schema_mapping
        target_config_with_missing_schema_mapping = {
            'schema_mapping': {
                'foo': {'target_schema_select_permissions': ['grantee1', 'grantee2']}
            }
        }
        assert utils.get_grantees(
            target_config_with_missing_schema_mapping, 'foo.foo'
        ) == ['grantee1', 'grantee2']

        # If grantees exist in schema_mapping then should not use the default_target_schema_select_permissions
        target_config = {
            'default_target_schema_select_permissions': ['grantee1', 'grantee2'],
            'schema_mapping': {
                'foo': {'target_schema_select_permissions': ['grantee3', 'grantee4']}
            },
        }
        assert utils.get_grantees(target_config, 'foo.foo') == ['grantee3', 'grantee4']

        # If target schema not exist in schema_mapping then should return the default_target_schema_select_permissions
        target_config = {
            'default_target_schema_select_permissions': ['grantee1', 'grantee2'],
            'schema_mapping': {
                'foo2': {'target_schema_select_permissions': ['grantee3', 'grantee4']}
            },
        }
        assert utils.get_grantees(target_config, 'foo.foo') == ['grantee1', 'grantee2']

        # default_target_schema_select_permissions as dict with string should return dict
        target_config_with_default_as_dict = {
            'default_target_schema_select_permissions': {
                'users': 'grantee_user1',
                'groups': 'grantee_group1',
            }
        }
        assert utils.get_grantees(target_config_with_default_as_dict, 'foo.foo') == {
            'users': ['grantee_user1'],
            'groups': ['grantee_group1'],
        }

        # default_target_schema_select_permissions as dict with list should return dict
        target_config_with_default_as_dict = {
            'default_target_schema_select_permissions': {
                'users': ['grantee_user1', 'grantee_user2'],
                'groups': ['grantee_group1', 'grantee_group2'],
            }
        }
        assert utils.get_grantees(target_config_with_default_as_dict, 'foo.foo') == {
            'users': ['grantee_user1', 'grantee_user2'],
            'groups': ['grantee_group1', 'grantee_group2'],
        }

    @patch(
        'pipelinewise.fastsync.commons.utils.multiprocessing.cpu_count', return_value=10
    )
    def test_get_cpu_cores_should_succeed(self, _):
        assert utils.get_cpu_cores() == 10

    def test_check_config_with_all_required_keys_present_should_succeed(self):
        config = {'key1': 1, 'key2': 2, 'key3': 3}
        required_keys = {'key1', 'key2', 'key3'}

        utils.check_config(config, required_keys)

    def test_check_config_with_some_required_keys_not_present_should_raise_exception(
        self,
    ):
        config = {'key1': 1, 'key2': 2, 'key3': 3}
        required_keys = {'key1', 'key4'}

        with pytest.raises(Exception):
            utils.check_config(config, required_keys)

    @patch(
        'pipelinewise.fastsync.commons.utils.multiprocessing.cpu_count', return_value=10
    )
    def test_get_pool_size_without_custom_size(self, _):
        """
        Calling get_pool_size without providing fastsync_parallelism return cpu core count
        """
        assert utils.get_pool_size({}) == 10

    @patch(
        'pipelinewise.fastsync.commons.utils.multiprocessing.cpu_count', return_value=10
    )
    def test_get_pool_size_with_custom_size_small(self, _):
        """
        Calling get_pool_size with fastsync_parallelism smaller than cpu core count return the fastsync_parallelism
        """
        assert utils.get_pool_size({'fastsync_parallelism': 2}) == 2

    @patch(
        'pipelinewise.fastsync.commons.utils.multiprocessing.cpu_count', return_value=10
    )
    def test_get_pool_size_with_custom_size_big(self, _):
        """
        Calling get_pool_size with fastsync_parallelism greater than cpu core count return the cpu core count
        """
        assert utils.get_pool_size({'fastsync_parallelism': 20}) == 10

    @mock.patch('pipelinewise.fastsync.commons.utils.get_tables_from_properties')
    @mock.patch('pipelinewise.fastsync.commons.utils.check_config')
    @mock.patch('pipelinewise.fastsync.commons.utils.load_json')
    @mock.patch('argparse.ArgumentParser.parse_args')
    def test_parse_args_without_tables(
        self, mock_args, load_json_mock, check_config_mock, get_tables_prop_mock
    ):
        """
        test args parsing:
            not tables are specified, this should return a tables equal to the list of selected tables
        """
        mock_args.return_value = argparse.Namespace(
            **{
                'tap': './tap.yml',
                'properties': './prop.json',
                'transform': None,
                'target': './target.yml',
                'tables': None,
                'temp_dir': './',
            }
        )

        load_json_mock.return_value = {}
        check_config_mock.return_value = None
        get_tables_prop_mock.return_value = {'schema.table_1', 'schema.table_2'}

        args = utils.parse_args({'tap': [], 'target': []})

        self.assertEqual(get_tables_prop_mock.call_count, 1)
        self.assertEqual(load_json_mock.call_count, 3)
        self.assertEqual(check_config_mock.call_count, 2)

        self.assertDictEqual(
            vars(args),
            {
                'tables': {'schema.table_1', 'schema.table_2'},
                'tap': {},
                'target': {},
                'transform': {},
                'properties': {},
                'temp_dir': './',
            },
        )

    @mock.patch('pipelinewise.fastsync.commons.utils.get_tables_from_properties')
    @mock.patch('pipelinewise.fastsync.commons.utils.check_config')
    @mock.patch('pipelinewise.fastsync.commons.utils.load_json')
    @mock.patch('argparse.ArgumentParser.parse_args')
    def test_parse_args_with_all_tables(
        self, mock_args, load_json_mock, check_config_mock, get_tables_prop_mock
    ):
        """
        test args parsing:
            all selected tables are specified
        """
        mock_args.return_value = argparse.Namespace(
            **{
                'tap': './tap.yml',
                'properties': './prop.json',
                'transform': None,
                'drop_pg_slot': True,
                'target': './target.yml',
                'tables': 'schema.table_1,schema.table_2',
                'temp_dir': './',
            }
        )

        load_json_mock.return_value = {}
        check_config_mock.return_value = None
        get_tables_prop_mock.return_value = {'schema.table_1', 'schema.table_2'}

        args = utils.parse_args({'tap': [], 'target': []})

        self.assertEqual(get_tables_prop_mock.call_count, 1)
        self.assertEqual(load_json_mock.call_count, 3)
        self.assertEqual(check_config_mock.call_count, 2)

        self.assertDictEqual(
            vars(args),
            {
                'tables': {'schema.table_1', 'schema.table_2'},
                'drop_pg_slot': True,
                'tap': {},
                'target': {},
                'transform': {},
                'properties': {},
                'temp_dir': './',
            },
        )

    @mock.patch('pipelinewise.fastsync.commons.utils.get_tables_from_properties')
    @mock.patch('pipelinewise.fastsync.commons.utils.check_config')
    @mock.patch('pipelinewise.fastsync.commons.utils.load_json')
    @mock.patch('argparse.ArgumentParser.parse_args')
    def test_parse_args_with_table_found(
        self, mock_args, load_json_mock, check_config_mock, get_tables_prop_mock
    ):
        """
        test args parsing:
            one table is specified out of 2, this should return a drop_pg_slot = False
        """
        mock_args.return_value = argparse.Namespace(
            **{
                'tap': './tap.yml',
                'properties': './prop.json',
                'transform': None,
                'target': './target.yml',
                'tables': 'schema.table_2',
                'temp_dir': './',
            }
        )

        load_json_mock.return_value = {}
        check_config_mock.return_value = None
        get_tables_prop_mock.return_value = {'schema.table_1', 'schema.table_2'}

        args = utils.parse_args({'tap': [], 'target': []})

        self.assertEqual(get_tables_prop_mock.call_count, 1)
        self.assertEqual(load_json_mock.call_count, 3)
        self.assertEqual(check_config_mock.call_count, 2)

        self.assertDictEqual(
            vars(args),
            {
                'tables': {'schema.table_2'},
                'tap': {},
                'target': {},
                'transform': {},
                'properties': {},
                'temp_dir': './',
            },
        )

    @mock.patch('pipelinewise.fastsync.commons.utils.get_tables_from_properties')
    @mock.patch('pipelinewise.fastsync.commons.utils.check_config')
    @mock.patch('pipelinewise.fastsync.commons.utils.load_json')
    @mock.patch('argparse.ArgumentParser.parse_args')
    def test_parse_args_with_table_not_selected(
        self, mock_args, load_json_mock, check_config_mock, get_tables_prop_mock
    ):
        """
        test args parsing:
            one table not found in selected tables, this should throw a  NotSelectedTableException exception
        """
        mock_args.return_value = argparse.Namespace(
            **{
                'tap': './tap.yml',
                'properties': './prop.json',
                'transform': None,
                'target': './target.yml',
                'tables': 'schema.table_not_selected',
                'temp_dir': './',
            }
        )

        load_json_mock.return_value = {}
        check_config_mock.return_value = None
        get_tables_prop_mock.return_value = {'schema.table_1', 'schema.table_2'}

        with pytest.raises(NotSelectedTableException):
            utils.parse_args({'tap': [], 'target': []})

        self.assertEqual(get_tables_prop_mock.call_count, 1)
        self.assertEqual(check_config_mock.call_count, 0)
        self.assertEqual(load_json_mock.call_count, 3)

    def test_gen_export_filename(self):
        """
        Test unique file name generator function
        """
        # Adding tap id and table name should generate uniqe filenames
        # including timestamps with milliseconds and random generated string
        #
        # Example: pipelinewise_tap_table_20210316-111338-878470_fastsync_L5C6VG9W.csv.gz
        self.assertRegex(
            utils.gen_export_filename('tap', 'table'),
            r'pipelinewise_tap_table_(\d{8})-(\d{6})-(\d{6})_fastsync_(.{8}).csv.gz',
        )

        # Generate filename with custom suffic, postfix and extension
        self.assertEqual(
            utils.gen_export_filename(
                'tap', 'table', suffix='suffix', postfix='postfix', ext='ext'
            ),
            'pipelinewise_tap_table_suffix_fastsync_postfix.ext',
        )
