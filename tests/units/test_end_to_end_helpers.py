import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from tests.end_to_end.helpers import assertions
from tests.end_to_end.helpers import env as env_module
from tests.end_to_end.helpers.env import E2EEnv
from tests.end_to_end import target_snowflake as target_snowflake_module
from tests.end_to_end.target_snowflake import TargetSnowflake
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB
from tests.end_to_end.target_snowflake.tap_mariadb.test_partial_sync_mariadb_to_sf import (
    MARIADB_FASTSYNC_COMPARISON_COLUMNS,
)
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres
from tests.end_to_end.target_snowflake.tap_postgres.test_partial_sync_pg_to_sf import (
    POSTGRES_FASTSYNC_COMPARISON_COLUMNS,
)


class EndToEndHelpersTestCase(TestCase):  # pylint: disable=too-many-public-methods
    """Tests for E2E control-flow helpers that do not require external services."""

    def setUp(self):
        self.env = mock.MagicMock()
        self.comparison_columns = [
            {
                'name': 'cid',
                'source_expression': '"cid"',
                'target_expression': '"CID"',
                'normalizer': 'integer',
            },
            {
                'name': 'payload',
                'source_expression': '"payload"',
                'target_expression': '"PAYLOAD"',
                'normalizer': 'json',
            },
            {
                'name': 'secret',
                'source_expression': '"secret"',
                'target_expression': '"SECRET"',
                'normalizer': 'text',
                'source_normalizer': 'hash_skip_first_2',
            },
            {
                'name': 'active',
                'source_expression': '"active"',
                'target_expression': '"ACTIVE"',
                'normalizer': 'boolean',
            },
        ]
        self.tap_parameters = {
            'env': self.env,
            'tap': 'postgres_to_sf',
            'tap_type': 'postgres',
            'target': 'snowflake',
            'source_db': 'public',
            'table': 'edgydata',
            'column': 'cid',
            'comparison_columns': self.comparison_columns,
        }

    @staticmethod
    def _write_state(temp_directory, state):
        state_path = Path(
            temp_directory, '.pipelinewise', 'snowflake', 'postgres_to_sf', 'state.json'
        )
        state_path.parent.mkdir(parents=True, exist_ok=True)
        with state_path.open('w', encoding='utf-8') as state_file:
            json.dump(state, state_file)
        return state_path

    def test_state_validation_accepts_structured_fastsync_bookmarks(self):
        """FastSync state without a Singer log must contain usable bookmarks."""
        with TemporaryDirectory() as temp_directory:
            self._write_state(
                temp_directory,
                {
                    'currently_syncing': None,
                    'bookmarks': {
                        'public-table': {'lsn': '16/B374D848', 'version': 1}
                    },
                },
            )
            with mock.patch.object(assertions.Path, 'home', return_value=Path(temp_directory)):
                assertions.assert_state_file_valid(
                    'snowflake',
                    'postgres_to_sf',
                    expected_streams={'public-table'},
                )

    def test_state_validation_rejects_malformed_fastsync_bookmarks(self):
        """A state file's existence alone cannot make malformed bookmarks pass."""
        malformed_states = (
            {},
            {'bookmarks': []},
            {'bookmarks': {'public-table': 'not-an-object'}},
            {'bookmarks': {'': {}}},
        )
        for state in malformed_states:
            with self.subTest(state=state), TemporaryDirectory() as temp_directory:
                self._write_state(temp_directory, state)
                with mock.patch.object(
                    assertions.Path, 'home', return_value=Path(temp_directory)
                ), self.assertRaises(AssertionError):
                    assertions.assert_state_file_valid('snowflake', 'postgres_to_sf')

    def test_state_validation_accepts_full_table_bookmark_without_progress(self):
        """FULL_TABLE state is structurally valid even though its bookmark is empty."""
        with TemporaryDirectory() as temp_directory:
            self._write_state(
                temp_directory,
                {'bookmarks': {'public-full-table': {}}},
            )
            with mock.patch.object(
                assertions.Path, 'home', return_value=Path(temp_directory)
            ):
                assertions.assert_state_file_valid(
                    'snowflake',
                    'postgres_to_sf',
                    expected_streams={'public-full-table'},
                )

    def test_state_validation_requires_progress_only_for_declared_streams(self):
        """An empty FULL_TABLE bookmark cannot satisfy a LOG/INCREMENTAL stream."""
        with TemporaryDirectory() as temp_directory:
            self._write_state(
                temp_directory,
                {'bookmarks': {'public-table': {}}},
            )
            with mock.patch.object(
                assertions.Path, 'home', return_value=Path(temp_directory)
            ), self.assertRaises(AssertionError):
                assertions.assert_state_file_valid(
                    'snowflake',
                    'postgres_to_sf',
                    expected_streams={'public-table'},
                    expected_progress_streams={'public-table'},
                )

    def test_state_validation_rejects_a_wrong_fastsync_stream(self):
        """A valid bookmark for another stream cannot satisfy the expected table."""
        with TemporaryDirectory() as temp_directory:
            self._write_state(
                temp_directory,
                {
                    'bookmarks': {
                        'public-other': {'lsn': '16/B374D848', 'version': 1}
                    }
                },
            )
            with mock.patch.object(
                assertions.Path, 'home', return_value=Path(temp_directory)
            ), self.assertRaises(AssertionError):
                assertions.assert_state_file_valid(
                    'snowflake',
                    'postgres_to_sf',
                    expected_streams={'public-table'},
                )

    def test_state_validation_compares_parsed_singer_state(self):
        """Singer state comparison detects semantic mismatch independent of formatting."""
        persisted_state = {'currently_syncing': None, 'bookmarks': {'public-table': {}}}
        emitted_state = {'currently_syncing': None, 'bookmarks': {'public-other': {}}}
        with TemporaryDirectory() as temp_directory:
            self._write_state(temp_directory, persisted_state)
            log_path = Path(temp_directory, 'run.log')
            with Path(f'{log_path}.success').open('w', encoding='utf-8') as log_file:
                log_file.write(
                    f'INFO STATE emitted from target: {json.dumps(persisted_state)}\n'
                    f'INFO STATE emitted from target: {json.dumps(emitted_state)}\n'
                )

            with mock.patch.object(
                assertions.Path, 'home', return_value=Path(temp_directory)
            ), self.assertRaises(AssertionError):
                assertions.assert_state_file_valid(
                    'snowflake',
                    'postgres_to_sf',
                    str(log_path),
                    require_emitted_state=True,
                )

    def test_state_validation_parses_target_snowflake_raw_json(self):
        """The real target-snowflake state output is a bare JSON line."""
        persisted_state = {
            'currently_syncing': None,
            'bookmarks': {'public-table': {'lsn': '16/B374D848'}},
        }
        with TemporaryDirectory() as temp_directory:
            self._write_state(temp_directory, persisted_state)
            log_path = Path(temp_directory, 'run.log')
            Path(f'{log_path}.success').write_text(
                f'INFO Emitting state {json.dumps(persisted_state)}\n'
                f'{json.dumps(persisted_state)}\n',
                encoding='utf-8',
            )

            with mock.patch.object(
                assertions.Path, 'home', return_value=Path(temp_directory)
            ):
                assertions.assert_state_file_valid(
                    'snowflake',
                    'postgres_to_sf',
                    str(log_path),
                    require_emitted_state=True,
                )

    def test_singer_state_validation_rejects_a_missing_emission(self):
        """Pre-existing FastSync state cannot mask a Singer state regression."""
        with TemporaryDirectory() as temp_directory:
            self._write_state(
                temp_directory,
                {'bookmarks': {'public-table': {'lsn': '16/B374D848'}}},
            )
            log_path = Path(temp_directory, 'run.log')
            Path(f'{log_path}.success').write_text(
                'time=2026-08-07 log_level=INFO message=No state emitted\n',
                encoding='utf-8',
            )

            with mock.patch.object(
                assertions.Path, 'home', return_value=Path(temp_directory)
            ), self.assertRaises(AssertionError):
                assertions.assert_state_file_valid(
                    'snowflake',
                    'postgres_to_sf',
                    str(log_path),
                    require_emitted_state=True,
                )

    def test_fastsync_state_marker_requires_the_expected_stream(self):
        """A marker for a sibling table cannot mask a missing FastSync state write."""
        with TemporaryDirectory() as temp_directory:
            log_path = Path(temp_directory, 'run.log')
            with Path(f'{log_path}.success').open('w', encoding='utf-8') as log_file:
                log_file.write(
                    'INFO FastSync state updated for stream: public-other\n'
                )

            with self.assertRaises(AssertionError):
                assertions.assert_fastsync_state_persisted(
                    str(log_path), {'public-table'}
                )

    def test_fastsync_state_marker_accepts_the_expected_stream(self):
        """The harness accepts a marker emitted after the requested table write."""
        with TemporaryDirectory() as temp_directory:
            log_path = Path(temp_directory, 'run.log')
            with Path(f'{log_path}.success').open('w', encoding='utf-8') as log_file:
                log_file.write(
                    'time=2026-08-07 18:00:00 '
                    'logger_name=pipelinewise.fastsync.commons.utils '
                    'log_level=INFO message=FastSync state updated for stream: '
                    'public-table\n'
                )

            assertions.assert_fastsync_state_persisted(
                str(log_path), {'public-table'}
            )

    def test_fastsync_state_marker_rejects_an_unexpected_sibling(self):
        """Exact markers catch a worker that persisted an unintended stream set."""
        with TemporaryDirectory() as temp_directory:
            log_path = Path(temp_directory, 'run.log')
            Path(f'{log_path}.success').write_text(
                'INFO FastSync state updated for stream: public-table\n'
                'INFO FastSync state updated for stream: public-sibling\n',
                encoding='utf-8',
            )

            with self.assertRaises(AssertionError):
                assertions.assert_fastsync_state_persisted(
                    str(log_path), {'public-table'}
                )

    def test_fastsync_state_marker_rejects_a_duplicate_execution(self):
        """Repeated completion markers cannot masquerade as one FastSync run."""
        with TemporaryDirectory() as temp_directory:
            log_path = Path(temp_directory, 'run.log')
            Path(f'{log_path}.success').write_text(
                'INFO FastSync state updated for stream: public-table\n'
                'INFO FastSync state updated for stream: public-table\n',
                encoding='utf-8',
            )

            with self.assertRaises(AssertionError):
                assertions.assert_fastsync_state_persisted(
                    str(log_path), {'public-table'}
                )

    def test_bounded_partial_sync_requires_state_to_remain_unchanged(self):
        """A bounded repair must not advance the shared Singer bookmark."""
        original_state = {
            'bookmarks': {'public-table': {'lsn': '16/B374D848'}}
        }
        with TemporaryDirectory() as temp_directory:
            self._write_state(temp_directory, original_state)
            with mock.patch.object(
                assertions.Path, 'home', return_value=Path(temp_directory)
            ):
                assertions._assert_partial_sync_state(  # pylint: disable=protected-access
                    self.tap_parameters,
                    end_value=10,
                    state_before=original_state,
                    log_path='unused',
                )

                self._write_state(
                    temp_directory,
                    {'bookmarks': {'public-table': {'lsn': 'changed'}}},
                )
                with self.assertRaises(AssertionError):
                    assertions._assert_partial_sync_state(  # pylint: disable=protected-access
                        self.tap_parameters,
                        end_value=10,
                        state_before=original_state,
                        log_path='unused',
                    )

    @mock.patch('tests.end_to_end.helpers.assertions.assert_resync_tables_success')
    def test_assert_resync_populates_target_compares_the_complete_fixture(self, resync_mock):
        """Every configured expression is compared after explicit normalization."""
        hashed_secret = 'se' + hashlib.sha256(b'cret').hexdigest()
        self.env.get_rows_from_source.return_value = [
            (1, {'b': [2], 'a': 1}, 'secret', 1),
        ]
        self.env.get_rows_from_target_snowflake.return_value = [
            (1, '{"a":1,"b":[2]}', hashed_secret, True),
        ]

        records = assertions.assert_resync_populates_target(
            self.tap_parameters, primary_key='cid'
        )

        self.assertEqual(records, [(1, '{"a":1,"b":[2]}', hashed_secret, True)])
        resync_mock.assert_called_once_with(
            'postgres_to_sf',
            'snowflake',
            expected_streams={'public-edgydata'},
            tables='public.edgydata',
        )
        self.env.get_rows_from_source.assert_called_once_with(
            tap_type='postgres',
            source_db='public',
            table='edgydata',
            columns=['"cid"', '"payload"', '"secret"', '"active"'],
            primary_key='cid',
        )
        self.env.get_rows_from_target_snowflake.assert_called_once_with(
            tap_type='postgres',
            table='edgydata',
            columns=['"CID"', '"PAYLOAD"', '"SECRET"', '"ACTIVE"'],
            primary_key='cid',
        )

    @mock.patch('tests.end_to_end.helpers.assertions.assert_resync_tables_success')
    def test_assert_resync_populates_target_rejects_a_partial_load(self, resync_mock):
        """A non-empty but incomplete target is a failure and is not retried."""
        hashed_secret = 'se' + hashlib.sha256(b'cret').hexdigest()
        self.env.get_rows_from_source.return_value = [
            (1, {'a': 1}, 'secret', 1),
            (2, {'a': 2}, 'secret', 1),
        ]
        self.env.get_rows_from_target_snowflake.return_value = [
            (1, '{"a":1}', hashed_secret, True),
        ]

        with self.assertRaisesRegex(
            AssertionError,
            'first mismatch at ordered row 2; expected .* got .<missing>.; '
            'expected 2 rows, got 1',
        ):
            assertions.assert_resync_populates_target(
                self.tap_parameters, primary_key='cid'
            )

        resync_mock.assert_called_once_with(
            'postgres_to_sf',
            'snowflake',
            expected_streams={'public-edgydata'},
            tables='public.edgydata',
        )

    @mock.patch('tests.end_to_end.helpers.assertions.assert_resync_tables_success')
    def test_assert_resync_populates_target_rejects_non_key_corruption(self, resync_mock):
        """A corrupt value outside the old key/sample pair must fail setup."""
        hashed_secret = 'se' + hashlib.sha256(b'cret').hexdigest()
        self.env.get_rows_from_source.return_value = [
            (1, {'nested': {'value': 1}}, 'secret', 1),
        ]
        self.env.get_rows_from_target_snowflake.return_value = [
            (1, '{"nested":{"value":999}}', hashed_secret, True),
        ]

        with self.assertRaisesRegex(AssertionError, 'FastSync did not reproduce public.edgydata'):
            assertions.assert_resync_populates_target(
                self.tap_parameters, primary_key='cid'
            )

        resync_mock.assert_called_once_with(
            'postgres_to_sf',
            'snowflake',
            expected_streams={'public-edgydata'},
            tables='public.edgydata',
        )

    @mock.patch('tests.end_to_end.helpers.assertions.assert_resync_tables_success')
    def test_assert_resync_populates_target_rejects_an_empty_source_fixture(self, resync_mock):
        """An accidentally empty source and target must not make setup pass."""
        self.env.get_rows_from_source.return_value = []
        self.env.get_rows_from_target_snowflake.return_value = []

        with self.assertRaisesRegex(AssertionError, 'Source fixture public.edgydata is empty'):
            assertions.assert_resync_populates_target(
                self.tap_parameters, primary_key='cid'
            )

        resync_mock.assert_called_once_with(
            'postgres_to_sf',
            'snowflake',
            expected_streams={'public-edgydata'},
            tables='public.edgydata',
        )

    @mock.patch('tests.end_to_end.helpers.assertions.assert_state_file_valid')
    @mock.patch('tests.end_to_end.helpers.assertions.assert_fastsync_state_persisted')
    @mock.patch('tests.end_to_end.helpers.assertions.assert_command_success')
    @mock.patch(
        'tests.end_to_end.helpers.assertions.tasks.find_run_tap_log_file',
        return_value='/tmp/run.fastsync.log',
    )
    @mock.patch(
        'tests.end_to_end.helpers.assertions.tasks.run_command',
        return_value=[
            0,
            'Writing output into /tmp/run.fastsync.log\n',
            '',
        ],
    )
    def test_resync_command_scopes_fastsync_to_the_requested_table(
        self,
        run_command_mock,
        _find_log_mock,
        _success_mock,
        _state_marker_mock,
        _state_file_mock,
    ):
        """Fixture setup must not resync unrelated tables or expect their markers."""
        assertions.assert_resync_tables_success(
            'postgres_to_sf',
            'snowflake',
            expected_streams={'public-edgydata'},
            tables='public.edgydata',
        )

        run_command_mock.assert_called_once_with(
            'pipelinewise fast_sync --tap postgres_to_sf --target snowflake '
            '--tables public.edgydata'
        )

    @mock.patch('builtins.print')
    def test_run_tap_failure_prints_started_engine_log_before_engine_assertion(
        self,
        print_mock,
    ):
        """A failed FastSync must expose its log even when Singer never starts."""
        with TemporaryDirectory() as temp_directory:
            log_path = Path(temp_directory, 'run.fastsync.log')
            Path(f'{log_path}.failed').write_text(
                'primary FastSync failure',
                encoding='utf-8',
            )
            stdout = f'Writing output into {log_path}\n'
            with mock.patch.object(
                assertions.tasks,
                'run_command',
                return_value=[1, stdout, ''],
            ), self.assertRaises(AssertionError):
                assertions.assert_run_tap_success(
                    'postgres_to_sf',
                    'snowflake',
                    ('fastsync', 'singer'),
                )

        printed_output = print_mock.call_args.args[0]
        self.assertIn('primary FastSync failure', printed_output)
        self.assertIn(f'{log_path}.failed', printed_output)

    def test_partial_sync_fixtures_compare_every_deterministic_column(self):
        """Keep full-row validation in both FastSync setup fixtures."""
        self.assertEqual(
            [column['name'] for column in MARIADB_FASTSYNC_COMPARISON_COLUMNS],
            [
                'weight_unit_id',
                'weight_unit_name',
                'isActive',
                'original_date_created',
                'date_created',
                'date_updated',
            ],
        )
        self.assertEqual(
            [column['name'] for column in POSTGRES_FASTSYNC_COMPARISON_COLUMNS],
            [
                'cid',
                'ctimentz',
                'ctimetz',
                'cjson',
                'cjsonb',
                'cvarchar',
                'date',
            ],
        )

    def test_get_rows_from_source_uses_the_requested_route_and_order(self):
        """Source verification must query the configured table and primary-key order."""
        env = mock.Mock(spec=E2EEnv)
        env.run_query_tap_postgres.return_value = [(1, '12:00'), (2, '13:00')]

        records = E2EEnv.get_rows_from_source(
            env,
            tap_type='postgres',
            source_db='public',
            table='edgydata',
            columns=['cid', 'ctimentz'],
            primary_key='cid',
        )

        self.assertEqual(records, [(1, '12:00'), (2, '13:00')])
        env.run_query_tap_postgres.assert_called_once_with(
            'SELECT cid, ctimentz FROM public.edgydata ORDER BY cid'
        )

    def test_get_rows_from_source_accepts_canonical_tap_route_identifiers(self):
        """Snowflake E2E base classes pass TAP_* identifiers to source helpers."""
        env = mock.Mock(spec=E2EEnv)

        for tap_type, method_name in (
            ('TAP_MYSQL', 'run_query_tap_mysql'),
            ('TAP_POSTGRES', 'run_query_tap_postgres'),
        ):
            with self.subTest(tap_type=tap_type):
                query_method = getattr(env, method_name)
                query_method.return_value = [(1,)]

                records = E2EEnv.get_rows_from_source(
                    env,
                    tap_type=tap_type,
                    source_db='source_db',
                    table='source_table',
                    columns=['id'],
                    primary_key='id',
                )

                self.assertEqual(records, [(1,)])
                query_method.assert_called_once_with(
                    'SELECT id FROM source_db.source_table ORDER BY id'
                )

    def test_get_rows_from_target_uses_the_unique_schema_and_order(self):
        """Target verification must use this run's schema and primary-key order."""
        env = mock.Mock(spec=E2EEnv)
        env.sf_schema_postfix = '_unique'
        env.run_query_target_snowflake.return_value = [(1, '12:00')]

        records = E2EEnv.get_rows_from_target_snowflake(
            env,
            tap_type='postgres',
            table='edgydata',
            columns=['cid', 'ctimentz'],
            primary_key='cid',
        )

        self.assertEqual(records, [(1, '12:00')])
        env.run_query_target_snowflake.assert_called_once_with(
            'SELECT cid, ctimentz FROM ppw_e2e_tap_postgres_unique.edgydata '
            'ORDER BY "CID"'
        )

    def test_get_rows_from_target_accepts_canonical_tap_route_identifier(self):
        """Canonical TAP_* names must not duplicate the schema's tap_ prefix."""
        env = mock.Mock(spec=E2EEnv)
        env.sf_schema_postfix = '_unique'
        env.run_query_target_snowflake.return_value = [(1,)]

        records = E2EEnv.get_rows_from_target_snowflake(
            env,
            tap_type='TAP_MYSQL',
            table='weight_unit',
            columns=['weight_unit_id'],
            primary_key='weight_unit_id',
        )

        self.assertEqual(records, [(1,)])
        env.run_query_target_snowflake.assert_called_once_with(
            'SELECT weight_unit_id FROM ppw_e2e_tap_mysql_unique.weight_unit '
            'ORDER BY "WEIGHT_UNIT_ID"'
        )

    def test_environment_uses_configured_snowflake_schema_postfix_everywhere(self):
        """A fixed override must replace the generated query/cleanup postfix."""
        env = object.__new__(E2EEnv)
        env.sf_schema_postfix = '_generated'

        with mock.patch.object(env_module, 'load_dotenv'), mock.patch.object(
            E2EEnv, '_is_env_connector_configured', return_value=True
        ), mock.patch.dict(
            env_module.os.environ,
            {'TARGET_SNOWFLAKE_SCHEMA_POSTFIX': '_configured'},
            clear=True,
        ):
            env._load_env()  # pylint: disable=protected-access

        self.assertEqual(env.sf_schema_postfix, '_configured')
        self.assertTrue(env.sf_schema_postfix_is_override)
        self.assertEqual(
            env.get_conn_env_var('TARGET_SNOWFLAKE', 'SCHEMA_POSTFIX'),
            '_configured',
        )

    def test_environment_keeps_generated_postfix_without_an_override(self):
        """The generated postfix is the single value when no override exists."""
        env = object.__new__(E2EEnv)
        env.sf_schema_postfix = '_generated'

        with mock.patch.object(env_module, 'load_dotenv'), mock.patch.object(
            E2EEnv, '_is_env_connector_configured', return_value=True
        ), mock.patch.dict(env_module.os.environ, {}, clear=True):
            env._load_env()  # pylint: disable=protected-access

        self.assertEqual(env.sf_schema_postfix, '_generated')
        self.assertFalse(env.sf_schema_postfix_is_override)
        self.assertEqual(
            env.get_conn_env_var('TARGET_SNOWFLAKE', 'SCHEMA_POSTFIX'),
            '_generated',
        )

    def test_partial_sync_command_includes_each_boundary_once(self):
        """Render both boundaries exactly once, including a zero start value."""
        command = assertions._get_command_for_partial_sync(  # pylint: disable=protected-access
            self.tap_parameters,
            start_value=0,
            end_value=7,
        )

        self.assertEqual(
            command,
            'pipelinewise partial_sync_table --tap postgres_to_sf --target snowflake '
            '--table public.edgydata --column cid --start_value 0 --end_value 7',
        )

    def test_partial_sync_command_omits_an_unspecified_end_boundary(self):
        """Do not render a synthetic end boundary when none was requested."""
        command = assertions._get_command_for_partial_sync(  # pylint: disable=protected-access
            self.tap_parameters,
            start_value=3,
        )

        self.assertEqual(
            command,
            'pipelinewise partial_sync_table --tap postgres_to_sf --target snowflake '
            '--table public.edgydata --column cid --start_value 3',
        )

    @mock.patch('tests.end_to_end.target_snowflake.shutil.rmtree')
    def test_remove_generated_config_removes_the_exact_directory(self, rmtree_mock):
        """Generated-target cleanup must remove only its resolved directory."""
        target = TargetSnowflake(methodName='runTest')

        target.remove_dir_from_config_dir('snowflake/postgres_to_sf')

        rmtree_mock.assert_called_once_with(
            os.path.join(
                target_snowflake_module.CONFIG_DIR,
                'snowflake/postgres_to_sf',
            )
        )

    def test_iceberg_cleanup_rejects_a_stale_target_pointer(self):
        """A completed E2E route cannot leave its target attempt pointer behind."""
        target = TargetSnowflake(methodName='runTest')
        target.target_id = 'snowflake'
        target.tap_id = 'postgres_to_sf'
        target.e2e_env = self.env
        target.iceberg_fastsync_s3_keys = mock.Mock(return_value=[])
        self.env.run_query_target_snowflake.return_value = []

        with TemporaryDirectory() as temp_directory, mock.patch.object(
            target_snowflake_module,
            'CONFIG_DIR',
            temp_directory,
        ):
            runtime_dir = Path(temp_directory, target.target_id)
            runtime_dir.mkdir(parents=True)
            Path(runtime_dir, 'iceberg-fastsync-target-stale.json').write_text(
                '{}',
                encoding='utf-8',
            )

            with self.assertRaises(AssertionError):
                target.assert_iceberg_fastsync_cleanup('TARGET_SCHEMA')

    @mock.patch(
        'tests.end_to_end.target_snowflake.shutil.rmtree',
        side_effect=FileNotFoundError,
    )
    def test_remove_generated_config_tolerates_a_missing_directory(self, rmtree_mock):
        """An already absent generated-target directory is clean state."""
        target = TargetSnowflake(methodName='runTest')

        target.remove_dir_from_config_dir('snowflake')

        rmtree_mock.assert_called_once()

    @mock.patch(
        'tests.end_to_end.target_snowflake.shutil.rmtree',
        side_effect=PermissionError('cleanup failed'),
    )
    def test_remove_generated_config_propagates_other_failures(self, _rmtree_mock):
        """Permission and filesystem failures must fail target setup."""
        target = TargetSnowflake(methodName='runTest')

        with self.assertRaisesRegex(PermissionError, 'cleanup failed'):
            target.remove_dir_from_config_dir('snowflake')

    @mock.patch('tests.end_to_end.helpers.env.shutil.rmtree')
    def test_environment_config_cleanup_removes_the_exact_directory(self, rmtree_mock):
        """Environment cleanup must resolve the requested generated target."""
        E2EEnv.remove_dir_from_config_dir('postgres_dwh')

        rmtree_mock.assert_called_once_with(
            os.path.join(env_module.CONFIG_DIR, 'postgres_dwh')
        )

    @mock.patch(
        'tests.end_to_end.helpers.env.shutil.rmtree',
        side_effect=FileNotFoundError,
    )
    def test_environment_config_cleanup_tolerates_absence(self, rmtree_mock):
        """Environment cleanup tolerates only an already absent directory."""
        E2EEnv.remove_dir_from_config_dir('snowflake')

        rmtree_mock.assert_called_once()

    @mock.patch(
        'tests.end_to_end.helpers.env.shutil.rmtree',
        side_effect=PermissionError('cleanup failed'),
    )
    def test_environment_config_cleanup_propagates_failures(self, _rmtree_mock):
        """Environment cleanup must surface permission failures."""
        with self.assertRaisesRegex(PermissionError, 'cleanup failed'):
            E2EEnv.remove_dir_from_config_dir('snowflake')

    def test_unconfigured_template_cleanup_propagates_permission_failures(self):
        """Stale rendered YAML cannot silently survive a failed deletion."""
        env = object.__new__(E2EEnv)

        with TemporaryDirectory() as temp_directory:
            template_path = os.path.join(temp_directory, 'optional.yml.template')
            with open(template_path, 'w', encoding='utf-8') as template_file:
                template_file.write('key: value')

            with mock.patch.object(
                    env, '_find_env_conn_by_template_name', return_value=['OPTIONAL']
            ), mock.patch.object(
                    env, '_is_env_connector_configured', return_value=False
            ), mock.patch.object(env_module.glob, 'glob', return_value=[template_path]), \
                    mock.patch.object(
                        env_module.os,
                        'remove',
                        side_effect=PermissionError('cleanup failed'),
                    ):
                with self.assertRaisesRegex(PermissionError, 'cleanup failed'):
                    env._init_test_project_dir(temp_directory)  # pylint: disable=protected-access

    def test_snowflake_setup_propagates_config_cleanup_failure(self):
        """Target setup must stop immediately when generated config cannot be cleaned."""
        target = TargetSnowflake(methodName='runTest')
        self.env.env = {'TAP_MYSQL': {'is_configured': True}}
        target.get_e2e_env = mock.Mock(return_value=self.env)
        target.remove_dir_from_config_dir = mock.Mock(
            side_effect=PermissionError('cleanup failed')
        )
        credentials_check = mock.Mock()
        setattr(target, 'check_snowflake_credentials_provided', credentials_check)
        target.check_validate_taps = mock.Mock()
        target.check_import_config = mock.Mock()

        with self.assertRaisesRegex(PermissionError, 'cleanup failed'):
            target.setUp('mariadb_to_sf', 'snowflake', 'TAP_MYSQL')

        credentials_check.assert_not_called()
        target.check_validate_taps.assert_not_called()
        target.check_import_config.assert_not_called()

    def test_snowflake_setup_resets_config_before_validate_and_import(self):
        """Register exact cleanup without deleting another run during setup."""
        target = TargetSnowflake(methodName='runTest')
        events = []
        self.env.env = {
            'TAP_MYSQL': {'is_configured': True},
            'TARGET_SNOWFLAKE': {'is_configured': True},
        }
        self.env.sf_schema_postfix = '_unique'
        self.env.sf_schema_postfix_is_override = False
        self.env.run_query_target_snowflake.side_effect = (
            lambda *args: events.append(('snowflake_query', args)) or []
        )
        target.get_e2e_env = mock.Mock(
            side_effect=lambda: events.append(('get_env', ())) or self.env
        )
        target.remove_dir_from_config_dir = mock.Mock(
            side_effect=lambda *args: events.append(('remove_config', args))
        )
        setattr(
            target,
            'check_snowflake_credentials_provided',
            mock.Mock(side_effect=lambda: events.append(('check_credentials', ()))),
        )
        target.check_validate_taps = mock.Mock(
            side_effect=lambda: events.append(('validate', ()))
        )
        target.check_import_config = mock.Mock(
            side_effect=lambda: events.append(('import', ()))
        )
        target.drop_sf_schema_if_exists = mock.Mock(
            side_effect=lambda *args: events.append(('drop_schema', args))
        )

        target.setUp('mariadb_to_sf', 'snowflake', 'TAP_MYSQL')

        self.assertEqual(
            events,
            [
                ('get_env', ()),
                ('remove_config', ('snowflake',)),
                ('check_credentials', ()),
                ('validate', ()),
                ('import', ()),
            ],
        )
        self.assertEqual(target.tap_type, 'TAP_MYSQL')

        target.doCleanups()

        self.assertEqual(
            events[5:],
            [
                ('drop_schema', ('PPW_E2E_TAP_MYSQL_2_UNIQUE',)),
                ('drop_schema', ('PPW_E2E_TAP_MYSQL_PUBLIC2_UNIQUE',)),
                ('drop_schema', ('PPW_E2E_TAP_MYSQL_UNIQUE',)),
                ('remove_config', ('snowflake/mariadb_to_sf',)),
            ],
        )

    def test_mariadb_setup_resets_source_before_validate_and_import(self):
        """Discovery must inspect the freshly reset MariaDB fixture."""
        target = TapMariaDB(methodName='runTest')
        events = []
        self.env.env = {
            'TAP_MYSQL': {'is_configured': True},
            'TARGET_SNOWFLAKE': {'is_configured': True},
        }
        self.env.sf_schema_postfix = '_unique'
        self.env.sf_schema_postfix_is_override = False
        self.env.setup_tap_mysql.side_effect = lambda: events.append(('reset_mysql', ()))
        target.get_e2e_env = mock.Mock(
            side_effect=lambda: events.append(('get_env', ())) or self.env
        )
        target.remove_dir_from_config_dir = mock.Mock(
            side_effect=lambda *args: events.append(('remove_config', args))
        )
        setattr(
            target,
            'check_snowflake_credentials_provided',
            mock.Mock(side_effect=lambda: events.append(('check_credentials', ()))),
        )
        target.check_validate_taps = mock.Mock(
            side_effect=lambda: events.append(('validate', ()))
        )
        target.check_import_config = mock.Mock(
            side_effect=lambda: events.append(('import', ()))
        )
        target.drop_sf_schema_if_exists = mock.Mock()

        target.setUp('mariadb_to_sf', 'snowflake')

        self.assertEqual(
            events,
            [
                ('get_env', ()),
                ('remove_config', ('snowflake',)),
                ('check_credentials', ()),
                ('reset_mysql', ()),
                ('validate', ()),
                ('import', ()),
            ],
        )
        self.env.setup_tap_mysql.assert_called_once_with()
        target.doCleanups()

    def test_postgres_setup_resets_source_before_validate_and_import(self):
        """Discovery must inspect the freshly reset PostgreSQL fixture."""
        target = TapPostgres(methodName='runTest')
        events = []
        self.env.env = {
            'TAP_POSTGRES': {'is_configured': True},
            'TARGET_SNOWFLAKE': {'is_configured': True},
        }
        self.env.sf_schema_postfix = '_unique'
        self.env.sf_schema_postfix_is_override = False
        self.env.clean_up_temp_dir.side_effect = lambda: events.append(('clean_temp', ()))
        self.env.setup_tap_postgres.side_effect = lambda: events.append(
            ('reset_postgres', ())
        )
        target.get_e2e_env = mock.Mock(
            side_effect=lambda: events.append(('get_env', ())) or self.env
        )
        target.remove_dir_from_config_dir = mock.Mock(
            side_effect=lambda *args: events.append(('remove_config', args))
        )
        setattr(
            target,
            'check_snowflake_credentials_provided',
            mock.Mock(side_effect=lambda: events.append(('check_credentials', ()))),
        )
        target.check_validate_taps = mock.Mock(
            side_effect=lambda: events.append(('validate', ()))
        )
        target.check_import_config = mock.Mock(
            side_effect=lambda: events.append(('import', ()))
        )
        target.drop_sf_schema_if_exists = mock.Mock()

        target.setUp('postgres_to_sf', 'snowflake')

        self.assertEqual(
            events,
            [
                ('get_env', ()),
                ('remove_config', ('snowflake',)),
                ('check_credentials', ()),
                ('clean_temp', ()),
                ('reset_postgres', ()),
                ('validate', ()),
                ('import', ()),
            ],
        )
        self.env.clean_up_temp_dir.assert_called_once_with()
        self.env.setup_tap_postgres.assert_called_once_with()
        target.doCleanups()

    def test_source_reset_failure_keeps_registered_cleanup(self):
        """A source-reset failure must stop discovery and retain base cleanup."""
        target = TapPostgres(methodName='runTest')
        self.env.env = {
            'TAP_POSTGRES': {'is_configured': True},
            'TARGET_SNOWFLAKE': {'is_configured': True},
        }
        self.env.sf_schema_postfix = '_current'
        self.env.sf_schema_postfix_is_override = False
        self.env.setup_tap_postgres.side_effect = RuntimeError('source reset failed')
        target.get_e2e_env = mock.Mock(return_value=self.env)
        target.remove_dir_from_config_dir = mock.Mock()
        setattr(target, 'check_snowflake_credentials_provided', mock.Mock())
        target.check_validate_taps = mock.Mock()
        target.check_import_config = mock.Mock()
        target.drop_sf_schema_if_exists = mock.Mock()

        with self.assertRaisesRegex(RuntimeError, 'source reset failed'):
            target.setUp('postgres_to_sf', 'snowflake')

        target.check_validate_taps.assert_not_called()
        target.check_import_config.assert_not_called()
        target.doCleanups()

        self.assertEqual(
            target.drop_sf_schema_if_exists.call_args_list,
            [
                mock.call('PPW_E2E_TAP_POSTGRES_2_CURRENT'),
                mock.call('PPW_E2E_TAP_POSTGRES_PUBLIC2_CURRENT'),
                mock.call('PPW_E2E_TAP_POSTGRES_CURRENT'),
            ],
        )
        self.assertEqual(
            target.remove_dir_from_config_dir.call_args_list,
            [
                mock.call('snowflake'),
                mock.call('snowflake/postgres_to_sf'),
            ],
        )
