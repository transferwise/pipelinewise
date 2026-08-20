import os
import shutil
import pytest

from pipelinewise import cli
from pipelinewise.cli.config import Config
from pipelinewise.cli.errors import InvalidConfigException

PIPELINEWISE_TEST_HOME = '/tmp/.pipelinewise'


def _table_format_target(**connection_settings):
    return {
        'id': 'test_target',
        'name': 'Test target',
        'type': 'target-snowflake',
        'db_conn': {
            'account': 'account',
            'dbname': 'database',
            'user': 'user',
            'private_key': 'private-key',
            'warehouse': 'warehouse',
            's3_bucket': 'bucket',
            's3_key_prefix': 'prefix/',
            'stage': 'schema.stage',
            'file_format': 'file-format',
            **connection_settings,
        },
    }


# Todo: Inherit from unittest.TestCase
class TestConfig:
    """
    Unit Tests for PipelineWise CLI Config class
    """

    @staticmethod
    def _get_config(json_config_dir, yaml_path):
        # Load a full configuration set from YAML files
        yaml_config_dir = f'{os.path.dirname(__file__)}/resources/{yaml_path}'
        vault_secret = f'{os.path.dirname(__file__)}/resources/vault-secret.txt'
        return Config.from_yamls(json_config_dir, yaml_config_dir, vault_secret)

    @staticmethod
    def _get_json_files_path(tap_id, json_config_dir):
        return {
            'main_config_json': f'{json_config_dir}/config.json',
            'target_config_json': f'{json_config_dir}/test_snowflake_target/config.json',
            'tap_config_json': f'{json_config_dir}/test_snowflake_target/{tap_id}/config.json',
            'tap_inheritable_config_json': f'{json_config_dir}/test_snowflake_target/{tap_id}/inheritable_config.json',
            'tap_selection_json': f'{json_config_dir}/test_snowflake_target/{tap_id}/selection.json',
            'tap_transformation_json': f'{json_config_dir}/test_snowflake_target/{tap_id}/transformation.json',
        }

    def test_constructor(self):
        """Test Config construction functions"""
        config = Config(PIPELINEWISE_TEST_HOME)

        # config dir and path should be generated automatically
        assert config.config_dir == PIPELINEWISE_TEST_HOME
        assert config.config_path == '{}/config.json'.format(PIPELINEWISE_TEST_HOME)
        assert config.targets == {}

    def test_connector_files(self):
        """Every singer connector must have a list of JSON files at certain locations"""
        assert Config.get_connector_files('/var/singer-connector') == {
            'config': '/var/singer-connector/config.json',
            'inheritable_config': '/var/singer-connector/inheritable_config.json',
            'properties': '/var/singer-connector/properties.json',
            'state': '/var/singer-connector/state.json',
            'transformation': '/var/singer-connector/transformation.json',
            'selection': '/var/singer-connector/selection.json',
            'pidfile': '/var/singer-connector/pipelinewise.pid',
        }

    @staticmethod
    def _table_format_tap(**settings):
        return {
            'id': 'test_tap',
            'name': 'Test tap',
            'type': 'tap-postgres',
            'db_conn': {},
            'target': 'test_target',
            'schemas': [],
            **settings,
        }

    @staticmethod
    def _table_format_target(**connection_settings):
        return _table_format_target(**connection_settings)

    @pytest.mark.parametrize(
        'settings',
        [
            {},
            {'target_table_format': 'native'},
            {'target_table_format': 'iceberg', 'iceberg_version': 3},
        ],
    )
    def test_target_table_format_schema_accepts_supported_settings(self, settings):
        """The tap schema accepts omission, native, and explicit Iceberg v3."""
        cli.utils.validate(
            self._table_format_tap(**settings),
            cli.utils.load_schema('tap'),
        )

    @pytest.mark.parametrize(
        'settings',
        [
            {'target_table_format': 'unsupported'},
            {'target_table_format': 'iceberg'},
            {'target_table_format': 'native', 'iceberg_version': 3},
            {'iceberg_version': 3},
            {'target_table_format': 'iceberg', 'iceberg_version': 2},
            {'target_table_format': 'iceberg', 'iceberg_version': 4},
            {'target_table_format': 'iceberg', 'iceberg_version': '3'},
        ],
    )
    def test_target_table_format_schema_rejects_invalid_settings(self, settings):
        """Iceberg version 3 is valid only with an explicit Iceberg format."""
        with pytest.raises(InvalidConfigException):
            cli.utils.validate(
                self._table_format_tap(**settings),
                cli.utils.load_schema('tap'),
            )

    @pytest.mark.parametrize('placement', ['root', 'db_conn'])
    @pytest.mark.parametrize(
        ('setting', 'value'),
        [
            ('iceberg_create', False),
            ('target_table_format', 'iceberg'),
            ('iceberg_version', 3),
        ],
    )
    def test_target_schema_rejects_reserved_table_format_settings(
        self, placement, setting, value
    ):
        """Shared target definitions cannot own per-tap format settings."""
        target = self._table_format_target()
        if placement == 'root':
            target[setting] = value
        else:
            target['db_conn'][setting] = value

        with pytest.raises(InvalidConfigException):
            cli.utils.validate(
                target,
                cli.utils.load_schema('target'),
            )

        # Reserved-key rules must not close the connector-specific db_conn schema.
        cli.utils.validate(
            self._table_format_target(connector_specific_option='value'),
            cli.utils.load_schema('target'),
        )

    @pytest.mark.parametrize('placement', ['root', 'db_conn'])
    @pytest.mark.parametrize(
        ('setting', 'value'),
        [
            ('iceberg_create', True),
            ('iceberg_create', False),
            ('iceberg_create', 'true'),
            ('iceberg_create', None),
            ('target_table_format', 'native'),
            ('target_table_format', 'iceberg'),
            ('iceberg_version', 3),
        ],
    )
    def test_target_table_format_placement_rejects_shared_target_settings(
        self, placement, setting, value
    ):
        """Placement policy reports reserved settings at either target level."""
        target = self._table_format_target()
        if placement == 'root':
            target[setting] = value
        else:
            target['db_conn'][setting] = value

        with pytest.raises(InvalidConfigException, match=setting):
            Config.validate_target_table_format_placement(target)

    @pytest.mark.parametrize(
        'tap_settings',
        [
            {},
            {'target_table_format': 'native'},
            {'target_table_format': 'iceberg', 'iceberg_version': 3},
        ],
    )
    def test_target_table_format_policy_accepts_tap_level_contract(self, tap_settings):
        """Omission, native, and explicit v3 are the complete tap contract."""
        Config.validate_target_table_format(
            self._table_format_tap(**tap_settings),
            self._table_format_target(),
        )

    @pytest.mark.parametrize(
        ('placement', 'setting', 'value'),
        [
            ('root', 'iceberg_create', True),
            ('root', 'iceberg_create', False),
            ('root', 'iceberg_create', 'true'),
            ('root', 'iceberg_create', None),
            ('db_conn', 'iceberg_create', False),
            ('db_conn', 'target_table_format', 'iceberg'),
            ('db_conn', 'iceberg_version', 3),
        ],
    )
    def test_tap_policy_and_schema_reject_reserved_setting_placements(
        self, placement, setting, value
    ):
        """Tap root rejects legacy flags and tap db_conn rejects every format key."""
        tap = self._table_format_tap()
        if placement == 'root':
            tap[setting] = value
        else:
            tap['db_conn'][setting] = value

        with pytest.raises(InvalidConfigException, match=setting):
            Config.validate_tap_table_format_placement(tap)
        with pytest.raises(InvalidConfigException, match=setting):
            Config.validate_target_table_format(tap, self._table_format_target())
        with pytest.raises(InvalidConfigException):
            cli.utils.validate(
                tap,
                cli.utils.load_schema('tap'),
            )

    def test_target_table_format_policy_rejects_non_snowflake_target(self):
        """The tap-level destination format is meaningful only for Snowflake."""
        target = self._table_format_target()
        target['type'] = 'target-postgres'

        with pytest.raises(InvalidConfigException, match='is not target-snowflake'):
            Config.validate_target_table_format(
                self._table_format_tap(target_table_format='native'),
                target,
            )

    @pytest.mark.parametrize(
        ('tap_settings', 'error_message'),
        [
            ({'type': 'tap-salesforce'}, None),
            ({'type': 'tap-mysql', 'hard_delete': False}, 'hard_delete: true'),
            ({'type': 'tap-salesforce', 'hard_delete': False}, 'hard_delete: true'),
            (
                {'type': 'tap-mysql', 'data_flattening_max_level': 1},
                'data_flattening_max_level: 0',
            ),
            (
                {'type': 'tap-postgres', 'data_flattening_max_level': 1},
                'data_flattening_max_level: 0',
            ),
        ],
    )
    def test_explicit_iceberg_route_policy(self, tap_settings, error_message):
        """Singer is source-agnostic; hard delete is universal and FastSync stays unflattened."""
        tap = self._table_format_tap(**{
            'target_table_format': 'iceberg',
            'iceberg_version': 3,
            **tap_settings,
        })

        if error_message:
            with pytest.raises(InvalidConfigException, match=error_message):
                Config.validate_target_table_format(tap, self._table_format_target())
            return

        Config.validate_target_table_format(tap, self._table_format_target())
        inheritable = Config(PIPELINEWISE_TEST_HOME).generate_inheritable_config(tap)
        assert inheritable['target_table_format'] == 'iceberg'
        assert inheritable['iceberg_version'] == 3
        assert inheritable['data_flattening_max_level'] == 10
        assert inheritable['hard_delete'] is True

    def test_target_table_format_is_isolated_between_taps(self, tmp_path):
        """Taps sharing one target get independent generated format settings."""
        config = Config(str(tmp_path))
        omitted_tap = self._table_format_tap(id='omitted_tap')
        native_tap = self._table_format_tap(
            id='native_tap', target_table_format='native'
        )
        iceberg_tap = self._table_format_tap(
            id='iceberg_tap', target_table_format='iceberg', iceberg_version=3
        )
        target = self._table_format_target()
        target['taps'] = [omitted_tap, native_tap, iceberg_tap]
        config.targets = {target['id']: target}

        config.save()

        target_dir = tmp_path / target['id']
        native_config = cli.utils.load_json(
            str(target_dir / native_tap['id'] / 'inheritable_config.json')
        )
        iceberg_config = cli.utils.load_json(
            str(target_dir / iceberg_tap['id'] / 'inheritable_config.json')
        )
        main_config = cli.utils.load_json(str(tmp_path / 'config.json'))
        main_taps = {
            tap['id']: tap for tap in main_config['targets'][0]['taps']
        }
        omitted_config = cli.utils.load_json(
            str(target_dir / omitted_tap['id'] / 'inheritable_config.json')
        )
        target_config = cli.utils.load_json(str(target_dir / 'config.json'))

        assert 'target_table_format' not in omitted_config
        assert 'iceberg_version' not in omitted_config
        assert native_config['target_table_format'] == 'native'
        assert 'iceberg_version' not in native_config
        assert iceberg_config['target_table_format'] == 'iceberg'
        assert iceberg_config['iceberg_version'] == 3
        assert 'target_table_format' not in main_taps['omitted_tap']
        assert 'iceberg_version' not in main_taps['omitted_tap']
        assert main_taps['native_tap']['target_table_format'] == 'native'
        assert 'iceberg_version' not in main_taps['native_tap']
        assert main_taps['iceberg_tap']['target_table_format'] == 'iceberg'
        assert main_taps['iceberg_tap']['iceberg_version'] == 3
        assert not Config.TARGET_FORMAT_KEYS.intersection(target_config)

    @pytest.mark.parametrize(
        ('tap_settings', 'expected_format_settings'),
        [
            (
                {
                    'type': 'tap-mysql',
                    'target_table_format': 'iceberg',
                    'iceberg_version': 3,
                },
                {'target_table_format': 'iceberg', 'iceberg_version': 3},
            ),
            ({'type': 'tap-mysql'}, {}),
            ({'type': 'tap-mysql', 'target_table_format': 'native'}, {}),
            (
                {
                    'type': 'tap-postgres',
                    'target_table_format': 'iceberg',
                    'iceberg_version': 3,
                },
                {},
            ),
        ],
    )
    def test_only_explicit_mysql_iceberg_settings_reach_tap_connector(
        self, tap_settings, expected_format_settings
    ):
        """Only tap-mysql needs destination format metadata during discovery."""
        tap = self._table_format_tap(**tap_settings)
        tap['db_conn'] = {'host': 'mysql.example'}

        generated = Config.generate_tap_connection_config(
            tap, {'server_id': 123}
        )

        assert generated == {
            'host': 'mysql.example',
            'server_id': 123,
            **expected_format_settings,
        }

    def test_from_yamls(self):
        """Test creating Config object using YAML configuration directory as the input"""

        # Create Config object by parsing target and tap YAMLs in a directory
        yaml_config_dir = f'{os.path.dirname(__file__)}/resources/test_yaml_config'

        vault_secret = f'{os.path.dirname(__file__)}/resources/vault-secret.txt'

        # Parse YAML files and create the config object
        config = Config.from_yamls(
            PIPELINEWISE_TEST_HOME, yaml_config_dir, vault_secret
        )

        # config dir and path should be generated automatically
        assert config.config_dir == PIPELINEWISE_TEST_HOME
        assert config.config_path == f'{PIPELINEWISE_TEST_HOME}/config.json'

        # Vault encrypted alert handlers should be loaded into global config
        assert config.global_config == {
            'alert_handlers': {
                'slack': {
                    'token': 'Vault Encrypted Secret Fruit',
                    'channel': '#slack-channel',
                }
            }
        }

        # The target dictionary should contain every target and tap parsed from YAML files
        assert config.targets == {
            'test_snowflake_target': {
                'id': 'test_snowflake_target',
                'name': 'Test Target Connector',
                'type': 'target-snowflake',
                'db_conn': {
                    'account': 'account',
                    'aws_access_key_id': 'access_key_id',
                    'aws_secret_access_key': 'secret_access_key',
                    'client_side_encryption_master_key': 'master_key',
                    'dbname': 'foo_db',
                    'file_format': 'foo_file_format',
                    'private_key': 'private_key_path',
                    's3_bucket': 's3_bucket',
                    's3_key_prefix': 's3_prefix/',
                    'stage': 'foo_stage',
                    'user': 'user',
                    'warehouse': 'MY_WAREHOUSE',
                },
                'files': {
                    'config': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/config.json',
                    'inheritable_config': f'{ PIPELINEWISE_TEST_HOME}/test_snowflake_target/inheritable_config.json',
                    'properties': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/properties.json',
                    'selection': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/selection.json',
                    'state': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/state.json',
                    'transformation': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/transformation.json',
                    'pidfile': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/pipelinewise.pid',
                },
                'taps': [
                    {
                        'id': 'mysql_sample',
                        'name': 'Sample MySQL Database',
                        'type': 'tap-mysql',
                        'owner': 'somebody@foo.com',
                        'target': 'test_snowflake_target',
                        'batch_size_rows': 20000,
                        'batch_wait_limit_seconds': 3600,
                        'split_large_files': True,
                        'split_file_chunk_size_mb': 500,
                        'split_file_max_chunks': 25,
                        'db_conn': {
                            'dbname': '<DB_NAME>',
                            'host': '<HOST>',
                            'password': '<PASSWORD>',
                            'port': 3306,
                            'user': '<USER>',
                        },
                        'files': {
                            'config': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/mysql_sample/config.json',
                            'inheritable_config': f'{PIPELINEWISE_TEST_HOME}'
                                                  f'/test_snowflake_target/mysql_sample/inheritable_config.json',
                            'properties': f'{PIPELINEWISE_TEST_HOME}/'
                                          f'test_snowflake_target/mysql_sample/properties.json',
                            'selection': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/mysql_sample/selection.json',
                            'state': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/mysql_sample/state.json',
                            'transformation': f'{PIPELINEWISE_TEST_HOME}'
                                              f'/test_snowflake_target/mysql_sample/transformation.json',
                            'pidfile': f'{PIPELINEWISE_TEST_HOME}/test_snowflake_target/mysql_sample/pipelinewise.pid',
                        },
                        'schemas': [
                            {
                                'source_schema': 'my_db',
                                'target_schema': 'repl_my_db',
                                'target_schema_select_permissions': ['grp_stats'],
                                'tables': [
                                    {
                                        'table_name': 'table_one',
                                        'replication_method': 'INCREMENTAL',
                                        'replication_key': 'last_update',
                                    },
                                    {
                                        'table_name': 'table_two',
                                        'replication_method': 'LOG_BASED',
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }
        }

    def test_from_invalid_mongodb_yamls(self):
        """Test creating Config object using invalid YAML configuration directory"""

        # Initialising config object with a tap that's referencing an unknown target should exit
        yaml_config_dir = '{}/resources/test_invalid_tap_mongo_yaml_config'.format(
            os.path.dirname(__file__)
        )
        vault_secret = '{}/resources/vault-secret.txt'.format(os.path.dirname(__file__))
        print(yaml_config_dir)
        with pytest.raises(InvalidConfigException):
            Config.from_yamls(PIPELINEWISE_TEST_HOME, yaml_config_dir, vault_secret)

    def test_from_invalid_yamls(self):
        """Test creating Config object using invalid YAML configuration directory"""

        # TODO: Make behaviours consistent.
        #   In some cases it raise exception in some other cases it does exit

        # Initialising Config object with a not existing directory should raise an exception
        with pytest.raises(Exception):
            Config.from_yamls(
                PIPELINEWISE_TEST_HOME, 'not-existing-yaml-config-directory'
            )

        # Initialising config object with a tap that's referencing an unknown target should exit
        yaml_config_dir = '{}/resources/test_invalid_yaml_config'.format(
            os.path.dirname(__file__)
        )
        vault_secret = '{}/resources/vault-secret.txt'.format(os.path.dirname(__file__))

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            Config.from_yamls(PIPELINEWISE_TEST_HOME, yaml_config_dir, vault_secret)
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    def test_from_invalid_yamls_fails(self):
        """
        Test creating Config object using invalid YAML configuration
        directory should fail due to duplicate targets
        """

        # TODO: Make behaviours consistent.
        #   In some cases it raise exception in some other cases it does exit

        # Initialising Config object with a not existing directory should raise an exception
        with pytest.raises(Exception):
            Config.from_yamls(
                PIPELINEWISE_TEST_HOME, 'not-existing-yaml-config-directory'
            )

        # Initialising config object with a tap that's referencing an unknown target should exit
        yaml_config_dir = f'{os.path.dirname(__file__)}/resources/test_invalid_yaml_config_with_duplicate_targets'
        vault_secret = f'{os.path.dirname(__file__)}/resources/vault-secret.txt'

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            Config.from_yamls(PIPELINEWISE_TEST_HOME, yaml_config_dir, vault_secret)
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    def test_getters(self):
        """Test Config getter functions"""
        config = Config(PIPELINEWISE_TEST_HOME)

        # Target and tap directory should be g
        assert config.get_temp_dir() == '{}/tmp'.format(PIPELINEWISE_TEST_HOME)
        assert config.get_target_dir('test-target-id') == '{}/test-target-id'.format(
            PIPELINEWISE_TEST_HOME
        )
        assert config.get_tap_dir(
            'test-target-id', 'test-tap-id'
        ) == '{}/test-target-id/test-tap-id'.format(PIPELINEWISE_TEST_HOME)

        assert config.get_connector_files('/var/singer-connector') == {
            'config': '/var/singer-connector/config.json',
            'inheritable_config': '/var/singer-connector/inheritable_config.json',
            'properties': '/var/singer-connector/properties.json',
            'state': '/var/singer-connector/state.json',
            'transformation': '/var/singer-connector/transformation.json',
            'selection': '/var/singer-connector/selection.json',
            'pidfile': '/var/singer-connector/pipelinewise.pid',
        }

    def test_save_config(self):
        """Test config target and tap JSON save functionalities"""

        json_config_dir = './pipelinewise-test-config'

        config = self._get_config(json_config_dir, yaml_path='test_yaml_config')
        # Save the config as singer compatible JSON files
        config.save()

        json_files = self._get_json_files_path('mysql_sample', json_config_dir)

        # Check content of the generated JSON files
        assert cli.utils.load_json(json_files['main_config_json']) == {
            'alert_handlers': {
                'slack': {
                    'token': 'Vault Encrypted Secret Fruit',
                    'channel': '#slack-channel',
                }
            },
            'targets': [
                {
                    'id': 'test_snowflake_target',
                    'type': 'target-snowflake',
                    'name': 'Test Target Connector',
                    'status': 'ready',
                    'taps': [
                        {
                            'id': 'mysql_sample',
                            'type': 'tap-mysql',
                            'name': 'Sample MySQL Database',
                            'owner': 'somebody@foo.com',
                            'stream_buffer_size': None,
                            'send_alert': True,
                            'enabled': True,
                        }
                    ],
                }
            ],
        }
        assert cli.utils.load_json(json_files['target_config_json']) == {
            'account': 'account',
            'aws_access_key_id': 'access_key_id',
            'aws_secret_access_key': 'secret_access_key',
            'client_side_encryption_master_key': 'master_key',
            'dbname': 'foo_db',
            'file_format': 'foo_file_format',
            'private_key': 'private_key_path',
            's3_bucket': 's3_bucket',
            's3_key_prefix': 's3_prefix/',
            'stage': 'foo_stage',
            'user': 'user',
            'warehouse': 'MY_WAREHOUSE',
        }
        assert cli.utils.load_json(json_files['tap_config_json']) == {
            'dbname': '<DB_NAME>',
            'host': '<HOST>',
            'port': 3306,
            'user': '<USER>',
            'password': '<PASSWORD>',
            'server_id': cli.utils.load_json(json_files['tap_config_json'])['server_id'],
        }
        assert cli.utils.load_json(json_files['tap_selection_json']) == {
            'selection': [
                {
                    'replication_key': 'last_update',
                    'replication_method': 'INCREMENTAL',
                    'tap_stream_id': 'my_db-table_one',
                },
                {'replication_method': 'LOG_BASED', 'tap_stream_id': 'my_db-table_two'},
            ]
        }
        assert cli.utils.load_json(json_files['tap_transformation_json']) == {'transformations': []}
        assert cli.utils.load_json(json_files['tap_inheritable_config_json']) == {
            'batch_size_rows': 20000,
            'batch_wait_limit_seconds': 3600,
            'data_flattening_max_level': 0,
            'flush_all_streams': False,
            'hard_delete': True,
            'parallelism': 0,
            'parallelism_max': 4,
            'primary_key_required': True,
            'schema_mapping': {
                'my_db': {
                    'target_schema': 'repl_my_db',
                    'target_schema_select_permissions': ['grp_stats'],
                }
            },
            'temp_dir': './pipelinewise-test-config/tmp',
            'tap_id': 'mysql_sample',
            'query_tag': '{"ppw_component": "tap-mysql", "tap_id": "mysql_sample", '
            '"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            'validate_records': False,
            'add_metadata_columns': False,
            'split_large_files': True,
            'split_file_chunk_size_mb': 500,
            'split_file_max_chunks': 25,
            'archive_load_files': False,
        }

        # Delete the generated JSON config directory
        shutil.rmtree(json_config_dir)
    def test_save_config_selected_tap(self):
        """Test config target and tap JSON save functionalities if specific taps are selected"""
        json_config_dir = './pipelinewise-test-config'
        config = self._get_config(json_config_dir, yaml_path='test_import_command')
        json_files = self._get_json_files_path('tap_two', json_config_dir)

        # Save the config as singer compatible JSON files
        config.save(['tap_two'])

        # Check content of the generated JSON files
        generated_json_content = cli.utils.load_json(json_files['main_config_json'])

        generated_targets = generated_json_content.get('targets')
        generated_targets_taps = generated_targets[0].pop('taps')

        assert generated_json_content.get('targets') == [
            {
                'id': 'test_snowflake_target',
                'type': 'target-snowflake',
                'name': 'Test Target Connector',
                'status': 'ready'
            }
        ]

        expected_generated_taps = [
            {
                'id': 'tap_one',
                'type': 'tap-mysql',
                'name': 'Sample MySQL Database',
                'owner': 'somebody@foo.com',
                'stream_buffer_size': None,
                'send_alert': True,
                'enabled': True,
            },
            {
                'id': 'tap_two',
                'type': 'tap-mysql',
                'name': 'Sample MySQL Database',
                'owner': 'somebody@foo.com',
                'stream_buffer_size': None,
                'send_alert': True,
                'enabled': True,
            },
            {
                'id': 'tap_three',
                'type': 'tap-mysql',
                'name': 'Sample MySQL Database',
                'owner': 'somebody@foo.com',
                'stream_buffer_size': None,
                'send_alert': True,
                'enabled': True,
            }
        ]
        assert len(generated_targets_taps) == len(expected_generated_taps)
        for tap in expected_generated_taps:
            assert tap in generated_targets_taps

        assert cli.utils.load_json(json_files['target_config_json']) == {
            'account': 'account',
            'aws_access_key_id': 'access_key_id',
            'aws_secret_access_key': 'secret_access_key',
            'client_side_encryption_master_key': 'master_key',
            'dbname': 'foo_db',
            'file_format': 'foo_file_format',
            'password': 'secret',
            's3_bucket': 's3_bucket',
            's3_key_prefix': 's3_prefix/',
            'stage': 'foo_stage',
            'user': 'user',
            'warehouse': 'MY_WAREHOUSE',
        }
        assert cli.utils.load_json(json_files['tap_config_json']) == {
            'dbname': '<DB_NAME>',
            'host': '<HOST>',
            'port': 3306,
            'user': '<USER>',
            'password': '<PASSWORD>',
            'server_id': cli.utils.load_json(json_files['tap_config_json'])['server_id'],
        }
        assert cli.utils.load_json(json_files['tap_selection_json']) == {
            'selection': [
                {
                    'replication_key': 'last_update',
                    'replication_method': 'INCREMENTAL',
                    'tap_stream_id': 'my_db-table_one',
                },
                {'replication_method': 'LOG_BASED', 'tap_stream_id': 'my_db-table_two'},
            ]
        }
        assert cli.utils.load_json(json_files['tap_transformation_json']) == {'transformations': []}
        assert cli.utils.load_json(json_files['tap_inheritable_config_json']) == {
            'batch_size_rows': 20000,
            'batch_wait_limit_seconds': 3600,
            'data_flattening_max_level': 0,
            'flush_all_streams': False,
            'hard_delete': True,
            'parallelism': 0,
            'parallelism_max': 4,
            'primary_key_required': True,
            'schema_mapping': {
                'my_db': {
                    'target_schema': 'repl_my_db',
                    'target_schema_select_permissions': ['grp_stats'],
                }
            },
            'temp_dir': './pipelinewise-test-config/tmp',
            'tap_id': 'tap_two',
            'query_tag': '{"ppw_component": "tap-mysql", "tap_id": "tap_two", '
            '"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            'validate_records': False,
            'add_metadata_columns': False,
            'split_large_files': True,
            'split_file_chunk_size_mb': 500,
            'split_file_max_chunks': 25,
            'archive_load_files': False,
        }

        tap_one_existence = os.path.exists(f'{json_config_dir}/test_snowflake_target/tap_one')

        # Delete the generated JSON config directory
        shutil.rmtree(json_config_dir)

        # Assert only tap_two is created
        assert tap_one_existence is False

    def test_save_config_with_optional_slack_channel_for_alerts(self):
        """Test config target and tap JSON save functionalities if there is a optional setting for slack channel"""

        # Load a full configuration set from YAML files
        yaml_config_dir = '{}/resources/test_yaml_config_with_slack_channel'.format(
            os.path.dirname(__file__)
        )
        vault_secret = '{}/resources/vault-secret.txt'.format(os.path.dirname(__file__))

        json_config_dir = './pipelinewise-test-config'
        config = Config.from_yamls(json_config_dir, yaml_config_dir, vault_secret)

        # Save the config as singer compatible JSON files
        config.save()

        # Check if every required JSON file created, both for target and tap
        main_config_json = '{}/config.json'.format(json_config_dir)

        # Check content of the generated JSON files
        actual_config_json = cli.utils.load_json(main_config_json)

        expected_taps_config = [
            {
                'id': 'mysql_sample_1',
                'type': 'tap-mysql',
                'name': 'Sample MySQL Database',
                'owner': 'somebody@foo.com',
                'stream_buffer_size': None,
                'send_alert': True,
                'enabled': True,
                'slack_alert_channel': '#test-channel_1'
            },
            {
                'id': 'mysql_sample_2',
                'type': 'tap-mysql',
                'name': 'Sample MySQL Database',
                'owner': 'somebody@foo.com',
                'stream_buffer_size': None,
                'send_alert': True,
                'enabled': True,
                'slack_alert_channel': '#test-channel_2'
            },
            {
                'id': 'mysql_sample_3',
                'type': 'tap-mysql',
                'name': 'Sample MySQL Database',
                'owner': 'somebody@foo.com',
                'stream_buffer_size': None,
                'send_alert': True,
                'enabled': True,
            }
        ]

        assert len(actual_config_json['targets'][0]['taps']) == 3

        for tap_config in expected_taps_config:
            assert tap_config in actual_config_json['targets'][0]['taps']

        # Delete the generated JSON config directory
        shutil.rmtree(json_config_dir)


@pytest.mark.parametrize('role', [None, '', True, 42])
def test_snowflake_role_rejects_invalid(role):
    """A configured Snowflake role must be a non-empty string."""
    with pytest.raises(InvalidConfigException):
        cli.utils.validate(
            _table_format_target(role=role),
            cli.utils.load_schema('target'),
        )


def test_snowflake_role_accepts_string():
    """A non-empty Snowflake role remains valid for Singer and FastSync."""
    cli.utils.validate(
        _table_format_target(role='PIPELINEWISE_LOADER'),
        cli.utils.load_schema('target'),
    )
