import os
import shutil

import pipelinewise.cli as cli
import pytest

PIPELINEWISE_TEST_HOME = '/tmp/.pipelinewise'


# pylint: disable=no-self-use,fixme
class TestConfig:
    """
    Unit Tests for PipelineWise CLI Config class
    """

    def test_constructor(self):
        """Test Config construction functions"""
        config = cli.config.Config(PIPELINEWISE_TEST_HOME)

        # config dir and path should be generated automatically
        assert config.config_dir == PIPELINEWISE_TEST_HOME
        assert config.config_path == '{}/config.json'.format(PIPELINEWISE_TEST_HOME)
        assert config.targets == {}

    def test_from_yamls(self):
        """Test creating Config object using YAML configuration directory as the input"""

        # Create Config object by parsing target and tap YAMLs in a directory
        yaml_config_dir = '{}/resources/test_yaml_config'.format(os.path.dirname(__file__))
        vault_secret = '{}/resources/vault-secret.txt'.format(os.path.dirname(__file__))

        # Parse YAML files and create the config object
        config = cli.config.Config.from_yamls(PIPELINEWISE_TEST_HOME, yaml_config_dir, vault_secret)

        # config dir and path should be generated automatically
        assert config.config_dir == PIPELINEWISE_TEST_HOME
        assert config.config_path == '{}/config.json'.format(PIPELINEWISE_TEST_HOME)

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
                    'password': 'secret',
                    's3_bucket': 's3_bucket',
                    's3_key_prefix': 's3_prefix/',
                    'stage': 'foo_stage',
                    'user': 'user',
                    'warehouse': 'MY_WAREHOUSE'
                },
                'files': {
                    'config': '{}/test_snowflake_target/config.json'.format(PIPELINEWISE_TEST_HOME),
                    'inheritable_config':
                        '{}/test_snowflake_target/inheritable_config.json'.format(PIPELINEWISE_TEST_HOME),
                    'properties': '{}/test_snowflake_target/properties.json'.format(PIPELINEWISE_TEST_HOME),
                    'selection': '{}/test_snowflake_target/selection.json'.format(PIPELINEWISE_TEST_HOME),
                    'state': '{}/test_snowflake_target/state.json'.format(PIPELINEWISE_TEST_HOME),
                    'transformation': '{}/test_snowflake_target/transformation.json'.format(PIPELINEWISE_TEST_HOME)
                },
                'taps': [{
                    'id': 'mysql_sample',
                    'name': 'Sample MySQL Database',
                    'type': 'tap-mysql',
                    'owner': 'somebody@foo.com',
                    'target': 'test_snowflake_target',
                    'batch_size_rows': 20000,
                    'db_conn': {
                        'dbname': '<DB_NAME>',
                        'host': '<HOST>',
                        'password': '<PASSWORD>',
                        'port': 3306,
                        'user': '<USER>'
                    },
                    'files': {
                        'config':
                            '{}/test_snowflake_target/mysql_sample/config.json'.format(PIPELINEWISE_TEST_HOME),
                        'inheritable_config':
                            '{}/test_snowflake_target/mysql_sample/inheritable_config.json'.format(
                                PIPELINEWISE_TEST_HOME),
                        'properties':
                            '{}/test_snowflake_target/mysql_sample/properties.json'.format(PIPELINEWISE_TEST_HOME),
                        'selection':
                            '{}/test_snowflake_target/mysql_sample/selection.json'.format(PIPELINEWISE_TEST_HOME),
                        'state':
                            '{}/test_snowflake_target/mysql_sample/state.json'.format(PIPELINEWISE_TEST_HOME),
                        'transformation':
                            '{}/test_snowflake_target/mysql_sample/transformation.json'.format(PIPELINEWISE_TEST_HOME)
                    },
                    'schemas': [{
                        'source_schema': 'my_db',
                        'target_schema': 'repl_my_db',
                        'target_schema_select_permissions': ['grp_stats'],
                        'tables': [{
                            'table_name': 'table_one',
                            'replication_method': 'INCREMENTAL',
                            'replication_key': 'last_update'
                        }, {
                            'table_name': 'table_two',
                            'replication_method': 'LOG_BASED'
                        }]
                    }]
                }]
            }
        }

    def test_from_invalid_mongodb_yamls(self):
        """Test creating Config object using invalid YAML configuration directory"""

        # Initialising config object with a tap that's referencing an unknown target should exit
        yaml_config_dir = '{}/resources/test_invalid_tap_mongo_yaml_config'.format(os.path.dirname(__file__))
        vault_secret = '{}/resources/vault-secret.txt'.format(os.path.dirname(__file__))
        print(yaml_config_dir)
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cli.config.Config.from_yamls(PIPELINEWISE_TEST_HOME, yaml_config_dir, vault_secret)

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    def test_from_invalid_yamls(self):
        """Test creating Config object using invalid YAML configuration directory"""

        # TODO: Make behaviours consistent.
        #   In some cases it raise exception in some other cases it does exit

        # Initialising Config object with a not existing directory should raise an exception
        with pytest.raises(Exception):
            cli.config.Config.from_yamls(PIPELINEWISE_TEST_HOME, 'not-existing-yaml-config-directory')

        # Initialising config object with a tap that's referencing an unknown target should exit
        yaml_config_dir = '{}/resources/test_invalid_yaml_config'.format(os.path.dirname(__file__))
        vault_secret = '{}/resources/vault-secret.txt'.format(os.path.dirname(__file__))

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cli.config.Config.from_yamls(PIPELINEWISE_TEST_HOME, yaml_config_dir, vault_secret)
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
            cli.config.Config.from_yamls(PIPELINEWISE_TEST_HOME, 'not-existing-yaml-config-directory')

        # Initialising config object with a tap that's referencing an unknown target should exit
        yaml_config_dir = f'{os.path.dirname(__file__)}/resources/test_invalid_yaml_config_with_duplicate_targets'
        vault_secret = f'{os.path.dirname(__file__)}/resources/vault-secret.txt'

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cli.config.Config.from_yamls(PIPELINEWISE_TEST_HOME, yaml_config_dir, vault_secret)
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    def test_getters(self):
        """Test Config getter functions"""
        config = cli.config.Config(PIPELINEWISE_TEST_HOME)

        # Target and tap directory should be g
        assert config.get_temp_dir() == '{}/tmp'.format(PIPELINEWISE_TEST_HOME)
        assert config.get_target_dir('test-target-id') == '{}/test-target-id'.format(PIPELINEWISE_TEST_HOME)
        assert config.get_tap_dir('test-target-id',
                                  'test-tap-id') == '{}/test-target-id/test-tap-id'.format(PIPELINEWISE_TEST_HOME)

        # TODO: get_connector_files is duplicated in config.py and pipelinewise.py
        #       Refactor to use only one
        assert \
            config.get_connector_files('/var/singer-connector') == \
            {
                'config': '/var/singer-connector/config.json',
                'inheritable_config': '/var/singer-connector/inheritable_config.json',
                'properties': '/var/singer-connector/properties.json',
                'state': '/var/singer-connector/state.json',
                'transformation': '/var/singer-connector/transformation.json',
                'selection': '/var/singer-connector/selection.json'
            }

    def test_save_config(self):
        """Test config target and tap JSON save functionalities"""

        # Load a full configuration set from YAML files
        yaml_config_dir = '{}/resources/test_yaml_config'.format(os.path.dirname(__file__))
        vault_secret = '{}/resources/vault-secret.txt'.format(os.path.dirname(__file__))

        json_config_dir = './pipelinewise-test-config'
        config = cli.config.Config.from_yamls(json_config_dir, yaml_config_dir, vault_secret)

        # Save the config as singer compatible JSON files
        config.save()

        # Check if every required JSON file created, both for target and tap
        main_config_json = '{}/config.json'.format(json_config_dir)
        target_config_json = '{}/test_snowflake_target/config.json'.format(json_config_dir)
        tap_config_json = '{}/test_snowflake_target/mysql_sample/config.json'.format(json_config_dir)
        tap_inheritable_config_json = '{}/test_snowflake_target/mysql_sample/inheritable_config.json'.format(
            json_config_dir)
        tap_selection_json = '{}/test_snowflake_target/mysql_sample/selection.json'.format(json_config_dir)
        tap_transformation_json = '{}/test_snowflake_target/mysql_sample/transformation.json'.format(json_config_dir)

        # Check content of the generated JSON files
        assert cli.utils.load_json(main_config_json) == {
            'targets':
                [{
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
                            'enabled': True,
                        }
                    ]
                }]
        }
        assert cli.utils.load_json(target_config_json) == {
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
            'warehouse': 'MY_WAREHOUSE'
        }
        assert cli.utils.load_json(tap_config_json) == {
            'dbname': '<DB_NAME>',
            'host': '<HOST>',
            'port': 3306,
            'user': '<USER>',
            'password': '<PASSWORD>',
            'server_id': cli.utils.load_json(tap_config_json)['server_id']
        }
        assert cli.utils.load_json(tap_selection_json) == {
            'selection': [
                {
                    'replication_key': 'last_update',
                    'replication_method': 'INCREMENTAL',
                    'tap_stream_id': 'my_db-table_one'
                },
                {
                    'replication_method': 'LOG_BASED',
                    'tap_stream_id': 'my_db-table_two'
                }
            ]
        }
        assert cli.utils.load_json(tap_transformation_json) == {
            'transformations': []
        }
        assert cli.utils.load_json(tap_inheritable_config_json) == {
            'batch_size_rows': 20000,
            'data_flattening_max_level': 0,
            'flush_all_streams': False,
            'hard_delete': True,
            'parallelism': 0,
            'parallelism_max': 4,
            'primary_key_required': True,
            'schema_mapping': {
                'my_db': {
                    'target_schema': 'repl_my_db',
                    'target_schema_select_permissions': ['grp_stats']
                }
            },
            'temp_dir': './pipelinewise-test-config/tmp',
            'validate_records': False,
            'add_metadata_columns': False
        }

        # Delete the generated JSON config directory
        shutil.rmtree(json_config_dir)
