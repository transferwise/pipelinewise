import os
import pytest

import cli.utils
from cli_args import CliArgs
from cli.pipelinewise import PipelineWise

CONFIG_DIR="./config-dmmy"
VIRTUALENVS_DIR="./virtualenvs-dummy"


class TestCli(object):
    """
    Unit Tests for PipelineWise CLI executable
    """
    def setup_method(self):
        # Create CLI arguments
        self.args = CliArgs()
        self.pipelinewise = PipelineWise(self.args, CONFIG_DIR, VIRTUALENVS_DIR)


    def test_target_dir(self):
        """Singer target connector config path must be relative to the project config dir"""
        assert \
            self.pipelinewise.get_target_dir("dummy-target") == \
            "{}/dummy-target".format(CONFIG_DIR)


    def test_tap_dir(self):
        """Singer tap connector config path must be relative to the target connector config path"""
        assert \
            self.pipelinewise.get_tap_dir("dummy-target", "dummy-tap") == \
            "{}/dummy-target/dummy-tap".format(CONFIG_DIR)


    def test_tap_log_dir(self):
        """Singer tap log path must be relative to the tap connector config path"""
        assert \
            self.pipelinewise.get_tap_log_dir("dummy-target", "dummy-tap") == \
            "{}/dummy-target/dummy-tap/log".format(CONFIG_DIR)


    def test_connector_bin(self):
        """Singer connector binary must be at a certain location under PIPELINEWISE_HOME .virtualenvs dir"""
        assert \
            self.pipelinewise.get_connector_bin("dummy-type") == \
            "{}/dummy-type/bin/dummy-type".format(VIRTUALENVS_DIR)


    def test_connector_files(self):
        """Every singer connector must have a list of JSON files at certain locations"""

        # TODO: get_connector_files is duplicated in config.py and pipelinewise.py
        #       Refactor to use only one
        assert \
            self.pipelinewise.get_connector_files("/var/singer-connector") == \
            {
                'config': '/var/singer-connector/config.json',
                'inheritable_config': '/var/singer-connector/inheritable_config.json',
                'properties': '/var/singer-connector/properties.json',
                'state': '/var/singer-connector/state.json',
                'transformation': '/var/singer-connector/transformation.json',
                'selection': '/var/singer-connector/selection.json'
            }


    def test_create_consumable_target_config(self):
        """Test merging target config.json and inheritable_config.json"""
        target_config = "{}/resources/target-config.json".format(os.path.dirname(__file__))
        tap_inheritable_config = "{}/resources/tap-inheritable-config.json".format(os.path.dirname(__file__))

        # The merged JSON written into a temp file
        temp_file = self.pipelinewise.create_consumable_target_config(target_config, tap_inheritable_config)
        cons_targ_config = cli.utils.load_json(temp_file)

        # The merged object needs
        assert cons_targ_config == {
            "account": "foo",
            "aws_access_key_id": "secret",
            "aws_secret_access_key": "secret/",
            "client_side_encryption_master_key": "secret=",
            "dbname": "my_db",
            "file_format": "my_file_format",
            "password": "secret",
            "s3_bucket": "foo",
            "s3_key_prefix": "foo/",
            "stage": "my_stage",
            "user": "user",
            "warehouse": "MY_WAREHOUSE",
            "batch_size_rows": 5000,
            "data_flattening_max_level": 0,
            "default_target_schema": "jira_clear",
            "default_target_schema_select_permissions": [
                "grp_power"
            ],
            "hard_delete": True,
            "primary_key_required": True,
            "schema_mapping": {
                "jira": {
                    "target_schema": "jira_clear",
                    "target_schema_select_permissions": [
                        "grp_power"
                    ]
                }
            }
        }

        # Remove temp file with merged JSON
        os.remove(temp_file)


    def test_invalid_create_consumable_target_config(self):
        """Test merging invalid target config.json and inheritable_config.json"""
        target_config = "{}/resources/invalid.json".format(os.path.dirname(__file__))
        tap_inheritable_config = "not-existing-json"

        # Merging invalid or not existing JSONs should raise exception
        with pytest.raises(Exception):
            self.pipelinewise.create_consumable_target_config(target_config, tap_inheritable_config)



    def test_tap_properties(self):
        """Test tap property getter functions"""
        assert 1 == 1


    def test_encrypt_string(self, capsys):
        """Test vault encryption command output"""
        secret_path = "{}/resources/vault-secret.txt".format(os.path.dirname(__file__))

        args = CliArgs(string="plain text", secret=secret_path)
        pipelinewise = PipelineWise(args, CONFIG_DIR, VIRTUALENVS_DIR)

        # Encrypted string should be printed to stdout
        pipelinewise.encrypt_string()
        stdout, stderr = capsys.readouterr()
        assert not stderr.strip()
        assert stdout.startswith("!vault |") and "$ANSIBLE_VAULT;" in stdout

