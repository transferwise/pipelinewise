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
        assert (
            self.pipelinewise.get_target_dir("dummy-target"),
            "{}/dummy-target".format(CONFIG_DIR))


    def test_tap_dir(self):
        """Singer tap connector config path must be relative to the target connector config path"""
        assert (
            self.pipelinewise.get_tap_dir("dummy-target", "dummy-tap"),
            "{}/dummy-target/dummy-tap".format(CONFIG_DIR))


    def test_tap_log_dir(self):
        """Singer tap log path must be relative to the tap connector config path"""
        assert(
            self.pipelinewise.get_tap_log_dir("dummy-target", "dummy-tap"),
            "{}/dummy-target/dummy-tap/log".format(CONFIG_DIR))


    def test_connector_bin(self):
        """Singer connector binary must be at a certain location under PIPELINEWISE_HOME .virtualenvs dir"""
        assert(
            self.pipelinewise.get_connector_bin("dummy-type"),
            "{}/dummy-type/bin/dummy-type".format(VIRTUALENVS_DIR))


    def test_connector_files(self):
        """Every singer connector must have a list of JSON files at certain locations"""
        assert(
            self.pipelinewise.get_connector_files("/var/singer-connector"),
            {
                'config': '/var/singer-connector/config.json',
                'inheritable_config': '/var/singer-connector/inheritable_config.json',
                'properties': '/var/singer-connector/properties.json',
                'state': '/var/singer-connector/state.json',
                'transformation': '/var/singer-connector/transformation.json',
                'selection': '/var/singer-connector/selection.json'
            })
