import os
from pathlib import Path

from pipelinewise.logger import Logger

from tests.units.cli.cli_args import CliArgs


# pylint: disable=no-self-use
class TestLogging:
    """
    Unit Tests for PipelineWise Logging functions
    """

    def setup_method(self):
        """Clear LOGGING_CONF_FILE on every test"""
        if 'LOGGING_CONF_FILE' in os.environ:
            del os.environ['LOGGING_CONF_FILE']

    def test_logging_default(self):
        """Debug option should be disabled by default, LOGGING_CONF_FILE"""
        args = CliArgs()
        Logger(debug=args.debug)
        path = os.path.join(
            Path(__file__).parent, '..', '..', 'pipelinewise', 'logging.conf'
        )
        assert os.environ['LOGGING_CONF_FILE'] == os.path.abspath(path)

    def test_logging_debug(self):
        """Providing debug option should set LOGGING_CONF_FILE env var"""
        args = CliArgs(debug=True)
        Logger(debug=args.debug)
        path = os.path.join(
            Path(__file__).parent, '..', '..', 'pipelinewise', 'logging_debug.conf'
        )
        assert os.environ['LOGGING_CONF_FILE'] == os.path.abspath(path)

    def test_custom_logging_conf(self):
        """Setting custom logging config should keep LOGGING_CONF_FILE"""
        path = os.path.join(Path(__file__).parent, 'logging_custom.conf')
        os.environ['LOGGING_CONF_FILE'] = path

        Logger()
        assert os.environ['LOGGING_CONF_FILE'] == path
