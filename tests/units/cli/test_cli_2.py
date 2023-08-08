import os
import shutil

from tempfile import NamedTemporaryFile
from unittest.mock import patch, call

from pipelinewise.cli import PipelineWise
from .cli_args import CliArgs

RESOURCES_DIR = '{}/resources'.format(os.path.dirname(__file__))
CONFIG_DIR = '{}/sample_json_config'.format(RESOURCES_DIR)
VIRTUALENVS_DIR = './virtualenvs-dummy'
TEST_PROJECT_NAME = 'test-project'
TEST_PROJECT_DIR = '{}/{}'.format(os.getcwd(), TEST_PROJECT_NAME)
PROFILING_DIR = './profiling'


# pylint: disable=no-self-use,attribute-defined-outside-init,fixme
class TestCli2:
    """
    Continuation of pipelinewise unit tests
    """
    def setup_method(self):
        """
        Setup method
        """
        self.args = CliArgs(log='coverage.log')
        self.pipelinewise = PipelineWise(
            self.args, CONFIG_DIR, VIRTUALENVS_DIR, PROFILING_DIR
        )
        if os.path.exists('/tmp/pwtest'):
            shutil.rmtree('/tmp/pwtest')

    def teardown_method(self):
        """
         Tearing down any files/objects
        """
        try:
            shutil.rmtree(TEST_PROJECT_DIR)
            shutil.rmtree(os.path.join(CONFIG_DIR, 'target_one/tap_one/log'))
        except Exception:
            pass

    def test_cleanup_after_deleted_config(self):
        """Test that cleanup of config of deleted taps and target takes place"""
        old_config = {
            'targets': [
                {
                    'id': 'target_one',
                    'type': 'target-snowflake',
                    'taps': [dict(id='tap_one', type='tap-mysql'), dict(id='tap_two', type='tap-postgres')]
                },
                {
                    'id': 'target_two',
                    'type': 'target-s3-csv',
                    'taps': [dict(id='tap_three', type='tap-mysql'), dict(id='tap_four', type='tap-kafka')]
                },
                {
                    'id': 'target_three',
                    'type': 'target-snowflake',
                    'taps': [dict(id='tap_five', type='tap-s3-csv')]
                }
            ]
        }

        with patch('pipelinewise.cli.pipelinewise.utils.silentremove') as silentremove:
            with patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot:
                deleted_taps_count = self.pipelinewise.cleanup_after_deleted_config(old_config)

        assert deleted_taps_count == 2
        assert silentremove.call_args_list == [
            call(f'{CONFIG_DIR}/target_two/tap_four'),
            call(f'{CONFIG_DIR}/target_three/tap_five'),
            call(f'{CONFIG_DIR}/target_three'),
        ]

        # not called because none of the deleted taps are tap-postgres
        drop_slot.assert_not_called()

    def test_cleanup_after_deleted_config_of_tap_postgres(self):
        """Test that cleanup of config and slot of deleted postgres tap takes place"""
        old_config = {
            'targets': [
                {
                    'id': 'target_one',
                    'type': 'target-snowflake',
                    'taps': [dict(id='tap_one', type='tap-mysql'), dict(id='tap_two', type='tap-postgres')]
                },
                {
                    'id': 'target_two',
                    'type': 'target-s3-csv',
                    'taps': [dict(id='tap_three', type='tap-mysql'), dict(id='tap_four', type='tap-postgres')]
                }
            ]
        }

        with patch('pipelinewise.cli.pipelinewise.utils.silentremove') as silentremove:
            with patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot:
                with patch('pipelinewise.cli.pipelinewise.Config.get_connector_config_file') as \
                        get_connector_config_file:
                    with NamedTemporaryFile(suffix='.json') as fhandler:
                        fhandler.write(b'{"host": "localhost"}')
                        fhandler.seek(0)
                        get_connector_config_file.return_value = fhandler.name

                        deleted_taps_count = self.pipelinewise.cleanup_after_deleted_config(old_config)

        assert deleted_taps_count == 1
        assert silentremove.call_args_list == [ call(f'{CONFIG_DIR}/target_two/tap_four')]

        # called because the deleted tap is a tap-postgres
        drop_slot.assert_called_once()
