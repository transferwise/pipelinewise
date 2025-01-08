import json
import os

from unittest import TestCase, mock

from pipelinewise import cli


class TestResetState(TestCase):
    """Testcases for reset state CLI"""
    def setUp(self):
        resources_dir = f'{os.path.dirname(__file__)}/resources'
        self.test_cli = cli
        self.test_cli.CONFIG_DIR = f'{resources_dir}/test_reset_state'
        self.test_cli.VENV_DIR = './virtualenvs-dummy'

    def _run_cli(self, arguments_dict: dict) -> None:
        """Running the test CLI application"""
        argv_list = ['main', 'reset_state']
        if arguments_dict.get('tap'):
            argv_list.extend(['--tap', arguments_dict['tap']])
        if arguments_dict.get('target'):
            argv_list.extend(['--target', arguments_dict['target']])

        with mock.patch('sys.argv', argv_list):
            self.test_cli.main()

    def test_reset_state_file_if_tap_is_pg(self):
        """ Test reset_state command for Postgres taps"""
        state_content = {
                'bookmarks': {
                    'foo_table': {
                        'lsn': 54321
                    },
                    'bar_table': {
                        'foo': 'bar'
                    }
                }
        }
        with open(f'{self.test_cli.CONFIG_DIR}/target_foo/tap_pg/state.json', 'w', encoding='utf-8') as state_file:
            json.dump(state_content, state_file)

        arguments = {
            'tap': 'tap_pg',
            'target': 'target_foo'
        }
        self._run_cli(arguments)

        with open(f'{self.test_cli.CONFIG_DIR}/target_foo/tap_pg/state.json', 'r', encoding='utf-8') as state_file:
            actual_state = json.load(state_file)

        expected_state = {
                'bookmarks': {
                    'foo_table': {
                        'lsn': 1
                    },
                    'bar_table': {
                        'foo': 'bar'
                    }
                }
        }
        self.assertDictEqual(expected_state, actual_state)

    def test_exit_with_error_1_if_tap_is_not_allowed(self):
        """ Test reset_state command exit with error 1 if tap is not allowed for it"""
        arguments = {
            'tap': 'tap_bar',
            'target': 'target_foo'
        }

        with self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)
