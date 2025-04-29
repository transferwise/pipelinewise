import json
import os
import random
import string

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

    def test_reset_state_file_if_tap_is_mysql(self):
        """ Test reset_state command for MySQL taps"""
        old_pos = random.randint(1, 100)
        new_pos = random.randint(1, 100)
        old_bin = ''.join(random.choice(string.ascii_letters) for _ in range(10))
        new_bin = ''.join(random.choice(string.ascii_letters) for _ in range(10))
        state_content = {
            'bookmarks': {
                "foo_table": {
                    "log_file": "mysql-bin.000001",
                    "log_pos": old_pos,
                    "version": 1
                },
                'bar_table': {
                    'foo': 'bar'
                }
            }
        }

        switchover_data = {
            'new_database_url': {
                'old_identifier': '',
                'new_identifier': '',
                'old_host': 'foo_database',
                'new_host': 'new_database_url',
                'old_binlog_filename': old_bin,
                'old_binlog_position': old_pos,
                'new_binlog_filename': new_bin,
                'new_binlog_position': new_pos,
                'switchover_utc_timestamp': '2025-04-04T15:15:45+00:00',
                'engine': 'mariadb'
            }
        }

        with open(f'{self.test_cli.CONFIG_DIR}/target_foo/tap_mysql/state.json', 'w', encoding='utf-8') as state_file:
            json.dump(state_content, state_file)

        with open(f'/tmp/switch_over_test.json', 'w', encoding='utf-8') as switchover_file:
            json.dump(switchover_data, switchover_file)

        arguments = {
            'tap': 'tap_mysql',
            'target': 'target_foo'
        }
        self._run_cli(arguments)

        with open(f'{self.test_cli.CONFIG_DIR}/target_foo/tap_mysql/state.json', 'r', encoding='utf-8') as state_file:
            actual_state = json.load(state_file)

        expected_state = {
            'bookmarks': {
                'foo_table': {
                    "log_file": new_bin,
                    "log_pos": new_pos,
                    "version": 1
                },
                'bar_table': {
                    'foo': 'bar'
                }
            }
        }
        self.assertDictEqual(expected_state, actual_state)

    def test_exit_with_error_1_if_tap_is_mysql_but_no_record_in_switchover(self):
        switchover_data = {
            'bar_new_database_url': {
                'old_identifier': '',
                'new_identifier': '',
                'old_host': 'foo_database',
                'new_host': 'new_database_url',
                'old_binlog_filename': 'foo',
                'old_binlog_position': 1,
                'new_binlog_filename': 'bar',
                'new_binlog_position': 2,
                'switchover_utc_timestamp': '2025-04-04T15:15:45+00:00',
                'engine': 'mariadb'
            }
        }

        state_content = {
            'bookmarks': {
                "foo_table": {
                    "log_file": "mysql-bin.000001",
                    "log_pos": 1,
                    "version": 1
                },
                'bar_table': {
                    'foo': 'bar'
                }
            }
        }

        with open(f'{self.test_cli.CONFIG_DIR}/target_foo/tap_mysql/state.json', 'w', encoding='utf-8') as state_file:
            json.dump(state_content, state_file)

        with open(f'/tmp/switch_over_test.json', 'w', encoding='utf-8') as switchover_file:
            json.dump(switchover_data, switchover_file)

        arguments = {
            'tap': 'tap_mysql',
            'target': 'target_foo'
        }

        with self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)


    def test_exit_with_error_1_if_tap_is_not_allowed(self):
        """ Test reset_state command exit with error 1 if tap is not allowed for it"""
        arguments = {
            'tap': 'tap_bar',
            'target': 'target_foo'
        }

        with self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)
