import contextlib
import json
import os
import random
import shutil
import string
import tempfile

from unittest import TestCase, mock

import pidfile

from pipelinewise import cli
from pipelinewise.cli import source_identity

PG_IDENTITY = {'system_identifier': '7300000000000000001', 'timeline_id': 2}
MYSQL_IDENTITY = {'host': 'new_database_url'}


class TestResetState(TestCase):
    """Testcases for reset state CLI"""
    def setUp(self):
        resources_dir = f'{os.path.dirname(__file__)}/resources'
        self.test_cli = cli

        # These tests overwrite the state files, so copy the fixtures rather than
        # dirtying the tracked ones.
        self._temp_dir = tempfile.mkdtemp()
        temp_config_dir = os.path.join(self._temp_dir, 'test_reset_state')
        shutil.copytree(f'{resources_dir}/test_reset_state', temp_config_dir)

        self._original_config_dir = cli.CONFIG_DIR
        self._original_venv_dir = cli.VENV_DIR
        self.test_cli.CONFIG_DIR = temp_config_dir
        self.test_cli.VENV_DIR = './virtualenvs-dummy'

    def tearDown(self):
        # Module level globals, so restore them to avoid leaking into other tests.
        cli.CONFIG_DIR = self._original_config_dir
        cli.VENV_DIR = self._original_venv_dir
        shutil.rmtree(self._temp_dir, ignore_errors=True)

    def _run_cli(self, arguments_dict: dict) -> None:
        """Running the test CLI application"""
        argv_list = ['main', 'reset_state']
        if arguments_dict.get('tap'):
            argv_list.extend(['--tap', arguments_dict['tap']])
        if arguments_dict.get('target'):
            argv_list.extend(['--target', arguments_dict['target']])
        if arguments_dict.get('boundary_lsn'):
            argv_list.extend(['--boundary_lsn', arguments_dict['boundary_lsn']])

        with mock.patch('sys.argv', argv_list):
            self.test_cli.main()

    def _write_pg_state(self, state_content):
        with open(f'{self.test_cli.CONFIG_DIR}/target_foo/tap_pg/state.json', 'w', encoding='utf-8') as state_file:
            json.dump(state_content, state_file)

    def _read_pg_state(self):
        with open(f'{self.test_cli.CONFIG_DIR}/target_foo/tap_pg/state.json', 'r', encoding='utf-8') as state_file:
            return json.load(state_file)

    def _assert_pg_reset_refused(self, arguments, state_content, verify_side_effect=None, pid_side_effect=None):
        self._write_pg_state(state_content)
        with mock.patch.object(
            source_identity, 'verify_postgres_slot_covers_boundary', side_effect=verify_side_effect
        ) as verify, mock.patch.object(
            source_identity, 'read_source_identity', return_value=PG_IDENTITY
        ), mock.patch(
            'pipelinewise.cli.pipelinewise.pidfile.PIDFile', side_effect=pid_side_effect
        ) if pid_side_effect else contextlib.nullcontext(), self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)
        self.assertDictEqual(state_content, self._read_pg_state())
        return verify

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
                },
                'source_identity': {'system_identifier': '7300000000000000001', 'timeline_id': 1},
        }
        self._write_pg_state(state_content)

        arguments = {
            'tap': 'tap_pg',
            'target': 'target_foo',
            'boundary_lsn': '16/B374D848',
        }
        with mock.patch.object(
            source_identity, 'verify_postgres_slot_covers_boundary', return_value='pipelinewise_db_tap_pg'
        ) as verify, mock.patch.object(
            source_identity, 'read_source_identity', return_value=PG_IDENTITY
        ):
            self._run_cli(arguments)

        verify.assert_called_once_with(mock.ANY, (0x16 << 32) + 0xB374D848)
        expected_state = {
                'bookmarks': {
                    'foo_table': {
                        'lsn': 1
                    },
                    'bar_table': {
                        'foo': 'bar'
                    }
                },
                'source_identity': PG_IDENTITY,
        }
        self.assertDictEqual(expected_state, self._read_pg_state())

    def test_reset_state_refuses_pg_without_boundary(self):
        """Postgres state is never reset to lsn=1 without the operator's boundary position"""
        verify = self._assert_pg_reset_refused(
            {'tap': 'tap_pg', 'target': 'target_foo'}, {'bookmarks': {'foo_table': {'lsn': 54321}}})
        verify.assert_not_called()

    def test_reset_state_refuses_pg_with_malformed_boundary(self):
        """A boundary that is not an X/Y LSN is rejected before touching the source"""
        verify = self._assert_pg_reset_refused(
            {'tap': 'tap_pg', 'target': 'target_foo', 'boundary_lsn': '12345'},
            {'bookmarks': {'foo_table': {'lsn': 54321}}})
        verify.assert_not_called()

    def test_reset_state_refuses_pg_when_slot_starts_after_boundary(self):
        """A slot that starts after the boundary would skip commits, so state is left unchanged"""
        self._assert_pg_reset_refused(
            {'tap': 'tap_pg', 'target': 'target_foo', 'boundary_lsn': '16/B374D848'},
            {'bookmarks': {'foo_table': {'lsn': 54321}}},
            verify_side_effect=source_identity.SourceIdentityError('starts after the boundary'))

    def test_reset_state_refuses_while_tap_is_running(self):
        """reset_state takes the tap lock, so a running tap cannot overwrite the reset"""
        verify = self._assert_pg_reset_refused(
            {'tap': 'tap_pg', 'target': 'target_foo', 'boundary_lsn': '16/B374D848'},
            {'bookmarks': {'foo_table': {'lsn': 54321}}},
            pid_side_effect=pidfile.AlreadyRunningError)
        verify.assert_not_called()

    def test_reset_state_holds_tap_lock_during_reset(self):
        """The state is rewritten inside the tap pidfile lock"""
        self._write_pg_state({'bookmarks': {'foo_table': {'lsn': 54321}}})
        events = []

        class RecordingLock:
            def __init__(self, path):
                events.append(('lock', os.path.basename(path)))

            def __enter__(self):
                return self

            def __exit__(self, *_):
                events.append(('unlock',))

        def verify(*_):
            events.append(('verify',))
            return 'slot'

        with mock.patch('pipelinewise.cli.pipelinewise.pidfile.PIDFile', RecordingLock), mock.patch.object(
            source_identity, 'verify_postgres_slot_covers_boundary', side_effect=verify
        ), mock.patch.object(source_identity, 'read_source_identity', return_value=PG_IDENTITY):
            self._run_cli({'tap': 'tap_pg', 'target': 'target_foo', 'boundary_lsn': '0/10'})

        self.assertEqual([('lock', 'pipelinewise.pid'), ('verify',), ('unlock',)], events)

    def test_reset_state_file_if_tap_is_mysql(self):
        """ Test reset_state command for MySQL taps"""
        old_pos = random.randint(1, 100)
        new_pos = random.randint(1, 100)
        old_bin = ''.join(random.choice(string.ascii_letters) for _ in range(10))
        new_bin = ''.join(random.choice(string.ascii_letters) for _ in range(10))
        state_content = {
            'bookmarks': {
                'foo_table': {
                    'log_file': 'mysql-bin.000001',
                    'log_pos': old_pos,
                    'version': 1
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

        with open('/tmp/switch_over_test.json', 'w', encoding='utf-8') as switchover_file:
            json.dump(switchover_data, switchover_file)

        arguments = {
            'tap': 'tap_mysql',
            'target': 'target_foo'
        }
        with mock.patch.object(source_identity, 'read_source_identity', return_value=MYSQL_IDENTITY) as identity:
            self._run_cli(arguments)

        identity.assert_called_once_with('tap-mysql', {'host': 'new_database_url'})
        with open(f'{self.test_cli.CONFIG_DIR}/target_foo/tap_mysql/state.json', 'r', encoding='utf-8') as state_file:
            actual_state = json.load(state_file)

        expected_state = {
            'bookmarks': {
                'foo_table': {
                    'log_file': new_bin,
                    'log_pos': new_pos,
                    'version': 1
                },
                'bar_table': {
                    'foo': 'bar'
                }
            },
            'source_identity': MYSQL_IDENTITY,
        }
        self.assertDictEqual(expected_state, actual_state)

    def test_exit_with_error_1_if_tap_is_mysql_but_no_record_in_switchover(self):
        """Test reset_state command returns error 1 if there is no record in json file for the tap"""
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
                'foo_table': {
                    'log_file': 'mysql-bin.000001',
                    'log_pos': 1,
                    'version': 1
                },
                'bar_table': {
                    'foo': 'bar'
                }
            }
        }

        with open(f'{self.test_cli.CONFIG_DIR}/target_foo/tap_mysql/state.json', 'w', encoding='utf-8') as state_file:
            json.dump(state_content, state_file)

        with open('/tmp/switch_over_test.json', 'w', encoding='utf-8') as switchover_file:
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
