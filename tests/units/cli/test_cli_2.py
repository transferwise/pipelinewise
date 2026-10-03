import json
import os
import shutil

from unittest.mock import Mock, patch, call

import pytest

from pipelinewise.cli import PipelineWise
from pipelinewise.cli.config import Config
from pipelinewise.cli.errors import PreRunChecksException
from .cli_args import CliArgs

RESOURCES_DIR = f'{os.path.dirname(__file__)}/resources'
CONFIG_DIR = f'{RESOURCES_DIR}/sample_json_config'
VIRTUALENVS_DIR = './virtualenvs-dummy'
TEST_PROJECT_NAME = 'test-project'
TEST_PROJECT_DIR = f'{os.getcwd()}/{TEST_PROJECT_NAME}'
PROFILING_DIR = './profiling'
EMPTY_CLEANUP_JOURNAL = {'version': 1, 'taps': [], 'targets': []}


def cleanup_item(
        tap_type='tap-postgres',
        *,
        target_id='warehouse',
        tap_id='orders',
        cleanup_kind=None,
):
    """Build one strict deleted-config cleanup journal item."""
    is_postgres = tap_type == 'tap-postgres'
    return {
        'target_id': target_id,
        'tap_id': tap_id,
        'tap_type': tap_type,
        'source_cleanup': 'postgres' if is_postgres else 'none',
        'cleanup_kind': cleanup_kind or ('postgres_slots' if is_postgres else 'local'),
    }


def project_config(taps, target_id='warehouse'):
    return {
        'targets': [{
            'id': target_id,
            'type': 'target-snowflake',
            'taps': taps,
        }],
    }


def write_cleanup_journal(config_dir, taps, targets=()):
    config_dir.mkdir(parents=True, exist_ok=True)
    path = config_dir / '.deleted-config-cleanup.json'
    path.write_text(json.dumps({
        'version': 1,
        'taps': taps,
        'targets': list(targets),
    }), encoding='utf-8')
    return path


def read_cleanup_journal(config_dir):
    return json.loads((config_dir / '.deleted-config-cleanup.json').read_text(encoding='utf-8'))


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

    def test_cleanup_after_deleted_config(self, tmp_path):
        """Test that cleanup of config of deleted taps and target takes place"""
        config_dir = tmp_path / 'pipelinewise'
        (config_dir / 'target_two' / 'tap_four').mkdir(parents=True)
        (config_dir / 'target_three' / 'tap_five').mkdir(parents=True)
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {
            'targets': [
                {
                    'id': 'target_one',
                    'type': 'target-snowflake',
                    'taps': [dict(id='tap_one', type='tap-mysql'), dict(id='tap_two', type='tap-postgres')],
                },
                {
                    'id': 'target_two',
                    'type': 'target-s3-csv',
                    'taps': [dict(id='tap_three', type='tap-mysql')],
                },
            ],
        }
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

        with patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot:
            deleted_taps_count = pipelinewise.cleanup_after_deleted_config(old_config)

        assert deleted_taps_count == 2
        assert not (config_dir / 'target_two' / 'tap_four').exists()
        assert (config_dir / 'target_two').exists()
        assert not (config_dir / 'target_three').exists()
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

        # not called because none of the deleted taps are tap-postgres
        drop_slot.assert_not_called()

    def test_cleanup_after_deleted_config_of_tap_postgres(self, tmp_path):
        """Test that cleanup of config and slot of deleted postgres tap takes place"""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'target_two' / 'tap_four'
        tap_dir.mkdir(parents=True)
        (tap_dir / 'config.json').write_text(json.dumps({'host': 'localhost'}), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {
            'targets': [
                {
                    'id': 'target_one',
                    'type': 'target-snowflake',
                    'taps': [dict(id='tap_one', type='tap-mysql'), dict(id='tap_two', type='tap-postgres')],
                },
                {
                    'id': 'target_two',
                    'type': 'target-s3-csv',
                    'taps': [dict(id='tap_three', type='tap-mysql')],
                },
            ],
        }
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

        with patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot:
            deleted_taps_count = pipelinewise.cleanup_after_deleted_config(old_config)

        assert deleted_taps_count == 1
        assert not tap_dir.exists()
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

        # called because the deleted tap is a tap-postgres
        drop_slot.assert_called_once_with(
            {'host': 'localhost', 'tap_id': 'tap_four'},
            allow_unsupported_version_for_config_removal=True,
        )

    def test_import_queues_deleted_cleanup_before_replacing_root_config(self):
        """The deletion diff is durable before Config.save replaces its source."""
        generated_config = Mock(targets={}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []
        calls = Mock()
        generated_config.save = calls.save
        self.pipelinewise._queue_deleted_config_cleanup = calls.queue
        calls.queue.return_value = {
            'deleted_taps_count': 0,
            'retained_taps': frozenset(),
        }
        self.pipelinewise._preserve_renamed_postgres_state = Mock()
        self.pipelinewise.load_config = Mock()
        self.pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

        with patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config):
            self.pipelinewise.import_project()

        assert calls.method_calls[:2] == [
            call.queue(
                self.pipelinewise.config,
                {'targets': []},
                selected_taps=['*'],
                validate_reuse=True,
                project_config={'targets': []},
            ),
            call.save(['*'], persisted_config={'targets': []}),
        ]

    def test_import_save_failure_does_not_cancel_pending_cleanup(self, tmp_path):
        """A validated same-ID re-add remains pending until Config.save succeeds."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        source = {'host': 'postgres', 'port': 5432, 'dbname': 'source'}
        (tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        cleanup_path = write_cleanup_journal(config_dir, [cleanup_item()], ['warehouse'])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {'targets': []}
        target = {
            'id': 'warehouse',
            'type': 'target-snowflake',
            'taps': [{'id': 'orders', 'type': 'tap-postgres', 'db_conn': source}],
        }
        generated_config = Mock(targets={'warehouse': target}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []
        generated_config.save.side_effect = RuntimeError('save failed')

        with patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config):
            with pytest.raises(RuntimeError, match='save failed'):
                pipelinewise.import_project()

        generated_config.save.assert_called_once()
        assert generated_config.save.call_args.args == (['*'],)
        assert generated_config.save.call_args.kwargs['persisted_config']['targets'][0]['taps'][0]['id'] == 'orders'
        assert json.loads(cleanup_path.read_text(encoding='utf-8')) == {
            'version': 1,
            'taps': [cleanup_item()],
            'targets': ['warehouse'],
        }

    @pytest.mark.parametrize(
        ('target_type', 'account'),
        [
            ('target-postgres', 'old-account'),
            ('target-snowflake', 'new-account'),
        ],
        ids=['type-change', 'connection-change'],
    )
    def test_partial_import_rejects_shared_target_change_with_retained_sibling(
            self, tmp_path, target_type, account,
    ):
        """Selecting one tap cannot reroute an unselected materialized sibling."""
        config_dir = tmp_path / 'pipelinewise'
        target_dir = config_dir / 'warehouse'
        target_dir.mkdir(parents=True)
        previous_connection = {'account': 'old-account', 'dbname': 'warehouse'}
        (target_dir / 'config.json').write_text(
            json.dumps(previous_connection),
            encoding='utf-8',
        )
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = project_config([
            {'id': 'orders', 'type': 'tap-postgres'},
            {'id': 'customers', 'type': 'tap-mysql'},
        ])
        current_target = {
            'id': 'warehouse',
            'type': target_type,
            'db_conn': {**previous_connection, 'account': account},
            'taps': [
                {'id': 'orders', 'type': 'tap-postgres'},
                {'id': 'customers', 'type': 'tap-mysql'},
            ],
        }
        config = Config(str(config_dir))
        config.targets = {'warehouse': current_target}
        persisted = config.build_persisted_config(['orders'], old_config)

        with pytest.raises(PreRunChecksException, match='cannot change shared target'):
            pipelinewise._validate_partial_target_runtime(
                config,
                persisted,
                old_config,
                ['orders'],
            )

    def test_partial_import_accepts_unchanged_shared_target_with_retained_sibling(self, tmp_path):
        """A partial tap update may reuse an unchanged shared target runtime."""
        config_dir = tmp_path / 'pipelinewise'
        target_dir = config_dir / 'warehouse'
        target_dir.mkdir(parents=True)
        connection = {'account': 'account', 'dbname': 'warehouse'}
        (target_dir / 'config.json').write_text(json.dumps(connection), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = project_config([
            {'id': 'orders', 'type': 'tap-postgres'},
            {'id': 'customers', 'type': 'tap-mysql'},
        ])
        current_target = {
            'id': 'warehouse',
            'type': 'target-snowflake',
            'db_conn': connection,
            'taps': [
                {'id': 'orders', 'type': 'tap-postgres'},
                {'id': 'customers', 'type': 'tap-mysql'},
            ],
        }
        config = Config(str(config_dir))
        config.targets = {'warehouse': current_target}
        persisted = config.build_persisted_config(['orders'], old_config)

        pipelinewise._validate_partial_target_runtime(
            config,
            persisted,
            old_config,
            ['orders'],
        )

    @pytest.mark.parametrize(
        'tap',
        [
            {'id': 'orders', 'type': 'tap-postgres',
             'db_conn': {'host': 'other', 'port': 5432, 'dbname': 'source'}},
            {'id': 'orders', 'type': 'tap-mysql', 'db_conn': {'host': 'postgres', 'dbname': 'source'}},
        ],
        ids=['source-mismatch', 'connector-mismatch'],
    )
    def test_pending_same_id_mismatch_is_rejected_before_config_save(self, tmp_path, tap):
        """A pending PostgreSQL identity cannot be overwritten by an unrelated re-add."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        source = {'host': 'postgres', 'port': 5432, 'dbname': 'source'}
        (tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        write_cleanup_journal(config_dir, [cleanup_item()])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {'targets': []}
        target = {'id': 'warehouse', 'type': 'target-snowflake', 'taps': [tap]}
        generated_config = Mock(targets={'warehouse': target}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []

        with patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config):
            with pytest.raises(PreRunChecksException, match='Pending cleanup|source identity'):
                pipelinewise.import_project()

        generated_config.save.assert_not_called()
        assert read_cleanup_journal(config_dir)['taps'] == [cleanup_item()]

    def test_failed_deleted_postgres_cleanup_is_retried_and_removes_target(self, tmp_path):
        """A failed slot cleanup remains queued after the generated config changed."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        (tap_dir / 'config.json').write_text(json.dumps({'host': 'localhost'}), encoding='utf-8')
        current_config = {'targets': []}
        (config_dir / 'config.json').write_text(json.dumps(current_config), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = {
            'targets': [{
                'id': 'warehouse',
                'type': 'target-snowflake',
                'taps': [{'id': 'orders', 'type': 'tap-postgres'}],
            }],
        }

        with patch(
                'pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot',
                side_effect=[RuntimeError('slot is active'), None],
        ) as drop_slot:
            with pytest.raises(RuntimeError, match='slot is active'):
                pipelinewise.cleanup_after_deleted_config(old_config)

            cleanup_path = config_dir / '.deleted-config-cleanup.json'
            assert json.loads(cleanup_path.read_text(encoding='utf-8')) == {
                'version': 1,
                'taps': [{
                    'target_id': 'warehouse',
                    'tap_id': 'orders',
                    'tap_type': 'tap-postgres',
                    'source_cleanup': 'postgres',
                    'cleanup_kind': 'postgres_slots',
                }],
                'targets': ['warehouse'],
            }
            assert pipelinewise.cleanup_after_deleted_config(current_config) == 0

        assert drop_slot.call_args_list == [
            call(
                {'host': 'localhost', 'tap_id': 'orders'},
                allow_unsupported_version_for_config_removal=True,
            ),
            call(
                {'host': 'localhost', 'tap_id': 'orders'},
                allow_unsupported_version_for_config_removal=True,
            ),
        ]
        assert not (config_dir / 'warehouse').exists()
        assert json.loads(cleanup_path.read_text(encoding='utf-8')) == EMPTY_CLEANUP_JOURNAL

    def test_local_cleanup_failure_retries_without_repeating_postgres_cleanup(self, tmp_path):
        """A durable local phase prevents a second source cleanup after a local failure."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        (tap_dir / 'config.json').write_text(json.dumps({'host': 'localhost'}), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {'targets': []}
        old_config = project_config([{'id': 'orders', 'type': 'tap-postgres'}])

        real_delete = pipelinewise._delete_tap_runtime
        with (
                patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot,
                patch.object(
                    pipelinewise,
                    '_delete_tap_runtime',
                    side_effect=[OSError('disk unavailable'), None],
                ) as delete_runtime,
        ):
            with pytest.raises(OSError, match='disk unavailable'):
                pipelinewise.cleanup_after_deleted_config(old_config)

            assert read_cleanup_journal(config_dir)['taps'] == [
                cleanup_item(cleanup_kind='local')
            ]
            assert tap_dir.exists()
            delete_runtime.side_effect = lambda target_id, tap_id: real_delete(target_id, tap_id)
            pipelinewise.cleanup_after_deleted_config({'targets': []})

        drop_slot.assert_called_once()
        assert not tap_dir.exists()
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

    def test_failed_local_phase_persist_retries_postgres_cleanup_before_deleting_runtime(self, tmp_path):
        """Source cleanup is repeated if its completed phase could not be made durable."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        (tap_dir / 'config.json').write_text(json.dumps({'host': 'localhost'}), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {'targets': []}
        old_config = project_config([{'id': 'orders', 'type': 'tap-postgres'}])
        real_persist = pipelinewise._persist_deleted_config_cleanup

        def fail_local_phase(cleanup):
            if cleanup['taps'] and cleanup['taps'][0]['cleanup_kind'] == 'local':
                raise OSError('cannot persist local phase')
            return real_persist(cleanup)

        with patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot:
            with patch.object(pipelinewise, '_persist_deleted_config_cleanup', side_effect=fail_local_phase):
                with pytest.raises(OSError, match='cannot persist local phase'):
                    pipelinewise.cleanup_after_deleted_config(old_config)

            assert read_cleanup_journal(config_dir)['taps'] == [cleanup_item()]
            assert tap_dir.exists()
            pipelinewise.cleanup_after_deleted_config({'targets': []})

        assert drop_slot.call_count == 2
        assert not tap_dir.exists()
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

    def test_retry_keeps_target_that_was_readded(self, tmp_path):
        """Retry removes an old tap without deleting its re-added target directory."""
        config_dir = tmp_path / 'pipelinewise'
        old_tap_dir = config_dir / 'warehouse' / 'orders'
        old_tap_dir.mkdir(parents=True)
        (old_tap_dir / 'config.json').write_text(json.dumps({'host': 'localhost'}), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = {
            'targets': [{
                'id': 'warehouse',
                'type': 'target-snowflake',
                'taps': [{'id': 'orders', 'type': 'tap-postgres'}],
            }],
        }
        pipelinewise.config = {'targets': []}

        with patch(
                'pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot',
                side_effect=RuntimeError('slot is active'),
        ):
            with pytest.raises(RuntimeError, match='slot is active'):
                pipelinewise.cleanup_after_deleted_config(old_config)

        replacement_dir = config_dir / 'warehouse' / 'customers'
        replacement_dir.mkdir()
        marker = replacement_dir / 'state.json'
        marker.write_text('{}', encoding='utf-8')
        readded_config = {
            'targets': [{
                'id': 'warehouse',
                'type': 'target-snowflake',
                'taps': [{'id': 'customers', 'type': 'tap-mysql'}],
            }],
        }
        pipelinewise.config = readded_config

        with patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot:
            pipelinewise.cleanup_after_deleted_config(readded_config)

        drop_slot.assert_called_once_with(
            {'host': 'localhost', 'tap_id': 'orders'},
            allow_unsupported_version_for_config_removal=True,
        )
        assert marker.exists()
        assert not old_tap_dir.exists()
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

    def test_retry_is_cancelled_when_previous_tap_id_adopts_deleted_tap(self, tmp_path):
        """A rename takes ownership of retained state before pending cleanup can retry."""
        config_dir = tmp_path / 'pipelinewise'
        old_tap_dir = config_dir / 'warehouse' / 'orders-old'
        old_tap_dir.mkdir(parents=True)
        source = {'host': 'localhost', 'port': 5432, 'dbname': 'source'}
        (old_tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = {
            'targets': [{
                'id': 'warehouse',
                'type': 'target-snowflake',
                'taps': [{'id': 'orders-old', 'type': 'tap-postgres'}],
            }],
        }
        pipelinewise.config = {
            'targets': [{'id': 'warehouse', 'type': 'target-snowflake', 'taps': []}],
        }

        with patch(
                'pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot',
                side_effect=RuntimeError('slot is active'),
        ):
            with pytest.raises(RuntimeError, match='slot is active'):
                pipelinewise.cleanup_after_deleted_config(old_config)

        adopted_config = {
            'targets': [{
                'id': 'warehouse',
                'type': 'target-snowflake',
                'taps': [{
                    'id': 'orders',
                    'type': 'tap-postgres',
                    'previous_tap_id': 'orders-old',
                    'db_conn': source,
                }],
            }],
        }
        cleanup_plan = pipelinewise._queue_deleted_config_cleanup(
            pipelinewise.config,
            adopted_config,
            selected_taps=['*'],
            validate_reuse=True,
        )
        pipelinewise.config = adopted_config
        with patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot:
            pipelinewise.cleanup_after_deleted_config(adopted_config, cleanup_plan=cleanup_plan)

        drop_slot.assert_not_called()
        assert old_tap_dir.exists()
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

    def test_removing_renamed_tap_cleans_historical_runtime_after_source_cleanup(self, tmp_path):
        """A renamed tap's retained old path remains durable until source cleanup succeeds."""
        config_dir = tmp_path / 'pipelinewise'
        old_tap_dir = config_dir / 'warehouse' / 'orders-old'
        current_tap_dir = config_dir / 'warehouse' / 'orders'
        old_tap_dir.mkdir(parents=True)
        current_tap_dir.mkdir(parents=True)
        source = {
            'host': 'postgres',
            'port': 5432,
            'dbname': 'source',
            'previous_tap_id': 'orders-old',
        }
        (old_tap_dir / 'state.json').write_text('{"bookmarks": {}}', encoding='utf-8')
        (current_tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = project_config([{
            'id': 'orders',
            'type': 'tap-postgres',
            'previous_tap_id': 'orders-old',
        }])
        new_config = project_config([])

        cleanup_plan = pipelinewise._queue_deleted_config_cleanup(
            old_config,
            new_config,
            selected_taps=['*'],
            validate_reuse=True,
        )
        pipelinewise.config = new_config

        assert read_cleanup_journal(config_dir)['taps'] == [
            cleanup_item(tap_id='orders'),
            cleanup_item(tap_id='orders-old', cleanup_kind='local'),
        ]

        with patch(
                'pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot',
                side_effect=RuntimeError('source cleanup failed'),
        ):
            with pytest.raises(RuntimeError, match='source cleanup failed'):
                pipelinewise.cleanup_after_deleted_config(old_config, cleanup_plan=cleanup_plan)

        assert current_tap_dir.exists()
        assert old_tap_dir.exists()
        assert read_cleanup_journal(config_dir)['taps'] == [
            cleanup_item(tap_id='orders'),
            cleanup_item(tap_id='orders-old', cleanup_kind='local'),
        ]

        with patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot:
            pipelinewise.cleanup_after_deleted_config(new_config)

        drop_slot.assert_called_once_with(
            {**source, 'tap_id': 'orders'},
            allow_unsupported_version_for_config_removal=True,
        )
        assert not current_tap_dir.exists()
        assert not old_tap_dir.exists()
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

    def test_partial_import_cannot_retire_previous_tap_runtime_for_unselected_tap(self, tmp_path):
        """Removing previous_tap_id requires writing that tap's generated runtime first."""
        config_dir = tmp_path / 'pipelinewise'
        config_dir.mkdir()
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = project_config([{
            'id': 'orders',
            'type': 'tap-postgres',
            'previous_tap_id': 'orders-old',
        }])
        new_config = project_config([{
            'id': 'orders',
            'type': 'tap-postgres',
            'db_conn': {'host': 'postgres', 'port': 5432, 'dbname': 'source'},
        }])

        with pytest.raises(PreRunChecksException, match='unselected tap.*generated runtime unchanged'):
            pipelinewise._queue_deleted_config_cleanup(
                old_config,
                new_config,
                selected_taps=['another-tap'],
                validate_reuse=True,
            )

        assert not (config_dir / '.deleted-config-cleanup.json').exists()

    @pytest.mark.parametrize(
        ('new_tap', 'owner_id'),
        [
            ({'id': 'orders', 'type': 'tap-postgres'}, 'orders'),
            ({'id': 'orders-new', 'type': 'tap-postgres', 'previous_tap_id': 'orders'}, 'orders-new'),
        ],
        ids=['same-id', 'previous-tap-id'],
    )
    def test_partial_import_rejects_unselected_pending_cleanup_owner(self, tmp_path, new_tap, owner_id):
        """A persisted owner cannot claim pending cleanup without writing its runtime files."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        source = {'host': 'postgres', 'port': 5432, 'dbname': 'source'}
        (tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        target_connection = {'account': 'warehouse'}
        (tap_dir.parent / 'config.json').write_text(json.dumps(target_connection), encoding='utf-8')
        write_cleanup_journal(config_dir, [cleanup_item()])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = project_config([new_tap])
        pipelinewise.args.taps = 'another-tap'
        target = {
            'id': 'warehouse',
            'type': 'target-snowflake',
            'db_conn': target_connection,
            'taps': [
                {**new_tap, 'db_conn': source},
                {'id': 'another-tap', 'type': 'tap-mysql', 'db_conn': {'host': 'mysql'}},
            ],
        }
        generated_config = Mock(targets={'warehouse': target}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []

        with patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config):
            with pytest.raises(PreRunChecksException, match=rf'--taps {owner_id}.*--taps "\*"'):
                pipelinewise.import_project()

        generated_config.save.assert_not_called()
        assert read_cleanup_journal(config_dir)['taps'] == [cleanup_item()]

    @pytest.mark.parametrize(
        ('old_type', 'new_tap'),
        [
            (
                'tap-postgres',
                {'id': 'orders', 'type': 'tap-postgres',
                 'db_conn': {'host': 'postgres', 'port': 5432, 'dbname': 'other'}},
            ),
            (
                'tap-postgres',
                {'id': 'orders', 'type': 'tap-mysql', 'db_conn': {'host': 'mysql'}},
            ),
            (
                'tap-mysql',
                {'id': 'orders', 'type': 'tap-postgres',
                 'db_conn': {'host': 'postgres', 'port': 5432, 'dbname': 'source'}},
            ),
        ],
        ids=['source-change', 'postgres-to-mysql', 'mysql-to-postgres'],
    )
    def test_in_place_postgres_identity_change_is_rejected_without_pending_cleanup(
            self, tmp_path, old_type, new_tap,
    ):
        """Changing a PostgreSQL source or connector type requires a completed delete/import cycle."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        source = {'host': 'postgres', 'port': 5432, 'dbname': 'source'}
        (tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = project_config([{'id': 'orders', 'type': old_type}])
        new_config = project_config([new_tap])

        with pytest.raises(PreRunChecksException, match='Cannot change existing tap|source identity'):
            pipelinewise._queue_deleted_config_cleanup(
                old_config,
                new_config,
                selected_taps=['*'],
                validate_reuse=True,
            )

        assert not (config_dir / '.deleted-config-cleanup.json').exists()

    @pytest.mark.parametrize(
        ('old_type', 'new_type'),
        [
            ('tap-postgres', 'tap-postgres'),
            ('tap-postgres', 'tap-mysql'),
            ('tap-mysql', 'tap-postgres'),
        ],
        ids=['postgres-to-postgres', 'postgres-to-mysql', 'mysql-to-postgres'],
    )
    def test_postgres_involved_target_move_is_rejected_before_config_save(
            self, tmp_path, old_type, new_type,
    ):
        """Moving a slot-owning tap ID across target runtime/PID paths requires cleanup first."""
        config_dir = tmp_path / 'pipelinewise'
        config_dir.mkdir()
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = project_config(
            [{'id': 'orders', 'type': old_type}],
            target_id='target-a',
        )
        pipelinewise.args.taps = 'orders'
        target = project_config(
            [{
                'id': 'orders',
                'type': new_type,
                'db_conn': {'host': 'source', 'port': 5432, 'dbname': 'database'},
            }, {
                'id': 'another-tap',
                'type': 'tap-mysql',
                'db_conn': {'host': 'mysql'},
            }],
            target_id='target-b',
        )['targets'][0]
        generated_config = Mock(targets={'target-b': target}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []

        with patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config):
            with pytest.raises(PreRunChecksException, match='[Cc]annot move.*targets'):
                pipelinewise.import_project()

        generated_config.save.assert_not_called()

    @pytest.mark.parametrize('change', ['target-move', 'connector-type', 'source'])
    def test_partial_import_defers_unselected_postgres_identity_change(
            self, tmp_path, change,
    ):
        """An unselected identity keeps its persisted owner until it is imported."""
        config_dir = tmp_path / 'pipelinewise'
        config_dir.mkdir()
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = project_config(
            [{'id': 'orders', 'type': 'tap-postgres'}],
            target_id='target-a',
        )
        pipelinewise.args.taps = 'another-tap'

        changed_target_id = 'target-b' if change == 'target-move' else 'target-a'
        changed_type = 'tap-mysql' if change == 'connector-type' else 'tap-postgres'
        changed_target = project_config(
            [{
                'id': 'orders',
                'type': changed_type,
                'db_conn': {'host': 'other-source' if change == 'source' else 'source'},
            }],
            target_id=changed_target_id,
        )['targets'][0]
        selected_target = project_config(
            [{'id': 'another-tap', 'type': 'tap-mysql', 'db_conn': {'host': 'mysql'}}],
            target_id='target-c',
        )['targets'][0]
        generated_config = Mock(
            targets={
                changed_target_id: changed_target,
                'target-c': selected_target,
            },
            global_config={},
        )
        generated_config.get_data_diff_definitions.return_value = []
        pipelinewise._discover_tap = Mock(return_value=None)
        pipelinewise._reconcile_postgres_publication_after_discovery = Mock(return_value=None)
        pipelinewise._preserve_renamed_postgres_state = Mock()
        pipelinewise.load_config = Mock()
        pipelinewise.cleanup_after_deleted_config = Mock(return_value=0)

        with patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config):
            pipelinewise.import_project()

        persisted = generated_config.save.call_args.kwargs['persisted_config']
        persisted_taps = pipelinewise._config_tap_definitions(persisted)
        assert persisted_taps[('target-a', 'orders')]['type'] == 'tap-postgres'
        assert (changed_target_id, 'orders') not in persisted_taps or changed_target_id == 'target-a'
        assert persisted_taps[('target-c', 'another-tap')]['type'] == 'tap-mysql'
        generated_config.save.assert_called_once_with(
            ['another-tap'],
            persisted_config=persisted,
        )

    def test_partial_import_cleans_pending_identity_ignored_by_brand_new_unselected_tap(
            self, tmp_path,
    ):
        """An unmaterialized YAML tap cannot claim or block an old cleanup tombstone."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        source = {'host': 'postgres', 'port': 5432, 'dbname': 'source'}
        (tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        write_cleanup_journal(config_dir, [cleanup_item()])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {'targets': []}
        pipelinewise.args.taps = 'another-tap'
        target = project_config([
            {'id': 'orders', 'type': 'tap-postgres', 'db_conn': source},
            {'id': 'another-tap', 'type': 'tap-mysql', 'db_conn': {'host': 'mysql'}},
        ])['targets'][0]
        generated_config = Mock(targets={'warehouse': target}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []
        pipelinewise._discover_tap = Mock(return_value=None)
        pipelinewise._reconcile_postgres_publication_after_discovery = Mock(return_value=None)
        pipelinewise._preserve_renamed_postgres_state = Mock()
        pipelinewise.load_config = Mock()

        with (
                patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config),
                patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot,
        ):
            pipelinewise.import_project()

        persisted = generated_config.save.call_args.kwargs['persisted_config']
        assert set(pipelinewise._config_tap_definitions(persisted)) == {
            ('warehouse', 'another-tap'),
        }
        drop_slot.assert_called_once_with(
            {**source, 'tap_id': 'orders'},
            allow_unsupported_version_for_config_removal=True,
        )
        assert not tap_dir.exists()
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

    def test_pending_postgres_cleanup_rejects_target_move_after_prior_config_save(self, tmp_path):
        """A retry detects target B even when the old root already stopped mentioning target A."""
        config_dir = tmp_path / 'pipelinewise'
        old_tap_dir = config_dir / 'target-a' / 'orders'
        new_tap_dir = config_dir / 'target-b' / 'orders'
        old_tap_dir.mkdir(parents=True)
        new_tap_dir.mkdir(parents=True)
        source = {'host': 'source', 'port': 5432, 'dbname': 'database'}
        (old_tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        (new_tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        write_cleanup_journal(
            config_dir,
            [cleanup_item(target_id='target-a')],
            ['target-a'],
        )
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        target = project_config(
            [{'id': 'orders', 'type': 'tap-postgres', 'db_conn': source}],
            target_id='target-b',
        )['targets'][0]
        pipelinewise.config = project_config(
            [{'id': 'orders', 'type': 'tap-postgres'}],
            target_id='target-b',
        )
        generated_config = Mock(targets={'target-b': target}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []

        with (
                patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config),
                patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot,
        ):
            with pytest.raises(PreRunChecksException, match='cannot move between targets'):
                pipelinewise.import_project()

        generated_config.save.assert_not_called()
        drop_slot.assert_not_called()
        assert read_cleanup_journal(config_dir)['taps'] == [cleanup_item(target_id='target-a')]

    def test_unchanged_in_place_postgres_source_is_allowed(self, tmp_path):
        """The guard accepts the same source using PostgreSQL's effective default port."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        source = {'host': 'postgres', 'dbname': 'source'}
        (tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = project_config([{'id': 'orders', 'type': 'tap-postgres'}])
        new_config = project_config([{
            'id': 'orders',
            'type': 'tap-postgres',
            'db_conn': {'host': 'postgres', 'port': 5432, 'dbname': 'source'},
        }])

        plan = pipelinewise._queue_deleted_config_cleanup(
            old_config,
            new_config,
            selected_taps=['*'],
            validate_reuse=True,
        )

        assert plan == {
            'deleted_taps_count': 0,
            'retained_taps': frozenset(),
        }
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

    @pytest.mark.parametrize('new_host', ['postgres', 'other-postgres'])
    def test_selected_postgres_validation_uses_rendered_source_not_root_projection(self, tmp_path, new_host):
        """The root inventory owns IDs while rendered YAML proves selected source identity."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        source = {'host': 'postgres', 'port': 5432, 'dbname': 'source'}
        (tap_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = project_config([{'id': 'orders', 'type': 'tap-postgres'}])
        persisted_config = project_config([{'id': 'orders', 'type': 'tap-postgres'}])
        rendered_config = project_config([{
            'id': 'orders',
            'type': 'tap-postgres',
            'db_conn': {**source, 'host': new_host},
        }])

        if new_host == 'postgres':
            pipelinewise._queue_deleted_config_cleanup(
                old_config,
                persisted_config,
                selected_taps=['orders'],
                validate_reuse=True,
                project_config=rendered_config,
            )
            assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL
        else:
            with pytest.raises(PreRunChecksException, match='source identity'):
                pipelinewise._queue_deleted_config_cleanup(
                    old_config,
                    persisted_config,
                    selected_taps=['orders'],
                    validate_reuse=True,
                    project_config=rendered_config,
                )

    def test_legacy_unselected_phantom_is_retained_then_fails_closed_on_deletion(self, tmp_path):
        """An old partial-import phantom is not loaded until YAML actually deletes its identity."""
        config_dir = tmp_path / 'pipelinewise'
        orders_dir = config_dir / 'warehouse' / 'orders'
        orders_dir.mkdir(parents=True)
        source = {'host': 'postgres', 'port': 5432, 'dbname': 'source'}
        (orders_dir / 'config.json').write_text(json.dumps(source), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_config = project_config([
            {'id': 'orders', 'type': 'tap-postgres'},
            {'id': 'phantom', 'type': 'tap-postgres'},
        ])
        retained_config = project_config([
            {'id': 'orders', 'type': 'tap-postgres'},
            {'id': 'phantom', 'type': 'tap-postgres'},
        ])
        rendered_config = project_config([
            {'id': 'orders', 'type': 'tap-postgres', 'db_conn': source},
            {'id': 'phantom', 'type': 'tap-postgres', 'db_conn': source},
        ])

        pipelinewise._queue_deleted_config_cleanup(
            old_config,
            retained_config,
            selected_taps=['orders'],
            validate_reuse=True,
            project_config=rendered_config,
        )
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

        deleted_config = project_config([{'id': 'orders', 'type': 'tap-postgres'}])
        deleted_project = project_config([{
            'id': 'orders',
            'type': 'tap-postgres',
            'db_conn': source,
        }])
        cleanup_plan = pipelinewise._queue_deleted_config_cleanup(
            old_config,
            deleted_config,
            selected_taps=['orders'],
            validate_reuse=True,
            project_config=deleted_project,
        )
        pipelinewise.config = deleted_config

        with pytest.raises(PreRunChecksException, match='phantom.*missing or empty'):
            pipelinewise.cleanup_after_deleted_config(old_config, cleanup_plan=cleanup_plan)

        assert read_cleanup_journal(config_dir)['taps'] == [cleanup_item(tap_id='phantom')]

    def test_local_phase_readd_after_partial_save_requires_cleanup_only_import(self, tmp_path):
        """A surviving local tombstone blocks overwrite even when the old root already lists the re-add."""
        config_dir = tmp_path / 'pipelinewise'
        write_cleanup_journal(
            config_dir,
            [cleanup_item(cleanup_kind='local')],
            ['warehouse'],
        )
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        old_root = project_config([{'id': 'orders', 'type': 'tap-postgres'}])
        pipelinewise.config = old_root
        new_config = project_config([{
            'id': 'orders',
            'type': 'tap-postgres',
            'db_conn': {'host': 'new-postgres', 'port': 5432, 'dbname': 'new-source'},
        }])
        generated_config = Mock(
            targets={'warehouse': new_config['targets'][0]},
            global_config={},
        )
        generated_config.get_data_diff_definitions.return_value = []

        with patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config):
            with pytest.raises(PreRunChecksException, match='Run one import with this tap absent'):
                pipelinewise.import_project()

        generated_config.save.assert_not_called()
        assert read_cleanup_journal(config_dir)['taps'] == [cleanup_item(cleanup_kind='local')]

        cleanup_only = {'targets': []}
        cleanup_plan = pipelinewise._queue_deleted_config_cleanup(
            old_root,
            cleanup_only,
            selected_taps=['*'],
            validate_reuse=True,
        )
        pipelinewise.config = cleanup_only
        pipelinewise.cleanup_after_deleted_config(old_root, cleanup_plan=cleanup_plan)

        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL
        readd_plan = pipelinewise._queue_deleted_config_cleanup(
            cleanup_only,
            new_config,
            selected_taps=['orders'],
            validate_reuse=True,
        )
        assert readd_plan['retained_taps'] == frozenset()

    def test_local_phase_same_id_readd_keeps_old_runtime_until_cleanup_only_import(self, tmp_path):
        """The two-step rule applies to exact re-adds of every connector type."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        marker = tap_dir / 'old-state.json'
        marker.write_text('{}', encoding='utf-8')
        write_cleanup_journal(config_dir, [cleanup_item(cleanup_kind='local')])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {'targets': []}
        target = project_config([{
            'id': 'orders',
            'type': 'tap-mysql',
            'db_conn': {'host': 'mysql', 'port': 3306, 'dbname': 'new-source'},
        }])['targets'][0]
        generated_config = Mock(targets={'warehouse': target}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []

        with patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config):
            with pytest.raises(PreRunChecksException, match='Run one import with this tap absent'):
                pipelinewise.import_project()

        generated_config.save.assert_not_called()
        assert marker.exists()
        assert read_cleanup_journal(config_dir)['taps'] == [cleanup_item(cleanup_kind='local')]

        pipelinewise.cleanup_after_deleted_config({'targets': []})

        assert not marker.exists()
        assert read_cleanup_journal(config_dir) == EMPTY_CLEANUP_JOURNAL

    def test_local_phase_previous_tap_id_adoption_is_rejected(self, tmp_path):
        """Local-phase adoption is rejected before rename state can be copied."""
        config_dir = tmp_path / 'pipelinewise'
        write_cleanup_journal(config_dir, [cleanup_item(cleanup_kind='local')])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {'targets': []}
        target = project_config([{
            'id': 'orders-new',
            'type': 'tap-postgres',
            'previous_tap_id': 'orders',
            'db_conn': {'host': 'postgres', 'port': 5432, 'dbname': 'source'},
        }])['targets'][0]
        generated_config = Mock(targets={'warehouse': target}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []

        with (
                patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config),
                patch.object(pipelinewise, '_preserve_renamed_postgres_state') as preserve_state,
        ):
            with pytest.raises(PreRunChecksException, match='cannot be adopted'):
                pipelinewise.import_project()

        preserve_state.assert_not_called()
        generated_config.save.assert_not_called()
        assert read_cleanup_journal(config_dir)['taps'] == [cleanup_item(cleanup_kind='local')]

    def test_rename_cannot_overwrite_destination_with_its_own_pending_cleanup(self, tmp_path):
        """One renamed tap cannot claim exact and previous identities before both are cleaned."""
        config_dir = tmp_path / 'pipelinewise'
        destination_dir = config_dir / 'warehouse' / 'orders-new'
        destination_dir.mkdir(parents=True)
        destination_config = destination_dir / 'config.json'
        destination_config.write_text(json.dumps({'host': 'destination-source'}), encoding='utf-8')
        write_cleanup_journal(config_dir, [
            cleanup_item(tap_id='orders-old'),
            cleanup_item(tap_id='orders-new'),
        ])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {'targets': []}
        target = project_config([{
            'id': 'orders-new',
            'type': 'tap-postgres',
            'previous_tap_id': 'orders-old',
            'db_conn': {'host': 'postgres', 'port': 5432, 'dbname': 'source'},
        }])['targets'][0]
        generated_config = Mock(targets={'warehouse': target}, global_config={})
        generated_config.get_data_diff_definitions.return_value = []

        with (
                patch('pipelinewise.cli.pipelinewise.Config.from_yamls', return_value=generated_config),
                patch.object(pipelinewise, '_preserve_renamed_postgres_state') as preserve_state,
        ):
            with pytest.raises(PreRunChecksException, match='own identity also has pending cleanup'):
                pipelinewise.import_project()

        preserve_state.assert_not_called()
        generated_config.save.assert_not_called()
        assert json.loads(destination_config.read_text(encoding='utf-8')) == {
            'host': 'destination-source',
        }
        assert not (destination_dir / 'state.json').exists()
        assert read_cleanup_journal(config_dir)['taps'] == [
            cleanup_item(tap_id='orders-old'),
            cleanup_item(tap_id='orders-new'),
        ]

    def test_missing_deleted_postgres_config_keeps_cleanup_pending(self, tmp_path):
        """Lost source credentials require explicit recovery instead of local-only cleanup."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {
            'targets': [{'id': 'warehouse', 'type': 'target-snowflake', 'taps': []}],
        }
        old_config = {
            'targets': [{
                'id': 'warehouse',
                'type': 'target-snowflake',
                'taps': [{'id': 'orders', 'type': 'tap-postgres'}],
            }],
        }

        with pytest.raises(PreRunChecksException, match='is missing or empty'):
            pipelinewise.cleanup_after_deleted_config(old_config)

        cleanup = json.loads((config_dir / '.deleted-config-cleanup.json').read_text(encoding='utf-8'))
        assert cleanup['taps'][0]['tap_id'] == 'orders'
        assert tap_dir.exists()

    def test_corrupt_deleted_config_cleanup_type_fails_closed(self, tmp_path):
        """A cleanup-kind mismatch cannot turn source cleanup into local deletion."""
        config_dir = tmp_path / 'pipelinewise'
        config_dir.mkdir()
        cleanup_path = config_dir / '.deleted-config-cleanup.json'
        cleanup_path.write_text(json.dumps({
            'version': 1,
            'taps': [{
                'target_id': 'warehouse',
                'tap_id': 'orders',
                'tap_type': 'tap-mysql',
                'source_cleanup': 'none',
                'cleanup_kind': 'postgres_slots',
            }],
            'targets': [],
        }), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)

        with pytest.raises(PreRunChecksException, match='Invalid deleted-config cleanup journal'):
            pipelinewise.cleanup_after_deleted_config({})

        assert cleanup_path.exists()

    def test_duplicate_deleted_config_cleanup_identity_fails_before_mutation(self, tmp_path):
        """Conflicting phases for one identity cannot skip PostgreSQL source cleanup."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        cleanup_path = write_cleanup_journal(config_dir, [
            cleanup_item(),
            cleanup_item(cleanup_kind='local'),
        ])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)

        with (
                patch.object(pipelinewise, '_drop_deleted_postgres_source') as drop_source,
                patch.object(pipelinewise, '_delete_tap_runtime') as delete_runtime,
        ):
            with pytest.raises(PreRunChecksException, match='Invalid deleted-config cleanup journal'):
                pipelinewise.cleanup_after_deleted_config({'targets': []})

        drop_source.assert_not_called()
        delete_runtime.assert_not_called()
        assert tap_dir.exists()
        assert cleanup_path.exists()

    @pytest.mark.parametrize(
        'item',
        [
            {**cleanup_item('tap-mysql'), 'source_cleanup': 'postgres'},
            {**cleanup_item(), 'source_cleanup': 'none'},
            {**cleanup_item('tap-mysql'), 'cleanup_kind': 'postgres_slots'},
        ],
        ids=['mysql-postgres-source', 'postgres-no-source', 'mysql-postgres-phase'],
    )
    def test_inconsistent_deleted_config_cleanup_phase_fails_closed(self, tmp_path, item):
        """Journal source and local phases must agree with the connector type."""
        config_dir = tmp_path / 'pipelinewise'
        cleanup_path = write_cleanup_journal(config_dir, [item])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)

        with pytest.raises(PreRunChecksException, match='Invalid deleted-config cleanup journal'):
            pipelinewise.cleanup_after_deleted_config({'targets': []})

        assert cleanup_path.exists()

    def test_duplicate_deleted_target_cleanup_fails_closed(self, tmp_path):
        """Duplicate target tombstones are malformed durable input."""
        config_dir = tmp_path / 'pipelinewise'
        cleanup_path = write_cleanup_journal(config_dir, [], ['warehouse', 'warehouse'])
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)

        with pytest.raises(PreRunChecksException, match='Invalid deleted-config cleanup journal'):
            pipelinewise.cleanup_after_deleted_config({'targets': []})

        assert cleanup_path.exists()

    def test_malformed_deleted_config_cleanup_json_has_actionable_error(self, tmp_path):
        """Malformed JSON is reported as a journal problem before cleanup runs."""
        config_dir = tmp_path / 'pipelinewise'
        config_dir.mkdir()
        cleanup_path = config_dir / '.deleted-config-cleanup.json'
        cleanup_path.write_text('{not-json', encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)

        with pytest.raises(PreRunChecksException, match='Cannot read deleted-config cleanup journal'):
            pipelinewise.cleanup_after_deleted_config({'targets': []})

        assert cleanup_path.read_text(encoding='utf-8') == '{not-json'

    def test_conflicting_retained_tap_id_keeps_postgres_cleanup_pending(self, tmp_path):
        """A retained config cannot redirect cleanup to another tap's source objects."""
        config_dir = tmp_path / 'pipelinewise'
        tap_dir = config_dir / 'warehouse' / 'orders'
        tap_dir.mkdir(parents=True)
        (tap_dir / 'config.json').write_text(json.dumps({
            'host': 'localhost',
            'tap_id': 'other-orders',
        }), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)
        pipelinewise.config = {'targets': []}
        old_config = project_config([{'id': 'orders', 'type': 'tap-postgres'}])

        with patch('pipelinewise.cli.pipelinewise.FastSyncTapPostgres.drop_slot') as drop_slot:
            with pytest.raises(PreRunChecksException, match='does not match'):
                pipelinewise.cleanup_after_deleted_config(old_config)

        drop_slot.assert_not_called()
        assert read_cleanup_journal(config_dir)['taps'] == [cleanup_item()]
        assert tap_dir.exists()

    @pytest.mark.parametrize(
        ('target_id', 'tap_id'),
        [('../warehouse', 'orders'), ('warehouse', '../orders'), ('.', 'orders'), ('warehouse', 'orders\\old')],
    )
    def test_corrupt_deleted_config_cleanup_path_fails_closed(self, tmp_path, target_id, tap_id):
        """Journal paths cannot escape or alias the configured runtime root."""
        config_dir = tmp_path / 'pipelinewise'
        config_dir.mkdir()
        cleanup_path = config_dir / '.deleted-config-cleanup.json'
        cleanup_path.write_text(json.dumps({
            'version': 1,
            'taps': [{
                'target_id': target_id,
                'tap_id': tap_id,
                'tap_type': 'tap-mysql',
                'source_cleanup': 'none',
                'cleanup_kind': 'local',
            }],
            'targets': [],
        }), encoding='utf-8')
        pipelinewise = PipelineWise(self.args, str(config_dir), VIRTUALENVS_DIR, PROFILING_DIR)

        with pytest.raises(PreRunChecksException, match='Invalid deleted-config cleanup journal'):
            pipelinewise.cleanup_after_deleted_config({})

        assert cleanup_path.exists()
