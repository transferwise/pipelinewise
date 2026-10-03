"""Keep historical PostgreSQL bookmarks attached to their original destination."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pipelinewise.cli.config import Config
from pipelinewise.cli.errors import PreRunChecksException
from pipelinewise.cli.pipelinewise import PipelineWise
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres


def _rename_fixture(tmp_path, target_type='target-postgres'):
    config = Config(str(tmp_path))
    old_files = Config.get_connector_files(str(tmp_path / 'warehouse' / 'Old-id'))
    new_files = Config.get_connector_files(str(tmp_path / 'warehouse' / 'new_id'))
    Path(old_files['config']).parent.mkdir(parents=True)
    source = {'host': 'primary', 'port': 5432, 'dbname': 'source', 'user': 'replicator', 'password': 'old'}
    tap = {
        'id': 'new_id', 'previous_tap_id': 'Old-id', 'type': 'tap-postgres', 'db_conn': source,
        'files': new_files, 'schemas': [{'source_schema': 'public', 'target_schema': 'replicated',
                                       'tables': [{'table_name': 'events', 'replication_method': 'LOG_BASED'}]}],
    }
    target_connection = {'host': 'destination', 'port': 5432, 'dbname': 'warehouse', 'password': 'old'}
    if target_type == 'target-snowflake':
        target_connection = {'account': 'account.region', 'dbname': 'warehouse', 'password': 'old'}
    target = {'id': 'warehouse', 'type': target_type, 'db_conn': target_connection, 'taps': [tap]}
    config.targets = {'warehouse': target}
    files = {
        config.config_path: {'targets': [{'id': 'warehouse', 'type': target_type}]},
        str(tmp_path / 'warehouse' / 'config.json'): target_connection,
        old_files['config']: {**source, 'tap_id': 'Old-id'},
        old_files['state']: {'bookmarks': {'public-events': {'lsn': 123}}},
        old_files['inheritable_config']: config.generate_inheritable_config(tap),
        old_files['selection']: {'selection': Config.generate_selection(tap)},
        old_files['transformation']: {'transformations': Config.generate_transformations(tap)},
    }
    for name, value in files.items():
        Path(name).write_text(json.dumps(value))
    runner = object.__new__(PipelineWise)
    return runner, config, target, tap, old_files, new_files


@pytest.mark.parametrize('existing_state', [False, True])
@pytest.mark.parametrize('change', [
    'source_host', 'source_port', 'source_database', 'source_user', 'target_host', 'target_database',
    'target_port', 'target_connector', 'target_schema', 'default_schema', 'table_format', 'flattening',
    'stream_selection', 'replication_method', 'transformations',
])
def test_rename_rejects_identity_or_data_shape_changes_before_overwriting_state(tmp_path, existing_state, change):
    runner, config, target, tap, _, new_files = _rename_fixture(tmp_path)
    with patch.object(runner, '_validate_postgres_rename_source') as source_check:
        if existing_state:
            runner._preserve_renamed_postgres_state(config, ['new_id'])
            Path(new_files['state']).write_text(json.dumps({'bookmarks': {'public-events': {'lsn': 456}}}))
        source_check.reset_mock()
        changes = {
            'source_host': (tap['db_conn'], 'host', 'another-primary'),
            'source_port': (tap['db_conn'], 'port', 5433),
            'source_database': (tap['db_conn'], 'dbname', 'another-source'),
            'source_user': (tap['db_conn'], 'user', 'another-replicator'),
            'target_host': (target['db_conn'], 'host', 'empty-destination'),
            'target_database': (target['db_conn'], 'dbname', 'empty-warehouse'),
            'target_port': (target['db_conn'], 'port', 5433),
            'target_connector': (target, 'type', 'target-snowflake'),
            'target_schema': (tap['schemas'][0], 'target_schema', 'empty-schema'),
            'default_schema': (tap, 'default_target_schema', 'empty-schema'),
            'table_format': (tap, 'target_table_format', 'iceberg'),
            'flattening': (tap, 'data_flattening_max_level', 2),
            'stream_selection': (tap['schemas'][0]['tables'][0], 'table_name', 'new-events'),
            'replication_method': (tap['schemas'][0]['tables'][0], 'replication_method', 'FULL_TABLE'),
            'transformations': (tap['schemas'][0]['tables'][0], 'transformations',
                                [{'column': 'payload', 'type': 'SET-NULL'}]),
        }
        mapping, key, value = changes[change]
        mapping[key] = value
        with pytest.raises(PreRunChecksException):
            runner._preserve_renamed_postgres_state(config, ['new_id'])
        source_check.assert_not_called()

    if existing_state:
        assert json.loads(Path(new_files['state']).read_text())['bookmarks']['public-events']['lsn'] == 456
    else:
        assert not Path(new_files['state']).exists()


@pytest.mark.parametrize('target_type', ['target-postgres', 'target-snowflake'])
def test_credential_rotation_keeps_existing_progress_and_old_files(tmp_path, target_type):
    runner, config, target, tap, old_files, new_files = _rename_fixture(tmp_path, target_type)
    tap['db_conn']['password'] = 'rotated-source-secret'
    target['db_conn']['password'] = 'rotated-target-secret'
    with patch.object(runner, '_validate_postgres_rename_source') as source_check:
        runner._preserve_renamed_postgres_state(config, ['new_id'])
        source_check.assert_called_once_with({**tap['db_conn'], 'tap_id': 'Old-id'})
        Path(new_files['state']).write_text(json.dumps({'bookmarks': {'public-events': {'lsn': 456}}}))
        runner._preserve_renamed_postgres_state(config, ['new_id'])
        source_check.assert_called_once()
    assert json.loads(Path(new_files['state']).read_text())['bookmarks']['public-events']['lsn'] == 456
    assert json.loads(Path(old_files['state']).read_text())['bookmarks']['public-events']['lsn'] == 123


@pytest.mark.parametrize('key', ['account', 'dbname'])
def test_snowflake_destination_cannot_change_during_rename(tmp_path, key):
    runner, config, target, _, _, new_files = _rename_fixture(tmp_path, 'target-snowflake')
    target['db_conn'][key] = 'another-destination'
    with pytest.raises(PreRunChecksException, match='target connection'):
        runner._preserve_renamed_postgres_state(config, ['new_id'])
    assert not Path(new_files['state']).exists()


@pytest.mark.parametrize('missing', ['inheritable_config', 'selection', 'transformation'])
def test_missing_old_mapping_evidence_prevents_state_adoption(tmp_path, missing):
    runner, config, _, _, old_files, new_files = _rename_fixture(tmp_path)
    Path(old_files[missing]).unlink()
    with pytest.raises(PreRunChecksException):
        runner._preserve_renamed_postgres_state(config, ['new_id'])
    assert not Path(new_files['state']).exists()


@pytest.mark.parametrize('slot', [None, ('other-db', 'wal2json', False), ('source', 'pgoutput', False),
                                  ('source', 'wal2json', True)])
def test_rename_requires_original_inactive_wal2json_slot(slot):
    connection = Mock()
    cursor = Mock()
    connection.cursor.return_value.__enter__ = Mock(return_value=cursor)
    connection.cursor.return_value.__exit__ = Mock(return_value=False)
    cursor.fetchone.return_value = slot
    with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
        with pytest.raises(PreRunChecksException, match='dedicated, inactive wal2json'):
            PipelineWise._validate_postgres_rename_source({'dbname': 'source', 'tap_id': 'Old-id'})
    connection.close.assert_called_once()
