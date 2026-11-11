import json
from unittest.mock import MagicMock, patch

import pytest

from pipelinewise.cli import source_identity
from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs

CONFIG_DIR = 'tests/units/cli/resources/sample_json_config'
VIRTUALENVS_DIR = './virtualenvs-dummy'

PG_CONFIG = {'host': 'pg-new', 'dbname': 'app', 'tap_id': 'tap_pg'}
PG_IDENTITY = {'system_identifier': '7300000000000000001', 'timeline_id': 2}


def _connection(fetchone=None, fetchall=None):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = fetchone
    cursor.fetchall.return_value = fetchall
    return connection, cursor


def _slot(**overrides):
    slot = {
        'slot_name': 'pipelinewise_app_tap_pg', 'database': 'app', 'plugin': 'wal2json', 'slot_type': 'logical',
        'temporary': False, 'active_pid': None, 'confirmed_flush_lsn': '16/B374D848',
    }
    slot.update(overrides)
    return tuple(slot.values())


def test_postgres_identity_combines_system_identifier_and_timeline():
    connection, cursor = _connection(fetchone=('7300000000000000001', '0000000200000016000000B3'))
    with patch.object(source_identity, 'open_source_connection', return_value=connection) as connect:
        identity = source_identity.read_source_identity('tap-postgres', PG_CONFIG)

    assert identity == PG_IDENTITY
    connect.assert_called_once_with('tap-postgres', PG_CONFIG)
    cursor.execute.assert_called_once_with(source_identity.POSTGRES_IDENTITY_SQL)
    connection.close.assert_called_once_with()


def test_promoted_replica_has_a_different_identity_from_its_source():
    """A promoted replica keeps the system identifier but not the timeline."""
    old = {'system_identifier': '7300000000000000001', 'timeline_id': 1}
    with pytest.raises(source_identity.SourceIdentityError, match='Tap state belongs to source'):
        source_identity.bind_state_to_source({'source_identity': old}, PG_IDENTITY)


@pytest.mark.parametrize('row, expected', [
    (('server_uuid', '3e11fa47-71ca-11e1-9e33-c80aa9429562'), {'server_uuid': '3e11fa47-71ca-11e1-9e33-c80aa9429562'}),
    (None, {'host': 'maria-new'}),
    (('server_uuid', ''), {'host': 'maria-new'}),
])
def test_mysql_identity_uses_server_uuid_or_host(row, expected):
    connection, cursor = _connection(fetchone=row)
    with patch.object(source_identity, 'open_source_connection', return_value=connection):
        identity = source_identity.read_source_identity('tap-mysql', {'host': 'maria-new'})

    assert identity == expected
    cursor.execute.assert_called_once_with(source_identity.MYSQL_SERVER_UUID_SQL)


def test_other_taps_have_no_identity_check():
    with patch.object(source_identity, 'open_source_connection') as connect:
        assert source_identity.read_source_identity('tap-yugabyte', {}) is None
    connect.assert_not_called()


def test_bind_records_identity_once_then_accepts_the_same_source():
    state = {'bookmarks': {}}
    assert source_identity.bind_state_to_source(state, PG_IDENTITY) is True
    assert state['source_identity'] == PG_IDENTITY
    assert source_identity.bind_state_to_source(state, dict(PG_IDENTITY)) is False


@pytest.mark.parametrize('value', [None, '', '12345', '0/', 'G/1', '1/2/3', '123456789/0'])
def test_parse_lsn_rejects_malformed_positions(value):
    with pytest.raises(source_identity.SourceIdentityError, match='Invalid PostgreSQL LSN'):
        source_identity.parse_lsn(value)


def test_parse_lsn_accepts_x_y_form():
    assert source_identity.parse_lsn('16/b374d848') == (0x16 << 32) + 0xB374D848


@pytest.mark.parametrize('boundary', ['16/B374D848', '16/B374D849'])
def test_slot_at_or_before_boundary_is_accepted(boundary):
    connection, cursor = _connection(fetchall=[_slot()])
    with patch.object(source_identity, 'open_source_connection', return_value=connection):
        name = source_identity.verify_postgres_slot_covers_boundary(PG_CONFIG, source_identity.parse_lsn(boundary))

    assert name == 'pipelinewise_app_tap_pg'
    cursor.execute.assert_called_once_with(
        source_identity.POSTGRES_SLOTS_SQL, ('pipelinewise_app', 'pipelinewise_app_tap_pg'))


def test_legacy_slot_is_preferred_like_the_tap():
    connection, _ = _connection(fetchall=[_slot(), _slot(slot_name='pipelinewise_app')])
    with patch.object(source_identity, 'open_source_connection', return_value=connection):
        name = source_identity.verify_postgres_slot_covers_boundary(PG_CONFIG, source_identity.parse_lsn('17/0'))

    assert name == 'pipelinewise_app'


@pytest.mark.parametrize('slots, message', [
    ([], 'does not exist'),
    ([_slot(confirmed_flush_lsn='16/B374D849')], 'after the boundary'),
    ([_slot(confirmed_flush_lsn=None, active_pid=42)], 'is not ready'),
    ([_slot(active_pid=42)], 'is not ready'),
    ([_slot(database='other')], 'does not belong to this tap'),
    ([_slot(plugin='pgoutput')], 'does not belong to this tap'),
    ([_slot(slot_type='physical')], 'does not belong to this tap'),
    ([_slot(temporary=True)], 'does not belong to this tap'),
])
def test_slot_that_cannot_cover_the_boundary_is_refused(slots, message):
    connection, _ = _connection(fetchall=slots)
    with patch.object(source_identity, 'open_source_connection', return_value=connection), pytest.raises(
        source_identity.SourceIdentityError, match=message
    ):
        source_identity.verify_postgres_slot_covers_boundary(PG_CONFIG, source_identity.parse_lsn('16/B374D848'))


def _pipelinewise(tmp_path, tap_type='tap-postgres', state=None, replication_method='LOG_BASED'):
    pipelinewise = PipelineWise.__new__(PipelineWise)
    pipelinewise.logger = MagicMock()
    pipelinewise.LOG_BASED = PipelineWise.LOG_BASED
    files = {name: str(tmp_path / f'{name}.json') for name in ('state', 'selection', 'config')}
    pipelinewise.tap = {'id': 'tap_pg', 'type': tap_type, 'files': files}
    (tmp_path / 'selection.json').write_text(json.dumps(
        {'selection': [{'tap_stream_id': 'public-one', 'replication_method': replication_method}]}))
    (tmp_path / 'config.json').write_text(json.dumps(PG_CONFIG))
    if state is not None:
        (tmp_path / 'state.json').write_text(json.dumps(state))
    return pipelinewise


def test_tap_records_identity_in_existing_state(tmp_path):
    pipelinewise = _pipelinewise(tmp_path, state={'bookmarks': {'public-one': {'lsn': 10}}})
    with patch.object(source_identity, 'read_source_identity', return_value=PG_IDENTITY) as identity:
        assert pipelinewise._bind_state_to_source() is True

    identity.assert_called_once_with('tap-postgres', PG_CONFIG)
    saved = json.loads((tmp_path / 'state.json').read_text())
    assert saved == {'bookmarks': {'public-one': {'lsn': 10}}, 'source_identity': PG_IDENTITY}


def test_tap_refuses_to_start_on_another_source(tmp_path):
    state = {'bookmarks': {'public-one': {'lsn': 10}},
             'source_identity': {'system_identifier': '7300000000000000001', 'timeline_id': 1}}
    pipelinewise = _pipelinewise(tmp_path, state=state)
    with patch.object(source_identity, 'read_source_identity', return_value=PG_IDENTITY), pytest.raises(
        source_identity.SourceIdentityError
    ):
        pipelinewise._bind_state_to_source()

    assert json.loads((tmp_path / 'state.json').read_text()) == state


@pytest.mark.parametrize('state_text', [None, ''])
def test_tap_without_state_defers_recording(tmp_path, state_text):
    pipelinewise = _pipelinewise(tmp_path)
    if state_text is not None:
        (tmp_path / 'state.json').write_text(state_text)
    with patch.object(source_identity, 'read_source_identity') as identity:
        assert pipelinewise._bind_state_to_source() is False
    identity.assert_not_called()


@pytest.mark.parametrize('tap_type, replication_method', [
    ('tap-postgres', 'INCREMENTAL'),
    ('tap-yugabyte', 'LOG_BASED'),
    ('tap-mongodb', 'LOG_BASED'),
])
def test_identity_is_checked_only_for_log_based_postgres_and_mysql(tmp_path, tap_type, replication_method):
    pipelinewise = _pipelinewise(tmp_path, tap_type=tap_type, state={'bookmarks': {}},
                                 replication_method=replication_method)
    with patch.object(source_identity, 'read_source_identity') as identity:
        assert pipelinewise._bind_state_to_source() is True
    identity.assert_not_called()


def test_slot_reset_clears_identity_with_bookmarks(tmp_path):
    state = {'bookmarks': {'public-one': {'lsn': 10}}, 'currently_syncing': None, 'source_identity': PG_IDENTITY}
    pipelinewise = _pipelinewise(tmp_path, state=state)

    backup = pipelinewise._clear_tap_bookmarks_before_source_slot_reset()

    assert json.loads((tmp_path / 'state.json').read_text()) == {'bookmarks': {}, 'currently_syncing': None}
    assert json.loads(open(backup, encoding='utf-8').read()) == state


def _run_tap_with_identity(tmp_path, bind_side_effect):
    instance = PipelineWise(CliArgs(target='target_one', tap='tap_one'), CONFIG_DIR, VIRTUALENVS_DIR)
    properties = tmp_path / 'properties.json'
    properties.write_text('{"streams": []}', encoding='utf-8')
    calls = MagicMock()
    with patch('pipelinewise.cli.pipelinewise.pidfile.PIDFile'), patch.object(
        instance, 'create_filtered_tap_properties',
        return_value=(str(properties), ['db-one'], str(properties), ['db-two']),
    ), patch.object(instance, '_bind_state_to_source', side_effect=bind_side_effect) as bind, patch.object(
        instance, '_convert_native_tables_to_iceberg',
    ) as convert, patch.object(instance, 'do_sync_tables') as do_sync_tables, patch.object(
        instance, 'run_tap_singer',
    ) as run_tap_singer, patch.object(instance, 'send_alert') as send_alert, patch.object(
        instance, '_print_tap_run_summary',
    ):
        calls.attach_mock(bind, 'bind')
        calls.attach_mock(convert, 'convert')
        calls.attach_mock(do_sync_tables, 'fastsync')
        calls.attach_mock(run_tap_singer, 'singer')
        try:
            instance.run_tap()
        except SystemExit as exc:
            return [entry[0] for entry in calls.mock_calls], send_alert, exc.code
    return [entry[0] for entry in calls.mock_calls], send_alert, None


def test_run_tap_checks_identity_before_any_writer(tmp_path):
    calls, send_alert, exit_code = _run_tap_with_identity(tmp_path, [True])

    assert exit_code is None
    assert calls == ['bind', 'convert', 'fastsync', 'singer']
    send_alert.assert_not_called()


def test_run_tap_binds_after_first_fastsync_creates_state(tmp_path):
    calls, _, _ = _run_tap_with_identity(tmp_path, [False, True])

    assert calls == ['bind', 'convert', 'fastsync', 'bind', 'singer']


def test_run_tap_refuses_to_start_on_identity_mismatch(tmp_path):
    error = source_identity.SourceIdentityError('Tap state belongs to source')
    calls, send_alert, exit_code = _run_tap_with_identity(tmp_path, error)

    assert exit_code == 1
    assert calls == ['bind']
    send_alert.assert_called_once_with(message='tap_one tap failed: source does not match tap state', exc=error)
