import json
from unittest.mock import MagicMock, call, patch

import psycopg2
import pymysql
import pytest

from pipelinewise.cli import source_identity
from pipelinewise.cli.pipelinewise import PipelineWise
from pipelinewise.fastsync.commons import tap_postgres
from tests.units.cli.cli_args import CliArgs

CONFIG_DIR = 'tests/units/cli/resources/sample_json_config'
VIRTUALENVS_DIR = './virtualenvs-dummy'

PG_CONFIG = {'host': 'pg-new', 'dbname': 'app', 'tap_id': 'tap_pg'}
PG_IDENTITY = {'host': 'pg-new', 'system_identifier': '7300000000000000001', 'timeline_id': 2}
MYSQL_IDENTITY = {'host': 'maria', 'server_id': 2}

pytestmark = pytest.mark.real_source_identity


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
    return slot


def test_postgres_identity_combines_host_system_identifier_and_timeline():
    connection, cursor = _connection(
        fetchone={'system_identifier': '7300000000000000001', 'wal_file': '0000000200000016000000B3'})
    with patch.object(source_identity, 'open_source_connection', return_value=connection) as connect:
        identity = source_identity.read_source_identity('tap-postgres', PG_CONFIG)

    assert identity == PG_IDENTITY
    connect.assert_called_once_with('tap-postgres', PG_CONFIG)
    cursor.execute.assert_called_once_with(tap_postgres.PRIMARY_HISTORY_SQL)
    connection.close.assert_called_once_with()


@pytest.mark.parametrize('old', [
    pytest.param({'host': 'pg-new', 'system_identifier': '7300000000000000001', 'timeline_id': 1},
                 id='promoted-replica-new-timeline'),
    pytest.param({'host': 'pg-sibling', 'system_identifier': '7300000000000000001', 'timeline_id': 2},
                 id='sibling-replica-same-timeline-other-host'),
    pytest.param({'system_identifier': '7300000000000000001', 'timeline_id': 2}, id='identity-without-host'),
])
def test_postgres_state_from_a_related_server_is_refused(old):
    with pytest.raises(source_identity.SourceIdentityError, match='Tap state belongs to source'):
        source_identity.bind_state_to_source({'source_identity': old}, PG_IDENTITY)


@pytest.mark.parametrize('rows, expected', [
    pytest.param([('server_id', '1'), ('server_uuid', '3e11fa47-71ca-11e1-9e33-c80aa9429562')],
                 {'server_uuid': '3e11fa47-71ca-11e1-9e33-c80aa9429562'}, id='mysql'),
    pytest.param([('server_id', '2')], {'host': 'maria', 'server_id': 2}, id='mariadb'),
    pytest.param([('server_id', '2'), ('server_uuid', '')], {'host': 'maria', 'server_id': 2}, id='empty-uuid'),
])
def test_mysql_identity_uses_server_uuid_or_host_and_server_id(rows, expected):
    connection, cursor = _connection(fetchall=rows)
    with patch.object(source_identity, 'open_source_connection', return_value=connection):
        identity = source_identity.read_source_identity('tap-mysql', {'host': 'maria'})

    assert identity == expected
    cursor.execute.assert_called_once_with(source_identity.MYSQL_IDENTITY_SQL)


def test_mariadb_without_server_id_has_no_identity():
    connection, _ = _connection(fetchall=[])
    with patch.object(source_identity, 'open_source_connection', return_value=connection), pytest.raises(
        source_identity.SourceIdentityError, match='server_id'
    ):
        source_identity.read_source_identity('tap-mysql', {'host': 'maria'})


def test_mariadb_server_behind_the_same_host_is_refused():
    """A repointed DNS name keeps the host string, but the server_id of the new server differs."""
    with pytest.raises(source_identity.SourceIdentityError, match='Tap state belongs to source'):
        source_identity.bind_state_to_source(
            {'source_identity': {'host': 'maria', 'server_id': 1}}, {'host': 'maria', 'server_id': 2}
        )


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


def _verify_boundary(boundary, slot, current_lsn='17/0', slot_name='pipelinewise_app_tap_pg'):
    connection, cursor = _connection(fetchone=(current_lsn,))
    with patch.object(source_identity, 'open_source_connection', return_value=connection), patch.object(
        source_identity.FastSyncTapPostgres, 'resolve_slot_name', return_value=slot_name,
    ) as resolve, patch.object(source_identity, 'read_slot', return_value=slot) as read_slot:
        name = source_identity.verify_postgres_slot_covers_boundary(PG_CONFIG, source_identity.parse_lsn(boundary))

    cursor.execute.assert_called_once_with(source_identity.POSTGRES_CURRENT_LSN_SQL)
    resolve.assert_called_once_with(connection, 'app', 'tap_pg')
    read_slot.assert_called_once_with(connection, slot_name)
    connection.close.assert_called_once_with()
    return name


@pytest.mark.parametrize('boundary', ['16/B374D848', '16/B374D849', '17/0'])
def test_slot_at_or_before_boundary_is_accepted(boundary):
    assert _verify_boundary(boundary, _slot()) == 'pipelinewise_app_tap_pg'


def test_slot_name_is_resolved_like_fastsync():
    """reset_state checks the slot FastSync and the tap use, legacy name included."""
    assert _verify_boundary('17/0', _slot(slot_name='pipelinewise_app'), slot_name='pipelinewise_app') == (
        'pipelinewise_app'
    )


def test_boundary_ahead_of_the_primary_is_refused():
    """A typo, a value from another server or a deliberately high value cannot pass the slot check."""
    with pytest.raises(source_identity.SourceIdentityError, match='ahead of the current WAL position 16/B374D848'):
        _verify_boundary('16/B374D849', _slot(confirmed_flush_lsn='16/0'), current_lsn='16/B374D848')


@pytest.mark.parametrize('slot, message', [
    (None, 'does not exist'),
    (_slot(confirmed_flush_lsn='16/B374D849'), 'after the boundary'),
    (_slot(confirmed_flush_lsn=None, active_pid=42), 'is not ready'),
    (_slot(active_pid=42), 'is not ready'),
    (_slot(database='other'), 'does not belong to this tap'),
    (_slot(plugin='pgoutput'), 'does not belong to this tap'),
    (_slot(slot_type='physical'), 'does not belong to this tap'),
    (_slot(temporary=True), 'does not belong to this tap'),
])
def test_slot_that_cannot_cover_the_boundary_is_refused(slot, message):
    with pytest.raises(source_identity.SourceIdentityError, match=message):
        _verify_boundary('16/B374D848', slot)


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


def _saved_state(tmp_path):
    return json.loads((tmp_path / 'state.json').read_text())


def test_tap_records_identity_in_state_without_log_positions(tmp_path):
    pipelinewise = _pipelinewise(tmp_path, state={'bookmarks': {'public-one': {'replication_key_value': 3}}})
    with patch.object(source_identity, 'read_source_identity', return_value=PG_IDENTITY) as identity:
        assert pipelinewise._check_state_source() == PG_IDENTITY

    identity.assert_called_once_with('tap-postgres', PG_CONFIG)
    assert _saved_state(tmp_path)['source_identity'] == PG_IDENTITY
    pipelinewise.logger.warning.assert_not_called()


@pytest.mark.parametrize('bookmark', [{'lsn': 10}, {'log_file': 'mysql-bin-changelog.000002', 'log_pos': 4}])
def test_first_identity_over_existing_log_positions_is_recorded_with_a_warning(tmp_path, bookmark):
    """State from before this release is trusted once; the warning makes that event auditable."""
    pipelinewise = _pipelinewise(tmp_path, state={'bookmarks': {'public-one': bookmark}})
    with patch.object(source_identity, 'read_source_identity', return_value=PG_IDENTITY):
        pipelinewise._check_state_source()

    assert _saved_state(tmp_path) == {'bookmarks': {'public-one': bookmark}, 'source_identity': PG_IDENTITY}
    pipelinewise.logger.warning.assert_called_once()
    assert 'already holds log positions' in pipelinewise.logger.warning.call_args.args[0]


def test_tap_refuses_to_start_on_another_source(tmp_path):
    state = {'bookmarks': {'public-one': {'lsn': 10}},
             'source_identity': dict(PG_IDENTITY, timeline_id=1)}
    pipelinewise = _pipelinewise(tmp_path, state=state)
    with patch.object(source_identity, 'read_source_identity', return_value=PG_IDENTITY), pytest.raises(
        source_identity.SourceIdentityError
    ):
        pipelinewise._check_state_source()

    assert _saved_state(tmp_path) == state


@pytest.mark.parametrize('state_text', [None, ''])
def test_tap_without_state_defers_reading_identity(tmp_path, state_text):
    pipelinewise = _pipelinewise(tmp_path)
    if state_text is not None:
        (tmp_path / 'state.json').write_text(state_text)
    with patch.object(source_identity, 'read_source_identity') as identity:
        assert pipelinewise._check_state_source() is None
    identity.assert_not_called()


def test_record_adds_identity_to_state_created_by_this_run(tmp_path):
    pipelinewise = _pipelinewise(tmp_path, state={'bookmarks': {'public-one': {'lsn': 10}}})
    with patch.object(source_identity, 'read_source_identity', return_value=PG_IDENTITY):
        pipelinewise._record_state_source()

    assert _saved_state(tmp_path)['source_identity'] == PG_IDENTITY
    pipelinewise.logger.warning.assert_not_called()


@pytest.mark.parametrize('state', [None, {'bookmarks': {}, 'source_identity': PG_IDENTITY}])
def test_record_skips_missing_or_bound_state(tmp_path, state):
    pipelinewise = _pipelinewise(tmp_path, state=state)
    with patch.object(source_identity, 'read_source_identity') as identity:
        pipelinewise._record_state_source()
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
        assert pipelinewise._check_state_source() is None
        pipelinewise._record_state_source()
    identity.assert_not_called()


def test_slot_reset_clears_identity_with_bookmarks(tmp_path):
    state = {'bookmarks': {'public-one': {'lsn': 10}}, 'currently_syncing': None, 'source_identity': PG_IDENTITY}
    pipelinewise = _pipelinewise(tmp_path, state=state)

    backup = pipelinewise._clear_tap_bookmarks_before_source_slot_reset()

    assert _saved_state(tmp_path) == {'bookmarks': {}, 'currently_syncing': None}
    assert json.loads(open(backup, encoding='utf-8').read()) == state


def _fast_sync_instance(tmp_path, tables=None, state=None):
    instance = PipelineWise(CliArgs(target='target_one', tap='tap_one', tables=tables), CONFIG_DIR, VIRTUALENVS_DIR)
    instance.tap['type'] = 'tap-postgres'
    instance.tap['files']['state'] = str(tmp_path / 'state.json')
    instance.tap['files']['properties'] = str(tmp_path / 'properties.json')
    (tmp_path / 'properties.json').write_text(json.dumps({'streams': [
        {'stream': name, 'metadata': [{'breadcrumb': [], 'metadata': {
            'schema-name': 'db_test_mysql', 'selected': True,
        }}]}
        for name in ('table_one', 'table_two')
    ]}))
    if state is not None:
        (tmp_path / 'state.json').write_text(json.dumps(state))
    return instance


def _run_fast_sync(instance, worker_state=None):
    """Run the fast_sync command with the real identity check, recording source and worker events."""
    calls = MagicMock()
    state_path = instance.tap['files']['state']

    def start():
        calls.start()
        if worker_state is not None:
            with open(state_path, 'w', encoding='utf-8') as state_file:
                json.dump(worker_state, state_file)

    def reset_slot(_config, *, before_reset):
        calls.reset_slot()
        before_reset()

    with patch('pipelinewise.cli.pipelinewise.pidfile.PIDFile'), patch(
        'pipelinewise.cli.pipelinewise.Process',
    ) as process, patch.object(
        source_identity, 'read_source_identity', return_value=PG_IDENTITY,
    ) as read_identity, patch.object(
        instance, '_preflight_source_slot_reset', return_value=({'tap': 'config'}, {}),
    ), patch(
        'pipelinewise.cli.pipelinewise.FastSyncTapPostgres.reset_slot', side_effect=reset_slot,
    ):
        process.return_value.start.side_effect = start
        process.return_value.exception = None
        process.return_value.exitcode = 0
        calls.attach_mock(read_identity, 'read_identity')
        try:
            instance.fast_sync()
        finally:
            events = [entry[0] for entry in calls.mock_calls]
    return events


LOG_POSITION = {'db_test_mysql-table_one': {'lsn': 10}}


def test_filtered_fast_sync_refuses_another_source_before_any_worker(tmp_path):
    state = {'bookmarks': LOG_POSITION, 'source_identity': dict(PG_IDENTITY, timeline_id=1)}
    instance = _fast_sync_instance(tmp_path, tables='db_test_mysql.table_one', state=state)

    with patch('pipelinewise.cli.pipelinewise.Process') as process, patch(
        'pipelinewise.cli.pipelinewise.FastSyncTapPostgres.reset_slot',
    ) as reset_slot, patch('pipelinewise.cli.pipelinewise.pidfile.PIDFile'), patch.object(
        source_identity, 'read_source_identity', return_value=PG_IDENTITY,
    ), pytest.raises(SystemExit) as exit_info:
        instance.fast_sync()

    assert exit_info.value.code == 1
    process.assert_not_called()
    reset_slot.assert_not_called()
    assert _saved_state(tmp_path) == state


def test_filtered_fast_sync_checks_identity_before_the_first_worker(tmp_path):
    state = {'bookmarks': LOG_POSITION, 'source_identity': PG_IDENTITY}
    instance = _fast_sync_instance(tmp_path, tables='db_test_mysql.table_one', state=state)

    events = _run_fast_sync(instance)

    assert events == ['read_identity', 'start']


def test_first_fast_sync_records_identity_once_workers_created_the_state(tmp_path):
    worker_state = {'bookmarks': LOG_POSITION}
    instance = _fast_sync_instance(tmp_path, tables='db_test_mysql.table_one')

    events = _run_fast_sync(instance, worker_state=worker_state)

    assert events == ['start', 'read_identity']
    assert _saved_state(tmp_path) == dict(worker_state, source_identity=PG_IDENTITY)


def test_unfiltered_reset_clears_identity_then_records_the_connected_source(tmp_path):
    """After a switchover, an unfiltered resync is the recovery path, so the old identity must not block it."""
    state = {'bookmarks': LOG_POSITION, 'currently_syncing': None, 'source_identity': dict(PG_IDENTITY, timeline_id=1)}
    instance = _fast_sync_instance(tmp_path, state=state)

    events = _run_fast_sync(instance)

    assert events[:3] == ['reset_slot', 'read_identity', 'start']
    assert _saved_state(tmp_path) == {'bookmarks': {}, 'currently_syncing': None, 'source_identity': PG_IDENTITY}


def _run_tap_with_identity(tmp_path, check_side_effect):
    instance = PipelineWise(CliArgs(target='target_one', tap='tap_one'), CONFIG_DIR, VIRTUALENVS_DIR)
    properties = tmp_path / 'properties.json'
    properties.write_text('{"streams": []}', encoding='utf-8')
    calls = MagicMock()
    with patch('pipelinewise.cli.pipelinewise.pidfile.PIDFile'), patch.object(
        instance, 'create_filtered_tap_properties',
        return_value=(str(properties), ['db-one'], str(properties), ['db-two']),
    ), patch.object(instance, '_check_state_source', side_effect=check_side_effect) as check, patch.object(
        instance, '_record_state_source',
    ) as record, patch.object(
        instance, '_convert_native_tables_to_iceberg',
    ) as convert, patch.object(instance, 'do_sync_tables') as do_sync_tables, patch.object(
        instance, 'run_tap_singer',
    ) as run_tap_singer, patch.object(instance, 'send_alert') as send_alert, patch.object(
        instance, '_print_tap_run_summary',
    ):
        calls.attach_mock(check, 'check')
        calls.attach_mock(record, 'record')
        calls.attach_mock(convert, 'convert')
        calls.attach_mock(do_sync_tables, 'fastsync')
        calls.attach_mock(run_tap_singer, 'singer')
        try:
            instance.run_tap()
        except SystemExit as exc:
            return calls.mock_calls, send_alert, exc.code
        except Exception as exc:
            return calls.mock_calls, send_alert, exc
    return calls.mock_calls, send_alert, None


def test_run_tap_checks_identity_before_any_writer_and_records_after(tmp_path):
    calls, send_alert, exit_code = _run_tap_with_identity(tmp_path, [PG_IDENTITY])

    assert exit_code is None
    assert [entry[0] for entry in calls] == ['check', 'convert', 'fastsync', 'singer', 'record']
    assert calls[2] == call.fastsync(['db-one'], checked_identity=PG_IDENTITY)
    send_alert.assert_not_called()


def test_run_tap_refuses_to_start_on_identity_mismatch(tmp_path):
    error = source_identity.SourceIdentityError('Tap state belongs to source')
    calls, send_alert, exit_code = _run_tap_with_identity(tmp_path, error)

    assert exit_code == 1
    assert [entry[0] for entry in calls] == ['check']
    send_alert.assert_called_once_with(message='tap_one tap failed: source does not match tap state', exc=error)


@pytest.mark.parametrize('error', [
    psycopg2.OperationalError('could not connect'),
    pymysql.err.OperationalError(2003, 'refused'),
])
def test_run_tap_starts_no_writer_when_the_identity_cannot_be_read(tmp_path, error):
    calls, send_alert, raised = _run_tap_with_identity(tmp_path, error)

    assert raised is error
    assert [entry[0] for entry in calls] == ['check']
    send_alert.assert_called_once_with(message='tap_one tap failed', exc=error)
