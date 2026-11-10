"""Durability, terminal pipe EOF, and cleanup regressions for managed final feedback."""

import json
import signal
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, call

import psycopg2
import pytest

import tap_postgres
from tap_postgres import final_feedback as feedback


def state_at(**stream_lsns):
    return {'bookmarks': {stream: {'lsn': lsn} for stream, lsn in stream_lsns.items()}}


@pytest.fixture
def managed_session(tmp_path, monkeypatch):
    state_file = tmp_path / 'state.json'
    state_file.write_text(json.dumps(state_at(first=200, second=200)), encoding='utf-8')
    coordinator = feedback.FinalFeedbackCoordinator(str(state_file))
    conn, cursor, metadata_conn, metadata_cursor = (Mock() for _ in range(4))
    cursor.read_message.return_value = None
    metadata_conn.cursor.return_value.__enter__ = Mock(return_value=metadata_cursor)
    metadata_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    metadata_cursor.fetchone.return_value = (True,)
    monkeypatch.setattr(feedback.post_db, 'open_connection', Mock(return_value=metadata_conn))
    monkeypatch.setattr(feedback, 'seal_singer_output', Mock())
    monkeypatch.setattr(feedback, 'select', Mock(return_value=([], [], [])))
    streams = [{'tap_stream_id': stream} for stream in ('first', 'second')]
    config = {'dbname': 'first_database'}
    coordinator.retain(conn, cursor, config, streams, 200, 'slot')
    return SimpleNamespace(coordinator=coordinator, state_file=state_file, conn=conn, cursor=cursor,
                           metadata_conn=metadata_conn, metadata_cursor=metadata_cursor,
                           config=config, streams=streams)


def test_waits_for_every_stream_before_capped_feedback_and_server_confirmation(managed_session):
    session = managed_session
    session.state_file.write_text(json.dumps(state_at(first=900, second=199)), encoding='utf-8')
    events = []

    def target_finishes(*_):
        session.cursor.send_feedback.assert_not_called()
        session.state_file.write_text(json.dumps(state_at(first=900, second=800)), encoding='utf-8')
        events.append('target_durable')

    feedback.select.side_effect = target_finishes
    session.cursor.send_feedback.side_effect = lambda **_: events.append('feedback')
    session.metadata_cursor.fetchone.side_effect = lambda: (events.append('server_confirmed') is None,)
    session.conn.close.side_effect = lambda: events.append('closed')

    session.coordinator.finish()

    assert events == ['target_durable', 'feedback', 'server_confirmed', 'closed']
    session.cursor.send_feedback.assert_called_once_with(write_lsn=200, flush_lsn=200, reply=True, force=True)
    assert session.metadata_conn.autocommit is True
    assert session.metadata_cursor.execute.call_args == call(
        'SELECT confirmed_flush_lsn >= %s::pg_lsn FROM pg_replication_slots WHERE slot_name = %s', ('0/C8', 'slot'))
    session.cursor.close.assert_called_once_with()
    session.metadata_conn.close.assert_called_once_with()


@pytest.mark.parametrize('invalid_state', [None, '{', '\udcff', '{}',
                                         json.dumps(state_at(first=200)),
                                         json.dumps(state_at(first=200, second=True)),
                                         json.dumps(state_at(first=200, second=-1)),
                                         json.dumps(state_at(first=200, second='200')),
                                         json.dumps(state_at(first=200, second=199))])
def test_missing_corrupt_or_stale_ack_times_out_without_feedback(managed_session, monkeypatch, invalid_state):
    session = managed_session
    if invalid_state is None:
        session.state_file.unlink()
    else:
        session.state_file.write_bytes(invalid_state.encode('utf-8', errors='surrogateescape'))
    session.coordinator.timeout_seconds = 3
    monkeypatch.setattr(feedback.time, 'monotonic', Mock(side_effect=range(10)))

    with pytest.raises(feedback.FinalFeedbackError, match='Timed out'):
        session.coordinator.finish()

    session.cursor.send_feedback.assert_not_called()
    feedback.post_db.open_connection.assert_not_called()
    session.conn.close.assert_called_once_with()
    session.cursor.close.assert_called_once_with()


@pytest.mark.parametrize('failure_point', ['send', 'connect', 'query'])
def test_postgres_failure_is_sanitized_and_closes_all_sessions(managed_session, failure_point):
    session = managed_session
    failure = psycopg2.OperationalError('private connection or source value')
    operations = {'send': session.cursor.send_feedback,
                  'connect': feedback.post_db.open_connection, 'query': session.metadata_cursor.execute}
    operations[failure_point].side_effect = failure
    other_conn, other_cursor = Mock(), Mock()
    session.coordinator.retain(other_conn, other_cursor, {}, session.streams, 200, 'second_slot')

    with pytest.raises(feedback.FinalFeedbackError, match='Connection failed') as error:
        session.coordinator.finish()

    assert 'private' not in str(error.value)
    session.conn.close.assert_called_once_with()
    other_conn.close.assert_called_once_with()
    other_cursor.close.assert_called_once_with()


def test_missing_server_slot_fails_without_claiming_confirmation(managed_session):
    session = managed_session
    session.metadata_cursor.fetchone.return_value = None
    with pytest.raises(feedback.FinalFeedbackError, match='slot is missing'):
        session.coordinator.finish()
    session.conn.close.assert_called_once_with()


def test_final_feedback_does_not_read_driver_keepalives_that_can_promote_flush_lsn(managed_session):
    session = managed_session
    session.cursor.read_message.side_effect = AssertionError('read after completed marker')
    session.metadata_cursor.fetchone.side_effect = [(False,), (True,)]
    session.coordinator.finish()
    session.cursor.read_message.assert_not_called()
    session.cursor.send_feedback.assert_called_once_with(write_lsn=200, flush_lsn=200, reply=True, force=True)
    feedback.select.assert_called_once_with([], [], [], feedback.POLL_INTERVAL_SECONDS)


@pytest.mark.parametrize('heartbeat_fails', [False, True])
def test_waiting_for_target_sends_only_nonadvancing_status_without_reading_wal(
        managed_session, monkeypatch, heartbeat_fails):
    session = managed_session
    session.state_file.write_text(json.dumps(state_at(first=100, second=100)), encoding='utf-8')
    session.cursor.read_message.side_effect = AssertionError('must not consume future WAL')
    clock = [0.0]
    monkeypatch.setattr(feedback.time, 'monotonic', lambda: clock[0])
    heartbeat_calls = []
    original_callback = feedback.extensions.get_wait_callback()

    def send_feedback(**values):
        heartbeat_calls.append(values)
        if values == {'force': True}:
            assert feedback.extensions.get_wait_callback() is original_callback
            if heartbeat_fails:
                raise psycopg2.OperationalError('heartbeat connection failure')
            session.state_file.write_text(json.dumps(state_at(first=300, second=400)), encoding='utf-8')

    def wait(*_):
        clock[0] += feedback.STATUS_INTERVAL_SECONDS

    session.cursor.send_feedback.side_effect = send_feedback
    feedback.select.side_effect = wait
    if heartbeat_fails:
        with pytest.raises(feedback.FinalFeedbackError, match='Connection failed'):
            session.coordinator.finish()
        assert heartbeat_calls == [{'force': True}]
        feedback.post_db.open_connection.assert_not_called()
    else:
        session.coordinator.finish()
        assert heartbeat_calls == [{'force': True}, {'write_lsn': 200, 'flush_lsn': 200, 'reply': True, 'force': True}]
    session.cursor.read_message.assert_not_called()
    session.conn.close.assert_called_once_with()


@pytest.mark.parametrize('fail', [False, True])
def test_metadata_wait_callback_is_restored_after_success_or_error(managed_session, fail):
    session = managed_session
    original_callback = feedback.extensions.get_wait_callback()
    callback_seen = []

    def execute(*_):
        callback_seen.append(feedback.extensions.get_wait_callback())
        if fail:
            raise psycopg2.OperationalError('metadata failure')

    session.metadata_cursor.execute.side_effect = execute
    if fail:
        with pytest.raises(feedback.FinalFeedbackError, match='Connection failed'):
            session.coordinator.finish()
    else:
        session.coordinator.finish()
    assert callback_seen
    assert all(callback is not original_callback for callback in callback_seen)
    assert feedback.extensions.get_wait_callback() is original_callback


@pytest.mark.parametrize('poll_state', [feedback.extensions.POLL_READ, feedback.extensions.POLL_WRITE])
def test_blackholed_metadata_socket_has_monotonic_deadline_and_restores_callback(
        managed_session, monkeypatch, poll_state):
    session = managed_session
    conn = Mock()
    conn.poll.return_value = poll_state
    original_callback = feedback.extensions.get_wait_callback()
    monkeypatch.setattr(feedback.time, 'monotonic', Mock(side_effect=[1, 2, 3]))

    with pytest.raises(feedback.FinalFeedbackError, match='Timed out'), session.coordinator._metadata_deadline(3):
        feedback.extensions.get_wait_callback()(conn)

    assert feedback.extensions.get_wait_callback() is original_callback
    assert conn.poll.call_count == 2
    readers = [conn] if poll_state == feedback.extensions.POLL_READ else []
    writers = [conn] if poll_state == feedback.extensions.POLL_WRITE else []
    assert feedback.select.call_args_list == [call(readers, writers, [], 2), call(readers, writers, [], 1)]


def test_server_confirmation_must_arrive_before_deadline(managed_session, monkeypatch):
    session = managed_session
    session.metadata_cursor.fetchone.return_value = (False,)
    session.coordinator.timeout_seconds = 6
    monkeypatch.setattr(feedback.time, 'monotonic', Mock(side_effect=range(20)))
    with pytest.raises(feedback.FinalFeedbackError, match='Timed out'):
        session.coordinator.finish()
    session.cursor.send_feedback.assert_called_once()
    session.metadata_conn.close.assert_called_once()


def test_snapshots_database_groups_and_seals_once(managed_session):
    session = managed_session
    session.config['dbname'] = 'second_database'
    other_conn, other_cursor = Mock(), Mock()
    other_cursor.read_message.return_value = None
    session.coordinator.retain(other_conn, other_cursor, session.config, session.streams, 200, 'second_slot')
    assert [item.conn_info['dbname'] for item in session.coordinator.sessions] == [
        'first_database', 'second_database']
    session.coordinator.finish()
    session.coordinator.finish()
    feedback.seal_singer_output.assert_called_once_with()
    assert [entry.args[0]['dbname'] for entry in feedback.post_db.open_connection.call_args_list] == [
        'first_database', 'second_database']
    other_conn.close.assert_called_once_with()


def test_empty_coordinator_does_not_close_output(monkeypatch):
    seal = Mock()
    monkeypatch.setattr(feedback, 'seal_singer_output', seal)
    feedback.FinalFeedbackCoordinator('state.json').finish()
    seal.assert_not_called()


def test_environment_opt_in_requires_state_file(monkeypatch):
    monkeypatch.delenv(feedback.FINAL_FEEDBACK_ENV, raising=False)
    assert feedback.FinalFeedbackCoordinator.from_environment(None) is None
    monkeypatch.setenv(feedback.FINAL_FEEDBACK_ENV, 'true')
    assert feedback.FinalFeedbackCoordinator.from_environment(None) is None
    monkeypatch.setenv(feedback.FINAL_FEEDBACK_ENV, '1')
    with pytest.raises(feedback.FinalFeedbackError, match='requires a target state file'):
        feedback.FinalFeedbackCoordinator.from_environment(None)


def test_unsupported_stdout_fails_without_touching_fd_one(monkeypatch):
    monkeypatch.setattr(feedback.sys, 'stdout', SimpleNamespace(fileno=lambda: 42))
    dup2 = Mock()
    monkeypatch.setattr(feedback.os, 'dup2', dup2)
    with pytest.raises(feedback.FinalFeedbackError, match='OS pipe descriptor 1'):
        feedback.seal_singer_output()
    dup2.assert_not_called()


CHILD_PROCESS = '''
import json
import os
import sys
from types import SimpleNamespace
from tap_postgres.final_feedback import FinalFeedbackCoordinator

state_path, feedback_path = sys.argv[1:]
read_fd, write_fd = os.pipe()

class Cursor:
    def fileno(self):
        return read_fd
    def read_message(self):
        return None
    def send_feedback(self, **values):
        with open(feedback_path, 'w') as handle:
            json.dump(values, handle)
    def close(self):
        os.close(read_fd)
        os.close(write_fd)

coordinator = FinalFeedbackCoordinator(state_path, timeout_seconds=15)
coordinator.retain(SimpleNamespace(close=lambda: None), Cursor(), {}, [{'tap_stream_id': 'first'}], 200, 'slot')
coordinator._confirm_receipt = lambda *_: True
print(json.dumps({'type': 'STATE', 'value': {'bookmarks': {'first': {'lsn': 200}}}}), flush=True)
coordinator.finish()
'''


@pytest.mark.parametrize('terminate', [False, True])
def test_actual_os_eof_while_process_waits_and_real_sigterm(tmp_path, terminate):
    state_file, feedback_file = tmp_path / 'state.json', tmp_path / 'feedback.json'
    state_file.write_text(json.dumps(state_at(first=100)), encoding='utf-8')
    process = subprocess.Popen([sys.executable, '-c', CHILD_PROCESS, str(state_file), str(feedback_file)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        with ThreadPoolExecutor(max_workers=1) as reader:
            output = reader.submit(process.stdout.read).result(timeout=5)
        assert json.loads(output)['value']['bookmarks']['first']['lsn'] == 200
        assert process.poll() is None  # EOF is observable before source process or session termination.
        assert not feedback_file.exists()
        if terminate:
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=5) == -signal.SIGTERM
            assert not feedback_file.exists()
        else:
            state_file.write_text(json.dumps(state_at(first=900)), encoding='utf-8')
            assert process.wait(timeout=5) == 0, process.stderr.read()
            assert json.loads(feedback_file.read_text(encoding='utf-8')) == {
                'write_lsn': 200, 'flush_lsn': 200, 'reply': True, 'force': True}
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


@pytest.mark.parametrize('fails', [False, True])
def test_main_finishes_only_after_sync_and_closes_on_failure(monkeypatch, fails):
    coordinator = Mock()
    args = SimpleNamespace(config={'host': 'host', 'user': 'user', 'password': 'password', 'port': 5432,
                                   'dbname': 'source'}, discover=False, properties={'streams': []},
                           catalog=None, state={}, state_file='state.json')
    events = []
    monkeypatch.setattr(tap_postgres, 'parse_args', lambda _: args)
    monkeypatch.setattr(feedback.FinalFeedbackCoordinator, 'from_environment', lambda _: coordinator)

    def do_sync(*_, **kwargs):
        assert kwargs['final_feedback'] is coordinator
        coordinator.finish.assert_not_called()
        events.extend(['first_group', 'second_group'])
        if fails:
            raise RuntimeError('sync failed')

    coordinator.finish.side_effect = lambda: events.append('finish')
    coordinator.close.side_effect = lambda: events.append('close')
    monkeypatch.setattr(tap_postgres, 'do_sync', do_sync)
    if fails:
        with pytest.raises(RuntimeError, match='sync failed'):
            tap_postgres.main_impl()
        assert events == ['first_group', 'second_group', 'close']
    else:
        tap_postgres.main_impl()
        assert events == ['first_group', 'second_group', 'finish', 'close']


@pytest.mark.parametrize('database_names', [('first_database',), ('first_database', 'second_database')])
def test_sync_hands_off_only_single_database_catalogs(monkeypatch, database_names):
    streams = [{'tap_stream_id': name, 'metadata': [{'breadcrumb': [], 'metadata': {'database-name': name}}]}
               for name in database_names]
    coordinator = Mock()
    monkeypatch.setattr(tap_postgres, 'is_selected_via_metadata', lambda _: True)
    monkeypatch.setattr(tap_postgres, 'any_logical_streams', lambda *_: True)
    monkeypatch.setattr(tap_postgres, 'refresh_streams_schema', Mock())
    monkeypatch.setattr(tap_postgres, 'sync_method_for_streams', lambda *_: ({}, [], streams))
    monkeypatch.setattr(tap_postgres.logical_replication, 'fetch_current_lsn', lambda _: 100)
    sync_logical = Mock(side_effect=lambda _config, _streams, state, *_args, **_kwargs: state)
    monkeypatch.setattr(tap_postgres, 'sync_logical_streams', sync_logical)

    tap_postgres.do_sync({'dbname': 'configured_database'}, {'streams': streams}, 'LOG_BASED', {},
                         'state.json', final_feedback=coordinator)

    expected_options = {'final_feedback': coordinator} if len(database_names) == 1 else {}
    assert sync_logical.call_count == len(database_names)
    assert all(entry.kwargs == expected_options for entry in sync_logical.call_args_list)
    coordinator.retain.assert_not_called()
