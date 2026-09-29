"""Exercise interrupted binlog replication against a real MySQL-family server."""

import copy

import pytest
import singer

from tap_mysql.connection import connect_with_backoff
from tap_mysql.sync_strategies import binlog

try:
    from tests.integration import utils as test_utils
except ImportError:
    import utils as test_utils


def legacy_gtid_context():
    """Create a selected stream with a pre-0.90 complete-marker bookmark."""
    connection = test_utils.get_test_connection()
    with connect_with_backoff(connection) as source:
        with source.cursor() as cursor:
            cursor.execute('CREATE TABLE recovery_rows (id INT PRIMARY KEY, content TEXT)')
            cursor.execute("INSERT INTO recovery_rows VALUES (0, 'seed')")
        source.commit()

    engine = test_utils.get_source_engine(connection)
    config = dict(
        test_utils.get_db_config(), engine=engine, use_gtid=True,
        filter_dbs=test_utils.DB_NAME,
    )
    catalog = test_utils.discover_catalog(connection, config)
    stream = next(entry for entry in catalog.streams if entry.table == 'recovery_rows')
    stream.stream = stream.tap_stream_id
    streams = binlog.generate_streams_map([stream])
    log_file, log_pos = binlog.fetch_current_log_file_and_pos(connection)
    state = {'bookmarks': {stream.tap_stream_id: {
        'log_file': log_file,
        'log_pos': log_pos,
        'gtid': binlog.fetch_current_gtid_pos(connection, engine),
        'version': 1,
    }}}
    return connection, config, stream.tap_stream_id, streams, state


def row_event_end_positions(connection, log_file, log_pos):
    """Return real row-event endpoints after a durable checkpoint."""
    with connect_with_backoff(connection) as source:
        with source.cursor() as cursor:
            cursor.execute('SET character_set_results = binary')
            cursor.execute('SHOW BINLOG EVENTS IN %s FROM %s', (log_file, log_pos))
            events = cursor.fetchall()
    return [event[4] for event in events
            if event[2].lower().startswith((b'write_rows', b'update_rows', b'delete_rows'))]


def test_legacy_gtid_migration_replays_to_a_real_transaction_boundary(monkeypatch):
    connection, config, stream_id, streams, state = legacy_gtid_context()
    with connect_with_backoff(connection) as source:
        source.begin()
        with source.cursor() as cursor:
            cursor.execute("INSERT INTO recovery_rows VALUES (1, 'first')")
            cursor.execute("INSERT INTO recovery_rows VALUES (2, 'second')")
        source.commit()

    messages = []
    monkeypatch.setattr(singer, 'write_message', messages.append)
    binlog.sync_binlog_stream(connection, config, streams, state)

    assert [message.record['id'] for message in messages if isinstance(message, singer.RecordMessage)] == [1, 2]
    assert state['bookmarks'][stream_id]['gtid_complete'] is True
    assert state['bookmarks'][stream_id]['gtid']

    messages.clear()
    binlog.sync_binlog_stream(connection, config, streams, state)
    assert not any(isinstance(message, singer.RecordMessage) for message in messages)


def test_legacy_gtid_migration_does_not_promote_an_open_transaction_endpoint(monkeypatch):
    connection, config, _, streams, state = legacy_gtid_context()
    original = copy.deepcopy(state)
    bookmark = next(iter(state['bookmarks'].values()))
    with connect_with_backoff(connection) as source:
        source.begin()
        with source.cursor() as cursor:
            cursor.execute("INSERT INTO recovery_rows VALUES (1, 'first')")
            cursor.execute("INSERT INTO recovery_rows VALUES (2, 'second')")
        source.commit()

    row_end_positions = row_event_end_positions(connection, bookmark['log_file'], bookmark['log_pos'])
    assert len(row_end_positions) >= 2
    assert row_end_positions[0] > bookmark['log_pos']
    endpoint_gtid = binlog.fetch_current_gtid_pos(connection, config['engine'])
    monkeypatch.setattr(
        binlog,
        'fetch_current_binlog_checkpoint',
        lambda *_: (bookmark['log_file'], row_end_positions[0], endpoint_gtid),
    )
    messages = []
    monkeypatch.setattr(singer, 'write_message', messages.append)

    with pytest.raises(
            RuntimeError,
            match='Durable state was retained; the next scheduled run will retry automatically'):
        binlog.sync_binlog_stream(connection, config, streams, state)

    assert state == original
    assert not any(isinstance(message, singer.StateMessage) for message in messages)


@pytest.mark.parametrize('use_gtid', [False, True])
def test_large_transaction_survives_interruption(use_gtid, monkeypatch):
    connection = test_utils.get_test_connection()
    with connect_with_backoff(connection) as source:
        with source.cursor() as cursor:
            cursor.execute('CREATE TABLE dropped_recovery_rows (id INT PRIMARY KEY)')
            cursor.execute('INSERT INTO dropped_recovery_rows VALUES (1)')
            cursor.execute('DROP TABLE dropped_recovery_rows')
            cursor.execute('CREATE TABLE recovery_rows (id INT PRIMARY KEY, content TEXT)')
            cursor.execute("INSERT INTO recovery_rows VALUES (0, 'seed')")
        source.commit()

    engine = test_utils.get_source_engine(connection)
    config = dict(test_utils.get_db_config(), engine=engine,
                  use_gtid=use_gtid, filter_dbs=test_utils.DB_NAME)
    catalog = test_utils.discover_catalog(connection, config)
    stream = next(entry for entry in catalog.streams if entry.table == 'recovery_rows')
    stream.stream = stream.tap_stream_id
    streams = binlog.generate_streams_map([stream])
    log_file, log_pos = binlog.fetch_current_log_file_and_pos(connection)
    state = {'bookmarks': {stream.tap_stream_id: {'log_file': log_file, 'log_pos': log_pos, 'version': 1}}}
    if use_gtid:
        state['bookmarks'][stream.tap_stream_id]['gtid'] = binlog.fetch_current_gtid_pos(connection, config['engine'])
        state['bookmarks'][stream.tap_stream_id]['gtid_complete'] = True

    expected = {row_id: f'row {row_id}: ' + 'x' * 250 for row_id in range(1, 1501)}
    with connect_with_backoff(connection) as source:
        source.begin()
        with source.cursor() as cursor:
            cursor.executemany('INSERT INTO recovery_rows VALUES (%s, %s)', list(expected.items()))
        source.commit()
        with source.cursor() as cursor:
            cursor.execute('INSERT INTO recovery_rows VALUES (%s, %s)', (1501, 'subsequent transaction'))
        source.commit()
    expected[1501] = 'subsequent transaction'

    with connect_with_backoff(connection) as source:
        with source.cursor() as cursor:
            cursor.execute('SET character_set_results = binary')
            cursor.execute('SHOW BINLOG EVENTS IN %s FROM %s', (log_file, log_pos))
            events = cursor.fetchall()
    unsafe_row_pos = next(
        event[1] for event in events
        if event[2].lower().startswith((b'write_rows', b'update_rows', b'delete_rows')))

    persisted = copy.deepcopy(state)
    records = {}
    emitted = 0
    reader_starts = []
    create_reader = binlog.create_binlog_stream_reader

    def capture_reader(*args, **kwargs):
        reader = create_reader(*args, **kwargs)
        reader_starts.append((reader.log_file, reader.log_pos))
        return reader

    monkeypatch.setattr(binlog, 'create_binlog_stream_reader', capture_reader)

    def interrupted_output(message):
        nonlocal persisted, emitted
        if isinstance(message, singer.StateMessage):
            persisted = copy.deepcopy(message.value)
        elif isinstance(message, singer.RecordMessage):
            emitted += 1
            if emitted == 1000:
                raise RuntimeError('simulated downstream interruption')
            records[message.record['id']] = message.record['content']

    monkeypatch.setattr(binlog, 'UPDATE_BOOKMARK_PERIOD', 1)
    monkeypatch.setattr(singer, 'write_message', interrupted_output)
    with pytest.raises(RuntimeError, match='simulated downstream interruption'):
        binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(state))
    assert emitted == 1000
    if use_gtid:
        assert persisted == state
    else:
        persisted = copy.deepcopy(state)
        persisted['bookmarks'][stream.tap_stream_id]['log_pos'] = unsafe_row_pos
        records.clear()

    def resumed_output(message):
        nonlocal persisted
        if isinstance(message, singer.StateMessage):
            persisted = copy.deepcopy(message.value)
        elif isinstance(message, singer.RecordMessage):
            records[message.record['id']] = message.record['content']

    monkeypatch.setattr(singer, 'write_message', resumed_output)
    binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(persisted))
    assert records == expected
    if not use_gtid:
        recovery_file, recovery_pos = reader_starts[-1]
        assert recovery_file == log_file
        assert binlog.BINLOG_START_POSITION < recovery_pos < unsafe_row_pos

    replayed = []
    monkeypatch.setattr(singer, 'write_message', replayed.append)
    binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(persisted))
    assert not any(isinstance(message, singer.RecordMessage) for message in replayed)
