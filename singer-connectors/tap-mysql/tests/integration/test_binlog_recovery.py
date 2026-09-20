"""Exercise interrupted binlog replication against a real MySQL-family server."""

import copy
import os

import pytest
import singer

from tap_mysql.connection import connect_with_backoff
from tap_mysql.sync_strategies import binlog

try:
    from tests.integration import utils as test_utils
except ImportError:
    import utils as test_utils


@pytest.mark.parametrize('use_gtid', [False, True])
def test_large_transaction_survives_interruption(use_gtid, monkeypatch):
    connection = test_utils.get_test_connection()
    with connect_with_backoff(connection) as source:
        with source.cursor() as cursor:
            cursor.execute('CREATE TABLE recovery_rows (id INT PRIMARY KEY, content TEXT)')
            cursor.execute("INSERT INTO recovery_rows VALUES (0, 'seed')")
        source.commit()

    config = dict(test_utils.get_db_config(), engine=os.getenv('TAP_MYSQL_ENGINE', 'mariadb'),
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

    persisted = copy.deepcopy(state)
    records = {}
    emitted = 0
    readers = []
    create_reader = binlog.create_binlog_stream_reader

    def capture_reader(*args, **kwargs):
        reader = create_reader(*args, **kwargs)
        readers.append(reader)
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
        legacy_state = copy.deepcopy(persisted)
        legacy_state['bookmarks'][stream.tap_stream_id]['log_pos'] = readers[-1].log_pos
        original_legacy = copy.deepcopy(legacy_state)
        with pytest.raises(ValueError, match='perform a full resync'):
            binlog.sync_binlog_stream(connection, config, streams, legacy_state)
        assert legacy_state == original_legacy

    def resumed_output(message):
        nonlocal persisted
        if isinstance(message, singer.StateMessage):
            persisted = copy.deepcopy(message.value)
        elif isinstance(message, singer.RecordMessage):
            records[message.record['id']] = message.record['content']

    monkeypatch.setattr(singer, 'write_message', resumed_output)
    binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(persisted))
    assert records == expected

    replayed = []
    monkeypatch.setattr(singer, 'write_message', replayed.append)
    binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(persisted))
    assert not any(isinstance(message, singer.RecordMessage) for message in replayed)
