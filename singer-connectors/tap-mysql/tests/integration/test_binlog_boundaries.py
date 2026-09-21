"""Exercise MariaDB checkpoint boundaries using the real binlog decoder."""

import copy
import os
import uuid

import pytest
import singer
from pymysql.err import OperationalError
from pymysqlreplication.event import MariadbGtidEvent, QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent

from tap_mysql.connection import connect_with_backoff
from tap_mysql.sync_strategies import binlog

try:
    from tests.integration import utils as test_utils
except ImportError:
    import utils as test_utils


@pytest.fixture
def mariadb_source():
    connection = test_utils.get_test_connection()
    with connect_with_backoff(connection) as source:
        with source.cursor() as cursor:
            cursor.execute('SELECT VERSION()')
            if 'MariaDB' not in cursor.fetchone()[0]:
                pytest.skip('MariaDB transaction boundary regression')
            cursor.execute('CREATE TABLE boundary_rows (id INT PRIMARY KEY, content TEXT) ENGINE=InnoDB')
            cursor.execute("INSERT INTO boundary_rows VALUES (0, 'seed')")
        source.commit()
    return connection


def replication_setup(connection, use_gtid):
    config = dict(test_utils.get_db_config(), engine='mariadb', use_gtid=use_gtid, filter_dbs=test_utils.DB_NAME)
    catalog = test_utils.discover_catalog(connection, config)
    stream = next(entry for entry in catalog.streams if entry.table == 'boundary_rows')
    stream.stream = stream.tap_stream_id
    log_file, log_pos = binlog.fetch_current_log_file_and_pos(connection)
    bookmark = {'log_file': log_file, 'log_pos': log_pos, 'version': 1}
    if use_gtid:
        bookmark.update(gtid=binlog.fetch_current_gtid_pos(connection, 'mariadb'), gtid_complete=True)
    return config, binlog.generate_streams_map([stream]), {'bookmarks': {stream.tap_stream_id: bookmark}}


def capture_decoded_events(monkeypatch):
    events = []
    create_reader = binlog.create_binlog_stream_reader

    def capture_reader(*args, **kwargs):
        reader = create_reader(*args, **kwargs)
        fetchone = reader.fetchone

        def capture_event():
            event = fetchone()
            if event is not None:
                events.append(event)
            return event

        reader.fetchone = capture_event
        return reader

    monkeypatch.setattr(binlog, 'create_binlog_stream_reader', capture_reader)
    return events


@pytest.mark.parametrize('use_gtid', [False, True])
def test_savepoint_does_not_acknowledge_an_uncommitted_transaction(mariadb_source, use_gtid, monkeypatch):
    connection = mariadb_source
    config, streams, state = replication_setup(connection, use_gtid)
    with connect_with_backoff(connection) as source:
        source.begin()
        with source.cursor() as cursor:
            cursor.execute("INSERT INTO boundary_rows VALUES (1, 'before savepoint')")
            cursor.execute('SAVEPOINT checkpoint_probe')
            cursor.execute("INSERT INTO boundary_rows VALUES (2, 'after savepoint')")
        source.commit()
        with source.cursor() as cursor:
            cursor.execute("INSERT INTO boundary_rows VALUES (3, 'next transaction')")
        source.commit()

    persisted = copy.deepcopy(state)
    records = {}
    events = capture_decoded_events(monkeypatch)
    monkeypatch.setattr(binlog, 'UPDATE_BOOKMARK_PERIOD', 1)

    def interrupted_output(message):
        nonlocal persisted
        if isinstance(message, singer.StateMessage):
            persisted = copy.deepcopy(message.value)
        elif isinstance(message, singer.RecordMessage):
            if message.record['id'] == 2:
                raise RuntimeError('simulated interruption after savepoint')
            records[message.record['id']] = message.record['content']

    monkeypatch.setattr(singer, 'write_message', interrupted_output)
    with pytest.raises(RuntimeError, match='simulated interruption after savepoint'):
        binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(state))
    assert any(isinstance(event, QueryEvent) and event.query.upper().startswith('SAVEPOINT') for event in events)
    assert not any(isinstance(event, QueryEvent) and event.query.upper() == 'BEGIN' for event in events)
    assert any(isinstance(event, MariadbGtidEvent) and not event.flags & 1 for event in events)
    assert persisted == state

    def resumed_output(message):
        nonlocal persisted
        if isinstance(message, singer.StateMessage):
            persisted = copy.deepcopy(message.value)
        elif isinstance(message, singer.RecordMessage):
            records[message.record['id']] = message.record['content']

    monkeypatch.setattr(singer, 'write_message', resumed_output)
    binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(persisted))
    assert records == {1: 'before savepoint', 2: 'after savepoint', 3: 'next transaction'}

    replayed = []
    monkeypatch.setattr(singer, 'write_message', replayed.append)
    binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(persisted))
    assert not any(isinstance(message, singer.RecordMessage) for message in replayed)


@pytest.mark.parametrize('use_gtid', [False, True])
def test_filtered_transactions_keep_advancing(mariadb_source, use_gtid, monkeypatch):
    connection = mariadb_source
    excluded_database = os.getenv('TAP_MYSQL_DB')
    create_database = not excluded_database or excluded_database == test_utils.DB_NAME
    if create_database:
        excluded_database = f'tap_mysql_excluded_{uuid.uuid4().hex[:8]}'
    excluded_table = f'boundary_excluded_{uuid.uuid4().hex[:8]}'
    quoted_database = '`' + excluded_database.replace('`', '``') + '`'
    qualified_table = f'{quoted_database}.`{excluded_table}`'

    try:
        with connect_with_backoff(connection) as source:
            with source.cursor() as cursor:
                if create_database:
                    cursor.execute(f'CREATE DATABASE {quoted_database}')
                cursor.execute(f'CREATE TABLE {qualified_table} (id INT PRIMARY KEY) ENGINE=MyISAM')
            source.commit()
        config, streams, state = replication_setup(connection, use_gtid)
        events = capture_decoded_events(monkeypatch)
        persisted = copy.deepcopy(state)
        records = []

        def capture_output(message):
            nonlocal persisted
            if isinstance(message, singer.StateMessage):
                persisted = copy.deepcopy(message.value)
            elif isinstance(message, singer.RecordMessage):
                records.append(message.record['id'])

        monkeypatch.setattr(singer, 'write_message', capture_output)
        monkeypatch.setattr(binlog, 'UPDATE_BOOKMARK_PERIOD', 1)
        with connect_with_backoff(connection) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute("INSERT INTO boundary_rows VALUES (1, 'selected')")
                cursor.execute(f'INSERT INTO {qualified_table} VALUES (1)')
                cursor.execute(f'INSERT INTO {qualified_table} VALUES (2)')
            source.commit()

        for iteration in range(3):
            if iteration:
                with connect_with_backoff(connection) as source:
                    source.autocommit(True)
                    with source.cursor() as cursor:
                        cursor.execute(f'INSERT INTO {qualified_table} VALUES (%s)', (iteration + 2,))
                    source.commit()
            expected_file, expected_pos = binlog.fetch_current_log_file_and_pos(connection)
            expected_gtid = binlog.fetch_current_gtid_pos(connection, 'mariadb')
            previous = copy.deepcopy(persisted)
            binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(persisted))
            bookmark = next(iter(persisted['bookmarks'].values()))
            assert (bookmark['log_file'], bookmark['log_pos']) == (expected_file, expected_pos)
            if use_gtid:
                assert bookmark['gtid'] == expected_gtid
            assert persisted != previous
            assert records == [1]

            unchanged = copy.deepcopy(persisted)
            binlog.sync_binlog_stream(connection, config, streams, copy.deepcopy(persisted))
            assert persisted == unchanged
            assert records == [1]

        # MariaDB may emit COMMIT for MyISAM writes; standalone-only completion has separate unit coverage.
        assert any(isinstance(event, MariadbGtidEvent) for event in events)
        assert all(event.schema == test_utils.DB_NAME for event in events if isinstance(event, WriteRowsEvent))
    finally:
        with connect_with_backoff(connection) as source:
            with source.cursor() as cursor:
                if create_database:
                    cursor.execute(f'DROP DATABASE IF EXISTS {quoted_database}')
                else:
                    cursor.execute(f'DROP TABLE IF EXISTS {qualified_table}')
            source.commit()


@pytest.mark.parametrize('require_transaction_boundary', [False, True])
def test_rotated_mariadb_binlog_headers_are_valid_checkpoints(mariadb_source, require_transaction_boundary):
    connection = mariadb_source
    with connect_with_backoff(connection) as source:
        with source.cursor() as cursor:
            try:
                cursor.execute('FLUSH BINARY LOGS')
            except OperationalError as exc:
                if exc.args[0] != 1227:
                    raise
                pytest.skip('Binlog rotation requires RELOAD; run with TAP_MYSQL_USER/TAP_MYSQL_PASSWORD '
                            'for a dedicated integration account with that privilege.')
    log_file, _ = binlog.fetch_current_log_file_and_pos(connection)
    binlog.verify_binlog_checkpoint(connection, log_file, 4, require_transaction_boundary)
    with connect_with_backoff(connection) as source:
        with source.cursor() as cursor:
            cursor.execute("INSERT INTO boundary_rows VALUES (1, 'after rotation')")
        source.commit()
    binlog.verify_binlog_checkpoint(connection, log_file, 4, require_transaction_boundary)
