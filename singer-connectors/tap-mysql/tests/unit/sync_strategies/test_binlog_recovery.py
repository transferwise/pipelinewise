"""Regression tests for binlog checkpoints and row identity."""

import copy
import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from pymysql.err import OperationalError, ProgrammingError
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.constants import FIELD_TYPE
from pymysqlreplication.event import (
    GtidEvent, MariadbGtidEvent, NotImplementedEvent, QueryEvent, XidEvent, XAPrepareEvent,
)
from pymysqlreplication.row_event import TableMapEvent, UpdateRowsEvent, WriteRowsEvent
from singer import Catalog, CatalogEntry, RecordMessage, Schema, StateMessage

import tap_mysql
from tap_mysql.connection import MySQLConnection, make_connection_wrapper
from tap_mysql.sync_strategies import binlog


MYSQL_SID = '3e11fa47-71ca-11e1-9e33-c80aa9429562'
GTID_POSITIONS = [
    ('mysql', f'{MYSQL_SID}:10'),
    ('mysql', f'{MYSQL_SID}:1'),
    ('mysql', f'{MYSQL_SID}:1-10'),
    ('mysql', f'{MYSQL_SID}:1-3:10'),
    ('mysql', f'{MYSQL_SID}:10,4e11fa47-71ca-11e1-9e33-c80aa9429562:5'),
    ('mariadb', '0-1-10'),
    ('mariadb', '0-1-10,1-2-5'),
]


def event(event_class, **values):
    """Provide only fields read from real decoder events."""
    result = Mock(spec=event_class)
    for key, value in values.items():
        setattr(result, key, value)
    return result


def write_event(first_id=1, count=1, **values):
    return event(
        WriteRowsEvent, schema='db', table='items',
        columns=[SimpleNamespace(name='id', type=FIELD_TYPE.LONG)],
        rows=[{'values': {'id': row_id}} for row_id in range(first_id, first_id + count)],
        flags=values.get('flags', 0),
    )


class Reader:
    """Model decoder positions and the restart token at every event boundary."""

    def __init__(self, events, gtid=None):
        self.events = events
        self.auto_position = gtid
        self.log_file = 'mysql-bin.000001'
        self.log_pos = 4
        self.positions = []

    def __iter__(self):
        for log_pos, row_event in self.events:
            self.positions.append(self.auto_position)
            self.log_pos = log_pos
            if isinstance(row_event, Exception):
                raise row_event
            yield row_event


@pytest.fixture
def stream():
    return CatalogEntry(
        table='items', stream='db-items', tap_stream_id='db-items',
        schema=Schema(properties={
            'id': Schema(type=['integer']),
            'value': Schema(type=['null', 'string']),
            '_sdc_deleted_at': Schema(type=['null', 'string'], format='date-time'),
        }),
        metadata=[{'breadcrumb': [], 'metadata': {
            'database-name': 'db', 'table-key-properties': ['id'],
        }}],
    )


def run(reader, stream, engine='mysql', end_pos=1000, state=None):
    state = state if state is not None else {'bookmarks': {'db-items': {
        'version': 1, 'log_file': reader.log_file, 'log_pos': 4,
        **({'gtid': reader.auto_position} if reader.auto_position else {}),
    }}}
    streams = {'db-items': {
        'catalog_entry': stream, 'desired_columns': set(stream.schema.properties),
    }}
    config = {'engine': engine, 'use_gtid': bool(reader.auto_position)}
    with patch.object(binlog.singer, 'write_message') as output:
        binlog._run_binlog_sync(None, reader, streams, state, config, reader.log_file, end_pos)
    return state, [call.args[0] for call in output.call_args_list]


def test_event_ending_at_sampled_position_is_emitted(stream):
    state, messages = run(Reader([(100, write_event())]), stream, end_pos=100)
    assert [message.record['id'] for message in messages if isinstance(message, RecordMessage)] == [1]
    assert state['bookmarks']['db-items']['log_pos'] == 100


def test_event_after_sampled_position_is_not_acknowledged(stream):
    state, messages = run(Reader([(100, write_event()), (200, write_event(2))]), stream, end_pos=150)
    assert [message.record['id'] for message in messages if isinstance(message, RecordMessage)] == [1]
    assert state['bookmarks']['db-items']['log_pos'] == 150


@pytest.mark.parametrize('engine, initial, transaction, expected, marker', [
    ('mysql', f'{MYSQL_SID}:1-10', f'{MYSQL_SID}:11', f'{MYSQL_SID}:1-11', GtidEvent),
    ('mariadb', '0-1-10', '0-1-11', '0-1-11', MariadbGtidEvent),
])
def test_gtid_waits_for_commit_before_checkpoint_and_reconnect(
        stream, engine, initial, transaction, expected, marker):
    reader = Reader([
        (100, event(marker, gtid=transaction, flags=0)),
        (200, event(QueryEvent, query='BEGIN')),
        (300, write_event(count=1000)),
        (400, write_event(1001)),
        (500, event(XidEvent)),
        (600, event(marker, gtid=transaction, flags=0)),
    ], initial)
    state, messages = run(reader, stream, engine)
    assert reader.positions == [initial] * 5 + [expected]
    assert [type(message) for message in messages] == [RecordMessage] * 1001 + [StateMessage]
    assert state['bookmarks']['db-items']['gtid'] == expected
    assert state['bookmarks']['db-items']['gtid_complete'] is True
    assert state['bookmarks']['db-items']['log_pos'] == 500


def test_gtid_interruption_does_not_checkpoint_partial_transaction(stream):
    initial = f'{MYSQL_SID}:1-10'
    reader = Reader([
        (100, event(GtidEvent, gtid=f'{MYSQL_SID}:11', flags=0)),
        (200, write_event(count=1000)),
        (300, RuntimeError('connection interrupted')),
    ], initial)
    state = {'bookmarks': {'db-items': {'gtid': initial, 'log_pos': 4, 'version': 1}}}
    original = copy.deepcopy(state)
    with patch.object(binlog.singer, 'write_message') as output:
        with pytest.raises(RuntimeError, match='connection interrupted'):
            binlog._run_binlog_sync(
                None, reader, {'db-items': {
                    'catalog_entry': stream, 'desired_columns': set(stream.schema.properties),
                }}, state, {'engine': 'mysql', 'use_gtid': True}, reader.log_file, 1000)
    assert state == original
    assert reader.auto_position == initial
    assert not any(isinstance(call.args[0], StateMessage) for call in output.call_args_list)


@pytest.mark.parametrize('terminal', [event(XidEvent), event(QueryEvent, query='COMMIT')])
def test_gtid_commit_at_exact_endpoint_is_acknowledged(stream, terminal):
    reader = Reader([
        (100, event(GtidEvent, gtid=f'{MYSQL_SID}:11', flags=0)),
        (200, write_event()),
        (300, terminal),
    ], f'{MYSQL_SID}:1-10')
    state, messages = run(reader, stream, end_pos=300)
    assert state['bookmarks']['db-items']['gtid'] == f'{MYSQL_SID}:1-11'
    assert state['bookmarks']['db-items']['log_pos'] == 300
    assert len(messages) == 1


def test_mariadb_standalone_statement_commits_after_last_row(stream):
    reader = Reader([
        (100, event(MariadbGtidEvent, gtid='0-1-11', flags=1)),
        (200, write_event()),
        (300, write_event(2, flags=1)),
    ], '0-1-10')
    state, _ = run(reader, stream, engine='mariadb')
    assert reader.positions == ['0-1-10'] * 3
    assert state['bookmarks']['db-items']['gtid'] == '0-1-11'


def test_gtid_ddl_without_begin_completes_position(stream):
    reader = Reader([
        (100, event(GtidEvent, gtid=f'{MYSQL_SID}:11', flags=0)),
        (200, event(QueryEvent, query='ALTER TABLE items ADD COLUMN label INT')),
    ], f'{MYSQL_SID}:1-10')
    state, _ = run(reader, stream)
    assert state['bookmarks']['db-items']['gtid'] == f'{MYSQL_SID}:1-11'


@pytest.mark.parametrize('before, after, expected_ids', [
    ({'id': 1, 'value': 'old'}, {'id': 2, 'value': 'new'}, [1, 2]),
    ({'id': 1, 'value': 'old'}, {'id': 1, 'value': 'new'}, [1]),
])
def test_primary_key_updates_remove_old_identity(stream, before, after, expected_ids):
    row_event = event(
        UpdateRowsEvent, rows=[{'before_values': before, 'after_values': after}], timestamp=1609459200,
        columns=[SimpleNamespace(name='id', type=FIELD_TYPE.LONG),
                 SimpleNamespace(name='value', type=FIELD_TYPE.VARCHAR)],
    )
    with patch.object(binlog.singer, 'write_message') as output:
        count = binlog.handle_update_rows_event(
            row_event, stream, {'bookmarks': {'db-items': {'version': 1}}},
            set(stream.schema.properties), 0, datetime.datetime.now(datetime.timezone.utc))
    records = [call.args[0].record for call in output.call_args_list]
    assert count == len(expected_ids)
    assert [record['id'] for record in records] == expected_ids
    if len(expected_ids) == 2:
        assert records[0]['_sdc_deleted_at'] == '2021-01-01T00:00:00+00:00'
    assert '_sdc_deleted_at' not in records[-1]
    assert before == {'id': 1, 'value': 'old'}


def test_merge_gtid_retains_other_server_and_domain_history():
    other_sid = '4e11fa47-71ca-11e1-9e33-c80aa9429562'
    assert binlog.merge_gtid_position(
        f'{MYSQL_SID}:1-10,{other_sid}:1-3', f'{MYSQL_SID}:11', 'mysql'
    ) == f'{MYSQL_SID}:1-11,{other_sid}:1-3'
    assert binlog.merge_gtid_position('0-1-10,1-2-20', '0-1-11', 'mariadb') == '0-1-11,1-2-20'


@pytest.mark.parametrize('engine, positions, expected', [
    ('mariadb', ['0-1-10,1-2-20', '0-1-12,1-2-19'], '0-1-10,1-2-19'),
    ('mysql', [f'{MYSQL_SID}:1-10:12-20', f'{MYSQL_SID}:1-15'], f'{MYSQL_SID}:1-10:12-15'),
])
def test_resuming_intersects_all_selected_stream_positions(engine, positions, expected):
    streams = {'stream1': {}, 'stream2': {}}
    state = {'bookmarks': {
        stream: {'gtid': position, 'gtid_complete': True} for stream, position in zip(streams, positions)
    }}
    assert binlog.calculate_gtid_bookmark(None, streams, state, engine) == expected


def test_mysql_resume_intersects_every_server_uuid():
    other_sid = '4e11fa47-71ca-11e1-9e33-c80aa9429562'
    streams = {'stream1': {}, 'stream2': {}}
    state = {'bookmarks': {
        'stream1': {'gtid': f'{MYSQL_SID}:1-10,{other_sid}:1-20', 'gtid_complete': True},
        'stream2': {'gtid': f'{MYSQL_SID}:1-12,{other_sid}:1-19', 'gtid_complete': True},
    }}
    assert binlog.calculate_gtid_bookmark(None, streams, state, 'mysql') == (
        f'{MYSQL_SID}:1-10,{other_sid}:1-19')


def test_disjoint_mysql_bookmarks_fail_without_claiming_unacknowledged_transactions():
    streams = {'stream1': {}, 'stream2': {}}
    state = {'bookmarks': {
        'stream1': {'gtid': f'{MYSQL_SID}:10', 'gtid_complete': True},
        'stream2': {'gtid': f'{MYSQL_SID}:20', 'gtid_complete': True},
    }}
    with pytest.raises(ValueError, match='no shared GTID checkpoint'):
        binlog.calculate_gtid_bookmark(None, streams, state, 'mysql')


@pytest.mark.parametrize('engine, position', GTID_POSITIONS)
@pytest.mark.parametrize('complete', [None, False, 'true', 1])
def test_legacy_gtid_checkpoint_requires_resync(engine, position, complete):
    bookmark = {'gtid': position}
    if complete is not None:
        bookmark['gtid_complete'] = complete
    state = {'bookmarks': {'stream1': bookmark}}
    original = copy.deepcopy(state)
    with pytest.raises(ValueError, match='legacy.*full resync'):
        binlog.calculate_gtid_bookmark(None, {'stream1': {}}, state, engine)
    assert state == original


@pytest.mark.parametrize('engine, position', GTID_POSITIONS)
def test_complete_gtid_sets_remain_resumable(engine, position):
    state = {'bookmarks': {'stream1': {'gtid': position, 'gtid_complete': True}}}
    assert binlog.calculate_gtid_bookmark(None, {'stream1': {}}, state, engine) == position


@pytest.mark.parametrize('auto_incrementing', [False, True])
def test_initial_snapshot_marks_fresh_gtid_as_complete(stream, auto_incrementing):
    state = {'bookmarks': {stream.tap_stream_id: {'version': 1}}}
    with patch.object(binlog, 'verify_binlog_config'), \
            patch.object(binlog, 'verify_gtid_config'), \
            patch.object(binlog, 'fetch_current_log_file_and_pos', return_value=('mysql-bin.000001', 4)), \
            patch.object(binlog, 'fetch_current_gtid_pos', return_value=f'{MYSQL_SID}:10'), \
            patch.object(tap_mysql, 'write_schema_message'), \
            patch.object(tap_mysql.full_table, 'pks_are_auto_incrementing', return_value=auto_incrementing), \
            patch.object(tap_mysql.full_table, 'sync_table'):
        tap_mysql.do_sync_historical_binlog(None, stream, state, {'id'}, True, 'mysql')
    binlog.common.whitelist_bookmark_keys(binlog.BOOKMARK_KEYS, stream.tap_stream_id, state)
    assert state['bookmarks'][stream.tap_stream_id]['gtid_complete'] is True
    assert binlog.calculate_gtid_bookmark(None, {stream.tap_stream_id: {}}, state, 'mysql') == f'{MYSQL_SID}:10'


def test_missing_gtid_bookmark_does_not_use_other_streams_later_position():
    with pytest.raises(ValueError, match='Every selected stream'):
        binlog.calculate_gtid_bookmark(
            None, {'stream1': {}, 'stream2': {}}, {'bookmarks': {'stream1': {'gtid': '0-1-10'}}}, 'mariadb')


def test_mariadb_gtid_inference_rejects_unsafe_legacy_file_checkpoint():
    with patch.object(binlog, 'calculate_bookmark', return_value=('mysql-bin.000001', 123)), \
            patch.object(binlog, 'verify_binlog_checkpoint', side_effect=ValueError('unsafe legacy checkpoint')), \
            patch.object(binlog, '_find_gtid_by_binlog_coordinates') as infer:
        with pytest.raises(ValueError, match='unsafe legacy checkpoint'):
            binlog.calculate_gtid_bookmark(None, {'stream1': {}}, {}, 'mariadb')
    infer.assert_not_called()


@pytest.mark.parametrize('event_type', ['Table_map', 'Query', 'Xid', 'Annotate_rows', 'Rows_query'])
def test_mariadb_gtid_inference_cannot_skip_a_partially_read_transaction(event_type):
    state = {'bookmarks': {'stream1': {'log_file': 'mysql-bin.000001', 'log_pos': 123}}}
    original = copy.deepcopy(state)
    with patch.object(binlog, 'calculate_bookmark', return_value=('mysql-bin.000001', 123)), \
            patch.object(binlog, 'connect_with_backoff') as connect, \
            patch.object(binlog, '_find_gtid_by_binlog_coordinates', return_value='0-1-10') as infer:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [('', 0, event_type, 1, 150, ''), ('', 150, 'Gtid', 1, 190, '')]
        with pytest.raises(ValueError, match='full resync'):
            binlog.calculate_gtid_bookmark(None, {'stream1': {}}, state, 'mariadb')
    infer.assert_not_called()
    assert state == original


@pytest.mark.parametrize('events', [[], ['Gtid'], ['Rotate'], ['Format_desc', 'Gtid_list', 'Gtid']])
def test_mariadb_gtid_inference_accepts_proven_transaction_boundaries(events):
    with patch.object(binlog, 'calculate_bookmark', return_value=('mysql-bin.000001', 123)), \
            patch.object(binlog, 'connect_with_backoff') as connect, \
            patch.object(binlog, '_find_gtid_by_binlog_coordinates', return_value='0-1-10'):
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.side_effect = [
            [('', 0, event_type, 1, 150, '') for event_type in events], [('mysql-bin.000001', 123)]
        ]
        assert binlog.calculate_gtid_bookmark(None, {'stream1': {}}, {}, 'mariadb') == '0-1-10'


@pytest.mark.parametrize('file_size', [120, 130])
def test_mariadb_gtid_inference_requires_exact_eof_not_past_eof_or_a_grown_file(file_size):
    with patch.object(binlog, 'calculate_bookmark', return_value=('mysql-bin.000001', 123)), \
            patch.object(binlog, 'connect_with_backoff') as connect, \
            patch.object(binlog, '_find_gtid_by_binlog_coordinates') as infer:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.side_effect = [[], [('mysql-bin.000001', file_size)]]
        with pytest.raises(ValueError, match='full resync'):
            binlog.calculate_gtid_bookmark(None, {'stream1': {}}, {}, 'mariadb')
    infer.assert_not_called()


def test_xa_fails_before_gtid_is_acknowledged(stream):
    initial = f'{MYSQL_SID}:1-10'
    reader = Reader([
        (100, event(GtidEvent, gtid=f'{MYSQL_SID}:11', flags=0)),
        (200, event(QueryEvent, query="XA START 'transaction'")),
    ], initial)
    with pytest.raises(ValueError, match='XA transactions'):
        run(reader, stream)
    assert reader.auto_position == initial


def test_xa_without_gtid_fails_before_any_rows_are_emitted(stream):
    reader = Reader([
        (100, event(QueryEvent, query="XA START 'transaction'")),
        (200, event(TableMapEvent)),
        (300, write_event(flags=1)),
    ])
    with patch.object(binlog.singer, 'write_message') as output:
        with pytest.raises(ValueError, match='XA transactions'):
            binlog._run_binlog_sync(
                None, reader, {'db-items': {
                    'catalog_entry': stream, 'desired_columns': set(stream.schema.properties),
                }}, {}, {'engine': 'mysql', 'use_gtid': False}, reader.log_file, 1000)
    output.assert_not_called()


def test_xa_prepare_cannot_be_acknowledged_by_a_later_transaction(stream):
    initial = f'{MYSQL_SID}:1-10'
    reader = Reader([
        (100, event(GtidEvent, gtid=f'{MYSQL_SID}:11', flags=0)),
        (200, event(QueryEvent, query='BEGIN')),
        (300, write_event()),
        (400, event(XAPrepareEvent)),
        (500, event(GtidEvent, gtid=f'{MYSQL_SID}:12', flags=0)),
        (600, event(XidEvent)),
    ], initial)
    state = {'bookmarks': {'db-items': {'gtid': initial, 'log_pos': 4, 'version': 1}}}
    original = copy.deepcopy(state)
    with pytest.raises(ValueError, match='XA transactions'):
        run(reader, stream, state=state)
    assert state == original
    assert reader.auto_position == initial
    with patch.object(binlog, 'BinLogStreamReader') as factory:
        binlog.create_binlog_stream_reader({'engine': 'mysql', 'use_gtid': True}, None, None, initial)
    assert XAPrepareEvent in factory.call_args.kwargs['only_events']


def test_file_position_checkpoint_follows_every_row_in_event(stream):
    state, messages = run(Reader([(100, write_event(count=1001))]), stream)
    assert [type(message) for message in messages] == [RecordMessage] * 1001 + [StateMessage]
    assert state['bookmarks']['db-items']['log_pos'] == 100


def test_mariadb_file_position_uses_position_aware_dump_protocol():
    with patch.object(binlog, 'BinLogStreamReader') as reader:
        binlog.create_binlog_stream_reader(
            {'engine': 'mariadb', 'use_gtid': False, 'server_id': 123}, 'mysql-bin.000001', 123, None)
    assert reader.call_args.kwargs['is_mariadb'] is False
    assert reader.call_args.kwargs['log_file'] == 'mysql-bin.000001'
    assert reader.call_args.kwargs['log_pos'] == 123
    assert reader.call_args.kwargs['resume_stream'] is True
    assert reader.call_args.kwargs['fail_on_table_metadata_unavailable'] is True


def test_resumed_binlog_validates_row_image_before_reading_or_acknowledging():
    with patch.object(binlog, 'verify_binlog_config', side_effect=ValueError('row image')):
        with patch.object(binlog, 'create_binlog_stream_reader') as reader:
            with patch.object(binlog.singer, 'write_message') as output:
                with pytest.raises(ValueError, match='row image'):
                    binlog.sync_binlog_stream(None, {}, {}, {})
    reader.assert_not_called()
    output.assert_not_called()


@pytest.mark.parametrize('values, setting', [
    (['ROW', 'FULL', 'PARTIAL_JSON'], 'binlog_row_value_options'),
    (['ROW', 'FULL', '', 1], 'binlog_transaction_compression'),
    (['ROW', 'FULL', '', 0, 1], 'log_bin_compress'),
])
def test_unsupported_binlog_encodings_fail_validation(values, setting):
    with patch.object(binlog, 'connect_with_backoff') as connect:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [[value] for value in values]
        with pytest.raises(ValueError, match=setting):
            binlog.verify_binlog_config(None)


def test_optional_source_variables_ignore_only_unknown_variable_errors():
    with patch.object(binlog, 'connect_with_backoff') as connect:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [['ROW'], ['FULL']]
        cursor.execute.side_effect = [None, None] + [OperationalError(1193, 'unknown')] * 3
        binlog.verify_binlog_config(None)
        cursor.fetchone.side_effect = [['ROW'], ['FULL']]
        cursor.execute.side_effect = [None, None, OperationalError(1045, 'denied')]
        with pytest.raises(OperationalError):
            binlog.verify_binlog_config(None)


@pytest.mark.parametrize('event_type', [39, 40, 165, 166, 167, 168, 169, 170, 171])
def test_unsupported_historical_encoding_does_not_advance_bookmark(stream, event_type):
    reader = Reader([(100, event(NotImplementedEvent, event_type=event_type)), (200, write_event())])
    state = {'bookmarks': {'db-items': {'log_file': reader.log_file, 'log_pos': 4}}}
    original = copy.deepcopy(state)
    with pytest.raises(ValueError, match='cannot decode'):
        run(reader, stream, state=state)
    assert state == original


def test_unknown_metadata_event_does_not_block_replication(stream):
    reader = Reader([(100, event(NotImplementedEvent, event_type=34)), (200, write_event())])
    state, messages = run(reader, stream)
    assert len(messages) == 1
    assert state['bookmarks']['db-items']['log_pos'] == 200


@pytest.mark.parametrize('query', [
    'TRUNCATE items', 'TRUNCATE TABLE `items`', 'TRUNCATE TABLE `db`.`items`',
    '/* comment */ TRUNCATE /* TABLE comment */ TABLE `items`', '/*! TRUNCATE TABLE items */',
    '-- comment\nTRUNCATE TABLE items', '# comment\nTRUNCATE TABLE items',
    'TRUNCATE TABLE "db"."items"', 'TRUNCATE "items"',
    'TRUNCATE TABLE DB.ITEMS',
])
def test_truncate_selected_table_fails_without_acknowledgment(stream, query):
    reader = Reader([(100, event(QueryEvent, query=query, schema=b'db')), (200, write_event())])
    state = {'bookmarks': {'db-items': {'log_file': reader.log_file, 'log_pos': 4}}}
    original = copy.deepcopy(state)
    with pytest.raises(ValueError, match='cannot replicate TRUNCATE'):
        run(reader, stream, state=state)
    assert state == original


@pytest.mark.parametrize('query', ['TRUNCATE TABLE other', 'TRUNCATE TABLE elsewhere.items'])
def test_truncate_unselected_table_does_not_block_replication(stream, query):
    reader = Reader([(100, event(QueryEvent, query=query, schema=b'db')), (200, write_event())])
    state, messages = run(reader, stream)
    assert len(messages) == 1
    assert state['bookmarks']['db-items']['log_pos'] == 200


@pytest.mark.parametrize('query', ['TRUNCATE TABLE items', 'TRUNCATE TABLE DB.ITEMS'])
def test_truncate_before_resynced_stream_bookmark_is_safe_to_pass(stream, query):
    reader = Reader([(100, event(QueryEvent, query=query, schema=b'db')),
                     (300, write_event(2))])
    state = {'bookmarks': {'db-items': {'log_file': reader.log_file, 'log_pos': 200, 'version': 1}}}
    state, messages = run(reader, stream, state=state)
    assert [message.record['id'] for message in messages if isinstance(message, RecordMessage)] == [2]
    assert state['bookmarks']['db-items']['log_pos'] == 300


def test_truncate_ansi_quoted_selected_table_from_unselected_database_fails(stream):
    with pytest.raises(ValueError, match='cannot replicate TRUNCATE'):
        binlog._reject_unsupported_query(
            event(QueryEvent, query='TRUNCATE "db"."items"', schema='other'),
            {stream.tap_stream_id: {'catalog_entry': stream}}, {}, Reader([]), None, 'mysql')


@pytest.mark.parametrize('table, quoted', [
    ('it/*special*/ems', '`it/*special*/ems`'), ('items`', '`items```'),
    ('it/*special*/ems', '"it/*special*/ems"'), ('items"', '"items"""'),
])
def test_truncate_preserves_special_characters_inside_quoted_table_names(stream, table, quoted):
    stream.table = table
    stream.stream = stream.tap_stream_id = f'db-{table}'
    streams = {stream.tap_stream_id: {'catalog_entry': stream}}
    with pytest.raises(ValueError, match='cannot replicate TRUNCATE'):
        binlog._reject_unsupported_query(
            event(QueryEvent, query=f'TRUNCATE TABLE {quoted}', schema='db'),
            streams, {}, Reader([]), None, 'mysql')


def test_advanced_stream_does_not_replay_acknowledged_file_position(stream):
    reader = Reader([(100, write_event(1)), (300, write_event(2))])
    state = {'bookmarks': {'db-items': {
        'log_file': reader.log_file, 'log_pos': 200, 'version': 1,
    }}}
    state, messages = run(reader, stream, state=state)
    assert [message.record['id'] for message in messages if isinstance(message, RecordMessage)] == [2]
    assert state['bookmarks']['db-items']['log_pos'] == 300


@pytest.mark.parametrize('engine, initial, committed, older, marker', [
    ('mysql', f'{MYSQL_SID}:1-10', f'{MYSQL_SID}:1-12', f'{MYSQL_SID}:11', GtidEvent),
    ('mariadb', '0-1-10', '0-1-12', '0-1-11', MariadbGtidEvent),
])
def test_advanced_stream_does_not_replay_acknowledged_gtid(
        stream, engine, initial, committed, older, marker):
    reader = Reader([
        (100, event(marker, gtid=older, flags=0)),
        (200, write_event(1)),
        (300, event(XidEvent)),
    ], initial)
    state = {'bookmarks': {'db-items': {
        'log_file': reader.log_file, 'log_pos': 400, 'gtid': committed, 'version': 1,
    }}}
    state, messages = run(reader, stream, engine, state=state)
    assert not messages
    assert state['bookmarks']['db-items']['gtid'] == committed
    assert state['bookmarks']['db-items']['log_pos'] == 400


def test_schema_ignores_are_scoped_to_the_source_stream(stream):
    second_stream = copy.deepcopy(stream)
    second_stream.table = 'other'
    second_stream.stream = second_stream.tap_stream_id = 'db-other'
    streams = {
        entry.tap_stream_id: {'catalog_entry': entry, 'desired_columns': set(entry.schema.properties)}
        for entry in (stream, second_stream)
    }
    events = [write_event(), write_event(2)]
    for row_event in events:
        row_event.columns.append(SimpleNamespace(name='added', type=FIELD_TYPE.LONG))
        row_event.rows[0]['values']['added'] = 42
    events[1].table = 'other'
    discovered = copy.deepcopy(second_stream)
    discovered.schema.properties['added'] = Schema(type=['integer'])
    for column in discovered.schema.properties.values():
        column.inclusion = 'available'
    reader = Reader(list(zip([100, 200], events)))
    with patch.object(binlog, 'should_run_discovery', side_effect=[False, True]):
        with patch.object(binlog, 'discover_catalog', return_value=Catalog([discovered])) as discovery:
            with patch.object(binlog.singer, 'write_message') as output:
                binlog._run_binlog_sync(
                    None, reader, streams, {}, {'use_gtid': False, 'engine': 'mysql'}, reader.log_file, 1000)
    discovery.assert_called_once_with(None, 'db', 'other')
    records = [call.args[0] for call in output.call_args_list if isinstance(call.args[0], RecordMessage)]
    assert 'added' not in records[0].record
    assert records[1].record['added'] == 42


@pytest.mark.parametrize('event_types', [[], ['Gtid'], ['Anonymous_Gtid'], ['Table_map'], ['Xid'],
                                      ['Annotate_rows', 'Table_map'], ['Rows_query', 'Query']])
def test_safe_binlog_checkpoint_requires_one_bounded_query(event_types):
    with patch.object(binlog, 'connect_with_backoff') as connect:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [('mysql-bin.000001', 100, name, 1, 200, '') for name in event_types]
        binlog.verify_binlog_checkpoint(None, 'mysql-bin.000001', 100)
    cursor.execute.assert_called_once_with('SHOW BINLOG EVENTS IN %s FROM %s LIMIT 16', ('mysql-bin.000001', 100))


@pytest.mark.parametrize('event_types', [['Write_rows'], ['Update_rows_v1'], ['Delete_rows_v2'],
                                      ['Transaction_payload'], ['Partial_update_rows'],
                                      ['Rows_query', 'Write_rows_v1'], ['Unknown'], ['Rows_query'] * 16])
def test_unsafe_legacy_checkpoint_stops_before_rows_can_be_skipped(event_types):
    with patch.object(binlog, 'connect_with_backoff') as connect:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [('mysql-bin.000001', 100, name, 1, 200, '') for name in event_types]
        with pytest.raises(ValueError, match='perform a full resync'):
            binlog.verify_binlog_checkpoint(None, 'mysql-bin.000001', 100)


@pytest.mark.parametrize('event_type', ['Annotate_rows', 'Table_map', 'Query'])
def test_mariadb_zero_position_legacy_checkpoint_cannot_hide_unread_rows(event_type):
    with patch.object(binlog, 'connect_with_backoff') as connect:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ('mysql-bin.000001', 100, event_type, 1, 0, ''),
            ('mysql-bin.000001', 200, 'Table_map', 1, 0, ''),
        ]
        with pytest.raises(ValueError, match='full resync'):
            binlog.verify_binlog_checkpoint(None, 'mysql-bin.000001', 100)


@pytest.mark.parametrize('error_code', [2006, 2013])
def test_file_reader_disconnect_stops_before_unsafe_internal_reconnect(stream, error_code):
    config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test',
              'use_gtid': False, 'engine': 'mysql'}
    with patch('tap_mysql.connection.connect_with_backoff'):
        source = make_connection_wrapper(config)()
    reader = BinLogStreamReader({}, 123, log_file='mysql-bin.000001', log_pos=300, resume_stream=True)
    reader._BinLogStreamReader__connected_stream = True
    reader._BinLogStreamReader__connected_ctl = True
    reader._stream_connection = source
    reader._ctl_connection = Mock()
    state = {'bookmarks': {'db-items': {'log_file': reader.log_file, 'log_pos': 4, 'version': 1}}}
    original = copy.deepcopy(state)
    with patch.object(MySQLConnection, '_read_packet', side_effect=OperationalError(error_code, 'network lost')), \
            patch.object(reader, '_BinLogStreamReader__connect_to_stream',
                         side_effect=RuntimeError('unsafe automatic reconnect')) as reconnect, \
            patch.object(binlog.singer, 'write_message') as output:
        with pytest.raises(RuntimeError, match='durable checkpoint'):
            binlog._run_binlog_sync(None, reader, {'db-items': {'catalog_entry': stream}}, state, config,
                                   reader.log_file, 1000)
    reconnect.assert_not_called()
    output.assert_not_called()
    assert state == original


def test_file_checkpoint_waits_for_commit_after_prior_nontransactional_statement(stream):
    reader = Reader([
        (100, event(TableMapEvent)), (200, write_event(1, flags=1)),
        (300, event(QueryEvent, query='BEGIN')), (400, event(TableMapEvent)),
        (500, write_event(2, flags=1)), (600, write_event(3, flags=1)), (700, event(XidEvent)),
    ])
    with patch.object(binlog, 'UPDATE_BOOKMARK_PERIOD', 1):
        _, messages = run(reader, stream)
    bookmarks = [message.value['bookmarks']['db-items']['log_pos']
                 for message in messages if isinstance(message, StateMessage)]
    assert bookmarks == [200, 700]


def test_new_mysql_status_command_is_used_only_after_syntax_error():
    with patch.object(binlog, 'connect_with_backoff') as connect:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = [ProgrammingError(1064, 'syntax error'), None]
        cursor.fetchone.return_value = ('mysql-bin.000001', 123)
        assert binlog.fetch_current_log_file_and_pos(Mock()) == ('mysql-bin.000001', 123)
        assert [call.args[0] for call in cursor.execute.call_args_list] == [
            'SHOW MASTER STATUS', 'SHOW BINARY LOG STATUS',
        ]


@pytest.mark.parametrize('error', [ProgrammingError(1142, 'denied'), OperationalError(2006, 'gone')])
def test_status_lookup_does_not_mask_connection_or_permission_errors(error):
    with patch.object(binlog, 'connect_with_backoff') as connect:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = error
        with pytest.raises(type(error)):
            binlog.fetch_current_log_file_and_pos(Mock())
        assert cursor.execute.call_count == 1
