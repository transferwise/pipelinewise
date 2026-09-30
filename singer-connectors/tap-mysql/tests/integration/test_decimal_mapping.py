"""Exact decimal extraction against the isolated dev-project source databases."""

import json
import os
import uuid
from unittest.mock import patch

import pytest
import singer
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent, MariadbGtidEvent, QueryEvent, RotateEvent, XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, TableMapEvent, UpdateRowsEvent, WriteRowsEvent

from tap_mysql.connection import MySQLConnection, connect_with_backoff
from tap_mysql.discover_utils import discover_catalog
from tap_mysql.sync_strategies import binlog, common, incremental


EXACT = '12345678901234567890.123456789012345678'
CHANGED = '12345678901234567890.123456789012345679'


@pytest.mark.parametrize('prefix', ['TAP_MYSQL', 'TAP_ORACLE_MYSQL'])
def test_decimal_select_and_binlog_keep_source_digits(prefix):
    if not os.environ.get(f'{prefix}_DB'):
        pytest.skip(f'{prefix}_DB is required for the dev-project decimal probe')
    config = {key: os.environ[f'{prefix}_{key.upper()}'] for key in ('host', 'port', 'user', 'password')}
    config['database'] = os.environ[f'{prefix}_DB']
    table = 'decimal_probe_' + uuid.uuid4().hex[:12]
    qualified = f'`{config["database"]}`.`{table}`'
    reader = None
    connection = MySQLConnection(config)
    try:
        with connect_with_backoff(connection) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE {qualified} (id INT PRIMARY KEY, amount DECIMAL(38,18), approximate DOUBLE)')
                cursor.execute('SHOW MASTER STATUS')
                log_file, log_pos, *_ = cursor.fetchone()
                cursor.execute(f'INSERT INTO {qualified} VALUES (1, %s, 1.25)', (EXACT,))

        catalog = discover_catalog(MySQLConnection(config), config['database'], table, decimal_target='snowflake')
        stream = catalog.streams[0]
        assert stream.schema.properties['amount'].to_dict()['decimal'] == {'precision': 38, 'scale': 18}
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'SELECT id, amount, approximate FROM {qualified}')
                record = common.row_to_singer_record(
                    stream, 1, cursor.fetchone(), ['id', 'amount', 'approximate'], None)
                assert json.loads(singer.messages.format_message(record))['record']['amount'] == EXACT
                cursor.execute(f'UPDATE {qualified} SET amount=%s WHERE id=1', (CHANGED,))
                cursor.execute(f'DELETE FROM {qualified} WHERE id=1')
                is_mariadb = 'mariadb' in source.get_server_info().lower()

        reader = BinLogStreamReader(
            connection_settings={
                'host': config['host'], 'port': int(config['port']), 'user': config['user'],
                'passwd': config['password'], 'ssl': {'': True},
            },
            server_id=int(uuid.uuid4().hex[:7], 16), only_schemas=[config['database']], only_tables=[table],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            log_file=log_file, log_pos=log_pos, resume_stream=True, blocking=False, is_mariadb=is_mariadb,
        )
        actual = []
        for event in reader:
            for row in event.rows:
                values = row.get('after_values', row.get('values'))
                record = binlog.row_to_singer_record(stream, 1, binlog.get_db_column_types(event), values, None)
                actual.append(json.loads(singer.messages.format_message(record))['record']['amount'])
        assert actual == [EXACT, CHANGED, CHANGED]
    finally:
        if reader is not None:
            reader.close()
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'DROP TABLE IF EXISTS {qualified}')


@pytest.mark.parametrize('prefix', ['TAP_MYSQL', 'TAP_ORACLE_MYSQL'])
@pytest.mark.parametrize('legacy_float', [False, True])
def test_decimal_incremental_bookmark_filters_adjacent_source_values(prefix, legacy_float):
    """String checkpoints retain DECIMAL comparison precision past double's range."""
    if not os.environ.get(f'{prefix}_DB'):
        pytest.skip(f'{prefix}_DB is required for the dev-project decimal probe')
    config = {key: os.environ[f'{prefix}_{key.upper()}'] for key in ('host', 'port', 'user', 'password')}
    config['database'] = os.environ[f'{prefix}_DB']
    table = 'decimal_boundary_' + uuid.uuid4().hex[:12]
    qualified = f'`{config["database"]}`.`{table}`'
    below = '12345678901234567890.123456789012345677'
    try:
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'CREATE TABLE {qualified} (id INT PRIMARY KEY, amount DECIMAL(38,18))')
                cursor.executemany(
                    f'INSERT INTO {qualified} VALUES (%s,%s)', [(1, below), (2, EXACT), (3, CHANGED)],
                )
                # PartialSync uses the same string bindings with inclusive endpoints.
                cursor.execute(
                    f'SELECT id FROM {qualified} WHERE amount >= %s AND amount <= %s', (EXACT, EXACT),
                )
                assert cursor.fetchall() == [(2,)]

        stream = discover_catalog(
            MySQLConnection(config), config['database'], table, decimal_target='snowflake',
        ).streams[0]
        md_map = singer.metadata.to_map(stream.metadata)
        md_map[()].update({'replication-method': 'INCREMENTAL', 'replication-key': 'amount'})
        for name in ('id', 'amount'):
            md_map[('properties', name)]['selected'] = True
        stream.metadata = singer.metadata.to_list(md_map)
        state = {'bookmarks': {stream.tap_stream_id: {
            'replication_key': 'amount', 'replication_key_value': float(EXACT) if legacy_float else EXACT, 'version': 1,
        }}}
        messages = []
        with patch('singer.write_message', side_effect=messages.append):
            incremental.sync_table(MySQLConnection(config), stream, state, ['id', 'amount'])
        records = [message.record for message in messages if isinstance(message, singer.RecordMessage)]
        expected = [(1, below), (2, EXACT), (3, CHANGED)] if legacy_float else [(2, EXACT), (3, CHANGED)]
        assert [(record['id'], record['amount']) for record in records] == expected
        assert state['bookmarks'][stream.tap_stream_id]['replication_key_value'] == CHANGED
    finally:
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'DROP TABLE IF EXISTS {qualified}')


@pytest.mark.parametrize('prefix', ['TAP_MYSQL', 'TAP_ORACLE_MYSQL'])
def test_queued_alter_replays_each_binlog_decimal_schema_without_repeated_discovery(prefix):
    """Historical values use their TABLE_MAP even when current discovery moved ahead."""
    if not os.environ.get(f'{prefix}_DB'):
        pytest.skip(f'{prefix}_DB is required for the dev-project decimal probe')
    config = {key: os.environ[f'{prefix}_{key.upper()}'] for key in ('host', 'port', 'user', 'password')}
    config.update(database=os.environ[f'{prefix}_DB'], decimal_target='snowflake', use_gtid=False)
    table = 'decimal_alter_' + uuid.uuid4().hex[:12]
    qualified = f'`{config["database"]}`.`{table}`'
    reader = None
    try:
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'CREATE TABLE {qualified} (id INT PRIMARY KEY, amount DECIMAL(5,2))')
                cursor.execute('SHOW MASTER STATUS')
                log_file, log_pos, *_ = cursor.fetchone()
                cursor.execute(f'INSERT INTO {qualified} VALUES (1, 123.45)')
                cursor.execute(f'UPDATE {qualified} SET amount=1.23 WHERE id=1')
                cursor.execute(f'ALTER TABLE {qualified} MODIFY COLUMN amount DECIMAL(5,3)')
                cursor.execute(f'INSERT INTO {qualified} VALUES (2, 2.345)')
                cursor.execute(f'UPDATE {qualified} SET amount=3.456 WHERE id=2')
                cursor.execute(f'DELETE FROM {qualified} WHERE id=2')
                cursor.execute(f'ALTER TABLE {qualified} MODIFY COLUMN amount DOUBLE')
                cursor.execute(f'INSERT INTO {qualified} VALUES (3, 12.345)')
                cursor.execute(f'ALTER TABLE {qualified} MODIFY COLUMN amount DECIMAL(5,3)')
                cursor.execute(f'UPDATE {qualified} SET amount=4.567 WHERE id=3')
                cursor.execute('SHOW MASTER STATUS')
                end_file, end_pos, *_ = cursor.fetchone()
                is_mariadb = 'mariadb' in source.get_server_info().lower()
        config['engine'] = 'mariadb' if is_mariadb else 'mysql'
        stream = discover_catalog(
            MySQLConnection(config), config['database'], table, decimal_target='snowflake',
        ).streams[0]
        md_map = singer.metadata.to_map(stream.metadata)
        md_map[()].update({'replication-method': 'LOG_BASED', 'selected': True})
        for name in ('id', 'amount'):
            md_map[('properties', name)]['selected'] = True
        stream.metadata = singer.metadata.to_list(md_map)
        assert stream.schema.properties['amount'].to_dict()['decimal'] == {'precision': 5, 'scale': 3}
        reader = BinLogStreamReader(
            connection_settings={
                'host': config['host'], 'port': int(config['port']), 'user': config['user'],
                'passwd': config['password'], 'ssl': {'': True},
            },
            server_id=int(uuid.uuid4().hex[:7], 16), only_schemas=[config['database']], only_tables=[table],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, TableMapEvent,
                         GtidEvent, MariadbGtidEvent, QueryEvent, RotateEvent, XidEvent],
            log_file=log_file, log_pos=log_pos, resume_stream=True, blocking=False, is_mariadb=is_mariadb,
        )
        state = {'bookmarks': {stream.tap_stream_id: {'log_file': log_file, 'log_pos': log_pos, 'version': 1}}}
        messages = []
        with patch('singer.write_message', side_effect=messages.append), \
                patch.object(binlog, 'discover_catalog', wraps=discover_catalog) as discovery:
            binlog._run_binlog_sync(
                MySQLConnection(config), reader, binlog.generate_streams_map([stream]), state,
                config, end_file, end_pos,
            )
        records = [message.record for message in messages if isinstance(message, singer.RecordMessage)]
        assert [record['amount'] for record in records] == [
            '123.45', '1.23', '2.345', '3.456', '3.456', 12.345, '4.567',
        ]
        schemas = [message.schema['properties']['amount'] for message in messages
                   if isinstance(message, singer.SchemaMessage)]
        assert [schema.get('decimal', schema['type']) for schema in schemas] == [
            {'precision': 5, 'scale': 2}, {'precision': 5, 'scale': 3}, ['null', 'number'],
            {'precision': 5, 'scale': 3},
        ]
        assert discovery.call_count == 4
        assert records[4]['_sdc_deleted_at'] is not None
        assert state['bookmarks'][stream.tap_stream_id]['log_pos'] == end_pos
    finally:
        if reader is not None:
            reader.close()
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'DROP TABLE IF EXISTS {qualified}')
