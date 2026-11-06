"""Exact decimal extraction against the isolated dev-project source databases."""

import json
import math
import os
import random
import struct
import uuid
from decimal import Decimal
from unittest.mock import patch

import pytest
import singer
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent, MariadbGtidEvent, QueryEvent, RotateEvent, XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, TableMapEvent, UpdateRowsEvent, WriteRowsEvent
from singer.catalog import Catalog

from tap_mysql import get_non_binlog_streams
from tap_mysql.connection import MySQLConnection, connect_with_backoff
from tap_mysql.discover_utils import discover_catalog
from tap_mysql.sync_strategies import binlog, common, full_table, incremental


EXACT = '12345678901234567890.123456789012345678'
CHANGED = '12345678901234567890.123456789012345679'
# Six-digit FLOAT text changes the first three stored values; -0.0 checks that the sign survives.
FLOAT_INPUTS = (16777217, 1234567.8, 0.1, -0.0)


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


def _float32(value):
    """Return the FLOAT a source stores for a value, as mysql-replication decodes it from the binlog."""
    return struct.unpack('<f', struct.pack('<f', value))[0]


def _float_source_config(prefix):
    if not os.environ.get(f'{prefix}_DB'):
        pytest.skip(f'{prefix}_DB is required for the dev-project FLOAT probe')
    config = {key: os.environ[f'{prefix}_{key.upper()}'] for key in ('host', 'port', 'user', 'password')}
    config['database'] = os.environ[f'{prefix}_DB']
    return config


def _selected_stream(config, table, **stream_metadata):
    stream = discover_catalog(MySQLConnection(config), config['database'], table).streams[0]
    md_map = singer.metadata.to_map(stream.metadata)
    md_map[()].update(stream_metadata)
    for name in stream.schema.properties:
        md_map[('properties', name)]['selected'] = True
    stream.metadata = singer.metadata.to_list(md_map)
    assert md_map[('properties', 'amount')]['datatype'] == 'float'
    return stream


def _wire_records(sync, *args):
    """Run a sync strategy and return its records as the target would parse them."""
    messages = []
    with patch('singer.write_message', side_effect=messages.append):
        sync(*args)
    return [
        json.loads(singer.messages.format_message(message))['record']
        for message in messages if isinstance(message, singer.RecordMessage)
    ]


def _assert_same_floats(actual, expected):
    """Compare values and the signs of zeros, which equality alone ignores."""
    assert actual == expected
    assert [math.copysign(1.0, value) for value in actual] == [math.copysign(1.0, value) for value in expected]


def _drop_table(config, qualified):
    with connect_with_backoff(MySQLConnection(config)) as source:
        source.autocommit(True)
        with source.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS {qualified}')


@pytest.mark.parametrize('prefix', ['TAP_MYSQL', 'TAP_ORACLE_MYSQL'])
def test_float_full_table_and_binlog_load_the_stored_value(prefix):
    """FULL_TABLE used to read FLOAT as six-digit text while binlog decoded the stored value."""
    config = _float_source_config(prefix)
    table = 'float_probe_' + uuid.uuid4().hex[:12]
    qualified = f'`{config["database"]}`.`{table}`'
    reader = None
    try:
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'CREATE TABLE {qualified} (id INT PRIMARY KEY, amount FLOAT)')
                cursor.execute('SHOW MASTER STATUS')
                log_file, log_pos, *_ = cursor.fetchone()
                cursor.executemany(f'INSERT INTO {qualified} VALUES (%s, %s)', list(enumerate(FLOAT_INPUTS, 1)))
                is_mariadb = 'mariadb' in source.get_server_info().lower()
        expected = [_float32(value) for value in FLOAT_INPUTS]
        assert expected[:3] == [16777216.0, 1234567.75, 0.10000000149011612]

        stream = _selected_stream(config, table, **{'replication-method': 'FULL_TABLE'})
        records = _wire_records(full_table.sync_table, MySQLConnection(config), stream, {}, ['id', 'amount'], 1)
        _assert_same_floats([record['amount'] for record in sorted(records, key=lambda r: r['id'])], expected)

        reader = BinLogStreamReader(
            connection_settings={
                'host': config['host'], 'port': int(config['port']), 'user': config['user'],
                'passwd': config['password'], 'ssl': {'': True},
            },
            server_id=int(uuid.uuid4().hex[:7], 16), only_schemas=[config['database']], only_tables=[table],
            only_events=[WriteRowsEvent], log_file=log_file, log_pos=log_pos, resume_stream=True,
            blocking=False, is_mariadb=is_mariadb,
        )
        binlog_values = []
        for event in reader:
            for row in event.rows:
                record = binlog.row_to_singer_record(
                    stream, 1, binlog.get_db_column_types(event), row['values'], None)
                binlog_values.append(json.loads(singer.messages.format_message(record))['record']['amount'])
        _assert_same_floats(binlog_values, expected)
    finally:
        if reader is not None:
            reader.close()
        _drop_table(config, qualified)


@pytest.mark.parametrize('prefix', ['TAP_MYSQL', 'TAP_ORACLE_MYSQL'])
def test_float_full_table_is_exact_across_sampled_values(prefix):
    """Every sampled FLOAT, including ones needing 17 significant digits as DOUBLE, loads unchanged."""
    config = _float_source_config(prefix)
    table = 'float_sample_' + uuid.uuid4().hex[:12]
    qualified = f'`{config["database"]}`.`{table}`'
    rng = random.Random(1805)
    values = []
    while len(values) < 500:
        bits = rng.getrandbits(32)
        # Normal exponents only: the source documents FLOAT's range without subnormals, NaN or infinity.
        if 0 < (bits >> 23) & 0xFF < 0xFF:
            values.append(struct.unpack('<f', bits.to_bytes(4, 'little'))[0])
    try:
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'CREATE TABLE {qualified} (id INT PRIMARY KEY, amount FLOAT)')
                cursor.executemany(f'INSERT INTO {qualified} VALUES (%s, %s)', list(enumerate(values, 1)))

        stream = _selected_stream(config, table, **{'replication-method': 'FULL_TABLE'})
        records = _wire_records(full_table.sync_table, MySQLConnection(config), stream, {}, ['id', 'amount'], 1)
        _assert_same_floats([record['amount'] for record in sorted(records, key=lambda r: r['id'])], values)
        assert any(len(Decimal(repr(value)).as_tuple().digits) == 17 for value in values)
    finally:
        _drop_table(config, qualified)


@pytest.mark.parametrize('prefix', ['TAP_MYSQL', 'TAP_ORACLE_MYSQL'])
def test_float_incremental_values_and_bookmark_are_exact(prefix):
    """A six-digit bookmark is not the stored maximum, so INCREMENTAL resumed from the wrong boundary."""
    config = _float_source_config(prefix)
    table = 'float_incremental_' + uuid.uuid4().hex[:12]
    qualified = f'`{config["database"]}`.`{table}`'
    try:
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'CREATE TABLE {qualified} (id INT PRIMARY KEY, amount FLOAT, KEY (amount))')
                cursor.executemany(f'INSERT INTO {qualified} VALUES (%s, %s)', list(enumerate(FLOAT_INPUTS, 1)))

        stream = _selected_stream(
            config, table, **{'replication-method': 'INCREMENTAL', 'replication-key': 'amount'})
        state = {}
        records = _wire_records(incremental.sync_table, MySQLConnection(config), stream, state, ['id', 'amount'])
        expected = sorted(_float32(value) for value in FLOAT_INPUTS)
        _assert_same_floats([record['amount'] for record in records], expected)
        assert state['bookmarks'][stream.tap_stream_id]['replication_key_value'] == 16777216.0

        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'INSERT INTO {qualified} VALUES (5, 16777218)')
        records = _wire_records(incremental.sync_table, MySQLConnection(config), stream, state, ['id', 'amount'])
        assert [(record['id'], record['amount']) for record in records] == [(1, 16777216.0), (5, 16777218.0)]
        assert state['bookmarks'][stream.tap_stream_id]['replication_key_value'] == 16777218.0
    finally:
        _drop_table(config, qualified)


@pytest.mark.parametrize('prefix', ['TAP_MYSQL', 'TAP_ORACLE_MYSQL'])
def test_float_auto_increment_key_bounds_full_table_by_the_stored_maximum(prefix):
    """A six-digit maximum key of 16777200 would leave the stored 16777216 row out of the copy."""
    config = _float_source_config(prefix)
    table = 'float_key_' + uuid.uuid4().hex[:12]
    qualified = f'`{config["database"]}`.`{table}`'
    # AUTO_INCREMENT generates a new key for an inserted value that rounds to zero, such as 0.1.
    keys = (1.1, 1234567.8, 16777217)
    try:
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(f'CREATE TABLE {qualified} (amount FLOAT AUTO_INCREMENT PRIMARY KEY, label CHAR(1))')
                cursor.executemany(f'INSERT INTO {qualified} VALUES (%s, %s)', [(key, 'x') for key in keys])

        stream = _selected_stream(config, table, **{'replication-method': 'FULL_TABLE'})
        assert full_table.pks_are_auto_incrementing(MySQLConnection(config), stream)
        state = {}
        records = _wire_records(full_table.sync_table, MySQLConnection(config), stream, state, ['amount', 'label'], 1)
        _assert_same_floats([record['amount'] for record in records], [_float32(key) for key in keys])
    finally:
        _drop_table(config, qualified)


@pytest.mark.parametrize('prefix', ['TAP_MYSQL', 'TAP_ORACLE_MYSQL'])
def test_columns_altered_from_float_before_rediscovery_load_their_new_values(prefix):
    """The run pairs the saved catalog's 'float' metadata with live schemas, so promoting would load DOUBLEs."""
    config = _float_source_config(prefix)
    table = 'float_altered_' + uuid.uuid4().hex[:12]
    qualified = f'`{config["database"]}`.`{table}`'
    big = 2 ** 53 + 1
    try:
        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(
                    f'CREATE TABLE {qualified} '
                    '(id FLOAT AUTO_INCREMENT PRIMARY KEY, amount FLOAT, label FLOAT, total FLOAT, rate FLOAT)'
                )
                cursor.execute(f'INSERT INTO {qualified} VALUES (1, 1.25, 2.5, 3, 0.1)')
        stale = _selected_stream(config, table, **{'replication-method': 'FULL_TABLE', 'selected': True})

        with connect_with_backoff(MySQLConnection(config)) as source:
            source.autocommit(True)
            with source.cursor() as cursor:
                cursor.execute(
                    f'ALTER TABLE {qualified} MODIFY id BIGINT AUTO_INCREMENT, MODIFY amount DECIMAL(18,2), '
                    'MODIFY label VARCHAR(10), MODIFY total BIGINT'
                )
                cursor.execute(f"INSERT INTO {qualified} VALUES (%s, 0.10, 'N/A', %s, 0.1)", (big, big))

        resolved = get_non_binlog_streams(
            MySQLConnection(config), Catalog([stale]),
            {'filter_dbs': config['database'], 'decimal_target': 'snowflake'}, {},
        ).streams
        assert [stream.tap_stream_id for stream in resolved] == [stale.tap_stream_id]
        columns = ['id', 'amount', 'label', 'total', 'rate']
        records = _wire_records(full_table.sync_table, MySQLConnection(config), resolved[0], {}, columns, 1)
        assert records == [
            {'id': 1, 'amount': '1.25', 'label': '2.5', 'total': 3, 'rate': _float32(0.1)},
            {'id': big, 'amount': '0.10', 'label': 'N/A', 'total': big, 'rate': _float32(0.1)},
        ]
    finally:
        _drop_table(config, qualified)
