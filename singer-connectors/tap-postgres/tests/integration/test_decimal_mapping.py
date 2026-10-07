"""Exact SQL numeric extraction from dev-project SELECT and wal2json output."""

import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import psycopg2
import pytest
import singer
from singer import metadata

from tap_postgres import db
from tap_postgres.discovery_utils import discover_db
from tap_postgres.sync_strategies import incremental, logical_replication


EXACT = '12345678901234567890.123456789012345678'
CHANGED = '12345678901234567890.123456789012345679'


def decimal_config():
    if not os.environ.get('TAP_POSTGRES_DB'):
        pytest.skip('TAP_POSTGRES_DB is required for the dev-project decimal probe')
    config = {key: os.environ[f'TAP_POSTGRES_{key.upper()}'] for key in ('host', 'port', 'user', 'password')}
    config.update(dbname=os.environ['TAP_POSTGRES_DB'], decimal_target='snowflake', use_secondary=False, limit=None)
    return config


@pytest.mark.parametrize(('precision', 'scale', 'value'), [(10, -2, '12300'), (3, 5, '0.00123')])
def test_signed_scale_and_scale_above_precision_survive_real_discovery(precision, scale, value):
    config = decimal_config()
    table = 'decimal_scale_' + uuid.uuid4().hex[:12]
    connection = psycopg2.connect(**{key: config[key] for key in ('host', 'port', 'user', 'password', 'dbname')})
    connection.autocommit = True
    try:
        if connection.server_version < 150000:
            pytest.skip('Negative scale and scale above precision require PostgreSQL 15 or later')
        with connection.cursor() as cursor:
            try:
                cursor.execute(f'CREATE TABLE public.{table} (id INT PRIMARY KEY, amount NUMERIC({precision},{scale}))')
                cursor.execute(f'INSERT INTO public.{table} VALUES (1, %s)', (value,))
                with db.open_connection(config) as discovery_connection:
                    stream = discover_db(discovery_connection, 'public', [table], decimal_target='snowflake')[0]
                assert stream['schema']['properties']['amount']['decimal'] == {'precision': precision, 'scale': scale}
                cursor.execute(f'SELECT id, amount FROM public.{table}')
                record = db.selected_row_to_singer_message(
                    stream, cursor.fetchone(), 1, ['id', 'amount'], None, metadata.to_map(stream['metadata']),
                )
                assert Decimal(record.record['amount']) == Decimal(value)
                assert logical_replication.changed_decimal_columns(
                    {'columns': [{'name': 'amount', 'type': f'numeric({precision},{scale})'}]}, stream,
                ) == set()
            finally:
                cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
    finally:
        connection.close()


@pytest.mark.parametrize('legacy_float', [False, True])
def test_incremental_decimal_bookmark_replays_adjacent_values(legacy_float):
    config = decimal_config()
    table = 'decimal_resume_' + uuid.uuid4().hex[:12]
    rows = [(1, '9999999999999999.98'), (2, '9999999999999999.99'), (3, '10000000000000000.00')]
    connection = psycopg2.connect(**{key: config[key] for key in ('host', 'port', 'user', 'password', 'dbname')})
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute(f'CREATE TABLE public.{table} (id INT PRIMARY KEY, amount NUMERIC(20,2))')
                cursor.executemany(f'INSERT INTO public.{table} VALUES (%s, %s)', rows)
                with db.open_connection(config) as discovery_connection:
                    stream = discover_db(discovery_connection, 'public', [table], decimal_target='snowflake')[0]
                md_map = metadata.to_map(stream['metadata'])
                md_map[()]['replication-key'] = 'amount'
                bookmark = float(rows[1][1]) if legacy_float else rows[1][1]
                state = {'bookmarks': {stream['tap_stream_id']: {'replication_key_value': bookmark}}}
                with patch('singer.write_message') as write:
                    result = incremental.sync_table(config, stream, state, ['id', 'amount'], md_map)
                records = [call.args[0].record for call in write.call_args_list
                           if isinstance(call.args[0], singer.RecordMessage)]
                assert [(record['id'], record['amount']) for record in records] == (rows if legacy_float else rows[1:])
                assert result['bookmarks'][stream['tap_stream_id']]['replication_key_value'] == rows[-1][1]
            finally:
                cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
    finally:
        connection.close()


@pytest.mark.parametrize('target_type', ['snowflake', 'postgres'])
def test_bounded_numeric_nan_survives_select_and_wal(target_type):
    config = decimal_config()
    config['decimal_target'] = target_type
    table = 'decimal_nan_' + uuid.uuid4().hex[:12]
    connection = psycopg2.connect(**{key: config[key] for key in ('host', 'port', 'user', 'password', 'dbname')})
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute(f'CREATE TABLE public.{table} (id INT PRIMARY KEY, amount NUMERIC(18,2), '
                               'approximate FLOAT8, enabled BOOLEAN, payload JSONB, nullable NUMERIC(18,2), '
                               'unbounded NUMERIC)')
                cursor.execute(f'ALTER TABLE public.{table} REPLICA IDENTITY FULL')
                cursor.execute('SELECT * FROM pg_create_logical_replication_slot(%s, %s, true)', (table, 'wal2json'))
                cursor.execute(f'INSERT INTO public.{table} VALUES (1, %s, 1.25, TRUE, %s, NULL, %s)',
                               ('NaN', '{"nested":1.25}', 'Infinity'))
                with db.open_connection(config) as discovery_connection:
                    stream = discover_db(discovery_connection, 'public', [table], decimal_target=target_type)[0]
                columns = ['id', 'amount', 'approximate', 'enabled', 'payload', 'nullable', 'unbounded']
                cursor.execute(f'SELECT id, amount, approximate, enabled, payload::text, nullable, unbounded '
                               f'FROM public.{table}')
                record = db.selected_row_to_singer_message(
                    stream, cursor.fetchone(), 1, columns, None, metadata.to_map(stream['metadata']),
                )
                assert json.loads(singer.messages.format_message(record))['record']['amount'] == 'NaN'
                cursor.execute(f'UPDATE public.{table} SET amount = %s WHERE id = 1', ('NaN',))
                cursor.execute(f'DELETE FROM public.{table} WHERE id = 1')
                cursor.execute(
                    "SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL, 'format-version', '2', "
                    "'include-types', 'true', 'include-typmod', 'true', 'actions', 'insert,update,delete', "
                    "'numeric-data-types-as-string', 'true', "
                    "'add-tables', %s)", (table, f'public.{table}'),
                )
                state = {'bookmarks': {stream['tap_stream_id']: {'version': 1}}}
                with patch('singer.write_message') as write:
                    for (payload,) in cursor.fetchall():
                        logical_replication.consume_message(
                            [stream], state, SimpleNamespace(payload=payload, data_start=1),
                            datetime.now(timezone.utc), config,
                        )
                records = [call.args[0].record for call in write.call_args_list
                           if isinstance(call.args[0], singer.RecordMessage)]
                assert [record['amount'] for record in records] == ['NaN', 'NaN', 'NaN']
                for record in records:
                    assert record['id'] == 1 and isinstance(record['id'], int)
                    assert record['approximate'] == 1.25 and isinstance(record['approximate'], float)
                    assert record['enabled'] is True
                    assert record['payload'] == {'nested': 1.25}
                    assert record['nullable'] is None
                    assert record['unbounded'] == 'Infinity'
                assert records[-1]['_sdc_deleted_at'] is not None
            finally:
                cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
    finally:
        connection.close()


def test_numeric_nan_bookmark_replays_later_finite_rows_with_contextual_warning():
    config = decimal_config()
    table = 'decimal_nan_resume_' + uuid.uuid4().hex[:12]
    connection = psycopg2.connect(**{key: config[key] for key in ('host', 'port', 'user', 'password', 'dbname')})
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute(f'CREATE TABLE public.{table} (id INT PRIMARY KEY, amount NUMERIC(18,2))')
                cursor.execute(f'INSERT INTO public.{table} VALUES (1, %s)', ('NaN',))
                with db.open_connection(config) as discovery_connection:
                    stream = discover_db(discovery_connection, 'public', [table], decimal_target='snowflake')[0]
                md_map = metadata.to_map(stream['metadata'])
                md_map[()]['replication-key'] = 'amount'
                with patch('singer.write_message'):
                    state = incremental.sync_table(config, stream, {}, ['id', 'amount'], md_map)
                assert state['bookmarks'][stream['tap_stream_id']]['replication_key_value'] == 'NaN'
                cursor.execute(f'INSERT INTO public.{table} VALUES (2, %s)', ('1.25',))
                with patch('singer.write_message') as write, patch.object(incremental.LOGGER, 'warning') as warning:
                    result = incremental.sync_table(config, stream, state, ['id', 'amount'], md_map)
                records = [call.args[0].record for call in write.call_args_list
                           if isinstance(call.args[0], singer.RecordMessage)]
                assert [(record['id'], record['amount']) for record in records] == [(2, '1.25'), (1, 'NaN')]
                assert result['bookmarks'][stream['tap_stream_id']]['replication_key_value'] == 'NaN'
                assert warning.call_args.args[1:] == (stream['tap_stream_id'], 'amount')
                assert 'full stream' in warning.call_args.args[0]
            finally:
                cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
    finally:
        connection.close()


def test_numeric_option_retry_recovers_from_a_real_replication_server_error():
    """An old-plugin response must leave the connection usable for one retry."""
    config = decimal_config()
    parameters = {key: config[key] for key in ('host', 'port', 'user', 'password', 'dbname')}
    table = 'decimal_option_' + uuid.uuid4().hex[:12]
    connection = psycopg2.connect(**parameters)
    connection.autocommit = True
    replication = None
    slot_created = False
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE TABLE public.{table} (id INT PRIMARY KEY, amount NUMERIC(18,2))')
            cursor.execute('SELECT * FROM pg_create_logical_replication_slot(%s, %s)', (table, 'wal2json'))
            slot_created = True
            with db.open_connection(config) as discovery_connection:
                stream = discover_db(discovery_connection, 'public', [table], decimal_target='postgres')[0]
        replication = psycopg2.connect(**parameters, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        replication_cursor = replication.cursor()
        calls = []

        def start_with_old_plugin_error(**kwargs):
            calls.append(kwargs)
            if 'numeric-data-types-as-string' in kwargs['options']:
                old_options = {key: value for key, value in kwargs['options'].items()
                               if key != 'numeric-data-types-as-string'}
                old_options['pipelinewise-unknown-test-option'] = True
                try:
                    replication_cursor.start_replication(**{**kwargs, 'options': old_options})
                except psycopg2.errors.InvalidParameterValue as exc:
                    raise psycopg2.errors.InvalidParameterValue(
                        'option "numeric-data-types-as-string" = "true" is unknown') from exc
            else:
                replication_cursor.start_replication(**kwargs)

        with patch.object(logical_replication.LOGGER, 'warning') as warning:
            logical_replication._start_replication(
                SimpleNamespace(execute=replication_cursor.execute, start_replication=start_with_old_plugin_error),
                [stream], table, 0, connection.server_version, 'postgres',
            )
        assert len(calls) == 2
        assert 'numeric-data-types-as-string' not in calls[-1]['options']
        warning.assert_called_once()
    finally:
        if replication is not None:
            replication.close()
        with connection.cursor() as cursor:
            if slot_created:
                cursor.execute('SELECT pg_drop_replication_slot(%s)', (table,))
            cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
        connection.close()


def test_decimal_select_and_wal_keep_source_digits():
    if not os.environ.get('TAP_POSTGRES_DB'):
        pytest.skip('TAP_POSTGRES_DB is required for the dev-project decimal probe')
    config = {key: os.environ[f'TAP_POSTGRES_{key.upper()}'] for key in ('host', 'port', 'user', 'password')}
    config.update(dbname=os.environ['TAP_POSTGRES_DB'], decimal_target='snowflake', use_secondary=False)
    table = 'decimal_probe_' + uuid.uuid4().hex[:12]
    slot = table + '_slot'
    connection = psycopg2.connect(**{key: config[key] for key in ('host', 'port', 'user', 'password', 'dbname')})
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    f'CREATE TABLE public.{table} (id INT PRIMARY KEY, amount NUMERIC(38,18), approximate FLOAT8)')
                cursor.execute(f'ALTER TABLE public.{table} REPLICA IDENTITY FULL')
                cursor.execute('SELECT * FROM pg_create_logical_replication_slot(%s, %s, true)', (slot, 'wal2json'))
                cursor.execute(f'INSERT INTO public.{table} VALUES (1, %s, 1.25)', (EXACT,))
                with db.open_connection(config) as discovery_connection:
                    stream = discover_db(discovery_connection, 'public', [table], decimal_target='snowflake')[0]
                assert stream['schema']['properties']['amount']['decimal'] == {'precision': 38, 'scale': 18}
                cursor.execute(f'SELECT id, amount, approximate FROM public.{table}')
                record = db.selected_row_to_singer_message(
                    stream, cursor.fetchone(), 1, ['id', 'amount', 'approximate'], None,
                    metadata.to_map(stream['metadata']),
                )
                assert json.loads(singer.messages.format_message(record))['record']['amount'] == EXACT
                cursor.execute(f'UPDATE public.{table} SET amount=%s WHERE id=1', (CHANGED,))
                cursor.execute(f'DELETE FROM public.{table} WHERE id=1')
                cursor.execute(
                    "SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL, 'format-version', '2', "
                    "'include-types', 'true', 'include-typmod', 'true', 'actions', 'insert,update,delete', "
                    "'add-tables', %s)", (slot, f'public.{table}'),
                )
                rows = cursor.fetchall()
                state = {'bookmarks': {stream['tap_stream_id']: {'version': 1}}}
                with patch('singer.write_message') as write:
                    for (payload,) in rows:
                        logical_replication.consume_message(
                            [stream], state, SimpleNamespace(payload=payload, data_start=1),
                            datetime.now(timezone.utc), config,
                        )
                records = [
                    call.args[0] for call in write.call_args_list if isinstance(call.args[0], singer.RecordMessage)
                ]
                assert [record.record['amount'] for record in records] == [EXACT, CHANGED, CHANGED]
                assert all(isinstance(record.record['approximate'], float) for record in records)
            finally:
                cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
    finally:
        connection.close()


@pytest.mark.parametrize('target', ['snowflake', 'postgres'])
@pytest.mark.parametrize('numeric_strings', [False, True])
def test_unbounded_numeric_integers_exceeding_python_digit_limit_survive_wal(target, numeric_strings):
    config = decimal_config()
    config['decimal_target'] = target
    table = 'decimal_large_' + uuid.uuid4().hex[:12]
    slot = table + '_slot'
    parameters = {key: config[key] for key in ('host', 'port', 'user', 'password', 'dbname')}
    connection = psycopg2.connect(**parameters)
    connection.autocommit = True
    value = '9' * 5000
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE TABLE public.{table} (id INT PRIMARY KEY, amount NUMERIC, approximate FLOAT8)')
            cursor.execute(f'ALTER TABLE public.{table} REPLICA IDENTITY FULL')
            cursor.execute('SELECT * FROM pg_create_logical_replication_slot(%s, %s, true)', (slot, 'wal2json'))
            with db.open_connection(config) as discovery_connection:
                stream = discover_db(discovery_connection, 'public', [table], decimal_target=target)[0]
            cursor.execute(f'INSERT INTO public.{table} VALUES (1, %s, 1.25)', (value,))
            cursor.execute(f'SELECT id, amount, approximate FROM public.{table}')
            selected = db.selected_row_to_singer_message(
                stream, cursor.fetchone(), 1, ['id', 'amount', 'approximate'], None,
                metadata.to_map(stream['metadata']),
            )
            assert selected.record['amount'] == value
            cursor.execute(f'UPDATE public.{table} SET amount=%s WHERE id=1', ('-' + value,))
            cursor.execute(f'DELETE FROM public.{table} WHERE id=1')
            cursor.execute(
                "SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL, 'format-version', '2', "
                "'include-types', 'true', 'include-typmod', 'true', 'actions', 'insert,update,delete', "
                "'numeric-data-types-as-string', %s, 'add-tables', %s)",
                (slot, str(numeric_strings).lower(), f'public.{table}'),
            )
            state = {'bookmarks': {stream['tap_stream_id']: {'version': 1}}}
            with patch('singer.write_message') as write:
                for (payload,) in cursor.fetchall():
                    state = logical_replication.consume_message(
                        [stream], state, SimpleNamespace(payload=payload, data_start=123),
                        datetime.now(timezone.utc), config,
                    )
            records = [call.args[0].record for call in write.call_args_list
                       if isinstance(call.args[0], singer.RecordMessage)]
            assert [record['amount'] for record in records] == [value, '-' + value, '-' + value]
            assert all(record['id'] == 1 and isinstance(record['id'], int) for record in records)
            assert all(record['approximate'] == 1.25 and isinstance(record['approximate'], float) for record in records)
            assert state['bookmarks'][stream['tap_stream_id']]['lsn'] == 123
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
        connection.close()
    with psycopg2.connect(**parameters) as verification:
        with verification.cursor() as cursor:
            cursor.execute('SELECT to_regclass(%s)', (f'public.{table}',))
            assert cursor.fetchone()[0] is None
            cursor.execute('SELECT count(*) FROM pg_replication_slots WHERE slot_name=%s', (slot,))
            assert cursor.fetchone()[0] == 0


def test_delete_first_after_decimal_type_change_refreshes_replica_identity():
    """A real wal2json DELETE supplies dimensions needed to refresh a stale schema."""
    if not os.environ.get('TAP_POSTGRES_DB'):
        pytest.skip('TAP_POSTGRES_DB is required for the dev-project decimal probe')
    config = {key: os.environ[f'TAP_POSTGRES_{key.upper()}'] for key in ('host', 'port', 'user', 'password')}
    config.update(
        dbname=os.environ['TAP_POSTGRES_DB'], decimal_target='snowflake', use_secondary=False, filter_schemas='public',
    )
    table = 'decimal_delete_' + uuid.uuid4().hex[:12]
    slot = table + '_slot'
    value_after_alter = '10000000000000000.0'
    connection = psycopg2.connect(**{key: config[key] for key in ('host', 'port', 'user', 'password', 'dbname')})
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute(f'CREATE TABLE public.{table} (id INT PRIMARY KEY, amount NUMERIC(18,2))')
                cursor.execute(f'ALTER TABLE public.{table} REPLICA IDENTITY FULL')
                cursor.execute(f'INSERT INTO public.{table} VALUES (1, %s)', ('9999999999999999.99',))
                with db.open_connection(config) as discovery_connection:
                    stream = discover_db(discovery_connection, 'public', [table], decimal_target='snowflake')[0]
                assert stream['schema']['properties']['amount']['decimal'] == {'precision': 18, 'scale': 2}
                cursor.execute('SELECT * FROM pg_create_logical_replication_slot(%s, %s, true)', (slot, 'wal2json'))
                cursor.execute(f'ALTER TABLE public.{table} ALTER COLUMN amount TYPE NUMERIC(19,1)')
                cursor.execute(f'SELECT amount FROM public.{table} WHERE id=1')
                assert cursor.fetchone()[0] == Decimal(value_after_alter)
                cursor.execute(f'DELETE FROM public.{table} WHERE id=1')
                cursor.execute(
                    "SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL, 'format-version', '2', "
                    "'include-types', 'true', 'include-typmod', 'true', 'actions', 'insert,update,delete', "
                    "'add-tables', %s)", (slot, f'public.{table}'),
                )
                rows = cursor.fetchall()
                payloads = [logical_replication.parse_wal_payload(payload, config) for (payload,) in rows]
                changes = [payload for payload in payloads if payload.get('action') in {'I', 'U', 'D'}]
                assert [payload['action'] for payload in changes] == ['D']
                amount = next(column for column in changes[0]['identity'] if column['name'] == 'amount')
                assert amount['type'] == 'numeric(19,1)'
                assert amount['value'] == Decimal(value_after_alter)

                emitted = []
                state = {'bookmarks': {stream['tap_stream_id']: {'version': 1}}}
                with patch('singer.write_message', side_effect=emitted.append), \
                        patch.object(
                            logical_replication.sync_common, 'write_schema_message', side_effect=emitted.append):
                    for (payload,) in rows:
                        logical_replication.consume_message(
                            [stream], state, SimpleNamespace(payload=payload, data_start=1),
                            datetime.now(timezone.utc), config,
                        )
                assert len(emitted) == 2
                assert emitted[0]['type'] == 'SCHEMA'
                assert emitted[0]['schema']['properties']['amount']['decimal'] == {'precision': 19, 'scale': 1}
                assert emitted[0]['key_properties'] == ['id']
                assert isinstance(emitted[1], singer.RecordMessage)
                assert emitted[1].record['amount'] == value_after_alter
                assert emitted[1].record['_sdc_deleted_at'] is not None
                assert state['bookmarks'][stream['tap_stream_id']]['lsn'] == 1
            finally:
                cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
    finally:
        connection.close()
