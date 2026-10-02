"""Real Snowflake regressions for busy-stream checkpoints and sparse updates."""

import copy
import io
import json
import uuid
from contextlib import redirect_stdout

import pytest

import target_snowflake
from target_snowflake.db_sync import DbSync, RECORD_UPDATE_MODE_PATCH, RECORD_UPDATE_MODE_SCHEMA_KEY

from .utils import get_test_config


def _schema(stream):
    return {
        'type': 'SCHEMA', 'stream': stream, 'key_properties': ['key1', 'key2'],
        'schema': {
            'type': 'object',
            RECORD_UPDATE_MODE_SCHEMA_KEY: RECORD_UPDATE_MODE_PATCH,
            'properties': {
                name: {'type': ['null', 'string']}
                for name in ('key1', 'key2', 'payload', 'label')
            },
        },
    }


def _record(stream, key1, key2, **values):
    return {'type': 'RECORD', 'stream': stream, 'record': {'key1': key1, 'key2': key2, **values}}


def _state(lsn):
    return {'bookmarks': {stream: {'lsn': lsn} for stream in ('fast', 'slow')}}


@pytest.mark.parametrize('table_format', ['native', 'iceberg'])
def test_busy_streams_checkpoint_durable_rows_before_source_interruption(table_format):
    """Preserve sparse rows and acknowledge only loaded streams before EOF."""
    config = get_test_config()
    schema = f'PW_CHECKPOINT_{uuid.uuid4().hex[:16].upper()}'
    config.update({
        'default_target_schema': schema,
        'data_flattening_max_level': 0,
        'batch_size_rows': 2,
        'batch_wait_limit_seconds': 0,
        'flush_all_streams': False,
        'parallelism': 1,
        'target_table_format': table_format,
        'client_side_encryption_master_key': '',
    })
    if table_format == 'iceberg':
        config['iceberg_version'] = 3
    config['s3_key_prefix'] = f'{config.get("s3_key_prefix") or ""}{schema.lower()}/'
    database = DbSync(config)
    messages = [
        _schema('fast'), _schema('slow'),
        _record('fast', 'a,b', 'c', payload='keep-a', label='initial-a'),
        _record('slow', 's', '1', payload='keep-slow', label='initial-slow'),
        {'type': 'STATE', 'value': _state(10)},
        _record('fast', 'a', 'b,c', payload='keep-b', label='initial-b'),
        _record('slow', 's', '1', label='still-buffered'),
        _record('fast', 'a,b', 'c', label='updated-a'),
        _record('fast', 'a,b', 'c', payload='updated-payload'),
        {'type': 'STATE', 'value': _state(20)},
        _record('fast', 'a', 'b,c', label='updated-b'),
    ]

    def interrupted_input():
        yield from map(json.dumps, messages)
        raise RuntimeError('source interrupted before EOF')

    output = io.StringIO()
    try:
        with redirect_stdout(output), pytest.raises(RuntimeError, match='source interrupted before EOF'):
            target_snowflake.persist_lines(config, interrupted_input())

        final_checkpoint = copy.deepcopy(_state(10))
        final_checkpoint['bookmarks']['fast']['lsn'] = 20
        assert [json.loads(line) for line in output.getvalue().splitlines()] == [_state(10), final_checkpoint]
        assert database.query(f'SELECT KEY1, KEY2, PAYLOAD, LABEL FROM {schema}.FAST ORDER BY KEY1, KEY2') == [
            {'KEY1': 'a', 'KEY2': 'b,c', 'PAYLOAD': 'keep-b', 'LABEL': 'updated-b'},
            {'KEY1': 'a,b', 'KEY2': 'c', 'PAYLOAD': 'updated-payload', 'LABEL': 'updated-a'},
        ]
        assert database.query(f'SELECT KEY1, KEY2, PAYLOAD, LABEL FROM {schema}.SLOW') == [
            {'KEY1': 's', 'KEY2': '1', 'PAYLOAD': 'keep-slow', 'LABEL': 'initial-slow'},
        ]
    finally:
        database.query(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
