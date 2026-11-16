"""Exercise the MariaDB TEXT compatibility contract through the actual Singer CLI."""

import hashlib
import json
import subprocess

import pytest

JSON_SCHEMA = {'type': ['null', 'object', 'array', 'string', 'number', 'boolean'], 'format': 'mariadb-json'}
PRIVATE = 'DO-NOT-EMIT-PRIVATE-VALUE'


def _schema(properties):
    return {'type': 'SCHEMA', 'stream': 'items', 'key_properties': [], 'schema': {
        'type': 'object', 'properties': properties,
    }}


def _invoke(tmp_path, rules, messages=(), catalog=None):
    config_path = tmp_path / 'transformations.json'
    config_path.write_text(json.dumps({'transformations': rules}), encoding='utf-8')
    command = ['transform-field', '--config', str(config_path)]
    if catalog:
        command.extend(['--validate', '--catalog', str(catalog)])
    return subprocess.run(command, input='\n'.join(json.dumps(message) for message in messages),
                          capture_output=True, text=True, timeout=30, check=False)


def test_cli_catalog_validation_preserves_original_format_without_target_configuration(tmp_path):
    catalog = tmp_path / 'catalog.json'
    catalog.write_text(json.dumps({'streams': [{
        'stream': 'items', 'tap_stream_id': 'items', 'schema': _schema({'payload': JSON_SCHEMA})['schema'],
        'metadata': [{'breadcrumb': [], 'metadata': {'selected': True}}],
    }]}), encoding='utf-8')
    original = catalog.read_bytes()
    result = _invoke(tmp_path, [{'tap_stream_name': 'items', 'field_id': 'payload', 'type': 'HASH'}], catalog=catalog)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ''
    assert catalog.read_bytes() == original


def test_cli_transforms_private_json_as_text_and_sends_schema_record_state_in_order(tmp_path):
    raw = f'  {{ "private" : "{PRIVATE}", "amount": 1.5000 }}  '
    rules = [{'tap_stream_name': 'items', 'field_id': 'payload', 'type': 'HASH'}]
    result = _invoke(tmp_path, rules, [
        _schema({'payload': JSON_SCHEMA}), {'type': 'RECORD', 'stream': 'items', 'record': {'payload': raw}},
        {'type': 'STATE', 'value': {'position': 9}},
    ])
    assert result.returncode == 0, result.stderr
    messages = [json.loads(line) for line in result.stdout.splitlines()]
    assert [message['type'] for message in messages] == ['SCHEMA', 'RECORD', 'STATE']
    assert messages[0]['schema']['properties']['payload'] == {**JSON_SCHEMA, 'format': 'mariadb-json-text'}
    assert messages[1]['record']['payload'] == hashlib.sha256(raw.encode('utf-8')).hexdigest()
    assert messages[2]['value'] == {'position': 9}
    assert PRIVATE not in result.stdout + result.stderr


@pytest.mark.parametrize('failure', ['transport', 'condition'])
def test_cli_failure_never_emits_private_record_or_acknowledges_state(tmp_path, failure):
    rules = [{'tap_stream_name': 'items', 'field_id': 'payload', 'type': 'HASH'}]
    value = {'private': PRIVATE} if failure == 'transport' else json.dumps({'private': PRIVATE})
    if failure == 'condition':
        rules[0]['when'] = [{'column': 'payload', 'regex_match': f'[{PRIVATE}'}]
    result = _invoke(tmp_path, rules, [
        _schema({'payload': JSON_SCHEMA}), {'type': 'RECORD', 'stream': 'items', 'record': {'payload': value}},
        {'type': 'STATE', 'value': {'position': 9}},
    ])
    assert result.returncode != 0
    assert [json.loads(line)['type'] for line in result.stdout.splitlines()] == ['SCHEMA']
    assert 'Cannot safely apply' in result.stderr
    assert PRIVATE not in result.stdout + result.stderr
