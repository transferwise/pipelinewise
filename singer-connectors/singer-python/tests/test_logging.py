"""Configured credentials must stay out of logs and CLI exception tracebacks."""

import base64
import io
import json
import logging
import subprocess
import sys
import threading
from urllib.parse import quote, quote_plus

import pytest

import singer.logger as singer_logger
from singer import utils
from singer.logger import CredentialRedactor, configure_log_redaction


@pytest.mark.parametrize('field', ['user', 'username', 'replica_user', 'password', 'replica_password', 'sasl.password'])
def test_credentials_are_redacted_without_hiding_hostnames(field):
    redact = CredentialRedactor([{field: 'source'}])
    assert redact(f'source.example source-replica source_2 "source" {field}="source"') == (
        f'source.example source-replica source_2 "source" {field}="[REDACTED]"'
    )


@pytest.mark.parametrize('render', [str, quote, quote_plus, repr, json.dumps])
def test_escaped_passwords_and_nested_credentials_are_redacted(render):
    password = 'secret / "value" \\ 日本語'
    redact = CredentialRedactor([{'credentials': {'password': password}}])
    assert render(password) not in redact('Password: ' + render(password))
    assert '[REDACTED]' in redact('Password: ' + render(password))


def test_short_passwords_and_basic_auth_are_redacted():
    redact = CredentialRedactor([{'username': 'source', 'password': 'p'}])
    assert redact('source.example user="source" password="p"') == (
        'source.example user="[REDACTED]" password="[REDACTED]"'
    )
    encoded = base64.b64encode(b'source:p').decode()
    assert redact('Authorization: Basic ' + encoded) == 'Authorization: Basic [REDACTED]'
    assert redact('Read 1 row from analytics.public.p') == 'Read 1 row from analytics.public.p'
    assert redact('mongodb://source:p@source.example/analytics') == (
        'mongodb://[REDACTED]@source.example/analytics'
    )


@pytest.mark.parametrize('password', ['1', 1, 0])
def test_numeric_passwords_do_not_hide_counts_or_same_name_databases(password):
    redact = CredentialRedactor([{'user': 'analytics', 'password': password}])
    text = ('dbname=\'analytics\' "analytics"."orders" analytics.public.orders rows=1 user=analytics password='
            + str(password))
    assert redact(text) == (
        'dbname=\'analytics\' "analytics"."orders" analytics.public.orders rows=1 user=[REDACTED] password=[REDACTED]'
    )
    assert redact("Access denied for user 'analytics'@'source.example'") == (
        "Access denied for user '[REDACTED]'@'source.example'"
    )
    encoded = base64.b64encode(f'analytics:{password}'.encode()).decode()
    assert redact('Authorization: Basic ' + encoded) == 'Authorization: Basic [REDACTED]'


@pytest.mark.parametrize('field', [
    'private_key_passphrase', 'aws_secret_access_key', 'client_side_encryption_master_key', 'api_key',
])
def test_secret_keys_and_passphrases_are_redacted(field):
    redact = CredentialRedactor([{field: 'private-secret-value'}])
    assert redact(json.dumps({field: 'private-secret-value'})) == json.dumps({field: '[REDACTED]'})
    assert redact('Rejected private-secret-value') == 'Rejected [REDACTED]'


@pytest.mark.parametrize('render', [str, repr, json.dumps])
def test_mysql_private_key_content_is_redacted(render):
    key = '-----BEGIN PRIVATE KEY-----\nSYNTHETIC_TEST_KEY_CONTENT\n-----END PRIVATE KEY-----'
    redact = CredentialRedactor([{'ssl_key': key}])
    assert redact('ssl_key=' + render(key)) == 'ssl_key=' + render('[REDACTED]')
    assert redact('TLS configuration rejected ' + key) == 'TLS configuration rejected [REDACTED]'


def test_key_paths_and_kms_identifiers_remain_available_for_diagnosis():
    redact = CredentialRedactor([{
        'private_key': '/keys/snowflake.pem', 'encryption_key': 'alias/stage-kms', 'replication_key': 'last_updated',
    }])
    text = 'private_key=/keys/snowflake.pem encryption_key=alias/stage-kms replication_key=last_updated'
    assert redact(text) == text


def test_twilio_basic_auth_credentials_are_redacted():
    redact = CredentialRedactor([{'account_sid': 'dummy-account', 'auth_token': 'dummy-token'}])
    encoded = base64.b64encode(b'dummy-account:dummy-token').decode()
    assert redact('user=dummy-account password=dummy-token Basic ' + encoded) == (
        'user=[REDACTED] password=[REDACTED] Basic [REDACTED]'
    )


def test_custom_formats_multiple_handlers_and_exception_text_are_protected(monkeypatch):
    monkeypatch.setattr(singer_logger, '_LOG_REDACTOR', CredentialRedactor())
    output = io.StringIO()
    logger = logging.getLogger('credential-test')
    handlers = [logging.StreamHandler(output), logging.StreamHandler(output)]
    for handler in handlers:
        handler.setFormatter(logging.Formatter('custom %(levelname)s %(message)s'))
    monkeypatch.setattr(logger, 'handlers', handlers)
    monkeypatch.setattr(logger, 'propagate', False)
    monkeypatch.setattr(logger, 'level', logging.INFO)
    configure_log_redaction({'user': 'private-login', 'password': 'private-password'})
    try:
        raise RuntimeError('authentication failed for user private-login with private-password')
    except RuntimeError:
        logger.exception('Cannot connect user %s', 'private-login')
    text = output.getvalue()
    assert text.count('custom ERROR Cannot connect user [REDACTED]') == 2
    assert text.count('RuntimeError: authentication failed for user [REDACTED] with [REDACTED]') == 2
    assert 'private-login' not in text and 'private-password' not in text


def test_config_validation_registers_credentials_and_preserves_custom_exception_hook(monkeypatch):
    monkeypatch.setattr(singer_logger, '_LOG_REDACTOR', CredentialRedactor())

    def custom_hook(*_args):
        pass

    monkeypatch.setattr(sys, 'excepthook', custom_hook)
    monkeypatch.setattr(threading, 'excepthook', custom_hook)
    with pytest.raises(RuntimeError, match='missing required keys'):
        utils.check_config({'user': 'private-login'}, ['host'])
    assert singer_logger._LOG_REDACTOR('user private-login') == 'user [REDACTED]'
    assert sys.excepthook is custom_hook
    assert threading.excepthook is custom_hook


def test_logger_reconfiguration_keeps_redaction(monkeypatch):
    monkeypatch.setattr(singer_logger, '_LOG_REDACTOR', CredentialRedactor())
    configure_log_redaction({'password': 'private-password'})
    logger = logging.getLogger('reconfigured-test')
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(logging.Formatter('later %(message)s'))
    monkeypatch.setattr(logger, 'handlers', [handler])
    monkeypatch.setattr(logger, 'propagate', False)
    monkeypatch.setattr(logger, 'level', logging.INFO)
    monkeypatch.setattr(logging.config, 'fileConfig', lambda *_args, **_kwargs: None)
    singer_logger.get_logger(logger.name).info('Password private-password')
    assert output.getvalue() == 'later Password [REDACTED]\n'
    handler.setFormatter(logging.Formatter('changed %(message)s'))
    logger.info('Password private-password')
    assert output.getvalue().endswith('changed Password [REDACTED]\n')


def test_uncaught_cli_exception_is_redacted_and_stdout_is_unchanged():
    code = (
        'import json; from singer import utils; '
        'utils.check_config({"user":"private-login","password":"private-password"}, []); '
        'print(json.dumps({"bookmarks":{"stream":{"value":"private-password"}}})); '
        'raise RuntimeError("authentication failed for user private-login with private-password")'
    )
    result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, check=False)
    assert result.returncode == 1
    assert 'private-login' not in result.stderr and 'private-password' not in result.stderr
    assert 'RuntimeError: authentication failed for user [REDACTED] with [REDACTED]' in result.stderr
    assert json.loads(result.stdout) == {'bookmarks': {'stream': {'value': 'private-password'}}}


def test_uncaught_thread_exception_is_redacted():
    code = (
        'import threading; from singer.logger import configure_log_redaction; '
        'configure_log_redaction({"user":"analytics", "password":1}); '
        'thread = threading.Thread(target=lambda: exec("raise RuntimeError(\'user=analytics password=1 rows=1\')")); '
        'thread.start(); thread.join()'
    )
    result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, check=False)
    assert result.returncode == 0
    assert 'Exception in thread' in result.stderr
    assert 'RuntimeError: user=[REDACTED] password=[REDACTED] rows=1' in result.stderr


def test_source_host_is_reported_once_for_many_connections(monkeypatch):
    monkeypatch.setattr(singer_logger, '_REPORTED_SOURCE_HOSTS', set())
    logger = logging.getLogger('host-test')
    output = io.StringIO()
    monkeypatch.setattr(logger, 'handlers', [logging.StreamHandler(output)])
    monkeypatch.setattr(logger, 'propagate', False)
    monkeypatch.setattr(logger, 'level', logging.INFO)
    for _ in range(10000):
        singer_logger.log_source_host(logger, 'PostgreSQL', 'primary.example')
    singer_logger.log_source_host(logger, 'PostgreSQL', 'replica.example')
    assert output.getvalue().splitlines() == [
        'Connecting to PostgreSQL source host: primary.example',
        'Connecting to PostgreSQL source host: replica.example',
    ]
    logger.setLevel(logging.DEBUG)
    singer_logger.log_source_host(logger, 'PostgreSQL', 'replica.example')
    assert len(output.getvalue().splitlines()) == 3
