import logging
import logging.config
import base64
import json
import os
import re
import sys
import threading
import traceback
from urllib.parse import quote, quote_plus


_IDENTITY_FIELDS = frozenset({'user', 'username', 'email', 'account_sid'})
_SECRET_FIELDS = frozenset({
    'password', 'passwd', 'pwd', 'passphrase', 'secret', 'token', 'api_key', 'ssl_key',
    'aws_secret_access_key', 'secret_access_key', 'client_side_encryption_master_key',
})
_CONTEXT_FIELDS = '|'.join(sorted(_IDENTITY_FIELDS | _SECRET_FIELDS | {'login'}, key=len, reverse=True))
_CREDENTIAL_PREFIX = (
    r'''(?P<prefix>(?i:(?<![\w.-])(?:[\w.-]*[_.-])?(?:''' + _CONTEXT_FIELDS
    + r''')(?:["']?\s*[:=]\s*|\s+)["']?))'''
)
_AUTHENTICATION_PREFIX = (
    r'''(?P<prefix>(?i:(?<![\w.-])(?:Bearer[ \t]+|'''
    r'''(?:(?:authentication|authorization|login)[ _-]*(?:failed|failure|error)'''
    r'''|(?:invalid|incorrect|expired|rejected)[ \t]+(?:credentials?|password|token)'''
    r'''|(?:credentials?|password|token)[ \t]+(?:invalid|incorrect|expired|rejected))'''
    r'''\b[ \t]*[:=]?[ \t]*)(?:\([ \t]*)?["']?))'''
)
_URI_USERINFO = re.compile(r'(?P<scheme>\b[a-z][a-z0-9+.-]*://)[^/\s?#@]+@', re.IGNORECASE)


def _value_pattern(values, prefix):
    if not values:
        return None
    alternatives = '|'.join(re.escape(value) for value in sorted(values, key=len, reverse=True))
    return re.compile(prefix + '(?:' + alternatives + r')(?![\w-]|\.[\w-])')


class CredentialRedactor:
    """Preserve diagnostic names while removing credentials from log text."""

    def __init__(self, configs=()):
        self.context_values = set()
        self.secret_values = set()
        self.short_secret_values = set()
        self.basic_auth_values = set()
        self.context_pattern = self.secret_pattern = self.basic_auth_pattern = None
        self.authentication_pattern = None
        for config in configs:
            self.add_config(config)

    def add_config(self, config):
        for key, value in config.items():
            if isinstance(value, dict):
                self.add_config(value)
            elif isinstance(value, (str, int, float)) and str(value):
                field_name = str(key).lower()
                field = re.split(r'[_.-]', field_name)[-1]
                is_secret = field_name in _SECRET_FIELDS or field in _SECRET_FIELDS
                if is_secret or field_name in _IDENTITY_FIELDS or field in _IDENTITY_FIELDS:
                    text = str(value)
                    variants = {
                        text, quote(text), quote(text, safe=''), quote_plus(text, safe=''),
                        repr(text)[1:-1], json.dumps(text)[1:-1],
                    }
                    self.context_values.update(variants)
                    # Short secrets need credential context so counts and identifiers remain useful.
                    if is_secret:
                        values = self.secret_values if len(text) >= 8 else self.short_secret_values
                        values.update(variants)
        user = next((config[key] for key in ('user', 'username', 'email', 'account_sid')
                     if config.get(key) is not None), None)
        password = next((config[key] for key in ('password', 'api_token', 'auth_token')
                         if config.get(key) is not None), None)
        if user is not None and password is not None:
            self.basic_auth_values.add(base64.b64encode(f'{user}:{password}'.encode()).decode())
            if config.get('email') and config.get('api_token'):
                self.basic_auth_values.add(base64.b64encode(f'{user}/token:{password}'.encode()).decode())
        self.context_pattern = _value_pattern(self.context_values, _CREDENTIAL_PREFIX)
        self.secret_pattern = _value_pattern(self.secret_values, r'(?<![\w.-])')
        self.authentication_pattern = _value_pattern(self.short_secret_values, _AUTHENTICATION_PREFIX)
        self.basic_auth_pattern = _value_pattern(self.basic_auth_values, r'(?P<prefix>(?i:\bBasic\s+))')

    def __call__(self, text):
        text = _URI_USERINFO.sub(r'\g<scheme>[REDACTED]@', text)
        for pattern in (self.context_pattern, self.basic_auth_pattern, self.authentication_pattern):
            if pattern:
                text = pattern.sub(lambda match: match.group('prefix') + '[REDACTED]', text)
        return self.secret_pattern.sub('[REDACTED]', text) if self.secret_pattern else text


_LOG_REDACTOR = CredentialRedactor()
_HANDLER_FORMAT = None
_REPORTED_SOURCE_HOSTS = set()


def _redacting_handler_format(handler, record):
    """Protect rendered messages and exceptions, including handlers added later."""
    return _LOG_REDACTOR(_HANDLER_FORMAT(handler, record))


def _redact_uncaught_exception(exc_type, exc_value, exc_traceback):
    sys.stderr.write(_LOG_REDACTOR(''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))))


def _redact_thread_exception(args):
    if args.exc_type is SystemExit:
        return
    name = args.thread.name if args.thread else 'unknown'
    text = f'Exception in thread {name}:\n'
    text += ''.join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
    sys.stderr.write(_LOG_REDACTOR(text))


def configure_log_redaction(config):
    """Protect standard logging handlers and default CLI/thread tracebacks."""
    global _HANDLER_FORMAT
    _LOG_REDACTOR.add_config(config)
    if logging.Handler.format is not _redacting_handler_format:
        _HANDLER_FORMAT = logging.Handler.format
        logging.Handler.format = _redacting_handler_format
    if sys.excepthook is sys.__excepthook__:
        sys.excepthook = _redact_uncaught_exception
    if threading.excepthook is threading.__excepthook__:
        threading.excepthook = _redact_thread_exception


def log_source_host(logger, source, host):
    """Report each source host once at INFO; repeated connections use DEBUG."""
    identity = (source, str(host))
    level = logging.DEBUG if identity in _REPORTED_SOURCE_HOSTS else logging.INFO
    _REPORTED_SOURCE_HOSTS.add(identity)
    logger.log(level, 'Connecting to %s source host: %s', source, host)


def get_logger(name='singer'):
    """Return a Logger instance to use in singer."""
    # Use custom logging config provided by environment variable
    if 'LOGGING_CONF_FILE' in os.environ and os.environ['LOGGING_CONF_FILE']:
        path = os.environ['LOGGING_CONF_FILE']
        logging.config.fileConfig(path, disable_existing_loggers=False)
    # Use the default logging conf that meets the singer specs criteria
    else:
        this_dir, _ = os.path.split(__file__)
        path = os.path.join(this_dir, 'logging.conf')
        logging.config.fileConfig(path, disable_existing_loggers=False)

    return logging.getLogger(name)
