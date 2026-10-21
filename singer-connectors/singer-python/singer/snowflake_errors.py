"""Keep source values out of Snowflake data-error diagnostics."""

import logging
import re

from functools import wraps


_ROW_VALUES = re.compile(r'\brow\s+values\s*:', re.IGNORECASE)
_SQLSTATE = re.compile(r'[A-Z0-9]{5}')
_QUERY_ID = re.compile(r'[A-Za-z0-9_-]{1,128}')


def sanitize_snowflake_error(error):
    """Redact a data-bearing SDK error in place without changing its identity."""
    if not any(
        cls.__module__ == 'snowflake.connector.errors' and cls.__name__ == 'Error'
        for cls in type(error).__mro__
    ):
        return

    code = getattr(error, 'errno', None)
    state = getattr(error, 'sqlstate', None)
    message = f"{getattr(error, 'raw_msg', '')} {getattr(error, 'msg', '')}"
    is_data_error = isinstance(state, str) and state.startswith('22') and _SQLSTATE.fullmatch(state) is not None
    if code != 100090 and not is_data_error and not _ROW_VALUES.search(message):
        return

    summary = 'Duplicate row detected during DML action' if code == 100090 else 'Snowflake data error'
    safe_message = summary + '. Row/value details: [REDACTED]'
    if isinstance(code, int) and not isinstance(code, bool):
        prefix = f'{code:06d}'
        if isinstance(state, str) and _SQLSTATE.fullmatch(state):
            prefix += f' ({state})'
        safe_message = prefix + ': ' + safe_message
    query_id = getattr(error, 'sfqid', None)
    if isinstance(query_id, str) and _QUERY_ID.fullmatch(query_id):
        safe_message += f'. Query ID: {query_id}'

    # SDK str(), repr(), raw messages and multiprocessing serialization use different fields.
    error.msg = error.raw_msg = safe_message
    error.args = (safe_message,)
    error.query = None
    error.__cause__ = error.__context__ = None
    error.__suppress_context__ = True
    if getattr(error, '__notes__', None):
        error.__notes__ = ['Snowflake error notes: [REDACTED]']


def _protect_driver_logging():
    """Prevent SDK DEBUG responses from logging source values before it raises."""
    parent = logging.getLogger('snowflake.connector')
    if parent.getEffectiveLevel() < logging.WARNING:
        parent.setLevel(logging.WARNING)
    for name, logger in tuple(logging.Logger.manager.loggerDict.items()):
        if name.startswith('snowflake.connector.') and isinstance(logger, logging.Logger):
            if logger.getEffectiveLevel() < logging.WARNING:
                logger.setLevel(logging.WARNING)


def sanitize_snowflake_errors(function):
    """Protect a Snowflake boundary while preserving failure and retry behavior."""
    @wraps(function)
    def protected(*args, **kwargs):
        _protect_driver_logging()
        try:
            return function(*args, **kwargs)
        except Exception as error:
            sanitize_snowflake_error(error)
            raise

    return protected
