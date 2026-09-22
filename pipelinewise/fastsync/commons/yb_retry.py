"""Retry policy for FastSync reads against a distributed source.

The same reasoning as the tap's own retry module: every statement FastSync
issues against YugabyteDB is a read, and a bulk export is restarted from the
beginning rather than resumed, so re-running one changes nothing but the time it
takes. Errors are therefore retried unless they are known to be permanent.

The permanent list covers only cases where a retry cannot help and would hide a
misconfiguration -- a missing privilege, a missing object, or a statement issued
in a context YugabyteDB forbids.
"""

import logging
import random
import time

import psycopg2

LOGGER = logging.getLogger(__name__)

DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_INITIAL_BACKOFF_SECONDS = 0.5
DEFAULT_MAX_BACKOFF_SECONDS = 60.0

PERMANENT_SQLSTATES = frozenset({
    '28000',  # invalid_authorization_specification
    '28P01',  # invalid_password
    '42501',  # insufficient_privilege -- e.g. yb_read_time without superuser
    '42P01',  # undefined_table
    '42703',  # undefined_column
    '42883',  # undefined_function
    '3D000',  # invalid_catalog_name
    '3F000',  # invalid_schema_name
    '25001',  # active_sql_transaction -- yb_read_time inside BEGIN/COMMIT
    '22023',  # invalid_parameter_value
    '42601',  # syntax_error -- deterministic, retrying only delays the failure
    '42P02',  # undefined_parameter
    '42804',  # datatype_mismatch
    '42846',  # cannot_coerce
})


class PermanentSourceError(Exception):
    """A source error that retrying cannot fix."""


def is_permanent(exc):
    """True when re-running the statement cannot change the outcome.

    An error with no SQLSTATE never reached the server's classifier -- a dropped
    socket, a DNS failure -- which is the transient case, not a permanent one.
    """
    state = getattr(exc, 'pgcode', None)
    return state is not None and state in PERMANENT_SQLSTATES


def _backoff_seconds(attempt, initial, maximum):
    """Exponential backoff with full jitter, so concurrent exports that fail
    together on a tablet move do not retry together."""
    return random.uniform(0, min(maximum, initial * (2 ** (attempt - 1))))


# pylint: disable=too-many-arguments,too-many-positional-arguments
def retry_read(operation, description, max_attempts=DEFAULT_MAX_ATTEMPTS,
               initial_backoff=DEFAULT_INITIAL_BACKOFF_SECONDS,
               max_backoff=DEFAULT_MAX_BACKOFF_SECONDS, before_retry=None):
    """Run `operation`, re-running it on any non-permanent source error.

    `operation` must be safe to run more than once, and must recreate any state
    it owns: a bulk export re-opens its output file, since a failed attempt
    leaves a partial one behind. `before_retry` runs first so the caller can
    discard that partial output.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except (psycopg2.Error, OSError) as exc:
            last_exc = exc
            state = getattr(exc, 'pgcode', None)
            if is_permanent(exc):
                raise PermanentSourceError(
                    f'{description} failed permanently (SQLSTATE {state}): {exc}'
                ) from exc
            if attempt == max_attempts:
                break
            delay = _backoff_seconds(attempt, initial_backoff, max_backoff)
            LOGGER.warning(
                '%s failed on attempt %s/%s (SQLSTATE %s); retrying in %.2fs: %s',
                description, attempt, max_attempts, state, delay,
                str(exc).strip().splitlines()[0],
            )
            if before_retry is not None:
                before_retry(attempt, exc)
            time.sleep(delay)
    raise last_exc
