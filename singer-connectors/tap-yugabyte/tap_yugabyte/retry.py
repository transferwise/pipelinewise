"""Retry policy for reads against a distributed source.

YugabyteDB surfaces a class of failures that a single-node Postgres never does:
a read whose scan outlives the clock-skew ambiguity window is asked to restart,
a tablet leader election makes a connection vanish mid-scan, a catalog version
bump from unrelated DDL invalidates a pinned snapshot. None of these mean the
query was wrong -- they mean it needs to be asked again.

Every statement this tap issues is a SELECT, and the full-table scan bookmarks
its position per bucket, so re-running one is idempotent: at worst a handful of
rows are re-emitted, and the target upserts them by primary key. That makes the
safe default the inverse of the usual one -- retry unless the error is known to
be permanent, rather than retry only when it is known to be transient.

The permanent list is deliberately short and covers only errors where retrying
cannot help and would hide a real misconfiguration: a missing privilege, a table
or column that does not exist, or the tap issuing a statement in a context
YugabyteDB forbids.
"""

import random
import time

import psycopg2
import singer

LOGGER = singer.get_logger('tap_yugabyte')

DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_INITIAL_BACKOFF_SECONDS = 0.5
DEFAULT_MAX_BACKOFF_SECONDS = 60.0

# Retrying these can never succeed: the cluster is telling us the request itself
# is wrong, not that it arrived at a bad moment. Failing fast keeps a
# misconfigured pipeline from looking like a slow one.
PERMANENT_SQLSTATES = frozenset({
    '28000',  # invalid_authorization_specification
    '28P01',  # invalid_password
    '42501',  # insufficient_privilege -- e.g. yb_read_time without superuser
    '42P01',  # undefined_table
    '42703',  # undefined_column
    '42883',  # undefined_function -- e.g. yb_hash_code on an unsupported build
    '3D000',  # invalid_catalog_name
    '3F000',  # invalid_schema_name
    '25001',  # active_sql_transaction -- yb_read_time inside BEGIN/COMMIT
    '22023',  # invalid_parameter_value
    '42704',  # undefined_object -- e.g. a GUC this server build does not have
    '42601',  # syntax_error -- deterministic, retrying only delays the failure
    '42P02',  # undefined_parameter
    '42804',  # datatype_mismatch
    '42846',  # cannot_coerce
})

# Retry, but not eight times. A SQLSTATE lands here when it covers two failures
# that deserve different treatment and nothing on the exception separates them.
#
# 57014 query_canceled is the case. The server answers it both when something
# cancels a statement and when `statement_timeout` expires. A cancel is
# genuinely transient: the second attempt runs unimpeded and succeeds. A
# statement_timeout shorter than one FETCH is the opposite -- the cap applies to
# every attempt equally, so all eight are guaranteed to fail identically, and
# the scan emits zero rows after burning the whole backoff schedule (measured:
# ~45s of pure backoff per bucket, and it fails anyway).
#
# Two attempts serve both. The cancel recovers on the second, which is all it
# ever needed. The misconfigured timeout fails in the time of two statements
# rather than eight plus backoff, and surfaces the real error promptly instead
# of leaving an operator watching a pipeline that is not doing anything.
#
# Deliberately NOT separated by message text. The distinguishing string --
# 'canceling statement due to statement timeout' against '...due to user
# request' -- is a server-side message: it is rewritten between releases and
# translated by lc_messages, so matching it fails open on exactly the clusters
# least like the one it was written against. The cap reaches the same outcome
# from the SQLSTATE alone.
MAX_ATTEMPTS_BY_SQLSTATE = {
    '57014': 2,  # query_canceled -- a cancel and a statement_timeout, same code
}

# Named only so operators can recognise them in logs; the policy retries anything
# absent from PERMANENT_SQLSTATES, so this list does not gate behaviour.
KNOWN_TRANSIENT_SQLSTATES = {
    '40001': 'serialization failure / read restart required',
    '40P01': 'deadlock detected',
    '57014': 'statement cancelled (timeout)',
    '57P01': 'server terminated the connection (admin shutdown)',
    '57P02': 'server crashed and is recovering',
    '57P03': 'server not yet accepting connections',
    '08000': 'connection exception',
    '08003': 'connection does not exist',
    '08006': 'connection failure',
    '08001': 'client could not establish connection',
    '08004': 'server rejected the connection',
    '53300': 'too many connections',
    '55006': 'object in use -- e.g. replication slot still active',
    'XX000': 'internal error -- e.g. MISMATCHED_SCHEMA after a catalog bump',
}


class PermanentSourceError(Exception):
    """A source error that retrying cannot fix."""


def _sqlstate(exc):
    return getattr(exc, 'pgcode', None)


def is_permanent(exc):
    """True when re-running the statement cannot change the outcome.

    An error carrying no SQLSTATE reached us before the server could classify it
    -- a dropped socket, a DNS failure, a half-open connection -- which is
    exactly the transient case, so it is not treated as permanent.
    """
    state = _sqlstate(exc)
    if state is None:
        return False
    return state in PERMANENT_SQLSTATES


def attempt_cap(exc, max_attempts):
    """How many attempts this error is worth, never more than `max_attempts`.

    Read off the error that just occurred rather than the run as a whole: the
    schedule a fault deserves is a property of the fault. An uncapped SQLSTATE
    -- or none at all, which is every connection-level fault -- gets the full
    allowance.
    """
    capped = MAX_ATTEMPTS_BY_SQLSTATE.get(_sqlstate(exc))
    if capped is None:
        return max_attempts
    return min(max_attempts, capped)


def _backoff_seconds(attempt, initial, maximum):
    """Exponential backoff with full jitter.

    Workers all fail together when a tablet moves, so without jitter they would
    also all retry together and re-create the contention they are backing off
    from.
    """
    ceiling = min(maximum, initial * (2 ** (attempt - 1)))
    return random.uniform(0, ceiling)


# pylint: disable=too-many-arguments,too-many-positional-arguments
def retry_read(operation, description, max_attempts=DEFAULT_MAX_ATTEMPTS,
               initial_backoff=DEFAULT_INITIAL_BACKOFF_SECONDS,
               max_backoff=DEFAULT_MAX_BACKOFF_SECONDS, on_retry=None):
    """Run `operation`, re-running it on any non-permanent source error.

    `operation` must be safe to run more than once. It is called with no
    arguments and should open its own connection and cursor: a connection that
    has already raised is not reusable, and a server-side cursor cannot be
    rewound, so recovery means starting the statement over rather than resuming
    the one that failed.

    `on_retry` is invoked before each re-run so the caller can re-read its
    bookmark and narrow the replacement query to the rows it still needs.

    Some SQLSTATEs stop short of `max_attempts`; see MAX_ATTEMPTS_BY_SQLSTATE.
    Giving up under a cap raises the source error exactly as exhausting the
    full allowance does -- it is a shorter schedule, not a new verdict, so a
    caller cannot tell the two apart and nothing downstream has to.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except (psycopg2.Error, OSError) as exc:
            last_exc = exc
            state = _sqlstate(exc)
            if is_permanent(exc):
                raise PermanentSourceError(
                    f'{description} failed permanently (SQLSTATE {state}): {exc}'
                ) from exc
            allowed = attempt_cap(exc, max_attempts)
            if attempt >= allowed:
                if allowed < max_attempts:
                    LOGGER.warning(
                        '%s giving up after attempt %s: SQLSTATE %s (%s) is '
                        'capped at %s attempts of the %s configured, because '
                        'the retries it does not get are the ones that could '
                        'not have differed. Last error: %s',
                        description, attempt, state,
                        KNOWN_TRANSIENT_SQLSTATES.get(state, 'unclassified'),
                        allowed, max_attempts,
                        str(exc).strip().splitlines()[0],
                    )
                break
            delay = _backoff_seconds(attempt, initial_backoff, max_backoff)
            LOGGER.warning(
                '%s failed on attempt %s/%s (SQLSTATE %s: %s); retrying in %.2fs: %s',
                description, attempt, allowed, state,
                KNOWN_TRANSIENT_SQLSTATES.get(state, 'unclassified'),
                delay, str(exc).strip().splitlines()[0],
            )
            if on_retry is not None:
                on_retry(attempt, exc)
            time.sleep(delay)
    raise last_exc
