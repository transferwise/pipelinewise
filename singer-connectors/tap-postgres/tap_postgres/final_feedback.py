"""Keep completed logical sessions alive until the managed target durably acknowledges EOF."""

import copy
import json
import math
import os
import stat
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from select import select
from typing import Any

import psycopg2
import singer
from psycopg2 import extensions
from psycopg2.extensions import connection
from psycopg2.extras import ReplicationCursor

import tap_postgres.db as post_db

LOGGER = singer.get_logger('tap_postgres')
FINAL_FEEDBACK_ENV = 'PIPELINEWISE_POSTGRES_FINAL_FEEDBACK'
FINAL_FEEDBACK_TIMEOUT_SECONDS = 600
POLL_INTERVAL_SECONDS = 0.1
STATUS_INTERVAL_SECONDS = 10


class FinalFeedbackError(RuntimeError):
    """The final durable checkpoint could not be confirmed to PostgreSQL."""


def minimum_acknowledged_lsn(state: dict, logical_streams: list[dict]) -> int:
    """Return the oldest valid durable LSN across all streams in a session."""
    acknowledged_lsns = [singer.get_bookmark(state, stream['tap_stream_id'], 'lsn') for stream in logical_streams]
    if not acknowledged_lsns or any(
            isinstance(lsn, bool) or not isinstance(lsn, int) or lsn < 0 for lsn in acknowledged_lsns):
        raise ValueError('State does not contain a valid LSN for every logical stream')
    return min(acknowledged_lsns)


def read_target_acknowledged_lsn(state_file: str, logical_streams: list[dict], previous_safe_lsn: int) -> int:
    """Read persisted target state without accepting missing, malformed or regressing checkpoints."""
    try:
        with open(state_file, encoding='utf-8') as state_handle:
            target_state = json.load(state_handle)
        return max(previous_safe_lsn, minimum_acknowledged_lsn(target_state, logical_streams))
    except (AttributeError, KeyError, OSError, TypeError, UnicodeError, ValueError):
        LOGGER.debug('Unable to read a valid target acknowledgement')
        return previous_safe_lsn


def seal_singer_output() -> None:
    """Deliver real pipe EOF while preserving stderr and the live replication sessions."""
    try:
        output_fd = sys.stdout.fileno()
        is_pipe = output_fd == 1 and stat.S_ISFIFO(os.fstat(output_fd).st_mode)
    except (AttributeError, OSError, ValueError):
        is_pipe = False
    if not is_pipe:
        raise FinalFeedbackError('Managed final feedback requires Singer stdout on OS pipe descriptor 1')
    sys.stdout.flush()
    # Closing a Python wrapper alone may leave fd 1 open. Replacing it releases
    # the OS pipe writer, so buffered transforms and the target can finish.
    with open(os.devnull, 'wb') as sink:
        os.dup2(sink.fileno(), output_fd)


@dataclass
class PendingFeedback:
    """Ownership of one session stopped at a fully emitted marker transaction commit."""

    connection: connection
    cursor: ReplicationCursor
    conn_info: dict[str, Any]
    logical_streams: list[dict]
    safe_lsn: int
    slot_name: str
    feedback_sent: bool = False
    confirmed: bool = False
    metadata_connection: connection | None = None
    last_status_sent_at: float = 0.0


class FinalFeedbackCoordinator:
    """Seal terminal output and complete only target-durable feedback on original sessions."""

    def __init__(self, state_file: str, timeout_seconds: float = FINAL_FEEDBACK_TIMEOUT_SECONDS) -> None:
        if not state_file:
            raise FinalFeedbackError('Managed final feedback requires a target state file')
        self.state_file = state_file
        self.timeout_seconds = timeout_seconds
        self.sessions: list[PendingFeedback] = []
        self.finished = False

    @classmethod
    def from_environment(cls, state_file: str | None) -> 'FinalFeedbackCoordinator | None':
        """Enable only the managed core's explicit opt-in, preserving standalone behavior."""
        if os.environ.get(FINAL_FEEDBACK_ENV) != '1':
            return None
        return cls(state_file)

    def retain(self, conn: connection, cur: ReplicationCursor, conn_info: dict[str, Any],
               logical_streams: list[dict], safe_lsn: int, slot_name: str) -> None:
        """Take ownership after the final committed boundary STATE has been emitted."""
        if self.finished:
            raise FinalFeedbackError('Cannot retain a replication session after terminal output')
        self.sessions.append(PendingFeedback(conn, cur, copy.deepcopy(conn_info),
                                             copy.deepcopy(logical_streams), safe_lsn, slot_name))

    @staticmethod
    def _remaining_seconds(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise FinalFeedbackError('Timed out waiting for durable target state and PostgreSQL final feedback')
        return remaining

    @classmethod
    def _wait_for_metadata(cls, conn: connection, deadline: float) -> None:
        """Bound libpq connection/query socket waits, including an unresponsive network."""
        while True:
            remaining = cls._remaining_seconds(deadline)
            poll_state = conn.poll()
            if poll_state == extensions.POLL_OK:
                return
            if poll_state not in (extensions.POLL_READ, extensions.POLL_WRITE):
                raise FinalFeedbackError('Unexpected PostgreSQL metadata connection poll state')
            readers = [conn] if poll_state == extensions.POLL_READ else []
            writers = [conn] if poll_state == extensions.POLL_WRITE else []
            try:
                select(readers, writers, [], remaining)
            except InterruptedError:
                pass

    @contextmanager
    def _metadata_deadline(self, deadline: float) -> Iterator[None]:
        """Scope psycopg's process-global wait callback to ordinary metadata I/O only."""
        previous_callback = extensions.get_wait_callback()
        extensions.set_wait_callback(lambda conn: self._wait_for_metadata(conn, deadline))
        try:
            yield
        finally:
            extensions.set_wait_callback(previous_callback)

    def _confirm_receipt(self, session: PendingFeedback, deadline: float) -> bool:
        """Read the primary slot watermark; never advance or alter a slot through SQL."""
        with self._metadata_deadline(deadline):
            return self._read_slot_confirmation(session, deadline)

    def _read_slot_confirmation(self, session: PendingFeedback, deadline: float) -> bool:
        """Connect and read under the deadline callback; do not drive replication here."""
        if session.metadata_connection is None:
            # libpq treats a connect timeout below two seconds as two seconds.
            remaining = self._remaining_seconds(deadline)
            if remaining < 2:
                return False
            session.metadata_connection = post_db.open_connection(
                session.conn_info, prioritize_primary=True, connect_timeout=min(30, math.floor(remaining)))
            session.metadata_connection.autocommit = True
        remaining_ms = max(1, math.floor(self._remaining_seconds(deadline) * 1000))
        with session.metadata_connection.cursor() as metadata_cursor:
            metadata_cursor.execute('SET statement_timeout = %s', (remaining_ms,))
            boundary = self._lsn_text(session.safe_lsn)
            metadata_cursor.execute(
                'SELECT confirmed_flush_lsn >= %s::pg_lsn FROM pg_replication_slots WHERE slot_name = %s',
                (boundary, session.slot_name))
            row = metadata_cursor.fetchone()
        self._remaining_seconds(deadline)
        if row is None:
            raise FinalFeedbackError('Replication slot is missing during final feedback confirmation')
        return row[0] is True

    def _advance(self, session: PendingFeedback, deadline: float) -> None:
        self._remaining_seconds(deadline)
        if not session.feedback_sent:
            acknowledged_lsn = read_target_acknowledged_lsn(self.state_file, session.logical_streams, -1)
            self._remaining_seconds(deadline)
            if acknowledged_lsn < session.safe_lsn:
                self._send_status_if_due(session)
                return
            LOGGER.info('Confirming final write up to %s, flush to %s (slot %s)',
                        self._lsn_text(session.safe_lsn), self._lsn_text(session.safe_lsn), session.slot_name)
            session.cursor.send_feedback(write_lsn=session.safe_lsn, flush_lsn=session.safe_lsn,
                                         reply=True, force=True)
            session.feedback_sent = True
        # Never consume more WAL after the completed marker. This bounds wasted
        # decoding while the target flushes and avoids psycopg2 keepalive logic
        # promoting flush_lsn to wal_end after the final explicit acknowledgement.
        session.confirmed = self._confirm_receipt(session, deadline)
        if session.confirmed:
            LOGGER.info('Confirmed final PostgreSQL feedback at %s (slot %s)',
                        self._lsn_text(session.safe_lsn), session.slot_name)

    @staticmethod
    def _lsn_text(lsn: int) -> str:
        return f'{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}'

    @staticmethod
    def _send_status_if_due(session: PendingFeedback) -> None:
        now = time.monotonic()
        if now - session.last_status_sent_at >= STATUS_INTERVAL_SECONDS:
            # Defaults of zero preserve psycopg2's already proven write/flush
            # positions. PostgreSQL processes replies even if its output blocks.
            session.cursor.send_feedback(force=True)
            session.last_status_sent_at = now

    def finish(self) -> None:
        """Emit EOF once, wait for durable target ACKs, verify feedback, and close owned sessions."""
        if self.finished:
            return
        self.finished = True
        try:
            if not self.sessions:
                return
            seal_singer_output()
            started_at = time.monotonic()
            deadline = started_at + self.timeout_seconds
            for session in self.sessions:
                session.last_status_sent_at = started_at
            while pending := [session for session in self.sessions if not session.confirmed]:
                for session in pending:
                    self._advance(session, deadline)
                if all(session.confirmed for session in pending):
                    break
                wait_seconds = min(POLL_INTERVAL_SECONDS, self._remaining_seconds(deadline))
                try:
                    select([], [], [], wait_seconds)
                except InterruptedError:
                    pass
            LOGGER.info('Confirmed final PostgreSQL feedback for %i logical sessions', len(self.sessions))
        except (psycopg2.Error, OSError, ValueError):
            raise FinalFeedbackError('Connection failed while confirming PostgreSQL final feedback') from None
        finally:
            self.close()

    def close(self) -> None:
        """Release every retained resource even after cancellation or another close failure."""
        sessions, self.sessions = self.sessions, []
        for session in sessions:
            for resource in (session.cursor, session.connection, session.metadata_connection):
                if resource is not None:
                    try:
                        resource.close()
                    except (psycopg2.Error, OSError):
                        LOGGER.warning('Unable to close a final-feedback PostgreSQL resource')
