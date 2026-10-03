"""Logical replication transport with explicitly acknowledged WAL positions."""

import datetime
import struct
import time
from collections import namedtuple
from select import select

import psycopg
import psycopg2
from psycopg import pq, sql


ReplicationMessage = namedtuple('ReplicationMessage', ['payload', 'data_start', 'wal_end'])
POSTGRES_EPOCH = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)


class ReplicationConnection:
    """Use libpq COPY BOTH without psycopg2's implicit keepalive acknowledgements."""

    def __init__(self, **config):
        config.pop('connection_factory', None)
        self.connection = psycopg.connect(**config, replication='database', autocommit=True)

    @property
    def server_version(self):
        return self.connection.info.server_version

    @property
    def encoding(self):
        return self.connection.info.encoding

    def get_parameter_status(self, name):
        return self.connection.info.parameter_status(name)

    def set_client_encoding(self, encoding):
        self.connection.execute(sql.SQL('SET client_encoding TO {}').format(sql.Literal(encoding)))

    def cursor(self):
        return ReplicationCursor(self)

    def close(self):
        self.connection.close()


class ReplicationCursor:
    """Preserve the last explicit flush LSN when replying to server keepalives."""

    def __init__(self, connection):
        self.connection = connection
        self.pgconn = connection.connection.pgconn
        self.write_lsn = 0
        self.flush_lsn = 0
        self.apply_lsn = 0
        self.wal_end = 0
        self.decode = False
        self.status_interval = 10
        self.last_feedback = time.monotonic()

    def execute(self, statement):
        self.connection.connection.execute(statement)

    def fileno(self):
        return self.pgconn.socket

    def start_replication(self, slot_name, start_lsn=0, decode=False, status_interval=10, options=None):
        if isinstance(start_lsn, int):
            start_lsn = f'{start_lsn >> 32:X}/{start_lsn & 0xFFFFFFFF:X}'
        option_list = sql.SQL(', ').join(
            sql.SQL('{} {}').format(sql.Identifier(key), sql.Literal(str(value)))
            for key, value in (options or {}).items()
        )
        statement = sql.SQL('START_REPLICATION SLOT {} LOGICAL {} ({})').format(
            sql.Identifier(slot_name), sql.SQL(start_lsn), option_list)
        result = self.pgconn.exec_(statement.as_bytes(self.connection.connection))
        if result.status != pq.ExecStatus.COPY_BOTH:
            raise psycopg2.ProgrammingError(result.error_message.decode(self.connection.encoding))
        self.pgconn.nonblocking = 1
        self.decode = decode
        self.status_interval = status_interval
        self.last_feedback = time.monotonic()

    def send_feedback(self, write_lsn=0, flush_lsn=0, apply_lsn=0, reply=False, force=False):
        self.write_lsn = max(self.write_lsn, write_lsn)
        self.flush_lsn = max(self.flush_lsn, flush_lsn)
        self.apply_lsn = max(self.apply_lsn, apply_lsn)
        if not (force or reply or time.monotonic() - self.last_feedback >= self.status_interval):
            return
        timestamp = int((datetime.datetime.now(datetime.timezone.utc) - POSTGRES_EPOCH).total_seconds() * 1000000)
        feedback = struct.pack('!cQQQqB', b'r', self.write_lsn, self.flush_lsn, self.apply_lsn, timestamp, int(reply))
        deadline = time.monotonic() + 30
        while self.pgconn.put_copy_data(feedback) == 0:
            self._flush_output(deadline)
        self._flush_output(deadline)
        self.last_feedback = time.monotonic()

    def _flush_output(self, deadline):
        while self.pgconn.flush():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise psycopg2.OperationalError('Timed out sending logical replication feedback')
            readable, _, _ = select([self], [self], [], min(remaining, 1))
            if readable:
                self.pgconn.consume_input()

    def read_message(self):
        self.send_feedback()
        consumed_input = False
        while True:
            length, buffer = self.pgconn.get_copy_data(1)
            if length == 0:
                if consumed_input:
                    return None
                self.pgconn.consume_input()
                consumed_input = True
                continue
            if length < 0:
                raise psycopg2.OperationalError('Logical replication connection ended before the run boundary')
            # Drain buffered messages before reading the socket so slow targets
            # cannot accumulate an unbounded second copy of the WAL stream.
            consumed_input = True
            payload = bytes(buffer)
            if payload[:1] == b'k' and len(payload) == 18:
                self.wal_end, _, reply = struct.unpack('!QqB', payload[1:])
                if reply:
                    self.send_feedback(force=True)
            elif payload[:1] == b'w' and len(payload) > 25:
                data_start, self.wal_end, _ = struct.unpack('!QQq', payload[1:25])
                data = payload[25:]
                if self.decode:
                    data = data.decode(self.connection.encoding)
                return ReplicationMessage(data, data_start, self.wal_end)
            else:
                raise psycopg2.OperationalError('Invalid logical replication transport message')

    def close(self):
        # Closing the owning connection ends COPY BOTH and releases its slot.
        pass
