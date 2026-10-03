"""Exercise keepalive acknowledgements against an actual PostgreSQL server."""

import time
import unittest
import uuid

import psycopg2

from tap_postgres import db
from tap_postgres.pgoutput import PgoutputDecoder
from tap_postgres.sync_strategies.logical_replication import int_to_lsn, lsn_to_int

from ..utils import get_test_connection, get_test_connection_config


class TestReplicationTransport(unittest.TestCase):
    def test_missing_publication_reports_postgresql_error_during_streaming(self):
        name = 'transport_error_' + uuid.uuid4().hex[:12]
        primary = get_test_connection()
        replication = None
        try:
            with primary.cursor() as cur:
                cur.execute(f'CREATE TABLE {name} (id integer PRIMARY KEY)')
                cur.execute('SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)', (name, 'pgoutput'))
                initial_lsn = lsn_to_int(cur.fetchone()[0])
                cur.execute(f'INSERT INTO {name} VALUES (1)')
            replication = db.open_connection(get_test_connection_config(), True, True)
            cursor = replication.cursor()
            cursor.start_replication(
                slot_name=name, start_lsn=initial_lsn,
                options={'proto_version': '1', 'publication_names': name})
            deadline = time.monotonic() + 10
            with self.assertRaisesRegex(psycopg2.OperationalError, f'publication "{name}" does not exist'):
                while time.monotonic() < deadline:
                    cursor.read_message()
                    time.sleep(0.01)
        finally:
            if replication is not None:
                replication.close()
            with primary.cursor() as cur:
                cur.execute('SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots '
                            'WHERE slot_name = %s', (name,))
                cur.execute(f'DROP TABLE IF EXISTS {name}')
            primary.close()

    def test_keepalive_progress_survives_crash_without_advancing_durable_slot_position(self):
        name = 'transport_' + uuid.uuid4().hex[:12]
        primary = get_test_connection()
        replication = None
        try:
            with primary.cursor() as cur:
                cur.execute(f'CREATE TABLE {name} (id integer PRIMARY KEY)')
                cur.execute(f'CREATE PUBLICATION {name} FOR TABLE {name}')
                cur.execute('SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)', (name, 'pgoutput'))
                initial_lsn = lsn_to_int(cur.fetchone()[0])
                cur.execute(f'INSERT INTO {name} VALUES (1)')

            replication = db.open_connection(get_test_connection_config(), True, True)
            cursor = replication.cursor()
            cursor.start_replication(
                slot_name=name, start_lsn=initial_lsn, status_interval=0.1,
                options={'proto_version': '1', 'publication_names': name, 'messages': 'false'})
            decoder = PgoutputDecoder()
            acknowledged_lsn = None
            deadline = time.monotonic() + 10
            while acknowledged_lsn is None:
                self.assertLess(time.monotonic(), deadline, 'Initial row did not reach the receiver')
                message = cursor.read_message()
                if message:
                    payload = decoder.decode(message.payload)
                    if payload['action'] == 'C':
                        acknowledged_lsn = payload['end_lsn']
                else:
                    time.sleep(0.01)

            cursor.send_feedback(write_lsn=acknowledged_lsn, flush_lsn=acknowledged_lsn, force=True)
            with primary.cursor() as cur:
                cur.execute("SELECT pg_logical_emit_message(FALSE, 'transport_ignored', 'unselected WAL')")
            cursor.send_feedback(reply=True, force=True)
            deadline = time.monotonic() + 10
            while cursor.wal_end <= acknowledged_lsn:
                self.assertLess(time.monotonic(), deadline, 'Server did not send its later WAL position')
                cursor.read_message()
                time.sleep(0.01)
            cursor.send_feedback(force=True)
            time.sleep(0.1)
            replication.close()
            replication = None

            with primary.cursor() as cur:
                deadline = time.monotonic() + 10
                while True:
                    cur.execute('SELECT active, confirmed_flush_lsn::text FROM pg_replication_slots '
                                'WHERE slot_name = %s', (name,))
                    active, flushed_lsn = cur.fetchone()
                    if not active:
                        break
                    self.assertLess(time.monotonic(), deadline, 'Slot did not release after connection loss')
                    time.sleep(0.01)
                self.assertEqual(int_to_lsn(acknowledged_lsn), flushed_lsn)
                cur.execute(f'INSERT INTO {name} VALUES (2)')

            replication = db.open_connection(get_test_connection_config(), True, True)
            cursor = replication.cursor()
            cursor.start_replication(
                slot_name=name, start_lsn=acknowledged_lsn,
                options={'proto_version': '1', 'publication_names': name})
            decoder = PgoutputDecoder()
            deadline = time.monotonic() + 10
            while True:
                self.assertLess(time.monotonic(), deadline, 'Restart did not deliver the next source row')
                message = cursor.read_message()
                if message:
                    payload = decoder.decode(message.payload)
                    if payload['action'] == 'I':
                        self.assertEqual('2', payload['columns'][0]['value'])
                        break
                else:
                    time.sleep(0.01)
        finally:
            if replication is not None:
                replication.close()
            with primary.cursor() as cur:
                deadline = time.monotonic() + 10
                while True:
                    cur.execute('SELECT active FROM pg_replication_slots WHERE slot_name = %s', (name,))
                    row = cur.fetchone()
                    if not row or not row[0]:
                        break
                    self.assertLess(time.monotonic(), deadline, 'Slot cleanup timed out')
                    time.sleep(0.01)
                if row:
                    cur.execute('SELECT pg_drop_replication_slot(%s)', (name,))
                cur.execute(f'DROP PUBLICATION IF EXISTS {name}')
                cur.execute(f'DROP TABLE IF EXISTS {name}')
            primary.close()
