"""Verify snapshot bookmarks identify flushed, replayable WAL records."""

import time
import unittest

from tap_postgres import db
from tap_postgres.pgoutput import PgoutputDecoder

from ..utils import get_test_connection, get_test_connection_config


class TestSnapshotBoundary(unittest.TestCase):
    def test_boundary_is_flushed_and_can_start_pgoutput_replay(self):
        source = get_test_connection()
        replication = None
        slot = 'snapshot_boundary_test'
        publication = 'snapshot_boundary_test'
        try:
            with source.cursor() as cursor:
                cursor.execute(f'CREATE TABLE {publication} (id integer PRIMARY KEY)')
                cursor.execute(f'CREATE PUBLICATION {publication} FOR TABLE {publication}')
                cursor.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))

            config = get_test_connection_config()
            boundary = db.capture_snapshot_boundary(config)
            with source.cursor() as cursor:
                cursor.execute('SELECT pg_current_wal_flush_lsn()::text')
                high, low = cursor.fetchone()[0].split('/')
                self.assertGreaterEqual((int(high, 16) << 32) + int(low, 16), boundary)

            with source.cursor() as cursor:
                cursor.execute(f'INSERT INTO {publication} VALUES (1)')

            replication = db.open_connection(config, logical_replication=True)
            cursor = replication.cursor()
            cursor.start_replication(slot_name=slot, decode=False, start_lsn=boundary, options={
                'proto_version': '1', 'publication_names': publication,
            })
            decoder = PgoutputDecoder()
            messages = []
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                message = cursor.read_message()
                if message is None:
                    time.sleep(0.01)
                    continue
                change = decoder.decode(message.payload)
                messages.append(change)
                if change['action'] == 'C' and any(item['action'] == 'I' for item in messages):
                    break

            self.assertIn('I', [change['action'] for change in messages])
            self.assertNotIn('M', [change['action'] for change in messages])
            self.assertEqual(messages[-1]['action'], 'C')
        finally:
            if replication is not None:
                replication.close()
            with source.cursor() as cursor:
                cursor.execute('SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots '
                               'WHERE slot_name = %s AND active_pid IS NOT NULL', (slot,))
                cursor.execute('SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots '
                               'WHERE slot_name = %s', (slot,))
                cursor.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cursor.execute(f'DROP TABLE IF EXISTS {publication}')
            source.close()
