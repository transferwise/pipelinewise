"""Preserve shared legacy history when PostgreSQL truncates two slot names alike."""

import unittest

from tap_postgres.sync_strategies import logical_replication

from ..utils import get_test_connection


class TestLegacySlotCollision(unittest.TestCase):
    def test_fresh_pgoutput_can_coexist_with_ambiguous_truncated_wal2json_slot(self):
        database = 'pgoutput_collision_' + 'd' * 43
        tap_id = 'collision_fresh_test'
        destination = logical_replication.generate_replication_slot_name(tap_id)
        shared, dedicated = logical_replication.legacy_replication_slot_names(database, tap_id)
        self.assertEqual(shared, dedicated)
        admin = get_test_connection(superuser=True)
        source = None
        try:
            with admin.cursor() as cursor:
                cursor.execute(f'DROP DATABASE IF EXISTS {database} WITH (FORCE)')
                cursor.execute(f'CREATE DATABASE {database}')
            source = get_test_connection(database)
            with source.cursor() as cursor:
                cursor.execute("SELECT pg_create_logical_replication_slot(%s, 'wal2json')", (shared,))
                with self.assertRaisesRegex(logical_replication.ReplicationSlotMigrationError, 'collide after'):
                    logical_replication._validate_replication_slot_candidates(cursor, database, tap_id)

                _, migration_source, _ = logical_replication._validate_replication_slot_candidates(
                    cursor, database, tap_id, fresh_start=True,
                )
                self.assertIsNone(migration_source)
                cursor.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (destination,))
                located = logical_replication.locate_replication_slot_by_cur(cursor, database, tap_id)
                self.assertIsNone(located.migration_source)
                cursor.execute('SELECT slot_name, plugin FROM pg_replication_slots WHERE database = %s', (database,))
                self.assertEqual(set(cursor.fetchall()), {(shared, 'wal2json'), (destination, 'pgoutput')})
        finally:
            if source is not None:
                source.close()
            with admin.cursor() as cursor:
                cursor.execute(f'DROP DATABASE IF EXISTS {database} WITH (FORCE)')
            admin.close()
