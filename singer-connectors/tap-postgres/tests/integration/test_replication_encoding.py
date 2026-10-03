"""Read both output plugins from a database whose encoding is not UTF8."""

import json
import time
import unittest

from tap_postgres import db
from tap_postgres.pgoutput import PgoutputDecoder

from ..utils import get_test_connection, get_test_connection_config


class TestReplicationEncoding(unittest.TestCase):
    database = 'pgoutput_latin1_test'

    def test_migration_plugins_preserve_non_ascii_names_and_values(self):
        admin = get_test_connection(superuser=True)
        source = None
        replication = None
        try:
            with admin.cursor() as cursor:
                cursor.execute(f'DROP DATABASE IF EXISTS {self.database} WITH (FORCE)')
                cursor.execute(
                    f"CREATE DATABASE {self.database} TEMPLATE template0 ENCODING 'LATIN1' "
                    "LC_COLLATE 'C' LC_CTYPE 'C'"
                )
            source = get_test_connection(self.database)
            with source.cursor() as cursor:
                cursor.execute('CREATE TABLE "café" (id integer PRIMARY KEY, "résumé" text)')
                cursor.execute('CREATE PUBLICATION encoding_test FOR TABLE "café"')

            for plugin in ('wal2json', 'pgoutput'):
                with self.subTest(plugin=plugin):
                    slot = f'encoding_test_{plugin}'
                    with source.cursor() as cursor:
                        cursor.execute('SELECT pg_create_logical_replication_slot(%s, %s)', (slot, plugin))
                        cursor.execute('INSERT INTO "café" VALUES (%s, %s)',
                                       (1 if plugin == 'wal2json' else 2, 'piñata £ café'))
                    replication = db.open_connection(
                        get_test_connection_config(self.database), logical_replication=True,
                        replication_plugin=plugin,
                    )
                    cursor = replication.cursor()
                    options = {'format-version': '2'} if plugin == 'wal2json' else {
                        'proto_version': '1', 'publication_names': 'encoding_test',
                    }
                    cursor.start_replication(slot_name=slot, options=options, decode=plugin == 'wal2json')
                    change = self._read_insert(cursor, plugin)

                    self.assertEqual(change['table'], 'café')
                    self.assertEqual({column['name']: column['value'] for column in change['columns']}['résumé'],
                                     'piñata £ café')
                    replication.close()
                    replication = None
        finally:
            if replication is not None:
                replication.close()
            if source is not None:
                source.close()
            with admin.cursor() as cursor:
                cursor.execute(
                    'SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots '
                    'WHERE database = %s AND active_pid IS NOT NULL', (self.database,),
                )
                cursor.execute(f'DROP DATABASE IF EXISTS {self.database} WITH (FORCE)')
            admin.close()

    @staticmethod
    def _read_insert(cursor, plugin):
        decoder = PgoutputDecoder()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            message = cursor.read_message()
            if message is None:
                time.sleep(0.01)
                continue
            change = json.loads(message.payload) if plugin == 'wal2json' else decoder.decode(message.payload)
            if change['action'] == 'I':
                return change
        raise AssertionError(f'Timed out reading the {plugin} insert')
