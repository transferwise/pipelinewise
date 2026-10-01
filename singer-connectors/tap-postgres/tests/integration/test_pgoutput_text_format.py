import os
import time
import unittest

from psycopg2 import sql

from tap_postgres import db
from tap_postgres.pgoutput import PgoutputDecoder
from tap_postgres.sync_strategies import logical_replication

from ..utils import get_test_connection, get_test_connection_config


class TestPgoutputTextFormat(unittest.TestCase):
    database = os.environ.get('TAP_POSTGRES_DB', 'postgres')
    table = 'pgoutput_text_format_test'
    publication = 'pgoutput_text_format_test_publication'
    slot = 'pgoutput_text_format_test_slot'

    @classmethod
    def _cleanup(cls, admin, replication_connection=None):
        if replication_connection is not None:
            replication_connection.close()

        with admin.cursor() as cursor:
            cursor.execute(
                'SELECT pg_terminate_backend(active_pid) '
                'FROM pg_replication_slots WHERE slot_name = %s AND active_pid IS NOT NULL',
                (cls.slot,),
            )
            cursor.execute(
                'SELECT pg_drop_replication_slot(slot_name) '
                'FROM pg_replication_slots WHERE slot_name = %s',
                (cls.slot,),
            )
            cursor.execute(
                sql.SQL('DROP PUBLICATION IF EXISTS {}').format(sql.Identifier(cls.publication))
            )
            cursor.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(sql.Identifier(cls.table)))

            role = sql.Identifier(os.environ['TAP_POSTGRES_USER'])
            database = sql.Identifier(cls.database)
            for setting in ('DateStyle', 'IntervalStyle', 'extra_float_digits'):
                cursor.execute(
                    sql.SQL('ALTER ROLE {} IN DATABASE {} RESET {}').format(
                        role,
                        database,
                        sql.Identifier(setting),
                    )
                )

    @classmethod
    def _prepare_source(cls, admin):
        role = sql.Identifier(os.environ['TAP_POSTGRES_USER'])
        database = sql.Identifier(cls.database)
        with admin.cursor() as cursor:
            cursor.execute(
                sql.SQL("ALTER ROLE {} IN DATABASE {} SET DateStyle = 'SQL, DMY'").format(role, database)
            )
            cursor.execute(
                sql.SQL("ALTER ROLE {} IN DATABASE {} SET IntervalStyle = 'sql_standard'").format(role, database)
            )
            cursor.execute(
                sql.SQL('ALTER ROLE {} IN DATABASE {} SET extra_float_digits = -15').format(role, database)
            )
            cursor.execute(
                sql.SQL(
                    'CREATE TABLE {} ('
                    'id integer PRIMARY KEY, '
                    'date_value date, '
                    'timestamp_value timestamp without time zone, '
                    'timestamptz_value timestamp with time zone, '
                    'interval_value interval, '
                    'float8_value double precision, '
                    'float4_value real)'
                ).format(sql.Identifier(cls.table))
            )
            cursor.execute(
                sql.SQL('CREATE PUBLICATION {} FOR TABLE {}').format(
                    sql.Identifier(cls.publication),
                    sql.Identifier(cls.table),
                )
            )
            cursor.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                (cls.slot,),
            )
            cursor.execute(
                sql.SQL(
                    "INSERT INTO {} VALUES ("
                    "1, DATE '2020-02-01', TIMESTAMP '2020-02-01 03:04:05.123456', "
                    "TIMESTAMPTZ '2020-02-01 03:04:05.123456+02', "
                    "INTERVAL '1 month 2 days 03:04:05.5', "
                    "'1.2345678901234567'::float8, '1.23456789'::float4)"
                ).format(sql.Identifier(cls.table))
            )

    def test_pgoutput_uses_stable_lossless_text_formats(self):
        admin = get_test_connection(self.database, superuser=True)
        replication_connection = None
        try:
            self._cleanup(admin)
            self._prepare_source(admin)

            connection_config = get_test_connection_config(self.database)
            replication_connection = db.open_connection(
                connection_config,
                logical_replication=True,
                prioritize_primary=True,
            )
            self.assertTrue(
                replication_connection.get_parameter_status('DateStyle').startswith('ISO')
            )
            self.assertEqual(
                'postgres',
                replication_connection.get_parameter_status('IntervalStyle'),
            )

            replication_cursor = replication_connection.cursor()
            replication_cursor.start_replication(
                slot_name=self.slot,
                decode=False,
                options={
                    'proto_version': '1',
                    'publication_names': self.publication,
                },
            )

            decoder = PgoutputDecoder()
            decoded_insert = None
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                message = replication_cursor.read_message()
                if message is None:
                    time.sleep(0.01)
                    continue
                decoded = decoder.decode(message.payload)
                if decoded['action'] == 'I':
                    decoded_insert = decoded
                    break

            self.assertIsNotNone(decoded_insert, 'Timed out before receiving the pgoutput insert')
            values = {column['name']: column['value'] for column in decoded_insert['columns']}
            self.assertDictEqual(
                {
                    'id': '1',
                    'date_value': '2020-02-01',
                    'timestamp_value': '2020-02-01 03:04:05.123456',
                    'timestamptz_value': '2020-02-01 01:04:05.123456+00',
                    'interval_value': '1 mon 2 days 03:04:05.5',
                    'float8_value': '1.2345678901234567',
                    'float4_value': '1.2345679',
                },
                values,
            )
            self.assertEqual(
                '2020-02-01T00:00:00+00:00',
                logical_replication.selected_value_to_singer_value_impl(
                    values['date_value'],
                    'date',
                    connection_config,
                ),
            )
        finally:
            self._cleanup(admin, replication_connection)
            admin.close()
