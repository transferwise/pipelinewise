"""Tests for large source-to-target regression fixtures."""

import hashlib
import json

from unittest import TestCase, mock

from tests.end_to_end.helpers import db as db_module
from tests.end_to_end.helpers import json_fixtures


class JsonFixturesTestCase(TestCase):
    """Prove the ticket fixture and its exact-value assertion stay strict."""

    def test_fixture_preserves_the_reported_boundary_and_shape(self):
        """The regression fixture must remain the exact >32 KiB JSON text."""
        payload = json_fixtures.ticket_20155_json_metadata()

        self.assertEqual(
            len(payload.encode('utf-8')),
            json_fixtures.TICKET_20155_JSON_METADATA_LENGTH,
        )
        self.assertEqual(
            hashlib.sha256(payload.encode('utf-8')).hexdigest(),
            json_fixtures.TICKET_20155_JSON_METADATA_SHA256,
        )
        parsed = json.loads(payload)
        self.assertEqual(len(parsed['chart_configuration']), 76)
        self.assertEqual(
            len(parsed['global_chart_configuration']['chartsInScope']), 76
        )

    def test_assertion_accepts_only_the_exact_text(self):
        """Exact source or target readback is accepted without normalization."""
        payload = json_fixtures.ticket_20155_json_metadata()

        json_fixtures.assert_ticket_20155_json_metadata(
            [(payload,)], 'test route'
        )

    def test_assertion_rejects_truncation(self):
        """A plausible non-empty prefix cannot masquerade as a successful copy."""
        payload = json_fixtures.ticket_20155_json_metadata()

        with self.assertRaisesRegex(
            AssertionError,
            'changed ticket 20155 JSON metadata.*50415 bytes',
        ):
            json_fixtures.assert_ticket_20155_json_metadata(
                [(payload[:-1],)], 'test route'
            )

    def test_assertion_rejects_double_encoding(self):
        """A valid JSON string containing the packet is still the wrong value."""
        payload = json_fixtures.ticket_20155_json_metadata()

        with self.assertRaisesRegex(
            AssertionError, 'changed ticket 20155 JSON metadata'
        ):
            json_fixtures.assert_ticket_20155_json_metadata(
                [(json.dumps(payload),)], 'test route'
            )

    def test_assertion_rejects_missing_or_duplicate_rows(self):
        """Row shape and cardinality are part of the replication contract."""
        payload = json_fixtures.ticket_20155_json_metadata()

        for rows in ([], [(payload,), (payload,)], [(payload, 'extra')]):
            with self.subTest(rows=len(rows)), self.assertRaisesRegex(
                AssertionError, 'expected one text value'
            ):
                json_fixtures.assert_ticket_20155_json_metadata(
                    rows, 'test route'
                )

    @mock.patch('tests.end_to_end.helpers.db.pymysql.connect')
    def test_mysql_query_helper_passes_fixture_as_a_parameter(self, connect_mock):
        """The large escaped value must not be interpolated into MySQL SQL."""
        cursor = connect_mock.return_value.__enter__.return_value \
            .cursor.return_value.__enter__.return_value
        cursor.rowcount = 0
        params = (json_fixtures.ticket_20155_json_metadata(),)

        db_module.run_query_mysql(
            'INSERT INTO fixture VALUES (%s)',
            host='mysql',
            port=3306,
            user='user',
            password='password',
            database='database',
            params=params,
        )

        cursor.execute.assert_called_once_with(
            'INSERT INTO fixture VALUES (%s)', params
        )
        connect_mock.assert_called_once_with(
            host='mysql',
            port=3306,
            user='user',
            password='password',
            database='database',
            charset='utf8mb4',
            cursorclass=db_module.pymysql.cursors.Cursor,
            ssl={'': True},
            autocommit=True,
        )

    @mock.patch('tests.end_to_end.helpers.db.psycopg2.connect')
    def test_postgres_query_helper_passes_fixture_as_a_parameter(self, connect_mock):
        """The large escaped value must not be interpolated into PostgreSQL SQL."""
        connection = connect_mock.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.rowcount = 0
        params = (json_fixtures.ticket_20155_json_metadata(),)

        db_module.run_query_postgres(
            'INSERT INTO fixture VALUES (%s)',
            host='postgres',
            port=5432,
            user='user',
            password='password',
            database='database',
            params=params,
        )

        cursor.execute.assert_called_once_with(
            'INSERT INTO fixture VALUES (%s)', params
        )
