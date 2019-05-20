import unittest
import os
import json
import datetime
import target_postgres

from nose.tools import assert_raises 
from target_postgres.db_sync import DbSync

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils


METADATA_COLUMNS = [
    '_sdc_batched_at',
    '_sdc_deleted_at',
    '_sdc_extracted_at',
    '_sdc_primary_key',
    '_sdc_received_at',
    '_sdc_sequence',
    '_sdc_table_version'
]


class TestIntegration(unittest.TestCase):
    """
    Integration Tests
    """
    @classmethod
    def setUp(self):
        self.config = test_utils.get_test_config()
        postgres = DbSync(self.config)
        if self.config['default_target_schema']:
            postgres.query("DROP SCHEMA IF EXISTS {} CASCADE".format(self.config['default_target_schema']))


    def remove_metadata_columns_from_rows(self, rows):
        """Removes metadata columns from a list of rows"""
        d_rows = []
        for r in rows:
            # Copy the original row to a new dict to keep the original dict
            # and remove metadata columns
            d_row = r.copy()
            for md_c in METADATA_COLUMNS:
                d_row.pop(md_c, None)

            # Add new row without metadata columns to the new list
            d_rows.append(d_row)

        return d_rows


    def assert_metadata_columns_exist(self, rows):
        """This is a helper assertion that checks if every row in a list has metadata columns"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertTrue(md_c in r)


    def assert_metadata_columns_not_exist(self, rows):
        """This is a helper assertion that checks metadata columns don't exist in any row"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertFalse(md_c in r)


    def assert_three_streams_are_into_postgres(self, should_metadata_columns_exist=False, should_hard_deleted_rows=False):
        """
        This is a helper assertion that checks if every data from the message-with-three-streams.json
        file is available in Postgres tables correctly.
        Useful to check different loading methods without duplicating assertions
        """
        postgres = DbSync(self.config)
        default_target_schema = self.config.get('default_target_schema', '')
        schema_mapping = self.config.get('schema_mapping', {})

        # Identify target schema name
        target_schema = None
        if default_target_schema is not None and default_target_schema.strip():
            target_schema = default_target_schema
        elif schema_mapping:
            target_schema = os.environ.get("TARGET_POSTGRES_SCHEMA")

        # Get loaded rows from tables
        table_one = postgres.query("SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = postgres.query("SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = postgres.query("SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))


        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}
        ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_one), expected_table_one)

        # ----------------------------------------------------------------------
        # Check rows in table_tow
        # ----------------------------------------------------------------------
        expected_table_two = []
        if not should_hard_deleted_rows:
            expected_table_two = [
                {'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_date': datetime.datetime(2019, 2, 1, 15, 12, 45)},
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 10, 2, 0, 0)}
            ]
        else:
            expected_table_two = [
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 10, 2, 0, 0)}
            ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_two), expected_table_two)

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = []
        if not should_hard_deleted_rows:
            expected_table_three = [
                    {'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_time': datetime.time(4, 0, 0)},
                    {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_time': datetime.time(7, 15, 0)},
                    {'c_int': 3, 'c_pk': 3, 'c_varchar': '3', 'c_time': datetime.time(23, 0, 3)}
            ]
        else:
            expected_table_three = [
                {'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_time': datetime.time(4, 0, 0)},
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_time': datetime.time(7, 15, 0)}
            ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_three), expected_table_three)

        # ----------------------------------------------------------------------
        # Check if metadata columns exist or not
        # ----------------------------------------------------------------------
        if should_metadata_columns_exist:
            self.assert_metadata_columns_exist(table_one)
            self.assert_metadata_columns_exist(table_two)
            self.assert_metadata_columns_exist(table_three)
        else:
            self.assert_metadata_columns_not_exist(table_one)
            self.assert_metadata_columns_not_exist(table_two)
            self.assert_metadata_columns_not_exist(table_three)


    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with assert_raises(json.decoder.JSONDecodeError):
            target_postgres.persist_lines(self.config, tap_lines)


    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with assert_raises(Exception):
            target_postgres.persist_lines(self.config, tap_lines)


    def test_loading_tables(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        target_postgres.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_into_postgres()


    def test_loading_tables_with_metadata_columns(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on adding metadata columns
        self.config['add_metadata_columns'] = True
        target_postgres.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_postgres(should_metadata_columns_exist=True)


    def test_loading_tables_with_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        target_postgres.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_postgres(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )


    def test_loading_with_multiple_schema(self):
        """Loading table with multiple SCHEMA messages"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multi-schemas.json')

        # Load with default settings
        target_postgres.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly
        self.assert_three_streams_are_into_postgres(
            should_metadata_columns_exist=False,
            should_hard_deleted_rows=False
        )


    def test_loading_unicode_characters(self):
        """Loading unicode encoded characters"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-unicode-characters.json')

        # Load with default settings
        target_postgres.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_unicode = postgres.query("SELECT * FROM {}.test_table_unicode ORDER BY c_pk".format(target_schema))

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_unicode),
            [
                    {'c_int': 1, 'c_pk': 1, 'c_varchar': 'Hello world, Καλημέρα κόσμε, コンニチハ'},
                    {'c_int': 2, 'c_pk': 2, 'c_varchar': 'Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年.'},
                    {'c_int': 3, 'c_pk': 3, 'c_varchar': 'Russian: Зарегистрируйтесь сейчас на Десятую Международную Конференцию по'},
                    {'c_int': 4, 'c_pk': 4, 'c_varchar': 'Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช'},
                    {'c_int': 5, 'c_pk': 5, 'c_varchar': 'Arabic: لقد لعبت أنت وأصدقاؤك لمدة وحصلتم علي من إجمالي النقاط'},
                    {'c_int': 6, 'c_pk': 6, 'c_varchar': 'Special Characters: [",\'!@£$%^&*()]'}
            ])


    def test_non_db_friendly_columns(self):
        """Loading non-db friendly columns like, camelcase, minus signs, etc."""
        tap_lines = test_utils.get_test_tap_lines('messages-with-non-db-friendly-columns.json')

        # Load with default settings
        target_postgres.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_non_db_friendly_columns = postgres.query("SELECT * FROM {}.test_table_non_db_friendly_columns ORDER BY c_pk".format(target_schema))

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_non_db_friendly_columns),
            [
                    {'c_pk': 1, 'camelcasecolumn': 'Dummy row 1', 'minus-column': 'Dummy row 1'},
                    {'c_pk': 2, 'camelcasecolumn': 'Dummy row 2', 'minus-column': 'Dummy row 2'},
                    {'c_pk': 3, 'camelcasecolumn': 'Dummy row 3', 'minus-column': 'Dummy row 3'},
                    {'c_pk': 4, 'camelcasecolumn': 'Dummy row 4', 'minus-column': 'Dummy row 4'},
                    {'c_pk': 5, 'camelcasecolumn': 'Dummy row 5', 'minus-column': 'Dummy row 5'},
            ])


    def test_schema_mapping(self):
        """Load stream into a specific schema, create indices and grant permissions"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True

        # Remove the default_target_schema and use schema mapping
        del self.config['default_target_schema']

        # ... and define a custom stream to schema mapping
        self.config['schema_mapping'] = {
          "tap_mysql_test": {
              "target_schema": os.environ.get("TARGET_POSTGRES_SCHEMA"),
              "indices": {
                "test_table_one": ["c_varchar"],
                "test_table_two": ["c_varchar", "c_int"]
              }
            }
          }
        target_postgres.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_postgres(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )
