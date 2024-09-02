import unittest
import os
import json
import datetime
import target_postgres

from unittest import mock

from target_postgres import RecordValidationException
from target_postgres.db_sync import DbSync
from psycopg2.errors import InvalidTextRepresentation

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils

METADATA_COLUMNS = [
    '_sdc_extracted_at',
    '_sdc_batched_at',
    '_sdc_deleted_at'
]


class TestIntegration(unittest.TestCase):
    """
    Integration Tests
    """

    @classmethod
    def setUp(cls):
        cls.config = test_utils.get_test_config()
        print(cls.config)
        cls.maxDiff = None
        postgres = DbSync(cls.config)
        if cls.config['default_target_schema']:
            postgres.query("DROP SCHEMA IF EXISTS {} CASCADE".format(cls.config['default_target_schema']))

    @staticmethod
    def remove_metadata_columns_from_rows(rows):
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

    def assert_multiple_streams_are_into_postgres(self, should_metadata_columns_exist=False,
                                                  should_hard_deleted_rows=False):
        """
        This is a helper assertion that checks if every data from the message-with-multiple-streams.json
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
        table_four = postgres.query("SELECT * FROM {}.test_table_four ORDER BY c_pk".format(target_schema))

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
        # Check rows in table_four
        # ----------------------------------------------------------------------
        expected_table_four = []
        if not should_hard_deleted_rows:
            expected_table_four = [
                {'c_pk': 1, 'c_smallint': 1, 'c_integer': 1, 'c_bigint': 1, 'c_nobound_int': 1},
                {'c_pk': 2, 'c_smallint': 2, 'c_integer': 2, 'c_bigint': 2, 'c_nobound_int': 2},
                {'c_pk': 3, 'c_smallint': 3, 'c_integer': 3, 'c_bigint': 3, 'c_nobound_int': 3},
            ]
        else:
            expected_table_four = [
                {'c_pk': 1, 'c_smallint': 1, 'c_integer': 1, 'c_bigint': 1, 'c_nobound_int': 1},
                {'c_pk': 2, 'c_smallint': 2, 'c_integer': 2, 'c_bigint': 2, 'c_nobound_int': 2},
            ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_four), expected_table_four)

        # ----------------------------------------------------------------------
        # Check if metadata columns exist or not
        # ----------------------------------------------------------------------
        if should_metadata_columns_exist:
            self.assert_metadata_columns_exist(table_one)
            self.assert_metadata_columns_exist(table_two)
            self.assert_metadata_columns_exist(table_three)
            self.assert_metadata_columns_exist(table_four)
        else:
            self.assert_metadata_columns_not_exist(table_one)
            self.assert_metadata_columns_not_exist(table_two)
            self.assert_metadata_columns_not_exist(table_three)
            self.assert_metadata_columns_not_exist(table_four)

    def assert_logical_streams_are_in_postgres(self, should_metadata_columns_exist=False):
        # Get loaded rows from tables
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = postgres.query("SELECT * FROM {}.logical1_table1 ORDER BY cid".format(target_schema))
        table_two = postgres.query("SELECT * FROM {}.logical1_table2 ORDER BY cid".format(target_schema))
        table_three = postgres.query("SELECT * FROM {}.logical2_table1 ORDER BY cid".format(target_schema))
        table_four = postgres.query("SELECT cid, ctimentz, ctimetz FROM {}.logical1_edgydata WHERE CID IN(1,2,3,4,5,6,8,9) ORDER BY cid".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'cid': 1, 'cvarchar': "inserted row", 'cvarchar2': None},
            {'cid': 2, 'cvarchar': 'inserted row', "cvarchar2": "inserted row"},
            {'cid': 3, 'cvarchar': "inserted row", 'cvarchar2': "inserted row"},
            {'cid': 4, 'cvarchar': "inserted row", 'cvarchar2': "inserted row"}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_tow
        # ----------------------------------------------------------------------
        expected_table_two = [
            {'cid': 1, 'cvarchar': "updated row"},
            {'cid': 2, 'cvarchar': 'updated row'},
            {'cid': 3, 'cvarchar': "updated row"},
            {'cid': 5, 'cvarchar': "updated row"},
            {'cid': 7, 'cvarchar': "updated row"},
            {'cid': 8, 'cvarchar': 'updated row'},
            {'cid': 9, 'cvarchar': "updated row"},
            {'cid': 10, 'cvarchar': 'updated row'}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = [
            {'cid': 1, 'cvarchar': "updated row"},
            {'cid': 2, 'cvarchar': 'updated row'},
            {'cid': 3, 'cvarchar': "updated row"},
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_four
        # ----------------------------------------------------------------------
        expected_table_four = [
            {'cid': 1, 'ctimentz': None, 'ctimetz': None},
            {'cid': 2, 'ctimentz': datetime.time(23, 0, 15), 'ctimetz': datetime.time(23, 0, 15)},
            {'cid': 3, 'ctimentz': datetime.time(12, 0, 15), 'ctimetz': datetime.time(12, 0, 15)},
            {'cid': 4, 'ctimentz': datetime.time(12, 0, 15), 'ctimetz': datetime.time(9, 0, 15)},
            {'cid': 5, 'ctimentz': datetime.time(12, 0, 15), 'ctimetz': datetime.time(15, 0, 15)},
            {'cid': 6, 'ctimentz': datetime.time(0, 0), 'ctimetz': datetime.time(0, 0)},
            {'cid': 8, 'ctimentz': datetime.time(0, 0), 'ctimetz': datetime.time(1, 0)},
            {'cid': 9, 'ctimentz': datetime.time(0, 0), 'ctimetz': datetime.time(0, 0)}
        ]

        # Check if metadata columns replicated correctly
        if should_metadata_columns_exist:
            self.assert_metadata_columns_exist(table_one)
            self.assert_metadata_columns_exist(table_two)
            self.assert_metadata_columns_exist(table_three)
            self.assert_metadata_columns_not_exist(table_four)
        else:
            self.assert_metadata_columns_not_exist(table_one)
            self.assert_metadata_columns_not_exist(table_two)
            self.assert_metadata_columns_not_exist(table_three)
            self.assert_metadata_columns_not_exist(table_four)

        # Check if data replicated correctly
        assert self.remove_metadata_columns_from_rows(table_one) == expected_table_one
        assert self.remove_metadata_columns_from_rows(table_two) == expected_table_two
        assert self.remove_metadata_columns_from_rows(table_three) == expected_table_three
        assert self.remove_metadata_columns_from_rows(table_four) == expected_table_four

    def assert_logical_streams_are_in_postgres_and_are_empty(self):
        # Get loaded rows from tables
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = postgres.query("SELECT * FROM {}.logical1_table1 ORDER BY CID".format(target_schema))
        table_two = postgres.query("SELECT * FROM {}.logical1_table2 ORDER BY CID".format(target_schema))
        table_three = postgres.query("SELECT * FROM {}.logical2_table1 ORDER BY CID".format(target_schema))
        table_four = postgres.query("SELECT * FROM {}.logical1_edgydata WHERE cid IN(1,2,3,4,5,6,8,9) ORDER BY cid".format(target_schema))

        self.assertEqual(table_one, [])
        self.assertEqual(table_two, [])
        self.assertEqual(table_three, [])
        self.assertEqual(table_four, [])

    def assert_binary_data_is_in_postgres(self, table_name, should_metadata_columns_exist=False):
        # Redshift doesn't have binary type. Binary formatted singer values loaded into VARCHAR columns
        # Get loaded rows from tables
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = postgres.query('SELECT * FROM {}.{} ORDER BY "new"'.format(target_schema, table_name))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'new': '706b32', 'data': '6461746132', 'created_at': datetime.datetime(2019, 12, 17, 16, 2, 55)},
            {'new': '706b34', 'data': '6461746134', 'created_at': datetime.datetime(2019, 12, 17, 16, 32, 22)},
        ]

        if should_metadata_columns_exist:
            assert self.remove_metadata_columns_from_rows(table_one) == expected_table_one
        else:
            assert table_one == expected_table_one

    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with self.assertRaises(json.decoder.JSONDecodeError):
            target_postgres.persist_lines(self.config, tap_lines)

    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with self.assertRaises(Exception):
            target_postgres.persist_lines(self.config, tap_lines)

    def test_loading_tables(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multiple-streams.json')
        target_postgres.persist_lines(self.config, tap_lines)

        self.assert_multiple_streams_are_into_postgres()

    def test_loading_tables_with_metadata_columns(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multiple-streams.json')

        # Turning on adding metadata columns
        self.config['add_metadata_columns'] = True
        target_postgres.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_multiple_streams_are_into_postgres(should_metadata_columns_exist=True)

    def test_loading_tables_with_defined_parallelism(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multiple-streams.json')

        # Turning on adding metadata columns
        self.config['parallelism'] = 1
        target_postgres.persist_lines(self.config, tap_lines)

        self.assert_multiple_streams_are_into_postgres()

    def test_loading_tables_with_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multiple-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        target_postgres.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_multiple_streams_are_into_postgres(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )

    def test_loading_with_multiple_schema(self):
        """Loading table with multiple SCHEMA messages"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multiple-streams.json')

        # Load with default settings
        target_postgres.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly
        self.assert_multiple_streams_are_into_postgres(
            should_metadata_columns_exist=False,
            should_hard_deleted_rows=False
        )

    def test_loading_table_with_reserved_word_as_name_and_hard_delete(self):
        """Loading a table where the name is a reserved word with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-reserved-name-as-table-name.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        target_postgres.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_binary_data_is_in_postgres(
            table_name='"order"',
            should_metadata_columns_exist=True
        )

    def test_loading_table_with_space(self):
        """Loading a table where the name has space"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-space-in-table-name.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        target_postgres.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_binary_data_is_in_postgres(
            table_name='"table with space and uppercase"',
            should_metadata_columns_exist=True
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
                {'c_int': 3, 'c_pk': 3,
                 'c_varchar': 'Russian: Зарегистрируйтесь сейчас на Десятую Международную Конференцию по'},
                {'c_int': 4, 'c_pk': 4, 'c_varchar': 'Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช'},
                {'c_int': 5, 'c_pk': 5, 'c_varchar': 'Arabic: لقد لعبت أنت وأصدقاؤك لمدة وحصلتم علي من إجمالي النقاط'},
                {'c_int': 6, 'c_pk': 6, 'c_varchar': 'Special Characters: [",\'!@£$%^&*()]'}
            ])

    def test_loading_long_text(self):
        """Loading long texts"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-long-texts.json')

        # Load with default settings
        target_postgres.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_long_texts = postgres.query("SELECT * FROM {}.test_table_long_texts ORDER BY c_pk".format(target_schema))

        # Test not very long texts by exact match
        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_long_texts)[:3],
            [
                {'c_int': 1, 'c_pk': 1,
                 'c_varchar': 'Up to 128 characters: Lorem ipsum dolor sit amet, consectetuer adipiscing elit.'},
                {'c_int': 2, 'c_pk': 2,
                 'c_varchar': 'Up to 256 characters: Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies.'},
                {'c_int': 3, 'c_pk': 3,
                 'c_varchar': 'Up to 1024 characters: Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum.'},
            ])

        # Test very long texts by string length
        record_4k = table_long_texts[3]
        record_32k = table_long_texts[4]
        self.assertEqual(
            [
                {'c_int': int(record_4k.get('c_int')), 'c_pk': int(record_4k.get('c_pk')),
                 'len': len(record_4k.get('c_varchar'))},
                {'c_int': int(record_32k.get('c_int')), 'c_pk': int(record_32k.get('c_pk')),
                 'len': len(record_32k.get('c_varchar'))},
            ],
            [
                {'c_int': 4, 'c_pk': 4, 'len': 4017},
                {'c_int': 5, 'c_pk': 5, 'len': 32003},
            ])

    def test_non_db_friendly_columns(self):
        """Loading non-db friendly columns like, camelcase, minus signs, etc."""
        tap_lines = test_utils.get_test_tap_lines('messages-with-non-db-friendly-columns.json')

        # Load with default settings
        target_postgres.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_non_db_friendly_columns = postgres.query(
            "SELECT * FROM {}.test_table_non_db_friendly_columns ORDER BY c_pk".format(target_schema))

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_non_db_friendly_columns),
            [
                {'c_pk': 1, 'camelcasecolumn': 'Dummy row 1', 'minus-column': 'Dummy row 1'},
                {'c_pk': 2, 'camelcasecolumn': 'Dummy row 2', 'minus-column': 'Dummy row 2'},
                {'c_pk': 3, 'camelcasecolumn': 'Dummy row 3', 'minus-column': 'Dummy row 3'},
                {'c_pk': 4, 'camelcasecolumn': 'Dummy row 4', 'minus-column': 'Dummy row 4'},
                {'c_pk': 5, 'camelcasecolumn': 'Dummy row 5', 'minus-column': 'Dummy row 5'},
            ])

    def test_nested_schema_unflattening(self):
        """Loading nested JSON objects into JSONB columns without flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Load with default settings - Flattening disabled
        target_postgres.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables - Transform JSON to string at query time
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        unflattened_table = postgres.query(
            """SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk""".format(target_schema))

        # Should be valid nested JSON strings
        self.assertEqual(
            self.remove_metadata_columns_from_rows(unflattened_table),
            [{
                'c_pk': 1,
                'c_array': [1, 2, 3],
                'c_object': {"key_1": "value_1"},
                'c_object_with_props': {"key_1": "value_1"},
                'c_nested_object': {"nested_prop_1": "nested_value_1", "nested_prop_2": "nested_value_2",
                                    "nested_prop_3": {"multi_nested_prop_1": "multi_value_1",
                                                      "multi_nested_prop_2": "multi_value_2"}}
            }])

    def test_nested_schema_flattening(self):
        """Loading nested JSON objects with flattening and not not flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Turning on data flattening
        self.config['data_flattening_max_level'] = 10

        # Load with default settings - Flattening enabled
        target_postgres.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        flattened_table = postgres.query(
            "SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk".format(target_schema))

        # Should be flattened columns
        self.assertEqual(
            self.remove_metadata_columns_from_rows(flattened_table),
            [{
                'c_pk': 1,
                'c_array': [1, 2, 3],
                'c_object': None,
                # Cannot map RECORD to SCHEMA. SCHEMA doesn't have properties that requires for flattening
                'c_object_with_props__key_1': 'value_1',
                'c_nested_object__nested_prop_1': 'nested_value_1',
                'c_nested_object__nested_prop_2': 'nested_value_2',
                'c_nested_object__nested_prop_3__multi_nested_prop_1': 'multi_value_1',
                'c_nested_object__nested_prop_3__multi_nested_prop_2': 'multi_value_2',
            }])

    def test_column_name_change(self):
        """Tests correct renaming of postgres columns after source change"""
        tap_lines_before_column_name_change = test_utils.get_test_tap_lines('messages-with-multiple-streams.json')
        tap_lines_after_column_name_change = test_utils.get_test_tap_lines(
            'messages-with-multiple-streams-modified-column.json')

        # Load with default settings
        target_postgres.persist_lines(self.config, tap_lines_before_column_name_change)
        target_postgres.persist_lines(self.config, tap_lines_after_column_name_change)

        # Get loaded rows from tables
        postgres = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = postgres.query("SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = postgres.query("SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = postgres.query("SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))

        # Get the previous column name from information schema in test_table_two
        previous_column_name = postgres.query("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_catalog = '{}'
               AND table_schema = '{}'
               AND table_name = 'test_table_two'
               AND ordinal_position = 1
            """.format(
            self.config.get('dbname', '').lower(),
            target_schema.lower()))[0]["column_name"]

        # Table one should have no changes
        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_one),
            [{'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}])

        # Table two should have versioned column
        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_two),
            [
                {previous_column_name: datetime.datetime(2019, 2, 1, 15, 12, 45), 'c_int': 1, 'c_pk': 1,
                 'c_varchar': '1', 'c_date': None},
                {previous_column_name: datetime.datetime(2019, 2, 10, 2), 'c_int': 2, 'c_pk': 2, 'c_varchar': '2',
                 'c_date': '2019-02-12 02:00:00'},
                {previous_column_name: None, 'c_int': 3, 'c_pk': 3, 'c_varchar': '2', 'c_date': '2019-02-15 02:00:00'}
            ]
        )

        # Table three should have renamed columns
        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_three),
            [
                {'c_int': 1, 'c_pk': 1, 'c_time': datetime.time(4, 0), 'c_varchar': '1', 'c_time_renamed': None},
                {'c_int': 2, 'c_pk': 2, 'c_time': datetime.time(7, 15), 'c_varchar': '2', 'c_time_renamed': None},
                {'c_int': 3, 'c_pk': 3, 'c_time': datetime.time(23, 0, 3), 'c_varchar': '3',
                 'c_time_renamed': datetime.time(8, 15)},
                {'c_int': 4, 'c_pk': 4, 'c_time': None, 'c_varchar': '4', 'c_time_renamed': datetime.time(23, 0, 3)}
            ])

    def test_schema_mapping(self):
        """Load stream into a specific schema, create indices and grant permissions"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multiple-streams.json')

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
        self.assert_multiple_streams_are_into_postgres(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )

    def test_grant_privileges(self):
        """Tests GRANT USAGE and SELECT privileges on newly created tables"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multiple-streams.json')

        # Create test users and groups
        postgres = DbSync(self.config)
        postgres.query("DROP USER IF EXISTS user_1")
        postgres.query("DROP USER IF EXISTS user_2")
        try:
            postgres.query("DROP GROUP group_1")  # DROP GROUP has no IF EXISTS
        except:
            pass
        try:
            postgres.query("DROP GROUP group_2")
        except:
            pass
        postgres.query("CREATE USER user_1 WITH PASSWORD 'Abcdefgh1234'")
        postgres.query("CREATE USER user_2 WITH PASSWORD 'Abcdefgh1234'")
        postgres.query("CREATE GROUP group_1 WITH USER user_1, user_2")
        postgres.query("CREATE GROUP group_2 WITH USER user_2")

        # When grantees is a string then privileges should be granted to single user
        postgres.query("DROP SCHEMA IF EXISTS {} CASCADE".format(self.config['default_target_schema']))
        self.config['default_target_schema_select_permissions'] = 'group_1'
        target_postgres.persist_lines(self.config, tap_lines)

        # When grantees is a list then privileges should be granted to list of user
        postgres.query("DROP SCHEMA IF EXISTS {} CASCADE".format(self.config['default_target_schema']))
        self.config['default_target_schema_select_permissions'] = ['group_1', 'group_2']
        target_postgres.persist_lines(self.config, tap_lines)

        # Grant privileges as dict should pass but should be ignored - Dict not supported for target-postgres
        postgres.query("DROP SCHEMA IF EXISTS {} CASCADE".format(self.config['default_target_schema']))
        self.config['default_target_schema_select_permissions'] = {
            'users': ['user_1', 'user_2'],
            'groups': ['group_1', 'group_2']}
        target_postgres.persist_lines(self.config, tap_lines)

        # Granting not existing group should raise exception
        postgres.query("DROP SCHEMA IF EXISTS {} CASCADE".format(self.config['default_target_schema']))
        with self.assertRaises(Exception):
            self.config['default_target_schema_select_permissions'] = 'group_not_exists_1'
            target_postgres.persist_lines(self.config, tap_lines)

        # Granting not existing list of groups should raise exception
        postgres.query("DROP SCHEMA IF EXISTS {} CASCADE".format(self.config['default_target_schema']))
        with self.assertRaises(Exception):
            self.config['default_target_schema_select_permissions'] = ['group_not_exists_1', 'group_not_exists_2']
            target_postgres.persist_lines(self.config, tap_lines)

    def test_logical_streams_from_pg_with_hard_delete_and_default_batch_size_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        target_postgres.persist_lines(self.config, tap_lines)

        self.assert_logical_streams_are_in_postgres(True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        target_postgres.persist_lines(self.config, tap_lines)

        self.assert_logical_streams_are_in_postgres(True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_and_no_records_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams-no-records.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        target_postgres.persist_lines(self.config, tap_lines)

        self.assert_logical_streams_are_in_postgres_and_are_empty()

    @mock.patch('target_postgres.emit_state')
    def test_flush_streams_with_no_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when no intermediate flush required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size big enough to never has to flush in the middle
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 1000
        target_postgres.persist_lines(self.config, tap_lines)

        # State should be emitted only once with the latest received STATE message
        self.assertEqual(
            mock_emit_state.mock_calls,
            [
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}})
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_postgres(True)

    @mock.patch('target_postgres.emit_state')
    def test_flush_streams_with_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when intermediate flushes required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        target_postgres.persist_lines(self.config, tap_lines)

        # State should be emitted multiple times, updating the positions only in the stream which got flushed
        self.assertEqual(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flushed edgydata until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #2 - Flushed logical1-logical1_table2 until lsn: 108201336
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #3 - Flushed logical1-logical1_table2 until lsn: 108237600
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #4 - Flushed logical1-logical1_table2 until lsn: 108238768
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #5 - Flushed logical1-logical1_table2 until lsn: 108239704,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream lsn: 108240872,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_postgres(True)

    @mock.patch('target_postgres.emit_state')
    def test_flush_streams_with_intermediate_flushes_on_all_streams(self, mock_emit_state):
        """Test emitting states when intermediate flushes required and flush_all_streams is enabled"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.config['flush_all_streams'] = True
        target_postgres.persist_lines(self.config, tap_lines)

        # State should be emitted 6 times, flushing every stream and updating every stream position
        self.assertEqual(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flush every stream until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #2 - Flush every stream until lsn 108201336
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #3 - Flush every stream until lsn: 108237600
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #4 - Flush every stream until lsn: 108238768
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #5 - Flush every stream until lsn: 108239704,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream until lsn: 108240872,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_postgres(True)

    def test_record_validation(self):
        """Test validating records"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-invalid-records.json')

        # Loading invalid records when record validation enabled should fail at ...
        self.config['validate_records'] = True

        with self.assertRaises(RecordValidationException):
            target_postgres.persist_lines(self.config, tap_lines)

        # Loading invalid records when record validation disabled should fail at load time
        self.config['validate_records'] = False
        with self.assertRaises(InvalidTextRepresentation):
            target_postgres.persist_lines(self.config, tap_lines)

    def test_loading_tables_with_custom_temp_dir(self):
        """Loading multiple tables from the same input tap using custom temp directory"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multiple-streams.json')

        # Setting custom temp_dir
        self.config['temp_dir'] = '~/.pipelinewise/tmp'
        target_postgres.persist_lines(self.config, tap_lines)

        self.assert_multiple_streams_are_into_postgres()
