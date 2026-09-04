import unittest

from tap_yugabyte import db


class TestDbFunctions(unittest.TestCase):
    maxDiff = None

    def test_value_to_singer_value(self):
        """Every element is converted from its sql_datatype to the correct singer type"""
        # JSON and JSONB are converted to dictionaries
        self.assertEqual({'test': 123}, db.selected_value_to_singer_value_impl('{"test": 123}', 'json'))
        self.assertEqual({'test': 123}, db.selected_value_to_singer_value_impl('{"test": 123}', 'jsonb'))

        # psycopg2 deserializes json/jsonb columns by default, so a list/dict is passed through as-is
        self.assertEqual([], db.selected_value_to_singer_value_impl([], 'json'))
        self.assertEqual([{'key': 'v'}], db.selected_value_to_singer_value_impl([{'key': 'v'}], 'jsonb'))

        # time with time zone values are converted to UTC and the time zone dropped.
        # Hour 24 is taken as 0
        self.assertEqual('20:00:00', db.selected_value_to_singer_value_impl('12:00:00-0800', 'time with time zone'))
        self.assertEqual('08:00:00', db.selected_value_to_singer_value_impl('24:00:00-0800', 'time with time zone'))

        # current_timestamp-derived time with time zone values carry microseconds
        self.assertEqual(
            '19:51:56', db.selected_value_to_singer_value_impl('11:51:56.418827-0800', 'time with time zone'))

        # bit is True only if the element is '1'
        self.assertEqual(True, db.selected_value_to_singer_value_impl('1', 'bit'))
        self.assertEqual(False, db.selected_value_to_singer_value_impl('0', 'bit'))

    def test_selected_value_to_singer_value_handles_arrays(self):
        """Array sql_datatypes are recursively converted element by element"""
        self.assertEqual([1, 2, 3], db.selected_value_to_singer_value([1, 2, 3], 'integer[]'))
        self.assertEqual([], db.selected_value_to_singer_value(None, 'integer[]'))
        self.assertEqual(5, db.selected_value_to_singer_value(5, 'integer'))

    def test_calculate_destination_stream_name(self):
        """Destination stream name combines the discovered schema name and the stream name"""
        stream = {'stream': 'country'}
        md_map = {(): {'schema-name': 'public'}}
        self.assertEqual('public-country', db.calculate_destination_stream_name(stream, md_map))

    def test_canonicalize_identifier(self):
        """Double quotes inside an identifier are doubled up so they can be safely quoted"""
        self.assertEqual('foo', db.canonicalize_identifier('foo'))
        self.assertEqual('fo""o', db.canonicalize_identifier('fo"o'))

    def test_fully_qualified_table_name(self):
        """Schema and table are individually quoted and joined with a dot"""
        self.assertEqual('"public"."country"', db.fully_qualified_table_name('public', 'country'))

    def test_fully_qualified_column_name(self):
        """Schema, table, and column are individually quoted and joined with dots"""
        self.assertEqual('"public"."country"."code"', db.fully_qualified_column_name('public', 'country', 'code'))

    def test_prepare_columns_for_select_sql_clamps_timestamps(self):
        """Timestamp columns are wrapped in a CASE expression that clamps out-of-range values"""
        md_map = {('properties', 'created_at'): {'sql-datatype': 'timestamp without time zone'}}
        sql = db.prepare_columns_for_select_sql('created_at', md_map)
        self.assertIn('CASE', sql)
        self.assertIn('"created_at"', sql)

    def test_prepare_columns_for_select_sql_clamps_dates(self):
        """Date columns are clamped too: YSQL/Postgres dates exceed Python's year-9999 limit"""
        md_map = {('properties', 'event_date'): {'sql-datatype': 'date'}}
        sql = db.prepare_columns_for_select_sql('event_date', md_map)
        self.assertIn('CASE', sql)
        self.assertIn('"event_date"', sql)
        self.assertNotIn('00:00:00', sql)

    def test_prepare_columns_for_select_sql_passes_through_other_types(self):
        """Non-timestamp columns are simply quoted, with no CASE wrapping"""
        md_map = {('properties', 'code'): {'sql-datatype': 'character'}}
        self.assertEqual(' "code" ', db.prepare_columns_for_select_sql('code', md_map))

    def test_selected_row_to_singer_message(self):
        """A selected row is converted into a RecordMessage with correctly typed values"""
        stream = {'stream': 'country', 'tap_stream_id': 'public-country'}
        md_map = {(): {'schema-name': 'public'},
                  ('properties', 'code'): {'sql-datatype': 'character'},
                  ('properties', 'name'): {'sql-datatype': 'text'}}
        message = db.selected_row_to_singer_message(
            stream, ('USA', 'United States'), 12345, ['code', 'name'], None, md_map)
        self.assertEqual('public-country', message.stream)
        self.assertEqual({'code': 'USA', 'name': 'United States'}, message.record)
        self.assertEqual(12345, message.version)
