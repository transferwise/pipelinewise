import unittest

from tap_yugabyte import db


class TestDbFunctions(unittest.TestCase):
    maxDiff = None

    def test_value_to_singer_value(self):
        """Every element is converted from its sql_datatype to the correct singer type"""
        # JSON and JSONB are converted to dictionaries
        self.assertEqual({'test': 123}, db.selected_value_to_singer_value_impl('{"test": 123}', 'json'))
        self.assertEqual({'test': 123}, db.selected_value_to_singer_value_impl('{"test": 123}', 'jsonb'))

        # time with time zone values are converted to UTC and the time zone dropped.
        # Hour 24 is taken as 0
        self.assertEqual('20:00:00', db.selected_value_to_singer_value_impl('12:00:00-0800', 'time with time zone'))
        self.assertEqual('08:00:00', db.selected_value_to_singer_value_impl('24:00:00-0800', 'time with time zone'))

        # bit is True only if the element is '1'
        self.assertEqual(True, db.selected_value_to_singer_value_impl('1', 'bit'))
        self.assertEqual(False, db.selected_value_to_singer_value_impl('0', 'bit'))
