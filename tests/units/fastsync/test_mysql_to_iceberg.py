"""Unit tests for fastsync mysql_to_iceberg module"""
import unittest
from unittest.mock import patch, Mock

from pipelinewise.fastsync.mysql_to_iceberg import tap_type_to_target_type


class TestMySQLToIceberg(unittest.TestCase):
    """Test mysql_to_iceberg module"""

    def test_tap_type_to_target_type_numeric_types(self):
        """Test numeric type mappings"""
        self.assertEqual(tap_type_to_target_type('tinyint'), 'INTEGER')
        self.assertEqual(tap_type_to_target_type('smallint'), 'INTEGER')
        self.assertEqual(tap_type_to_target_type('mediumint'), 'INTEGER')
        self.assertEqual(tap_type_to_target_type('int'), 'INTEGER')
        self.assertEqual(tap_type_to_target_type('bigint'), 'BIGINT')
        self.assertEqual(tap_type_to_target_type('float'), 'DOUBLE')
        self.assertEqual(tap_type_to_target_type('double'), 'DOUBLE')
        self.assertEqual(tap_type_to_target_type('decimal'), 'NUMERIC')
        self.assertEqual(tap_type_to_target_type('numeric'), 'NUMERIC')

    def test_tap_type_to_target_type_string_types(self):
        """Test string type mappings"""
        self.assertEqual(tap_type_to_target_type('char'), 'STRING')
        self.assertEqual(tap_type_to_target_type('varchar'), 'STRING')
        self.assertEqual(tap_type_to_target_type('text'), 'STRING')
        self.assertEqual(tap_type_to_target_type('tinytext'), 'STRING')
        self.assertEqual(tap_type_to_target_type('mediumtext'), 'STRING')
        self.assertEqual(tap_type_to_target_type('longtext'), 'STRING')

    def test_tap_type_to_target_type_temporal_types(self):
        """Test temporal type mappings"""
        self.assertEqual(tap_type_to_target_type('date'), 'DATE')
        self.assertEqual(tap_type_to_target_type('datetime'), 'TIMESTAMP')
        self.assertEqual(tap_type_to_target_type('timestamp'), 'TIMESTAMP')
        self.assertEqual(tap_type_to_target_type('time'), 'STRING')
        self.assertEqual(tap_type_to_target_type('year'), 'INTEGER')

    def test_tap_type_to_target_type_binary_types(self):
        """Test binary type mappings"""
        self.assertEqual(tap_type_to_target_type('binary'), 'BINARY')
        self.assertEqual(tap_type_to_target_type('varbinary'), 'BINARY')
        self.assertEqual(tap_type_to_target_type('blob'), 'BINARY')
        self.assertEqual(tap_type_to_target_type('tinyblob'), 'BINARY')
        self.assertEqual(tap_type_to_target_type('mediumblob'), 'BINARY')
        self.assertEqual(tap_type_to_target_type('longblob'), 'BINARY')

    def test_tap_type_to_target_type_json(self):
        """Test JSON type mapping"""
        self.assertEqual(tap_type_to_target_type('json'), 'STRING')

    def test_tap_type_to_target_type_enum_and_set(self):
        """Test ENUM and SET type mappings"""
        self.assertEqual(tap_type_to_target_type('enum'), 'STRING')
        self.assertEqual(tap_type_to_target_type('set'), 'STRING')

    def test_tap_type_to_target_type_bit(self):
        """Test BIT type mapping"""
        self.assertEqual(tap_type_to_target_type('bit'), 'BOOLEAN')

    def test_tap_type_to_target_type_unknown_type_defaults_to_string(self):
        """Test unknown types default to STRING"""
        self.assertEqual(tap_type_to_target_type('unknown_type'), 'STRING')
        self.assertEqual(tap_type_to_target_type('custom_type'), 'STRING')


if __name__ == '__main__':
    unittest.main()
