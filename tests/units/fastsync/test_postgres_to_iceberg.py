"""Unit tests for fastsync postgres_to_iceberg module"""
import unittest
from unittest.mock import patch, Mock

from pipelinewise.fastsync.postgres_to_iceberg import tap_type_to_target_type


class TestPostgresToIceberg(unittest.TestCase):
    """Test postgres_to_iceberg module"""

    def test_tap_type_to_target_type_numeric_types(self):
        """Test numeric type mappings"""
        self.assertEqual(tap_type_to_target_type('serial'), 'BIGINT')
        self.assertEqual(tap_type_to_target_type('bigserial'), 'BIGINT')
        self.assertEqual(tap_type_to_target_type('smallint'), 'INTEGER')
        self.assertEqual(tap_type_to_target_type('integer'), 'INTEGER')
        self.assertEqual(tap_type_to_target_type('bigint'), 'BIGINT')
        self.assertEqual(tap_type_to_target_type('real'), 'DOUBLE')
        self.assertEqual(tap_type_to_target_type('double precision'), 'DOUBLE')
        self.assertEqual(tap_type_to_target_type('numeric'), 'NUMERIC')
        self.assertEqual(tap_type_to_target_type('decimal'), 'NUMERIC')

    def test_tap_type_to_target_type_string_types(self):
        """Test string type mappings"""
        self.assertEqual(tap_type_to_target_type('character varying'), 'STRING')
        self.assertEqual(tap_type_to_target_type('varchar'), 'STRING')
        self.assertEqual(tap_type_to_target_type('character'), 'STRING')
        self.assertEqual(tap_type_to_target_type('char'), 'STRING')
        self.assertEqual(tap_type_to_target_type('text'), 'STRING')

    def test_tap_type_to_target_type_temporal_types(self):
        """Test temporal type mappings"""
        self.assertEqual(tap_type_to_target_type('date'), 'DATE')
        self.assertEqual(tap_type_to_target_type('timestamp'), 'TIMESTAMP')
        self.assertEqual(tap_type_to_target_type('timestamp without time zone'), 'TIMESTAMP')
        self.assertEqual(tap_type_to_target_type('timestamp with time zone'), 'TIMESTAMP')
        self.assertEqual(tap_type_to_target_type('time'), 'STRING')
        self.assertEqual(tap_type_to_target_type('time without time zone'), 'STRING')
        self.assertEqual(tap_type_to_target_type('time with time zone'), 'STRING')

    def test_tap_type_to_target_type_boolean(self):
        """Test boolean type mapping"""
        self.assertEqual(tap_type_to_target_type('boolean'), 'BOOLEAN')

    def test_tap_type_to_target_type_binary_types(self):
        """Test binary type mappings"""
        self.assertEqual(tap_type_to_target_type('bytea'), 'BINARY')

    def test_tap_type_to_target_type_json_types(self):
        """Test JSON type mappings"""
        self.assertEqual(tap_type_to_target_type('json'), 'STRING')
        self.assertEqual(tap_type_to_target_type('jsonb'), 'STRING')

    def test_tap_type_to_target_type_array_types(self):
        """Test array type mappings"""
        self.assertEqual(tap_type_to_target_type('ARRAY'), 'STRING')
        self.assertEqual(tap_type_to_target_type('integer[]'), 'STRING')

    def test_tap_type_to_target_type_uuid(self):
        """Test UUID type mapping"""
        self.assertEqual(tap_type_to_target_type('uuid'), 'STRING')

    def test_tap_type_to_target_type_unknown_type_defaults_to_string(self):
        """Test unknown types default to STRING"""
        self.assertEqual(tap_type_to_target_type('unknown_type'), 'STRING')
        self.assertEqual(tap_type_to_target_type('custom_type'), 'STRING')


if __name__ == '__main__':
    unittest.main()
