import unittest
from unittest.mock import patch

from target_snowflake.exceptions import InvalidFileFormatException, FileFormatNotFoundException
from target_snowflake.file_format import FileFormat, FileFormatTypes
from target_snowflake.file_formats import csv, parquet


class TestFileFormat(unittest.TestCase):
    """
    Unit Tests
    """

    def test_get_formatter(self):
        self.assertEqual(FileFormat._get_formatter(FileFormatTypes.CSV), csv)
        self.assertEqual(FileFormat._get_formatter(FileFormatTypes.PARQUET), parquet)
        with self.assertRaises(InvalidFileFormatException):
            FileFormat._get_formatter('UNKNOWN')


    @patch('target_snowflake.db_sync.DbSync.query')
    def test_detect_file_format_type(self, query_patch):
        minimal_config = {
            'account': 'foo',
            'dbname': 'foo',
            'user': 'foo',
            'password': 'foo',
            'warehouse': 'foo',
            'default_target_schema': 'foo',
            'file_format': 'foo',
            's3_bucket': 'foo',
            'stage': 'foo.foo'
        }

        # List method should return values as list
        self.assertEqual(FileFormatTypes.list(), ['csv', 'parquet'])

        # CSV should be supported
        query_patch.return_value = [{ 'type': 'CSV' }]
        file_format = FileFormat('foo', query_patch)
        self.assertEqual(file_format.file_format_type, FileFormatTypes.CSV)

        # File format functions should be mapped to csv module
        self.assertEqual(file_format.formatter.records_to_file.__module__, csv.records_to_file.__module__)
        self.assertEqual(file_format.formatter.create_merge_sql.__module__, csv.create_merge_sql.__module__)
        self.assertEqual(file_format.formatter.create_copy_sql.__module__, csv.create_copy_sql.__module__)

        # Parquet should be supported
        query_patch.return_value = [{ 'type': 'PARQUET' }]
        file_format = FileFormat('foo', query_patch)
        self.assertEqual(file_format.file_format_type, FileFormatTypes.PARQUET)

        # File format functions should be mapped to parquet module
        self.assertEqual(file_format.formatter.records_to_file.__module__, parquet.records_to_file.__module__)
        self.assertEqual(file_format.formatter.create_merge_sql.__module__, parquet.create_merge_sql.__module__)
        self.assertEqual(file_format.formatter.create_copy_sql.__module__, parquet.create_copy_sql.__module__)

        # Empty result should raise exception
        query_patch.return_value = []
        with self.assertRaises(FileFormatNotFoundException):
            FileFormat('foo', query_patch)

        # Multiple result rows should raise exception
        query_patch.return_value = [{ 'type': 'CSV' }, { 'type': 'CSV'}]
        with self.assertRaises(FileFormatNotFoundException):
            FileFormat('foo', query_patch)

        # Not supported file format type should raise exception
        query_patch.return_value = [{ 'type': 'NOT_SUPPORTED_TYPE' }]
        with self.assertRaises(InvalidFileFormatException):
            FileFormat('foo', query_patch)
