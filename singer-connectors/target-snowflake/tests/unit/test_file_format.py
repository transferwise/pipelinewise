import json
import unittest
from unittest.mock import patch

from target_snowflake.exceptions import InvalidFileFormatException, FileFormatNotFoundException
from target_snowflake.file_format import FileFormat, FileFormatTypes
from target_snowflake.file_formats import csv


def _csv_file_format_result(name='FOO', database_name='DB', schema_name='SCHEMA', **overrides):
    options = dict(csv.REQUIRED_FILE_FORMAT_OPTIONS)
    options.update(overrides)
    return [{
        'name': name,
        'database_name': database_name,
        'schema_name': schema_name,
        'type': 'CSV',
        'format_options': json.dumps(options),
    }]


class TestFileFormat(unittest.TestCase):
    """
    Unit Tests
    """

    def test_get_formatter(self):
        self.assertEqual(FileFormat._get_formatter(FileFormatTypes.CSV), csv)
        with self.assertRaises(InvalidFileFormatException):
            FileFormat._get_formatter('UNKNOWN')

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_detect_file_format_type(self, query_patch):
        # CSV should be supported
        query_patch.return_value = _csv_file_format_result()
        file_format = FileFormat('foo', query_patch)
        self.assertEqual(file_format.file_format_type, FileFormatTypes.CSV)

        # File format functions should be mapped to csv module
        self.assertEqual(file_format.formatter.records_to_file.__module__, csv.records_to_file.__module__)
        self.assertEqual(file_format.formatter.create_merge_sql.__module__, csv.create_merge_sql.__module__)
        self.assertEqual(file_format.formatter.create_copy_sql.__module__, csv.create_copy_sql.__module__)

        # Parquet staging should fail explicitly rather than falling back to CSV
        query_patch.return_value = [{'name': 'FOO', 'type': 'PARQUET'}]
        with self.assertRaisesRegex(InvalidFileFormatException, 'CSV'):
            FileFormat('foo', query_patch)

        # Empty result should raise exception
        query_patch.return_value = []
        with self.assertRaises(FileFormatNotFoundException):
            FileFormat('foo', query_patch)

        # Multiple result rows should raise exception
        query_patch.return_value = [{'name': 'FOO', 'type': 'CSV'}, {'name': 'FOO', 'type': 'CSV'}]
        with self.assertRaises(FileFormatNotFoundException):
            FileFormat('foo', query_patch)

        # Not supported file format type should raise exception
        query_patch.return_value = [{'name': 'FOO', 'type': 'NOT_SUPPORTED_TYPE'}]
        with self.assertRaises(InvalidFileFormatException):
            FileFormat('foo', query_patch)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_rejects_incompatible_csv_options(self, query_patch):
        for option, expected in csv.REQUIRED_FILE_FORMAT_OPTIONS.items():
            if isinstance(expected, bool):
                incompatible = not expected
            elif isinstance(expected, int):
                incompatible = expected + 1
            elif isinstance(expected, list):
                incompatible = ['\\N']
            else:
                incompatible = f'{expected}-invalid'

            with self.subTest(option=option):
                query_patch.return_value = _csv_file_format_result(**{option: incompatible})
                with self.assertRaisesRegex(InvalidFileFormatException, option):
                    FileFormat('foo', query_patch)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_rejects_missing_or_malformed_csv_options(self, query_patch):
        query_patch.return_value = [{'name': 'FOO', 'type': 'CSV'}]
        with self.assertRaisesRegex(InvalidFileFormatException, 'did not return format_options'):
            FileFormat('foo', query_patch)

        query_patch.return_value = [{'name': 'FOO', 'type': 'CSV', 'format_options': '{not-json'}]
        with self.assertRaisesRegex(InvalidFileFormatException, 'invalid format_options'):
            FileFormat('foo', query_patch)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_scopes_lookup_to_configured_schema(self, query_patch):
        for name, scope in (
            ('foo', ''),
            ('schema.foo', ' "SCHEMA"'),
            ('db.schema.foo', ' "DB"."SCHEMA"'),
            (' db . schema . foo ', ' "DB"."SCHEMA"'),
        ):
            with self.subTest(name=name):
                query_patch.reset_mock()
                query_patch.return_value = _csv_file_format_result()
                self.assertEqual(FileFormat(name, query_patch).file_format_type, FileFormatTypes.CSV)
                query_patch.assert_called_once_with(f"SHOW FILE FORMATS LIKE 'FOO' IN SCHEMA{scope}")

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_double_dot_resolves_public_schema(self, query_patch):
        for name, database in (('db..foo', 'DB'), ('"Db.Name" . . foo', 'Db.Name')):
            with self.subTest(name=name):
                query_patch.reset_mock()
                query_patch.return_value = _csv_file_format_result(database_name=database, schema_name='PUBLIC')
                self.assertEqual(FileFormat(name, query_patch).file_format_type, FileFormatTypes.CSV)
                query_patch.assert_called_once_with(
                    f'SHOW FILE FORMATS LIKE \'FOO\' IN SCHEMA "{database}"."PUBLIC"'
                )

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_preserves_quoted_identifier_components(self, query_patch):
        query_patch.return_value = _csv_file_format_result(
            name='Csv.format', database_name='Db.Name', schema_name='My "schema"'
        )
        file_format = FileFormat('"Db.Name"."My ""schema"""."Csv.format"', query_patch)
        self.assertEqual(file_format.file_format_type, FileFormatTypes.CSV)
        query_patch.assert_called_once_with(
            'SHOW FILE FORMATS LIKE \'Csv.format\' IN SCHEMA "Db.Name"."My ""schema"""'
        )

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_selects_exact_qualified_object_not_same_named_format(self, query_patch):
        query_patch.return_value = (
            _csv_file_format_result(database_name='OTHER_DB', NULL_IF=['\\N'])
            + _csv_file_format_result(schema_name='OTHER_SCHEMA', NULL_IF=['\\N'])
            + _csv_file_format_result()
        )
        self.assertEqual(FileFormat('db.schema.foo', query_patch).file_format_type, FileFormatTypes.CSV)

        query_patch.return_value = (
            _csv_file_format_result(database_name='OTHER_DB')
            + _csv_file_format_result(schema_name='OTHER_SCHEMA')
        )
        with self.assertRaises(FileFormatNotFoundException):
            FileFormat('db.schema.foo', query_patch)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_does_not_validate_compatible_namesake_instead_of_configured_format(self, query_patch):
        query_patch.return_value = (
            _csv_file_format_result(schema_name='OTHER_SCHEMA')
            + _csv_file_format_result(NULL_IF=['\\N'])
        )
        with self.assertRaisesRegex(InvalidFileFormatException, 'db.schema.foo.*NULL_IF'):
            FileFormat('db.schema.foo', query_patch)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_filters_like_wildcards_and_quoted_case_exactly(self, query_patch):
        query_patch.return_value = (
            _csv_file_format_result(name='CSV_FORMAT_EXTRA', NULL_IF=['\\N'])
            + _csv_file_format_result(name='CsvXFormat%', NULL_IF=['\\N'])
            + _csv_file_format_result(name='Csv_Format%')
        )
        self.assertEqual(FileFormat('"Csv_Format%"', query_patch).file_format_type, FileFormatTypes.CSV)
        query_patch.assert_called_once_with("SHOW FILE FORMATS LIKE 'Csv_Format%' IN SCHEMA")

        query_patch.return_value = _csv_file_format_result(name='CSV_FORMAT%')
        with self.assertRaises(FileFormatNotFoundException):
            FileFormat('"Csv_Format%"', query_patch)

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_escapes_name_inside_like_literal(self, query_patch):
        query_patch.return_value = _csv_file_format_result(name="Csv'Format\\")
        FileFormat('"Csv\'Format\\"', query_patch)
        query_patch.assert_called_once_with("SHOW FILE FORMATS LIKE 'Csv''Format\\\\\\\\' IN SCHEMA")

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_rejects_malformed_identifiers_before_query(self, query_patch):
        for name in ('', 'db...foo', 'db.', '.foo', 'db.schema.foo.extra', '"unfinished', 'foo; DROP TABLE x', '""'):
            with self.subTest(name=name):
                with self.assertRaisesRegex(InvalidFileFormatException, 'Invalid named file format identifier'):
                    FileFormat(name, query_patch)
        query_patch.assert_not_called()
