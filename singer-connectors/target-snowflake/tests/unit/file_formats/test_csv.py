import csv as csv_module
import gzip
import json
import os
import tempfile
import unittest

import target_snowflake.file_formats.csv as csv


def _mock_record_to_csv_line(record, schema, data_flattening_max_level=0):
    return record


def _read_csv_row(csv_line):
    return next(csv_module.reader(
        [csv_line],
        escapechar='\\',
        doublequote=False,
    ))


class TestCsv(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.config = {}

    def test_required_file_format_options(self):
        self.assertEqual(csv.REQUIRED_FILE_FORMAT_OPTIONS, {
            'TYPE': 'CSV',
            'RECORD_DELIMITER': '\n',
            'FIELD_DELIMITER': ',',
            'SKIP_HEADER': 0,
            'PARSE_HEADER': False,
            'ESCAPE': '\\',
            'TRIM_SPACE': False,
            'FIELD_OPTIONALLY_ENCLOSED_BY': '"',
            'NULL_IF': [],
            'SKIP_BLANK_LINES': False,
            'EMPTY_FIELD_AS_NULL': True,
            'ENCODING': 'UTF8',
            'MULTI_LINE': True,
        })

    def test_write_record_to_uncompressed_file(self):
        records = {
            'pk_1': 'data1,data2,data3,data4',
            'pk_2': 'data5,data6,data7,data8'
        }
        schema = {}

        # Write uncompressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False)
        with open(csv_file.name, 'wb') as f:
            csv.write_records_to_file(f, records, schema, _mock_record_to_csv_line)

        # Read and validate uncompressed CSV file
        with open(csv_file.name, 'rt') as f:
            self.assertEqual(f.readlines(), ['data1,data2,data3,data4\n',
                                             'data5,data6,data7,data8\n'])

        os.remove(csv_file.name)

    def test_write_records_to_compressed_file(self):
        records = {
            'pk_1': 'data1,data2,data3,data4',
            'pk_2': 'data5,data6,data7,data8'
        }
        schema = {}

        # Write gzip compressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False)
        with gzip.open(csv_file.name, 'wb') as f:
            csv.write_records_to_file(f, records, schema, _mock_record_to_csv_line)

        # Read and validate gzip compressed CSV file
        with gzip.open(csv_file.name, 'rt') as f:
            self.assertEqual(f.readlines(), ['data1,data2,data3,data4\n',
                                             'data5,data6,data7,data8\n'])

        os.remove(csv_file.name)

    def test_record_to_csv_line(self):
        record = {
            'key1': '1',
            'key2': '2030-01-22',
            'key3': '10000-01-22 12:04:22',
            'key4': '25:01:01',
            'key5': 'I\'m good',
            'key6': None,
        }

        schema = {
            'key1': {
                'type': ['null', 'string', 'integer'],
            },
            'key2': {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'date'},
                    {'type': ['null', 'string']}
                ]
            },
            'key3': {
                'type': ['null', 'string'], 'format': 'date-time',
            },
            'key4': {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'time'},
                    {'type': ['null', 'string']}
                ]
            },
            'key5': {
                'type': ['null', 'string'],
            },
            'key6': {
                'type': ['null', 'string'], 'format': 'time',
            },
        }

        self.assertEqual(csv.record_to_csv_line(record, schema),
                         '"1","2030-01-22","10000-01-22 12:04:22","25:01:01","I\'m good",')

    def test_record_to_csv_line_preserves_falsey_values_except_null(self):
        record = {
            'empty_string': '',
            'empty_list': [],
            'empty_object': {},
            'false': False,
            'none': None,
        }
        schema = {
            'empty_string': {'type': ['null', 'string']},
            'empty_list': {'type': ['null', 'array']},
            'empty_object': {'type': ['null', 'object']},
            'false': {'type': ['null', 'boolean']},
            'none': {'type': ['null', 'string']},
        }

        self.assertEqual(
            csv.record_to_csv_line(record, schema),
            '"","[]","{}",false,',
        )

    def test_record_to_csv_line_keeps_non_string_scalars_unquoted(self):
        record = {
            'integer': 1,
            'number': 1.5,
            'boolean': False,
        }
        schema = {
            'integer': {'type': ['null', 'integer']},
            'number': {'type': ['null', 'number']},
            'boolean': {'type': ['null', 'boolean']},
        }

        self.assertEqual(
            csv.record_to_csv_line(record, schema),
            '1,1.5,false',
        )

    def test_record_to_csv_line_preserves_multiline_and_escaped_characters(self):
        record = {
            'lf': 'line one\nline two',
            'cr': 'line one\rline two',
            'crlf': 'line one\r\nline two',
            'tab': 'left\tright',
            'literal_escapes': r'left\nright\tend',
            'literal_null_marker': r'\N',
            'quoted_comma': 'say "hello", world',
            'unicode': '初雪',
            'trailing_backslash': 'C:\\data\\',
            'empty_string': '',
            'sql_null': None,
        }
        schema = {
            column: {'type': ['null', 'string']}
            for column in record
        }

        csv_line = csv.record_to_csv_line(record, schema)

        self.assertEqual(_read_csv_row(csv_line)[:-1], list(record.values())[:-1])
        self.assertEqual(_read_csv_row(csv_line)[-1], '')
        self.assertIn('line one\nline two', csv_line)
        self.assertIn('line one\rline two', csv_line)
        self.assertIn('line one\r\nline two', csv_line)
        self.assertIn('left\tright', csv_line)
        self.assertIn(r'left\\nright\\tend', csv_line)
        self.assertIn(r'"\\N"', csv_line)
        self.assertIn('C:\\\\data\\\\', csv_line)
        self.assertTrue(csv_line.endswith(',"",'))

    def test_record_to_csv_line_preserves_serialized_mariadb_json_roots(self):
        mariadb_json_schema = {
            'type': ['null', 'object', 'array', 'string', 'number', 'boolean'],
            'format': 'mariadb-json',
        }
        record = {
            'object_value': '{"nested":[null,{}],"unicode":"初"}',
            'array_value': '[1,null,{}]',
            'string_value': '"text"',
            'integer_value': '42',
            'fractional_value': '1.5',
            'true_value': 'true',
            'false_value': 'false',
            'json_null': 'null',
            'sql_null': None,
        }
        schema = {column: mariadb_json_schema for column in record}

        fields = _read_csv_row(csv.record_to_csv_line(record, schema))

        self.assertEqual(fields[:-1], list(record.values())[:-1])
        self.assertEqual(
            [json.loads(value) for value in fields[:-1]],
            [
                {'nested': [None, {}], 'unicode': '初'},
                [1, None, {}],
                'text',
                42,
                1.5,
                True,
                False,
                None,
            ],
        )
        self.assertEqual(fields[-1], '')

    def test_create_copy_sql(self):
        self.assertEqual(csv.create_copy_sql(table_name='foo_table',
                                             stage_name='foo_stage',
                                             s3_key='foo_s3_key.csv',
                                             file_format_name='foo_file_format',
                                             columns=[{'name': 'COL_1'},
                                                      {'name': 'COL_2'},
                                                      {'name': 'COL_3',
                                                       'trans': 'parse_json'}]),

                         "COPY INTO foo_table (COL_1, COL_2, COL_3) FROM "
                         "'@foo_stage/foo_s3_key.csv' "
                         "FILE_FORMAT = (format_name='foo_file_format')")

    def test_create_merge_sql(self):
        self.assertEqual(csv.create_merge_sql(table_name='foo_table',
                                             stage_name='foo_stage',
                                             s3_key='foo_s3_key.csv',
                                             file_format_name='foo_file_format',
                                             columns=[{'name': 'COL_1', 'trans': ''},
                                                      {'name': 'COL_2', 'trans': ''},
                                                      {'name': 'COL_3', 'trans': 'parse_json'}],
                                             pk_merge_condition='s.COL_1 = t.COL_1'),

                         "MERGE INTO foo_table t USING ("
                         "SELECT ($1) COL_1, ($2) COL_2, parse_json($3) COL_3 "
                         "FROM '@foo_stage/foo_s3_key.csv' "
                         "(FILE_FORMAT => 'foo_file_format')) s "
                         "ON s.COL_1 = t.COL_1 "
                         "WHEN MATCHED THEN UPDATE SET COL_1=s.COL_1, COL_2=s.COL_2, COL_3=s.COL_3 "
                         "WHEN NOT MATCHED THEN "
                         "INSERT (COL_1, COL_2, COL_3) "
                         "VALUES (s.COL_1, s.COL_2, s.COL_3)")

    def test_create_merge_sql_with_restricted_update_columns(self):
        self.assertEqual(csv.create_merge_sql(table_name='foo_table',
                                             stage_name='foo_stage',
                                             s3_key='foo_s3_key.csv',
                                             file_format_name='foo_file_format',
                                             columns=[{'name': 'COL_1', 'trans': ''},
                                                      {'name': 'COL_2', 'trans': ''},
                                                      {'name': 'COL_3', 'trans': 'parse_json'}],
                                             pk_merge_condition='s.COL_1 = t.COL_1',
                                             update_columns={'COL_2'}),

                         "MERGE INTO foo_table t USING ("
                         "SELECT ($1) COL_1, ($2) COL_2, parse_json($3) COL_3 "
                         "FROM '@foo_stage/foo_s3_key.csv' "
                         "(FILE_FORMAT => 'foo_file_format')) s "
                         "ON s.COL_1 = t.COL_1 "
                         "WHEN MATCHED THEN UPDATE SET COL_2=s.COL_2 "
                         "WHEN NOT MATCHED THEN "
                         "INSERT (COL_1, COL_2, COL_3) "
                         "VALUES (s.COL_1, s.COL_2, s.COL_3)")

    def test_create_merge_sql_with_no_update_columns(self):
        merge_sql = csv.create_merge_sql(table_name='foo_table',
                                         stage_name='foo_stage',
                                         s3_key='foo_s3_key.csv',
                                         file_format_name='foo_file_format',
                                         columns=[{'name': 'COL_1', 'trans': ''},
                                                  {'name': 'COL_2', 'trans': ''}],
                                         pk_merge_condition='s.COL_1 = t.COL_1',
                                         update_columns=[])

        self.assertNotIn('WHEN MATCHED THEN UPDATE', merge_sql)
        self.assertIn('INSERT (COL_1, COL_2) VALUES (s.COL_1, s.COL_2)', merge_sql)
