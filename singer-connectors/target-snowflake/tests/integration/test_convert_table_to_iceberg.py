import logging
import os
import unittest
import uuid
from decimal import Decimal

from target_snowflake.convert_table_to_iceberg import CopyNativeToIceberg

try:
    import tests.integration.utils as test_utils
except ImportError:
    import utils as test_utils


REQUIRED_SNOWFLAKE_ENV = (
    'TARGET_SNOWFLAKE_ACCOUNT',
    'TARGET_SNOWFLAKE_DBNAME',
    'TARGET_SNOWFLAKE_USER',
    'TARGET_SNOWFLAKE_PRIVATE_KEY',
    'TARGET_SNOWFLAKE_WAREHOUSE',
)

NATIVE_COLUMN_TYPES = [
    {'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 19, 'NUMERIC_SCALE': 0},
    {'COLUMN_NAME': 'AMOUNT', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 38, 'NUMERIC_SCALE': 10},
    {'COLUMN_NAME': 'CREATED_AT', 'DATA_TYPE': 'TIMESTAMP_TZ', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None},
    {'COLUMN_NAME': 'EVENT_TIME', 'DATA_TYPE': 'TIME', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None},
    {'COLUMN_NAME': 'PAYLOAD', 'DATA_TYPE': 'VARIANT', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None},
    {'COLUMN_NAME': 'Display Name', 'DATA_TYPE': 'TEXT', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None},
]

ICEBERG_COLUMN_TYPES = [
    {'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 19, 'NUMERIC_SCALE': 0},
    {'COLUMN_NAME': 'AMOUNT', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 38, 'NUMERIC_SCALE': 10},
    {'COLUMN_NAME': 'CREATED_AT', 'DATA_TYPE': 'TIMESTAMP_LTZ', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None},
    {'COLUMN_NAME': 'EVENT_TIME', 'DATA_TYPE': 'TIME', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None},
    {'COLUMN_NAME': 'PAYLOAD', 'DATA_TYPE': 'TEXT', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None},
    {'COLUMN_NAME': 'Display Name', 'DATA_TYPE': 'TEXT', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None},
]

EXPECTED_ROWS = [
    {
        'ID': 7,
        'AMOUNT': Decimal('1234567890.1234567890'),
        'CREATED_EPOCH_MICROSECOND': 1704164645123456,
        'EVENT_TIME': '12:34:56.123456',
        'PAYLOAD': '{"kind":"first"}',
        'DISPLAY_NAME': 'First row',
    },
    {
        'ID': 8,
        'AMOUNT': Decimal('-0.0000000001'),
        'CREATED_EPOCH_MICROSECOND': 1717270245654321,
        'EVENT_TIME': '23:59:59.987654',
        'PAYLOAD': None,
        'DISPLAY_NAME': 'Second row',
    },
]

EXPECTED_PRIMARY_KEY = [
    {'COLUMN_NAME': 'ID', 'KEY_SEQUENCE': 1},
    {'COLUMN_NAME': 'Display Name', 'KEY_SEQUENCE': 2},
]


@unittest.skipUnless(
    all(os.environ.get(name) for name in REQUIRED_SNOWFLAKE_ENV),
    'Snowflake credentials are required for CopyNativeToIceberg integration tests',
)
class TestCopyNativeToIcebergIntegration(unittest.TestCase):
    def setUp(self):
        self.config = test_utils.get_db_config()

        self.executor = object.__new__(CopyNativeToIceberg)
        self.executor.connection_config = self.config
        self.executor.fqtn = self.config['dbname']
        self.executor.logger = logging.getLogger(__name__)
        self.database = self._query(
            'SELECT CURRENT_DATABASE() AS "DATABASE_NAME"'
        )[0]['DATABASE_NAME']
        self.schema = f'PW_CONVERT_{uuid.uuid4().hex[:12].upper()}'
        self.table_name = 'SOURCE_TABLE'
        self.schema_fqtn = self._qualified_name(self.database, self.schema)
        self.fqtn = self._qualified_name(self.database, self.schema, self.table_name)

        self.executor.fqtn = self.fqtn
        self.addCleanup(self._drop_schema)

        self._query(f'CREATE SCHEMA {self.schema_fqtn}')
        self._create_source_table()

    @staticmethod
    def _quote_identifier(identifier):
        return '"' + identifier.replace('"', '""') + '"'

    @classmethod
    def _qualified_name(cls, *identifiers):
        return '.'.join(cls._quote_identifier(identifier) for identifier in identifiers)

    def _table(self, suffix=''):
        return self._qualified_name(self.database, self.schema, f'{self.table_name}{suffix}')

    def _query(self, sql):
        return self.executor.query(sql)

    def _drop_schema(self):
        self._query(f'DROP SCHEMA IF EXISTS {self.schema_fqtn} CASCADE')

    def _create_source_table(self):
        self._query(
            f'CREATE TABLE {self.fqtn} ('
            '"ID" NUMBER(19,0), '
            '"AMOUNT" NUMBER(38,10), '
            '"CREATED_AT" TIMESTAMP_TZ, '
            '"EVENT_TIME" TIME(9), '
            '"PAYLOAD" VARIANT, '
            '"Display Name" TEXT, '
            'PRIMARY KEY ("ID", "Display Name")'
            ')'
        )
        self._query(
            f'INSERT INTO {self.fqtn} '
            '("ID", "AMOUNT", "CREATED_AT", "EVENT_TIME", "PAYLOAD", "Display Name") '
            'SELECT 7, 1234567890.1234567890, '
            "TO_TIMESTAMP_TZ('2024-01-02 03:04:05.123456 +00:00', "
            "'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'), "
            "TO_TIME('12:34:56.123456789', 'HH24:MI:SS.FF9'), "
            "PARSE_JSON('{\"kind\":\"first\"}'), 'First row' "
            'UNION ALL SELECT 8, -0.0000000001, '
            "TO_TIMESTAMP_TZ('2024-06-01 12:30:45.654321 -07:00', "
            "'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'), "
            "TO_TIME('23:59:59.987654321', 'HH24:MI:SS.FF9'), "
            "NULL, 'Second row'"
        )

    def _all_table_names(self):
        rows = self._query(
            'SELECT "TABLE_NAME" '
            f'FROM {self._quote_identifier(self.database)}."INFORMATION_SCHEMA"."TABLES" '
            f"WHERE \"TABLE_SCHEMA\" = '{self.schema}' ORDER BY \"TABLE_NAME\""
        )
        return {row['TABLE_NAME'] for row in rows}

    def _iceberg_table_names(self):
        rows = self._query(f'SHOW TERSE ICEBERG TABLES IN SCHEMA {self.schema_fqtn}')
        return {row.get('name', row.get('NAME')) for row in rows}

    def _column_types(self, table_name):
        return self._query(
            'SELECT "COLUMN_NAME", "DATA_TYPE", '
            'IFF("DATA_TYPE" = \'NUMBER\', "NUMERIC_PRECISION", NULL) AS "NUMERIC_PRECISION", '
            'IFF("DATA_TYPE" = \'NUMBER\', "NUMERIC_SCALE", NULL) AS "NUMERIC_SCALE" '
            f'FROM {self._quote_identifier(self.database)}."INFORMATION_SCHEMA"."COLUMNS" '
            f"WHERE \"TABLE_SCHEMA\" = '{self.schema}' AND \"TABLE_NAME\" = '{table_name}' "
            'ORDER BY "ORDINAL_POSITION"'
        )

    def _rows(self, table_fqtn, iceberg):
        payload = '"PAYLOAD"' if iceberg else '"PAYLOAD"::VARCHAR'
        rows = self._query(
            'SELECT "ID", "AMOUNT", '
            'DATE_PART(EPOCH_MICROSECOND, "CREATED_AT") AS "CREATED_EPOCH_MICROSECOND", '
            'TO_CHAR(CAST("EVENT_TIME" AS TIME(6)), \'HH24:MI:SS.FF6\') AS "EVENT_TIME", '
            f'{payload} AS "PAYLOAD", "Display Name" AS "DISPLAY_NAME" '
            f'FROM {table_fqtn} ORDER BY "ID"'
        )
        for row in rows:
            row['CREATED_EPOCH_MICROSECOND'] = int(row['CREATED_EPOCH_MICROSECOND'])
        return rows

    def _datetime_precision(self, table_name, column_name):
        rows = self._query(
            'SELECT "DATETIME_PRECISION" '
            f'FROM {self._quote_identifier(self.database)}."INFORMATION_SCHEMA"."COLUMNS" '
            f"WHERE \"TABLE_SCHEMA\" = '{self.schema}' AND \"TABLE_NAME\" = '{table_name}' "
            f"AND \"COLUMN_NAME\" = '{column_name}'"
        )
        return rows[0]['DATETIME_PRECISION']

    def _primary_key(self, table_fqtn):
        return self._query(
            [
                f'SHOW PRIMARY KEYS IN TABLE {table_fqtn}',
                'SELECT "column_name" AS "COLUMN_NAME", "key_sequence" AS "KEY_SEQUENCE" '
                'FROM TABLE(RESULT_SCAN(-1)) ORDER BY "key_sequence"',
            ]
        )

    def test_eventual_native_keeps_native_and_creates_iceberg_companion(self):
        CopyNativeToIceberg(self.config, self.fqtn, eventual='NATIVE')

        self.assertEqual(self._all_table_names(), {'SOURCE_TABLE', 'SOURCE_TABLE_ICEBERG'})
        self.assertEqual(self._iceberg_table_names(), {'SOURCE_TABLE_ICEBERG'})
        self.assertEqual(self._column_types('SOURCE_TABLE'), NATIVE_COLUMN_TYPES)
        self.assertEqual(self._column_types('SOURCE_TABLE_ICEBERG'), ICEBERG_COLUMN_TYPES)
        self.assertEqual(self._datetime_precision('SOURCE_TABLE_ICEBERG', 'CREATED_AT'), 6)
        self.assertEqual(self._datetime_precision('SOURCE_TABLE', 'EVENT_TIME'), 9)
        self.assertEqual(self._datetime_precision('SOURCE_TABLE_ICEBERG', 'EVENT_TIME'), 6)
        self.assertEqual(self._primary_key(self._table()), EXPECTED_PRIMARY_KEY)
        self.assertEqual(self._primary_key(self._table('_ICEBERG')), EXPECTED_PRIMARY_KEY)
        self.assertEqual(self._rows(self._table(), iceberg=False), EXPECTED_ROWS)
        self.assertEqual(self._rows(self._table('_ICEBERG'), iceberg=True), EXPECTED_ROWS)

    def test_eventual_iceberg_keeps_native_backup_and_promotes_loaded_iceberg(self):
        CopyNativeToIceberg(self.config, self.fqtn, eventual='ICEBERG')

        self.assertEqual(self._all_table_names(), {'SOURCE_TABLE', 'SOURCE_TABLE_NATIVE'})
        self.assertEqual(self._iceberg_table_names(), {'SOURCE_TABLE'})
        self.assertEqual(self._column_types('SOURCE_TABLE_NATIVE'), NATIVE_COLUMN_TYPES)
        self.assertEqual(self._column_types('SOURCE_TABLE'), ICEBERG_COLUMN_TYPES)
        self.assertEqual(self._datetime_precision('SOURCE_TABLE', 'CREATED_AT'), 6)
        self.assertEqual(self._datetime_precision('SOURCE_TABLE_NATIVE', 'EVENT_TIME'), 9)
        self.assertEqual(self._datetime_precision('SOURCE_TABLE', 'EVENT_TIME'), 6)
        self.assertEqual(self._primary_key(self._table('_NATIVE')), EXPECTED_PRIMARY_KEY)
        self.assertEqual(self._primary_key(self._table()), EXPECTED_PRIMARY_KEY)
        self.assertEqual(self._rows(self._table('_NATIVE'), iceberg=False), EXPECTED_ROWS)
        self.assertEqual(self._rows(self._table(), iceberg=True), EXPECTED_ROWS)
