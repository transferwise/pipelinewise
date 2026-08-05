import unittest

from target_snowflake.convert_table_to_iceberg import CopyNativeToIceberg


class TestCopyNativeToIceberg(unittest.TestCase):
    def setUp(self):
        self.converter = object.__new__(CopyNativeToIceberg)
        self.converter.fqtn = '"DATABASE"."SCHEMA"."TABLE"'

    def test_parse_fqtn_accepts_quoted_and_unquoted_identifiers(self):
        self.assertEqual(
            self.converter.parse_fqtn('"DATABASE"."SCHEMA"."TABLE"'),
            ("DATABASE", "SCHEMA", "TABLE"),
        )
        self.assertEqual(
            self.converter.parse_fqtn("DATABASE.SCHEMA.TABLE"),
            ("DATABASE", "SCHEMA", "TABLE"),
        )

    def test_parse_fqtn_rejects_invalid_identifiers(self):
        for fqtn in (None, "", "TABLE", "SCHEMA.TABLE", "DATABASE.SCHEMA.TABLE.EXTRA"):
            with self.subTest(fqtn=fqtn), self.assertRaises(ValueError):
                self.converter.parse_fqtn(fqtn)

    def test_get_create_iceberg_builds_native_companion_with_primary_key(self):
        self.converter.eventual = "NATIVE"

        statement = self.converter.get_create_iceberg(
            [{"COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER(19,0)"}, {"COLUMN_NAME": "NAME", "DATA_TYPE": "VARCHAR"}],
            [{"COLUMN_NAME": "ID"}],
        )

        self.assertEqual(
            statement,
            "CREATE ICEBERG TABLE DATABASE.SCHEMA.TABLE_ICEBERG ( ID NUMBER(19,0), NAME VARCHAR, PRIMARY KEY (ID)) "
            "DATA_RETENTION_TIME_IN_DAYS=1 TARGET_FILE_SIZE='16MB' ENABLE_DATA_COMPACTION=TRUE",
        )

    def test_get_create_iceberg_builds_replacement_without_primary_key(self):
        self.converter.eventual = "ICEBERG"

        statement = self.converter.get_create_iceberg([{"COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER(19,0)"}], [])

        self.assertEqual(
            statement,
            "CREATE ICEBERG TABLE DATABASE.SCHEMA.TABLE ( ID NUMBER(19,0)) DATA_RETENTION_TIME_IN_DAYS=1 "
            "TARGET_FILE_SIZE='16MB' ENABLE_DATA_COMPACTION=TRUE",
        )

    def test_get_query_copy_to_iceberg_casts_timestamp_for_both_destinations(self):
        columns = [
            {"COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER"},
            {"COLUMN_NAME": "CREATED_AT", "DATA_TYPE": "TIMESTAMP_TZ"},
        ]

        self.converter.eventual = "NATIVE"
        self.assertEqual(
            self.converter.get_query_copy_to_iceberg(columns),
            "INSERT INTO DATABASE.SCHEMA.TABLE_ICEBERG SELECT ID, TO_TIMESTAMP_LTZ(CREATED_AT) AS CREATED_AT "
            "FROM DATABASE.SCHEMA.TABLE",
        )

        self.converter.eventual = "ICEBERG"
        self.assertEqual(
            self.converter.get_query_copy_to_iceberg(columns),
            "INSERT INTO DATABASE.SCHEMA.TABLE SELECT ID, TO_TIMESTAMP_LTZ(CREATED_AT) AS CREATED_AT "
            "FROM DATABASE.SCHEMA.TABLE_NATIVE",
        )
