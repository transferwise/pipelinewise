import json

from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB


TAP_ID = 'mariadb_to_sf_iceberg'
TARGET_ID = 'snowflake'
LARGE_VARCHAR_LENGTH = 16_777_217


class TestIcebergV3MariaDBToSnowflake(TapMariaDB):
    """Exercise MariaDB Singer, FullSync, and PartialSync into Iceberg."""

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.source_db = self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB')
        self.target_schema = (
            f'PPW_E2E_TAP_MYSQL{self.e2e_env.sf_schema_postfix}'
        ).upper()
        self.initial_s3_keys = self.iceberg_fastsync_s3_keys()

    def prepare_source(self):
        """Add large text columns before catalog discovery."""
        super().prepare_source()
        self.e2e_env.run_query_tap_mysql(
            'ALTER TABLE edgydata ADD COLUMN large_text LONGTEXT'
        )
        self.e2e_env.run_query_tap_mysql(
            'ALTER TABLE address ADD COLUMN large_text LONGTEXT'
        )

    def _assert_managed_v3(self, table_name):
        format_rows = self.e2e_env.run_query_target_snowflake(
            'SELECT IS_ICEBERG FROM INFORMATION_SCHEMA.TABLES '
            f"WHERE TABLE_SCHEMA = '{self.target_schema}' "
            f"AND TABLE_NAME = '{table_name.upper()}'"
        )
        self.assertEqual(format_rows, [('YES',)])
        version_rows = self.e2e_env.run_query_target_snowflake(
            "SHOW PARAMETERS LIKE 'ICEBERG_VERSION' IN TABLE "
            f'"{self.target_schema}"."{table_name.upper()}"'
        )
        self.assertEqual(str(version_rows[0][1]), '3')
        merge_on_read_rows = self.e2e_env.run_query_target_snowflake(
            "SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' IN TABLE "
            f'"{self.target_schema}"."{table_name.upper()}"'
        )
        self.assertEqual(len(merge_on_read_rows), 1)
        self.assertEqual(str(merge_on_read_rows[0][1]).upper(), 'DISABLED')
        self.assertEqual(str(merge_on_read_rows[0][3]).upper(), 'TABLE')

    def _edgy_rows(self):
        return self.e2e_env.run_query_target_snowflake(
            'SELECT "ORDER", TO_JSON("CJSON"), "C_VARCHAR" '
            f'FROM "{self.target_schema}"."EDGYDATA" ORDER BY "ORDER"'
        )

    def _assert_large_text(self, table_name, key_column, key_value):
        rows = self.e2e_env.run_query_target_snowflake(
            'SELECT LENGTH("LARGE_TEXT") '
            f'FROM "{self.target_schema}"."{table_name.upper()}" '
            f'WHERE "{key_column.upper()}" = {key_value}'
        )
        self.assertEqual(rows, [(LARGE_VARCHAR_LENGTH,)])

    def _assert_large_text_column_width(self, table_name):
        rows = self.e2e_env.run_query_target_snowflake(
            'SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS '
            f"WHERE TABLE_SCHEMA = '{self.target_schema}' "
            f"AND TABLE_NAME = '{table_name.upper()}' "
            "AND COLUMN_NAME = 'LARGE_TEXT'"
        )
        self.assertEqual(rows, [(134217728,)])

    def test_fullsync_hands_over_to_singer_on_managed_iceberg_v3(self):
        """Initial FastSync and recurring engines preserve MariaDB values."""
        large_json = json.dumps({'large': 'x' * 70000, 'nested': [None, {}]})
        self.e2e_env.run_query_tap_mysql(
            'INSERT INTO edgydata (`order`, c_varchar, `group`, `case`, cjson) '
            'VALUES (%s, %s, %s, %s, %s)',
            (1001, 'fastsync-json', 1, 'A', large_json),
        )
        self.e2e_env.run_query_tap_mysql(
            'UPDATE edgydata SET large_text = REPEAT(%s, %s) WHERE `order` = 1001',
            ('m', LARGE_VARCHAR_LENGTH),
        )

        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            expected_state_streams={
                'fastsync': {
                    f'{self.source_db}-edgydata': True,
                    f'{self.source_db}-address': True,
                    f'{self.source_db}-no_pk_table': False,
                }
            },
        )

        for table_name in ('edgydata', 'address', 'no_pk_table'):
            self._assert_managed_v3(table_name)
        column_type_rows = self.e2e_env.run_query_target_snowflake(
            'SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS '
            f"WHERE TABLE_SCHEMA = '{self.target_schema}' "
            "AND TABLE_NAME = 'EDGYDATA' AND COLUMN_NAME = 'CJSON'"
        )
        self.assertEqual(column_type_rows, [('VARIANT',)])
        rows = self._edgy_rows()
        self.assertEqual([row[0] for row in rows], list(range(1, 12)) + [1001])
        self.assertEqual(json.loads(rows[-1][1]), json.loads(large_json))
        self.assertEqual(rows[-1][2], 'fastsync-json')
        self._assert_large_text('edgydata', 'order', 1001)
        self._assert_large_text_column_width('edgydata')
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )

        self.e2e_env.run_query_tap_mysql(
            'INSERT INTO edgydata (`order`, c_varchar, `group`, `case`, cjson) '
            'VALUES (%s, %s, %s, %s, %s)',
            (1002, 'singer-json', 2, 'B', json.dumps({'singer': [None, {}]})),
        )
        self.e2e_env.run_query_tap_mysql(
            'DELETE FROM no_pk_table WHERE id > 10'
        )
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            expected_state_streams={
                'fastsync': {f'{self.source_db}-no_pk_table': False}
            },
        )

        rows = self._edgy_rows()
        self.assertEqual([row[0] for row in rows], list(range(1, 12)) + [1001, 1002])
        self.assertEqual(json.loads(rows[-1][1]), {'singer': [None, {}]})
        full_rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT "ID" FROM "{self.target_schema}"."NO_PK_TABLE" ORDER BY "ID"'
        )
        self.assertEqual([row[0] for row in full_rows], list(range(1, 11)))
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )

    def test_partial_sync_merges_a_bounded_range_into_managed_iceberg_v3(self):
        """PartialSync changes only the requested MariaDB key range."""
        assertions.assert_resync_tables_success(
            self.tap_id,
            self.target_id,
            tables=f'{self.source_db}.address',
            expected_state_streams={
                'fastsync': {f'{self.source_db}-address': True}
            },
        )
        self._assert_managed_v3('address')
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )
        sentinel_before = self.e2e_env.run_query_target_snowflake(
            'SELECT "STREET_NUMBER" '
            f'FROM "{self.target_schema}"."ADDRESS" WHERE "ADDRESS_ID" = 1'
        )
        self.assertEqual(len(sentinel_before), 1)

        self.e2e_env.run_query_tap_mysql(
            "UPDATE address SET street_number = '99999' WHERE address_id = 1"
        )
        self.e2e_env.run_query_tap_mysql(
            'UPDATE address SET street_number = 4321, '
            'large_text = REPEAT(%s, %s) WHERE address_id = 2',
            ('u', LARGE_VARCHAR_LENGTH),
        )
        self.e2e_env.run_query_tap_mysql(
            'INSERT INTO address '
            '(isactive, street_number, date_created, date_updated, '
            'supplier_supplier_id, zip_code_zip_code_id, large_text) '
            'VALUES (1, 9876, NOW(), NOW(), 0, 1234, REPEAT(%s, %s))',
            ('i', LARGE_VARCHAR_LENGTH),
        )
        inserted_id = self.e2e_env.run_query_tap_mysql(
            'SELECT MAX(address_id) FROM address'
        )[0][0]
        assertions.assert_partial_sync_table_success(
            {
                'env': self.e2e_env,
                'tap': self.tap_id,
                'tap_type': 'mysql',
                'target': self.target_id,
                'source_db': self.source_db,
                'table': 'address',
                'column': 'address_id',
            },
            start_value=2,
            end_value=inserted_id,
        )

        rows = self.e2e_env.run_query_target_snowflake(
            'SELECT "ADDRESS_ID", "STREET_NUMBER" '
            f'FROM "{self.target_schema}"."ADDRESS" '
            f'WHERE "ADDRESS_ID" IN (1, 2, {inserted_id}) ORDER BY "ADDRESS_ID"'
        )
        self.assertEqual(
            rows,
            [
                (1, sentinel_before[0][0]),
                (2, '4321'),
                (inserted_id, '9876'),
            ],
        )
        self._assert_large_text('address', 'address_id', 2)
        self._assert_large_text('address', 'address_id', inserted_id)
        self._assert_large_text_column_width('address')
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )
