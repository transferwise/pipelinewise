from pipelinewise.fastsync import mysql_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.helpers.json_fixtures import (
    assert_ticket_20155_json_metadata,
    ticket_20155_json_metadata,
)
from tests.end_to_end.target_snowflake.tap_mariadb import (
    TapMariaDB,
    mariadb_initial_state_expectations,
    mariadb_recurring_state_expectations,
)

TAP_ID = 'mariadb_to_sf'
TARGET_ID = 'snowflake'
FASTSYNC_JSON_ROW_KEY = 'f'
SINGER_JSON_ROW_KEY = 's'


class TestReplicateMariaDBToSF(TapMariaDB):
    """
    Replicate data from MariaDB to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def _assert_tinyint_boolean_mapping(self):
        """Prove width-one TINYINT modifiers map to BOOLEAN, including 0/2/NULL."""
        target_schema = f'PPW_E2E_TAP_MYSQL{self.e2e_env.sf_schema_postfix}'.upper()
        column_types = self.e2e_env.run_query_target_snowflake(
            'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS '
            f"WHERE TABLE_SCHEMA = '{target_schema}' "
            "AND TABLE_NAME = 'ALL_DATATYPES' "
            "AND COLUMN_NAME IN ('C_TINYINT_BOOL', 'C_TINYINT_UNSIGNED_BOOL') "
            'ORDER BY COLUMN_NAME'
        )
        self.assertEqual(
            column_types,
            [
                ('C_TINYINT_BOOL', 'BOOLEAN'),
                ('C_TINYINT_UNSIGNED_BOOL', 'BOOLEAN'),
            ],
        )

        boolean_rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT C_CHAR, C_TINYINT_BOOL, C_TINYINT_UNSIGNED_BOOL FROM '
            f'{target_schema}.ALL_DATATYPES '
            "WHERE C_CHAR IN ('n', 't', 'z') ORDER BY C_CHAR"
        )
        self.assertEqual(
            boolean_rows,
            [
                ('n', None, None),
                ('t', True, True),
                ('z', False, False),
            ],
        )

    def _insert_ticket_json_metadata(self, row_key):
        self.e2e_env.run_query_tap_mysql(
            'INSERT INTO all_datatypes (c_char, c_text) VALUES (%s, %s)',
            (row_key, ticket_20155_json_metadata()),
        )

    def _assert_source_ticket_json_metadata(self, row_key, sync_engine):
        rows = self.e2e_env.run_query_tap_mysql(
            'SELECT c_text FROM all_datatypes WHERE c_char = %s',
            (row_key,),
        )
        assert_ticket_20155_json_metadata(
            rows, f'MariaDB {sync_engine} source row {row_key}'
        )

    def _assert_target_ticket_json_metadata(self, row_key, sync_engine):
        rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT "C_TEXT" FROM ppw_e2e_tap_mysql'
            f'{self.e2e_env.sf_schema_postfix}.all_datatypes '
            f'WHERE "C_CHAR" = \'{row_key}\''
        )
        assert_ticket_20155_json_metadata(
            rows, f'MariaDB {sync_engine} Snowflake row {row_key}'
        )

    def test_replicate_mariadb_to_sf(self):
        """
        Replicate data from MariaDB to Snowflake
        """

        self._insert_ticket_json_metadata(FASTSYNC_JSON_ROW_KEY)
        self._assert_source_ticket_json_metadata(
            FASTSYNC_JSON_ROW_KEY, 'FastSync'
        )
        assertions.assert_resync_tables_success(
            self.tap_id,
            self.target_id,
            tables='mysql_source_db.all_datatypes',
            expected_state_streams={
                'fastsync': {'mysql_source_db-all_datatypes': True}
            },
        )
        self._assert_target_ticket_json_metadata(
            FASTSYNC_JSON_ROW_KEY, 'FastSync'
        )

        self._insert_ticket_json_metadata(SINGER_JSON_ROW_KEY)
        self._assert_source_ticket_json_metadata(
            SINGER_JSON_ROW_KEY, 'Singer'
        )

        # 1. Run tap first time - both fastsync and a singer should be triggered
        initial_state_expectations = mariadb_initial_state_expectations()
        initial_state_expectations['fastsync'].pop(
            'mysql_source_db-all_datatypes'
        )
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            expected_state_streams=initial_state_expectations,
        )
        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_mysql,
            self.e2e_env.run_query_target_snowflake,
            self.e2e_env.sf_schema_postfix,
        )
        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_mysql,
            self.e2e_env.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
        self._assert_tinyint_boolean_mapping()
        self._assert_target_ticket_json_metadata(
            SINGER_JSON_ROW_KEY, 'Singer'
        )

        # Verify UTF-8 special characters (including \u00ef) survive full_sync via CSV
        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT "C_VARCHAR" FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.edgydata'
            f' WHERE "ORDER" = 11'
        )
        self.assertEqual(len(result), 1)
        self.assertIn('\u00ef', result[0][0])

        # 2. Make changes in MariaDB source database
        #  LOG_BASED
        self.e2e_env.run_query_tap_mysql(
            'UPDATE weight_unit SET isactive = 0 WHERE weight_unit_id IN (2, 3, 4)'
        )
        self.e2e_env.run_query_tap_mysql(
            'INSERT INTO edgydata (c_varchar, `group`, `case`, cjson, c_time) VALUES'
            "('Lorem ipsum dolor sit amet', 10, 'A', '[]', '00:00:00'),"
            "('Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช', 20, 'A', '{}', '12:00:59'),"
            "('Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年.', null,'B', "
            '\'[{"key": "ValueOne", "actions": []}, {"key": "ValueTwo", "actions": []}]\','
            " '9:1:00'),"
            "('Special Characters: [\"\\,"
            "!@£$%^&*()]\\\\', null, 'B', "
            "null, '12:00:00'),"
            "('	', 20, 'B', null, '15:36:10'),"
            "(CONCAT(CHAR(0x0000 using utf16), '<- null char'), 20, 'B', null, '15:36:10')"
        )

        self.e2e_env.run_query_tap_mysql('UPDATE all_datatypes SET c_point = NULL')
        self.e2e_env.delete_record_from_source('mysql', 'weight_unit', 'WHERE weight_unit_id=25')

        #  INCREMENTAL
        self.e2e_env.run_query_tap_mysql(
            'INSERT INTO address(isactive, street_number, date_created, date_updated,'
            ' supplier_supplier_id, zip_code_zip_code_id)'
            'VALUES (1, 1234, NOW(), NOW(), 0, 1234)'
        )
        self.e2e_env.run_query_tap_mysql(
            'UPDATE address SET street_number = 9999, date_updated = NOW()'
            ' WHERE address_id = 1'
        )
        #  FULL_TABLE
        self.e2e_env.run_query_tap_mysql('DELETE FROM no_pk_table WHERE id > 10')

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'singer'],
            expected_state_streams=mariadb_recurring_state_expectations(),
        )
        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_mysql,
            self.e2e_env.run_query_target_snowflake,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_mysql,
            self.e2e_env.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            {'blob_col'},
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT COUNT(*) FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.weight_unit '
            f'WHERE "WEIGHT_UNIT_ID" = 25'
        )
        self.assertEqual(result[0][0], 0)

        # Checking if mask-date transformation is working
        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT count(1) FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.address '
            f'where MONTH(date_created) != 1 or DAY(date_created)::int != 1;'
        )[0][0]

        self.assertEqual(result, 0)

        # Checking if conditional MASK-NUMBER transformation is working
        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT count(1) FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.address '
            f"where zip_code_zip_code_id != 0 and street_number REGEXP '[801]';"
        )[0][0]

        self.assertEqual(result, 0)

        # Checking if conditional SET-NULL transformation is working
        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT count(1) FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.edgydata '
            f'where "GROUP" is not null and "CASE" = \'B\';'
        )[0][0]

        self.assertEqual(result, 0)
