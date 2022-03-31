from pipelinewise.fastsync import mysql_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB

TAP_ID = "mariadb_to_sf"
TARGET_ID = "snowflake"


class TestReplicateMariaDBToSF(TapMariaDB):
    """
    Replicate data from MariaDB to Snowflake
    """

    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def tearDown(self):
        super().tearDown()

    def test_replicate_mariadb_to_sf(self):
        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ["fastsync", "singer"]
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

        # 2. Make changes in MariaDB source database
        #  LOG_BASED
        self.e2e_env.run_query_tap_mysql(
            "UPDATE weight_unit SET isactive = 0 WHERE weight_unit_id IN (2, 3, 4)"
        )
        self.e2e_env.run_query_tap_mysql(
            "INSERT INTO edgydata (c_varchar, `group`, `case`, cjson, c_time) VALUES"
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

        self.e2e_env.run_query_tap_mysql("UPDATE all_datatypes SET c_point = NULL")

        #  INCREMENTAL
        self.e2e_env.run_query_tap_mysql(
            "INSERT INTO address(isactive, street_number, date_created, date_updated,"
            " supplier_supplier_id, zip_code_zip_code_id)"
            "VALUES (1, 1234, NOW(), NOW(), 0, 1234)"
        )
        self.e2e_env.run_query_tap_mysql(
            "UPDATE address SET street_number = 9999, date_updated = NOW()"
            " WHERE address_id = 1"
        )
        #  FULL_TABLE
        self.e2e_env.run_query_tap_mysql("DELETE FROM no_pk_table WHERE id > 10")

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ["fastsync", "singer"]
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
            {"blob_col"},
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        # Checking if mask-date transformation is working
        result = self.e2e_env.run_query_target_snowflake(
            f"SELECT count(1) FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.address "
            f"where MONTH(date_created) != 1 or DAY(date_created)::int != 1;"
        )[0][0]

        self.assertEqual(result, 0)

        # Checking if conditional MASK-NUMBER transformation is working
        result = self.e2e_env.run_query_target_snowflake(
            f"SELECT count(1) FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.address "
            f"where zip_code_zip_code_id != 0 and street_number REGEXP '[801]';"
        )[0][0]

        self.assertEqual(result, 0)

        # Checking if conditional SET-NULL transformation is working
        result = self.e2e_env.run_query_target_snowflake(
            f"SELECT count(1) FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.edgydata "
            f'where "GROUP" is not null and "CASE" = \'B\';'
        )[0][0]

        self.assertEqual(result, 0)
