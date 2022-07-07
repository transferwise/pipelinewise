from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB

TAP_ID = 'mariadb_to_sf'
TARGET_ID = 'snowflake'
TABLE = 'weight_unit'
COLUMN = 'weight_unit_id'
START_VALUE = '5'
END_VALUE = '7'


class TestPartialSyncMariaDBToSF(TapMariaDB):
    """
    Test cases for Partial sync table from MariaDB to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def test_partial_sync_mariadb_to_sf(self):
        """
        Test partial sync table from MariaDB to Snowflake
        """
        source_db = self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB')
        assertions.assert_partial_sync_table_success(
            self.e2e_env,
            self.tap_id,
            'mysql',
            self.target_id,
            source_db,
            TABLE,
            COLUMN,
            START_VALUE,
            END_VALUE
        )

        index_of_column = 0

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        # for 7=> id =>5
        expected_records_for_column = [1, 5, 6, 7]

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', TABLE, index_of_column, expected_records_for_column
        )
