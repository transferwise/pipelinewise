from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB

TAP_ID = 'postgres_to_sf'
TARGET_ID = 'snowflake'
TABLE = 'edgydata'
COLUMN = 'cid'
START_VALUE = '3'
END_VALUE = '5'


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
        assertions.assert_partial_sync_table_success(
            self.e2e_env,
            self.tap_id,
            'postgres',
            self.target_id,
            'public',
            TABLE,
            COLUMN,
            START_VALUE,
            END_VALUE
        )

        index_of_column = 0

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        # for 5=> id =>3
        expected_records_for_column = [1, 3, 4, 5]

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'postgres', TABLE, index_of_column, expected_records_for_column
        )
