from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB


class TestReplicateMariaDBToSFSoftDelete(TapMariaDB):
    """Replicate MariaDB deletions to Snowflake as soft deletes."""

    def setUp(self):
        super().setUp(tap_id="mariadb_to_sf_soft_delete", target_id="snowflake")

    def test_log_based_delete_preserves_deleted_timestamp(self):
        """Preserve valid deletion metadata while nulling an invalid source date."""
        assertions.assert_run_tap_success(self.tap_id, self.target_id, ["fastsync", "singer"])

        self.e2e_env.delete_record_from_source("mysql", "weight_unit", "WHERE weight_unit_id=25")

        assertions.assert_run_tap_success(self.tap_id, self.target_id, ["singer"])

        records = self.e2e_env.run_query_target_snowflake(
            f'SELECT "_SDC_DELETED_AT", "DATE_CREATED" '
            f"FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.weight_unit "
            f'WHERE "WEIGHT_UNIT_ID" = 25'
        )
        self.assertEqual(len(records), 1)
        self.assertIsNotNone(records[0][0])
        self.assertIsNone(records[0][1])
