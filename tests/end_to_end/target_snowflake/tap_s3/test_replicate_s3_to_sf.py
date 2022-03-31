from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_s3 import TapS3

TAP_ID = 's3_csv_to_sf'
TARGET_ID = 'snowflake'


class TestReplicateS3ToSF(TapS3):
    """
    Test replicate S3 to SF
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def assert_columns_exist(self):
        """
        Check if every table and column exists in target snowflake
        """

        assertions.assert_cols_in_table(
            self.e2e_env.run_query_target_snowflake,
            'ppw_e2e_tap_s3_csv',
            'countries',
            ['CITY', 'COUNTRY', 'CURRENCY', 'ID', 'LANGUAGE'],
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
        assertions.assert_cols_in_table(
            self.e2e_env.run_query_target_snowflake,
            'ppw_e2e_tap_s3_csv',
            'people',
            [
                'BIRTH_DATE',
                'EMAIL',
                'FIRST_NAME',
                'GENDER',
                'GROUP',
                'ID',
                'IP_ADDRESS',
                'IS_PENSIONEER',
                'LAST_NAME',
            ],
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

    def test_replicate_s3_to_sf(self):
        """
        Replicate csv files from s3 to Snowflake, check if return code is zero and success log file created
        """

        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ['fastsync', 'singer']
        )
        self.assert_columns_exist()

        # 2. Run tap second time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ['fastsync', 'singer']
        )
        self.assert_columns_exist()
