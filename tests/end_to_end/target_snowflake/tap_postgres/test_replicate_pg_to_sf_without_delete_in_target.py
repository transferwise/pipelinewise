from datetime import datetime
import dateutil.parser

from pipelinewise.fastsync import postgres_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres

TAP_ID = 'postgres_to_sf_without_delete_in_target'
TARGET_ID = 'snowflake'


class TestReplicatePGToSFWithoutDeleteInTarget(TapPostgres):
    """
    Resync tables from Postgres to Snowflake without replicating deletes
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def test_replicate_pg_to_sf_without_delete_in_target(self):
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ['fastsync', 'singer']
        )

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT _SDC_DELETED_AT FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"'
            f" where cvarchar='A';"
        )[0][0]

        self.assertIsNone(result)

        # Delete row in source
        self.e2e_env.run_query_tap_postgres(
            'DELETE FROM public."table_with_space and UPPERCase" WHERE cvarchar = \'A\';'
        )

        # Run tap second time
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ['singer'], profiling=True
        )

        deleted_row = self.e2e_env.run_query_target_snowflake(
            f'SELECT _SDC_DELETED_AT FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"'
            f" where cvarchar='A';"
        )[0]

        # Validate that the entire row data is still in the target
        for column in deleted_row:
            self.assertIsNotNone(column)

        deleted_at = self.e2e_env.run_query_target_snowflake(
            f'SELECT _SDC_DELETED_AT FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"'
            f" where cvarchar='A';"
        )[0][0]

        # Validate that _sdc_deleted_at column exists and has been set
        self.assertIsNotNone(deleted_at)
