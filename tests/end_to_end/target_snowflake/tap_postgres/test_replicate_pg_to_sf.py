from datetime import datetime

from pipelinewise.fastsync import postgres_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres

TAP_ID = 'postgres_to_sf'
TARGET_ID = 'snowflake'


class TestReplicatePGToSF(TapPostgres):
    """
    Resync tables from Postgres to Snowflake using splitting large files option.
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def test_replicate_pg_to_sf(self):
        """
        Resync tables from Postgres to Snowflake using splitting large files option.
        """

        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ['fastsync', 'singer']
        )

        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_postgres,
            self.e2e_env.run_query_target_snowflake,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_postgres,
            self.e2e_env.run_query_target_snowflake,
            postgres_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        assertions.assert_date_column_naive_in_target(
            self.e2e_env.run_query_target_snowflake,
            'updated_at',
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"',
        )

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f"where cvarchar='H';"
        )[0][0]

        self.assertEqual(result, datetime(9999, 12, 31, 23, 59, 59, 998993)) # if bump snowflake-connector -> 999000

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f"where cvarchar='I';"
        )[0][0]

        self.assertEqual(result, datetime(9999, 12, 31, 23, 59, 59, 998993))  # if bump snowflake-connector -> 999000

        # 2. Make changes in PG source database
        #  LOG_BASED
        self.e2e_env.run_query_tap_postgres(
            'insert into public."table_with_space and UPPERCase" (cvarchar, updated_at) values '
            "('X', '2020-01-01 08:53:56.8+10'),"
            "('Y', '2020-12-31 12:59:00.148+00'),"
            "('faaaar future', '15000-05-23 12:40:00.148'),"
            "('BC', '2020-01-23 01:40:00 BC'),"
            "('Z', null),"
            "('W', '2020-03-03 12:30:00');"
        )

        #  INCREMENTAL
        last_id = self.e2e_env.run_query_tap_postgres(
            'SELECT max(id) from public.city'
        )[0][0]
        self.e2e_env.run_query_tap_postgres(
            'INSERT INTO public.city (id, name, countrycode, district, population) '
            f"VALUES ({last_id+1}, 'Bath', 'GBR', 'England', 88859)"
        )

        self.e2e_env.run_query_tap_postgres(
            'UPDATE public.edgydata SET '
            "cjson = json '{\"data\": 1234}', "
            "cjsonb = jsonb '{\"data\": 2345}', "
            "cvarchar = 'Liewe Maatjies UPDATED' WHERE cid = 23"
        )

        #  FULL_TABLE
        self.e2e_env.run_query_tap_postgres(
            "DELETE FROM public.country WHERE code = 'UMI'"
        )

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ['fastsync', 'singer'], profiling=True
        )

        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_postgres,
            self.e2e_env.run_query_target_snowflake,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_postgres,
            self.e2e_env.run_query_target_snowflake,
            postgres_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        assertions.assert_date_column_naive_in_target(
            self.e2e_env.run_query_target_snowflake,
            'updated_at',
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"',
        )

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE"'
            f" where cvarchar='X';"
        )[0][0]

        self.assertEqual(result, datetime(2019, 12, 31, 22, 53, 56, 800000))

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f"where cvarchar='faaaar future';"
        )[0][0]

        self.assertEqual(result, datetime(9999, 12, 31, 23, 59, 59, 999000))

        result = self.e2e_env.run_query_target_snowflake(
            f'SELECT updated_at FROM '
            f'ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."TABLE_WITH_SPACE AND UPPERCASE" '
            f"where cvarchar='BC';"
        )[0][0]

        self.assertEqual(result, datetime(9999, 12, 31, 23, 59, 59, 999000))
