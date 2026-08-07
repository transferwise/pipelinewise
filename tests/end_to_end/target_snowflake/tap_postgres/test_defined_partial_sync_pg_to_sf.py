from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres

TAP_ID = 'pg_to_sf_defined_partial_sync'
TARGET_ID = 'snowflake'


def _postgres_comparison_column(name, normalizer):
    return {
        'name': name,
        'source_expression': f'"{name}"',
        'target_expression': f'"{name.upper()}"',
        'normalizer': normalizer,
    }


CITY_COMPARISON_COLUMNS = [
    _postgres_comparison_column('id', 'integer'),
    _postgres_comparison_column('name', 'text'),
    _postgres_comparison_column('countrycode', 'text'),
    _postgres_comparison_column('district', 'text'),
    _postgres_comparison_column('population', 'integer'),
]

ORDER_COMPARISON_COLUMNS = [
    _postgres_comparison_column('id', 'integer'),
    _postgres_comparison_column('cvarchar', 'text'),
    _postgres_comparison_column('created_at', 'datetime'),
]


class TestDefinedPartialSyncPGToSF(TapPostgres):
    """
    Defined Partial Sync from Postgres to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def _assert_partial_rows_equal(
            self, table, primary_key, start_value, comparison_columns,
            source_table=None, target_table=None):
        assertions.assert_source_target_rows_equal(
            {
                'env': self.e2e_env,
                'tap_type': self.tap_type,
                'source_db': 'public',
                'table': table,
                'source_table': source_table or table,
                'target_table': target_table or table,
                'comparison_columns': comparison_columns,
            },
            primary_key=primary_key,
            where_clause=f'WHERE {primary_key} >= {start_value}',
            operation='defined PartialSync',
        )

    def _manipulate_target_tables(self):
        self.e2e_env.run_query_target_snowflake(
            f'INSERT INTO ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."ORDER" '
            "(id, CVARCHAR) VALUES (1, 'A')")

        self.e2e_env.run_query_target_snowflake(
            f'DELETE FROM ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}."ORDER" '
            'WHERE id=6')
        self.e2e_env.run_query_target_snowflake(
            f'INSERT INTO ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}.CITY '
            "(id, name) VALUES (1, 'foo')")

        self.e2e_env.run_query_target_snowflake(
            f'DELETE FROM ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}.CITY '
            'WHERE id=500')

        self.e2e_env.run_query_target_snowflake(
            f'DELETE FROM ppw_e2e_tap_postgres{self.e2e_env.sf_schema_postfix}.customers '
            'WHERE id=15')

    # pylint: disable=invalid-name
    def test_defined_partial_sync_pg_to_sf(self):
        """
        Testing defined partial syn from Postgres to Snowflake
        """

        from_value_city = 500
        from_value_order = 5
        # run-tap command
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'partialsync', 'singer'],
            expected_state_streams={
                'fastsync': {'public-customers': True},
                'partialsync': {
                    'public-city': True,
                    'public-order': True,
                },
            },
        )

        # partial sync

        source_records_city = self.e2e_env.get_source_records_count(self.tap_type, 'city')
        expected_records = source_records_city - from_value_city + 1
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'city', expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type, 'city', expected_records, f'WHERE id >= {from_value_city}')
        self._assert_partial_rows_equal(
            'city',
            'id',
            from_value_city,
            CITY_COMPARISON_COLUMNS,
        )

        # Partial sync
        source_records_order = self.e2e_env.get_source_records_count(self.tap_type, '"order"')
        expected_records = source_records_order - from_value_order + 1

        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'order', expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type, 'order', expected_records, f'WHERE id >= {from_value_order}')
        self._assert_partial_rows_equal(
            'order',
            'id',
            from_value_order,
            ORDER_COMPARISON_COLUMNS,
            source_table='"order"',
            target_table='"ORDER"',
        )

        # Full fastsync
        source_records_customers = self.e2e_env.get_source_records_count(self.tap_type, 'customers')
        expected_records = source_records_customers
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'customers', expected_records)

        self._manipulate_target_tables()

        # sync-tables command
        assertions.assert_resync_tables_success(
            self.tap_id,
            self.target_id,
            sync_engines=('fastsync', 'partialsync'),
            expected_state_streams={
                'fastsync': {'public-customers': True},
                'partialsync': {
                    'public-city': True,
                    'public-order': True,
                },
            },
        )

        expected_records = source_records_order - from_value_order + 1
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'order', expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type, 'order', expected_records, f'WHERE id >= {from_value_order}')
        self._assert_partial_rows_equal(
            'order',
            'id',
            from_value_order,
            ORDER_COMPARISON_COLUMNS,
            source_table='"order"',
            target_table='"ORDER"',
        )

        # Partial sync
        additional_record_in_target = 1
        total_expected_records = source_records_city + additional_record_in_target - from_value_city + 1
        expected_records_greater_than_from_value = source_records_city - from_value_city + 1
        expected_records_less_than_from_value = 1
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'city', total_expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type,
            'city', expected_records_greater_than_from_value, f'WHERE id >= {from_value_city}')
        self._assert_partial_rows_equal(
            'city',
            'id',
            from_value_city,
            CITY_COMPARISON_COLUMNS,
        )

        # To test if target table is not dropped
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type, 'city', expected_records_less_than_from_value, f'WHERE id < {from_value_city}')

        # Full fastsync
        expected_records = source_records_customers
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'customers', expected_records)
