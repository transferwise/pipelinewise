from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB

TAP_ID = 'mariadb_to_sf_defined_partial_sync'
TARGET_ID = 'snowflake'


class TestDefinedPartialSyncMariaDBToSF(TapMariaDB):
    """
    Defined Partial Sync from MariaDB to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def _manipulate_target_tables(self):
        self.e2e_env.run_query_target_snowflake(
            f'INSERT INTO ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.address '
            '(address_id, street_number, supplier_supplier_id, zip_code_zip_code_id) VALUES (1, 1, 1, 1)')

        self.e2e_env.run_query_target_snowflake(
            f'DELETE FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.address '
            'WHERE address_id=500')
        self.e2e_env.run_query_target_snowflake(
            f'INSERT INTO ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.weight_unit '
            "(weight_unit_id, weight_unit_name) VALUES (1, 'foo')")

        self.e2e_env.run_query_target_snowflake(
            f'DELETE FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.weight_unit '
            'WHERE weight_unit_id=25')

        self.e2e_env.run_query_target_snowflake(
            f'DELETE FROM ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.customers '
            'WHERE id=15')

    # pylint: disable=invalid-name
    def test_defined_partial_sync_mariadb_to_sf(self):
        """
        Testing defined partial syn from Mariadb to Snowflake
        """

        from_value_weight = 5
        from_value_address = 400
        # run-tap command
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ['fastsync', 'singer']

        )

        # partial sync

        source_records_weight = self.e2e_env.get_source_records_count(self.tap_type, 'weight_unit')
        expected_records = source_records_weight - from_value_weight + 1
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'weight_unit', expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type,
            'weight_unit', expected_records, f'WHERE weight_unit_id >= {from_value_weight}')

        # Partial sync
        source_records_address = self.e2e_env.get_source_records_count(self.tap_type, 'address')
        expected_records = source_records_address - from_value_address + 1

        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'address', expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type, 'address', expected_records, f'WHERE address_id >= {from_value_address}')

        # Full fastsync
        source_records_customers = self.e2e_env.get_source_records_count(self.tap_type, 'customers')
        expected_records = source_records_customers
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'customers', expected_records)

        self._manipulate_target_tables()

        # sync-tables command
        assertions.assert_resync_tables_success(self.tap_id, self.target_id)

        expected_records = source_records_weight - from_value_weight + 1
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'weight_unit', expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type,
            'weight_unit', expected_records, f'WHERE weight_unit_id >= {from_value_weight}')

        # Partial sync
        additional_record_in_target = 1
        total_expected_records = source_records_address + additional_record_in_target - from_value_address + 1
        expected_records_greater_than_from_value = source_records_address - from_value_address + 1
        expected_records_less_than_from_value = 1
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'address', total_expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type,
            'address', expected_records_greater_than_from_value, f'WHERE address_id >= {from_value_address}')

        # To test if target table is not dropped
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type,
            'address', expected_records_less_than_from_value, f'WHERE address_id < {from_value_address}')

        # Full fastsync
        expected_records = source_records_customers
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'customers', expected_records)
