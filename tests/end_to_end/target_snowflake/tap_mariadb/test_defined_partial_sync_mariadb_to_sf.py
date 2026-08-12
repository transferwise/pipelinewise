from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB

TAP_ID = 'mariadb_to_sf_defined_partial_sync'
TARGET_ID = 'snowflake'


def _mysql_comparison_column(name, normalizer, source_expression=None):
    return {
        'name': name,
        'source_expression': source_expression or f'`{name}`',
        'target_expression': f'"{name.upper()}"',
        'normalizer': normalizer,
    }


def _safe_mysql_datetime(column):
    quoted_column = f'`{column}`'
    return (
        f'CASE WHEN YEAR({quoted_column}) = 0 '
        f'OR MONTH({quoted_column}) NOT BETWEEN 1 AND 12 '
        f'OR DAY({quoted_column}) = 0 '
        f'OR DAY({quoted_column}) > '
        f'DAY(LAST_DAY(DATE_FORMAT({quoted_column}, "%Y-%m-01"))) '
        f'THEN NULL ELSE {quoted_column} END'
    )


WEIGHT_UNIT_COMPARISON_COLUMNS = [
    _mysql_comparison_column('weight_unit_id', 'integer'),
    _mysql_comparison_column('weight_unit_name', 'text'),
    _mysql_comparison_column('isActive', 'boolean'),
    _mysql_comparison_column('original_date_created', 'text'),
    _mysql_comparison_column(
        'date_created',
        'datetime',
        _safe_mysql_datetime('date_created'),
    ),
    _mysql_comparison_column(
        'date_updated',
        'datetime',
        _safe_mysql_datetime('date_updated'),
    ),
]

ADDRESS_COMPARISON_COLUMNS = [
    _mysql_comparison_column('address_id', 'integer'),
    _mysql_comparison_column('isActive', 'boolean'),
    _mysql_comparison_column('street_number', 'text'),
    _mysql_comparison_column('date_created', 'datetime'),
    _mysql_comparison_column('date_updated', 'datetime'),
    _mysql_comparison_column('supplier_supplier_id', 'integer'),
    _mysql_comparison_column('zip_code_zip_code_id', 'integer'),
]


class TestDefinedPartialSyncMariaDBToSF(TapMariaDB):
    """
    Defined Partial Sync from MariaDB to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def _assert_partial_rows_equal(
            self, table, primary_key, start_value, comparison_columns):
        assertions.assert_source_target_rows_equal(
            {
                'env': self.e2e_env,
                'tap_type': self.tap_type,
                'source_db': self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB'),
                'table': table,
                'comparison_columns': comparison_columns,
            },
            primary_key=primary_key,
            where_clause=f'WHERE {primary_key} >= {start_value}',
            operation='defined PartialSync',
        )

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

    def _assert_string_sentinels(self):
        """Pin the fixture rows that distinguish SQL NULL from an empty string."""
        self.assertEqual(
            self.e2e_env.get_rows_from_source(
                tap_type=self.tap_type,
                source_db=self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB'),
                table='address',
                columns=['address_id', 'street_number'],
                primary_key='address_id',
                where_clause='WHERE address_id IN (1001, 1002)',
            ),
            ((1001, ''), (1002, None)),
        )

    # pylint: disable=invalid-name
    def test_defined_partial_sync_mariadb_to_sf(self):
        """
        Testing defined partial syn from Mariadb to Snowflake
        """

        from_value_weight = 5
        from_value_address = 400
        self._assert_string_sentinels()
        # run-tap command
        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ['fastsync', 'partialsync', 'singer'],
            expected_state_streams={
                'fastsync': {'mysql_source_db-customers': True},
                'partialsync': {
                    'mysql_source_db-weight_unit': True,
                    'mysql_source_db-address': True,
                },
            },
        )

        # partial sync

        source_records_weight = self.e2e_env.get_source_records_count(self.tap_type, 'weight_unit')
        expected_records = source_records_weight - from_value_weight + 1
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'weight_unit', expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type,
            'weight_unit', expected_records, f'WHERE weight_unit_id >= {from_value_weight}')
        self._assert_partial_rows_equal(
            'weight_unit',
            'weight_unit_id',
            from_value_weight,
            WEIGHT_UNIT_COMPARISON_COLUMNS,
        )

        # Partial sync
        source_records_address = self.e2e_env.get_source_records_count(self.tap_type, 'address')
        expected_records = source_records_address - from_value_address + 1

        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'address', expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type, 'address', expected_records, f'WHERE address_id >= {from_value_address}')
        self._assert_partial_rows_equal(
            'address',
            'address_id',
            from_value_address,
            ADDRESS_COMPARISON_COLUMNS,
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
                'fastsync': {'mysql_source_db-customers': True},
                'partialsync': {
                    'mysql_source_db-weight_unit': True,
                    'mysql_source_db-address': True,
                },
            },
        )

        expected_records = source_records_weight - from_value_weight + 1
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'weight_unit', expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type,
            'weight_unit', expected_records, f'WHERE weight_unit_id >= {from_value_weight}')
        self._assert_partial_rows_equal(
            'weight_unit',
            'weight_unit_id',
            from_value_weight,
            WEIGHT_UNIT_COMPARISON_COLUMNS,
        )

        # Partial sync
        additional_record_in_target = 1
        total_expected_records = source_records_address + additional_record_in_target - from_value_address + 1
        expected_records_greater_than_from_value = source_records_address - from_value_address + 1
        expected_records_less_than_from_value = 1
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'address', total_expected_records)
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type,
            'address', expected_records_greater_than_from_value, f'WHERE address_id >= {from_value_address}')
        self._assert_partial_rows_equal(
            'address',
            'address_id',
            from_value_address,
            ADDRESS_COMPARISON_COLUMNS,
        )

        # To test if target table is not dropped
        assertions.assert_record_count_in_sf(
            self.e2e_env, self.tap_type,
            'address', expected_records_less_than_from_value, f'WHERE address_id < {from_value_address}')

        # Full fastsync
        expected_records = source_records_customers
        assertions.assert_record_count_in_sf(self.e2e_env, self.tap_type, 'customers', expected_records)
