# E2E runbook

Read [AGENTS.md](AGENTS.md) first. Run local E2E only when explicitly requested;
otherwise prefer PR CI. These recipes are not permission to start a suite.
The authoritative shard selection is `.github/workflows/e2e_tests.yml`; keep
these recipes aligned when the matrix changes.

## Shard recipes

These groups mirror `.github/workflows/e2e_tests.yml` in required-check order
`e2e_tests_01` through `e2e_tests_23`; update both together. CI runs twenty-two
Snowflake groups concurrently on isolated runners; local groups share/reset
fixtures/config and must run serially:

```bash
run_e2e() { docker exec -t pipelinewise pytest "$@" -vx --timer-top-n 10; }
run_sf_decimal() { docker exec -t pipelinewise bash scripts/test_decimal_connectors.sh snowflake "$@"; }

run_e2e \
  tests/end_to_end/test_target_postgres.py \
  tests/end_to_end/test_numeric_replication.py::test_decimal_fastsync_postgres \
  tests/end_to_end/test_numeric_replication.py::test_postgres_decimal_float_underflow \
  tests/end_to_end/test_postgres_stream_buffer_recovery.py \
  tests/end_to_end/data_diff/test_postgres_to_postgres.py \
  tests/end_to_end/data_diff/test_mysql_to_postgres.py \
  tests/end_to_end/data_diff/test_yugabyte_to_postgres.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_postgres/test_auto_native_to_iceberg_pg_to_sf.py \
  tests/end_to_end/data_diff/test_mysql_to_snowflake.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_replica_to_sf.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_postgres/test_snowflake_iceberg_publisher.py \
  'tests/end_to_end/test_numeric_replication.py::test_mysql_float_exact_snowflake[mariadb-v3]'

run_sf_decimal \
  tests/integration/test_decimals.py::test_numeric_array_json_text_loads_like_parse_json_without_versioning \
  tests/integration/test_decimals.py::test_native_nan_float_key_loaded_twice_keeps_one_row

run_e2e \
  tests/end_to_end/target_snowflake/tap_postgres/test_partial_sync_pg_to_sf.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_multiline_native_pg_to_sf.py \
  tests/end_to_end/data_diff/test_postgres_to_snowflake.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_partial_sync_mariadb_to_sf.py::TestPartialSyncMariaDBToSF::test_partial_sync_removes_existing_row_deleted_from_source

run_e2e \
  tests/end_to_end/target_snowflake/tap_mariadb/test_partial_sync_mariadb_to_sf.py::TestPartialSyncMariaDBToSF::test_partial_sync_if_table_does_not_exist_in_target \
  tests/end_to_end/target_snowflake/tap_mariadb/test_partial_sync_mariadb_to_sf.py::TestPartialSyncMariaDBToSF::test_terminated_partial_sync_kills_blocked_source_query \
  tests/end_to_end/target_snowflake/tap_mariadb/test_partial_sync_mariadb_to_sf.py::TestPartialSyncMariaDBToSF::test_partial_sync_if_there_is_additional_column_in_source \
  tests/end_to_end/target_snowflake/tap_mariadb/test_partial_sync_mariadb_to_sf.py::TestPartialSyncMariaDBToSF::test_partial_sync_if_there_is_additional_column_in_target \
  tests/end_to_end/target_snowflake/tap_mariadb/test_partial_sync_mariadb_to_sf.py::TestPartialSyncMariaDBToSF::test_partial_sync_mariadb_to_sf \
  tests/end_to_end/target_snowflake/tap_mariadb/test_partial_sync_mariadb_to_sf.py::TestPartialSyncMariaDBToSF::test_unbounded_empty_range_removes_target_only_row \
  tests/end_to_end/target_snowflake/tap_yugabyte/test_replicate_yugabyte_to_sf.py

run_e2e \
  tests/end_to_end/target_snowflake/tap_postgres/test_iceberg_v3_postgres_to_sf.py::TestIcebergV3PostgresToSnowflake::test_full_and_bounded_partial_sync_preserve_multiline_bytes \
  tests/end_to_end/target_snowflake/tap_postgres/test_iceberg_v3_postgres_to_sf.py::TestIcebergV3PostgresToSnowflake::test_fullsync_hands_over_to_singer_on_managed_iceberg_v3 \
  tests/end_to_end/target_snowflake/tap_mysql/test_iceberg_v3_mysql_to_sf.py::TestIcebergV3MySQLToSnowflake::test_fullsync_hands_over_to_singer_on_managed_iceberg_v3 \
  'tests/end_to_end/test_numeric_replication.py::test_postgres_float_nonfinite_snowflake[postgres-v3]'

run_e2e \
  tests/end_to_end/target_snowflake/tap_mariadb/test_iceberg_v3_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_mongodb/test_replicate_mongodb_to_sf.py \
  'tests/end_to_end/test_numeric_replication.py::test_mysql_float_exact_snowflake[mysql-v3]' \
  'tests/end_to_end/test_numeric_replication.py::test_mysql_float_exact_snowflake[mariadb-native]'

run_e2e \
  tests/end_to_end/target_snowflake/test_source_transformation_exports.py \
  tests/end_to_end/target_snowflake/tap_mysql/test_iceberg_v3_mysql_to_sf.py::TestIcebergV3MySQLToSnowflake::test_full_and_bounded_partial_sync_preserve_multiline_bytes \
  tests/end_to_end/target_snowflake/tap_mysql/test_iceberg_v3_mysql_to_sf.py::TestIcebergV3MySQLToSnowflake::test_partial_sync_merges_a_bounded_range_into_managed_iceberg_v3 \
  tests/end_to_end/target_snowflake/tap_mysql/test_multiline_native_mysql_to_sf.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_defined_partial_sync_pg_to_sf.py \
  tests/end_to_end/target_snowflake/tap_s3/test_replicate_s3_to_sf.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_resync_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_replicate_pg_to_sf_with_archive_load_files.py \
  'tests/end_to_end/test_numeric_replication.py::test_legacy_decimal_float_migration_requires_opt_in[postgres-native]'

run_sf_decimal \
  tests/integration/test_decimals.py::test_postgres_bounded_numeric_key_keeps_nan_and_canonical_identity

run_e2e \
  tests/end_to_end/target_snowflake/tap_mariadb/test_resync_mariadb_to_sf_table_size_check.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_multiline_native_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_defined_partial_sync_mariadb_to_sf.py \
  tests/end_to_end/target_snowflake/tap_mariadb/test_resync_mariadb_to_sf_with_split_large_files.py \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fallback_snowflake[mariadb-native]'

run_e2e \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fastsync_snowflake[postgres-native]' \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fastsync_snowflake[yugabyte-native]' \
  tests/end_to_end/target_snowflake/test_source_transformation_publication.py::test_transformations_are_private_through_publication \
  tests/end_to_end/target_snowflake/tap_postgres/test_iceberg_v3_postgres_to_sf.py::TestIcebergV3PostgresToSnowflake::test_partial_sync_merges_a_bounded_range_into_managed_iceberg_v3

run_e2e \
  'tests/end_to_end/test_numeric_replication.py::test_legacy_decimal_float_migration_requires_opt_in[mysql-native]' \
  'tests/end_to_end/test_numeric_replication.py::test_legacy_decimal_float_migration_requires_opt_in[mysql-v3]' \
  'tests/end_to_end/test_numeric_replication.py::test_legacy_decimal_float_migration_requires_opt_in[mariadb-v3]'

run_sf_decimal \
  tests/integration/test_decimals.py::test_float_tokens_load_nonfinite_values_into_existing_float_column

run_e2e \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fallback_snowflake[postgres-native]' \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fallback_snowflake[postgres-v3]' \
  tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_to_sf_with_custom_buffer_size.py \
  'tests/end_to_end/test_numeric_replication.py::test_legacy_decimal_float_migration_requires_opt_in[postgres-v3]' \
  'tests/end_to_end/test_numeric_replication.py::test_legacy_decimal_float_migration_requires_opt_in[mariadb-native]'

run_e2e \
  tests/end_to_end/target_snowflake/test_source_transformation_publication.py::test_singer_and_fastsync_preserve_mapped_types_and_values \
  tests/end_to_end/target_snowflake/test_source_transformation_publication.py::test_existing_singer_semantic_differences_remain_explicit \
  tests/end_to_end/target_snowflake/test_source_transformation_publication.py::test_ambiguous_regex_is_rejected_before_export \
  tests/end_to_end/target_snowflake/test_source_transformation_publication.py::test_bit_varying_conditions_match_snowflake

run_e2e \
  tests/end_to_end/test_numeric_replication.py::test_yugabyte_bounded_numeric_key_snowflake \
  tests/end_to_end/test_numeric_replication.py::test_mysql_bounded_decimal_key_snowflake \
  tests/end_to_end/test_numeric_replication.py::test_mysql_extended_types_snowflake \
  tests/end_to_end/test_numeric_replication.py::test_mysql_legacy_binary_width_partial_sync_snowflake \
  tests/end_to_end/target_snowflake/tap_mongodb/test_auto_native_to_iceberg_mongodb_to_sf.py

run_sf_decimal \
  tests/integration/test_decimals.py::test_opt_in_exact_decimal_load_versions_history_and_retries_without_reversion \
  tests/integration/test_decimals.py::test_decimal_precision_reversal_in_one_process_refreshes_startup_cache \
  tests/integration/test_decimals.py::test_decimal_archive_bounds_preserve_exact_text_and_ignore_nulls \
  tests/integration/test_runtime_contracts.py::test_cli_loads_utf8_and_acknowledges_committed_state \
  tests/integration/test_runtime_contracts.py::test_cli_rejects_invalid_file_format_before_loading \
  tests/integration/test_runtime_contracts.py::test_rejects_managed_text_variant_changes_without_versioning \
  tests/integration/test_decimals.py::test_float_fallback_keys_preserve_distinct_rows_across_batches \
  tests/integration/test_runtime_contracts.py::test_rejects_managed_merge_on_read_before_schema_changes \
  tests/integration/test_decimals.py::test_bounded_nan_retains_rows_in_copy_and_merge

run_e2e \
  tests/end_to_end/target_snowflake/test_native_to_iceberg_converter.py

run_sf_decimal \
  tests/integration/test_decimals.py::test_legacy_text_binary_key_matches_uppercase_fastsync_hex \
  tests/integration/test_runtime_contracts.py::test_rejects_managed_v2_before_schema_changes

run_e2e \
  tests/end_to_end/target_snowflake/tap_postgres/test_resync_pg_to_sf_table_size_check.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_replicate_pg_to_sf.py \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fastsync_snowflake[postgres-v3]' \
  'tests/end_to_end/test_numeric_replication.py::test_mysql_float_exact_snowflake[mysql-native]' \
  'tests/end_to_end/test_numeric_replication.py::test_postgres_float_nonfinite_snowflake[yugabyte-native]'

run_e2e \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fastsync_snowflake[mysql-native]' \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fastsync_snowflake[mysql-v3]' \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fastsync_snowflake[mariadb-native]' \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fastsync_snowflake[mariadb-v3]'

run_sf_decimal \
  tests/integration/test_runtime_contracts.py::test_sparse_patch_batches_preserve_omitted_values_and_apply_null \
  tests/integration/test_runtime_contracts.py::test_polymorphic_and_untyped_source_fields_load_after_flattening

run_e2e \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fallback_snowflake[mysql-native]' \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fallback_snowflake[mysql-v3]' \
  'tests/end_to_end/test_numeric_replication.py::test_decimal_fallback_snowflake[mariadb-v3]'

run_sf_decimal \
  tests/integration/test_runtime_contracts.py::test_failed_copy_or_merge_does_not_acknowledge_state \
  tests/integration/test_decimals.py::test_decimal_primary_keys_coalesce_equal_numeric_values

run_e2e \
  tests/end_to_end/target_snowflake/tap_mysql/test_iceberg_v3_mysql_to_sf.py::TestIcebergV3MySQLToSnowflake::test_iceberg_replication_preserves_keys_and_supplementary_unicode

run_sf_decimal \
  tests/integration/test_decimals.py::test_float_fallback_retains_overflow_rows_nulls_and_stable_column \
  tests/integration/test_decimals.py::test_decimal_key_change_retains_existing_type_and_adds_other_columns

run_e2e \
  tests/end_to_end/target_snowflake/tap_yugabyte/test_partial_sync_yugabyte_to_sf.py \
  tests/end_to_end/target_snowflake/tap_yugabyte/test_partial_sync_repair_yugabyte_to_sf.py \
  tests/end_to_end/target_snowflake/tap_postgres/test_resync_pg_to_sf_with_split_large_files.py

run_e2e \
  tests/end_to_end/test_yugabyte_recovery.py

run_sf_decimal \
  tests/integration/test_decimals.py::test_mysql_extended_composite_key_upgrade_preserves_historical_updates_and_deletes \
  tests/integration/test_runtime_contracts.py::test_rejects_table_format_change_without_altering_rows

run_e2e \
  'tests/end_to_end/test_numeric_replication.py::test_postgres_float_nonfinite_snowflake[postgres-native]' \
  tests/end_to_end/test_numeric_replication.py::test_postgres_bounded_numeric_key_snowflake

run_sf_decimal \
  tests/integration/test_decimals.py::test_legacy_float_decimal_keys_coalesce_collisions_and_keep_last_patch_and_delete
```

Run all twenty-three only for a full suite; otherwise run every affected group. MariaDB,
PostgreSQL, and genuine MySQL cover native and explicit v3.
Do not infer one format from another. `SHOW PRIMARY KEYS` does not prove Iceberg
identifier fields; compare raw-metadata `identifier-field-ids` with current
schema field IDs.


## Target-snowflake connector integration

Run this database-backed recipe only within the authorised scope and with
isolated fixtures. It must not overlap another session's Snowflake tests.
Paths below are relative to the repository root, not this runbook.

Run credentialed target-snowflake integration only in the ready `pipelinewise`
container and never beside another Snowflake group. The suite drops
`TARGET_SNOWFLAKE_SCHEMA`; dedicate it. After changing `dev-project/.env`,
recreate the CLI container and await its current-start readiness marker:

```bash
docker compose -f dev-project/docker-compose.yml up -d --force-recreate --no-deps pipelinewise
```

Use the bounded current-start readiness check in [AGENTS.md](AGENTS.md);
do not stream logs.
Require `PipelineWise Dev environment is ready in Docker container(s).`. The
CSV suite needs standard Snowflake/S3 variables,
`TARGET_SNOWFLAKE_SCHEMA`, and `TARGET_SNOWFLAKE_FILE_FORMAT_CSV` (which may
reuse `TARGET_SNOWFLAKE_FILE_FORMAT`); ensure the private key is readable.

Run the integration suite with plaintext upload explicitly selected and a
separate coverage file:

```bash
docker exec -t -e CLIENT_SIDE_ENCRYPTION_MASTER_KEY= \
  -e COVERAGE_FILE=/tmp/target-snowflake-integration.coverage pipelinewise bash -c '
  cd /opt/pipelinewise/singer-connectors/target-snowflake
  . ./venv/bin/activate
  pytest tests/integration -vvx \
    -k "not test_loading_tables_with_client_side_encryption or wrong_master_key" \
    --cov=target_snowflake --cov-report=term-missing
'
```

This excludes one successful client-side encryption test while retaining CSV
external and table-stage loads plus wrong-key rejection. Report this intentional
deselection and any unexpected skips. Full `make integration_test` separately
requires a real client-side encryption master key. Report actual counts and unit,
integration, and combined coverage separately; combining them does not satisfy
either individual gate.
