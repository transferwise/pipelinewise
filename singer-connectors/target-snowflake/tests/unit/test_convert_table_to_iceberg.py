import sys
import unittest
from unittest.mock import Mock, call, mock_open, patch

import target_snowflake
from snowflake.connector.errors import ProgrammingError
from target_snowflake.convert_table_to_iceberg import CopyNativeToIceberg


class TestCopyNativeToIceberg(unittest.TestCase):
    def setUp(self):
        self.converter = object.__new__(CopyNativeToIceberg)
        self.converter.fqtn = 'database.schema.table'

    @staticmethod
    def _single_column():
        return {'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 19, 'NUMERIC_SCALE': 0}

    @staticmethod
    def _state_results(original_native=False, native_backup=False, original_iceberg=False, iceberg_staging=False):
        tables = []
        if original_native:
            tables.append({'name': 'TABLE', 'is_iceberg': 'N'})
        if native_backup:
            tables.append({'name': 'TABLE_NATIVE', 'is_iceberg': 'N'})
        if original_iceberg:
            tables.append({'name': 'TABLE', 'is_iceberg': 'Y'})
        if iceberg_staging:
            tables.append({'name': 'TABLE_ICEBERG', 'is_iceberg': 'Y'})
        return (tables,)

    @staticmethod
    def _state_calls():
        return [call('SHOW TABLES IN SCHEMA "DATABASE"."SCHEMA" STARTS WITH \'TABLE\'')]

    @staticmethod
    def _drop_staging_call():
        return call('DROP ICEBERG TABLE IF EXISTS "DATABASE"."SCHEMA"."TABLE_ICEBERG"')

    @staticmethod
    def _rename_native_call():
        return call('ALTER TABLE "DATABASE"."SCHEMA"."TABLE" RENAME TO "DATABASE"."SCHEMA"."TABLE_NATIVE"')

    @staticmethod
    def _promote_iceberg_call():
        return call(
            'ALTER ICEBERG TABLE "DATABASE"."SCHEMA"."TABLE_ICEBERG" '
            'RENAME TO "DATABASE"."SCHEMA"."TABLE"'
        )

    @staticmethod
    def _restore_native_call():
        return call('ALTER TABLE "DATABASE"."SCHEMA"."TABLE_NATIVE" RENAME TO "DATABASE"."SCHEMA"."TABLE"')

    @staticmethod
    def _single_column_flow_calls():
        return [
            call('SHOW TABLES IN SCHEMA "DATABASE"."SCHEMA" STARTS WITH \'TABLE\''),
            call(
                'SELECT "COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE", '
                '"DATETIME_PRECISION" '
                'FROM "DATABASE"."INFORMATION_SCHEMA"."COLUMNS" '
                "WHERE \"TABLE_SCHEMA\" = 'SCHEMA' AND \"TABLE_NAME\" = 'TABLE' "
                'ORDER BY "ORDINAL_POSITION"'
            ),
            call(
                [
                    'SHOW PRIMARY KEYS IN TABLE "DATABASE"."SCHEMA"."TABLE";',
                    'SELECT "column_name" AS "COLUMN_NAME" FROM TABLE(RESULT_SCAN(-1)) '
                    'ORDER BY "key_sequence";',
                ]
            ),
            call('DROP ICEBERG TABLE IF EXISTS "DATABASE"."SCHEMA"."TABLE_ICEBERG"'),
            call(
                'CREATE ICEBERG TABLE "DATABASE"."SCHEMA"."TABLE_ICEBERG" ("ID" NUMBER(19,0)) '
                "DATA_RETENTION_TIME_IN_DAYS=1 TARGET_FILE_SIZE='16MB' ENABLE_DATA_COMPACTION=TRUE"
            ),
            call(
                'INSERT INTO "DATABASE"."SCHEMA"."TABLE_ICEBERG" '
                'SELECT "ID" FROM "DATABASE"."SCHEMA"."TABLE"'
            ),
        ]

    def test_parse_fqtn_preserves_quoted_names_and_canonicalizes_unquoted_names(self):
        self.assertEqual(
            self.converter.parse_fqtn('database.schema.table'),
            ('DATABASE', 'SCHEMA', 'TABLE'),
        )
        self.assertEqual(
            self.converter.parse_fqtn('"Mixed.DB".schema."Table ""Name"""'),
            ('Mixed.DB', 'SCHEMA', 'Table "Name"'),
        )

    def test_parse_fqtn_rejects_invalid_identifiers(self):
        invalid_identifiers = (
            None,
            '',
            'TABLE',
            'SCHEMA.TABLE',
            'DATABASE.SCHEMA.TABLE.EXTRA',
            'DATABASE.SCHEMA.TABLE NAME',
            'DATABASE.SCHEMA.""',
            'DATABASE.SCHEMA."UNTERMINATED',
        )

        for fqtn in invalid_identifiers:
            with self.subTest(fqtn=fqtn), self.assertRaises(ValueError):
                self.converter.parse_fqtn(fqtn)

    @patch.object(CopyNativeToIceberg, 'query')
    def test_native_flow_preserves_source_metadata_and_executes_exact_queries(self, query):
        native_columns = [
            {'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 19, 'NUMERIC_SCALE': 0},
            {'COLUMN_NAME': 'AMOUNT', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 38, 'NUMERIC_SCALE': 10},
            {'COLUMN_NAME': 'LEGACY_NUMBER', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': None},
            {'COLUMN_NAME': 'Display Name', 'DATA_TYPE': 'TEXT'},
            {'COLUMN_NAME': 'CREATED_AT', 'DATA_TYPE': 'TIMESTAMP_TZ'},
            {'COLUMN_NAME': 'EVENT_TIME', 'DATA_TYPE': 'TIME', 'DATETIME_PRECISION': 9},
            {'COLUMN_NAME': 'PAYLOAD', 'DATA_TYPE': 'VARIANT'},
            {'COLUMN_NAME': 'IS_ACTIVE', 'DATA_TYPE': 'BOOLEAN'},
        ]
        original_columns = [column.copy() for column in native_columns]
        primary_key = [{'COLUMN_NAME': 'ID'}, {'COLUMN_NAME': 'Display Name'}]
        query.side_effect = [
            *self._state_results(original_native=True),
            native_columns,
            primary_key,
            [],
            [],
            [],
        ]

        CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='NATIVE')

        self.assertEqual(native_columns, original_columns)
        self.assertEqual(
            query.call_args_list,
            [
                *self._state_calls(),
                call(
                    'SELECT "COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE", '
                    '"DATETIME_PRECISION" '
                    'FROM "DATABASE"."INFORMATION_SCHEMA"."COLUMNS" '
                    "WHERE \"TABLE_SCHEMA\" = 'SCHEMA' AND \"TABLE_NAME\" = 'TABLE' "
                    'ORDER BY "ORDINAL_POSITION"'
                ),
                call(
                    [
                        'SHOW PRIMARY KEYS IN TABLE "DATABASE"."SCHEMA"."TABLE";',
                        'SELECT "column_name" AS "COLUMN_NAME" FROM TABLE(RESULT_SCAN(-1)) '
                        'ORDER BY "key_sequence";',
                    ]
                ),
                self._drop_staging_call(),
                call(
                    'CREATE ICEBERG TABLE "DATABASE"."SCHEMA"."TABLE_ICEBERG" '
                    '("ID" NUMBER(19,0), "AMOUNT" NUMBER(38,10), "LEGACY_NUMBER" NUMBER(38,0), '
                    '"Display Name" VARCHAR, "CREATED_AT" TIMESTAMP_LTZ(6), "EVENT_TIME" TIME(6), '
                    '"PAYLOAD" TEXT, '
                    '"IS_ACTIVE" BOOLEAN, PRIMARY KEY ("ID", "Display Name")) '
                    "DATA_RETENTION_TIME_IN_DAYS=1 TARGET_FILE_SIZE='16MB' ENABLE_DATA_COMPACTION=TRUE"
                ),
                call(
                    'INSERT INTO "DATABASE"."SCHEMA"."TABLE_ICEBERG" '
                    'SELECT "ID", "AMOUNT", "LEGACY_NUMBER", "Display Name", '
                    'TO_TIMESTAMP_LTZ("CREATED_AT") AS "CREATED_AT", '
                    'CAST("EVENT_TIME" AS TIME(6)) AS "EVENT_TIME", "PAYLOAD", "IS_ACTIVE" '
                    'FROM "DATABASE"."SCHEMA"."TABLE"'
                ),
            ],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_iceberg_flow_preserves_quoted_identifiers_and_executes_exact_queries(self, query):
        native_columns = [
            {'COLUMN_NAME': 'ID', 'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 19, 'NUMERIC_SCALE': 0},
            {'COLUMN_NAME': 'Odd"Column', 'DATA_TYPE': 'TIMESTAMP_TZ'},
        ]
        query.side_effect = [
            [{'name': 'Table Name', 'is_iceberg': 'N'}],
            native_columns,
            [],
            [],
            [],
            [],
            [],
            [],
        ]

        CopyNativeToIceberg(
            connection_config={},
            fqtn='"Mixed.DB"."select"."Table Name"',
            eventual='ICEBERG',
        )

        self.assertEqual(
            query.call_args_list,
            [
                call('SHOW TABLES IN SCHEMA "Mixed.DB"."select" STARTS WITH \'Table Name\''),
                call(
                    'SELECT "COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE", '
                    '"DATETIME_PRECISION" '
                    'FROM "Mixed.DB"."INFORMATION_SCHEMA"."COLUMNS" '
                    "WHERE \"TABLE_SCHEMA\" = 'select' AND \"TABLE_NAME\" = 'Table Name' "
                    'ORDER BY "ORDINAL_POSITION"'
                ),
                call(
                    [
                        'SHOW PRIMARY KEYS IN TABLE "Mixed.DB"."select"."Table Name";',
                        'SELECT "column_name" AS "COLUMN_NAME" FROM TABLE(RESULT_SCAN(-1)) '
                        'ORDER BY "key_sequence";',
                    ]
                ),
                call('DROP ICEBERG TABLE IF EXISTS "Mixed.DB"."select"."Table Name_ICEBERG"'),
                call(
                    'CREATE ICEBERG TABLE "Mixed.DB"."select"."Table Name_ICEBERG" '
                    '("ID" NUMBER(19,0), "Odd""Column" TIMESTAMP_LTZ(6)) '
                    "DATA_RETENTION_TIME_IN_DAYS=1 TARGET_FILE_SIZE='16MB' ENABLE_DATA_COMPACTION=TRUE"
                ),
                call(
                    'INSERT INTO "Mixed.DB"."select"."Table Name_ICEBERG" '
                    'SELECT "ID", TO_TIMESTAMP_LTZ("Odd""Column") AS "Odd""Column" '
                    'FROM "Mixed.DB"."select"."Table Name"'
                ),
                call(
                    'ALTER TABLE "Mixed.DB"."select"."Table Name" '
                    'RENAME TO "Mixed.DB"."select"."Table Name_NATIVE"'
                ),
                call(
                    'ALTER ICEBERG TABLE "Mixed.DB"."select"."Table Name_ICEBERG" '
                    'RENAME TO "Mixed.DB"."select"."Table Name"'
                ),
            ],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_stale_staging_cleanup_failure_stops_before_create(self, query):
        cleanup_error = RuntimeError('stale cleanup failed')
        query.side_effect = [
            *self._state_results(original_native=True, iceberg_staging=True),
            [self._single_column()],
            [],
            cleanup_error,
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, cleanup_error)
        self.assertEqual(query.call_args_list, self._single_column_flow_calls()[:4])

    @patch.object(CopyNativeToIceberg, 'query')
    def test_iceberg_create_failure_cleans_staging_without_renaming_live_table(self, query):
        create_error = RuntimeError('create failed')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            create_error,
            [],
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, create_error)
        self.assertEqual(
            query.call_args_list,
            self._single_column_flow_calls()[:5] + [self._drop_staging_call()],
        )

    @patch('target_snowflake.convert_table_to_iceberg.get_logger')
    @patch.object(CopyNativeToIceberg, 'query')
    def test_staging_cleanup_failure_does_not_mask_create_error(self, query, get_logger):
        create_error = RuntimeError('create failed')
        cleanup_error = RuntimeError('cleanup failed')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            create_error,
            cleanup_error,
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, create_error)
        get_logger.return_value.exception.assert_called_once_with(
            'Failed to drop Iceberg staging table after %s',
            'staging load failure',
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_iceberg_insert_failure_cleans_staging_without_renaming_live_table(self, query):
        insert_error = RuntimeError('insert failed')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            insert_error,
            [],
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, insert_error)
        self.assertEqual(query.call_args_list, self._single_column_flow_calls() + [self._drop_staging_call()])

    @patch.object(CopyNativeToIceberg, 'query')
    def test_native_rename_error_cleans_staging_when_original_still_exists(self, query):
        rename_error = ProgrammingError('native rename failed')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
            rename_error,
            *self._state_results(original_native=True, iceberg_staging=True),
            [],
        ]

        with self.assertRaises(ProgrammingError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, rename_error)
        self.assertEqual(
            query.call_args_list,
            self._single_column_flow_calls()
            + [self._rename_native_call(), *self._state_calls(), self._drop_staging_call()],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_native_rename_transport_error_preserves_staging_while_original_is_visible(self, query):
        rename_error = RuntimeError('native rename response lost')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
            rename_error,
            *self._state_results(original_native=True, iceberg_staging=True),
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, rename_error)
        self.assertEqual(
            query.call_args_list,
            self._single_column_flow_calls() + [self._rename_native_call(), *self._state_calls()],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_native_rename_error_continues_when_state_confirms_commit(self, query):
        rename_error = RuntimeError('native rename response lost')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
            rename_error,
            *self._state_results(native_backup=True, iceberg_staging=True),
            [],
        ]

        CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertEqual(
            query.call_args_list,
            self._single_column_flow_calls()
            + [self._rename_native_call(), *self._state_calls(), self._promote_iceberg_call()],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_native_rename_inconclusive_state_preserves_staging_and_backup(self, query):
        rename_error = RuntimeError('native rename response lost')
        inspection_error = RuntimeError('state inspection failed')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
            rename_error,
            inspection_error,
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, rename_error)
        self.assertEqual(
            query.call_args_list,
            self._single_column_flow_calls() + [self._rename_native_call(), self._state_calls()[0]],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_promotion_error_is_success_when_state_confirms_commit(self, query):
        promotion_error = RuntimeError('promotion response lost')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
            [],
            promotion_error,
            *self._state_results(native_backup=True, original_iceberg=True),
        ]

        CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertEqual(
            query.call_args_list,
            self._single_column_flow_calls()
            + [self._rename_native_call(), self._promote_iceberg_call(), *self._state_calls()],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_promotion_inconclusive_state_preserves_staging_and_backup(self, query):
        promotion_error = RuntimeError('promotion response lost')
        inspection_error = RuntimeError('state inspection failed')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
            [],
            promotion_error,
            inspection_error,
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, promotion_error)
        self.assertEqual(
            query.call_args_list,
            self._single_column_flow_calls()
            + [self._rename_native_call(), self._promote_iceberg_call(), self._state_calls()[0]],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_promotion_failure_restores_native_cleans_staging_and_raises_original(self, query):
        promotion_error = RuntimeError('promotion failed')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
            [],
            promotion_error,
            *self._state_results(native_backup=True, iceberg_staging=True),
            [],
            [],
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, promotion_error)
        self.assertEqual(
            query.call_args_list,
            self._single_column_flow_calls()
            + [
                self._rename_native_call(),
                self._promote_iceberg_call(),
                *self._state_calls(),
                self._restore_native_call(),
                self._drop_staging_call(),
            ],
        )

    @patch('target_snowflake.convert_table_to_iceberg.get_logger')
    @patch.object(CopyNativeToIceberg, 'query')
    def test_recovery_failure_preserves_staging_and_raises_promotion_error(self, query, get_logger):
        promotion_error = RuntimeError('promotion failed')
        recovery_error = RuntimeError('recovery failed')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
            [],
            promotion_error,
            *self._state_results(native_backup=True, iceberg_staging=True),
            recovery_error,
            *self._state_results(native_backup=True, iceberg_staging=True),
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, promotion_error)
        self.assertEqual(
            query.call_args_list,
            self._single_column_flow_calls()
            + [
                self._rename_native_call(),
                self._promote_iceberg_call(),
                *self._state_calls(),
                self._restore_native_call(),
                *self._state_calls(),
            ],
        )
        get_logger.return_value.exception.assert_called_once_with(
            'Failed to restore native table %s after Iceberg promotion failed; '
            'preserving Iceberg staging and native backup',
            '"DATABASE"."SCHEMA"."TABLE"',
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_recovery_error_cleans_staging_when_state_confirms_restore_committed(self, query):
        promotion_error = RuntimeError('promotion failed')
        recovery_error = RuntimeError('recovery response lost')
        query.side_effect = [
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
            [],
            promotion_error,
            *self._state_results(native_backup=True, iceberg_staging=True),
            recovery_error,
            *self._state_results(original_native=True, iceberg_staging=True),
            [],
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, promotion_error)
        self.assertEqual(query.call_args_list[-1], self._drop_staging_call())

    def test_number_type_preserves_available_precision_and_scale(self):
        self.assertEqual(
            self.converter._iceberg_data_type(
                {'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': 22, 'NUMERIC_SCALE': None}
            ),
            'NUMBER(22,0)',
        )
        self.assertEqual(
            self.converter._iceberg_data_type(
                {'DATA_TYPE': 'NUMBER', 'NUMERIC_PRECISION': None, 'NUMERIC_SCALE': 4}
            ),
            'NUMBER(38,4)',
        )

    def test_time_type_is_explicitly_limited_to_iceberg_microsecond_precision(self):
        self.assertEqual(
            self.converter._iceberg_data_type(
                {'DATA_TYPE': 'TIME', 'DATETIME_PRECISION': 9}
            ),
            'TIME(6)',
        )

    def test_timestamp_types_are_explicitly_limited_to_microsecond_precision(self):
        """Account defaults cannot silently create nanosecond Iceberg timestamps."""
        self.assertEqual(
            self.converter._iceberg_data_type({'DATA_TYPE': 'TIMESTAMP_TZ'}),
            'TIMESTAMP_LTZ(6)',
        )
        self.assertEqual(
            self.converter._iceberg_data_type({'DATA_TYPE': 'TIMESTAMP_LTZ'}),
            'TIMESTAMP_LTZ(6)',
        )
        self.assertEqual(
            self.converter._iceberg_data_type({'DATA_TYPE': 'TIMESTAMP_NTZ'}),
            'TIMESTAMP_NTZ(6)',
        )

    @patch('target_snowflake.convert_table_to_iceberg.get_logger')
    @patch.object(CopyNativeToIceberg, 'query')
    def test_already_iceberg_is_idempotent_success_and_warns_about_metadata(self, query, get_logger):
        query.side_effect = self._state_results(original_iceberg=True)

        CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table')

        self.assertEqual(query.call_args_list, self._state_calls())
        get_logger.return_value.warning.assert_any_call(
            'Replication and all writes to %s must be stopped before conversion. '
            'Run this utility with a role that can read every row and unmasked value. '
            'The role must have CREATE ICEBERG TABLE on its schema. --eventual ICEBERG cutover, or recovery '
            'that renames a native table, also requires ownership of that native table. '
            'TIMESTAMP_TZ becomes TIMESTAMP_LTZ(6); TIMESTAMP_NTZ, TIMESTAMP_LTZ, and TIME are limited '
            'to microsecond precision; original timezone offsets are lost and VARIANT becomes TEXT. '
            'Reapply metadata not copied by this utility, including grants, policies, tags, comments, '
            'nullability, and defaults.',
            'database.schema.table',
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_resumes_loaded_staging_without_recopied_data(self, query):
        query.side_effect = [
            *self._state_results(native_backup=True, iceberg_staging=True),
            [],
        ]

        CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertEqual(query.call_args_list, self._state_calls() + [self._promote_iceberg_call()])

    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_native_retry_restores_native_and_keeps_loaded_companion(self, query):
        query.side_effect = [
            *self._state_results(native_backup=True, iceberg_staging=True),
            [],
        ]

        CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='NATIVE')

        self.assertEqual(query.call_args_list, self._state_calls() + [self._restore_native_call()])

    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_resumed_promotion_error_is_success_when_commit_is_confirmed(self, query):
        promotion_error = RuntimeError('promotion response lost')
        query.side_effect = [
            *self._state_results(native_backup=True, iceberg_staging=True),
            promotion_error,
            *self._state_results(native_backup=True, original_iceberg=True),
        ]

        CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertEqual(
            query.call_args_list,
            self._state_calls() + [self._promote_iceberg_call(), *self._state_calls()],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_failed_resumed_promotion_restores_native_and_cleans_staging(self, query):
        promotion_error = RuntimeError('promotion failed')
        query.side_effect = [
            *self._state_results(native_backup=True, iceberg_staging=True),
            promotion_error,
            *self._state_results(native_backup=True, iceberg_staging=True),
            [],
            [],
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, promotion_error)
        self.assertEqual(
            query.call_args_list,
            self._state_calls()
            + [
                self._promote_iceberg_call(),
                *self._state_calls(),
                self._restore_native_call(),
                self._drop_staging_call(),
            ],
        )

    @patch('target_snowflake.convert_table_to_iceberg.get_logger')
    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_restore_failure_preserves_objects_and_original_promotion_error(self, query, get_logger):
        promotion_error = RuntimeError('promotion failed')
        recovery_error = RuntimeError('recovery failed')
        query.side_effect = [
            *self._state_results(native_backup=True, iceberg_staging=True),
            promotion_error,
            *self._state_results(native_backup=True, iceberg_staging=True),
            recovery_error,
            *self._state_results(native_backup=True, iceberg_staging=True),
        ]

        with self.assertRaises(RuntimeError) as raised:
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertIs(raised.exception, promotion_error)
        self.assertEqual(
            query.call_args_list,
            self._state_calls()
            + [
                self._promote_iceberg_call(),
                *self._state_calls(),
                self._restore_native_call(),
                *self._state_calls(),
            ],
        )
        get_logger.return_value.exception.assert_called_once_with(
            'Failed to restore native table after resumed Iceberg promotion; '
            'preserving Iceberg staging and native backup'
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_restores_backup_only_before_restarting_conversion(self, query):
        query.side_effect = [
            *self._state_results(native_backup=True),
            [],
            [self._single_column()],
            [],
            [],
            [],
            [],
        ]

        CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='NATIVE')

        self.assertEqual(
            query.call_args_list,
            self._state_calls() + [self._restore_native_call()] + self._single_column_flow_calls()[1:],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_restore_error_restarts_when_state_confirms_commit(self, query):
        restore_error = RuntimeError('restore response lost')
        query.side_effect = [
            *self._state_results(native_backup=True),
            restore_error,
            *self._state_results(original_native=True),
            [self._single_column()],
            [],
            [],
            [],
            [],
        ]

        CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='NATIVE')

        self.assertEqual(
            query.call_args_list,
            self._state_calls()
            + [self._restore_native_call(), *self._state_calls()]
            + self._single_column_flow_calls()[1:],
        )

    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_unsafe_state_requires_manual_recovery_without_mutation(self, query):
        query.side_effect = self._state_results(iceberg_staging=True)

        with self.assertRaisesRegex(RuntimeError, 'inspect the original, _NATIVE, and _ICEBERG tables'):
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual='ICEBERG')

        self.assertEqual(query.call_args_list, self._state_calls())

    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_original_and_native_backup_requires_manual_recovery(self, query):
        for eventual in ('NATIVE', 'ICEBERG'):
            with self.subTest(eventual=eventual):
                query.reset_mock()
                query.side_effect = self._state_results(original_native=True, native_backup=True)

                with self.assertRaisesRegex(RuntimeError, 'inspect the original, _NATIVE, and _ICEBERG tables'):
                    CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table', eventual=eventual)

                self.assertEqual(query.call_args_list, self._state_calls())

    @patch.object(CopyNativeToIceberg, 'query')
    def test_startup_missing_show_metadata_fails_without_mutation(self, query):
        query.return_value = [{'name': 'TABLE'}]

        with self.assertRaisesRegex(RuntimeError, 'name and is_iceberg metadata'):
            CopyNativeToIceberg(connection_config={}, fqtn='database.schema.table')

        self.assertEqual(query.call_args_list, self._state_calls())

    @patch.object(CopyNativeToIceberg, 'query')
    def test_check_iceberg_ignores_non_exact_like_match(self, query):
        self.converter.logger = Mock()
        query.return_value = [{'name': 'TABLE_BACKUP'}]

        self.assertFalse(self.converter.check_iceberg())

    @patch('target_snowflake.convert_table_to_iceberg.get_logger')
    @patch.object(CopyNativeToIceberg, 'query')
    @patch('builtins.open', new_callable=mock_open, read_data='{}')
    def test_cli_already_iceberg_returns_success(self, _config_file, query, _get_logger):
        query.side_effect = self._state_results(original_iceberg=True)

        with patch.object(
            sys,
            'argv',
            ['copy-native-to-iceberg', '-c', 'config.json', '-t', 'database.schema.table', '-e', 'iceberg'],
        ):
            result = target_snowflake.copy_native_to_iceberg()

        self.assertIsNone(result)
        self.assertEqual(query.call_args_list, self._state_calls())

    @patch('target_snowflake.CopyNativeToIceberg')
    @patch('builtins.open', new_callable=mock_open, read_data='{}')
    def test_cli_keeps_quoted_fqtn_case(self, _config_file, converter):
        fqtn = '"Mixed.DB"."select"."Table Name"'

        with patch.object(sys, 'argv', ['copy-native-to-iceberg', '-c', 'config.json', '-t', fqtn, '-e', 'iceberg']):
            target_snowflake.copy_native_to_iceberg()

        converter.assert_called_once_with(connection_config={}, fqtn=fqtn, eventual='ICEBERG')
