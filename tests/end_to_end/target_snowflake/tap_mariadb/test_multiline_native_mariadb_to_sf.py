"""Native Snowflake multiline FastSync coverage for MariaDB."""

import json
from pathlib import Path

from tests.end_to_end.target_snowflake import CONFIG_DIR
from tests.end_to_end.target_snowflake.multiline_values import (
    PARTIALSYNC_TEXT_VALUES,
    SOURCE_ONLY_SENTINEL,
    assert_native_multiline_table,
    exercise_multiline_fastsync,
    prepare_mysql_multiline_table,
    snowflake_utf8_hex_rows,
    utf8_hex_rows,
)
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB
from tests.end_to_end.target_snowflake.mariadb_json_values import (
    JSON_COLUMNS,
    JSON_VALUES,
    exercise_mariadb_json_replication,
    prepare_mariadb_json_table,
)


TAP_ID = 'mariadb_to_sf_native_multiline'
TARGET_ID = 'snowflake'


class TestNativeMultilineMariaDBToSnowflake(TapMariaDB):
    """Exercise exact MariaDB text through native FullSync and PartialSync."""

    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.source_db = self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB')
        self.target_schema = (
            f'PPW_E2E_TAP_MYSQL_2{self.e2e_env.sf_schema_postfix}'
        ).upper()

    def prepare_source(self):
        """Create the multiline table before catalog discovery."""
        super().prepare_source()
        self.addCleanup(
            self.e2e_env.run_query_tap_mysql,
            'DROP TABLE IF EXISTS multiline_values',
        )
        prepare_mysql_multiline_table(self.e2e_env.run_query_tap_mysql)

    def test_native_full_and_bounded_partial_sync_preserve_multiline_bytes(self):
        """Native MariaDB FastSync preserves exact source text bytes."""
        exercise_multiline_fastsync(
            self,
            self.e2e_env.run_query_tap_mysql,
            self.source_db,
            'mysql',
            self.target_schema,
            lambda: assert_native_multiline_table(self, self.target_schema),
        )
        assertions.assert_run_tap_success(self.tap_id, self.target_id, ['singer'])
        self.assertEqual(
            snowflake_utf8_hex_rows(self.e2e_env, self.target_schema),
            utf8_hex_rows((SOURCE_ONLY_SENTINEL, *PARTIALSYNC_TEXT_VALUES)),
        )


class TestNativeMariaDBJSONToSnowflake(TapMariaDB):
    """Exercise new and established JSON columns on native Snowflake."""

    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.source_db = self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB')
        self.target_schema = f'PPW_E2E_TAP_MYSQL_2{self.e2e_env.sf_schema_postfix}'.upper()

    def prepare_source(self):
        super().prepare_source()
        prepare_mariadb_json_table(self, 'tap_mysql_to_sf_native_multiline.yml')

    def test_json_roots_and_retained_text_across_replication_engines(self):
        """Native Singer/PartialSync retain TEXT until explicit FullSync rebuilds."""
        exercise_mariadb_json_replication(self, iceberg=False, tap_yaml_name='tap_mysql_to_sf_native_multiline.yml')

    def test_legacy_generated_config_keeps_json_text_through_fullsync_and_singer(self):
        """An upgrade without import_config keeps old JSON schemas and populated TEXT."""
        tap_dir = Path(CONFIG_DIR) / self.target_id / self.tap_id
        config_path = tap_dir / 'config.json'
        config = json.loads(config_path.read_text(encoding='utf-8'))
        config.pop('json_target')
        config_path.write_text(json.dumps(config), encoding='utf-8')

        catalog_path = tap_dir / 'properties.json'
        catalog = json.loads(catalog_path.read_text(encoding='utf-8'))
        stream = next(stream for stream in catalog['streams'] if stream['stream'] == 'multiline_values')
        for column in JSON_COLUMNS:
            stream['schema']['properties'][column] = {'type': ['null', 'string']}
        root = next(entry['metadata'] for entry in stream['metadata'] if entry['breadcrumb'] == [])
        root.pop('mariadb-json-discovery')
        catalog_path.write_text(json.dumps(catalog), encoding='utf-8')

        def assert_text_rows(values: tuple[str | None, ...]) -> None:
            columns = self.e2e_env.run_query_target_snowflake(
                'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS '
                f"WHERE TABLE_SCHEMA = '{self.target_schema}' AND TABLE_NAME = 'MULTILINE_VALUES' "
                "AND COLUMN_NAME LIKE 'PAYLOAD%' ORDER BY COLUMN_NAME"
            )
            self.assertEqual(columns, [(column.upper(), 'TEXT') for column in JSON_COLUMNS])
            projection = ', '.join(f'HEX_ENCODE("{column.upper()}")' for column in JSON_COLUMNS)
            rows = self.e2e_env.run_query_target_snowflake(
                f'SELECT "ID", {projection} FROM "{self.target_schema}"."MULTILINE_VALUES" ORDER BY "ID"'
            )
            self.assertEqual(rows, [
                (row_id, *((None if value is None else value.encode('utf-8').hex().upper(),) * len(JSON_COLUMNS)))
                for row_id, value in enumerate(values, 1)
            ])

        stream_id = f'{self.source_db}-multiline_values'
        assertions.assert_resync_tables_success(
            self.tap_id, self.target_id,
            tables=f'{self.source_db}.multiline_values',
            expected_state_streams={'fastsync': {stream_id: True}},
        )
        assert_text_rows(JSON_VALUES)

        # Preserve untouched FullSync rows while Singer consumes a real binlog update.
        updated = '  { "after": "Singer", "unicode": "雪" }  '
        self.e2e_env.run_query_tap_mysql(
            'UPDATE multiline_values SET payload = %s, payload_hash = %s, payload_hidden = %s WHERE id = 1',
            (updated, updated, updated),
        )
        assertions.assert_run_tap_success(self.tap_id, self.target_id, ['singer'])
        assert_text_rows((updated, *JSON_VALUES[1:]))
