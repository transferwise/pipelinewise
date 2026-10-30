"""run_tap converts a Singer-only tap's native Snowflake table to managed Iceberg v3."""

from pathlib import Path

import yaml

from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake import CONFIG_DIR, TEST_PROJECTS_DIR_PATH
from tests.end_to_end.target_snowflake.tap_mongodb import TapMongoDB


TAP_ID = 'mongo_to_sf_auto_iceberg'
TARGET_ID = 'snowflake'


class TestAutoNativeToIcebergMongoDBToSnowflake(TapMongoDB):
    """Singer keeps replicating change streams into the converted table."""

    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.mongodb_con = self.e2e_env.get_tap_mongodb_connection()
        self.target_schema = f'PPW_E2E_TAP_MONGODB{self.e2e_env.sf_schema_postfix}'.upper()
        self.tap_yaml = Path(TEST_PROJECTS_DIR_PATH) / 'tap_mongodb_to_sf_auto_iceberg.yml'
        original = self.tap_yaml.read_text(encoding='utf-8')
        self.addCleanup(self.tap_yaml.write_text, original, encoding='utf-8')

    def _table_formats(self):
        rows = self.e2e_env.run_query_target_snowflake(
            'SELECT TABLE_NAME, IS_ICEBERG FROM INFORMATION_SCHEMA.TABLES '
            f"WHERE TABLE_SCHEMA = '{self.target_schema}' AND TABLE_TYPE = 'BASE TABLE'"
        )
        return dict(rows)

    def _snowflake_count(self):
        return self.e2e_env.run_query_target_snowflake(
            f'SELECT COUNT(*) FROM "{self.target_schema}"."MY_COLLECTION"'
        )[0][0]

    def test_singer_only_tap_converts_then_replicates(self):
        """A native LOG_BASED collection converts, then its next change reaches the Iceberg table."""
        assertions.assert_run_tap_success(TAP_ID, TARGET_ID, ['fastsync', 'singer'])
        assert self._table_formats()['MY_COLLECTION'] == 'NO'

        tap = yaml.safe_load(self.tap_yaml.read_text(encoding='utf-8'))
        tap.update(
            target_table_format='iceberg',
            iceberg_version=3,
            native_to_iceberg_conversion={'enabled': True},
        )
        self.tap_yaml.write_text(yaml.safe_dump(tap, sort_keys=False), encoding='utf-8')
        self.check_validate_taps()
        self.check_import_config()
        self.mongodb_con.my_collection.insert_one({'id': 2001, 'note': 'after conversion'})

        assertions.assert_run_tap_success(TAP_ID, TARGET_ID, ['singer'])

        formats = self._table_formats()
        assert formats['MY_COLLECTION'] == 'YES'
        assert formats['MY_COLLECTION_NATIVE'] == 'NO'
        assert not list((Path(CONFIG_DIR) / TARGET_ID).glob('iceberg-recovery-*.json'))
        assert self._snowflake_count() == self.mongodb_con.my_collection.count_documents({})
        assert self.e2e_env.run_query_target_snowflake(
            f'SELECT COUNT(*) FROM "{self.target_schema}"."MY_COLLECTION" WHERE DOCUMENT:id = 2001'
        )[0][0] == 1
