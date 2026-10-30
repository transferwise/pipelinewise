"""run_tap converts a PostgreSQL tap's native Snowflake tables to managed Iceberg v3."""

import json
from pathlib import Path

import yaml

from tests.end_to_end.helpers import assertions, tasks
from tests.end_to_end.target_snowflake import CONFIG_DIR, TEST_PROJECTS_DIR_PATH
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres


TAP_ID = 'postgres_to_sf_auto_iceberg'
TARGET_ID = 'snowflake'
TABLES = ('COUNTRY', 'EDGYDATA')


class TestAutoNativeToIcebergPostgresToSnowflake(TapPostgres):
    """Enable conversion on a tap that already replicates into native tables."""

    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.target_schema = f'PPW_E2E_TAP_POSTGRES{self.e2e_env.sf_schema_postfix}'.upper()
        self.tap_yaml = Path(TEST_PROJECTS_DIR_PATH) / 'tap_postgres_to_sf_auto_iceberg.yml'
        original = self.tap_yaml.read_text(encoding='utf-8')
        self.addCleanup(self.tap_yaml.write_text, original, encoding='utf-8')

    def _table_formats(self):
        rows = self.e2e_env.run_query_target_snowflake(
            'SELECT TABLE_NAME, IS_ICEBERG FROM INFORMATION_SCHEMA.TABLES '
            f"WHERE TABLE_SCHEMA = '{self.target_schema}' AND TABLE_TYPE = 'BASE TABLE'"
        )
        return dict(rows)

    def _snowflake_count(self, table):
        return self.e2e_env.run_query_target_snowflake(
            f'SELECT COUNT(*) FROM "{self.target_schema}"."{table}"'
        )[0][0]

    def _manifests(self):
        return sorted((Path(CONFIG_DIR) / TARGET_ID).glob('iceberg-recovery-*.json'))

    def _enable_conversion(self):
        tap = yaml.safe_load(self.tap_yaml.read_text(encoding='utf-8'))
        tap.update(
            target_table_format='iceberg',
            iceberg_version=3,
            native_to_iceberg_conversion={'enabled': True},
        )
        self.tap_yaml.write_text(yaml.safe_dump(tap, sort_keys=False), encoding='utf-8')
        self.check_validate_taps()
        self.check_import_config()

    def _preflight(self):
        return_code, stdout, stderr = tasks.run_command(
            f'pipelinewise copy_native_to_iceberg --target {TARGET_ID} --tap {TAP_ID} '
            '--preflight-only --iceberg-version 3 --output-format json'
        )
        assert return_code == 0, f'STDOUT: {stdout}\nSTDERR: {stderr}'
        # JSON reports keep stdout for the report and send log lines to stderr.
        report = json.loads(stdout)
        return report, {table['table'].rsplit('.', 1)[1]: table for table in report['tables']}

    def test_run_tap_converts_native_tables_before_replicating(self):
        """A blocked table stops the run unchanged; once fixed, both writers use the converted tables."""
        assertions.assert_run_tap_success(TAP_ID, TARGET_ID, ['fastsync', 'singer'])
        assert {table: self._table_formats()[table] for table in TABLES} == {'COUNTRY': 'NO', 'EDGYDATA': 'NO'}

        self.e2e_env.run_query_target_snowflake(
            f'CREATE TABLE "{self.target_schema}"."COUNTRY_NATIVE" ("LEFTOVER" NUMBER)'
        )
        self._enable_conversion()

        report, tables = self._preflight()
        assert report['ready'] is False
        assert tables['EDGYDATA']['state'] == 'native'
        assert tables['EDGYDATA']['action'] == 'convert'
        assert tables['EDGYDATA']['blockers'] == []
        assert tables['COUNTRY']['state'] == 'blocked'
        assert 'COUNTRY_NATIVE" already exists' in tables['COUNTRY']['blockers'][0]

        return_code, stdout, stderr = tasks.run_command(
            f'pipelinewise run_tap --tap {TAP_ID} --target {TARGET_ID}'
        )
        assert return_code == 1, f'STDOUT: {stdout}\nSTDERR: {stderr}'
        assert 'Automatic native-to-Iceberg conversion stopped' in stdout + stderr
        assert {table: self._table_formats()[table] for table in TABLES} == {'COUNTRY': 'NO', 'EDGYDATA': 'NO'}
        assert not self._manifests()

        self.e2e_env.run_query_target_snowflake(f'DROP TABLE "{self.target_schema}"."COUNTRY_NATIVE"')
        self.e2e_env.run_query_tap_postgres(
            "INSERT INTO public.edgydata (cvarchar) VALUES ('replicated after conversion')"
        )

        assertions.assert_run_tap_success(TAP_ID, TARGET_ID, ['fastsync', 'singer'])

        formats = self._table_formats()
        assert {table: formats[table] for table in TABLES} == {'COUNTRY': 'YES', 'EDGYDATA': 'YES'}
        assert {formats[f'{table}_NATIVE'] for table in TABLES} == {'NO'}
        assert not self._manifests()
        for table in TABLES:
            source_count = self.e2e_env.get_source_records_count('postgres', f'public.{table.lower()}')
            assert self._snowflake_count(table) == source_count, table
        assert self.e2e_env.run_query_target_snowflake(
            f'SELECT COUNT(*) FROM "{self.target_schema}"."EDGYDATA" '
            "WHERE CVARCHAR = 'replicated after conversion'"
        )[0][0] == 1

        report, tables = self._preflight()
        assert report['ready'] is True
        assert {tables[table]['state'] for table in TABLES} == {'iceberg'}
