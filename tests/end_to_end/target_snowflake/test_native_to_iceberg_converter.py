"""Credentialed target-only coverage for manual native-to-Iceberg conversion."""

import json
import os
import subprocess
import uuid

from pathlib import Path

import pytest

from snowflake.connector.errors import ProgrammingError

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
    TABLE_FORMAT_MISSING,
    TABLE_FORMAT_NATIVE,
    SnowflakeIcebergPublisher,
    SnowflakeObjectName,
    SnowflakeQueryAdapter,
    quote_identifier,
    sql_string_literal,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    EVENTUAL_ICEBERG,
    SnowflakeNativeToIcebergConverter,
)
from tests.end_to_end.target_snowflake.mariadb_json_values import JSON_VALUES


REQUIRED_ENVIRONMENT = {
    'account': 'TARGET_SNOWFLAKE_ACCOUNT',
    'dbname': 'TARGET_SNOWFLAKE_DBNAME',
    'user': 'TARGET_SNOWFLAKE_USER',
    'private_key': 'TARGET_SNOWFLAKE_PRIVATE_KEY',
    'warehouse': 'TARGET_SNOWFLAKE_WAREHOUSE',
}
TARGET_ID = 'snowflake-converter'
TABLE_COMMENT = 'source: C:\\data' + '\\'
COLUMN_COMMENT = "column: x\\' || CURRENT_USER() || '"


class LostPromotionResponseAdapter:
    """Raise after one real Iceberg promotion has committed."""

    def __init__(self, adapter):
        self.adapter = adapter
        self.connection_config = adapter.connection_config
        self.raised = False

    def query(self, query, params=None, query_tag_props=None):
        """Delegate a query and simulate one lost committed response."""
        result = self.adapter.query(query, params, query_tag_props)
        if (
            not self.raised
            and query.startswith('ALTER ICEBERG TABLE')
            and ' RENAME TO ' in query
        ):
            self.raised = True
            raise RuntimeError('simulated lost promotion response')
        return result


def _target_config():
    config = {
        key: os.environ.get(environment_name)
        for key, environment_name in REQUIRED_ENVIRONMENT.items()
    }
    missing = [
        environment_name
        for key, environment_name in REQUIRED_ENVIRONMENT.items()
        if not config[key]
    ]
    if missing:
        pytest.fail(f'Missing required Snowflake E2E configuration: {missing}')
    role = os.environ.get('TARGET_SNOWFLAKE_ROLE')
    if role:
        config['role'] = role
    return config


@pytest.fixture(name='conversion_environment')
def _conversion_environment(tmp_path):
    """Create a unique schema and always remove it after conversion."""
    target_config = _target_config()
    adapter = SnowflakeQueryAdapter(target_config)
    database_rows = adapter.query('SELECT CURRENT_DATABASE() AS "DATABASE_NAME"')
    database = database_rows[0]['DATABASE_NAME']
    schema = f'PW_E2E_NATIVE_ICEBERG_{uuid.uuid4().hex[:10]}'.upper()
    schema_name = SnowflakeObjectName(database, schema, 'PLACEHOLDER')
    quoted_schema = schema_name.quoted.rsplit('.', 1)[0]
    config_dir = tmp_path / 'pipelinewise-config'
    target_dir = config_dir / TARGET_ID
    target_dir.mkdir(parents=True)
    (config_dir / 'config.json').write_text(json.dumps({
        'targets': [{
            'id': TARGET_ID,
            'name': 'Snowflake converter E2E',
            'type': 'target-snowflake',
            'status': 'ready',
            'taps': [],
        }],
    }), encoding='utf-8')
    (target_dir / 'config.json').write_text(
        json.dumps(target_config),
        encoding='utf-8',
    )
    adapter.query(f'CREATE SCHEMA {quoted_schema}')
    try:
        yield adapter, database, schema, config_dir, target_dir
    finally:
        adapter.query(f'DROP SCHEMA IF EXISTS {quoted_schema} CASCADE')


def _create_native_table(adapter, table):
    adapter.query(
        f'CREATE TABLE {table.quoted} ('
        '"TENANT_ID" NUMBER(38,0), '
        '"ID" NUMBER(38,0), '
        '"PAYLOAD" VARIANT, '
        '"NOTE" VARCHAR, '
        'PRIMARY KEY ("TENANT_ID", "ID")'
        ')'
    )
    adapter.query(
        f'COMMENT ON TABLE {table.quoted} IS %(comment)s',
        {'comment': TABLE_COMMENT},
    )
    adapter.query(
        f'COMMENT ON COLUMN {table.quoted}."NOTE" IS %(comment)s',
        {'comment': COLUMN_COMMENT},
    )
    payload = json.dumps({
        'large': 'x' * 70000,
        'nested': [None, {}, 'Zażółć gęślą jaźń'],
    })
    adapter.query(
        f'INSERT INTO {table.quoted} ("TENANT_ID", "ID", "PAYLOAD", "NOTE") '
        'SELECT 10, 1, PARSE_JSON(%(payload)s), %(note)s',
        {'payload': payload, 'note': "quotes ' and unicode ✓"},
    )
    adapter.query(
        f'INSERT INTO {table.quoted} ("TENANT_ID", "ID", "PAYLOAD", "NOTE") '
        "SELECT 10, 2, PARSE_JSON('null'), NULL "
        "UNION ALL SELECT 20, 3, NULL, ''"
    )


def _rows(adapter, table):
    return adapter.query(
        'SELECT "TENANT_ID", "ID", '
        'CASE WHEN "PAYLOAD" IS NULL THEN \'SQL_NULL\' '
        'WHEN IS_NULL_VALUE("PAYLOAD") THEN \'JSON_NULL\' '
        'ELSE TO_JSON("PAYLOAD") END AS "PAYLOAD_VALUE", '
        '"NOTE" '
        f'FROM {table.quoted} ORDER BY "TENANT_ID", "ID"'
    )


def _run_conversion_cli(config_dir, table, eventual=None, *, success=True):
    command = [
        'pipelinewise',
        'copy_native_to_iceberg',
        '--target',
        TARGET_ID,
        '--table',
        table.quoted,
        '--iceberg-version',
        '3',
    ]
    if eventual is not None:
        command.extend(['--eventual', eventual])
    command_env = os.environ.copy()
    command_env['PIPELINEWISE_CONFIG_DIRECTORY'] = str(config_dir)
    process = subprocess.run(
        command,
        capture_output=True,
        check=False,
        env=command_env,
        text=True,
        timeout=300,
    )
    assert (process.returncode == 0) == success, (
        f'Command failed: {command}\nstdout:\n{process.stdout}\nstderr:\n{process.stderr}'
    )
    return process


def _row_value(row, name, default=None):
    for key in (name, name.upper(), name.lower()):
        if key in row:
            return row[key]
    return default


def _ordered_primary_key(adapter, table):
    rows = adapter.query(f'SHOW PRIMARY KEYS IN TABLE {table.quoted}')
    return tuple(
        _row_value(row, 'COLUMN_NAME')
        for row in sorted(rows, key=lambda row: int(_row_value(row, 'KEY_SEQUENCE')))
    )


def _supported_metadata(adapter, table):
    column_rows = adapter.query(
        'SELECT "COLUMN_NAME", "COMMENT" '
        f'FROM {quote_identifier(table.database)}."INFORMATION_SCHEMA"."COLUMNS" '
        'WHERE "TABLE_SCHEMA" = %(schema)s AND "TABLE_NAME" = %(table)s '
        'ORDER BY "ORDINAL_POSITION"',
        {'schema': table.schema, 'table': table.table},
    )
    table_rows = adapter.query(
        'SELECT "COMMENT", "TABLE_OWNER" '
        f'FROM {quote_identifier(table.database)}."INFORMATION_SCHEMA"."TABLES" '
        'WHERE "TABLE_SCHEMA" = %(schema)s AND "TABLE_NAME" = %(table)s',
        {'schema': table.schema, 'table': table.table},
    )
    quoted_schema = '.'.join(
        quote_identifier(identifier)
        for identifier in (table.database, table.schema)
    )
    show_rows = adapter.query(
        f'SHOW TABLES IN SCHEMA {quoted_schema} STARTS WITH '
        f'{sql_string_literal(table.table)}'
    )
    exact_show_rows = [
        row for row in show_rows
        if _row_value(row, 'name') == table.table
    ]
    assert len(table_rows) == 1
    assert len(exact_show_rows) == 1
    grant_rows = adapter.query(f'SHOW GRANTS ON TABLE {table.quoted}')
    grants = tuple(sorted(
        (
            str(_row_value(row, 'privilege')).upper(),
            str(_row_value(row, 'granted_to')).upper(),
            _row_value(row, 'grantee_name'),
            str(_row_value(row, 'grant_option')).upper() == 'TRUE',
        )
        for row in grant_rows
        if str(_row_value(row, 'privilege')).upper() != 'OWNERSHIP'
    ))
    tag_rows = adapter.query(
        'SELECT "TAG_DATABASE", "TAG_SCHEMA", "TAG_NAME", "TAG_VALUE", '
        '"APPLY_METHOD", "LEVEL" '
        f'FROM TABLE({quote_identifier(table.database)}."INFORMATION_SCHEMA".'
        "TAG_REFERENCES(%(table)s, 'TABLE'))",
        {'table': table.quoted},
    )
    tags = tuple(sorted(
        tuple(_row_value(row, field) for field in (
            'TAG_DATABASE', 'TAG_SCHEMA', 'TAG_NAME', 'TAG_VALUE'
        ))
        for row in tag_rows
        if str(_row_value(row, 'LEVEL', '')).upper() == 'TABLE'
        and str(_row_value(row, 'APPLY_METHOD', '')).upper() != 'INHERITED'
    ))
    return {
        'column_comments': tuple(
            (_row_value(row, 'COLUMN_NAME'), _row_value(row, 'COMMENT'))
            for row in column_rows
        ),
        'table_comment': _row_value(table_rows[0], 'COMMENT'),
        'owner': _row_value(table_rows[0], 'TABLE_OWNER'),
        'owner_role_type': _row_value(exact_show_rows[0], 'owner_role_type'),
        'grants': grants,
        'tags': tags,
    }


def _add_supported_metadata(adapter, table):
    role = adapter.query('SELECT CURRENT_ROLE() AS "ROLE_NAME"')[0]['ROLE_NAME']
    adapter.query(
        f'GRANT SELECT ON TABLE {table.quoted} TO ROLE {quote_identifier(role)}'
    )
    tag = table.with_table('CONVERTER_TAG')
    try:
        adapter.query(f'CREATE TAG {tag.quoted}')
        adapter.query(
            f'ALTER TABLE {table.quoted} SET TAG {tag.quoted} = \'conversion-e2e\''
        )
    except ProgrammingError as exc:
        if getattr(exc, 'sqlstate', None) != '42501':
            raise
        return False
    return True


def _formats(adapter, runtime_dir, table):
    publisher = SnowflakeIcebergPublisher(adapter, str(runtime_dir))
    return (
        publisher.discover_table_format(table.schema, table.table),
        publisher.discover_table_format(table.schema, table.with_suffix('_ICEBERG').table),
        publisher.discover_table_format(table.schema, table.with_suffix('_NATIVE').table),
    )


def _merge_on_read_behavior(adapter, table):
    rows = adapter.query(
        "SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' "
        f'IN TABLE {table.quoted}'
    )
    assert len(rows) == 1
    assert str(_row_value(rows[0], 'level')).upper() == 'TABLE'
    return str(_row_value(rows[0], 'value')).upper()


def test_default_then_promotes_copy(conversion_environment):
    """A later invocation validates and promotes the default v3 companion."""
    adapter, database, schema, config_dir, runtime_dir = conversion_environment
    table = SnowflakeObjectName(database, schema, 'DEFAULT_COPY')
    _create_native_table(adapter, table)
    tag_added = _add_supported_metadata(adapter, table)
    expected_rows = _rows(adapter, table)
    expected_metadata = _supported_metadata(adapter, table)
    current_role = adapter.query(
        'SELECT CURRENT_ROLE() AS "CURRENT_ROLE"'
    )[0]['CURRENT_ROLE']
    assert expected_metadata['owner'] == current_role
    assert expected_metadata['owner_role_type'] == 'ROLE'
    assert _ordered_primary_key(adapter, table) == ('TENANT_ID', 'ID')
    assert expected_metadata['table_comment'] == TABLE_COMMENT
    assert ('NOTE', COLUMN_COMMENT) in expected_metadata['column_comments']
    assert any(grant[0] == 'SELECT' for grant in expected_metadata['grants'])
    if tag_added:
        assert expected_metadata['tags'] == ((
            database,
            schema,
            'CONVERTER_TAG',
            'conversion-e2e',
        ),)

    _run_conversion_cli(config_dir, table)

    assert _formats(adapter, runtime_dir, table) == (
        TABLE_FORMAT_NATIVE,
        TABLE_FORMAT_MANAGED_ICEBERG_V3,
        TABLE_FORMAT_MISSING,
    )
    assert _rows(adapter, table.with_suffix('_ICEBERG')) == expected_rows
    assert (
        _merge_on_read_behavior(adapter, table.with_suffix('_ICEBERG'))
        == 'DISABLED'
    )
    assert _ordered_primary_key(adapter, table.with_suffix('_ICEBERG')) == (
        'TENANT_ID',
        'ID',
    )
    assert _supported_metadata(adapter, table.with_suffix('_ICEBERG')) == expected_metadata
    assert not list(Path(runtime_dir).glob('iceberg-recovery-*.json'))

    _run_conversion_cli(config_dir, table, EVENTUAL_ICEBERG)
    assert _formats(adapter, runtime_dir, table) == (
        TABLE_FORMAT_MANAGED_ICEBERG_V3,
        TABLE_FORMAT_MISSING,
        TABLE_FORMAT_NATIVE,
    )
    assert _rows(adapter, table) == expected_rows
    assert _merge_on_read_behavior(adapter, table) == 'DISABLED'
    assert _rows(adapter, table.with_suffix('_NATIVE')) == expected_rows
    assert _ordered_primary_key(adapter, table) == ('TENANT_ID', 'ID')
    assert _ordered_primary_key(adapter, table.with_suffix('_NATIVE')) == (
        'TENANT_ID',
        'ID',
    )
    assert _supported_metadata(adapter, table) == expected_metadata
    assert _supported_metadata(adapter, table.with_suffix('_NATIVE')) == expected_metadata
    assert not list(Path(runtime_dir).glob('iceberg-recovery-*.json'))


def test_conversion_truncates_temporal_values_and_preserves_native_backup(conversion_environment):
    """Real Snowflake values lose only their sub-microsecond digits during conversion."""
    adapter, database, schema, config_dir, runtime_dir = conversion_environment
    table = SnowflakeObjectName(database, schema, 'TEMPORAL_PRECISION')
    adapter.query("ALTER SESSION SET TIMEZONE = 'UTC'")
    adapter.query(
        f'CREATE TABLE {table.quoted} ('
        '"ID" NUMBER(19,0), "T" TIME(9), "N" TIMESTAMP_NTZ(9), "L" TIMESTAMP_LTZ(9))'
    )
    for row_id, value in enumerate(('23:59:59.999999999', '00:00:00.000000001', '12:34:56.123456000'), 1):
        adapter.query(
            f'INSERT INTO {table.quoted} SELECT %(id)s, TO_TIME(%(time)s), '
            'TO_TIMESTAMP_NTZ(%(timestamp)s), TO_TIMESTAMP_LTZ(%(timestamp)s)',
            {'id': row_id, 'time': value, 'timestamp': f'2024-01-02 {value}'},
        )
    adapter.query(f'INSERT INTO {table.quoted} VALUES (4, NULL, NULL, NULL)')

    def formatted_rows(target):
        return adapter.query(
            'SELECT "ID", TO_CHAR("T", \'HH24:MI:SS.FF9\') AS "T", '
            'TO_CHAR("N", \'YYYY-MM-DD HH24:MI:SS.FF9\') AS "N", '
            'TO_CHAR("L", \'YYYY-MM-DD HH24:MI:SS.FF9\') AS "L" '
            f'FROM {target.quoted} ORDER BY "ID"'
        )

    original = formatted_rows(table)
    assert [row['T'] for row in original] == [
        '23:59:59.999999999', '00:00:00.000000001', '12:34:56.123456000', None,
    ]
    expected = []
    for row in original:
        expected.append({
            key: value[:-3] + '000' if key != 'ID' and value is not None else value
            for key, value in row.items()
        })

    _run_conversion_cli(config_dir, table, EVENTUAL_ICEBERG)

    assert formatted_rows(table) == expected
    assert formatted_rows(table.with_suffix('_NATIVE')) == original
    assert original != expected
    assert _formats(adapter, runtime_dir, table) == (
        TABLE_FORMAT_MANAGED_ICEBERG_V3, TABLE_FORMAT_MISSING, TABLE_FORMAT_NATIVE,
    )
    assert not list(Path(runtime_dir).glob('iceberg-recovery-*.json'))


def test_cutover_recovers_lost_response(conversion_environment):
    """A committed promotion is reconciled and retains the native backup."""
    adapter, database, schema, config_dir, runtime_dir = conversion_environment
    table = SnowflakeObjectName(database, schema, 'PROMOTED_COPY')
    _create_native_table(adapter, table)
    expected_rows = _rows(adapter, table)
    lossy_adapter = LostPromotionResponseAdapter(adapter)

    SnowflakeNativeToIcebergConverter(lossy_adapter, str(runtime_dir)).convert(
        table.quoted,
        eventual=EVENTUAL_ICEBERG,
        iceberg_version=3,
    )

    assert lossy_adapter.raised
    assert _formats(adapter, runtime_dir, table) == (
        TABLE_FORMAT_MANAGED_ICEBERG_V3,
        TABLE_FORMAT_MISSING,
        TABLE_FORMAT_NATIVE,
    )
    assert _rows(adapter, table) == expected_rows
    assert _rows(adapter, table.with_suffix('_NATIVE')) == expected_rows
    assert not list(Path(runtime_dir).glob('iceberg-recovery-*.json'))

    _run_conversion_cli(config_dir, table, EVENTUAL_ICEBERG)
    assert _rows(adapter, table) == expected_rows


def _register_json_catalog(config_dir, runtime_dir, table, schema_property):
    """Register a selected writer so the CLI must apply its catalog contract."""
    tap_id = 'mariadb-json-converter'
    config_path = config_dir / 'config.json'
    config = json.loads(config_path.read_text(encoding='utf-8'))
    config['targets'][0]['taps'] = [{
        'id': tap_id, 'name': tap_id, 'type': 'tap-mysql', 'status': 'ready',
    }]
    config_path.write_text(json.dumps(config), encoding='utf-8')
    tap_dir = runtime_dir / tap_id
    tap_dir.mkdir(exist_ok=True)
    (tap_dir / 'config.json').write_text('{}', encoding='utf-8')
    (tap_dir / 'inheritable_config.json').write_text(json.dumps({
        'schema_mapping': {'mariadb': {'target_schema': table.schema}},
        'data_flattening_max_level': 0,
    }), encoding='utf-8')
    (tap_dir / 'properties.json').write_text(json.dumps({'streams': [{
        'tap_stream_id': f'mariadb-{table.table.lower()}',
        'schema': {'properties': {'payload': schema_property}},
        'metadata': [{'breadcrumb': [], 'metadata': {
            'selected': True,
            'mariadb-json-discovery': {'version': 1, 'engine': 'mariadb'},
        }}],
    }]}), encoding='utf-8')


@pytest.mark.parametrize('physical_type', ['TEXT', 'VARIANT'])
def test_mariadb_json_catalog_conversion_preserves_physical_type(conversion_environment, physical_type):
    """Marked MariaDB JSON retains bytes or roots; generic class mismatches refuse."""
    adapter, database, schema, config_dir, runtime_dir = conversion_environment
    table = SnowflakeObjectName(database, schema, f'JSON_{physical_type}')
    adapter.query(f'CREATE TABLE {table.quoted} ("ID" NUMBER, "PAYLOAD" {physical_type}, PRIMARY KEY ("ID"))')
    expression = '%(payload)s' if physical_type == 'TEXT' else 'PARSE_JSON(%(payload)s)'
    for row_id, value in enumerate(JSON_VALUES, 1):
        adapter.query(
            f'INSERT INTO {table.quoted} SELECT %(id)s, {expression}',
            {'id': row_id, 'payload': value},
        )

    def contents(target):
        if physical_type == 'TEXT':
            projection = '"PAYLOAD" IS NULL AS "SQL_NULL", HEX_ENCODE("PAYLOAD") AS "PAYLOAD_VALUE"'
        else:
            projection = ('"PAYLOAD" IS NULL AS "SQL_NULL", TYPEOF("PAYLOAD") AS "JSON_TYPE", '
                          'TO_JSON("PAYLOAD") AS "PAYLOAD_VALUE"')
        return adapter.query(f'SELECT "ID", {projection} FROM {target.quoted} ORDER BY "ID"')

    expected = contents(table)
    generic = {'type': ['null', 'object']} if physical_type == 'TEXT' else {'type': ['null', 'string']}
    _register_json_catalog(config_dir, runtime_dir, table, generic)
    rejected = _run_conversion_cli(config_dir, table, success=False)
    assert 'tap catalog maps it to' in rejected.stdout + rejected.stderr
    assert _formats(adapter, runtime_dir, table) == (TABLE_FORMAT_NATIVE, TABLE_FORMAT_MISSING, TABLE_FORMAT_MISSING)
    assert contents(table) == expected

    _register_json_catalog(config_dir, runtime_dir, table, {
        'type': ['null', 'object', 'array', 'string', 'number', 'boolean'],
        'format': 'mariadb-json',
    })
    _run_conversion_cli(config_dir, table, EVENTUAL_ICEBERG)
    assert contents(table) == expected
    assert contents(table.with_suffix('_NATIVE')) == expected
    columns = adapter.query(
        'SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS '
        'WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s AND COLUMN_NAME = \'PAYLOAD\'',
        {'schema': schema, 'table': table.table},
    )
    assert columns == [{'DATA_TYPE': physical_type}]
    assert _formats(adapter, runtime_dir, table) == (
        TABLE_FORMAT_MANAGED_ICEBERG_V3, TABLE_FORMAT_MISSING, TABLE_FORMAT_NATIVE,
    )
