"""MariaDB JSON root and legacy serialization checks shared by native and v3."""

import hashlib
import json
from pathlib import Path

import yaml

from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake import CONFIG_DIR, TEST_PROJECTS_DIR_PATH
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB


JSON_VALUES = (
    '{ "nested": [null, {}, "雪"], "number": 1e2 }',
    '[ 0, false, null, {} ]',
    r'"line\nliteral\\n\u96ea"',
    '  1.2300e+02  ',
    ' true ',
    ' false ',
    ' null ',
    None,
)
JSON_TYPES = ('OBJECT', 'ARRAY', 'VARCHAR', 'DOUBLE', 'BOOLEAN', 'BOOLEAN', 'NULL_VALUE', None)
PLAIN_TEXT = 'not JSON: whitespace  \\n 雪\n'
JSON_COLUMNS = ('payload', 'payload_hash', 'payload_hidden')
LEGACY_TRANSFORMATIONS = [
    {'column': 'payload_hash', 'type': 'HASH'},
    {'column': 'payload_hidden', 'type': 'MASK-HIDDEN'},
]


def prepare_mariadb_json_table(test_case: TapMariaDB, tap_yaml_name: str) -> None:
    """Prepare one selected table and restore the rendered tap YAML on cleanup."""
    tap_yaml = Path(TEST_PROJECTS_DIR_PATH) / tap_yaml_name
    original = tap_yaml.read_text(encoding='utf-8')
    test_case.addCleanup(tap_yaml.write_text, original, encoding='utf-8')
    config = yaml.safe_load(original)
    config['schemas'][0]['tables'] = [{'table_name': 'multiline_values', 'replication_method': 'LOG_BASED'}]
    config['version_legacy_float_columns'] = True
    tap_yaml.write_text(yaml.safe_dump(config, sort_keys=False), encoding='utf-8')

    source = test_case.e2e_env.run_query_tap_mysql
    test_case.addCleanup(source, 'DROP TABLE IF EXISTS multiline_values')
    source('DROP TABLE IF EXISTS multiline_values')
    source(
        'CREATE TABLE multiline_values ('
        'id INTEGER NOT NULL PRIMARY KEY, payload JSON NULL, payload_hash JSON NULL, '
        'payload_hidden JSON NULL, value_text LONGTEXT NULL) '
        'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    )
    for row_id, value in enumerate(JSON_VALUES, 1):
        source(
            'INSERT INTO multiline_values (id, payload, payload_hash, payload_hidden, value_text) '
            'VALUES (%s, %s, %s, %s, %s)',
            (row_id, value, value, value, PLAIN_TEXT),
        )


def _assert_discovered_json(test_case: TapMariaDB) -> None:
    tap_dir = Path(CONFIG_DIR) / test_case.target_id / test_case.tap_id
    config = json.loads((tap_dir / 'config.json').read_text(encoding='utf-8'))
    test_case.assertEqual(config['json_target'], 'snowflake')
    inherited = json.loads((tap_dir / 'inheritable_config.json').read_text(encoding='utf-8'))
    test_case.assertIs(inherited['version_legacy_float_columns'], True)
    catalog = json.loads((tap_dir / 'properties.json').read_text(encoding='utf-8'))
    stream = next(stream for stream in catalog['streams'] if stream['stream'] == 'multiline_values')
    for column in JSON_COLUMNS:
        test_case.assertEqual(stream['schema']['properties'][column]['format'], 'mariadb-json')
    test_case.assertNotIn('format', stream['schema']['properties']['value_text'])
    root = next(entry['metadata'] for entry in stream['metadata'] if entry['breadcrumb'] == [])
    test_case.assertEqual(root['mariadb-json-discovery'], {'version': 1, 'engine': 'mariadb'})


def _assert_json_columns(test_case: TapMariaDB, payload_type: str) -> None:
    rows = test_case.e2e_env.run_query_target_snowflake(
        'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS '
        f"WHERE TABLE_SCHEMA = '{test_case.target_schema}' AND TABLE_NAME = 'MULTILINE_VALUES' "
        "AND (COLUMN_NAME LIKE 'PAYLOAD%' OR COLUMN_NAME = 'VALUE_TEXT') ORDER BY COLUMN_NAME"
    )
    test_case.assertEqual(rows, [
        *((column.upper(), payload_type) for column in JSON_COLUMNS), ('VALUE_TEXT', 'TEXT'),
    ])


def _assert_variant_values(test_case: TapMariaDB, *, reverse: bool = False) -> None:
    _assert_json_columns(test_case, 'VARIANT')
    projection = ', '.join(
        f'TYPEOF("{column.upper()}"), TO_JSON("{column.upper()}"), "{column.upper()}" IS NULL, '
        f'IS_NULL_VALUE("{column.upper()}")'
        for column in JSON_COLUMNS
    )
    rows = test_case.e2e_env.run_query_target_snowflake(
        f'SELECT "ID", {projection}, HEX_ENCODE("VALUE_TEXT") '
        f'FROM "{test_case.target_schema}"."MULTILINE_VALUES" ORDER BY "ID"'
    )
    test_case.assertEqual(len(rows), len(JSON_VALUES))
    expected_values = tuple(zip(JSON_VALUES, JSON_TYPES))
    if reverse:
        expected_values = tuple(reversed(expected_values))
    for row, (expected, expected_type) in zip(rows, expected_values):
        for index, column in enumerate(JSON_COLUMNS):
            actual_type, serialized, sql_null, json_null = row[1 + 4 * index:5 + 4 * index]
            test_case.assertEqual(actual_type, expected_type, column)
            test_case.assertEqual(None if serialized is None else json.loads(serialized),
                                  None if expected is None else json.loads(expected), column)
            test_case.assertEqual(sql_null, expected is None, column)
            test_case.assertEqual(json_null, None if expected is None else expected_type == 'NULL_VALUE', column)
        test_case.assertEqual(row[-1], PLAIN_TEXT.encode('utf-8').hex().upper())


def _assert_raw_values(test_case: TapMariaDB, values: tuple[str | None, ...]) -> None:
    _assert_json_columns(test_case, 'TEXT')
    rows = test_case.e2e_env.run_query_target_snowflake(
        'SELECT "ID", "PAYLOAD" IS NULL, HEX_ENCODE("PAYLOAD") '
        f'FROM "{test_case.target_schema}"."MULTILINE_VALUES" ORDER BY "ID"'
    )
    test_case.assertEqual(rows, [
        (row_id, value is None, None if value is None else value.encode('utf-8').hex().upper())
        for row_id, value in enumerate(values, 1)
    ])


def _assert_transformed_values(test_case: TapMariaDB, values: tuple[str | None, ...]) -> None:
    rows = test_case.e2e_env.run_query_target_snowflake(
        'SELECT "ID", "PAYLOAD_HASH", "PAYLOAD_HIDDEN" '
        f'FROM "{test_case.target_schema}"."MULTILINE_VALUES" ORDER BY "ID"'
    )
    test_case.assertEqual(rows, [
        (row_id, None if value is None else hashlib.sha256(value.encode('utf-8')).hexdigest(), 'hidden')
        for row_id, value in enumerate(values, 1)
    ])


def _set_transformations(tap_yaml_name: str, transformations: list[dict[str, str]]) -> None:
    tap_yaml = Path(TEST_PROJECTS_DIR_PATH) / tap_yaml_name
    config = yaml.safe_load(tap_yaml.read_text(encoding='utf-8'))
    config['schemas'][0]['tables'][0]['transformations'] = transformations
    tap_yaml.write_text(yaml.safe_dump(config, sort_keys=False), encoding='utf-8')


def _restore_legacy_catalog(test_case: TapMariaDB) -> None:
    """Reproduce the saved string schemas and rules from before JSON alias discovery."""
    tap_dir = Path(CONFIG_DIR) / test_case.target_id / test_case.tap_id
    properties_path = tap_dir / 'properties.json'
    catalog = json.loads(properties_path.read_text(encoding='utf-8'))
    stream = next(stream for stream in catalog['streams'] if stream['stream'] == 'multiline_values')
    for column in JSON_COLUMNS:
        stream['schema']['properties'][column] = {'type': ['null', 'string']}
    root = next(entry['metadata'] for entry in stream['metadata'] if entry['breadcrumb'] == [])
    root.pop('mariadb-json-discovery')
    properties_path.write_text(json.dumps(catalog), encoding='utf-8')
    transformations = [
        {'tap_stream_name': f'{test_case.source_db}-multiline_values', 'field_id': rule['column'], 'type': rule['type']}
        for rule in LEGACY_TRANSFORMATIONS
    ]
    (tap_dir / 'transformation.json').write_text(json.dumps({'transformations': transformations}), encoding='utf-8')


def exercise_mariadb_json_replication(test_case: TapMariaDB, *, iceberg: bool, tap_yaml_name: str) -> None:
    """Prove new VARIANT, catalog upgrades with retained TEXT rules, and bounded repair."""
    _assert_discovered_json(test_case)
    stream_id = f'{test_case.source_db}-multiline_values'
    assertions.assert_run_tap_success(
        test_case.tap_id, test_case.target_id, ['fastsync', 'singer'],
        expected_state_streams={'fastsync': {stream_id: True}},
    )
    _assert_variant_values(test_case)
    table = f'"{test_case.target_schema}"."MULTILINE_VALUES"'
    formats = test_case.e2e_env.run_query_target_snowflake(
        'SELECT IS_ICEBERG FROM INFORMATION_SCHEMA.TABLES '
        f"WHERE TABLE_SCHEMA = '{test_case.target_schema}' AND TABLE_NAME = 'MULTILINE_VALUES'"
    )
    test_case.assertEqual(formats, [('YES' if iceberg else 'NO',)])
    if iceberg:
        version = test_case.e2e_env.run_query_target_snowflake(
            f"SHOW PARAMETERS LIKE 'ICEBERG_VERSION' IN TABLE {table}"
        )
        test_case.assertEqual(str(version[0][1]), '3')

    # Switch every root through Singer, so a successful initial FullSync is insufficient.
    source = test_case.e2e_env.run_query_tap_mysql
    for row_id, value in enumerate(reversed(JSON_VALUES), 1):
        source(
            'UPDATE multiline_values SET payload = %s, payload_hash = %s, payload_hidden = %s WHERE id = %s',
            (value, value, value, row_id),
        )
    assertions.assert_run_tap_success(test_case.tap_id, test_case.target_id, ['singer'])
    _assert_variant_values(test_case, reverse=True)

    # Reproduce the physical layout of an established pre-upgrade table without resetting state.
    target = test_case.e2e_env.run_query_target_snowflake
    alter_table = 'ALTER ICEBERG TABLE' if iceberg else 'ALTER TABLE'
    for name in JSON_COLUMNS:
        column = name.upper()
        target(f'{alter_table} {table} ADD COLUMN "{column}_TEXT" VARCHAR(134217728)')
        target(f'UPDATE {table} SET "{column}_TEXT" = TO_JSON("{column}")')
        target(f'{alter_table} {table} DROP COLUMN "{column}"')
        target(f'{alter_table} {table} RENAME COLUMN "{column}_TEXT" TO "{column}"')
    target(f'UPDATE {table} SET "PAYLOAD_HASH" = SHA2("PAYLOAD_HASH", 256), "PAYLOAD_HIDDEN" = \'hidden\'')
    _set_transformations(tap_yaml_name, LEGACY_TRANSFORMATIONS)
    _restore_legacy_catalog(test_case)
    test_case.check_import_config()
    _assert_discovered_json(test_case)
    raw_values = tuple(None if value is None else '\n ' + value + '\t ' for value in JSON_VALUES)
    for row_id, value in enumerate(raw_values, 1):
        source(
            'UPDATE multiline_values SET payload = %s, payload_hash = %s, payload_hidden = %s WHERE id = %s',
            (value, value, value, row_id),
        )
    assertions.assert_run_tap_success(test_case.tap_id, test_case.target_id, ['singer'])
    _assert_raw_values(test_case, raw_values)
    _assert_transformed_values(test_case, raw_values)

    partial_values = tuple(None if value is None else ' \t' + value + '\n' for value in JSON_VALUES)
    for row_id, value in enumerate(partial_values, 1):
        source(
            'UPDATE multiline_values SET payload = %s, payload_hash = %s, payload_hidden = %s WHERE id = %s',
            (value, value, value, row_id),
        )
    assertions.assert_partial_sync_table_success(
        {'env': test_case.e2e_env, 'tap': test_case.tap_id, 'tap_type': 'mysql',
         'target': test_case.target_id, 'source_db': test_case.source_db,
         'table': 'multiline_values', 'column': 'id'},
        start_value=2, end_value=len(JSON_VALUES),
    )
    _assert_raw_values(test_case, (raw_values[0], *partial_values[1:]))
    _assert_transformed_values(test_case, (raw_values[0], *partial_values[1:]))

    _set_transformations(tap_yaml_name, [])
    test_case.check_import_config()
    assertions.assert_resync_tables_success(
        test_case.tap_id, test_case.target_id,
        tables=f'{test_case.source_db}.multiline_values',
        expected_state_streams={'fastsync': {stream_id: True}},
    )
    _assert_variant_values(test_case)
