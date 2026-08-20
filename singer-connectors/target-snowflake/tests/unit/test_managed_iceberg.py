import csv as csv_module
import json
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from unittest.mock import Mock

import pytest

from target_snowflake import db_sync, managed_iceberg
from target_snowflake.exceptions import (
    TableFormatDiscoveryException,
    TableFormatMismatchException,
)
from target_snowflake.file_formats import csv as csv_file_format


REPOSITORY_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / 'tests'
    / 'resources'
    / 'snowflake_managed_iceberg_contract.json'
)


def _repository_fixture():
    with REPOSITORY_CONTRACT_PATH.open(encoding='utf-8') as contract_file:
        return json.load(contract_file)


def _v3_behavior_cases():
    return _repository_fixture()['behavior_cases']['3']


def test_v3_registry_entry_is_a_complete_contract():
    contract = managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[3]

    assert contract.version == 3
    assert contract.table_format == managed_iceberg.TABLE_FORMAT_MANAGED_ICEBERG_V3
    assert set(contract.column_types) == {
        'binary',
        'boolean',
        'date',
        'float',
        'number',
        'text',
        'time',
        'timestamp_ntz',
        'variant',
    }
    assert contract.table_option_semantics['ICEBERG_MERGE_ON_READ_BEHAVIOR'] == 'DISABLED'
    assert contract.copy_on_write_level == 'TABLE'
    assert callable(contract.canonical_type)
    assert callable(contract.canonical_existing_column)
    assert callable(contract.existing_table_validator)
    assert callable(contract.type_compatibility.mismatch_reason)


def test_contract_rejects_an_incomplete_type_mapping():
    with pytest.raises(
        ValueError,
        match='must map every logical column type exactly once',
    ):
        managed_iceberg.ManagedIcebergContract(
            version=4,
            table_format='managed_iceberg_v4',
            column_types={'text': 'varchar(134217728)'},
            table_option_semantics={
                'ICEBERG_MERGE_ON_READ_BEHAVIOR': 'DISABLED',
            },
            copy_on_write_level='TABLE',
            canonical_type=Mock(),
            canonical_existing_column=Mock(),
            existing_table_validator=Mock(),
            type_compatibility=managed_iceberg.ColumnTypeCompatibility(
                compatible_pairs=frozenset(),
                forbidden_pairs=frozenset(),
                mismatch_reason=Mock(),
            ),
        )


def test_contract_cannot_reuse_another_versions_format():
    v3_contract = managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[3]

    with pytest.raises(ValueError, match='must identify its exact version'):
        managed_iceberg.ManagedIcebergContract(
            version=4,
            table_format=v3_contract.table_format,
            column_types=v3_contract.column_types,
            table_option_semantics=v3_contract.table_option_semantics,
            copy_on_write_level=v3_contract.copy_on_write_level,
            canonical_type=v3_contract.canonical_type,
            canonical_existing_column=v3_contract.canonical_existing_column,
            existing_table_validator=v3_contract.existing_table_validator,
            type_compatibility=v3_contract.type_compatibility,
        )


def test_registry_and_derived_format_map_are_immutable():
    with pytest.raises(TypeError):
        managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[4] = (
            managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[3]
        )
    with pytest.raises(TypeError):
        managed_iceberg.SUPPORTED_MANAGED_ICEBERG_FORMATS[4] = 'managed_iceberg_v4'


def test_a_registry_key_alone_does_not_enable_a_future_version(monkeypatch):
    monkeypatch.setattr(
        managed_iceberg,
        'SUPPORTED_MANAGED_ICEBERG_CONTRACTS',
        MappingProxyType({4: managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[3]}),
    )

    assert managed_iceberg.is_supported_iceberg_version(4) is False
    with pytest.raises(ValueError, match='Unsupported managed Iceberg version'):
        managed_iceberg.get_managed_iceberg_contract(4)


def test_connector_contract_exactly_matches_repository_contract():
    expected_contract = _repository_fixture()['declarative_contract']

    assert managed_iceberg.repository_contract() == expected_contract


def test_contract_rejects_invalid_version_and_empty_mapping():
    contract = managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[3]

    with pytest.raises(ValueError, match='positive exact integer'):
        replace(contract, version=-1, table_format='managed_iceberg_v-1')

    empty_mapping = dict(contract.column_types)
    empty_mapping['text'] = ' '
    with pytest.raises(ValueError, match='non-empty strings'):
        replace(contract, column_types=empty_mapping)

    with pytest.raises(ValueError, match='must be executable'):
        replace(contract, canonical_type=lambda _data_type: None)


def test_registry_rejects_duplicate_or_incomplete_contracts():
    contract = managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[3]

    with pytest.raises(ValueError, match='Duplicate'):
        managed_iceberg.managed_iceberg_contract_registry(contract, contract)
    with pytest.raises(ValueError, match='complete contracts'):
        managed_iceberg.managed_iceberg_contract_registry(object())


def test_column_compatibility_copies_inputs_and_rejects_malformed_pairs():
    compatible_pairs = {('FLOAT', 'DOUBLE')}
    forbidden_pairs = {frozenset({'TEXT', 'VARIANT'})}
    compatibility = managed_iceberg.ColumnTypeCompatibility(
        compatible_pairs=compatible_pairs,
        forbidden_pairs=forbidden_pairs,
        mismatch_reason=Mock(),
    )
    compatible_pairs.add(('DATE', 'TIME'))
    forbidden_pairs.add(frozenset({'DATE', 'TIME'}))

    assert compatibility.compatible_pairs == frozenset({('FLOAT', 'DOUBLE')})
    assert compatibility.forbidden_pairs == frozenset({
        frozenset({'TEXT', 'VARIANT'}),
    })
    with pytest.raises(ValueError, match='uppercase type names'):
        managed_iceberg.ColumnTypeCompatibility(
            compatible_pairs={('float', 'DOUBLE')},
            forbidden_pairs=frozenset(),
            mismatch_reason=Mock(),
        )


def test_connector_executes_shared_logical_type_cases():
    contract = managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[3]

    for case in _v3_behavior_cases()['logical_types']:
        assert contract.column_types[case['logical_type']] == case['declared_type']
        assert managed_iceberg.column_type(
            case['schema'],
            is_iceberg_table=True,
            iceberg_version=3,
        ) == case['declared_type']
        assert contract.canonical_type(case['declared_type']) == case['canonical_type']


def test_connector_executes_shared_canonical_type_cases():
    canonical_type = managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[3].canonical_type

    for case in _v3_behavior_cases()['canonical_types']:
        if case['accepted']:
            assert canonical_type(case['input']) == case['expected']
        else:
            with pytest.raises(ValueError):
                canonical_type(case['input'])


def test_connector_executes_shared_existing_column_cases():
    canonical_column = (
        managed_iceberg.SUPPORTED_MANAGED_ICEBERG_CONTRACTS[3]
        .canonical_existing_column
    )

    for case in _v3_behavior_cases()['existing_columns']:
        if case['accepted']:
            name, data_type, nullable = canonical_column(case['row'], 'DB.SCHEMA.TABLE')
            assert {
                'name': name,
                'data_type': data_type,
                'nullable': nullable,
            } == case['expected']
        else:
            with pytest.raises((
                TableFormatDiscoveryException,
                TableFormatMismatchException,
                TypeError,
                ValueError,
            )):
                canonical_column(case['row'], 'DB.SCHEMA.TABLE')


def test_connector_executes_shared_table_parameter_cases():
    for case in _v3_behavior_cases()['table_parameters']:
        if case['accepted']:
            managed_iceberg.validate_managed_iceberg_v3_copy_on_write(
                case['rows'],
                'DB.SCHEMA.TABLE',
            )
        else:
            with pytest.raises((
                TableFormatDiscoveryException,
                TableFormatMismatchException,
            )):
                managed_iceberg.validate_managed_iceberg_v3_copy_on_write(
                    case['rows'],
                    'DB.SCHEMA.TABLE',
                )


def test_connector_executes_shared_version_metadata_cases():
    for case in _v3_behavior_cases()['version_metadata']:
        rows = [{'key': 'ICEBERG_VERSION', 'value': case['value']}]
        if case['accepted']:
            assert managed_iceberg.managed_iceberg_contract(
                rows,
                'DB.SCHEMA.TABLE',
            ).version == 3
        else:
            with pytest.raises(TableFormatDiscoveryException):
                managed_iceberg.managed_iceberg_contract(
                    rows,
                    'DB.SCHEMA.TABLE',
                )


def test_connector_emits_shared_create_and_add_column_cases():
    for case in _v3_behavior_cases()['emitted_columns']:
        definition = managed_iceberg.column_clause(
            case['column_name'],
            case['schema'],
            is_iceberg_table=True,
            iceberg_version=3,
        )

        assert definition.upper() == case['expected_definition']
        assert managed_iceberg.column_type(
            case['schema'],
            is_iceberg_table=True,
            iceberg_version=3,
        ).upper() == case['expected_physical_type']

        create_sql = managed_iceberg.create_iceberg_table_query(
            'DB.SCHEMA.TABLE',
            [definition],
            (),
            3,
        )
        assert case['expected_definition'] in create_sql.upper()

        change_plan = managed_iceberg.plan_column_changes(
            {case['column_name']: case['schema']},
            {},
            is_iceberg_table=True,
            iceberg_version=3,
        )
        assert tuple(value.upper() for value in change_plan.additions) == (
            case['expected_definition'],
        )


def test_connector_executes_shared_mariadb_json_transport_case():
    case = _v3_behavior_cases()['mariadb_json_transport']
    schema = case['schema']

    assert managed_iceberg.column_type(
        schema,
        is_iceberg_table=True,
        iceberg_version=3,
    ).upper() == case['expected_physical_type']
    assert db_sync.column_trans(schema) == case['expected_transform']

    record = {
        f'root_{index}': value
        for index, value in enumerate(case['serialized_roots'])
    }
    record['sql_null'] = None
    columns = {name: schema for name in record}
    fields = next(csv_module.reader(
        [csv_file_format.record_to_csv_line(record, columns)],
        escapechar='\\',
    ))

    assert fields[:-1] == case['serialized_roots']
    assert [json.loads(value) for value in fields[:-1]] == (
        case['expected_decoded_roots']
    )
    assert fields[-1] == ''


def test_sql_string_literal_escapes_backslashes_before_quotes():
    for value, expected in (
        ('TABLE\\ARCHIVE', "'TABLE\\\\ARCHIVE'"),
        ('TABLE' + '\\', "'TABLE\\\\'"),
        ("TABLE\\'ARCHIVE", "'TABLE\\\\''ARCHIVE'"),
    ):
        assert managed_iceberg.sql_string_literal(value) == expected


def test_mismatch_reason_requires_a_supported_version():
    with pytest.raises(ValueError, match='requires integer version 3'):
        managed_iceberg.iceberg_text_variant_mismatch_reason('TEXT', None)


def test_v3_contract_drives_type_mapping_and_table_options():
    assert managed_iceberg.column_type(
        {'type': ['string']},
        is_iceberg_table=True,
        iceberg_version=3,
    ) == 'varchar(134217728)'
    assert managed_iceberg.column_type(
        {'type': ['number']},
        is_iceberg_table=True,
        iceberg_version=3,
    ) == 'double'

    ddl = managed_iceberg.create_iceberg_table_query(
        'SCHEMA."TABLE"',
        ['"ID" number(38,0)'],
        ['PRIMARY KEY("ID")'],
        3,
    )

    assert ddl == (
        'CREATE ICEBERG TABLE IF NOT EXISTS SCHEMA."TABLE" '
        '("ID" number(38,0), PRIMARY KEY("ID")) '
        "CATALOG='SNOWFLAKE' ICEBERG_VERSION=3 "
        'DATA_RETENTION_TIME_IN_DAYS=1 '
        "TARGET_FILE_SIZE='16MB' ENABLE_DATA_COMPACTION=TRUE "
        "ICEBERG_MERGE_ON_READ_BEHAVIOR='DISABLED'"
    )


def test_column_change_plan_is_pure_and_uses_v3_compatibility_rules():
    flatten_schema = {
        'id': {'type': ['integer']},
        'name': {'type': ['string']},
        'metric': {'type': ['number']},
        'created_at': {'type': ['string'], 'format': 'date-time'},
        'payload': {'type': ['object']},
        'new_text': {'type': ['string']},
    }
    existing_types = {
        'ID': 'NUMBER',
        'NAME': 'TEXT',
        'METRIC': 'FLOAT',
        'CREATED_AT': 'TIMESTAMP_TZ',
        'PAYLOAD': 'NUMBER',
    }

    plan = managed_iceberg.plan_column_changes(
        flatten_schema,
        existing_types,
        is_iceberg_table=True,
        iceberg_version=3,
    )

    assert plan.additions == ('"NEW_TEXT" varchar(134217728)',)
    assert plan.replacements == ((
        '"PAYLOAD"',
        '"PAYLOAD" variant',
    ),)
    assert existing_types == {
        'ID': 'NUMBER',
        'NAME': 'TEXT',
        'METRIC': 'FLOAT',
        'CREATED_AT': 'TIMESTAMP_TZ',
        'PAYLOAD': 'NUMBER',
    }


@pytest.mark.parametrize(
    ('current_type', 'schema', 'message'),
    [
        ('TEXT', {'type': ['object']}, 'mapping requires VARIANT'),
        ('VARIANT', {'type': ['string']}, 'requires VARCHAR\\(134217728\\)'),
    ],
)
def test_column_change_plan_rejects_v3_text_variant_transitions(
    current_type,
    schema,
    message,
):
    with pytest.raises(TableFormatMismatchException, match=message):
        managed_iceberg.plan_column_changes(
            {'payload': schema},
            {'PAYLOAD': current_type},
            is_iceberg_table=True,
            iceberg_version=3,
        )
