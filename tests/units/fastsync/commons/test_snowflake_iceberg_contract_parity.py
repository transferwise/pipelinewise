"""Executable cross-layer parity guard for managed Snowflake Iceberg."""

import json
from pathlib import Path
from types import MappingProxyType
from unittest.mock import MagicMock

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_versions as versions
from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    IcebergColumn,
    IcebergTableSpec,
    PUBLICATION_ADDITIVE_OVERWRITE,
    SnowflakeObjectName,
)
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    SnowflakeIcebergPublisher,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    TableCompatibilityError,
    TableFormatDiscoveryError,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    FakeSnowflake,
    make_attempt,
    v3_snapshot,
)


CONTRACT_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / 'resources'
    / 'snowflake_managed_iceberg_contract.json'
)


def _contract_fixture():
    with CONTRACT_FIXTURE.open(encoding='utf-8') as contract_file:
        return json.load(contract_file)


def _v3_behavior_cases():
    return _contract_fixture()['behavior_cases']['3']


def test_core_matches_contract():
    """Core exposes exactly the shared supported-version declaration."""
    expected_contract = _contract_fixture()['declarative_contract']

    assert versions.repository_contract() == expected_contract


def test_core_runs_logical_type_cases():
    """The core implementation executes every shared logical mapping vector."""
    spec = versions.MANAGED_ICEBERG_VERSION_SPECS[3]

    for case in _v3_behavior_cases()['logical_types']:
        assert spec.logical_to_physical_types[case['logical_type']] == case['declared_type']
        assert spec.physical_type_for_logical(
            case['logical_type']
        ) == case['canonical_type']


def test_core_runs_canonical_type_cases():
    """The core canonicalizer executes every shared accept/reject vector."""
    canonical_type = versions.MANAGED_ICEBERG_VERSION_SPECS[3].canonical_type

    for case in _v3_behavior_cases()['canonical_types']:
        if case['accepted']:
            assert canonical_type(case['input']) == case['expected']
        else:
            with pytest.raises(ValueError):
                canonical_type(case['input'])


def test_core_runs_existing_column_cases():
    """The core metadata adapter executes every shared existing-column vector."""
    for case in _v3_behavior_cases()['existing_columns']:
        if case['accepted']:
            column = IcebergColumn.from_snowflake_row(case['row'], 3)
            assert {
                'name': column.name,
                'data_type': column.data_type,
                'nullable': column.nullable,
            } == case['expected']
        else:
            with pytest.raises((
                TableCompatibilityError,
                TableFormatDiscoveryError,
                TypeError,
                ValueError,
            )):
                IcebergColumn.from_snowflake_row(case['row'], 3)


def test_core_runs_parameter_cases():
    """The core copy-on-write validator executes every shared metadata vector."""
    spec = versions.MANAGED_ICEBERG_VERSION_SPECS[3]
    target = SnowflakeObjectName('DB', 'SCHEMA', 'TABLE')

    for case in _v3_behavior_cases()['table_parameters']:
        if case['accepted']:
            spec.validate_parameter_rows(case['rows'], target, spec)
        else:
            with pytest.raises((TableCompatibilityError, TableFormatDiscoveryError)):
                spec.validate_parameter_rows(case['rows'], target, spec)


def test_core_runs_version_metadata_cases(tmp_path):
    """Metadata versions never gain support through lossy integer coercion."""
    for case in _v3_behavior_cases()['version_metadata']:
        snowflake = FakeSnowflake(responses=[
            [{'name': 'TABLE', 'is_iceberg': 'Y'}],
            [{'name': 'TABLE', 'catalog_name': 'SNOWFLAKE'}],
            [{'key': 'ICEBERG_VERSION', 'value': case['value']}],
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'DISABLED',
                'level': 'TABLE',
            }],
        ])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        if case['accepted']:
            assert versions.parse_exact_integer_metadata(case['value']) == 3
            assert publisher.discover_table_format(
                'SCHEMA',
                'TABLE',
            ) == 'managed_iceberg_v3'
        else:
            with pytest.raises(ValueError):
                versions.parse_exact_integer_metadata(case['value'])
            with pytest.raises(TableFormatDiscoveryError):
                publisher.discover_table_format('SCHEMA', 'TABLE')


def test_core_emits_create_and_add_cases(tmp_path):
    """FastSync CREATE and ADD SQL execute the shared physical-type vectors."""
    for case in _v3_behavior_cases()['emitted_columns']:
        desired = IcebergTableSpec.from_fastsync(
            'DB',
            'SCHEMA',
            'TABLE',
            [case['fastsync_definition']],
            (),
            iceberg_version=3,
        )
        source_column = desired.columns[0]

        assert source_column.data_type == case['expected_physical_type']
        assert source_column.definition == case['expected_definition']

        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        create_sql = publisher.publication_service._ctas_sql(  # pylint: disable=protected-access
            desired,
            desired.name.staging_name('fixture-load'),
            iceberg_version=3,
        )
        assert case['expected_definition'] in create_sql

        existing = IcebergTableSpec(
            desired.name,
            desired.columns[1:],
            (),
        )
        snapshot = v3_snapshot(existing)
        publisher.inspect_table = MagicMock(return_value=snapshot)
        attempt = make_attempt(
            desired,
            method=PUBLICATION_ADDITIVE_OVERWRITE,
            snapshot=snapshot,
        )
        plan = publisher.plan_full_sync(attempt, desired)

        assert plan.preparation_statements == (
            f'ALTER ICEBERG TABLE {desired.name.quoted} '
            f'ADD COLUMN {case["expected_definition"]}',
        )


def test_core_registries_are_immutable():
    """Callers cannot enable partial strategies by mutating one public map."""
    with pytest.raises(TypeError):
        versions.MANAGED_ICEBERG_VERSION_SPECS[4] = versions.MANAGED_ICEBERG_V3_SPEC
    with pytest.raises(TypeError):
        versions.SUPPORTED_MANAGED_ICEBERG_TABLE_FORMATS[4] = 'managed_iceberg_v4'
    with pytest.raises(TypeError):
        versions.MANAGED_ICEBERG_TABLE_OPTIONS_BY_VERSION[4] = ''


def test_core_registry_rejects_bad_specs():
    """Registration accepts only complete, uniquely versioned specifications."""
    with pytest.raises(ValueError, match='Duplicate'):
        versions.managed_iceberg_version_registry(
            versions.MANAGED_ICEBERG_V3_SPEC,
            versions.MANAGED_ICEBERG_V3_SPEC,
        )
    with pytest.raises(ValueError, match='complete specifications'):
        versions.managed_iceberg_version_registry(object())


def test_core_lookup_checks_key_identity(monkeypatch):
    """A registry key alone cannot claim support for a different strategy."""
    monkeypatch.setattr(
        versions,
        'MANAGED_ICEBERG_VERSION_SPECS',
        MappingProxyType({4: versions.MANAGED_ICEBERG_V3_SPEC}),
    )

    assert versions.is_supported_managed_iceberg_version(4) is False
    with pytest.raises(ValueError, match='Unsupported managed Iceberg table format'):
        versions.managed_iceberg_spec_for_table_format('managed_iceberg_v3')
