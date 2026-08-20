"""Tests for the managed-Iceberg v3 copy-on-write contract."""

from unittest.mock import MagicMock

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    IcebergColumn,
    IcebergTableSpec,
    PUBLICATION_MISSING_CTAS,
    PUBLICATION_PARTIAL_BOOTSTRAP_CTAS,
    PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
    PUBLICATION_REPLACEMENT_CTAS,
    SnowflakeIcebergPublisher,
    SnowflakeTableMetadata,
    TableCompatibilityError,
    TableFormatDiscoveryError,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    EVENTUAL_ICEBERG,
    NativeToIcebergConversionError,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    RECOVERY_IDENTITY,
    FakeSnowflake,
    assert_managed_v3_copy_on_write_ddl,
    make_attempt,
    missing_snapshot,
    v3_snapshot,
)
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import (
    FakeSnowflake as ConverterFakeSnowflake,
    _converter,
    _manifest_files,
)


@pytest.mark.parametrize(
    ('parameter_rows', 'error_type', 'message'),
    (
        (
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'AUTO',
                'level': 'DEFAULT',
            }],
            TableCompatibilityError,
            'must set ICEBERG_MERGE_ON_READ_BEHAVIOR',
        ),
        (
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'ENABLED',
                'level': 'TABLE',
            }],
            TableCompatibilityError,
            'must set ICEBERG_MERGE_ON_READ_BEHAVIOR',
        ),
        (
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'DISABLED',
                'level': 'ACCOUNT',
            }],
            TableCompatibilityError,
            'explicitly at TABLE level',
        ),
        ([], TableFormatDiscoveryError, 'exactly one'),
        (
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'DISABLED',
            }],
            TableFormatDiscoveryError,
            'malformed',
        ),
        (
            [
                {
                    'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                    'value': 'DISABLED',
                    'level': 'TABLE',
                },
                {
                    'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                    'value': 'DISABLED',
                    'level': 'TABLE',
                },
            ],
            TableFormatDiscoveryError,
            'exactly one',
        ),
        (
            RuntimeError('parameter lookup failed'),
            TableFormatDiscoveryError,
            'could not read ICEBERG_MERGE_ON_READ_BEHAVIOR',
        ),
    ),
)
def test_discovery_rejects_unsafe_parameter(
    tmp_path,
    parameter_rows,
    error_type,
    message,
):
    """Managed-v3 discovery fails closed unless copy-on-write is explicit."""
    snowflake = FakeSnowflake([
        [{'name': 'TABLE', 'is_iceberg': True}],
        [{'name': 'TABLE', 'catalog_name': 'SNOWFLAKE'}],
        [{'key': 'ICEBERG_VERSION', 'value': 3}],
        parameter_rows,
    ])
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))

    with pytest.raises(error_type, match=message):
        publisher.discover_table_format('SCHEMA', 'TABLE')

    assert all(query.startswith('SHOW ') for query, _, _ in snowflake.queries)


def test_prepare_rejects_inherited_value(tmp_path, spec):
    """Publication state and target remain untouched after an unsafe preflight."""
    table_row = {'name': spec.name.table, 'is_iceberg': True, 'id': 'target-id'}
    snowflake = FakeSnowflake([
        [table_row],
        [table_row],
        [{'name': spec.name.table, 'catalog_name': 'SNOWFLAKE'}],
        [{'key': 'ICEBERG_VERSION', 'value': 3}],
        [{
            'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
            'value': 'AUTO',
            'level': 'DEFAULT',
        }],
    ])
    publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))

    with pytest.raises(TableCompatibilityError, match='does not alter'):
        publisher.prepare_full_sync(
            spec,
            {'lsn': '1/2'},
            recovery_identity=RECOVERY_IDENTITY,
        )

    assert all(query.startswith('SHOW ') for query, _, _ in snowflake.queries)
    assert not list(tmp_path.glob('iceberg-recovery-*.json'))


def _full_replacement_snapshot(spec):
    columns = tuple(
        IcebergColumn('LEGACY_PAYLOAD', column.data_type, column.nullable)
        if column.name == 'PAYLOAD'
        else column
        for column in spec.columns
    )
    return v3_snapshot(IcebergTableSpec(spec.name, columns, spec.primary_key))


def _ctas_statement(tmp_path, spec, method):
    if method == PUBLICATION_MISSING_CTAS:
        kind = 'full'
        snapshot = missing_snapshot()
        context = {}
    elif method == PUBLICATION_REPLACEMENT_CTAS:
        kind = 'full'
        snapshot = _full_replacement_snapshot(spec)
        context = {'replacement_metadata': SnowflakeTableMetadata().as_dict()}
    elif method == PUBLICATION_PARTIAL_BOOTSTRAP_CTAS:
        kind = 'partial'
        snapshot = missing_snapshot()
        context = {'drop_target': False}
    else:
        kind = 'partial'
        snapshot = v3_snapshot(spec)
        context = {
            'drop_target': True,
            'replacement_metadata': SnowflakeTableMetadata().as_dict(),
        }

    publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
    publisher.inspect_table = MagicMock(return_value=snapshot)
    publisher._verify_replacement_metadata = MagicMock()  # pylint: disable=protected-access
    attempt = make_attempt(
        spec,
        kind=kind,
        method=method,
        snapshot=snapshot,
        context=context,
    )
    plan = (
        publisher.plan_full_sync(attempt, spec)
        if kind == 'full'
        else publisher.plan_partial_sync(attempt, spec)
    )
    assert plan.method == method
    return plan.publication_statements[0]


@pytest.mark.parametrize(
    'method',
    (
        PUBLICATION_MISSING_CTAS,
        PUBLICATION_REPLACEMENT_CTAS,
        PUBLICATION_PARTIAL_BOOTSTRAP_CTAS,
        PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
    ),
)
def test_publisher_ctas_uses_copy_on_write(tmp_path, spec, method):
    """Every full and partial CTAS path uses the exact non-deprecated option."""
    statement = _ctas_statement(tmp_path, spec, method)

    assert_managed_v3_copy_on_write_ddl(statement)


def test_converter_ctas_uses_copy_on_write(tmp_path):
    """Native conversion creates a managed-v3 copy-on-write companion."""
    snowflake = ConverterFakeSnowflake()

    _converter(tmp_path, snowflake).convert('database.schema.table')

    statement = next(
        sql for sql, _, _ in snowflake.queries if sql.startswith('CREATE')
    )
    assert_managed_v3_copy_on_write_ddl(statement)


@pytest.mark.parametrize(
    ('parameter_rows', 'message'),
    (
        (
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'AUTO',
                'level': 'DEFAULT',
            }],
            'must set ICEBERG_MERGE_ON_READ_BEHAVIOR',
        ),
        (
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'ENABLED',
                'level': 'TABLE',
            }],
            'must set ICEBERG_MERGE_ON_READ_BEHAVIOR',
        ),
        (
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'DISABLED',
                'level': 'ACCOUNT',
            }],
            'explicitly at TABLE level',
        ),
        ([], 'exactly one'),
        (
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'DISABLED',
            }],
            'malformed',
        ),
    ),
)
def test_converter_rejects_unsafe_companion(
    tmp_path,
    parameter_rows,
    message,
):
    """An existing companion is never adopted with an unsafe write mode."""
    snowflake = ConverterFakeSnowflake()
    snowflake.formats['TABLE_ICEBERG'] = 'iceberg'
    snowflake.merge_on_read_parameters = parameter_rows

    with pytest.raises(NativeToIcebergConversionError, match=message):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    assert snowflake.formats == {
        'TABLE': 'native',
        'TABLE_ICEBERG': 'iceberg',
    }
    assert not any(
        sql.startswith(('CREATE', 'ALTER', 'DROP'))
        for sql, _, _ in snowflake.queries
    )
    assert not _manifest_files(tmp_path)


def test_converter_rejects_promoted_mode(tmp_path):
    """A promoted target must retain the table-level copy-on-write contract."""
    snowflake = ConverterFakeSnowflake()
    snowflake.formats = {'TABLE': 'iceberg', 'TABLE_NATIVE': 'native'}
    snowflake.merge_on_read_parameters = [{
        'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
        'value': 'DISABLED',
        'level': 'ACCOUNT',
    }]

    with pytest.raises(
        NativeToIcebergConversionError,
        match='explicitly at TABLE level',
    ):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
        )

    assert snowflake.formats == {'TABLE': 'iceberg', 'TABLE_NATIVE': 'native'}
    assert not any(
        sql.startswith(('CREATE', 'ALTER', 'DROP'))
        for sql, _, _ in snowflake.queries
    )
    assert not _manifest_files(tmp_path)


def test_converter_verifies_created_mode(tmp_path):
    """A newly created companion is rejected when Snowflake reports it unsafe."""
    snowflake = ConverterFakeSnowflake()
    snowflake.merge_on_read_parameters = [{
        'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
        'value': 'ENABLED',
        'level': 'TABLE',
    }]

    with pytest.raises(
        NativeToIcebergConversionError,
        match='must set ICEBERG_MERGE_ON_READ_BEHAVIOR',
    ):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    assert any(
        sql.startswith('CREATE ICEBERG TABLE')
        for sql, _, _ in snowflake.queries
    )
    assert not any(
        sql.startswith(('ALTER', 'DROP'))
        for sql, _, _ in snowflake.queries
    )
