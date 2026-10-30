"""Read-only preflight and representation checks for native-to-Iceberg conversion."""

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_converter
from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    EVENTUAL_ICEBERG,
    NativeToIcebergConversionError,
    SnowflakeTableName,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_validation import (
    manual_recovery_identity,
)
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import (
    FakeSnowflake,
    _converter,
    _manifest_files,
)


TABLE = 'database.schema.table'
PROBE = 'SELECT COUNT_IF(MOD(DATE_PART(NANOSECOND'
MUTATIONS = ('CREATE', 'ALTER', 'GRANT', 'DROP')


def _native_column(name, data_type, **overrides):
    row = {
        'COLUMN_NAME': name,
        'DATA_TYPE': data_type,
        'IS_NULLABLE': 'YES',
        'NUMERIC_PRECISION': None,
        'NUMERIC_SCALE': None,
        'DATETIME_PRECISION': 9,
        'COMMENT': None,
        'COLUMN_DEFAULT': None,
        'IS_IDENTITY': 'NO',
    }
    row.update(overrides)
    return row


def _grant(privilege, granted_to, grantee):
    return {
        'privilege': privilege,
        'granted_to': granted_to,
        'grantee_name': grantee,
        'grant_option': 'false',
    }


def _mutations(snowflake, start=0):
    return [sql for sql, _, _ in snowflake.queries[start:] if sql.startswith(MUTATIONS)]


def _lossy_snowflake():
    snowflake = FakeSnowflake()
    snowflake.native_extra_columns = [
        _native_column('UPDATED_AT', 'TIMESTAMP_NTZ'),
    ]
    snowflake.sub_microsecond_counts = {'UPDATED_AT': 3}
    return snowflake


def test_preflight_accepts_convertible_table_without_side_effects(tmp_path):
    """A convertible table reports no blockers, writes no local state, and runs no DDL."""
    snowflake = FakeSnowflake()

    assert _converter(tmp_path, snowflake).preflight(TABLE) == ()

    assert not list(tmp_path.iterdir())
    assert not _mutations(snowflake)
    assert not any(sql.startswith(PROBE) for sql, _, _ in snowflake.queries)


def test_preflight_collects_every_conversion_blocker(tmp_path):
    """Representation, grant, and precision blockers are reported together."""
    snowflake = FakeSnowflake()
    snowflake.native_extra_columns = [
        _native_column('CREATED_TZ', 'TIMESTAMP_TZ'),
        _native_column('LOCATION', 'GEOGRAPHY', DATETIME_PRECISION=None),
        _native_column('UPDATED_AT', 'TIMESTAMP_NTZ'),
        _native_column('OPENS_AT', 'TIME'),
    ]
    snowflake.sub_microsecond_counts = {'UPDATED_AT': 3}
    snowflake.grants = [
        _grant('REBUILD', 'ROLE', 'MAINTAINER'),
        _grant('SELECT', 'SHARE', 'PARTNER_SHARE'),
        _grant('OWNERSHIP', 'ROLE', 'OWNER_ROLE'),
        _grant('SELECT', 'ROLE', 'READER'),
    ]

    blockers = _converter(tmp_path, snowflake).preflight(TABLE)

    assert blockers == (
        'Column CREATED_TZ is TIMESTAMP_TZ; managed Iceberg v3 would drop its time-zone offsets',
        'Native Snowflake type GEOGRAPHY on column LOCATION is not supported by managed '
        'Iceberg v3 conversion',
        'The REBUILD grant to ROLE MAINTAINER cannot be replayed onto a managed Iceberg table',
        'The SELECT grant to SHARE PARTNER_SHARE cannot be replayed onto a managed Iceberg table',
        'Column UPDATED_AT has 3 value(s) with sub-microsecond precision that managed '
        'Iceberg v3 would truncate',
    )
    probes = [sql for sql, _, _ in snowflake.queries if sql.startswith(PROBE)]
    assert probes == [
        'SELECT COUNT_IF(MOD(DATE_PART(NANOSECOND, "UPDATED_AT"), 1000) <> 0) AS "SUB_MICROSECOND_0", '
        'COUNT_IF(MOD(DATE_PART(NANOSECOND, "OPENS_AT"), 1000) <> 0) AS "SUB_MICROSECOND_1" '
        'FROM "DATABASE"."SCHEMA"."TABLE"'
    ]
    assert not list(tmp_path.iterdir())
    assert not _mutations(snowflake)


def test_preflight_reports_the_first_unsafe_metadata(tmp_path):
    """Metadata the converter refuses outright is reported as one blocker."""
    snowflake = FakeSnowflake()
    snowflake.policy_rows = [{'POLICY_KIND': 'ROW_ACCESS_POLICY', 'POLICY_NAME': 'TENANT'}]

    blockers = _converter(tmp_path, snowflake).preflight(TABLE)

    assert len(blockers) == 1
    assert 'policies' in blockers[0]
    assert not list(tmp_path.iterdir())


def test_preflight_rejects_other_database_before_queries(tmp_path):
    """The configured database boundary applies to read-only checks too."""
    snowflake = FakeSnowflake()

    blockers = _converter(tmp_path, snowflake).preflight('other_database.schema.table')

    assert len(blockers) == 1
    assert 'outside the configured target database' in blockers[0]
    assert snowflake.queries == []


def test_missing_probe_evidence_fails_closed(tmp_path):
    """An incomplete precision probe is an error rather than a clean report."""
    snowflake = _lossy_snowflake()
    original_query = snowflake.query

    def query(sql, params=None, query_tag_props=None):
        if sql.startswith(PROBE):
            snowflake.queries.append((sql, params, query_tag_props))
            return []
        return original_query(sql, params, query_tag_props)

    snowflake.query = query

    with pytest.raises(NativeToIcebergConversionError, match='sub-microsecond evidence'):
        _converter(tmp_path, snowflake).preflight(TABLE)


@pytest.mark.parametrize('eventual', ['native', EVENTUAL_ICEBERG])
def test_convert_refuses_unrepresentable_values_before_any_change(tmp_path, eventual):
    """A table v3 cannot hold exactly fails before a manifest or Iceberg table exists."""
    snowflake = _lossy_snowflake()

    with pytest.raises(NativeToIcebergConversionError, match='would truncate'):
        _converter(tmp_path, snowflake).convert(TABLE, eventual=eventual)

    assert snowflake.formats == {'TABLE': 'native'}
    assert not _manifest_files(tmp_path)
    assert not _mutations(snowflake)


def test_convert_refuses_unreplayable_grant_before_copy(tmp_path):
    """A grant the Iceberg table cannot carry fails before the long copy starts."""
    snowflake = FakeSnowflake()
    snowflake.grants = [_grant('SELECT', 'SHARE', 'PARTNER_SHARE')]

    with pytest.raises(NativeToIcebergConversionError, match='cannot be replayed'):
        _converter(tmp_path, snowflake).convert(TABLE)

    assert not _manifest_files(tmp_path)
    assert not _mutations(snowflake)


def test_adoption_refuses_unrepresentable_source(tmp_path):
    """An existing companion is never adopted from a source v3 cannot represent."""
    snowflake = _lossy_snowflake()
    snowflake.formats['TABLE_ICEBERG'] = 'iceberg'

    with pytest.raises(NativeToIcebergConversionError, match='would truncate'):
        _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert not _manifest_files(tmp_path)
    assert not _mutations(snowflake)
    assert not any(sql.startswith('SELECT COUNT(*) AS "ROW_COUNT"') for sql, _, _ in snowflake.queries)


def test_resumed_attempt_rechecks_before_staging(tmp_path):
    """A pending attempt that has not staged evidence is checked again before its copy."""
    snowflake = FakeSnowflake()
    converter = _converter(tmp_path, snowflake)
    table = SnowflakeTableName.parse(TABLE)
    attempt = snowflake_iceberg_converter._new_conversion_attempt(
        table,
        EVENTUAL_ICEBERG,
        converter._inspect_source(table),
        manual_recovery_identity(snowflake.connection_config),
    )
    converter.recovery_coordinator.save_conversion_attempt(attempt)
    snowflake.grants = [_grant('REBUILD', 'ROLE', 'MAINTAINER')]
    query_count = len(snowflake.queries)

    with pytest.raises(NativeToIcebergConversionError, match='cannot be replayed'):
        _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)

    assert len(_manifest_files(tmp_path)) == 1
    assert not _mutations(snowflake, query_count)


def test_convertible_temporal_column_is_probed_then_converted(tmp_path):
    """Microsecond-exact temporal values pass the probe and convert normally."""
    snowflake = FakeSnowflake()
    snowflake.native_extra_columns = [_native_column('UPDATED_AT', 'TIMESTAMP_NTZ')]
    snowflake.companion_extra_columns = [
        _native_column('UPDATED_AT', 'TIMESTAMP_NTZ', DATETIME_PRECISION=6),
    ]

    _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    sql = [statement for statement, _, _ in snowflake.queries]
    probe = next(index for index, statement in enumerate(sql) if statement.startswith(PROBE))
    create = next(index for index, statement in enumerate(sql) if statement.startswith('CREATE'))
    assert probe < create
    assert 'CAST("UPDATED_AT" AS TIMESTAMP_NTZ(6)) AS "UPDATED_AT"' in sql[create]
