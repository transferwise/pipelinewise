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
PROBE = 'SELECT COALESCE(COUNT_IF(MOD(DATE_PART(NANOSECOND'
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
    snowflake.companion_extra_columns = [
        _native_column('UPDATED_AT', 'TIMESTAMP_NTZ', DATETIME_PRECISION=6),
    ]
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
        'Column UPDATED_AT has 3 value(s) with sub-microsecond precision that this conversion would truncate',
    )
    probes = [sql for sql, _, _ in snowflake.queries if sql.startswith(PROBE)]
    assert probes == [
        'SELECT COALESCE(COUNT_IF(MOD(DATE_PART(NANOSECOND, "UPDATED_AT"), 1000) <> 0), 0) '
        'AS "SUB_MICROSECOND_0", '
        'COALESCE(COUNT_IF(MOD(DATE_PART(NANOSECOND, "OPENS_AT"), 1000) <> 0), 0) '
        'AS "SUB_MICROSECOND_1" '
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
@pytest.mark.parametrize('data_type', ['TIME', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ'])
def test_convert_truncates_temporal_values_without_precision_probe(tmp_path, eventual, data_type):
    """Conversion validates the same microsecond projection that it copies."""
    snowflake = _lossy_snowflake()
    snowflake.native_extra_columns = [_native_column('UPDATED_AT', data_type)]
    snowflake.companion_extra_columns = [_native_column('UPDATED_AT', data_type, DATETIME_PRECISION=6)]

    _converter(tmp_path, snowflake).convert(TABLE, eventual=eventual)

    expected = (
        {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'} if eventual == 'native'
        else {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    )
    assert snowflake.formats == expected
    statements = [sql for sql, _, _ in snowflake.queries]
    assert not any(sql.startswith(PROBE) for sql in statements)
    projection = (
        f'CAST(DATE_TRUNC(\'MICROSECOND\', "UPDATED_AT") AS {data_type}(6)) AS "UPDATED_AT"'
    )
    create = next(sql for sql in statements if sql.startswith('CREATE'))
    assert projection in create
    source_evidence = [sql for sql in statements if 'HASH_AGG(*)' in sql and projection in sql]
    assert source_evidence
    assert not _manifest_files(tmp_path)


@pytest.mark.parametrize('data_type', ['TIME', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ'])
def test_preflight_refuses_sub_microsecond_values_without_changes(tmp_path, data_type):
    snowflake = _lossy_snowflake()
    snowflake.native_extra_columns = [_native_column('UPDATED_AT', data_type)]

    blockers = _converter(tmp_path, snowflake).preflight(TABLE)

    assert len(blockers) == 1 and 'would truncate' in blockers[0]
    assert not _mutations(snowflake)
    assert not _manifest_files(tmp_path)


def test_convert_refuses_unreplayable_grant_before_copy(tmp_path):
    """A grant the Iceberg table cannot carry fails before the long copy starts."""
    snowflake = FakeSnowflake()
    snowflake.grants = [_grant('SELECT', 'SHARE', 'PARTNER_SHARE')]

    with pytest.raises(NativeToIcebergConversionError, match='cannot be replayed'):
        _converter(tmp_path, snowflake).convert(TABLE)

    assert not _manifest_files(tmp_path)
    assert not _mutations(snowflake)


@pytest.mark.parametrize('mismatch', [False, True])
def test_adoption_compares_projected_temporal_contents(tmp_path, mismatch):
    """Adoption permits truncation while refusing a genuine projected-content mismatch."""
    snowflake = _lossy_snowflake()
    snowflake.formats['TABLE_ICEBERG'] = 'iceberg'
    if mismatch:
        snowflake.staging_evidence = (2, 54321)
        with pytest.raises(NativeToIcebergConversionError, match='does not match'):
            _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)
        assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
        assert not _mutations(snowflake)
    else:
        _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)
        assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not any(sql.startswith(PROBE) for sql, _, _ in snowflake.queries)


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


def _submitted_attempt_over_temporal_column(tmp_path):
    """Leave a SUBMITTED manifest beside a validated companion, as a rejected promotion does."""
    snowflake = FakeSnowflake()
    snowflake.native_extra_columns = [_native_column('UPDATED_AT', 'TIMESTAMP_NTZ')]
    snowflake.companion_extra_columns = [
        _native_column('UPDATED_AT', 'TIMESTAMP_NTZ', DATETIME_PRECISION=6),
    ]
    snowflake.fail_promotion = RuntimeError('promotion rejected')
    with pytest.raises(RuntimeError, match='promotion rejected'):
        _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)
    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    snowflake.fail_promotion = None
    return snowflake


@pytest.mark.parametrize('companion', ['kept', 'dropped'])
def test_resumed_submitted_attempt_allows_temporal_truncation(tmp_path, companion):
    """Resumption validates projected contents without rescanning temporal precision."""
    snowflake = _submitted_attempt_over_temporal_column(tmp_path)
    if companion == 'dropped':
        del snowflake.formats['TABLE_ICEBERG']
    snowflake.sub_microsecond_counts = {'UPDATED_AT': 2}
    query_count = len(snowflake.queries)

    _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)

    assert not _manifest_files(tmp_path)
    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not any(sql.startswith(PROBE) for sql, _, _ in snowflake.queries[query_count:])


def _text_column(name, **overrides):
    return _native_column(name, 'TEXT', DATETIME_PRECISION=None, CHARACTER_MAXIMUM_LENGTH=134217728, **overrides)


def test_empty_temporal_table_is_convertible(tmp_path):
    """COUNT_IF is NULL over an empty table, which has no value to truncate."""
    snowflake = FakeSnowflake()
    snowflake.source_rows = 0
    snowflake.source_evidence = (0, 0)
    snowflake.staging_evidence = (0, 0)
    snowflake.native_extra_columns = [_native_column('UPDATED_AT', 'TIMESTAMP_NTZ')]
    snowflake.companion_extra_columns = [
        _native_column('UPDATED_AT', 'TIMESTAMP_NTZ', DATETIME_PRECISION=6),
    ]

    assert _converter(tmp_path, snowflake).preflight(TABLE) == ()
    _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}


def test_convert_refuses_catalog_class_mismatch_before_any_change(tmp_path):
    """A column the tap catalog now maps to VARIANT is refused before any manifest or copy."""
    snowflake = FakeSnowflake()
    snowflake.native_extra_columns = [_text_column('ATTRIBUTES')]

    with pytest.raises(NativeToIcebergConversionError, match='catalog maps it to VARIANT'):
        _converter(tmp_path, snowflake).convert(
            TABLE, eventual=EVENTUAL_ICEBERG, writer_types={'ATTRIBUTES': 'VARIANT'},
        )

    assert not _manifest_files(tmp_path)
    assert not _mutations(snowflake)


def test_resumed_attempt_checks_catalog_classes_before_cutover(tmp_path):
    """A pending attempt started without the tap catalog is checked against it before cutover."""
    snowflake = FakeSnowflake()
    snowflake.native_extra_columns = [_text_column('ATTRIBUTES')]
    snowflake.companion_extra_columns = [_text_column('ATTRIBUTES')]
    snowflake.fail_promotion = RuntimeError('promotion rejected')
    with pytest.raises(RuntimeError, match='promotion rejected'):
        _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)
    snowflake.fail_promotion = None
    query_count = len(snowflake.queries)

    with pytest.raises(NativeToIcebergConversionError, match='catalog maps it to VARIANT'):
        _converter(tmp_path, snowflake).convert(
            TABLE, eventual=EVENTUAL_ICEBERG, writer_types={'ATTRIBUTES': 'VARIANT'},
        )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert len(_manifest_files(tmp_path)) == 1
    assert not _mutations(snowflake, query_count)

    _converter(tmp_path, snowflake).convert(
        TABLE, eventual=EVENTUAL_ICEBERG, writer_types={'ATTRIBUTES': 'TEXT'},
    )
    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}


def test_refusals_stop_a_new_attempt_before_any_change(tmp_path):
    """A refusal from the caller stops a fresh attempt without a manifest, DDL, or full-table probe."""
    snowflake = FakeSnowflake()
    snowflake.native_extra_columns = [_native_column('UPDATED_AT', 'TIMESTAMP_NTZ')]

    with pytest.raises(NativeToIcebergConversionError, match='Cannot convert .*: tap legacy also writes it'):
        _converter(tmp_path, snowflake).convert(
            TABLE, eventual=EVENTUAL_ICEBERG, refusals=('tap legacy also writes it',),
        )

    assert not _manifest_files(tmp_path)
    assert not _mutations(snowflake)
    assert not any(sql.startswith(PROBE) for sql, _, _ in snowflake.queries)


def test_refusals_stop_an_interrupted_cutover_only_after_its_name_is_back(tmp_path):
    """A refused resume restores the primary name first, then stops before another cutover."""
    snowflake = FakeSnowflake()
    snowflake.fail_promotion = RuntimeError('promotion rejected')
    with pytest.raises(RuntimeError, match='promotion rejected'):
        _converter(tmp_path, snowflake).convert(TABLE, eventual=EVENTUAL_ICEBERG)
    snowflake.fail_promotion = None
    # A run killed between the two renames leaves only the companions.
    snowflake.formats = {'TABLE_NATIVE': 'native', 'TABLE_ICEBERG': 'iceberg'}

    with pytest.raises(NativeToIcebergConversionError, match='tap legacy also writes it'):
        _converter(tmp_path, snowflake).convert(
            TABLE, eventual=EVENTUAL_ICEBERG, refusals=('tap legacy also writes it',),
        )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert len(_manifest_files(tmp_path)) == 1


def test_refusals_never_carry_over_to_the_next_conversion(tmp_path):
    """A converter reused across tables applies each call's refusals only to that call."""
    snowflake = FakeSnowflake()
    converter = _converter(tmp_path, snowflake)
    with pytest.raises(NativeToIcebergConversionError, match='earlier refusal'):
        converter.convert(TABLE, eventual=EVENTUAL_ICEBERG, refusals=('earlier refusal',))

    converter.convert(TABLE, eventual=EVENTUAL_ICEBERG)

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
