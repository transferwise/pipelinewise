"""Rerun and recovery safety tests for manual native-to-Iceberg conversion."""

import json

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_converter
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    PHASE_SUBMITTED,
    RecoveryManifestError,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    EVENTUAL_ICEBERG,
    EVENTUAL_NATIVE,
    ROLLBACK_REQUIRED,
    NativeToIcebergConversionError,
    SnowflakeTableName,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_validation import (
    manual_recovery_identity,
)
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import (
    AUTOMATED_REFERENCES_GRANT,
    FakeSnowflake,
    _converter,
    _manifest_files,
    revoke_reader_grant_after_promotion,
)


TABLE = 'database.schema.table'
PROMOTED = {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
ROW_COUNT_SCAN = 'SELECT COUNT(*) AS "ROW_COUNT"'
ADDED_COLUMN = {
    'COLUMN_NAME': 'ADDED_AFTER_CUTOVER',
    'DATA_TYPE': 'NUMBER',
    'IS_NULLABLE': 'YES',
    'NUMERIC_PRECISION': 38,
    'NUMERIC_SCALE': 0,
    'DATETIME_PRECISION': None,
    'COMMENT': None,
    'COLUMN_DEFAULT': None,
    'IS_IDENTITY': 'NO',
}


def _convert(tmp_path, snowflake, eventual=EVENTUAL_ICEBERG):
    _converter(tmp_path, snowflake).convert(TABLE, eventual=eventual, iceberg_version=3)


def _renames(queries):
    return [sql for sql, _, _ in queries if ' RENAME TO ' in sql]


def _manifest(tmp_path):
    return json.loads(_manifest_files(tmp_path)[0].read_text(encoding='utf-8'))


def _published_after_transient_error(tmp_path, snowflake):
    snowflake.post_promotion_query_error = (ROW_COUNT_SCAN, RuntimeError('connection reset'))
    with pytest.raises(RuntimeError, match='connection reset'):
        _convert(tmp_path, snowflake)
    assert snowflake.formats == PROMOTED
    assert _manifest(tmp_path)['phase'] == 'published'


@pytest.mark.parametrize('drift', ['grant', 'rows'])
def test_rerun_after_completed_cutover_changes_nothing(tmp_path, drift):
    """A rerun on a completed conversion never revalidates or rolls back the live table."""
    snowflake = FakeSnowflake()
    _convert(tmp_path, snowflake)
    if drift == 'grant':
        snowflake.companion_grants = [AUTOMATED_REFERENCES_GRANT]
    else:
        snowflake.staging_evidence = (3, 99999)
    query_count = len(snowflake.queries)

    _convert(tmp_path, snowflake)

    assert snowflake.formats == PROMOTED
    assert not _manifest_files(tmp_path)
    assert not any(
        sql.startswith(('CREATE', 'ALTER', 'GRANT', ROW_COUNT_SCAN))
        for sql, _, _ in snowflake.queries[query_count:]
    )


def test_native_rerun_after_completed_cutover_needs_manual_recovery(tmp_path):
    """A completed cutover cannot be turned back into a native-mode copy."""
    snowflake = FakeSnowflake()
    _convert(tmp_path, snowflake)
    query_count = len(snowflake.queries)

    with pytest.raises(NativeToIcebergConversionError, match='Cannot safely reconcile'):
        _convert(tmp_path, snowflake, EVENTUAL_NATIVE)

    assert snowflake.formats == PROMOTED
    assert not _manifest_files(tmp_path)
    assert not any(
        sql.startswith(('CREATE', 'ALTER', 'GRANT'))
        for sql, _, _ in snowflake.queries[query_count:]
    )


@pytest.mark.parametrize('eventual', [EVENTUAL_NATIVE, EVENTUAL_ICEBERG])
def test_rejects_table_created_as_iceberg(tmp_path, eventual):
    """A table that was never native is refused before any change."""
    snowflake = FakeSnowflake()
    snowflake.formats = {'TABLE': 'iceberg'}

    with pytest.raises(NativeToIcebergConversionError, match='already an Iceberg table'):
        _convert(tmp_path, snowflake, eventual)

    assert snowflake.formats == {'TABLE': 'iceberg'}
    assert not _manifest_files(tmp_path)
    assert not any(
        sql.startswith(('CREATE', 'ALTER', 'GRANT')) for sql, _, _ in snowflake.queries
    )


def test_post_cutover_content_drift_keeps_promoted_table(tmp_path):
    """Changed contents after promotion keep the Iceberg table live and the manifest."""
    snowflake = FakeSnowflake()
    snowflake.post_promotion_staging_evidence = (2, 54321)

    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _convert(tmp_path, snowflake)

    assert snowflake.formats == PROMOTED
    assert len(_renames(snowflake.queries)) == 2
    manifest = _manifest(tmp_path)
    assert manifest['phase'] == 'published'
    assert ROLLBACK_REQUIRED not in manifest['context']

    query_count = len(snowflake.queries)
    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _convert(tmp_path, snowflake)

    assert not _renames(snowflake.queries[query_count:])
    assert _manifest(tmp_path)['phase'] == 'published'

    snowflake.staging_evidence = snowflake.source_evidence
    query_count = len(snowflake.queries)
    _convert(tmp_path, snowflake)

    assert not _renames(snowflake.queries[query_count:])
    assert snowflake.formats == PROMOTED
    assert not _manifest_files(tmp_path)


def test_combined_drift_never_rolls_back(tmp_path):
    """Content drift wins over a metadata mismatch, so changed rows are never renamed away."""
    snowflake = FakeSnowflake()
    snowflake.post_promotion_staging_evidence = (2, 54321)
    revoke_reader_grant_after_promotion(snowflake)

    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _convert(tmp_path, snowflake)

    assert snowflake.formats == PROMOTED
    assert len(_renames(snowflake.queries)) == 2
    assert ROLLBACK_REQUIRED not in _manifest(tmp_path)['context']


def test_backup_schema_drift_keeps_promoted_table(tmp_path):
    """A changed native backup schema never triggers a rename of the promoted table."""
    snowflake = FakeSnowflake()
    _published_after_transient_error(tmp_path, snowflake)
    snowflake.column_comment = 'changed after cutover'
    query_count = len(snowflake.queries)

    with pytest.raises(NativeToIcebergConversionError, match='Native backup schema'):
        _convert(tmp_path, snowflake)

    assert not _renames(snowflake.queries[query_count:])
    assert snowflake.formats == PROMOTED
    assert _manifest(tmp_path)['phase'] == 'published'


@pytest.mark.parametrize(
    ('failing_query', 'message'),
    [
        (ROW_COUNT_SCAN, 'connection reset'),
        ('SHOW ICEBERG TABLES', 'connection reset'),
        ("SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR'", 'could not read'),
    ],
)
def test_transient_post_cutover_error_keeps_promoted_table(tmp_path, failing_query, message):
    """A connection error in either validation stage opens no second reader gap."""
    snowflake = FakeSnowflake()
    snowflake.post_promotion_query_error = (failing_query, RuntimeError('connection reset'))

    with pytest.raises(RuntimeError, match=message):
        _convert(tmp_path, snowflake)

    assert snowflake.formats == PROMOTED
    assert len(_renames(snowflake.queries)) == 2
    manifest = _manifest(tmp_path)
    assert manifest['phase'] == 'published'
    assert ROLLBACK_REQUIRED not in manifest['context']

    query_count = len(snowflake.queries)
    _convert(tmp_path, snowflake)

    assert not _renames(snowflake.queries[query_count:])
    assert snowflake.formats == PROMOTED
    assert not _manifest_files(tmp_path)


def test_missing_primary_outside_cutover_needs_manual_recovery(tmp_path):
    """An absent primary that no interrupted cutover explains is never renamed automatically."""
    snowflake = FakeSnowflake()
    _published_after_transient_error(tmp_path, snowflake)
    snowflake.formats['TABLE_ICEBERG'] = snowflake.formats.pop('TABLE')
    query_count = len(snowflake.queries)

    with pytest.raises(NativeToIcebergConversionError, match='Cannot safely reconcile'):
        _convert(tmp_path, snowflake)

    assert not _renames(snowflake.queries[query_count:])
    assert snowflake.formats == {'TABLE_ICEBERG': 'iceberg', 'TABLE_NATIVE': 'native'}


def test_reversed_cutover_with_published_manifest_needs_manual_recovery(tmp_path):
    """A published manifest never repeats pre-cutover DDL after a manual reversal."""
    snowflake = FakeSnowflake()
    _published_after_transient_error(tmp_path, snowflake)
    snowflake.formats = {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    query_count = len(snowflake.queries)

    with pytest.raises(NativeToIcebergConversionError, match='Cannot safely reconcile'):
        _convert(tmp_path, snowflake)

    assert all(
        sql.startswith('SHOW TABLES IN SCHEMA') for sql, _, _ in snowflake.queries[query_count:]
    )
    assert _manifest(tmp_path)['phase'] == 'published'


def test_published_manifest_never_restores_stale_backup(tmp_path):
    """A published manifest whose promoted table vanished never makes the stale backup live."""
    snowflake = FakeSnowflake()
    _published_after_transient_error(tmp_path, snowflake)
    del snowflake.formats['TABLE']
    query_count = len(snowflake.queries)

    with pytest.raises(NativeToIcebergConversionError, match='Cannot safely reconcile'):
        _convert(tmp_path, snowflake)

    assert not _renames(snowflake.queries[query_count:])
    assert snowflake.formats == {'TABLE_NATIVE': 'native'}
    assert _manifest(tmp_path)['phase'] == 'published'


def test_post_cutover_column_change_keeps_promoted_table(tmp_path):
    """A column added to the live table after cutover is never rolled away."""
    snowflake = FakeSnowflake()
    snowflake.post_promotion_companion_extra_columns = [ADDED_COLUMN]

    with pytest.raises(NativeToIcebergConversionError, match='schema or primary key'):
        _convert(tmp_path, snowflake)

    assert snowflake.formats == PROMOTED
    assert len(_renames(snowflake.queries)) == 2
    manifest = _manifest(tmp_path)
    assert manifest['phase'] == 'published'
    assert ROLLBACK_REQUIRED not in manifest['context']


def test_rollback_resume_refuses_changed_columns(tmp_path):
    """A pending rollback never renames away a promoted table whose columns changed."""
    snowflake = FakeSnowflake()
    revoke_reader_grant_after_promotion(snowflake)
    snowflake.interrupt_iceberg_rollback = True
    with pytest.raises(SystemExit, match='first rollback interruption'):
        _convert(tmp_path, snowflake)
    snowflake.interrupt_iceberg_rollback = False
    snowflake.companion_extra_columns = [ADDED_COLUMN]
    query_count = len(snowflake.queries)

    with pytest.raises(NativeToIcebergConversionError, match='schema or primary key'):
        _convert(tmp_path, snowflake)

    assert not _renames(snowflake.queries[query_count:])
    assert snowflake.formats == {'TABLE': 'iceberg', 'TABLE_NATIVE': 'native'}
    assert _manifest(tmp_path)['context'][ROLLBACK_REQUIRED] is True


def test_rollback_resume_refuses_changed_promoted_table(tmp_path):
    """A pending rollback never renames away a promoted table whose contents changed."""
    snowflake = FakeSnowflake()
    revoke_reader_grant_after_promotion(snowflake)
    snowflake.interrupt_iceberg_rollback = True

    with pytest.raises(SystemExit, match='first rollback interruption'):
        _convert(tmp_path, snowflake)

    assert snowflake.formats == {'TABLE': 'iceberg', 'TABLE_NATIVE': 'native'}
    snowflake.interrupt_iceberg_rollback = False
    snowflake.staging_evidence = (3, 99999)
    query_count = len(snowflake.queries)

    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _convert(tmp_path, snowflake)

    assert not _renames(snowflake.queries[query_count:])
    assert snowflake.formats == {'TABLE': 'iceberg', 'TABLE_NATIVE': 'native'}
    assert _manifest(tmp_path)['context'][ROLLBACK_REQUIRED] is True


@pytest.mark.parametrize(
    'formats',
    [
        {'TABLE_ICEBERG': 'iceberg', 'TABLE_NATIVE': 'native'},
        {'TABLE': 'iceberg', 'TABLE_NATIVE': 'native'},
    ],
)
def test_rollback_marker_without_evidence_resumes(tmp_path, formats):
    """An evidence-less rollback marker left by an earlier release finishes and cuts over again."""
    snowflake = FakeSnowflake()
    converter = _converter(tmp_path, snowflake)
    table = SnowflakeTableName.parse(TABLE)
    attempt = snowflake_iceberg_converter._new_conversion_attempt(
        table,
        EVENTUAL_ICEBERG,
        converter._inspect_source(table),
        manual_recovery_identity(snowflake.connection_config),
    )
    attempt.update_manifest_payload({ROLLBACK_REQUIRED: True})
    converter.recovery_coordinator.save_conversion_attempt(attempt)
    snowflake.formats = dict(formats)

    _convert(tmp_path, snowflake)

    assert snowflake.formats == PROMOTED
    assert not _manifest_files(tmp_path)


@pytest.mark.parametrize('interrupted_phase', ['staging_created', 'submitted'])
def test_recreates_dropped_companion(tmp_path, interrupted_phase):
    """A retry recreates a dropped companion from any phase before cutover."""
    snowflake = FakeSnowflake()
    if interrupted_phase == 'staging_created':
        snowflake.staging_evidence = (2, 54321)
        expected_error, message = NativeToIcebergConversionError, 'does not match'
    else:
        snowflake.fail_promotion = RuntimeError('promotion rejected')
        expected_error, message = RuntimeError, 'promotion rejected'

    with pytest.raises(expected_error, match=message):
        _convert(tmp_path, snowflake)

    assert _manifest(tmp_path)['phase'] == interrupted_phase
    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    del snowflake.formats['TABLE_ICEBERG']
    snowflake.staging_evidence = snowflake.source_evidence
    snowflake.fail_promotion = None

    _convert(tmp_path, snowflake)

    assert snowflake.formats == PROMOTED
    assert not _manifest_files(tmp_path)
    assert len([sql for sql, _, _ in snowflake.queries if sql.startswith('CREATE')]) == 2


def test_query_tags_reset_between_calls(tmp_path):
    """A reused converter never tags new queries with an earlier attempt."""
    snowflake = FakeSnowflake()
    converter = _converter(tmp_path, snowflake)
    converter.convert(TABLE, iceberg_version=3)
    first_load_id = next(
        tags['load_id'] for _, _, tags in snowflake.queries if 'load_id' in tags
    )
    query_count = len(snowflake.queries)

    converter.convert(TABLE, eventual=EVENTUAL_ICEBERG, iceberg_version=3)

    second_call_tags = [tags for _, _, tags in snowflake.queries[query_count:]]
    assert 'load_id' not in second_call_tags[0]
    assert all(tags.get('load_id') != first_load_id for tags in second_call_tags)


def test_unloadable_conversion_manifest_is_never_saved(tmp_path):
    """A manifest that a retry could not load is rejected before it is written."""
    snowflake = FakeSnowflake()
    snowflake.staging_evidence = (2, 54321)
    converter = _converter(tmp_path, snowflake)
    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        converter.convert(TABLE, iceberg_version=3)
    manifest_path = _manifest_files(tmp_path)[0]
    persisted = manifest_path.read_bytes()
    attempt = converter.recovery_coordinator.recovery_store(
        SnowflakeTableName.parse(TABLE)
    ).load()
    attempt.phase = PHASE_SUBMITTED

    with pytest.raises(RecoveryManifestError, match='row count and row fingerprint'):
        converter.recovery_coordinator.save_conversion_attempt(attempt)

    assert manifest_path.read_bytes() == persisted
