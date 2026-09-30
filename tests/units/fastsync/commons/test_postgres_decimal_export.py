"""PostgreSQL decimal export normalization keeps target and key semantics explicit."""

from argparse import Namespace
import io
from unittest.mock import MagicMock, Mock, patch

from pipelinewise.fastsync import postgres_to_postgres, postgres_to_snowflake
from pipelinewise.fastsync.commons.partial_sync_boundary import PartialSyncBoundary
from pipelinewise.fastsync.commons.rdbms_source import RdbmsSnowflakeSource
from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres


def _column(name, data_type, precision=None, scale=None):
    values = (name, data_type, f'"{name}"', None, precision, scale)
    names = (
        'column_name', 'data_type', 'safe_sql_value', 'character_maximum_length', 'numeric_precision', 'numeric_scale',
    )
    return {**dict(enumerate(values)), **dict(zip(names, values))}


def _source(target='snowflake', primary_keys=('"ID"',), precision=10, scale=2):
    if target == 'snowflake':
        adapter = RdbmsSnowflakeSource.postgres(FastSyncTapPostgres, postgres_to_snowflake.tap_type_to_target_type)
        source = adapter.create(Namespace(tap={}, transform=None), None)
    else:
        source = FastSyncTapPostgres({}, postgres_to_postgres.tap_type_to_target_type)
    source.curr = MagicMock()
    source.get_table_columns = Mock(return_value=[
        _column('id', 'integer'), _column('Mixed', 'numeric', precision, scale),
    ])
    source.get_primary_keys = Mock(return_value=primary_keys)
    source.query = Mock(return_value=[{'has_nan_keys': False}])
    return source


def _copy_sql(source, boundary=None):
    with patch('pipelinewise.fastsync.commons.tap_postgres.split_gzip.open', return_value=io.BytesIO()):
        source.copy_table('public.items', 'unused.csv', boundary=boundary)
    return source.curr.copy_expert.call_args.args[0]


def test_snowflake_nan_normalization_preserves_mixed_case_alias_and_metadata_order():
    source = _source()
    source.get_table_columns.return_value[1]['safe_sql_value'] = '"Mixed" AS mixed'
    sql = _copy_sql(source)
    assert 'NULLIF("Mixed", \'NaN\'::numeric) AS "Mixed"' in sql
    assert 'AS _ppw_numeric_export("id", "Mixed", "_sdc_extracted_at", "_sdc_batched_at", "_sdc_deleted_at")' in sql
    source.query.assert_not_called()


def test_postgres_target_preserves_nan_without_normalization_or_key_queries():
    source = _source('postgres')
    sql = _copy_sql(source)
    assert 'NULLIF' not in sql
    assert '_ppw_numeric_export' not in sql
    source.query.assert_not_called()
    source.get_primary_keys.assert_not_called()


def test_bounded_numeric_nan_primary_key_exports_canonical_text_without_guard():
    source = _source(primary_keys=('"MIXED"',))
    sql = _copy_sql(source)
    assert "CASE WHEN POSITION('.' IN CAST(\"Mixed\" AS TEXT)) > 0" in sql
    assert 'NULLIF' not in sql
    source.query.assert_not_called()
    source.curr.copy_expert.assert_called_once()


def test_bounded_numeric_text_key_preserves_partial_range():
    source = _source(primary_keys=('"MIXED"',))
    source.curr.mogrify.return_value = ' WHERE "id" >= 1 AND "id" <= 2'
    sql = _copy_sql(source, PartialSyncBoundary('id', 1, 2))
    assert 'WHERE "id" >= 1 AND "id" <= 2' in sql
    assert "CASE WHEN POSITION('.' IN CAST(\"Mixed\" AS TEXT)) > 0" in sql
    assert 'NULLIF' not in sql
    source.query.assert_not_called()


def test_decimal_text_key_partial_export_preserves_column_names_and_nan_identity():
    source = _source(primary_keys=('"MIXED"',), precision=None, scale=None)
    source.curr.mogrify.return_value = ' WHERE "id" >= 1 AND "id" <= 2'
    sql = _copy_sql(source, PartialSyncBoundary('id', 1, 2))
    assert 'CASE WHEN POSITION' in sql
    assert 'WHERE "id" >= 1 AND "id" <= 2' in sql
    assert 'NULLIF' not in sql
    source.query.assert_not_called()


def test_nan_normalization_occurs_after_conditional_transformations():
    source = _source()
    source.source_transformations = {'transformations': [{
        'tap_stream_name': 'public-items', 'field_id': 'Mixed', 'type': 'MASK-NUMBER',
        'when': [{'column': 'id', 'equals': 1}],
    }]}
    sql = _copy_sql(source)
    assert 'COPY (SELECT "id", NULLIF("Mixed"' in sql
    assert 'CASE WHEN ("id" = 1) THEN 0 ELSE "Mixed" END AS "Mixed"' in sql
