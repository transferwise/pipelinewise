"""Structured PartialSync boundary tests."""

from datetime import date, datetime, time
from decimal import Decimal
from types import SimpleNamespace

import pytest

from pipelinewise.fastsync.commons.partial_sync_boundary import (
    PartialSyncBoundary,
    PartialSyncBoundaryError,
)


@pytest.mark.parametrize(
    ('dialect', 'column_name', 'expected_statement'),
    (
        (
            'mysql',
            'select`%s',
            ' WHERE `select``%%s` >= %s AND `select``%%s` <= %s',
        ),
        (
            'postgres',
            'select"%s',
            ' WHERE "select""%%s" >= %s AND "select""%%s" <= %s',
        ),
    ),
)
def test_source_sql_binds_values(
    dialect,
    column_name,
    expected_statement,
):
    """Source SQL contains no boundary value before driver rendering."""
    start_value = "x\\' OR 1=1 --"
    end_value = "z'; SELECT CURRENT_USER --"
    boundary = PartialSyncBoundary(column_name, start_value, end_value)

    source_sql = boundary.source_sql(dialect, [column_name])

    assert source_sql.statement == expected_statement
    assert source_sql.parameters == (start_value, end_value)
    assert start_value not in source_sql.statement
    assert end_value not in source_sql.statement


def test_snowflake_sql_escapes_values():
    """Snowflake SQL is regenerated safely from structured values."""
    boundary = PartialSyncBoundary(
        'Mixed Name',
        "x\\' OR CURRENT_USER() --",
        'trail\\',
    )

    assert boundary.snowflake_where_clause() == (
        ' WHERE "MIXED NAME" >= \'x\\\\\'\' OR CURRENT_USER() --\' '
        'AND "MIXED NAME" <= \'trail\\\\\''
    )


def test_column_resolves_catalog_case():
    """Case-compatible legacy input becomes an exact persisted source name."""
    boundary = PartialSyncBoundary('UPDATED AT', '1')

    resolved = boundary.resolved(['id', 'Updated At'])

    assert resolved.column_name == 'Updated At'
    assert resolved.snowflake_where_clause() == (
        ' WHERE "UPDATED AT" >= \'1\''
    )


@pytest.mark.parametrize(
    'source_columns',
    (
        ['id'],
        ['Updated At', 'updated at'],
    ),
)
def test_ambiguous_column_fails_closed(source_columns):
    """A configured predicate never falls back to an unchecked identifier."""
    boundary = PartialSyncBoundary('UPDATED AT', '1')

    with pytest.raises(PartialSyncBoundaryError, match='not found unambiguously'):
        boundary.resolved(source_columns)


def test_null_start_retains_no_row_range():
    """The historical literal NULL boundary remains a deliberate no-row range."""
    boundary = PartialSyncBoundary('id', 'NULL')

    assert boundary.source_sql('mysql', ['id']).statement == (
        ' WHERE `id` >= NULL'
    )
    assert boundary.source_sql('mysql', ['id']).parameters == ()
    assert boundary.snowflake_where_clause() == ' WHERE "ID" >= NULL'


def test_source_values_use_string_coercion():
    """Typed dynamic results retain the old quoted-literal comparison semantics."""
    boundary = PartialSyncBoundary('id', 1, Decimal('2.50'))

    assert boundary.source_sql('postgres', ['id']).parameters == ('1', '2.50')


def test_boundary_validates_required_fields():
    """Missing structured fields cannot become executable None predicates."""
    with pytest.raises(PartialSyncBoundaryError, match='column'):
        PartialSyncBoundary('', '1')
    with pytest.raises(PartialSyncBoundaryError, match='start value'):
        PartialSyncBoundary('id', None)
    with pytest.raises(PartialSyncBoundaryError, match='Boolean'):
        PartialSyncBoundary('id', '1', drop_target=None)


@pytest.mark.parametrize(
    'value',
    (
        Decimal('123.4500'),
        date(2026, 8, 19),
        datetime(2026, 8, 19, 12, 34, 56, 789000),
        time(12, 34, 56),
    ),
)
def test_manifest_restores_typed_boundary(value):
    """Recovery preserves driver-bindable boundary types."""
    boundary = PartialSyncBoundary('id', value)
    payload = SimpleNamespace(**boundary.as_context())

    assert PartialSyncBoundary.from_manifest_payload(payload) == boundary
