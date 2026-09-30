"""Non-credentialed tests for E2E database metadata helpers."""

import pytest

from pipelinewise.fastsync import mysql_to_postgres, mysql_to_snowflake, postgres_to_postgres, postgres_to_snowflake
from tests.end_to_end.helpers import assertions, db


def test_sf_varchar_metadata_keeps_width():
    """Generic column checks compare the physical Snowflake string width."""
    query = db.sql_get_columns_snowflake(['target_schema'])

    assert "data_type IN ('TEXT', 'VARCHAR')" in query
    assert 'TO_VARCHAR(character_maximum_length)' in query
    assert "'VARCHAR('" in query


def test_max_width_sf_varchar_matches():
    """A maximum-width FastSync string matches normalized Snowflake metadata."""

    def run_query_tap_mysql(_query):
        return [('address', 'street_number:varchar:varchar(5)')]

    def run_query_target_snowflake(_query):
        return [('ADDRESS', 'STREET_NUMBER:VARCHAR(134217728):')]

    assertions.assert_all_columns_exist(
        run_query_tap_mysql,
        run_query_target_snowflake,
        mysql_to_snowflake.tap_type_to_target_type,
    )


def test_decimal_metadata_queries_keep_declared_dimensions():
    """Both metadata formats expose precision and scale without changing base types."""
    postgres_query = db.sql_get_columns_postgres(['public'])
    snowflake_query = db.sql_get_columns_snowflake(['target_schema'])
    assert "CASE WHEN data_type = 'numeric' THEN numeric_precision::text" in postgres_query
    assert "CASE WHEN data_type = 'numeric' THEN numeric_scale::text" in postgres_query
    assert "data_type IN ('NUMBER', 'NUMERIC', 'DECIMAL')" in snowflake_query
    assert 'TO_VARCHAR(numeric_precision)' in snowflake_query
    assert 'TO_VARCHAR(numeric_scale)' in snowflake_query


def _assert_column_types(source_name, target_name, mapper, source_columns, target_columns):
    def source_runner(_query):
        return [('amounts', source_columns)]

    def target_runner(_query):
        return [('AMOUNTS', target_columns)]

    source_runner.__name__ = f'run_query_tap_{source_name}'
    target_runner.__name__ = f'run_query_target_{target_name}'
    assertions.assert_all_columns_exist(source_runner, target_runner, mapper.tap_type_to_target_type)


@pytest.mark.parametrize(('source_name', 'target_name', 'mapper', 'source_columns', 'target_columns'), [
    ('mysql', 'snowflake', mysql_to_snowflake, 'amount:decimal:decimal(10,0)', 'AMOUNT:NUMBER::10:0'),
    ('mysql', 'postgres', mysql_to_postgres, 'amount:decimal:decimal(65,30) unsigned', 'amount:numeric::65:30'),
    ('postgres', 'snowflake', postgres_to_snowflake, 'amount:numeric::38:18', 'AMOUNT:NUMBER::38:18'),
    ('postgres', 'postgres', postgres_to_postgres, 'amount:numeric:::', 'amount:numeric:::'),
    ('postgres', 'postgres', postgres_to_postgres, 'amount:numeric::5:-2', 'amount:numeric::5:-2'),
    ('mysql', 'snowflake', mysql_to_snowflake, 'id:int:int(11);value:double:double',
     'ID:NUMBER::38:0;VALUE:FLOAT:::'),
    ('postgres', 'snowflake', postgres_to_snowflake, 'id:integer:::;value:double precision:::',
     'ID:NUMBER::38:0;VALUE:FLOAT:::'),
    ('mysql', 'snowflake', mysql_to_snowflake, "status:enum:enum('ready:yes','ready:no')",
     'STATUS:VARCHAR(134217728):::'),
])
def test_column_assertions_preserve_decimal_dimensions(
    source_name, target_name, mapper, source_columns, target_columns,
):
    """SQL decimal mappings use exact dimensions; integer/float comparisons stay unchanged."""
    _assert_column_types(source_name, target_name, mapper, source_columns, target_columns)


@pytest.mark.parametrize('target_columns', [
    'AMOUNT:NUMBER::37:18',
    'AMOUNT:NUMBER::38:17',
    'AMOUNT:NUMBER:::',
    'AMOUNT:FLOAT:::',
])
def test_decimal_column_assertions_reject_wrong_dimensions_or_float(target_columns):
    """A matching base type alone cannot satisfy a declared PostgreSQL decimal."""
    with pytest.raises(Exception, match=r'Expected: numeric\(38,18\) Actual:'):
        _assert_column_types('postgres', 'snowflake', postgres_to_snowflake, 'amount:numeric::38:18', target_columns)


def test_mysql_decimal_column_assertion_rejects_wrong_scale():
    with pytest.raises(Exception, match=r'Expected: numeric\(10,0\) Actual: numeric\(10,2\)'):
        _assert_column_types(
            'mysql', 'postgres', mysql_to_postgres, 'amount:decimal:decimal(10,0)', 'amount:numeric::10:2',
        )
