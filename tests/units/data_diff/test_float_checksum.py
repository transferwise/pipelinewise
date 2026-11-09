"""FLOAT planning and aggregate safeguards independent of database credentials."""

from unittest.mock import MagicMock

import pytest

from pipelinewise.data_diff.adapters import DataDiffExecutionError, MySQLAdapter, PostgresAdapter, SnowflakeAdapter
from pipelinewise.data_diff.comparison import UnsupportedComparisonError, checksum_columns
from pipelinewise.data_diff.engine import build_metric_query
from pipelinewise.data_diff.float64 import FLOAT_NORMALIZATION_ERRORS, expected_float_sql, float64_projection


@pytest.mark.parametrize(('source_type', 'physical_type', 'conversion'), [
    ('tap-postgres', 'double precision', 'postgres_float_text'),
    ('tap-postgres', 'real', 'postgres_float_text'),
    ('tap-yugabyte', 'double precision', 'postgres_float_text'),
    ('tap-yugabyte', 'real', 'postgres_float_text'),
    ('tap-postgres', 'numeric', 'postgres_numeric'),
    ('tap-postgres', 'bigint', 'postgres_numeric'),
    ('tap-mysql', 'float', 'mysql_numeric'),
    ('tap-mysql', 'double', 'mysql_numeric'),
    ('tap-mysql', 'decimal', 'mysql_decimal'),
    ('tap-mysql', 'numeric', 'mysql_decimal'),
    ('tap-mysql', 'dec', 'mysql_decimal'),
    ('tap-mysql', 'fixed', 'mysql_decimal'),
    ('tap-mysql', 'bigint', 'mysql_numeric'),
])
def test_float_plan_uses_source_contract_and_actual_target_type(source_type, physical_type, conversion):
    pairs = [('amount', {'name': 'amount', 'data_type': physical_type}, {'name': 'AMOUNT', 'data_type': 'FLOAT'})]

    source, target = checksum_columns(pairs, source_type=source_type, target_type='target-snowflake')

    assert source[0]['checksum_kind'] == target[0]['checksum_kind'] == 'float64'
    assert source[0]['checksum_float_conversion'] == conversion
    assert 'checksum_float_conversion' not in target[0]
    assert 'checksum_kind' not in pairs[0][1]


@pytest.mark.parametrize(('source_type', 'target_type', 'source_physical', 'target_physical'), [
    ('tap-postgres', 'target-postgres', 'double precision', 'double precision'),
    ('tap-postgres', 'target-snowflake', 'double precision', 'NUMBER'),
    ('tap-postgres', 'target-snowflake', 'text', 'FLOAT'),
    ('tap-unknown', 'target-snowflake', 'double precision', 'FLOAT'),
])
def test_float_support_does_not_expand_other_comparisons(source_type, target_type, source_physical, target_physical):
    pairs = [('amount', {'name': 'amount', 'data_type': source_physical},
              {'name': 'AMOUNT', 'data_type': target_physical, 'numeric_scale': 0})]

    with pytest.raises(UnsupportedComparisonError):
        checksum_columns(pairs, source_type=source_type, target_type=target_type)


def test_exact_number_plan_keeps_wider_scale_for_mismatch_detection():
    source, target = checksum_columns([
        ('amount', {'name': 'amount', 'data_type': 'numeric', 'numeric_scale': 8},
         {'name': 'AMOUNT', 'data_type': 'NUMBER', 'numeric_scale': 3}),
    ], source_type='tap-postgres', target_type='target-snowflake')

    assert source[0]['checksum_kind'] == target[0]['checksum_kind'] == 'exact_numeric'
    assert source[0]['checksum_scale'] == target[0]['checksum_scale'] == 8


def test_integer_source_conversion_promotes_before_absolute_value():
    expression = expected_float_sql('"amount"', 'postgres_numeric')

    assert 'abs(("amount")::numeric)' in expression
    assert 'abs("amount")' not in expression
    assert expected_float_sql('"amount"', 'postgres_float_text') == '("amount")::text::double precision'
    assert expected_float_sql('`amount`', 'mysql_numeric') == '(`amount` * 1E0)'
    assert expected_float_sql('"AMOUNT"', 'identity') == '"AMOUNT"'


@pytest.mark.parametrize('adapter_type', [PostgresAdapter, MySQLAdapter, SnowflakeAdapter])
def test_float_query_keeps_integer_checksum_and_null_framing(adapter_type):
    adapter = adapter_type(None, 30)
    sql = build_metric_query(
        adapter, 'public', 'payments', '_ppw_f64_0_value', 'updated_at', ('row_count', 'row_checksum'),
        checksum_columns_for_query=[{'name': 'amount', 'checksum_kind': 'float64'}],
    )

    assert sql.count('%s') == 2
    assert "THEN MD5('N')" in sql
    assert "MD5(CONCAT('V'," in sql
    assert 'SUM(' in sql
    assert FLOAT_NORMALIZATION_ERRORS in sql
    assert f'AS {adapter.quote("_ppw_f64_0_value")}' not in sql
    assert f'AS {adapter.quote("__ppw_f64_0_value")}' in sql


def test_actual_target_float_is_never_clamped_or_rounded_to_source_scale():
    stages, columns = float64_projection(
        [{'name': 'AMOUNT', 'checksum_kind': 'float64'}], SnowflakeAdapter(None, 30).quote, 'snowflake',
    )

    assert stages[0] == ['"AMOUNT" AS "_ppw_f64_0_value"']
    assert 'checksum_float_invalid' in columns[0]
    assert not any('ROUND(' in expression for stage in stages for expression in stage)


def test_mysql_decimal_rounding_does_not_use_the_inexact_decimal_to_double_parser():
    source, _target = checksum_columns([
        ('amount', {'name': 'amount', 'data_type': 'decimal', 'numeric_precision': 65, 'numeric_scale': 30},
         {'name': 'AMOUNT', 'data_type': 'FLOAT'}),
    ], source_type='tap-mysql', target_type='target-snowflake')
    stages, columns = MySQLAdapter(None, 30).checksum_projection(source)
    sql = ' '.join(expression for stage in stages for expression in stage)

    assert source[0]['checksum_float_conversion'] == 'mysql_decimal'
    assert 'DIV' in sql and 'MOD(' in sql
    assert 'AS `_ppw_f64_0_remainder`' in sql
    assert 'AS `_ppw_f64_0_denominator`' in sql
    assert 'MOD(`_ppw_f64_0_integer`, 2) = 1' in sql
    assert columns[0]['checksum_float_token'] == '`_ppw_f64_0_token`'


@pytest.mark.parametrize('adapter_type', [PostgresAdapter, MySQLAdapter, SnowflakeAdapter])
@pytest.mark.parametrize('invalid_count', [None, 0, 1])
def test_normalization_errors_cannot_produce_a_passing_checksum(adapter_type, invalid_count):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {'row_checksum': 123, FLOAT_NORMALIZATION_ERRORS: invalid_count}
    adapter = adapter_type(connection, 30)

    if invalid_count:
        with pytest.raises(DataDiffExecutionError, match='normalize finite binary64'):
            adapter.execute_metrics('SELECT aggregate', (), ('row_checksum',))
    else:
        result = adapter.execute_metrics('SELECT aggregate', (), ('row_checksum',))
        assert result.values == {'row_checksum': '123'}
