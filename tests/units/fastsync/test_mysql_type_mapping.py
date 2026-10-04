"""MariaDB and MySQL FastSync mappings shared by native and Iceberg targets."""

import pytest

from pipelinewise.fastsync import mysql_to_snowflake
from pipelinewise.fastsync.commons.snowflake_iceberg_model import IcebergTableSpec


@pytest.mark.parametrize(
    ('source_type', 'column_type', 'native_type', 'iceberg_type'),
    [
        ('tinyblob', 'tinyblob', 'BINARY', 'BINARY(67108864)'),
        ('blob', 'blob', 'BINARY', 'BINARY(67108864)'),
        ('mediumblob', 'mediumblob', 'BINARY', 'BINARY(67108864)'),
        ('longblob', 'longblob', 'BINARY', 'BINARY(67108864)'),
        ('set', "set('first','second')", 'VARCHAR(134217728)', 'VARCHAR(134217728)'),
        ('year', 'year(4)', 'NUMERIC(38,0)', 'NUMBER(38,0)'),
    ],
)
def test_explicit_source_types_map_consistently(source_type, column_type, native_type, iceberg_type):
    mapped = mysql_to_snowflake.tap_type_to_target_type(source_type, column_type)
    iceberg = IcebergTableSpec.from_fastsync('DB', 'SCHEMA', 'ITEMS', [f'VALUE {mapped}'], [])

    assert mapped == native_type
    assert iceberg.columns[0].data_type == iceberg_type
