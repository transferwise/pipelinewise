from unittest.mock import MagicMock, patch

import pytest
from singer import metadata

from tap_mysql import discover_utils


def _column(is_json_alias=False):
    return discover_utils.Column(
        'source_db',
        'events',
        'payload',
        'longtext',
        4294967295,
        None,
        None,
        'longtext',
        '',
        is_json_alias,
    )


@pytest.mark.parametrize(
    ('config', 'expected'),
    [
        (
            {
                'engine': 'mariadb',
                'target_table_format': 'iceberg',
                'iceberg_version': 3,
            },
            True,
        ),
        (
            {
                'engine': 'mysql',
                'target_table_format': 'iceberg',
                'iceberg_version': 3,
            },
            False,
        ),
        ({'engine': 'mariadb', 'target_table_format': 'native'}, False),
        ({'engine': 'mariadb'}, False),
        (
            {
                'engine': 'mariadb',
                'target_table_format': 'iceberg',
                'iceberg_version': True,
            },
            False,
        ),
        (
            {
                'engine': 'mariadb',
                'target_table_format': 'iceberg',
                'iceberg_version': 3.0,
            },
            False,
        ),
    ],
)
def test_mariadb_json_aliases_require_explicit_iceberg_v3(config, expected):
    assert discover_utils.mariadb_json_aliases_enabled(config) is expected


def test_json_alias_schema_and_metadata_preserve_physical_type():
    column = _column(is_json_alias=True)

    schema = discover_utils.schema_for_column(column)
    metadata_map = metadata.to_map(
        discover_utils.create_column_metadata([column])
    )

    assert schema.type == [
        'null',
        'object',
        'array',
        'string',
        'number',
        'boolean',
    ]
    assert schema.format == 'mariadb-json'
    assert metadata_map[('properties', 'payload')]['datatype'] == 'json'
    assert metadata_map[('properties', 'payload')]['sql-datatype'] == 'longtext'


def test_ordinary_longtext_remains_a_string():
    schema = discover_utils.schema_for_column(_column())

    assert schema.type == ['null', 'string']
    assert schema.format is None


@pytest.mark.parametrize('detect_json_aliases', [False, True])
def test_json_alias_catalog_query_is_opt_in_and_exact(detect_json_aliases):
    connection = MagicMock()
    open_connection = MagicMock()
    cursor = MagicMock()
    open_connection.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = None

    context = MagicMock()
    context.__enter__.return_value = open_connection
    with patch(
        'tap_mysql.discover_utils.connect_with_backoff', return_value=context
    ):
        discover_utils.discover_catalog(
            connection, detect_json_aliases=detect_json_aliases
        )

    columns_sql = cursor.execute.call_args_list[1].args[0]
    if detect_json_aliases:
        assert 'information_schema.check_constraints' in columns_sql
        assert "c.data_type = 'longtext'" in columns_sql
        assert "REPLACE(LOWER(cc.check_clause), ' ', '') =" in columns_sql
        assert "CONCAT('json_valid(`'" in columns_sql
        assert ' LIKE ' not in columns_sql
    else:
        assert 'information_schema.check_constraints' not in columns_sql
        assert 'is_json_alias' not in columns_sql
