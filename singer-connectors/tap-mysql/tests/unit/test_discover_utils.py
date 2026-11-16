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
        ({'engine': 'mariadb', 'json_target': 'snowflake'}, True),
        ({'engine': 'mariadb', 'json_target': 'snowflake', 'target_table_format': 'native'}, True),
        ({'engine': 'mysql', 'json_target': 'snowflake'}, False),
        ({'engine': 'mariadb', 'decimal_target': 'snowflake'}, False),
        ({'engine': 'mariadb', 'json_target': 'postgres',
          'target_table_format': 'iceberg', 'iceberg_version': 3}, False),
        ({'engine': 'mariadb', 'json_target': 'snowflake',
          'target_table_format': 'iceberg', 'iceberg_version': 2}, False),
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
def test_mariadb_json_aliases_require_snowflake_or_legacy_iceberg_v3(config, expected):
    assert discover_utils.mariadb_json_aliases_enabled(config) is expected


@pytest.mark.parametrize('engine', [None, '', 'postgres', 'unknown'])
def test_snowflake_json_discovery_requires_resolved_engine(engine):
    with pytest.raises(ValueError, match='requires a resolved MySQL or MariaDB'):
        discover_utils.json_discovery_options({'engine': engine, 'json_target': 'snowflake'})


@pytest.mark.parametrize('engine', ['mysql', 'mariadb'])
@pytest.mark.parametrize('table_format', [None, 'native', 'iceberg'])
def test_json_discovery_options_cover_both_engines_and_snowflake_formats(engine, table_format):
    config = {'engine': engine, 'json_target': 'snowflake', 'target_table_format': table_format}
    if table_format == 'iceberg':
        config['iceberg_version'] = 3

    options = discover_utils.json_discovery_options(config)

    assert options['json_discovery_engine'] == engine
    assert options.get('detect_json_aliases', False) is (engine == 'mariadb')


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


@pytest.mark.parametrize('data_type', ['geometrycollection', 'geomcollection'])
def test_geometry_collection_names_remain_selected_and_spatial(data_type):
    column = discover_utils.Column(
        'source_db', 'events', 'location', data_type, None, None, None, data_type, '', False,
    )

    schema = discover_utils.schema_for_column(column)
    column_metadata = metadata.to_map(discover_utils.create_column_metadata([column]))

    assert schema.type == ['null', 'object']
    assert schema.format == 'spatial'
    assert column_metadata[('properties', 'location')]['selected-by-default'] is True
    assert discover_utils.is_supported_column_type(data_type)


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
        assert 'cc.table_name = tc.table_name' in columns_sql
        assert "c.data_type = 'longtext'" in columns_sql
        assert 'LOWER(TRIM(cc.check_clause)) =' in columns_sql
        assert "REPLACE(LOWER(cc.check_clause), ' ', '')" not in columns_sql
        assert "CONCAT('json_valid(`'" in columns_sql
        assert "REPLACE(LOWER(c.column_name), '`', '``')" in columns_sql
        assert ' LIKE ' not in columns_sql
    else:
        assert 'information_schema.check_constraints' not in columns_sql
        assert 'is_json_alias' not in columns_sql


@pytest.mark.parametrize('engine', [None, 'mysql', 'mariadb'])
@pytest.mark.parametrize('has_json_alias', [False, True])
def test_discovery_evidence_covers_all_successfully_read_streams(engine, has_json_alias):
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ('source_db', 'events', 'BASE TABLE', 2),
        ('source_db', 'other', 'BASE TABLE', 0),
    ]
    columns = [
        _column(has_json_alias)._replace(column_name='pay load'),
        _column(has_json_alias)._replace(column_name='payload'),
        _column(has_json_alias)._replace(column_name='pay`load'),
        _column(False)._replace(table_name='other'),
    ]
    cursor.fetchone.side_effect = [*columns, None]
    with patch('tap_mysql.discover_utils.connect_with_backoff') as connect:
        connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        catalog = discover_utils.discover_catalog(
            MagicMock(), detect_json_aliases=has_json_alias, json_discovery_engine=engine,
        )

    assert len(catalog.streams) == 2
    assert set(catalog.streams[0].schema.properties) == {'pay load', 'payload', 'pay`load'}
    for entry in catalog.streams:
        root_metadata = metadata.to_map(entry.metadata)[()]
        if engine is None:
            assert 'mariadb-json-discovery' not in root_metadata
        else:
            assert root_metadata['mariadb-json-discovery'] == {'version': 1, 'engine': engine}


def test_failed_json_metadata_read_does_not_return_catalog_evidence():
    cursor = MagicMock()
    cursor.fetchall.return_value = [('source_db', 'events', 'BASE TABLE', 2)]
    cursor.fetchone.side_effect = [_column(True), RuntimeError('metadata read failed')]
    with patch('tap_mysql.discover_utils.connect_with_backoff') as connect:
        connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        with pytest.raises(RuntimeError, match='metadata read failed'):
            discover_utils.discover_catalog(
                MagicMock(), detect_json_aliases=True, json_discovery_engine='mariadb',
            )
