"""Explicit Singer support for MariaDB and MySQL SET, BLOB, and YEAR columns."""

from types import SimpleNamespace

import pytest
from pymysql.constants import FIELD_TYPE
from singer import CatalogEntry, Schema

from tap_mysql import discover_utils
from tap_mysql.sync_strategies import binlog


def column(data_type, column_type=None, maximum_length=None):
    """Create one discovery row for a source column type."""
    return discover_utils.Column(
        'source', 'items', 'value', data_type, maximum_length, None, None,
        column_type or data_type, '', False,
    )


@pytest.mark.parametrize('data_type', ['tinyblob', 'blob', 'mediumblob', 'longblob'])
def test_blob_discovery_uses_binary_transport(data_type):
    schema = discover_utils.schema_for_column(column(data_type)).to_dict()

    assert schema == {
        'type': ['null', 'string'],
        'format': 'binary',
        'inclusion': 'available',
    }
    assert discover_utils.is_supported_column_type(data_type)


def test_set_discovery_uses_string_transport():
    schema = discover_utils.schema_for_column(
        column('set', "set('first','second')", maximum_length=12),
    ).to_dict()

    assert schema == {
        'type': ['null', 'string'],
        'maxLength': 12,
        'inclusion': 'available',
    }
    assert discover_utils.is_supported_column_type('set')


def test_year_discovery_marks_an_exact_snowflake_integer():
    schema = discover_utils.schema_for_column(column('year', 'year(4)')).to_dict()

    assert schema == {
        'type': ['null', 'integer'],
        'format': 'singer.year',
        'minimum': 0,
        'maximum': 2155,
        'inclusion': 'available',
    }
    assert discover_utils.is_supported_column_type('year')


def test_binlog_values_match_full_table_transport():
    catalog = CatalogEntry(
        stream='source-items',
        schema=Schema.from_dict({
            'type': 'object',
            'properties': {
                'contents': {'type': ['null', 'string'], 'format': 'binary'},
                'labels': {'type': ['null', 'string']},
                'calendar_year': {'type': ['null', 'integer'], 'format': 'singer.year'},
            },
        }),
    )

    message = binlog.row_to_singer_record(
        catalog,
        1,
        {'contents': FIELD_TYPE.BLOB, 'labels': FIELD_TYPE.SET, 'calendar_year': FIELD_TYPE.YEAR},
        {'contents': b'\x00\xff', 'labels': {'second', 'first'}, 'calendar_year': 2024},
        None,
        {'labels': ('first', 'second')},
    )

    assert message.record == {
        'contents': '00ff',
        'labels': 'first,second',
        'calendar_year': 2024,
    }


@pytest.mark.parametrize(('value', 'expected'), [(set(), ''), (None, None)])
def test_binlog_set_keeps_empty_distinct_from_null(value, expected):
    assert binlog.serialize_set(value, ('first', 'second')) == expected


def test_binlog_set_metadata_retains_declaration_order():
    event = SimpleNamespace(columns=[
        SimpleNamespace(name='labels', type=FIELD_TYPE.SET, set_values=['second', 'first']),
        SimpleNamespace(name='year', type=FIELD_TYPE.YEAR),
    ])

    assert binlog.get_set_column_values(event) == {'labels': ('second', 'first')}


def test_pinned_binlog_decoder_preserves_empty_sets():
    method = getattr(binlog.RowsEvent, '_RowsEvent__read_values_name')
    event = object.__new__(binlog.RowsEvent)
    event.packet = SimpleNamespace(read_uint_by_size=lambda _size: 0)
    source_column = SimpleNamespace(type=FIELD_TYPE.SET, size=1, set_values=['first'])

    assert method(event, source_column, b'\x00', 0, b'\x01', False, False, None, 0) == set()
    assert method(event, source_column, b'\x01', 0, b'\x01', False, False, None, 0) is None


def test_pinned_binlog_decoder_preserves_zero_year():
    method = getattr(binlog.RowsEvent, '_RowsEvent__read_values_name')
    event = object.__new__(binlog.RowsEvent)
    event.packet = SimpleNamespace(read_uint8=lambda: 0)
    source_column = SimpleNamespace(type=FIELD_TYPE.YEAR)

    assert method(event, source_column, b'\x00', 0, b'\x01', False, False, None, 0) == 0
