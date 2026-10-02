"""Regression coverage for unambiguous buffered record identities."""

from unittest.mock import Mock

import pytest

from target_postgres import store_record
from target_postgres.db_sync import DbSync, RECORD_UPDATE_MODE_PATCH


def keyed_sync():
    """Use real record-key logic without opening a database connection."""
    db = Mock(record_update_mode=RECORD_UPDATE_MODE_PATCH, data_flattening_max_level=0)
    db.stream_schema_message = {'key_properties': ['first', 'second']}
    db.flatten_schema = {key: {'type': ['string']} for key in ['first', 'second', 'payload', 'status']}
    return db


@pytest.mark.parametrize('left,right', [
    (('a,b', 'c'), ('a', 'b,c')),
    (('', ','), (',', '')),
    (('a"b', 'c\\d'), ('a', 'b"c\\d')),
])
def test_patch_records_with_distinct_composite_keys_never_coalesce(left, right):
    """Delimiter characters inside keys cannot merge independent PATCH records."""
    db = keyed_sync()
    first = {'first': left[0], 'second': left[1], 'payload': 'left'}
    second = {'first': right[0], 'second': right[1], 'status': 'right'}
    first_key = DbSync.record_primary_key_string(db, first)
    second_key = DbSync.record_primary_key_string(db, second)
    records = {}

    store_record(records, first_key, first, db)
    store_record(records, second_key, second, db)
    store_record(records, first_key, {'first': left[0], 'second': left[1], 'status': 'updated'}, db)

    assert len(records) == 2
    assert records[first_key]['payload'] == 'left'
    assert records[first_key]['status'] == 'updated'
    assert records[second_key] == second


@pytest.mark.parametrize('record', [{'first': 'a'}, {'first': 'a', 'second': None}])
def test_missing_or_null_primary_key_is_rejected_before_buffering(record):
    """An incomplete identity must not overwrite another buffered record."""
    with pytest.raises(ValueError, match="Primary key 'second'.*missing or null"):
        DbSync.record_primary_key_string(keyed_sync(), record)


def test_zero_and_false_are_valid_primary_key_components():
    """Falsy scalar keys remain usable and distinct from empty values."""
    db = keyed_sync()
    key = DbSync.record_primary_key_string(db, {'first': 0, 'second': False})
    assert key != DbSync.record_primary_key_string(db, {'first': '', 'second': ''})
    assert key == DbSync.record_primary_key_string(db, {'first': 0, 'second': False, 'payload': 'changed'})
