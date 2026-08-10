from unittest.mock import MagicMock, call

from pipelinewise.fastsync.commons.target_snowflake import (
    FastSyncTargetSnowflake,
)


def test_new_only_primary_key_normalization():
    """Normalize a new target without altering an existing native or Iceberg table."""
    target = object.__new__(FastSyncTargetSnowflake)
    create_sql = (
        'CREATE TABLE IF NOT EXISTS "TEST_SCHEMA"."TEST_TABLE" '
        '("ID" INTEGER,_SDC_EXTRACTED_AT TIMESTAMP_NTZ,'
        '_SDC_BATCHED_AT TIMESTAMP_NTZ,_SDC_DELETED_AT VARCHAR, '
        'PRIMARY KEY ("ID"))'
    )
    show_sql = (
        'SHOW TABLES IN SCHEMA "TEST_SCHEMA" STARTS WITH \'TEST_TABLE\''
    )
    query_tag = {'schema': 'test_schema', 'table': 'test_table'}

    target.query = MagicMock(side_effect=[[{'name': 'TEST_TABLE'}], []])
    target.create_table(
        'test_schema',
        'test_table',
        ['"ID" INTEGER'],
        ['"ID"'],
        allow_replace_table=False,
        normalize_primary_keys='if_created',
    )
    assert target.query.call_args_list == [
        call(show_sql, query_tag_props=query_tag),
        call(create_sql, query_tag_props=query_tag),
    ]

    target.query = MagicMock(side_effect=[[], [], []])
    target.create_table(
        'test_schema',
        'test_table',
        ['"ID" INTEGER'],
        ['"ID"'],
        allow_replace_table=False,
        normalize_primary_keys='if_created',
    )
    assert target.query.call_args_list[-1] == call(
        'alter table "TEST_SCHEMA"."TEST_TABLE" '
        'alter column "ID" drop not null;',
        query_tag_props=query_tag,
    )
