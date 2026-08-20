"""Non-credentialed tests for E2E database metadata helpers."""

from pipelinewise.fastsync import mysql_to_snowflake
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
