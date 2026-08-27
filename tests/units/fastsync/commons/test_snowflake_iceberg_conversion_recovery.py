"""Terminal recovery tests for native-to-Iceberg conversion."""

import json
from unittest.mock import patch

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    EVENTUAL_ICEBERG,
    EVENTUAL_NATIVE,
)
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import (
    FakeSnowflake,
    _converter,
    _manifest_files,
)


@pytest.mark.parametrize(
    ('eventual', 'expected_formats'),
    (
        (
            EVENTUAL_NATIVE,
            {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'},
        ),
        (
            EVENTUAL_ICEBERG,
            {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'},
        ),
    ),
)
def test_final_retry_validates_and_cleans(
    tmp_path,
    eventual,
    expected_formats,
):
    """A crash after final persistence retries validation without a transition."""
    snowflake = FakeSnowflake()
    converter = _converter(tmp_path, snowflake)
    with patch.object(
        converter.recovery_coordinator,
        'delete_conversion_attempt',
        side_effect=RuntimeError('final manifest delete interrupted'),
    ), pytest.raises(RuntimeError, match='final manifest delete interrupted'):
        converter.convert(
            'database.schema.table',
            eventual=eventual,
            iceberg_version=3,
        )

    manifest_path = _manifest_files(tmp_path)[0]
    persisted = json.loads(manifest_path.read_text(encoding='utf-8'))
    assert persisted['phase'] == 'finalized'
    assert snowflake.formats == expected_formats
    query_count = len(snowflake.queries)

    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=eventual,
        iceberg_version=3,
    )

    recovery_sql = [sql for sql, _, _ in snowflake.queries[query_count:]]
    assert len([
        sql
        for sql in recovery_sql
        if sql.startswith('SELECT COUNT(*) AS "ROW_COUNT"')
    ]) == 2
    assert not any(
        sql.startswith('CREATE ') or ' RENAME TO ' in sql
        for sql in recovery_sql
    )
    assert snowflake.formats == expected_formats
    assert not _manifest_files(tmp_path)
