"""Reader-outage and interruption tests for manual Iceberg cutover."""

import json
from unittest.mock import patch

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_converter
from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    EVENTUAL_ICEBERG,
    SnowflakeNativeToIcebergConverter,
)
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import (
    FakeSnowflake,
)


def _converter(tmp_path, snowflake=None):
    return SnowflakeNativeToIcebergConverter(
        snowflake or FakeSnowflake(),
        str(tmp_path),
    )


def _manifest_files(tmp_path):
    return list(tmp_path.glob('iceberg-recovery-*.json'))


@patch.object(snowflake_iceberg_converter.LOGGER, 'warning')
def test_cutover_warns_for_reader_outage(warning, tmp_path):
    """Cutover warns that the primary name can remain absent after interruption."""
    _converter(tmp_path).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
    )

    warning.assert_any_call(
        'eventual=iceberg conversion requires a controlled reader-and-writer outage '
        'for %s. The primary table name is temporarily '
        'absent during promotion and rollback and can remain absent after an '
        'interruption. Retry the identical command before resuming readers or writers.',
        '"DATABASE"."SCHEMA"."TABLE"',
    )


def test_missing_primary_recovers_on_retry(tmp_path):
    """A retry promotes staging after interruption between the two renames."""
    snowflake = FakeSnowflake()
    snowflake.interrupt_after_native_rename = True

    with pytest.raises(SystemExit, match='interruption after native rename'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
        )

    assert snowflake.formats == {
        'TABLE_ICEBERG': 'iceberg',
        'TABLE_NATIVE': 'native',
    }
    manifest_path = _manifest_files(tmp_path)[0]
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    assert manifest['phase'] == 'submitted'

    snowflake.interrupt_after_native_rename = False
    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
    )

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not _manifest_files(tmp_path)
