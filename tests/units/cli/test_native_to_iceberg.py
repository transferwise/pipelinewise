"""Tap-scoped native-to-Iceberg conversion reports."""

import json
from unittest.mock import patch

import pytest

from pipelinewise.cli import native_to_iceberg
from pipelinewise.cli.errors import PreRunChecksException
from pipelinewise.fastsync.commons.snowflake_iceberg_conversion_plan import (
    STATE_BLOCKED,
    STATE_ICEBERG,
    STATE_NATIVE,
    STATE_NEW,
    ConversionTarget,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_model import SnowflakeObjectName
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import FakeSnowflake


FASTSYNC_CONFIG = {'dbname': 'database', 'default_target_schema': 'schema'}


def _target(table, writer_types=None):
    return ConversionTarget(
        f'public-{table.lower()}',
        SnowflakeObjectName('DATABASE', 'SCHEMA', table),
        writer_types or {},
    )


def _report(tmp_path, snowflake, targets):
    with patch.object(native_to_iceberg, 'SnowflakeQueryAdapter', return_value=snowflake):
        return native_to_iceberg.preflight_report({'dbname': 'database'}, str(tmp_path), targets)


def test_report_classifies_and_preflights_without_side_effects(tmp_path):
    """Native tables get preflight blockers; new and Iceberg tables need no action."""
    snowflake = FakeSnowflake()
    snowflake.formats = {'TABLE': 'native', 'DONE': 'iceberg'}

    reports = _report(tmp_path, snowflake, [_target('TABLE'), _target('DONE'), _target('LATER')])

    assert [(report.table, report.state, report.action, report.blockers) for report in reports] == [
        ('DATABASE.SCHEMA.TABLE', STATE_NATIVE, 'convert', ()),
        ('DATABASE.SCHEMA.DONE', STATE_ICEBERG, 'none: already Iceberg', ()),
        ('DATABASE.SCHEMA.LATER', STATE_NEW, 'none: the writers create it as Iceberg', ()),
    ]
    assert not list(tmp_path.iterdir())
    assert not any(sql.startswith(('CREATE', 'ALTER', 'GRANT')) for sql, _, _ in snowflake.queries)


def test_report_includes_catalog_type_blockers(tmp_path):
    """A native column the Iceberg writers would refuse is reported before enabling."""
    snowflake = FakeSnowflake()
    snowflake.native_extra_columns = [{
        'COLUMN_NAME': 'ATTRIBUTES',
        'DATA_TYPE': 'TEXT',
        'IS_NULLABLE': 'YES',
        'COMMENT': None,
        'COLUMN_DEFAULT': None,
        'IS_IDENTITY': 'NO',
    }]

    reports = _report(tmp_path, snowflake, [_target('TABLE', {'ATTRIBUTES': 'VARIANT', 'ID': 'TEXT'})])

    assert reports[0].action == 'resolve the blockers'
    assert reports[0].blockers == (
        'Column ATTRIBUTES is TEXT but the tap catalog maps it to VARIANT; '
        'the Iceberg writers would refuse the converted table',
    )


def test_report_records_inspection_failures_per_table(tmp_path):
    """A failed inspection is a blocker for that table, not a crash of the report."""
    snowflake = FakeSnowflake()

    with patch.object(
        native_to_iceberg.SnowflakeNativeToIcebergConverter,
        'preflight',
        side_effect=RuntimeError('insufficient privileges'),
    ):
        reports = _report(tmp_path, snowflake, [_target('TABLE')])

    assert reports[0].blockers == ('Cannot inspect the table: insufficient privileges',)
    assert reports[0].action == 'resolve the blockers'


def test_render_table_and_json():
    """The report renders for people and machines, and is ready only without blockers."""
    reports = [
        native_to_iceberg.TableReport(
            'public-orders', 'DB.S.ORDERS', STATE_NATIVE, 'convert', 10, 2048, (),
        ),
        native_to_iceberg.TableReport(
            'public-users', 'DB.S.USERS', STATE_BLOCKED, 'resolve the blockers', None, None,
            ('first reason', 'second reason'),
        ),
    ]

    text = native_to_iceberg.render_preflight_report(reports, ['tap-wide reason'], 'table')
    payload = json.loads(native_to_iceberg.render_preflight_report(reports, [], 'json'))

    assert text.splitlines()[0] == 'Blocker: tap-wide reason'
    assert 'DB.S.ORDERS' in text and 'first reason' in text and 'second reason' in text
    assert text.splitlines()[-1].startswith('Not ready')
    assert payload['ready'] is False
    assert payload['tables'][1]['blockers'] == ['first reason', 'second reason']
    assert json.loads(
        native_to_iceberg.render_preflight_report(reports[:1], [], 'json')
    )['ready'] is True


@pytest.mark.parametrize(
    ('formats', 'message'),
    [
        ({'TABLE': 'native'}, 'DATABASE.SCHEMA.TABLE is native'),
        ({'TABLE': 'native', 'TABLE_NATIVE': 'native'}, 'DATABASE.SCHEMA.TABLE is blocked'),
        ({'TABLE': 'iceberg'}, None),
        ({}, None),
    ],
)
def test_fastsync_readiness_refuses_native_targets(tmp_path, formats, message):
    """FastSync may only publish to a missing or Iceberg target."""
    snowflake = FakeSnowflake()
    snowflake.formats = formats

    with patch.object(native_to_iceberg, 'SnowflakeQueryAdapter', return_value=snowflake):
        if message is None:
            native_to_iceberg.assert_fastsync_targets_ready(FASTSYNC_CONFIG, str(tmp_path), ['schema.table'])
        else:
            with pytest.raises(PreRunChecksException, match=message):
                native_to_iceberg.assert_fastsync_targets_ready(
                    FASTSYNC_CONFIG, str(tmp_path), ['schema.table'],
                )

    assert [sql for sql, _, _ in snowflake.queries] == ['SHOW TABLES IN SCHEMA "DATABASE"."SCHEMA"']
    assert not list(tmp_path.iterdir())


def test_tap_blockers_require_flattening_level_zero():
    """Automatic conversion needs the level-0 column mapping the preflight assumes."""
    assert native_to_iceberg.tap_blockers({'data_flattening_max_level': 0}) == []
    assert native_to_iceberg.tap_blockers({}) == []
    assert native_to_iceberg.tap_blockers({'data_flattening_max_level': 10}) == [
        'data_flattening_max_level is 10; native_to_iceberg_conversion requires 0'
    ]
