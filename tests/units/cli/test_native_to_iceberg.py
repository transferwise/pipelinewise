"""Tap-scoped native-to-Iceberg conversion reports."""

import json
from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest

from pipelinewise.cli import native_to_iceberg
from pipelinewise.cli.errors import NativeToIcebergConversionException, PreRunChecksException
from pipelinewise.fastsync.commons.snowflake_iceberg_conversion_plan import (
    STATE_BLOCKED,
    STATE_ICEBERG,
    STATE_NATIVE,
    STATE_NEW,
    STATE_PENDING,
    ClassifiedTarget,
    ConversionTarget,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_model import SnowflakeObjectName
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import FakeSnowflake
from tests.units.fastsync.commons.test_snowflake_iceberg_converter_preflight import (
    _submitted_attempt_over_temporal_column,
)


FASTSYNC_CONFIG = {'dbname': 'database', 'default_target_schema': 'schema'}


def _target(table, writer_types=None):
    return ConversionTarget(
        f'public-{table.lower()}',
        SnowflakeObjectName('DATABASE', 'SCHEMA', table),
        writer_types or {},
    )


def _report(tmp_path, snowflake, targets):
    with patch.object(native_to_iceberg, 'SnowflakeConversionQueryAdapter', return_value=snowflake):
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
        ({'TABLE_NATIVE': 'native', 'TABLE_ICEBERG': 'iceberg'}, None),
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

    assert [sql for sql, _, _ in snowflake.queries] == ['SHOW TABLES IN SCHEMA "DATABASE"."SCHEMA" LIMIT 10000']
    assert not list(tmp_path.iterdir())


def test_fastsync_readiness_bounds_its_message():
    """A refusal over many tables names a bounded number and logs every one."""
    targets = [_classified(f'T{index:02d}', STATE_NATIVE) for index in range(25)]

    with patch.object(native_to_iceberg, 'SnowflakeQueryAdapter'), patch.object(
        native_to_iceberg, 'classify', return_value=targets,
    ), patch.object(native_to_iceberg.LOGGER, 'error') as log_error:
        with pytest.raises(PreRunChecksException) as error:
            native_to_iceberg.assert_fastsync_targets_ready(FASTSYNC_CONFIG, '/runtime', ['schema.t'])

    assert 'DATABASE.SCHEMA.T19 is native' in str(error.value)
    assert 'DATABASE.SCHEMA.T20' not in str(error.value)
    assert 'and 5 more tables listed in the log' in str(error.value)
    assert log_error.call_count == 25


def _classified(table, state, bytes_=None, reasons=(), stream=None, writer_types=None, primary='native'):
    target = ConversionTarget(
        stream or f'public-{table.lower()}',
        SnowflakeObjectName('DATABASE', 'SCHEMA', table),
        writer_types or {},
    )
    return ClassifiedTarget(target, state, reasons, None, bytes_, primary)


@contextmanager
def _gate(first, final=(), preflight=None, convert_error=None):
    converter = MagicMock()
    converter.preflight.side_effect = preflight or (lambda fqtn, writer_types: ())
    converter.convert.side_effect = convert_error
    with patch.object(native_to_iceberg, 'SnowflakeConversionQueryAdapter') as adapter, patch.object(
        native_to_iceberg, 'SnowflakeNativeToIcebergConverter', return_value=converter,
    ), patch.object(native_to_iceberg, 'classify', side_effect=[list(first), list(final)]) as classify:
        yield converter, adapter, classify


def _quoted(table):
    return f'"DATABASE"."SCHEMA"."{table}"'


def test_gate_resumes_pending_first_then_converts_smallest_first():
    """Interrupted cutovers close first; native tables convert from smallest to unknown size."""
    first = [
        _classified('DONE', STATE_ICEBERG),
        _classified('BIG', STATE_NATIVE, 900),
        _classified('HALF', STATE_PENDING),
        _classified('UNKNOWN', STATE_NATIVE),
        _classified('SMALL', STATE_NATIVE, 10),
        _classified('LATER', STATE_NEW),
    ]
    final = [_classified(name, STATE_ICEBERG) for name in ('DONE', 'BIG', 'HALF', 'UNKNOWN', 'SMALL')]

    with _gate(first, final) as (converter, adapter, classify):
        native_to_iceberg.convert_tap_tables({'dbname': 'database'}, '/runtime', ['targets'], 'WH')

    adapter.assert_called_once_with({'dbname': 'database'}, 'WH')
    assert converter.convert.call_args_list == [
        call(_quoted(table), eventual='iceberg', writer_types={})
        for table in ('HALF', 'SMALL', 'BIG', 'UNKNOWN')
    ]
    assert [entry.args[0] for entry in converter.preflight.call_args_list] == [
        _quoted(table) for table in ('SMALL', 'BIG', 'UNKNOWN')
    ]
    assert classify.call_count == 2


def test_gate_resumes_pending_conversions_before_refusing_blocked_tables():
    """An unrelated blocker never keeps an interrupted cutover's table name missing."""
    first = [
        _classified('USERS', STATE_BLOCKED, reasons=('"USERS_NATIVE" already exists',)),
        _classified('HALF', STATE_PENDING),
    ]

    with _gate(first) as (converter, _, _):
        with pytest.raises(NativeToIcebergConversionException, match='USERS .*needs an operator'):
            native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    assert converter.convert.call_args_list == [call(_quoted('HALF'), eventual='iceberg', writer_types={})]


def test_gate_skips_the_post_check_when_nothing_changed():
    """Once every table is Iceberg, a run lists each schema once."""
    first = [_classified('DONE', STATE_ICEBERG), _classified('LATER', STATE_NEW)]

    with _gate(first) as (converter, _, classify):
        native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    assert classify.call_count == 1
    converter.convert.assert_not_called()


def test_gate_names_the_exact_report_command_and_bounds_its_message():
    """The stop message points to the full report command and names at most 20 tables."""
    first = [_classified(f'T{index:02d}', STATE_BLOCKED, reasons=('reason',)) for index in range(23)]

    with _gate(first):
        with pytest.raises(NativeToIcebergConversionException) as error:
            native_to_iceberg.convert_tap_tables({}, '/runtime', [], target_id='snowflake', tap_id='orders')

    message = str(error.value)
    assert 'pipelinewise copy_native_to_iceberg --target snowflake --tap orders --preflight-only' in message
    assert '--iceberg-version 3' in message
    assert 'T19' in message and 'T20' not in message
    assert 'and 3 more tables listed in the log' in message


def test_gate_logs_unexpected_conversion_errors_with_traceback():
    """An unexpected converter error keeps its traceback in the tap log."""
    with _gate([_classified('A', STATE_NATIVE, 1)], convert_error=KeyError('owner_role_type')), patch.object(
        native_to_iceberg.LOGGER, 'exception',
    ) as log_exception:
        with pytest.raises(NativeToIcebergConversionException, match="failed: 'owner_role_type'"):
            native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    log_exception.assert_called_once_with('Unexpected error while converting %s', 'DATABASE.SCHEMA.A')


def test_gate_stops_on_blocked_tables_before_any_change():
    """A table that needs an operator stops the whole tap before anything converts."""
    first = [
        _classified('ORDERS', STATE_NATIVE, 10),
        _classified('USERS', STATE_BLOCKED, reasons=('"USERS_NATIVE" already exists',)),
    ]

    with _gate(first) as (converter, _, _):
        with pytest.raises(NativeToIcebergConversionException) as error:
            native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    message = str(error.value)
    assert 'DATABASE.SCHEMA.USERS (stream public-users) needs an operator' in message
    assert '"USERS_NATIVE" already exists' in message
    assert 'Replication did not start' in message
    converter.preflight.assert_not_called()
    converter.convert.assert_not_called()


def test_gate_preflights_every_table_before_converting_any():
    """One unconvertible table stops the run before even the clean tables change."""
    first = [_classified('CLEAN', STATE_NATIVE, 1), _classified('LOSSY', STATE_NATIVE, 2)]

    def preflight(fqtn, writer_types):
        return ('Column TS is TIMESTAMP_TZ',) if 'LOSSY' in fqtn else ()

    with _gate(first, preflight=preflight) as (converter, _, _):
        with pytest.raises(NativeToIcebergConversionException, match='LOSSY.*cannot be converted.*TIMESTAMP_TZ'):
            native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    converter.convert.assert_not_called()


def test_gate_stops_at_the_first_failed_conversion():
    """A failed conversion names its table, keeps earlier conversions, and skips later ones."""
    first = [_classified('A', STATE_NATIVE, 1), _classified('B', STATE_NATIVE, 2), _classified('C', STATE_NATIVE, 3)]

    with _gate(first, convert_error=[None, RuntimeError('copy failed')]) as (converter, _, classify):
        with pytest.raises(
            NativeToIcebergConversionException,
            match=r'conversion of DATABASE.SCHEMA.B \(stream public-b\) failed: copy failed',
        ):
            native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    assert converter.convert.call_count == 2
    assert classify.call_count == 1


def test_gate_post_check_requires_every_table_to_be_iceberg():
    """Replication starts only when every planned table is new or Iceberg."""
    with _gate([_classified('A', STATE_NATIVE, 1)], [_classified('A', STATE_PENDING)]):
        with pytest.raises(NativeToIcebergConversionException, match='A .*is still not Iceberg'):
            native_to_iceberg.convert_tap_tables({}, '/runtime', [])


def test_gate_checks_every_stream_of_a_shared_table():
    """A second stream's blocker on a shared table is never hidden by de-duplication."""
    first = [
        _classified('T', STATE_NATIVE, 1, stream='a-t'),
        _classified('T', STATE_BLOCKED, 1, reasons=('FastSync writes another table',), stream='b-t'),
    ]

    with _gate(first) as (converter, _, _):
        with pytest.raises(NativeToIcebergConversionException, match=r'T \(stream b-t\) needs an operator'):
            native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    converter.preflight.assert_not_called()
    converter.convert.assert_not_called()


def test_gate_refuses_streams_that_disagree_on_a_column_class():
    """Two streams expecting TEXT and VARIANT in one column would make a writer refuse it."""
    first = [
        _classified('T', STATE_NATIVE, 1, stream='a-t', writer_types={'DOC': 'TEXT'}),
        _classified('T', STATE_NATIVE, 1, stream='b-t', writer_types={'DOC': 'VARIANT'}),
    ]

    with _gate(first) as (converter, _, _):
        with pytest.raises(
            NativeToIcebergConversionException,
            match='Column DOC is TEXT for stream a-t but VARIANT for stream b-t',
        ):
            native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    converter.convert.assert_not_called()


def test_gate_passes_every_stream_class_to_preflight_and_conversion():
    """Preflight and conversion of a shared table use the classes of all of its streams."""
    first = [
        _classified('T', STATE_NATIVE, 1, stream='a-t', writer_types={'DOC': 'VARIANT'}),
        _classified('T', STATE_NATIVE, 1, stream='b-t', writer_types={'NOTE': 'TEXT'}),
    ]
    merged = {'DOC': 'VARIANT', 'NOTE': 'TEXT'}

    with _gate(first, [_classified('T', STATE_ICEBERG)]) as (converter, _, _):
        native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    converter.preflight.assert_called_once_with(_quoted('T'), merged)
    converter.convert.assert_called_once_with(_quoted('T'), eventual='iceberg', writer_types=merged)


def test_gate_converts_a_shared_table_once():
    """Two streams that write the same table convert it once."""
    first = [
        _classified('T', STATE_NATIVE, 1, stream='a-t'),
        _classified('T', STATE_NATIVE, 1, stream='b-t'),
    ]

    with _gate(first, [_classified('T', STATE_ICEBERG)]) as (converter, _, _):
        native_to_iceberg.convert_tap_tables({}, '/runtime', [])

    assert converter.convert.call_count == 1


def test_gate_wraps_unexpected_errors():
    """Discovery failures stop the tap with the same next action."""
    with patch.object(native_to_iceberg, 'SnowflakeConversionQueryAdapter'), patch.object(
        native_to_iceberg, 'classify', side_effect=RuntimeError('listing may be truncated'),
    ):
        with pytest.raises(NativeToIcebergConversionException, match='stopped: listing may be truncated'):
            native_to_iceberg.convert_tap_tables({}, '/runtime', [])


def test_gate_drives_the_real_converter(tmp_path):
    """A native table is preflighted, converted, and verified through the real converter."""
    snowflake = FakeSnowflake()

    with patch.object(native_to_iceberg, 'SnowflakeConversionQueryAdapter', return_value=snowflake):
        native_to_iceberg.convert_tap_tables({'dbname': 'database'}, str(tmp_path), [_target('TABLE')])

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not list(tmp_path.glob('iceberg-recovery-*.json'))


def test_tap_blockers_require_flattening_level_zero():
    """Automatic conversion needs the level-0 column mapping the preflight assumes."""
    iceberg = {'target_table_format': 'iceberg'}
    assert native_to_iceberg.tap_blockers({**iceberg, 'data_flattening_max_level': 0}) == []
    assert native_to_iceberg.tap_blockers(iceberg) == []
    assert native_to_iceberg.tap_blockers({**iceberg, 'data_flattening_max_level': 10}) == [
        'data_flattening_max_level is 10; native_to_iceberg_conversion requires 0'
    ]


def test_tap_blockers_wait_for_the_imported_iceberg_catalog():
    """A report on a catalog discovered with native settings can never claim ready."""
    blockers = native_to_iceberg.tap_blockers({'data_flattening_max_level': 0})

    assert len(blockers) == 1
    assert 'not imported with target_table_format: iceberg yet' in blockers[0]
    assert 'native_to_iceberg_conversion enabled: false' in blockers[0]


def test_report_preflights_pending_native_tables(tmp_path):
    """A pending attempt that resuming would refuse is shown as blocked, not ready."""
    snowflake = _submitted_attempt_over_temporal_column(tmp_path)
    snowflake.sub_microsecond_counts = {'UPDATED_AT': 2}

    reports = _report(tmp_path, snowflake, [_target('TABLE')])

    assert reports[0].state == STATE_PENDING
    assert reports[0].action == 'resolve the blockers'
    assert 'would truncate' in reports[0].blockers[0]
    assert native_to_iceberg.render_preflight_report(reports, [], 'json').count('"ready": false') == 1


def test_report_shows_column_class_conflicts_between_streams(tmp_path):
    """Streams that disagree on a shared table's column class make the report not ready."""
    snowflake = FakeSnowflake()
    targets = [
        ConversionTarget('a-table', SnowflakeObjectName('DATABASE', 'SCHEMA', 'TABLE'), {'ID': 'TEXT'}),
        ConversionTarget('b-table', SnowflakeObjectName('DATABASE', 'SCHEMA', 'TABLE'), {'ID': 'VARIANT'}),
    ]

    reports = _report(tmp_path, snowflake, targets)

    assert [report.blockers for report in reports] == [
        ('Column ID is TEXT for stream a-table but VARIANT for stream b-table',),
    ] * 2


def test_report_preflights_blocked_native_tables_too():
    """A blocked native table also shows its conversion blockers in the same report."""
    snowflake = FakeSnowflake()
    snowflake.formats = {'TABLE': 'native', 'TABLE_NATIVE': 'native'}
    snowflake.native_extra_columns = [{
        'COLUMN_NAME': 'CREATED_AT',
        'DATA_TYPE': 'TIMESTAMP_TZ',
        'IS_NULLABLE': 'YES',
        'COMMENT': None,
        'COLUMN_DEFAULT': None,
        'IS_IDENTITY': 'NO',
    }]

    with patch.object(native_to_iceberg, 'SnowflakeConversionQueryAdapter', return_value=snowflake):
        reports = native_to_iceberg.preflight_report({'dbname': 'database'}, '/runtime', [_target('TABLE')])

    assert reports[0].state == STATE_BLOCKED
    assert len(reports[0].blockers) == 2
    assert 'TABLE_NATIVE" already exists' in reports[0].blockers[0]
    assert 'Column CREATED_AT is TIMESTAMP_TZ' in reports[0].blockers[1]
