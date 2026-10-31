"""run_tap converts a tap's native Snowflake tables before any writer runs."""

from unittest.mock import MagicMock, call, patch

import pidfile
import pytest

from pipelinewise.cli.errors import NativeToIcebergConversionException
from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs


CONFIG_DIR = 'tests/units/cli/resources/sample_json_config'
VIRTUALENVS_DIR = './virtualenvs-dummy'
STREAMS = ['db_test_mysql-table_one', 'db_test_mysql-table_two']


@pytest.fixture(name='pipelinewise')
def _pipelinewise():
    instance = PipelineWise(CliArgs(target='target_one', tap='tap_one'), CONFIG_DIR, VIRTUALENVS_DIR)
    instance.tap['target_table_format'] = 'iceberg'
    instance.tap['iceberg_version'] = 3
    instance.tap['native_to_iceberg_conversion'] = {'enabled': True, 'warehouse': 'CONVERSION_WH'}
    return instance


@pytest.mark.parametrize(
    ('conversion', 'stream_ids'),
    [(None, STREAMS), ({'enabled': False}, STREAMS), ({'enabled': True}, [])],
)
def test_conversion_is_skipped_unless_enabled_with_streams(pipelinewise, conversion, stream_ids):
    """Taps without the setting, or with nothing selected, never touch Snowflake here."""
    pipelinewise.tap['native_to_iceberg_conversion'] = conversion

    with patch.object(pipelinewise, '_run_in_child_process') as run_child:
        pipelinewise._convert_native_tables_to_iceberg(stream_ids)

    run_child.assert_not_called()


def test_enabled_conversion_runs_in_a_child_process(pipelinewise):
    """Conversion runs in a child so later FastSync forks inherit no Snowflake state."""
    with patch.object(pipelinewise, '_run_in_child_process') as run_child:
        pipelinewise._convert_native_tables_to_iceberg(STREAMS)

    run_child.assert_called_once_with(
        pipelinewise._run_native_to_iceberg_conversion,
        STREAMS,
        'CONVERSION_WH',
    )


def test_conversion_plans_the_selected_streams(pipelinewise):
    """The child plans the run's streams with the tap's schema mapping and warehouse."""
    with patch('pipelinewise.cli.native_to_iceberg.convert_tap_tables') as convert_tables:
        pipelinewise._run_native_to_iceberg_conversion(STREAMS[:1], 'CONVERSION_WH')

    target_config, runtime_dir, targets, warehouse = convert_tables.call_args.args
    assert convert_tables.call_args.kwargs == {'target_id': 'target_one', 'tap_id': 'tap_one'}
    assert target_config['dbname'] == 'my_db'
    assert target_config['schema_mapping']['db_test_mysql']['target_schema'] == 'db_test_mysql'
    assert runtime_dir == f'{CONFIG_DIR}/target_one'
    assert [(target.stream_id, target.table.key, target.blockers) for target in targets] == [
        ('db_test_mysql-table_one', 'MY_DB.DB_TEST_MYSQL.TABLE_ONE', ()),
    ]
    assert warehouse == 'CONVERSION_WH'


def test_dead_conversion_child_still_alerts(pipelinewise):
    """A child killed without reporting becomes a conversion stop, so run_tap alerts."""
    with patch.object(
        pipelinewise, '_run_in_child_process', side_effect=ChildProcessError('exited with status -9'),
    ):
        with pytest.raises(NativeToIcebergConversionException) as error:
            pipelinewise._convert_native_tables_to_iceberg(STREAMS)

    assert 'stopped: exited with status -9' in str(error.value)
    assert '--target target_one --tap tap_one --preflight-only' in str(error.value)


def _run_tap(pipelinewise, tmp_path):
    calls = MagicMock()
    properties = tmp_path / 'properties.json'
    properties.write_text('{"streams": []}', encoding='utf-8')
    with patch.object(
        pipelinewise,
        'create_filtered_tap_properties',
        return_value=(str(properties), STREAMS[:1], str(properties), STREAMS[1:]),
    ), patch.object(
        pipelinewise, '_run_in_child_process',
    ) as run_child, patch.object(
        pipelinewise, 'do_sync_tables',
    ) as do_sync_tables, patch.object(
        pipelinewise, 'run_tap_singer',
    ) as run_tap_singer, patch.object(
        pipelinewise, 'send_alert',
    ) as send_alert, patch.object(
        pipelinewise, '_print_tap_run_summary',
    ) as summary:
        calls.attach_mock(run_child, 'convert')
        calls.attach_mock(do_sync_tables, 'fastsync')
        calls.attach_mock(run_tap_singer, 'singer')
        pipelinewise.run_tap()
    return calls, send_alert, summary


def test_run_tap_converts_before_fastsync_and_singer(pipelinewise, tmp_path):
    """Writers start only after the conversion step has left every table Iceberg."""
    calls, send_alert, summary = _run_tap(pipelinewise, tmp_path)

    assert [entry[0] for entry in calls.mock_calls] == ['convert', 'fastsync', 'singer']
    assert calls.mock_calls[0] == call.convert(
        pipelinewise._run_native_to_iceberg_conversion, STREAMS, 'CONVERSION_WH',
    )
    send_alert.assert_not_called()
    assert summary.call_args.args[0] == PipelineWise.STATUS_SUCCESS


def test_conversion_runs_inside_the_tap_pidfile(pipelinewise, tmp_path):
    """The pidfile keeps every other writer of the tap out for the whole conversion."""
    calls = MagicMock()
    pid_file = MagicMock()
    calls.attach_mock(pid_file.return_value.__enter__, 'enter_pidfile')
    calls.attach_mock(pid_file.return_value.__exit__, 'exit_pidfile')
    properties = tmp_path / 'properties.json'
    properties.write_text('{"streams": []}', encoding='utf-8')
    with patch('pipelinewise.cli.pipelinewise.pidfile.PIDFile', pid_file), patch.object(
        pipelinewise,
        'create_filtered_tap_properties',
        return_value=(str(properties), STREAMS[:1], str(properties), STREAMS[1:]),
    ), patch.object(pipelinewise, '_run_in_child_process') as run_child, patch.object(
        pipelinewise, 'do_sync_tables',
    ) as do_sync_tables, patch.object(pipelinewise, 'run_tap_singer') as run_tap_singer:
        calls.attach_mock(run_child, 'convert')
        calls.attach_mock(do_sync_tables, 'fastsync')
        calls.attach_mock(run_tap_singer, 'singer')
        pipelinewise.run_tap()

    assert [entry[0] for entry in calls.mock_calls] == [
        'enter_pidfile', 'convert', 'fastsync', 'singer', 'exit_pidfile',
    ]
    pid_file.assert_called_once_with(pipelinewise.tap['files']['pidfile'])


def test_running_instance_prevents_conversion(pipelinewise, tmp_path):
    """A second run_tap of the same tap never starts converting."""
    properties = tmp_path / 'properties.json'
    properties.write_text('{"streams": []}', encoding='utf-8')
    with patch(
        'pipelinewise.cli.pipelinewise.pidfile.PIDFile',
        side_effect=pidfile.AlreadyRunningError,
    ), patch.object(
        pipelinewise,
        'create_filtered_tap_properties',
        return_value=(str(properties), STREAMS[:1], str(properties), STREAMS[1:]),
    ), patch.object(pipelinewise, '_run_in_child_process') as run_child:
        with pytest.raises(SystemExit):
            pipelinewise.run_tap()

    run_child.assert_not_called()


def test_failed_conversion_stops_the_tap_and_alerts(pipelinewise, tmp_path):
    """A conversion failure fails the whole tap closed; nothing replicates into native tables."""
    error = NativeToIcebergConversionException('DATABASE.SCHEMA.TABLE cannot be converted')
    properties = tmp_path / 'properties.json'
    properties.write_text('{"streams": []}', encoding='utf-8')

    with patch.object(
        pipelinewise,
        'create_filtered_tap_properties',
        return_value=(str(properties), STREAMS[:1], str(properties), STREAMS[1:]),
    ), patch.object(pipelinewise, '_run_in_child_process', side_effect=error), patch.object(
        pipelinewise, 'do_sync_tables',
    ) as do_sync_tables, patch.object(
        pipelinewise, 'run_tap_singer',
    ) as run_tap_singer, patch.object(
        pipelinewise, 'send_alert',
    ) as send_alert, patch.object(
        pipelinewise, '_print_tap_run_summary',
    ) as summary:
        with pytest.raises(SystemExit) as exit_info:
            pipelinewise.run_tap()

    assert exit_info.value.code == 1
    do_sync_tables.assert_not_called()
    run_tap_singer.assert_not_called()
    send_alert.assert_called_once_with(
        message='tap_one tap failed: native-to-Iceberg conversion stopped',
        exc=error,
    )
    assert summary.call_args.args[0] == PipelineWise.STATUS_FAILED
