"""run_tap converts a tap's native Snowflake tables before any writer runs."""

import json
from unittest.mock import MagicMock, call, patch

import pidfile
import pytest

from pipelinewise.cli.errors import NativeToIcebergConversionException
from pipelinewise.cli.pipelinewise import PipelineWise
from pipelinewise.fastsync.commons.snowflake_iceberg_conversion_plan import ConversionTarget
from pipelinewise.fastsync.commons.snowflake_iceberg_model import SnowflakeObjectName
from tests.units.cli.cli_args import CliArgs


CONFIG_DIR = 'tests/units/cli/resources/sample_json_config'
VIRTUALENVS_DIR = './virtualenvs-dummy'
STREAMS = ['db_test_mysql-table_one', 'db_test_mysql-table_two']


@pytest.fixture(name='pipelinewise')
def _pipelinewise():
    instance = PipelineWise(CliArgs(target='target_one', tap='tap_one'), CONFIG_DIR, VIRTUALENVS_DIR)
    instance.tap['target_table_format'] = 'iceberg'
    instance.tap['iceberg_version'] = 3
    instance.tap['native_to_iceberg_conversion'] = {'enabled': True, 'conversion_warehouse': 'CONVERSION_WH'}
    # The sample tap's generated files are native; this fixture models a tap imported as Iceberg v3.
    imported = instance._iceberg_target_config
    instance._iceberg_target_config = lambda: {**imported(), 'target_table_format': 'iceberg', 'iceberg_version': 3}
    instance._stale_catalog_reasons = lambda: []
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


def test_a_stale_catalog_refuses_conversions(pipelinewise):
    """A catalog the last import did not rediscover stops new conversions, not finished tables."""
    pipelinewise._stale_catalog_reasons = lambda: ['stale catalog']
    with patch('pipelinewise.cli.native_to_iceberg.convert_tap_tables') as convert_tables:
        pipelinewise._run_native_to_iceberg_conversion(STREAMS[:1], None)

    assert convert_tables.call_args.kwargs['tap_refusals'] == ['stale catalog']


def test_main_config_alone_never_converts_a_tap_imported_as_native():
    """A tap whose generated files lack its Iceberg settings stops before any Snowflake work."""
    instance = PipelineWise(CliArgs(target='target_one', tap='tap_one'), CONFIG_DIR, VIRTUALENVS_DIR)
    instance.tap['native_to_iceberg_conversion'] = {'enabled': True}

    with patch.object(instance, '_run_in_child_process') as run_child:
        with pytest.raises(NativeToIcebergConversionException, match='imported configuration of tap tap_one'):
            instance._convert_native_tables_to_iceberg(STREAMS)

    run_child.assert_not_called()


def test_conversion_plans_the_selected_streams(pipelinewise):
    """The child plans the run's streams with the tap's schema mapping and conversion warehouse."""
    with patch('pipelinewise.cli.native_to_iceberg.convert_tap_tables') as convert_tables:
        pipelinewise._run_native_to_iceberg_conversion(STREAMS[:1], 'CONVERSION_WH')

    target_config, runtime_dir, targets, conversion_warehouse = convert_tables.call_args.args
    assert convert_tables.call_args.kwargs == {
        'other_writers': [], 'tap_refusals': [], 'target_id': 'target_one', 'tap_id': 'tap_one',
    }
    assert target_config['dbname'] == 'my_db'
    assert target_config['schema_mapping']['db_test_mysql']['target_schema'] == 'db_test_mysql'
    assert runtime_dir == f'{CONFIG_DIR}/target_one'
    assert [(target.stream_id, target.table.key, target.blockers) for target in targets] == [
        ('db_test_mysql-table_one', 'MY_DB.DB_TEST_MYSQL.TABLE_ONE', ()),
    ]
    assert conversion_warehouse == 'CONVERSION_WH'


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


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding='utf-8')


ICEBERG_FILES = {'target_table_format': 'iceberg', 'iceberg_version': 3}
CONVERTS = {'native_to_iceberg_conversion': {'enabled': True}}


def _project(tmp_path):
    """Write a config dir where orders converts and other taps write some of its tables."""
    taps = {
        'snow': [
            ('orders', ICEBERG_FILES, ['public-orders', 'public-items'], CONVERTS),
            ('legacy', {}, ['public-orders'], {}),
            ('mirror', ICEBERG_FILES, ['public-items'], {}),
            ('unrelated', {}, ['public-users'], {}),
        ],
        'snow_too': [('twin', ICEBERG_FILES, ['public-items'], CONVERTS)],
        'elsewhere': [('far', {}, ['public-orders'], {})],
        'csv': [('dump', {}, ['public-orders'], {})],
    }
    targets = {
        'snow': ('target-snowflake', {'account': 'ACC', 'dbname': 'analytics', 'user': 'loader'}),
        'snow_too': ('target-snowflake', {'account': 'acc', 'dbname': 'ANALYTICS', 'user': 'loader'}),
        'elsewhere': ('target-snowflake', {'account': 'acc', 'dbname': 'other_db', 'user': 'loader'}),
        'csv': ('target-s3-csv', {'s3_bucket': 'bucket'}),
    }
    main = {'targets': []}
    for target_id, (target_type, target_config) in targets.items():
        _write_json(tmp_path / target_id / 'config.json', target_config)
        entries = []
        for tap_id, files_format, streams, settings in taps[target_id]:
            entries.append({'id': tap_id, 'name': tap_id, 'type': 'tap-postgres', **files_format, **settings})
            tap_dir = tmp_path / target_id / tap_id
            _write_json(tap_dir / 'config.json', {})
            _write_json(tap_dir / 'inheritable_config.json', {
                'schema_mapping': {'public': {'target_schema': 'sales'}}, **files_format,
            })
            _write_json(tap_dir / 'selection.json', {
                'selection': [{'tap_stream_id': stream, 'replication_method': 'LOG_BASED'} for stream in streams],
            })
            _write_json(tap_dir / 'properties.json', {'streams': [
                {
                    'tap_stream_id': stream,
                    'schema': {'properties': {
                        'doc': {'type': ['null', 'object']},
                        'address': {'type': ['null', 'object'], 'properties': {'city': {'type': ['null', 'string']}}},
                    }},
                    'metadata': [{'breadcrumb': [], 'metadata': {'selected': True}}],
                }
                for stream in streams
            ]})
        main['targets'].append({'id': target_id, 'name': target_id, 'type': target_type, 'taps': entries})
    _write_json(tmp_path / 'config.json', main)
    return PipelineWise(CliArgs(target='snow', tap='orders'), str(tmp_path), VIRTUALENVS_DIR)


PLANNED = [
    ConversionTarget(stream, SnowflakeObjectName('ANALYTICS', 'SALES', table))
    for stream, table in (('public-orders', 'ORDERS'), ('public-items', 'ITEMS'))
]


def test_other_writers_cover_every_tap_writing_a_planned_table(tmp_path):
    """Taps under any target on the same database count, refused unless they write Iceberg safely."""
    instance = _project(tmp_path)

    writers = instance._other_writers({'account': 'ACC', 'dbname': 'analytics'}, PLANNED)

    assert sorted((writer.target.stream_id, writer.target.table.key) for writer in writers) == [
        ('public-items of tap mirror', 'ANALYTICS.SALES.ITEMS'),
        ('public-items of tap twin of target snow_too', 'ANALYTICS.SALES.ITEMS'),
        ('public-orders of tap legacy', 'ANALYTICS.SALES.ORDERS'),
    ]
    refusals = {writer.target.stream_id: writer.refusal for writer in writers}
    assert refusals['public-items of tap mirror'] is None
    assert 'does not write managed Iceberg v3' in refusals['public-orders of tap legacy']
    assert 'two targets cannot coordinate a conversion' in refusals['public-items of tap twin of target snow_too']
    mirror = next(writer for writer in writers if writer.refusal is None)
    assert mirror.target.writer_types == {'DOC': 'VARIANT', 'ADDRESS': 'VARIANT'}


def test_other_writers_skip_a_target_that_cannot_be_read(tmp_path):
    """An unreadable target config is logged and skipped instead of stopping the tap."""
    instance = _project(tmp_path)
    (tmp_path / 'snow_too' / 'config.json').write_text(
        '{"account": "{{ env_var[\'MISSING_VAR_FOR_THIS_TEST\'] }}"}', encoding='utf-8',
    )

    with patch.object(instance.logger, 'warning') as warning:
        writers = instance._other_writers({'account': 'ACC', 'dbname': 'analytics'}, PLANNED[1:])

    assert [writer.target.stream_id for writer in writers] == ['public-items of tap mirror']
    assert warning.call_args.args[1] == 'snow_too'


def test_writer_classes_skip_only_the_objects_a_flattening_tap_splits(tmp_path):
    """A tap that flattens deeper writes an object with properties as child columns; other classes still count."""
    instance = _project(tmp_path)
    settings = tmp_path / 'snow' / 'mirror' / 'inheritable_config.json'
    settings.write_text(json.dumps({
        'schema_mapping': {'public': {'target_schema': 'sales'}}, 'data_flattening_max_level': 10, **ICEBERG_FILES,
    }), encoding='utf-8')

    imported = instance._imported_tap_targets({'dbname': 'x'})
    writers = instance._other_writers({'account': 'ACC', 'dbname': 'analytics'}, PLANNED)

    assert [target.writer_types for target in imported if target.stream_id == 'public-items'] == [
        {'DOC': 'VARIANT', 'ADDRESS': 'VARIANT'},  # orders, at level 0
        {'DOC': 'VARIANT'},  # mirror, at level 10
    ]
    mirror = next(writer for writer in writers if 'mirror' in writer.target.stream_id)
    assert mirror.target.writer_types == {'DOC': 'VARIANT'}
