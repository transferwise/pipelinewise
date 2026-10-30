import json
import signal

from unittest.mock import call, patch

import pytest

from pipelinewise import cli
from pipelinewise.cli.errors import CommandSpecificArgumentsException
from pipelinewise.cli.errors import PreRunChecksException
from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs


CONFIG_DIR = 'tests/units/cli/resources/sample_json_config'
VIRTUALENVS_DIR = './virtualenvs-dummy'


def _args(**overrides):
    values = {
        'command': 'copy_native_to_iceberg',
        'target': 'snowflake',
        'table': 'DATABASE.SCHEMA.TABLE',
        'iceberg_version': 3,
    }
    values.update(overrides)
    return CliArgs(**values)


def test_accepts_valid_conversion_args():
    """The manual conversion command accepts its complete argument set."""
    cli._validate_command_specific_arguments(_args())


@pytest.mark.parametrize(
    ('override', 'message'),
    [
        (
            {'target': '*'},
            'You must specify a destination name using the argument --target',
        ),
        (
            {'target': ''},
            'You must specify a destination name using the argument --target',
        ),
        (
            {'target': '   '},
            'You must specify a destination name using the argument --target',
        ),
        (
            {'table': '*'},
            'You must specify a fully qualified Snowflake table using the argument --table',
        ),
        (
            {'table': ''},
            'You must specify a fully qualified Snowflake table using the argument --table',
        ),
        (
            {'table': '\t '},
            'You must specify a fully qualified Snowflake table using the argument --table',
        ),
        (
            {'iceberg_version': None},
            'You must explicitly specify Iceberg version 3 using --iceberg-version 3',
        ),
        (
            {'iceberg_version': 2},
            'You must explicitly specify Iceberg version 3 using --iceberg-version 3',
        ),
        (
            {'iceberg_version': 4},
            'You must explicitly specify Iceberg version 3 using --iceberg-version 3',
        ),
        (
            {'iceberg_version': 3.0},
            'You must explicitly specify Iceberg version 3 using --iceberg-version 3',
        ),
    ],
)
def test_rejects_invalid_conversion_args(override, message):
    """Missing or unsupported conversion arguments fail before dispatch."""
    with pytest.raises(CommandSpecificArgumentsException, match=message):
        cli._validate_command_specific_arguments(_args(**override))


@patch(
    'pipelinewise.fastsync.commons.snowflake_iceberg_converter.'
    'SnowflakeNativeToIcebergConverter'
)
@patch('pipelinewise.fastsync.commons.snowflake_iceberg.SnowflakeConversionQueryAdapter')
def test_dispatches_target_only_conversion(mock_adapter, mock_converter):
    """The root command uses generated target credentials without resolving a tap."""
    args = _args(target='target_one', eventual='iceberg')
    pipelinewise = PipelineWise(args, CONFIG_DIR, VIRTUALENVS_DIR)

    pipelinewise.copy_native_to_iceberg()

    target_config = cli.utils.load_json(
        f'{CONFIG_DIR}/target_one/config.json'
    )
    mock_adapter.assert_called_once_with(target_config)
    mock_converter.assert_called_once_with(
        mock_adapter.return_value,
        runtime_dir=f'{CONFIG_DIR}/target_one',
    )
    mock_converter.return_value.convert.assert_called_once_with(
        'DATABASE.SCHEMA.TABLE',
        eventual='iceberg',
        iceberg_version=3,
    )


@patch('pipelinewise.cli.pipelinewise.signal.signal')
def test_target_only_signal_exits(mock_signal):
    """SIGTERM exits safely without asking a missing tap to stop."""
    pipelinewise = PipelineWise(
        _args(target='target_one'),
        CONFIG_DIR,
        VIRTUALENVS_DIR,
    )
    handler = pipelinewise._stop_command_on_signal

    assert mock_signal.call_args_list == [
        call(signal.SIGINT, handler),
        call(signal.SIGTERM, handler),
    ]
    with patch.object(pipelinewise, 'stop_tap') as stop_tap:
        with pytest.raises(SystemExit) as error:
            handler(signal.SIGTERM, None)

    assert error.value.code == 1
    stop_tap.assert_not_called()


@pytest.mark.parametrize(
    'scope',
    [{'tap': 'tap_one', 'table': '*'}, {'tap': '*', 'table': 'DATABASE.SCHEMA.TABLE'}],
)
def test_accepts_preflight_scopes(scope):
    """A feasibility report covers either one tap or one table."""
    cli._validate_command_specific_arguments(_args(preflight_only=True, **scope))


@pytest.mark.parametrize(
    ('override', 'message'),
    [
        ({'preflight_only': True, 'tap': 'tap_one'}, 'either a source name using --tap'),
        ({'preflight_only': True, 'table': '*'}, 'either a source name using --tap'),
        ({'preflight_only': True, 'table': '*', 'iceberg_version': None}, 'either a source name'),
        ({'tap': 'tap_one'}, '--tap requires --preflight-only'),
        ({'preflight_only': True, 'iceberg_version': None}, '--iceberg-version 3'),
    ],
)
def test_rejects_invalid_preflight_scopes(override, message):
    """A report needs exactly one scope, and only reports accept a tap."""
    with pytest.raises(CommandSpecificArgumentsException, match=message):
        cli._validate_command_specific_arguments(_args(**override))


@patch('pipelinewise.cli.pipelinewise.signal.signal')
def test_tap_scoped_report_never_stops_the_tap(mock_signal):
    """A report beside a running tap exits on SIGTERM without touching its pidfile."""
    pipelinewise = PipelineWise(
        _args(target='target_one', tap='tap_one', table='*', preflight_only=True),
        CONFIG_DIR,
        VIRTUALENVS_DIR,
    )
    handler = pipelinewise._stop_command_on_signal

    assert mock_signal.call_args_list == [
        call(signal.SIGINT, handler),
        call(signal.SIGTERM, handler),
    ]


@patch('pipelinewise.cli.native_to_iceberg.preflight_report', return_value=[])
def test_tap_scoped_report_plans_selected_streams(mock_report, capsys):
    """A tap report plans the selected streams with the tap's schema mapping."""
    pipelinewise = PipelineWise(
        _args(
            target='target_one',
            tap='tap_one',
            table='*',
            preflight_only=True,
            output_format='json',
        ),
        CONFIG_DIR,
        VIRTUALENVS_DIR,
    )

    pipelinewise.copy_native_to_iceberg()

    target_config, runtime_dir, targets, warehouse = mock_report.call_args.args
    assert runtime_dir == f'{CONFIG_DIR}/target_one'
    assert warehouse is None
    assert target_config['dbname'] == 'my_db'
    assert target_config['data_flattening_max_level'] == 0
    assert [(target.stream_id, target.table.key, target.blockers) for target in targets] == [
        ('db_test_mysql-table_one', 'MY_DB.DB_TEST_MYSQL.TABLE_ONE', ()),
        ('db_test_mysql-table_two', 'MY_DB.DB_TEST_MYSQL.TABLE_TWO', ()),
    ]
    assert json.loads(capsys.readouterr().out) == {'ready': True, 'blockers': [], 'tables': []}


@patch('pipelinewise.cli.native_to_iceberg.preflight_report', return_value=[])
def test_table_scoped_report_uses_the_named_table(mock_report, capsys):
    """A table report needs no tap and plans exactly the named table."""
    pipelinewise = PipelineWise(
        _args(target='target_one', preflight_only=True),
        CONFIG_DIR,
        VIRTUALENVS_DIR,
    )

    pipelinewise.copy_native_to_iceberg()

    _, _, targets, warehouse = mock_report.call_args.args
    assert warehouse is None
    assert [target.table.key for target in targets] == ['DATABASE.SCHEMA.TABLE']
    assert capsys.readouterr().out.strip().endswith('Ready: no blockers found.')


@patch(
    'pipelinewise.fastsync.commons.snowflake_iceberg_converter.'
    'SnowflakeNativeToIcebergConverter'
)
def test_rejects_non_snowflake_target(mock_converter):
    """The target-only command cannot operate on another connector type."""
    pipelinewise = PipelineWise(
        _args(target='target_two'),
        CONFIG_DIR,
        VIRTUALENVS_DIR,
    )

    with pytest.raises(PreRunChecksException, match='target-snowflake'):
        pipelinewise.copy_native_to_iceberg()

    mock_converter.assert_not_called()
