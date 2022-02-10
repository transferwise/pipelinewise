import os
import sys
import pytest


from tempfile import TemporaryDirectory
from unittest import mock

from pipelinewise.cli import commands
from pipelinewise.cli.errors import StreamBufferTooLargeException


# pylint: disable=no-self-use,fixme
class TestCommands:
    """
    Unit tests for PipelineWise CLI commands functions
    """

    def test_exists_and_executable(self):
        """Tests the function that detect if a file exists and executable"""
        if sys.platform != 'win32':
            # Should be true if absolute path given to binary executable
            assert commands.exists_and_executable('/bin/ls') is True
            # Should be true if executable available via the PATH environment variable
            assert commands.exists_and_executable('ls') is True

        # Should be false if file not exists
        assert commands.exists_and_executable('invalid_executable') is False
        # Should be false if file exists but not executable
        with TemporaryDirectory() as temp_dir:
            with open(f'{temp_dir}/test.tmp', 'w', encoding='utf-8') as tmp_file:
                tmp_file.write('foo')
            assert commands.exists_and_executable(f'{temp_dir}/test.tmp') is False

    @mock.patch('pipelinewise.cli.commands._verify_json_file', mock.MagicMock(return_value=True))
    def test_build_tap_command(self):
        """Tests the function that generates tap executable command"""

        # we are using some config files which does not exist, so we patch the method that verifies the json files
        # State file should not be included if state file path not passed

        tap = commands.TapParams(
            id='my_tap_mysql',
            type='tap_mysql',
            bin='/bin/tap_mysql.py',
            python_bin='/bin/python3',
            config='.ppw/config.json',
            properties='.ppw/properties.json',
            state=None,
        )

        # profiling is not enabled
        command = commands.build_tap_command(
            tap=tap, profiling_mode=False, profiling_dir=None
        )

        assert (
            command
            == '/bin/tap_mysql.py --config .ppw/config.json --catalog .ppw/properties.json '
        )

        # profiling is enabled
        command = commands.build_tap_command(
            tap=tap, profiling_mode=True, profiling_dir='./profiling'
        )

        assert (
            command
            == '/bin/python3 -m cProfile -o ./profiling/tap_my_tap_mysql.pstat /bin/tap_mysql.py '
            '--config .ppw/config.json --catalog .ppw/properties.json '
        )

        # State file should not be included if state file passed but file not exists
        tap = commands.TapParams(
            id='my_tap_mysql',
            type='tap_mysql',
            bin='/bin/tap_mysql.py',
            python_bin='/bin/python3',
            config='.ppw/config.json',
            properties='.ppw/properties.json',
            state='.pipelinewise/state.json',
        )

        # profiling is not enabled
        command = commands.build_tap_command(
            tap=tap, profiling_dir=None, profiling_mode=False
        )

        assert (
            command
            == '/bin/tap_mysql.py --config .ppw/config.json --catalog .ppw/properties.json '
        )

        # profiling is enabled
        command = commands.build_tap_command(
            tap=tap, profiling_mode=True, profiling_dir='./profiling'
        )

        assert (
            command
            == '/bin/python3 -m cProfile -o ./profiling/tap_my_tap_mysql.pstat /bin/tap_mysql.py '
            '--config .ppw/config.json --catalog .ppw/properties.json '
        )

        # State file should be included if state file passed and file exists
        state_mock = __file__

        tap = commands.TapParams(
            id='my_tap_mysql',
            type='tap_mysql',
            bin='/bin/tap_mysql.py',
            python_bin='/bin/python3',
            config='.ppw/config.json',
            properties='.ppw/properties.json',
            state=state_mock,
        )
        # profiling is not enabled
        command = commands.build_tap_command(
            tap=tap, profiling_mode=False, profiling_dir=None
        )

        assert (
            command
            == f'/bin/tap_mysql.py --config .ppw/config.json --catalog .ppw/properties.json '
            f'--state {state_mock}'
        )

        # profiling is enabled
        command = commands.build_tap_command(
            tap=tap, profiling_mode=True, profiling_dir='./profiling'
        )

        assert (
            command
            == f'/bin/python3 -m cProfile -o ./profiling/tap_my_tap_mysql.pstat /bin/tap_mysql.py '
            f'--config .ppw/config.json --catalog .ppw/properties.json --state {state_mock}'
        )

    @mock.patch('pipelinewise.cli.commands._verify_json_file', mock.MagicMock(return_value=True))
    def test_build_target_command(self):
        """Tests the function that generates target executable command"""

        # we are using some config files which does not exist, so we patch the method that verifies the json files
        # Should return a input piped command with an executable target command

        target = commands.TargetParams(
            id='my_target',
            type='target-snowflake',
            bin='/bin/target_postgres.py',
            python_bin='/bin/python',
            config='.ppw/config.json',
        )

        # profiling is not enabled
        command = commands.build_target_command(
            target=target, profiling_mode=False, profiling_dir=None
        )

        assert command == '/bin/target_postgres.py --config .ppw/config.json'

        # profiling is enabled
        command = commands.build_target_command(
            target=target, profiling_mode=True, profiling_dir='./profiling'
        )
        assert (
            command
            == '/bin/python -m cProfile -o ./profiling/target_my_target.pstat /bin/target_postgres.py'
            ' --config .ppw/config.json'
        )

    def test_build_transform_command(self):
        """Tests the function that generates transform executable command"""
        # Should return empty string if config file exists but no transformation
        transform_config = '{}/resources/transform-config-empty.json'.format(
            os.path.dirname(__file__)
        )
        transform_bin = '/bin/transform_field.py'

        transform = commands.TransformParams(
            config=transform_config,
            bin=transform_bin,
            python_bin='/bin/python',
            tap_id='my_tap',
            target_id='my_target',
        )
        # profiling disabled
        command = commands.build_transformation_command(
            transform=transform, profiling_mode=False, profiling_dir=None
        )
        assert command is None

        # profiling enabled
        command = commands.build_transformation_command(
            transform=transform, profiling_mode=True, profiling_dir='./profiling'
        )
        assert command is None

        # Should return a input piped command with an executable transform command
        transform_config = '{}/resources/transform-config.json'.format(
            os.path.dirname(__file__)
        )

        transform = commands.TransformParams(
            config=transform_config,
            bin=transform_bin,
            python_bin='/bin/python',
            tap_id='my_tap',
            target_id='my_target',
        )
        # profiling disabled
        command = commands.build_transformation_command(
            transform=transform, profiling_mode=False, profiling_dir=None
        )

        assert command == f'/bin/transform_field.py --config {transform_config}'

        # profiling enabled
        command = commands.build_transformation_command(
            transform=transform, profiling_mode=True, profiling_dir='./profiling'
        )

        assert (
            command
            == f'/bin/python -m cProfile -o ./profiling/transformation_my_tap_my_target.pstat '
            f'/bin/transform_field.py --config {transform_config}'
        )

    # pylint: disable=invalid-name
    def test_build_stream_buffer_command(self):
        """Tests the function that generates stream buffer executable command"""
        # Should return empty string if buffer size is invalid or too small
        assert commands.build_stream_buffer_command() is None
        assert commands.build_stream_buffer_command(buffer_size=None) is None
        assert commands.build_stream_buffer_command(buffer_size=0) is None
        assert commands.build_stream_buffer_command(buffer_size=-10) is None

        # Should use the minimum buffer size if enabled but less than minimal buffer size
        assert (
            commands.build_stream_buffer_command(buffer_size=1)
            == f'mbuffer -m {commands.MIN_STREAM_BUFFER_SIZE}M'
        )

        # Should raise StreamBufferTooLargeException if buffer_size is greater than the max allowed
        with pytest.raises(StreamBufferTooLargeException):
            commands.build_stream_buffer_command(
                buffer_size=commands.MAX_STREAM_BUFFER_SIZE + 1000
            )

        # Should use custom buffer size if between max and min buffer size
        assert (
            commands.build_stream_buffer_command(buffer_size=100) == 'mbuffer -m 100M'
        )

        # Should use custom buffer binary executable if bin parameter provided
        assert (
            commands.build_stream_buffer_command(
                buffer_size=100, stream_buffer_bin='dummy_buffer'
            )
            == 'dummy_buffer -m 100M'
        )

        # Should log mbuffer status to log file with .running extension
        assert (
            commands.build_stream_buffer_command(
                buffer_size=100, log_file='stream_buffer.log'
            )
            == 'mbuffer -m 100M -q -l stream_buffer.log.running'
        )

    @mock.patch('pipelinewise.cli.commands._verify_json_file', mock.MagicMock(return_value=True))
    def test_build_singer_command(self):
        """Tests the function that generates the full singer singer command
        that connects the required components with linux pipes"""

        # we are using some config files which does not exist, so we patch the method that verifies the json files

        transform_config = '{}/resources/transform-config.json'.format(
            os.path.dirname(__file__)
        )
        transform_config_empty = '{}/resources/transform-config-empty.json'.format(
            os.path.dirname(__file__)
        )
        state_mock = __file__

        # Should generate a command with tap state and transformation
        tap_params = commands.TapParams(
            id='my_tap',
            type='tap-mysql',
            bin='/bin/tap_mysql.py',
            python_bin='/bin/python',
            config='.ppw/config.json',
            properties='.ppw/properties.json',
            state=state_mock,
        )

        target_params = commands.TargetParams(
            id='my_target',
            type='target-postgres',
            bin='/bin/target_postgres.py',
            python_bin='/bin/python',
            config='.ppw/config.json',
        )

        transform_params = commands.TransformParams(
            bin='/bin/transform_field.py',
            python_bin='/bin/python',
            config=transform_config,
            tap_id='my_tap',
            target_id='my_target',
        )
        # profiling disabled
        command = commands.build_singer_command(
            tap_params, target_params, transform_params
        )

        assert (
            command
            == f'/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json '
            f'--state {state_mock}'
            f' | /bin/transform_field.py --config {transform_config}'
            ' | /bin/target_postgres.py --config .ppw/config.json'
        )

        # profiling enabled
        command = commands.build_singer_command(
            tap_params,
            target_params,
            transform_params,
            profiling_mode=True,
            profiling_dir='./profiling',
        )

        assert (
            command == f'/bin/python -m cProfile -o ./profiling/tap_my_tap.pstat '
            f'/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json '
            f'--state {state_mock}'
            f' | /bin/python -m cProfile -o ./profiling/transformation_my_tap_my_target.pstat '
            f'/bin/transform_field.py --config {transform_config}'
            ' | /bin/python -m cProfile -o ./profiling/target_my_target.pstat /bin/target_postgres.py '
            '--config .ppw/config.json'
        )

        # Should generate a command with tap state and transformation and stream buffer

        # profiling disabled
        command = commands.build_singer_command(
            tap_params, target_params, transform_params, stream_buffer_size=10
        )

        assert (
            command
            == f'/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json '
            f'--state {state_mock}'
            f' | /bin/transform_field.py --config {transform_config}'
            ' | mbuffer -m 10M'
            ' | /bin/target_postgres.py --config .ppw/config.json'
        )

        # profiling enabled
        command = commands.build_singer_command(
            tap_params,
            target_params,
            transform_params,
            profiling_mode=True,
            profiling_dir='./profiling',
            stream_buffer_size=10,
        )

        assert (
            command
            == f'/bin/python -m cProfile -o ./profiling/tap_my_tap.pstat /bin/tap_mysql.py '
            f'--config .ppw/config.json --properties .ppw/properties.json --state {state_mock}'
            f' | /bin/python -m cProfile -o ./profiling/transformation_my_tap_my_target.pstat '
            f'/bin/transform_field.py --config {transform_config}'
            ' | mbuffer -m 10M'
            ' | /bin/python -m cProfile -o ./profiling/target_my_target.pstat /bin/target_postgres.py'
            ' --config .ppw/config.json'
        )

        # Should generate a command without state and with transformation
        tap_params = commands.TapParams(
            id='my_tap',
            type='tap-mysql',
            bin='/bin/tap_mysql.py',
            python_bin='/bin/python',
            config='.ppw/config.json',
            properties='.ppw/properties.json',
            state=None,
        )

        # profiling disabled
        command = commands.build_singer_command(
            tap_params, target_params, transform_params
        )

        assert (
            command
            == f'/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json '
            f' | /bin/transform_field.py --config {transform_config}'
            ' | /bin/target_postgres.py --config .ppw/config.json'
        )

        # profiling enabled
        command = commands.build_singer_command(
            tap_params,
            target_params,
            transform_params,
            profiling_mode=True,
            profiling_dir='./profiling',
        )

        assert (
            command == f'/bin/python -m cProfile -o ./profiling/tap_my_tap.pstat '
            f'/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json '
            f' | /bin/python -m cProfile -o ./profiling/transformation_my_tap_my_target.pstat '
            f'/bin/transform_field.py --config {transform_config}'
            ' | /bin/python -m cProfile -o ./profiling/target_my_target.pstat '
            '/bin/target_postgres.py --config .ppw/config.json'
        )

        # Should generate a command with state and without transformation
        tap_params = commands.TapParams(
            id='my_tap',
            type='tap-mysql',
            bin='/bin/tap_mysql.py',
            python_bin='/bin/python',
            config='.ppw/config.json',
            properties='.ppw/properties.json',
            state=state_mock,
        )

        transform_params = commands.TransformParams(
            bin='/bin/transform_field.py',
            python_bin='/bin/python',
            config=transform_config_empty,
            tap_id='my_tap',
            target_id='my_target',
        )
        # profiling disabled
        command = commands.build_singer_command(
            tap_params, target_params, transform_params
        )

        assert (
            command
            == f'/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json '
            f'--state {state_mock}'
            ' | /bin/target_postgres.py --config .ppw/config.json'
        )

        # profiling enabled
        command = commands.build_singer_command(
            tap_params,
            target_params,
            transform_params,
            profiling_mode=True,
            profiling_dir='./profiling',
        )

        assert (
            command
            == f'/bin/python -m cProfile -o ./profiling/tap_my_tap.pstat /bin/tap_mysql.py '
            f'--config .ppw/config.json --properties .ppw/properties.json '
            f'--state {state_mock}'
            ' | /bin/python -m cProfile -o ./profiling/target_my_target.pstat /bin/target_postgres.py '
            '--config .ppw/config.json'
        )

        # Should generate a command without state and transformation
        tap_params = commands.TapParams(
            id='my_tap',
            type='tap-mysql',
            bin='/bin/tap_mysql.py',
            python_bin='/bin/python',
            config='.ppw/config.json',
            properties='.ppw/properties.json',
            state='.ppw/state.json',
        )
        # profiling disabled
        command = commands.build_singer_command(
            tap_params, target_params, transform_params
        )

        assert (
            command
            == '/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json '
            ' | /bin/target_postgres.py --config .ppw/config.json'
        )

        # profiling enabled
        command = commands.build_singer_command(
            tap_params,
            target_params,
            transform_params,
            profiling_mode=True,
            profiling_dir='./profiling',
        )

        assert (
            command
            == '/bin/python -m cProfile -o ./profiling/tap_my_tap.pstat /bin/tap_mysql.py '
            '--config .ppw/config.json --properties .ppw/properties.json '
            ' | /bin/python -m cProfile -o ./profiling/target_my_target.pstat /bin/target_postgres.py '
            '--config .ppw/config.json'
        )

    @mock.patch('pipelinewise.cli.commands._verify_json_file', mock.MagicMock(return_value=True))
    def test_build_fastsync_command(self):
        """Tests the function that generates the fastsync command"""

        # we are using some config files which does not exist, so we patch the method that verifies the json files

        transform_config = '{}/resources/transform-config.json'.format(
            os.path.dirname(__file__)
        )
        state_mock = __file__
        venv_dir = '.dummy_venv_dir'
        temp_dir = 'dummy_temp_dir'

        # Should generate a fastsync command with transformation

        tap_params = commands.TapParams(
            id='my_tap',
            type='tap-mysql',
            bin='/bin/tap_mysql.py',
            python_bin='/tap-mysql/bin/python',
            config='.ppw/tap_config.json',
            properties='.ppw/properties.json',
            state=state_mock,
        )

        target_params = commands.TargetParams(
            id='my_target',
            type='target-postgres',
            bin='/bin/target_postgres.py',
            python_bin='/target-postgres/bin/python',
            config='.ppw/target_config.json',
        )

        transform_params = commands.TransformParams(
            bin='/bin/transform_field.py',
            python_bin='transform/bin/python',
            config=None,
            tap_id='my_tap',
            target_id='my_target',
        )

        # profiling disabled
        command = commands.build_fastsync_command(
            tap_params, target_params, transform_params, venv_dir, temp_dir
        )
        assert (
            command == '.dummy_venv_dir/pipelinewise/bin/mysql-to-postgres'
            ' --tap .ppw/tap_config.json'
            ' --properties .ppw/properties.json'
            f' --state {state_mock}'
            ' --target .ppw/target_config.json'
            ' --temp_dir dummy_temp_dir'
        )

        # profiling enabled
        command = commands.build_fastsync_command(
            tap_params,
            target_params,
            transform_params,
            venv_dir,
            temp_dir,
            profiling_mode=True,
            profiling_dir='./profiling',
        )

        assert (
            command == '.dummy_venv_dir/pipelinewise/bin/python -m cProfile '
            '-o ./profiling/fastsync_my_tap_my_target.pstat'
            ' .dummy_venv_dir/pipelinewise/bin/mysql-to-postgres'
            ' --tap .ppw/tap_config.json'
            ' --properties .ppw/properties.json'
            f' --state {state_mock}'
            ' --target .ppw/target_config.json'
            ' --temp_dir dummy_temp_dir'
        )

        # Should generate a fastsync command with transformation
        transform_params = commands.TransformParams(
            bin='/bin/transform_field.py',
            python_bin='transform/bin/python',
            config=transform_config,
            tap_id='my_tap',
            target_id='my_target',
        )

        # profiling disabled
        command = commands.build_fastsync_command(
            tap_params, target_params, transform_params, venv_dir, temp_dir
        )
        assert (
            command == '.dummy_venv_dir/pipelinewise/bin/mysql-to-postgres'
            ' --tap .ppw/tap_config.json'
            ' --properties .ppw/properties.json'
            f' --state {state_mock}'
            ' --target .ppw/target_config.json'
            ' --temp_dir dummy_temp_dir'
            f' --transform {transform_config}'
        )

        # profiling enabled
        command = commands.build_fastsync_command(
            tap_params,
            target_params,
            transform_params,
            venv_dir,
            temp_dir,
            profiling_mode=True,
            profiling_dir='./profiling',
        )

        assert (
            command == '.dummy_venv_dir/pipelinewise/bin/python -m cProfile'
            ' -o ./profiling/fastsync_my_tap_my_target.pstat'
            ' .dummy_venv_dir/pipelinewise/bin/mysql-to-postgres'
            ' --tap .ppw/tap_config.json'
            ' --properties .ppw/properties.json'
            f' --state {state_mock}'
            ' --target .ppw/target_config.json'
            ' --temp_dir dummy_temp_dir'
            f' --transform {transform_config}'
        )

        # Should generate a fastsync command with specific list of tables
        command = commands.build_fastsync_command(
            tap_params,
            target_params,
            transform_params,
            venv_dir,
            temp_dir,
            tables='public.table_one,public.table_two',
        )
        assert (
            command == '.dummy_venv_dir/pipelinewise/bin/mysql-to-postgres'
            ' --tap .ppw/tap_config.json'
            ' --properties .ppw/properties.json'
            f' --state {state_mock}'
            ' --target .ppw/target_config.json'
            ' --temp_dir dummy_temp_dir'
            f' --transform {transform_config}'
            ' --tables public.table_one,public.table_two'
        )

    def test_run_command(self):
        """Test run command functions

        Run command runs everything enclosed by /bin/bash -o pipefail -c '{}'
        This means arguments should pass as plain string after the command

        Return value is an array of: [return_code, stdout, stderr]
        """
        # Printing something to stdout should return 0
        [returncode, stdout, stderr] = commands.run_command('echo this is a test line')
        assert [returncode, stdout, stderr] == [0, 'this is a test line\n', '']

        # Running an invalid command should return 127 and some error message to stdout
        [returncode, stdout, stderr] = commands.run_command(
            'invalid-command this is an invalid command'
        )
        assert [returncode, stdout] == [127, '']
        assert stderr != ''

        # If loggin enabled then a success command should create log file with success status
        [returncode, stdout, stderr] = commands.run_command(
            'echo this is a test line', log_file='./test.log'
        )
        assert [returncode, stdout, stderr] == [0, 'this is a test line\n', None]
        assert os.path.isfile('test.log.success')
        os.remove('test.log.success')

        # If logging enabled then a failed command should create log file with failed status
        # NOTE: When logging is enabled and the command fails then it raises an exception
        #       This behaviour is not in sync with no logging option
        # TODO: Sync failed command execution behaviour with logging and no-logging option
        #       Both should return [rc, stdout, stderr] list or both should raise exception
        with pytest.raises(Exception):
            commands.run_command(
                'invalid-command this is an invalid command', log_file='./test.log'
            )
        assert os.path.isfile('test.log.failed')
        os.remove('test.log.failed')
