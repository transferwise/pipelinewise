import os
import sys
import pytest
import pipelinewise.cli.commands as commands

from pipelinewise.cli.errors import StreamBufferTooLargeException


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
        assert commands.exists_and_executable(__file__) is False

    def test_build_tap_command(self):
        """Tests the function that generates tap executable command"""
        # State file should not be included if state file path not passed
        command = commands.build_tap_command(tap_type='tap_mysql',
                                             tap_bin='/bin/tap_mysql.py',
                                             config='.ppw/config.json',
                                             properties='.ppw/properties.json')
        assert command == '/bin/tap_mysql.py --config .ppw/config.json --catalog .ppw/properties.json '

        # State file should not be included if state file passed but file not exists
        command = commands.build_tap_command(tap_type='tap_mysql',
                                             tap_bin='/bin/tap_mysql.py',
                                             config='.ppw/config.json',
                                             properties='.ppw/properties.json',
                                             state='.pipelinewise/state.json')
        assert command == '/bin/tap_mysql.py --config .ppw/config.json --catalog .ppw/properties.json '

        # State file should be included if state file passed and file exists
        state_mock = __file__
        command = commands.build_tap_command(tap_type='tap_mysql',
                                             tap_bin='/bin/tap_mysql.py',
                                             config='.ppw/config.json',
                                             properties='.ppw/properties.json',
                                             state=state_mock)
        assert command == f'/bin/tap_mysql.py --config .ppw/config.json --catalog .ppw/properties.json ' \
                          f'--state {state_mock}'

    def test_build_target_command(self):
        """Tests the function that generates target executable command"""
        # Should return a input piped command with an executable target command
        command = commands.build_target_command(target_bin='/bin/target_postgres.py',
                                                config='.ppw/config.json')
        assert command == '/bin/target_postgres.py --config .ppw/config.json'

    def test_build_transform_command(self):
        """Tests the function that generates transform executable command"""
        # Should return empty string if config file exists but no transformation
        transform_config = '{}/resources/transform-config-empty.json'.format(os.path.dirname(__file__))
        command = commands.build_transformation_command(transform_bin='/bin/transform_field.py',
                                                        config=transform_config)
        assert command is None

        # Should return a input piped command with an executable transform command
        transform_config = '{}/resources/transform-config.json'.format(os.path.dirname(__file__))
        command = commands.build_transformation_command(transform_bin='/bin/transform_field.py',
                                                        config=transform_config)
        assert command == f'/bin/transform_field.py --config {transform_config}'

    def test_build_stream_buffer_command(self):
        """Tests the function that generates stream buffer executable command"""
        # Should return empty string if buffer size is invalid or too small
        assert commands.build_stream_buffer_command() is None
        assert commands.build_stream_buffer_command(buffer_size=None) is None
        assert commands.build_stream_buffer_command(buffer_size=0) is None
        assert commands.build_stream_buffer_command(buffer_size=-10) is None

        # Should use the minimum buffer size if enabled but less than minimal buffer size
        assert commands.build_stream_buffer_command(buffer_size=1) == f'mbuffer -m {commands.MIN_STREAM_BUFFER_SIZE}M'

        # Should raise StreamBufferTooLargeException if buffer_size is greater than the max allowed
        with pytest.raises(StreamBufferTooLargeException):
            commands.build_stream_buffer_command(buffer_size=commands.MAX_STREAM_BUFFER_SIZE + 1000)

        # Should use custom buffer size if between max and min buffer size
        assert commands.build_stream_buffer_command(buffer_size=100) == 'mbuffer -m 100M'

        # Should use custom buffer binary executable if bin parameter provided
        assert commands.build_stream_buffer_command(buffer_size=100, stream_buffer_bin='dummy_buffer') == \
               f'dummy_buffer -m 100M'

    def test_build_singer_command(self):
        """Tests the function that generates the full singer singer command
        that connects the required components with linux pipes"""
        transform_config = '{}/resources/transform-config.json'.format(os.path.dirname(__file__))
        transform_config_empty = '{}/resources/transform-config-empty.json'.format(os.path.dirname(__file__))
        state_mock = __file__

        # Should generate a command with tap state and transformation
        tap_params = commands.TapParams(type='tap-mysql',
                                        bin='/bin/tap_mysql.py',
                                        config='.ppw/config.json',
                                        properties='.ppw/properties.json',
                                        state=state_mock)
        target_params = commands.TargetParams(type='target-postgres',
                                              bin='/bin/target_postgres.py',
                                              config='.ppw/config.json')
        transform_params = commands.TransformParams(bin='/bin/transform_field.py',
                                                    config=transform_config)

        command = commands.build_singer_command(tap_params, target_params, transform_params)

        assert command == f'/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json ' \
                          f'--state {state_mock}' \
                          f' | /bin/transform_field.py --config {transform_config}' \
                          ' | /bin/target_postgres.py --config .ppw/config.json'

        # Should generate a command without state and with transformation
        tap_params = commands.TapParams(type='tap-mysql',
                                        bin='/bin/tap_mysql.py',
                                        config='.ppw/config.json',
                                        properties='.ppw/properties.json',
                                        state=None)
        target_params = commands.TargetParams(type='target-postgres',
                                              bin='/bin/target_postgres.py',
                                              config='.ppw/config.json')
        transform_params = commands.TransformParams(bin='/bin/transform_field.py',
                                                    config=transform_config)

        command = commands.build_singer_command(tap_params, target_params, transform_params)

        assert command == f'/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json ' \
                          f' | /bin/transform_field.py --config {transform_config}' \
                          ' | /bin/target_postgres.py --config .ppw/config.json'

        # Should generate a command with state and without transformation
        tap_params = commands.TapParams(type='tap-mysql',
                                        bin='/bin/tap_mysql.py',
                                        config='.ppw/config.json',
                                        properties='.ppw/properties.json',
                                        state=state_mock)
        target_params = commands.TargetParams(type='target-postgres',
                                              bin='/bin/target_postgres.py',
                                              config='.ppw/config.json')
        transform_params = commands.TransformParams(bin='/bin/transform_field.py',
                                                    config=transform_config_empty)

        command = commands.build_singer_command(tap_params, target_params, transform_params)

        assert command == f'/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json ' \
                          f'--state {state_mock}' \
                          ' | /bin/target_postgres.py --config .ppw/config.json'

        # Should generate a command without state and transformation
        tap_params = commands.TapParams(type='tap-mysql',
                                        bin='/bin/tap_mysql.py',
                                        config='.ppw/config.json',
                                        properties='.ppw/properties.json',
                                        state='.ppw/state.json')
        target_params = commands.TargetParams(type='target-postgres',
                                              bin='/bin/target_postgres.py',
                                              config='.ppw/config.json')
        transform_params = commands.TransformParams(bin='/bin/transform_field.py',
                                                    config=transform_config_empty)

        command = commands.build_singer_command(tap_params, target_params, transform_params)

        assert command == '/bin/tap_mysql.py --config .ppw/config.json --properties .ppw/properties.json ' \
                          ' | /bin/target_postgres.py --config .ppw/config.json'

    def test_build_fastsync_command(self):
        """Tests the function that generates the fastsync command"""
        transform_config = '{}/resources/transform-config.json'.format(os.path.dirname(__file__))
        state_mock = __file__
        venv_dir = '.dummy_venv_dir'
        temp_dir = 'dummy_temp_dir'

        # Should generate a fastsync command with transformation
        tap_params = commands.TapParams(type='tap-mysql',
                                        bin='/bin/tap_mysql.py',
                                        config='.ppw/tap_config.json',
                                        properties='.ppw/properties.json',
                                        state=state_mock)
        target_params = commands.TargetParams(type='target-postgres',
                                              bin='/bin/target_postgres.py',
                                              config='.ppw/target_config.json')
        transform_params = commands.TransformParams(bin='/bin/transform_field.py',
                                                    config=None)
        command = commands.build_fastsync_command(tap_params, target_params, transform_params, venv_dir, temp_dir)
        assert command == '.dummy_venv_dir/pipelinewise/bin/mysql-to-postgres' \
                          ' --tap .ppw/tap_config.json' \
                          ' --properties .ppw/properties.json' \
                          f' --state {state_mock}' \
                          ' --target .ppw/target_config.json' \
                          ' --temp_dir dummy_temp_dir'

        # Should generate a fastsync command with transformation
        transform_params = commands.TransformParams(bin='/bin/transform_field.py',
                                                    config=transform_config)
        command = commands.build_fastsync_command(tap_params, target_params, transform_params, venv_dir, temp_dir)
        assert command == '.dummy_venv_dir/pipelinewise/bin/mysql-to-postgres' \
                          ' --tap .ppw/tap_config.json' \
                          ' --properties .ppw/properties.json' \
                          f' --state {state_mock}' \
                          ' --target .ppw/target_config.json' \
                          ' --temp_dir dummy_temp_dir' \
                          f' --transform {transform_config}'

        # Should generate a fastsync command with specific list of tables
        command = commands.build_fastsync_command(tap_params, target_params, transform_params, venv_dir, temp_dir,
                                                  tables='public.table_one,public.table_two')
        assert command == '.dummy_venv_dir/pipelinewise/bin/mysql-to-postgres' \
                          ' --tap .ppw/tap_config.json' \
                          ' --properties .ppw/properties.json' \
                          f' --state {state_mock}' \
                          ' --target .ppw/target_config.json' \
                          ' --temp_dir dummy_temp_dir' \
                          f' --transform {transform_config}' \
                          ' --tables public.table_one,public.table_two'
