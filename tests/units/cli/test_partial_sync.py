import os

from copy import deepcopy
from unittest import TestCase, mock

from pipelinewise import cli


@mock.patch.object(cli.pipelinewise, '__name__', 'test_logger')
class PartialSyncCLITestCase(TestCase):
    """Testcases for partial sync CLI"""

    def setUp(self) -> None:
        resources_dir = f'{os.path.dirname(__file__)}/resources'
        self.test_cli = cli
        self.test_cli.CONFIG_DIR = f'{resources_dir}/test_partial_sync'
        self.test_cli.VENV_DIR = './virtualenvs-dummy'

    def _run_cli(self, arguments_dict: dict) -> None:
        """Running the test CLI application"""
        argv_list = ['main', 'partial_sync_table']
        if arguments_dict.get('tap'):
            argv_list.extend(['--tap', arguments_dict['tap']])
        if arguments_dict.get('target'):
            argv_list.extend(['--target', arguments_dict['target']])
        if arguments_dict.get('table'):
            argv_list.extend(['--table', arguments_dict['table']])
        if arguments_dict.get('column'):
            argv_list.extend(['--column', arguments_dict['column']])
        if arguments_dict.get('start_value'):
            argv_list.extend(['--start_value', arguments_dict['start_value']])
        if arguments_dict.get('end_value'):
            argv_list.extend(['--end_value', arguments_dict['end_value']])

        with mock.patch('sys.argv', argv_list):
            self.test_cli.main()

    @mock.patch('builtins.print')
    def test_exit_with_error_1_if_mandatory_arguments_not_exist(self, mocked_print):
        """Test partial_sync_table command exit with error code 1 if mandatory argument not exist"""

        mandatory_arguments = {'tap': 'foo_tap', 'target': 'foo_target', 'table': 'foo_table',
                               'column': 'foo_column', 'start_value': 'foo_start_value'}
        test_missed_args_and_messages = [
            ('tap', 'You must specify a source name using the argument --tap'),
            ('target', 'You must specify a destination name using the argument --target'),
            ('table', 'You must specify a source table by using the argument --table'),
            ('column', 'You must specify a column by using the argument --column'),
            ('start_value', 'You must specify a start value by using the argument --start_value'),
        ]
        for missed_arg, expected_message in test_missed_args_and_messages:
            arguments = deepcopy(mandatory_arguments)
            del arguments[missed_arg]
            with self.assertRaises(SystemExit) as system_exit:
                self._run_cli(arguments)
            self.assertEqual(system_exit.exception.code, 1)

            mocked_print.assert_called_with(expected_message)

    def test_exit_with_error_1_if_not_supporting_target(self):
        """Test partial_sync_table command exit with error code 1 if target is not supported

        supporting:
          mysql -> snowflake
          postgres -> snowflake
        """

        target_type = 'target-s3-csv'
        tap = 'tap_mysql'
        target = 'target_not_supported'
        tap_type = 'tap-mysql'

        expected_log_message = f'ERROR:test_logger:Error! {tap}({tap_type})-{target}({target_type})' \
                               f' pair is not supported for the partial sync!'

        with self.assertLogs('test_logger') as actual_logs, self.assertRaises(SystemExit) as system_exit:
            self._run_cli({'tap': tap, 'target': target,
                           'table': 'foo_table', 'column': 'foo_column', 'start_value': 'foo'})

        self.assertEqual(system_exit.exception.code, 1)
        self.assertEqual(expected_log_message, actual_logs.output[0])

    def test_exit_with_error_1_if_not_supporting_tap(self):
        """Test partial_sync_table command exit with error code 1 if tap is not supported

        supporting:
          mysql -> snowflake
          postgres -> snowflake
        """

        target = 'target_snowflake'
        tap = 'tap_not_support'
        tap_type = 'tap-kafka'
        target_type = 'target-snowflake'

        expected_log_message = f'ERROR:test_logger:Error! {tap}({tap_type})-{target}({target_type})' \
                               f' pair is not supported for the partial sync!'

        with self.assertLogs('test_logger') as actual_logs, self.assertRaises(SystemExit) as system_exit:
            self._run_cli({'tap': tap, 'target': target,
                           'table': 'foo_table', 'column': 'foo_column', 'start_value': 'foo'})

        self.assertEqual(system_exit.exception.code, 1)
        self.assertEqual(expected_log_message, actual_logs.output[0])


    def test_it_returns_error_1_if_tap_is_not_enabled(self):
        """Test log message and exit code is 1 if tap is not enabled"""
        tap_name = 'Source MySQL'
        arguments = {
            'tap': 'tap_mysql_disabled',
            'target': 'target_snowflake',
            'table': 'foo_table',
            'column': 'foo_column',
            'start_value': '1',
            'end_value': '10'
        }
        expected_log_message = f'INFO:test_logger:Tap {tap_name} is not enabled.'

        with self.assertLogs('test_logger') as actual_logs, self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)
        self.assertEqual(expected_log_message, actual_logs.output[1])

    def test_it_returns_error_1_if_tap_configuration_not_completed(self):
        """Test log message and exit code is 1 if tap configuration is not completed"""
        tap_type = 'tap-mysql'
        target_type = 'target-snowflake'
        arguments = {
            'tap': 'tap_mysql',
            'target': 'target_snowflake',
            'table': 'foo_table',
            'column': 'foo_column',
            'start_value': '1',
            'end_value': '10'
        }

        expected_log_message = 'ERROR:test_logger:Table sync function is not implemented from' \
                               f' {tap_type} datasources to {target_type} type of targets'

        with self.assertLogs('test_logger') as actual_logs, self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)
        self.assertEqual(expected_log_message, actual_logs.output[1])

    @mock.patch('pipelinewise.cli.pipelinewise.PipelineWise._check_if_complete_tap_configuration')
    def test_it_returns_error_1_if_there_is_no_state_file(self, mocked_check):
        """Test log message and exit code is 1 if there is no state file
         (tap is not run before or problem with state)"""

        mocked_check.return_value = True
        arguments = {
            'tap': 'tap_no_state',
            'target': 'target_snowflake',
            'table': 'mysql_source_db.table_one',
            'column': 'id',
            'start_value': '1',
            'end_value': '10'
        }

        expected_log_message = f'ERROR:test_logger:Could not find state file in ' \
                               f'"{self.test_cli.CONFIG_DIR}/{arguments["target"]}/{arguments["tap"]}/state.json"!'

        with self.assertLogs('test_logger') as actual_logs, self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)
        self.assertEqual(expected_log_message, actual_logs.output[1])

    @mock.patch('pipelinewise.cli.pipelinewise.PipelineWise._check_if_complete_tap_configuration')
    def test_if_calling_the_module_partial_sync_table_correctly(self, mocked_check):
        """Test if the generated command for calling the partial sync module is correct"""

        mocked_check.return_value = True
        arguments = {
            'tap': 'tap_mysql',
            'target': 'target_snowflake',
            'table': 'mysql_source_db.table_one',
            'column': 'id',
            'start_value': '1',
            'end_value': '10'
        }

        with mock.patch('pipelinewise.cli.commands.run_command') as mocked_run_command:
            self._run_cli(arguments)

        call_args = mocked_run_command.call_args.args
        self.assertEqual(2, len(call_args))

        # Because each instance of Pipelinewise has a random postfix for log filename, we test it in this way!
        self.assertRegex(
            call_args[0],
            f'^{self.test_cli.VENV_DIR}/pipelinewise/bin/partial-mysql-to-snowflake '
            f'--tap {self.test_cli.CONFIG_DIR}/{arguments["target"]}/{arguments["tap"]}/config.json '
            f'--properties {self.test_cli.CONFIG_DIR}/{arguments["target"]}/{arguments["tap"]}/properties.json '
            f'--state {self.test_cli.CONFIG_DIR}/target_snowflake/tap_mysql/state.json '
            f'--target {self.test_cli.CONFIG_DIR}/tmp/target_config_[a-z0-9_]{{8}}.json '
            f'--temp_dir {self.test_cli.CONFIG_DIR}/tmp '
            f'--transform {self.test_cli.CONFIG_DIR}/target_snowflake/tap_mysql/transformation.json '
            f'--table {arguments["table"]} --column {arguments["column"]} '
            f'--start_value {arguments["start_value"]} --end_value {arguments["end_value"]}$'
        )

        self.assertRegex(call_args[1], f'^{self.test_cli.CONFIG_DIR}/{arguments["target"]}/{arguments["tap"]}/log/'
                                       f'{arguments["target"]}-{arguments["tap"]}-[0-9]{{8}}_[0-9]{{6}}'
                                       r'\.partialsync\.log')

    @mock.patch('pipelinewise.cli.pipelinewise.PipelineWise._check_if_complete_tap_configuration')
    def test_not_end_value_in_calling_command_if_no_end_value_in_cli_parameter(self, mocked_check):
        """Test the generated command for calling the partial sync module has no end value if the main cli is
        called without end value"""

        mocked_check.return_value = True
        arguments = {
            'tap': 'tap_mysql',
            'target': 'target_snowflake',
            'table': 'mysql_source_db.table_one',
            'column': 'id',
            'start_value': '1',
        }

        with mock.patch('pipelinewise.cli.commands.run_command') as mocked_run_command:
            self._run_cli(arguments)

        call_args = mocked_run_command.call_args.args
        self.assertTuple(call_args, ('', ''))
        self.assertEqual(2, len(call_args))
        # Because each instance of Pipelinewise has a random postfix for log filename, we test it in this way!
        self.assertRegex(
            call_args[0],
            f'^{self.test_cli.VENV_DIR}/pipelinewise/bin/partial-mysql-to-snowflake '
            f'--tap {self.test_cli.CONFIG_DIR}/{arguments["target"]}/{arguments["tap"]}/config.json '
            f'--properties {self.test_cli.CONFIG_DIR}/{arguments["target"]}/{arguments["tap"]}/properties.json '
            f'--state {self.test_cli.CONFIG_DIR}/target_snowflake/tap_mysql/state.json '
            f'--target {self.test_cli.CONFIG_DIR}/tmp/target_config_[a-z0-9_]{{8}}.json '
            f'--temp_dir {self.test_cli.CONFIG_DIR}/tmp '
            f'--transform {self.test_cli.CONFIG_DIR}/target_snowflake/tap_mysql/transformation.json '
            f'--table {arguments["table"]} --column {arguments["column"]} '
            f'--start_value {arguments["start_value"]}$'
        )

        self.assertRegex(call_args[1], f'^{self.test_cli.CONFIG_DIR}/{arguments["target"]}/{arguments["tap"]}/log/'
                                       f'{arguments["target"]}-{arguments["tap"]}-[0-9]{{8}}_[0-9]{{6}}'
                                       r'\.partialsync\.log')

    @mock.patch('pipelinewise.cli.pipelinewise.PipelineWise._check_if_complete_tap_configuration')
    def test_it_returns_error_1_if_table_does_not_exist_in_config(self, mocked_check):
        """Test it exit with error 1 if input table not in the config file"""

        mocked_check.return_value = True
        arguments = {
            'tap': 'tap_mysql',
            'target': 'target_snowflake',
            'table': 'foo_table',
            'column': 'foo_column',
            'start_value': '1',
            'end_value': '10'
        }

        expected_log_message = f'ERROR:test_logger:Not found table "{arguments["table"]}" in properties!'

        with self.assertLogs('test_logger') as actual_logs, self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)
        self.assertEqual(expected_log_message, actual_logs.output[1])

    @mock.patch('pipelinewise.cli.pipelinewise.PipelineWise._check_if_complete_tap_configuration')
    def test_it_returns_error_1_if_column_does_not_exist_in_config(self, mocked_check):
        """Test it exit with error 1 if input column not in the config file"""

        mocked_check.return_value = True
        arguments = {
            'tap': 'tap_mysql',
            'target': 'target_snowflake',
            'table': 'mysql_source_db.table_one',
            'column': 'foo_column',
            'start_value': '1',
            'end_value': '10'
        }

        expected_log_message = f'ERROR:test_logger:Not found column "{arguments["column"]}" in properties!'

        with self.assertLogs('test_logger') as actual_logs, self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)
        self.assertEqual(expected_log_message, actual_logs.output[1])

    @mock.patch('pipelinewise.cli.pipelinewise.PipelineWise._check_if_complete_tap_configuration')
    def test_it_returns_error_1_if_column_type_invalid_for_partial_sync(self, mocked_check):
        """Test it exit with error 1 if input column type is invalid for partial sync"""

        mocked_check.return_value = True
        arguments = {
            'tap': 'tap_mysql',
            'target': 'target_snowflake',
            'table': 'mysql_source_db.table_one',
            'column': 'boolean_column',
            'start_value': '1'
        }

        expected_log_message = f'ERROR:test_logger:column "{arguments["column"]}" has invalid type for partial sync!'

        with self.assertLogs('test_logger') as actual_logs, self.assertRaises(SystemExit) as system_exit:
            self._run_cli(arguments)

        self.assertEqual(system_exit.exception.code, 1)
        self.assertEqual(expected_log_message, actual_logs.output[1])
