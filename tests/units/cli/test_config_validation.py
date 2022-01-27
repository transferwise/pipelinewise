import json
import threading
import time

from tempfile import TemporaryDirectory
from unittest import TestCase

from pipelinewise.cli import commands


class TestConfigValidation(TestCase):
    """Unit tests for json properties validation"""
    def setUp(self):
        commands.PARAMS_VALIDATION_RETRY_PERIOD_SEC = 0.1
        commands.PARAMS_VALIDATION_RETRY_TIMES = 3
        self.sec_to_repair_json_file = 0.15  #: pylint: disable=attribute-defined-outside-init
        self.temp_dir = TemporaryDirectory()  #: pylint: disable=consider-using-with
        self.invalid_json_file = f'{self.temp_dir.name}/invalid_file.json'
        self.valid_json_file = f'{self.temp_dir.name}/valid_file.json'
        self.empty_file = f'{self.temp_dir.name}/empty_file.json'

        with open(self.valid_json_file, 'w', encoding='utf-8') as valid_file:
            json.dump({'foo': 'bar'}, valid_file)

        with open(self.empty_file, 'w', encoding='utf-8') as empty_file:
            empty_file.write('')

    def tearDown(self):
        self.temp_dir.cleanup()

    class AsyncWriteJsonFile(threading.Thread):
        """Helper class to asynchronous write on a file"""
        def __init__(self, file_name, content, waiting_time):
            threading.Thread.__init__(self)
            self.file_name = file_name
            self.content = content
            self.waiting_time = waiting_time

        def run(self):
            time.sleep(self.waiting_time)
            with open(self.file_name, 'w', encoding='utf-8') as json_file:
                json_file.write(self.content)

    @staticmethod
    def _assert_tap_config(config, properties, state):
        commands.TapParams(
            id='foo',
            type='bar',
            bin='foo_bin',
            python_bin='foo/python',
            config=config,
            properties=properties,
            state=state,
        )

    @staticmethod
    def _assert_target_config(config):
        commands.TargetParams(
            id='foo',
            type='bar',
            bin='foo_bin',
            python_bin='foo/python',
            config=config,
        )

    def _assert_retry_validation_of_json_file(self, config, properties, state):
        invalid_file_to_be_fixed_later = self.invalid_json_file

        # Starts with an invalid file and since the main method is retrying we fix the file after some seconds
        fixed_file = self.AsyncWriteJsonFile(invalid_file_to_be_fixed_later,
                                             '{"foo": "bar"}',
                                             self.sec_to_repair_json_file)
        fixed_file.start()
        fixed_file.join()

        self._assert_tap_config(config=config, properties=properties, state=state)

    def test_tap_config_json_validation_retry(self):
        """Test it retries if any json file is invalid"""
        test_cases = [(self.invalid_json_file, self.valid_json_file, self.valid_json_file),
                      (self.valid_json_file, self.invalid_json_file, self.valid_json_file),
                      (self.valid_json_file, self.valid_json_file, self.invalid_json_file)
                      ]

        for config, properties, state in test_cases:
            self._assert_retry_validation_of_json_file(config, properties, state)

    def test_tap_config_json_valid_if_state_file_does_not_exist(self):
        """Test it is valid if state file does not exists at all"""
        not_exists_state_file = f'{self.temp_dir.name}/not_exists.json'
        self._assert_tap_config(config=self.valid_json_file,
                                properties=self.valid_json_file,
                                state=not_exists_state_file)

    def test_tap_config_json_valid_if_state_file_is_empty(self):
        """Test it is valid if state file is empty"""
        self._assert_tap_config(
            config=self.valid_json_file,
            properties=self.valid_json_file,
            state=self.empty_file
        )

    def test_tap_config_json_raise_exception_on_invalid_content_for_state_file(self):
        """Test if exception is raised exception on invalid content"""
        invalid_file_contents = (' ', 'foo', '{"foo": 1')

        for invalid_content in invalid_file_contents:
            with open(self.invalid_json_file, 'w', encoding='utf-8') as invalid_file:
                invalid_file.write(invalid_content)

            with self.assertRaises(commands.RunCommandException) as command_exception:
                self._assert_tap_config(
                    config=self.valid_json_file,
                    properties=self.valid_json_file,
                    state=self.invalid_json_file,
                )
            assert str(command_exception.exception) == f'Invalid json file for state: {self.invalid_json_file}'

    def test_tap_config_valid_if_json_property_is_none(self):
        """Test TapConfig is valid if a json property is None"""
        test_cases = (
            (None, self.valid_json_file, self.valid_json_file),
            (self.valid_json_file, None, self.valid_json_file),
            (self.valid_json_file, self.valid_json_file, None)
        )
        for config, properties, state in test_cases:
            self._assert_tap_config(
                config=config,
                properties=properties,
                state=state
            )

    def test_tap_config_raise_exception_if_not_valid_json_after_retries(self):
        """Test it raises and exception if invalid json files yet after many retries"""
        invalid_file_contents = ['', ' ', 'foo', '{"foo": 1']

        for invalid_content in invalid_file_contents:
            with open(self.invalid_json_file, 'w', encoding='utf-8') as invalid_file:
                invalid_file.write(invalid_content)

            test_cases = (
                (self.invalid_json_file, self.valid_json_file),
                (self.valid_json_file,  self.invalid_json_file)
            )
            for config, properties in test_cases:
                with self.assertRaises(commands.RunCommandException) as command_exception:
                    self._assert_tap_config(
                        config=config,
                        properties=properties,
                        state=self.valid_json_file,
                    )

                    assert str(command_exception.exception) ==\
                           f'Invalid json file for {"config" if config == self.invalid_json_file else "properties"}:' \
                           f' {self.invalid_json_file}'

    def test_target_config_json_validation_retires(self):
        """Test it retries if any json file is invalid"""
        invalid_file_to_be_fixed_later = self.invalid_json_file

        # It starts with an invalid file and since the main method is retrying we fix the file after some seconds
        fixed_file = self.AsyncWriteJsonFile(invalid_file_to_be_fixed_later,
                                             '{"foo": "bar"}',
                                             self.sec_to_repair_json_file)
        fixed_file.start()
        fixed_file.join()

        self._assert_target_config(config=invalid_file_to_be_fixed_later)

    def test_target_config_valid_if_json_property_is_none(self):
        """Test TargetConfig is valid if config is None"""
        self._assert_target_config(config=None)

    def test_target_config_raises_exception_if_not_valid_json_after_retries(self):
        """Test if it raises and exception if invalid json files yet after many retries"""
        invalid_file_contents = ['', 'foo', '{"foo": 1']

        for invalid_content in invalid_file_contents:
            with open(self.invalid_json_file, 'w', encoding='utf-8') as invalid_file:
                invalid_file.write(invalid_content)

            with self.assertRaises(commands.RunCommandException) as command_exception:
                self._assert_target_config(config=self.invalid_json_file)

            assert str(command_exception.exception) == f'Invalid json file for config: {self.invalid_json_file}'
