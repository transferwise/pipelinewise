import json
import threading
import time

from tempfile import TemporaryDirectory
from unittest import TestCase

from pipelinewise.cli import commands


class TestConfigValidation(TestCase):
    """Unit tests for json properties validation"""
    def setup_class(self):
        """SetUp Class"""
        commands.PARAMS_VALIDATION_RETRY_PERIOD_SEC = 0.1
        commands.PARAMS_VALIDATION_RETRY_TIMES = 3
        self.sec_to_repair_json_file = 0.15  #: pylint: disable=attribute-defined-outside-init

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

    def _assert_retry_validation_of_json_file(self, json_files_situation):
        with TemporaryDirectory() as temp_dir:
            invalid_json_file = f'{temp_dir}/invalid_file.json'
            valid_json_file = f'{temp_dir}/valid_file.json'
            with open(valid_json_file, 'w', encoding='utf-8') as valid_file:
                json.dump({'foo': 'bar'}, valid_file)

            with open(invalid_json_file, 'w', encoding='utf-8') as invalid_file:
                invalid_file.write('foo')

            # Starts with an invalid file and since the main method is retrying we fix the file after some seconds
            fixed_file = self.AsyncWriteJsonFile(invalid_json_file, '{"foo": "bar"}', self.sec_to_repair_json_file)
            fixed_file.start()
            fixed_file.join()

            self._assert_tap_config(config=locals()[f'{json_files_situation["config"]}_json_file'],
                                    properties=locals()[f'{json_files_situation["properties"]}_json_file'],
                                    state=locals()[f'{json_files_situation["state"]}_json_file']
                                    )

    def test_tap_config_json_validation(self):
        """Test it retries if any json file is invalid"""
        test_cases = (
            {'config': 'invalid', 'properties': 'valid', 'state': 'valid'},
            {'config': 'valid', 'properties': 'invalid', 'state': 'valid'},
            {'config': 'valid', 'properties': 'valid', 'state': 'invalid'},
        )
        for json_files_situation in test_cases:
            self._assert_retry_validation_of_json_file(json_files_situation)

    def test_tap_config_json_valid_if_state_file_does_not_exist(self):
        """Test it is valid if state file does not exists at all"""
        with TemporaryDirectory() as temp_dir:
            valid_json_file = f'{temp_dir}/valid_file.json'
            not_exists_state_file = f'{temp_dir}/not_exists.json'
            with open(valid_json_file, 'w', encoding='utf-8') as valid_file:
                json.dump({'foo': 'bar'}, valid_file)

            self._assert_tap_config(config=valid_json_file, properties=valid_json_file, state=not_exists_state_file)

    # pylint: disable=invalid-name
    def test_tap_config_json_valid_if_state_file_is_empty_and_raise_exception_on_invalid_content(self):
        """Test it is valid if state file is empty and raise exception if invalid content"""
        invalid_file_contents = [' ', 'foo', '{"foo": 1']

        with TemporaryDirectory() as temp_dir:
            invalid_json_file = f'{temp_dir}/invalid_file.json'
            valid_json_file = f'{temp_dir}/valid_file.json'
            empty_file = f'{temp_dir}/empty_stat.json'

            with open(valid_json_file, 'w', encoding='utf-8') as valid_file:
                json.dump({'foo': 'bar'}, valid_file)

            # 1. asserting if raises exception on invalid contents
            for invalid_content in invalid_file_contents:
                with open(invalid_json_file, 'w', encoding='utf-8') as invalid_file:
                    invalid_file.write(invalid_content)

                with self.assertRaises(commands.RunCommandException) as command_exception:
                    self._assert_tap_config(
                        config=valid_json_file,
                        properties=valid_json_file,
                        state=invalid_json_file,
                    )
                assert str(command_exception.exception) == f'Invalid json file for state: {invalid_json_file}'

            # 2. asserting if it is valid with empty file
            with open(empty_file, 'w', encoding='utf-8') as state_file:
                state_file.write('')
            self._assert_tap_config(
                config=valid_json_file,
                properties=valid_json_file,
                state=empty_file
            )

    def test_tap_config_valid_if_json_property_is_none(self):
        """Test TapConfig is valid if a json property is None"""
        with TemporaryDirectory() as temp_dir:
            valid_json_file = f'{temp_dir}/valid_file.json'
            with open(valid_json_file, 'w', encoding='utf-8') as valid_file:
                json.dump({'foo': 'bar'}, valid_file)

            test_cases = (
                (None, valid_json_file, valid_json_file),
                (valid_json_file, None, valid_json_file),
                (valid_json_file, valid_json_file, None)
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

        with TemporaryDirectory() as temp_dir:
            invalid_json_file = f'{temp_dir}/invalid_file.json'
            valid_json_file = f'{temp_dir}/valid_file.json'

            with open(valid_json_file, 'w', encoding='utf-8') as valid_file:
                json.dump({'foo': 'bar'}, valid_file)

            for invalid_content in invalid_file_contents:
                with open(invalid_json_file, 'w', encoding='utf-8') as invalid_file:
                    invalid_file.write(invalid_content)

                for case_number in range(2):
                    with self.assertRaises(commands.RunCommandException) as command_exception:
                        self._assert_tap_config(
                            config=invalid_json_file if case_number == 0 else valid_json_file,
                            properties=invalid_json_file if case_number == 1 else valid_json_file,
                            state=valid_json_file,
                        )

                assert str(command_exception.exception) ==\
                       f'Invalid json file for {"config" if case_number == 0 else "properties"}: {invalid_json_file}'

    def test_target_config_json_validation(self):
        """Test it retries if any json file is invalid"""
        with TemporaryDirectory() as temp_dir:
            test_json_file = f'{temp_dir}/test_file.json'

            with open(test_json_file, 'w', encoding='utf-8') as invalid_file:
                invalid_file.write('foo')

            # It starts with an invalid file and since the main method is retrying we fix the file after some seconds
            fixed_file = self.AsyncWriteJsonFile(test_json_file, '{"foo": "bar"}', self.sec_to_repair_json_file)
            fixed_file.start()
            fixed_file.join()

            self._assert_target_config(config=test_json_file)

    def test_target_config_valid_if_json_property_is_none(self):
        """Test TargetConfig is valid if config is None"""
        self._assert_target_config(config=None)

    def test_target_config_raise_exception_if_not_valid_json_after_retries(self):
        """Test if it raises and exception if invalid json files yet after many retries"""
        invalid_file_contents = ['', 'foo', '{"foo": 1']

        with TemporaryDirectory() as temp_dir:
            invalid_json_file = f'{temp_dir}/invalid_file.json'

            for invalid_content in invalid_file_contents:
                with open(invalid_json_file, 'w', encoding='utf-8') as invalid_file:
                    invalid_file.write(invalid_content)

                with self.assertRaises(commands.RunCommandException) as command_exception:
                    self._assert_target_config(config=invalid_json_file)

                assert str(command_exception.exception) == f'Invalid json file for config: {invalid_json_file}'
