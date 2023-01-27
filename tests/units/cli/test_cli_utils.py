import os
import pytest
import re

from tempfile import TemporaryDirectory, NamedTemporaryFile, TemporaryFile

from pipelinewise import cli
from pipelinewise.cli.errors import InvalidConfigException

VIRTUALENVS_DIR = './virtualenvs-dummy'


# pylint: disable=no-self-use,too-many-public-methods,fixme
class TestUtils:
    """
    Unit Tests for PipelineWise CLI utility functions
    """

    def assert_json_is_invalid(self, schema, invalid_target):
        """Simple assertion to check if validate function exits with error"""
        with pytest.raises(InvalidConfigException):
            cli.utils.validate(invalid_target, schema)

    def test_json_detectors(self):
        """Testing JSON detector functions"""
        assert cli.utils.is_json('{Invalid JSON}') is False

        assert cli.utils.is_json('[]') is True
        assert cli.utils.is_json('{}') is True
        assert cli.utils.is_json('{"prop": 123}') is True
        assert cli.utils.is_json('{"prop-str":"dummy-string","prop-int":123,"prop-bool":true}') is True

        assert cli.utils.is_json_file('./dummy-json') is False
        assert cli.utils.is_json_file('{}/resources/example.json'.format(os.path.dirname(__file__))) is True
        assert cli.utils.is_json_file('{}/resources/invalid.json'.format(os.path.dirname(__file__))) is False
        assert cli.utils.is_json_file('{}/resources'.format(os.path.dirname(__file__))) is False

    def test_json_loader(self):
        """Testing JSON loader functions"""
        # Loading JSON file that not exist should return None
        assert cli.utils.load_json('/invalid/location/to/json') is None

        # Loading JSON file with invalid JSON syntax should raise exception
        with pytest.raises(Exception):
            cli.utils.load_json('{}/resources/invalid.json'.format(os.path.dirname(__file__)))

        # Loading JSON should return python dict
        assert \
            cli.utils.load_json('{}/resources/example.json'.format(os.path.dirname(__file__))) == \
            {
                'glossary': {
                    'title': 'example glossary',
                    'GlossDiv': {
                        'title': 'S',
                        'GlossList': {
                            'GlossEntry': {
                                'ID': 'SGML',
                                'SortAs': 'SGML',
                                'GlossTerm': 'Standard Generalized Markup Language',
                                'Acronym': 'SGML',
                                'Abbrev': 'ISO 8879:1986',
                                'GlossDef': {
                                    'para': 'A meta-markup language, used to create markup languages such as DocBook.',
                                    'GlossSeeAlso': ['GML', 'XML']
                                },
                                'GlossSee': 'markup'
                            }
                        }
                    }
                }
            }

    def test_json_saver(self):
        """Testing JSON save functions"""
        obj = {'foo': 'bar'}
        # Saving to invalid path should raise exception
        with pytest.raises(Exception):
            cli.utils.save_json(obj, '/invalid/path')

        # Saving and reloading should match
        cli.utils.save_json(obj, 'test-json.json')
        assert cli.utils.load_json('test-json.json') == obj

        # Delete output file, it's not required
        os.remove('test-json.json')

    def test_yaml_detectors(self):
        """Testing YAML detector functions"""
        assert cli.utils.is_yaml("""
            foo:
            -bar""") is False

        assert cli.utils.is_yaml('id: 123') is True
        assert cli.utils.is_yaml("""
            id: 123
            details:
                - prop1: 123
                - prop2: 456
            """) is True

        assert cli.utils.is_yaml_file('./dummy-yaml') is False
        assert cli.utils.is_yaml_file('{}/resources/example.yml'.format(os.path.dirname(__file__))) is True
        assert cli.utils.is_yaml_file('{}/resources/invalid.yml'.format(os.path.dirname(__file__))) is False
        assert cli.utils.is_yaml_file('{}/resources'.format(os.path.dirname(__file__))) is False

    def test_yaml_loader(self):
        """Testing YAML loader functions"""
        # Loading YAML file that not exist should return None
        assert cli.utils.load_yaml('/invalid/location/to/yaml') is None

        # Loading YAML file with invalid YAML syntax should raise exception
        with pytest.raises(Exception):
            cli.utils.load_yaml('{}/resources/invalid.yml'.format(os.path.dirname(__file__)))

        # Loading YAML file with valid YAML syntax but invalid vault secret file should raise exception
        with pytest.raises(Exception):
            cli.utils.load_yaml('{}/resources/example.yml'.format(os.path.dirname(__file__)),
                                'invalid-secret-file-path')

        # Loading valid YAML file with no vault encryption
        assert \
            cli.utils.load_yaml('{}/resources/example.yml'.format(os.path.dirname(__file__))) == \
            ['Apple', 'Orange', 'Strawberry', 'Mango']

        # Loading valid YAML file with vault encrypted properties
        assert \
            cli.utils.load_yaml(
                '{}/resources/example-with-vault.yml'.format(os.path.dirname(__file__)),
                '{}/resources/vault-secret.txt'.format(os.path.dirname(__file__))) == \
            ['Apple', 'Orange', 'Strawberry', 'Mango', 'Vault Encrypted Secret Fruit']

        os.environ['APP_SECRET'] = app_secret = 'my-secret'
        os.environ['APP_ENVIRONMENT'] = app_environment = 'test'
        assert \
            cli.utils.load_yaml('{}/resources/example-with-jinja-env-var.yml'.format(os.path.dirname(__file__))) == \
            {'app': 'my-app', 'secret': app_secret, 'environment': app_environment}

    def test_sample_file_path(self):
        """Sample files must be global config, tap, target YAML or README file"""
        for sample in cli.utils.get_sample_file_paths():
            assert os.path.isfile(sample) is True
            assert \
                re.match('.*config.yml$', sample) or \
                re.match('.*(tap|target)_.*.yml.sample$', sample) or \
                re.match('.*README.md$', sample)

    def test_extract_log_attributes(self):
        """Log files must match to certain pattern with embedded attributes in the file name"""
        assert \
            cli.utils.extract_log_attributes('snowflake-fx-20190508_000038.singer.log.success') == \
            {
                'filename': 'snowflake-fx-20190508_000038.singer.log.success',
                'target_id': 'snowflake',
                'tap_id': 'fx',
                'timestamp': '2019-05-08T00:00:38',
                'sync_engine': 'singer',
                'status': 'success'
            }

        assert \
            cli.utils.extract_log_attributes('snowflake-fx-20190508_231238.fastsync.log.running') == \
            {
                'filename': 'snowflake-fx-20190508_231238.fastsync.log.running',
                'target_id': 'snowflake',
                'tap_id': 'fx',
                'timestamp': '2019-05-08T23:12:38',
                'sync_engine': 'fastsync',
                'status': 'running'
            }

        assert \
            cli.utils.extract_log_attributes('dummy-log-file.log') == \
            {
                'filename': 'dummy-log-file.log',
                'target_id': 'unknown',
                'tap_id': 'unknown',
                'timestamp': '1970-01-01T00:00:00',
                'sync_engine': 'unknown',
                'status': 'unknown'
            }

    def test_fastsync_bin(self):
        """Fastsync binary paths must point to pipelinewise virtual environments"""
        # Giving tap and target types should be enough to generate full path to fastsync binaries
        assert \
            cli.utils.get_fastsync_bin(VIRTUALENVS_DIR, 'mysql', 'snowflake') == \
            '{}/pipelinewise/bin/mysql-to-snowflake'.format(VIRTUALENVS_DIR)

    def test_vault(self):
        """Test vault encrypt and decrypt functionalities"""
        # Encrypting with not existing file with secret should exit
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cli.utils.vault_encrypt('plain_test', 'not-existing-secret-file')
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

        # Encrypted string should start with $ANSIBLE_VAULT; identifier
        encrypted_str = str(
            cli.utils.vault_encrypt('plain_text', '{}/resources/vault-secret.txt'.format(os.path.dirname(__file__))))
        assert encrypted_str.startswith("b'$ANSIBLE_VAULT;") is True

        # Formatted encrypted string should start with special token and should keep the original vault encrypted value
        formatted_encrypted_str = cli.utils.vault_format_ciphertext_yaml(encrypted_str)
        assert formatted_encrypted_str.startswith('!vault |') and "b'$ANSIBLE_VAULT;" in formatted_encrypted_str

        # Optional name argument should add the name to the output string as a key
        formatted_encrypted_str = cli.utils.vault_format_ciphertext_yaml(encrypted_str, name='encrypted_plain_text')
        assert formatted_encrypted_str.startswith(
            'encrypted_plain_text: !vault |') and "b'$ANSIBLE_VAULT;" in formatted_encrypted_str

    def test_schema_loader(self):
        """Test JSON Schema loader functions"""
        # Loading JSON schema file that not exist should exit
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            assert cli.utils.load_schema('/invalid/location/to/schema') is None
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

        # Loading existing JSON schema should be loaded correctly
        tap_schema = cli.utils.load_json('{}/../../../pipelinewise/cli/schemas/tap.json'.format(
            os.path.dirname(__file__)))
        assert cli.utils.load_schema('tap') == tap_schema

    def test_json_validate_tap(self):
        """Test JSON schema validator functions on taps"""
        schema = cli.utils.load_schema('tap')

        # Valid instance should return None
        valid_tap = cli.utils.load_yaml('{}/resources/tap-valid-mysql.yml'.format(os.path.dirname(__file__)))
        assert cli.utils.validate(valid_tap, schema) is None

        # Invalid instance should exit
        invalid_tap = cli.utils.load_yaml('{}/resources/tap-invalid.yml'.format(os.path.dirname(__file__)))
        self.assert_json_is_invalid(schema, invalid_tap)

    def test_json_validate_target(self):
        """Test JSON schema validator functions on targets"""
        schema = cli.utils.load_schema('target')

        # Valid instance should return None
        valid_target = cli.utils.load_yaml('{}/resources/target-valid-s3-csv.yml'.format(os.path.dirname(__file__)))
        assert cli.utils.validate(valid_target, schema) is None

        # Invalid instance should exit
        invalid_target = cli.utils.load_yaml('{}/resources/target-invalid-s3-csv.yml'.format(os.path.dirname(__file__)))
        self.assert_json_is_invalid(schema, invalid_target)

    def test_delete_keys(self):
        """Test dictionary functions"""
        # Delete single key with empty value
        assert cli.utils.delete_empty_keys({'foo': 'bar', 'foo2': None}) == {'foo': 'bar'}

        # Delete multiple keys with empty value
        assert cli.utils.delete_empty_keys({
            'foo': 'bar',
            'foo2': None,
            'foo3': None,
            'foo4': 'bar4'
        }) == {'foo': 'bar', 'foo4': 'bar4'}

        # Delete single key by name
        assert cli.utils.delete_keys_from_dict({'foo': 'bar', 'foo2': 'bar2'}, ['foo2']) == {'foo': 'bar'}

        # Delete single key by name
        assert cli.utils.delete_keys_from_dict({
            'foo': 'bar',
            'foo2': 'bar2',
            'foo3': None,
            'foo4': 'bar4'
        }, ['foo2', 'foo4']) == {'foo': 'bar', 'foo3': None}

        # Delete multiple keys from list of nested dictionaries
        assert cli.utils.delete_keys_from_dict(
            [{'foo': 'bar', 'foo2': 'bar2'},
             {'foo3': {'nested_foo': 'nested_bar', 'nested_foo2': 'nested_bar2'}}],
            ['foo2', 'nested_foo']) == [{'foo': 'bar'}, {'foo3': {'nested_foo2': 'nested_bar2'}}]

    def test_silentremove_success_if_file_doesnt_exist(self):
        """Test removing a non-existing file should not raise exception"""
        assert cli.utils.silentremove('this-file-not-exists.json') is None

    # pylint: disable=R1732
    def test_silentremove_successfully_removes_file(self):
        """Test removing a file that exists"""
        with NamedTemporaryFile(delete=False) as file:
            cli.utils.silentremove(file.name)
            assert os.path.exists(file.name) is False

    def test_silentremove_successfully_removes_directory(self):
        """Test removing an existing directory works"""
        with TemporaryDirectory() as directory:
            cli.utils.silentremove(directory)
            assert os.path.exists(directory) is False

    def test_silentremove_success_if_directory_doesnt_exist(self):
        """Test removing an existing directory works"""
        assert cli.utils.silentremove('tmp/folder_doesnt_exist') is None

    def test_tap_properties(self):
        """Test tap property getter functions"""
        tap_mysql = cli.utils.load_yaml('{}/resources/tap-valid-mysql.yml'.format(os.path.dirname(__file__)))

        # Every tap should have catalog argument --properties or --catalog
        tap_catalog_argument = cli.utils.get_tap_property(tap_mysql, 'tap_catalog_argument')
        assert tap_catalog_argument in ['--catalog', '--properties']

        # Every tap should have extra_config_keys defined in dict
        assert isinstance(cli.utils.get_tap_extra_config_keys(tap_mysql), dict) is True

        # MySQL stream_id should be formatted as {{schema_name}}-{{table_name}}
        assert cli.utils.get_tap_stream_id(tap_mysql, 'dummy_db', 'dummy_schema', 'dummy_table') == \
               'dummy_schema-dummy_table'

        # MySQL stream_name should be formatted as {{schema_name}}-{{table_name}}
        assert cli.utils.get_tap_stream_name(tap_mysql, 'dummy_db', 'dummy_schema',
                                             'dummy_table') == 'dummy_schema-dummy_table'

        # MySQL stream_name should be formatted as {{schema_name}}-{{table_name}}
        assert cli.utils.get_tap_default_replication_method(tap_mysql) == 'LOG_BASED'

        # Get property value by tap type
        assert cli.utils.get_tap_property_by_tap_type('tap-mysql', 'default_replication_method') == 'LOG_BASED'

        # Kafka encoding and parameterised local_store_dir should be added as default extra config keys
        tap_kafka = cli.utils.load_yaml('{}/resources/tap-valid-kafka.yml'.format(os.path.dirname(__file__)))
        assert cli.utils.get_tap_extra_config_keys(tap_kafka, temp_dir='/my/temp/dir') == {
            'local_store_dir': '/my/temp/dir',
            'encoding': 'utf-8'
        }

        # Snwoflake tables list should be added to tap_config_extras
        tap_snowflake = cli.utils.load_yaml('{}/resources/tap-valid-snowflake.yml'.format(os.path.dirname(__file__)))
        assert cli.utils.get_tap_extra_config_keys(tap_snowflake) == {
            'tables': 'SCHEMA_1.TABLE_ONE,SCHEMA_1.TABLE_TWO'
        }

    def test_get_tap_target_names(self):
        """Test get tap and target yamls"""
        expected_tap_names = {'tap_test.yml', 'tap_2test.yml', 'tap_valid.yaml'}
        expected_target_names = {'target_test.yml'}
        tap_names, target_names = cli.utils.get_tap_target_names(f'{os.path.dirname(__file__)}'
                                                                 f'/resources/test_tap_target_names')

        assert tap_names == expected_tap_names
        assert target_names == expected_target_names

    def test_create_temp_file(self):
        """Test temp files created at the right location"""
        # By default temp files should be created in system temp directory
        temp_file = cli.utils.create_temp_file()[1]
        assert os.path.isfile(temp_file)
        os.remove(temp_file)

        # Providing extra dir argument should create the target directory even if it's not exist
        temp_file = cli.utils.create_temp_file(dir='./temp_dir_to_create_automatically/deep_temp_dir')[1]
        assert os.path.isfile(temp_file)
        os.remove(temp_file)

        # Providing dir, suffix and prefix arguments should create the target_directory with custom prefix and suffix
        temp_file = cli.utils.create_temp_file(dir='./temp_dir_to_create_automatically/deep_temp_dir',
                                               suffix='.json',
                                               prefix='pipelinewise_test_temp_file_')[1]
        assert os.path.isfile(temp_file)
        os.remove(temp_file)

    def test_find_errors_in_log_file(self):
        """Test reading the last n lines of a file"""
        # Should return an empty list if no error in the file
        log_file = '{}/resources/sample_log_files/tap-run-no-errors.log'.format(os.path.dirname(__file__))
        assert cli.utils.find_errors_in_log_file(log_file) == []

        # Should return the line with errors
        log_file = '{}/resources/sample_log_files/tap-run-errors.log'.format(os.path.dirname(__file__))
        assert cli.utils.find_errors_in_log_file(log_file) == \
               ['time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=EXCEPTION This is an exception\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=ERROR This is an error\n',
                'pymysql.err.OperationalError: (2013, '
                "'Lost connection to MySQL server during query ([Errno 104] Connection reset by peer)')\n",
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=ERROR '
                'message=error with status PGRES_COPY_BOTH and no message from the libpq\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL '
                'message=error with status PGRES_COPY_BOTH and no message from the libpq\n',
                'snowflake.connector.errors.ProgrammingError: 091003 (22000): '
                'Failure using stage area. Cause: [Access Denied (Status Code: 403; Error Code: AccessDenied)]\n',
                'botocore.exceptions.HTTPClientError: An HTTP Client raised and unhandled exception: '
                "'No field numbered 1 is present in this asn1crypto.keys.PublicKeyInfo'\n",
                'foo.exception.FakeException: This is a test exception\n',
                'foo.error.FakeError: This is a test exception\n']

        # Should return the default max number of errors
        log_file = '{}/resources/sample_log_files/tap-run-lot-of-errors.log'.format(os.path.dirname(__file__))
        assert cli.utils.find_errors_in_log_file(log_file) == \
               ['time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 1\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 2\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 3\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 4\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 5\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 6\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 7\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 8\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 9\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 10\n']

        # Should return the custom max number of errors
        log_file = '{}/resources/sample_log_files/tap-run-lot-of-errors.log'.format(os.path.dirname(__file__))
        assert cli.utils.find_errors_in_log_file(log_file, max_errors=2) == \
               ['time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 1\n',
                'time=2020-07-15 11:24:43 logger_name=tap_postgres log_level=CRITICAL This is a critical error 2\n']

        # Should return the custom max number of errors
        log_file = '{}/resources/sample_log_files/tap-run-errors.log'.format(os.path.dirname(__file__))
        assert cli.utils.find_errors_in_log_file(log_file, error_pattern=re.compile('CUSTOM-ERR-PATTERN')) == \
               ['CUSTOM-ERR-PATTERN: This is a custom pattern error message\n']

    def test_generate_rand_str_exc(self):
        """generate_random_string given a length lower than 1, expect an exception"""
        with pytest.raises(Exception):
            cli.utils.generate_random_string(-1)

    def test_generate_rand_str_warning(self):
        """generate_random_string given a length between 1 and 8 expect result and warning"""
        with pytest.warns(Warning):
            random_str = cli.utils.generate_random_string(5)
            assert len(random_str) == 5

    def test_generate_rand_str_success(self):
        """generate_random_string given a length greater than or eq to 8 expect result"""
        random_str = cli.utils.generate_random_string(10)
        assert len(random_str) == 10

    def test_create_backup_of_the_file(self):
        """test if create_backup_of_the_file method works correctly when original file exists"""
        with TemporaryDirectory() as temp_dir:
            file_content = 'foo'
            test_file = 'test.tmp'
            # Creating the original file
            with open(f'{temp_dir}/{test_file}', 'w', encoding='utf-8') as tmp_file:
                tmp_file.write(file_content)

            cli.utils.create_backup_of_the_file(f'{temp_dir}/test.tmp')

            with open(f'{temp_dir}/{test_file}.bak', 'r', encoding='utf-8') as bak_file:
                bak_content = bak_file.read()

            assert bak_content == file_content

    def test_create_backup_of_the_file_if_original_file_does_not_exist(self):
        """test if create_backup_of_the_file method works correctly when original file not exists"""
        with TemporaryDirectory() as temp_dir:
            test_file = 'test.tmp'

            cli.utils.create_backup_of_the_file(f'{temp_dir}/test.tmp')

            with open(f'{temp_dir}/{test_file}.bak', 'r', encoding='utf-8') as bak_file:
                bak_content = bak_file.read()

            assert bak_content == 'ORIGINAL FILE DID NOT EXIST!'
