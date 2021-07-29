import os
import pipelinewise.cli as cli
import pytest

TAP_GITHUB_YAML = '{}/resources/tap-github.yml'.format(os.path.dirname(__file__))

# pylint: disable=no-self-use,too-many-public-methods,fixme
class TestUtils:
    """
    Unit Tests for Tap Github PipelineWise CLI utility functions
    """
    def assert_json_is_invalid(self, schema, invalid_yaml):
        """Simple assertion to check if validate function exits with error"""
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            cli.utils.validate(invalid_yaml, schema)

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    def test_should_pass_with_valid_json_schema(self):
        """
        Test Should pass with valid json schema
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        assert cli.utils.validate(actual_yaml, schema) is None

    def test_should_fail_if_organization_missing_when_repos_include_is_missing(self):
        """
        Test should fail if organization is missing when repos include is missing
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        del actual_yaml['db_conn']['organization']
        del actual_yaml['db_conn']['repos_include']

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_access_token_is_missing(self):
        """
        Test Should fail when access token is missing
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        del actual_yaml['db_conn']['access_token']

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_start_date_is_missing(self):
        """
        Test should fail when start date is missing
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        del actual_yaml['db_conn']['start_date']

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_access_token_is_not_string(self):
        """
        Test should fail when acess token is not string
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        actual_yaml['db_conn']['access_token'] = 123456

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_start_date_is_not_string(self):
        """
        Test should fail when start date is not string
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        actual_yaml['db_conn']['start_date'] = 123456

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_organization_is_not_string(self):
        """
        Test should fail when organization is not string
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        actual_yaml['db_conn']['organization'] = []

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_repos_include_is_not_string(self):
        """
        Test should fail when repos include is not string
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        actual_yaml['db_conn']['repos_include'] = []

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_repos_exclude_is_not_string(self):
        """
        Test should fail when repos exclude is not string
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        actual_yaml['db_conn']['repos_include'] = {}

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_repository_is_not_string(self):
        """
        Test should fail when repository is not string
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        actual_yaml['db_conn']['repository'] = {}

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_include_archived_is_not_boolean(self):
        """
        Test should fail when include archived is not boolean
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        actual_yaml['db_conn']['include_archived'] = 'false'

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_include_disabled_is_not_boolean(self):
        """
        Test should fail when include disabled is not boolean
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        actual_yaml['db_conn']['include_archived'] = 'false'

        self.assert_json_is_invalid(schema, actual_yaml)

    def test_should_fail_when_max_rate_limit_wait_seconds_is_not_integer(self):
        """
        Test should fail when max rate limit wait seconds is not integer
        """
        schema = cli.utils.load_schema('tap')

        actual_yaml = cli.utils.load_yaml(TAP_GITHUB_YAML)
        actual_yaml['db_conn']['max_rate_limit_wait_seconds'] = '111'

        self.assert_json_is_invalid(schema, actual_yaml)
