import os
import pytest

from .env import E2EEnv
from . import tasks
from . import assertions

DIR = os.path.dirname(__file__)


# pylint: disable=attribute-defined-outside-init
class TestTargetRedshift:
    """
    End to end tests for Target Redshift
    """

    def setup_method(self):
        """Initialise test project by generating YAML files from
        templates for all the configured connectors"""
        self.project_dir = os.path.join(DIR, 'test-project')
        self.e2e = E2EEnv(self.project_dir)

    def teardown_method(self):
        """Delete test directories and database objects"""

    @pytest.mark.dependency(name='import_config')
    def test_import_project(self):
        """Import the YAML project with taps and target and do discovery mode
        to write the JSON files for singer connectors """

        # Skip every target_postgres related test if env vars not provided
        if not self.e2e.env['TARGET_REDSHIFT']['is_configured']:
            pytest.skip('Target Redshift environment variables are not provided')

        # Setup and clean source and target databases
        self.e2e.setup_tap_mysql()
        self.e2e.setup_tap_postgres()
        self.e2e.setup_target_redshift()

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(f'pipelinewise import_config --dir {self.project_dir}')
        assertions.assert_command_success(return_code, stdout, stderr)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_rs(self):
        """Replicate data from Postgres to Redshift DWH"""
        assertions.assert_run_tap_success('postgres_to_rs', 'redshift', ['singer'])
        # Add an object reference to avoid to use classmethod. TODO: Add more real tests
        assert self.e2e == self.e2e

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_rs(self):
        """Replicate data from Postgres to Redshift DWH"""
        assertions.assert_run_tap_success('postgres_to_rs', 'redshift', ['singer'])
        # Add an object reference to avoid to use classmethod. TODO: Add more real tests
        assert self.e2e == self.e2e
