import os
import pytest

from .env import E2EEnv
from . import tasks
from . import assertions

DIR = os.path.dirname(__file__)


# pylint: disable=attribute-defined-outside-init
class TestTargetPostgres:
    """
    End to end tests for Target Postgres
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

        # Skip every target_postgres related test if required env vars not provided
        if not self.e2e.env['TARGET_POSTGRES']['is_configured']:
            pytest.skip('Target Postgres environment variables are not provided')

        # Setup and clean source and target databases
        self.e2e.setup_tap_mysql()
        self.e2e.setup_tap_postgres()
        self.e2e.setup_target_postgres()

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(f'pipelinewise import_config --dir {self.project_dir}')
        assertions.assert_command_success(return_code, stdout, stderr)

    # pylint: disable=fixme,no-self-use
    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_pg(self):
        """Replicate data from MariaDB to Postgres DWH
        Check if return code is zero and success log file created"""
        # TODO - Real and more complex e2e tests will be added here
        assert True

    # pylint: disable=fixme,no-self-use
    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_pg(self):
        """Replicate data from Postgres to Postgres DWH, check if return code is zero and success log file created"""
        # TODO - Real and more complex e2e tests will be added here
        assert True
