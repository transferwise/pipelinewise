import os
import re
import glob
import shlex
import pytest
import psycopg2
import subprocess

from dotenv import load_dotenv


DIR = os.path.dirname(__file__)

class TestE2E(object):
    """
    End to end tests
    """
    def setup_method(self):
        self.env = self.load_env()
        self.project_dir = os.path.join(DIR, "test-project")
        self.init_test_project_dir()

    def teardown_method(self):
        pass

    def load_env(self):
        """Load environment variables in priority order:
            1: Existing environment variables 
            2: Docker compose .env environment variables"""
        load_dotenv(dotenv_path=os.path.join(DIR, "../../dev-project/.env"))
        env = {}
        
        env['DB_TAP_POSTGRES_HOST'] = os.environ.get('DB_TAP_POSTGRES_HOST')
        env['DB_TAP_POSTGRES_PORT'] = os.environ.get('DB_TAP_POSTGRES_PORT')
        env['DB_TAP_POSTGRES_USER'] = os.environ.get('DB_TAP_POSTGRES_USER')
        env['DB_TAP_POSTGRES_PASSWORD'] = os.environ.get('DB_TAP_POSTGRES_PASSWORD')
        env['DB_TAP_POSTGRES_DB'] = os.environ.get('DB_TAP_POSTGRES_DB')

        env['DB_TAP_MYSQL_HOST'] = os.environ.get('DB_TAP_MYSQL_HOST')
        env['DB_TAP_MYSQL_PORT'] = os.environ.get('DB_TAP_MYSQL_PORT')
        env['DB_TAP_MYSQL_USER'] = os.environ.get('DB_TAP_MYSQL_USER')
        env['DB_TAP_MYSQL_PASSWORD'] = os.environ.get('DB_TAP_MYSQL_PASSWORD')
        env['DB_TAP_MYSQL_DB'] = os.environ.get('DB_TAP_MYSQL_DB')

        env['DB_TARGET_POSTGRES_HOST'] = os.environ.get('DB_TARGET_POSTGRES_HOST')
        env['DB_TARGET_POSTGRES_PORT'] = os.environ.get('DB_TARGET_POSTGRES_PORT')
        env['DB_TARGET_POSTGRES_USER'] = os.environ.get('DB_TARGET_POSTGRES_USER')
        env['DB_TARGET_POSTGRES_PASSWORD'] = os.environ.get('DB_TARGET_POSTGRES_PASSWORD')
        env['DB_TARGET_POSTGRES_DB'] = os.environ.get('DB_TARGET_POSTGRES_DB')

        return env 

    def init_test_project_dir(self):
        """Load every YML template from test-project directory, replace the environment
        variables to real values and save as consumable YAML files"""
        yml_templates = glob.glob("{}/*.yml.template".format(self.project_dir))
        for template_path in yml_templates:
            with open(template_path, 'r') as file:
                yaml = file.read()

                # Replace environment variables with string replace. PyYAML can't do it automatically 
                for env_var in self.env.keys():
                    yaml = yaml.replace("${{{}}}".format(env_var), self.env[env_var])

            yaml_path = template_path.replace(".template", "")
            with open(yaml_path, "w+") as file:
                file.write(yaml)

    def clean_target_postgres(self):
        """Clean postgres_dwh"""
        try:
            conn = psycopg2.connect(host=self.env['DB_TARGET_POSTGRES_HOST'],
                                    port=self.env['DB_TARGET_POSTGRES_PORT'],
                                    user=self.env['DB_TARGET_POSTGRES_USER'],
                                    password=self.env['DB_TARGET_POSTGRES_PASSWORD'],
                                    database=self.env['DB_TARGET_POSTGRES_DB'])
            conn.set_session(autocommit=True)
            cur = conn.cursor()

            # Drop target schemas if exists
            cur.execute("DROP SCHEMA IF EXISTS mysql_grp24 CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS postgres_world CASCADE;")

            # Create groups required for tests
            cur.execute("DROP GROUP IF EXISTS group1;")
            cur.execute("CREATE GROUP group1;")
        finally:
            cur.close()
            conn.close()

    def run_command(self, command):
        """Run shell command and return returncode, stdout and stderr"""
        proc = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        x = proc.communicate()
        rc = proc.returncode
        stdout = x[0].decode('utf-8')
        stderr = x[1].decode('utf-8')

        return [rc, stdout, stderr]

    def find_run_tap_log_file(self, stdout):
        """Pipelinewise creates log file per running tap instances in a dynamically created directory:
            ~/.pipelinewise/<TARGET_ID>/<TAP_ID>/log
            
            Every log file matches the pattern:
            <TARGET_ID>-<TAP_ID>-<DATE>_<TIME>.<SYNC_ENGINE>.log.<STATUS>

            The generated full path is logged to STDOUT when tap starting"""
        return re.search('Writing output into (.+log)', stdout).group(1)

    def assert_command_success(self, rc, stdout, stderr, log_path=None):
        """Assert helper function to check if command finished successfully.
        In case of failure it logs stdout, stderr and content of the failed command log
        if exists"""
        if rc != 0 or stderr != "":
            failed_log = ""
            # Load failed log file if exists
            if os.path.isfile("{}.success".format(log_path)):
                with open(log_path, 'r') as file:
                    failed_log = file.read()

            if failed_log:
                assert not "STDOUT: {}\nSTDERR: {}\nFAILED LOG: {}".format(str(stdout), str(stderr), failed_log)
            else:
                assert not "STDOUT: {}\nSTDERR: {}".format(str(stdout), str(stderr))

        assert True

    @pytest.mark.dependency(name="import_config")
    def test_import_project(self):
        """Import the YAML project with taps and target and do discovery mode to write the JSON files for singer connectors"""
        self.clean_target_postgres()
        [rc, stdout, stderr] = self.run_command("pipelinewise import_config --dir {}".format(self.project_dir))
        self.assert_command_success(rc, stdout, stderr)

    @pytest.mark.dependency(depends=["import_config"])
    def test_replicate_mariadb_to_postgres(self):
        """Replicate data from MariaDB to Postgres DWH, check if return code is zero and success log file created"""
        [rc, stdout, stderr] = self.run_command("pipelinewise run_tap --tap mariadb_source --target postgres_dwh")
        self.assert_command_success(rc, stdout, stderr)
        assert os.path.isfile("{}.success".format(self.find_run_tap_log_file(stdout)))

    @pytest.mark.dependency(depends=["import_config"])
    def test_replicate_postgres_to_postgres(self):
        """Replicate data from Postgres to Postgres DWH, check if return code is zero and success log file created"""
        [rc, stdout, stderr] = self.run_command("pipelinewise run_tap --tap postgres_source --target postgres_dwh")
        self.assert_command_success(rc, stdout, stderr)
        assert os.path.isfile("{}.success".format(self.find_run_tap_log_file(stdout)))

