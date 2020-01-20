import os
import re
import glob
import shlex
import shutil
import pytest
import psycopg2
import subprocess
import snowflake.connector

from dotenv import load_dotenv
from pathlib import Path

DIR = os.path.dirname(__file__)
USER_HOME = os.path.expanduser('~')
CONFIG_DIR = os.path.join(USER_HOME, '.pipelinewise')


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
        env = {
            'DB_TAP_POSTGRES_HOST': os.environ.get('DB_TAP_POSTGRES_HOST'),
            'DB_TAP_POSTGRES_PORT': os.environ.get('DB_TAP_POSTGRES_PORT'),
            'DB_TAP_POSTGRES_USER': os.environ.get('DB_TAP_POSTGRES_USER'),
            'DB_TAP_POSTGRES_PASSWORD': os.environ.get('DB_TAP_POSTGRES_PASSWORD'),
            'DB_TAP_POSTGRES_DB': os.environ.get('DB_TAP_POSTGRES_DB'),
            'DB_TAP_MYSQL_HOST': os.environ.get('DB_TAP_MYSQL_HOST'),
            'DB_TAP_MYSQL_PORT': os.environ.get('DB_TAP_MYSQL_PORT'),
            'DB_TAP_MYSQL_USER': os.environ.get('DB_TAP_MYSQL_USER'),
            'DB_TAP_MYSQL_PASSWORD': os.environ.get('DB_TAP_MYSQL_PASSWORD'),
            'DB_TAP_MYSQL_DB': os.environ.get('DB_TAP_MYSQL_DB'),
            'DB_TARGET_POSTGRES_HOST': os.environ.get('DB_TARGET_POSTGRES_HOST'),
            'DB_TARGET_POSTGRES_PORT': os.environ.get('DB_TARGET_POSTGRES_PORT'),
            'DB_TARGET_POSTGRES_USER': os.environ.get('DB_TARGET_POSTGRES_USER'),
            'DB_TARGET_POSTGRES_PASSWORD': os.environ.get('DB_TARGET_POSTGRES_PASSWORD'),
            'DB_TARGET_POSTGRES_DB': os.environ.get('DB_TARGET_POSTGRES_DB'),
            'TARGET_SNOWFLAKE_ACCOUNT': os.environ.get('TARGET_SNOWFLAKE_ACCOUNT'),
            'TARGET_SNOWFLAKE_DBNAME': os.environ.get('TARGET_SNOWFLAKE_DBNAME'),
            'TARGET_SNOWFLAKE_USER': os.environ.get('TARGET_SNOWFLAKE_USER'),
            'TARGET_SNOWFLAKE_PASSWORD': os.environ.get('TARGET_SNOWFLAKE_PASSWORD'),
            'TARGET_SNOWFLAKE_WAREHOUSE': os.environ.get('TARGET_SNOWFLAKE_WAREHOUSE'),
            'TARGET_SNOWFLAKE_AWS_ACCESS_KEY': os.environ.get('TARGET_SNOWFLAKE_AWS_ACCESS_KEY'),
            'TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY': os.environ.get('TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY'),
            'TARGET_SNOWFLAKE_S3_BUCKET': os.environ.get('TARGET_SNOWFLAKE_S3_BUCKET'),
            'TARGET_SNOWFLAKE_S3_KEY_PREFIX': os.environ.get('TARGET_SNOWFLAKE_S3_KEY_PREFIX'),
            'TARGET_SNOWFLAKE_STAGE': os.environ.get('TARGET_SNOWFLAKE_STAGE'),
            'TARGET_SNOWFLAKE_FILE_FORMAT': os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT'),
            'TAP_S3_CSV_SOURCE_AWS_KEY': os.environ.get('TAP_S3_CSV_SOURCE_AWS_KEY'),
            'TAP_S3_CSV_SOURCE_AWS_SECRET_ACCESS_KEY': os.environ.get('TAP_S3_CSV_SOURCE_AWS_SECRET_ACCESS_KEY'),
            'TAP_S3_CSV_SOURCE_BUCKET': os.environ.get('TAP_S3_CSV_SOURCE_BUCKET'),
        }

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
        with psycopg2.connect(host=self.env['DB_TARGET_POSTGRES_HOST'],
                              port=self.env['DB_TARGET_POSTGRES_PORT'],
                              user=self.env['DB_TARGET_POSTGRES_USER'],
                              password=self.env['DB_TARGET_POSTGRES_PASSWORD'],
                              database=self.env['DB_TARGET_POSTGRES_DB']) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                # Drop target schemas if exists
                cur.execute("DROP SCHEMA IF EXISTS mysql_grp24 CASCADE;")
                cur.execute("DROP SCHEMA IF EXISTS postgres_world CASCADE;")

                # Create groups required for tests
                cur.execute("DROP GROUP IF EXISTS group1;")
                cur.execute("CREATE GROUP group1;")

        # Clean config directory
        shutil.rmtree(os.path.join(CONFIG_DIR, 'postgres_dwh'), ignore_errors=True)

    def clean_target_snowflake(self):
        """Clean snowflake"""
        with snowflake.connector.connect(account=self.env['TARGET_SNOWFLAKE_ACCOUNT'],
                                         database=self.env['TARGET_SNOWFLAKE_DBNAME'],
                                         warehouse=self.env['TARGET_SNOWFLAKE_WAREHOUSE'],
                                         user=self.env['TARGET_SNOWFLAKE_USER'],
                                         password=self.env['TARGET_SNOWFLAKE_PASSWORD'],
                                         autocommit=True) as conn:
            with conn.cursor() as cur:
                # Drop target schemas if exists
                cur.execute("DROP SCHEMA IF EXISTS mysql_grp24")
                cur.execute("DROP SCHEMA IF EXISTS postgres_world_sf")
                cur.execute("DROP SCHEMA IF EXISTS s3_feeds")

        # Clean config directory
        shutil.rmtree(os.path.join(CONFIG_DIR, 'snowflake'), ignore_errors=True)

    def run_command(self, command):
        """Run shell command and return returncode, stdout and stderr"""
        proc = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        x = proc.communicate()
        rc = proc.returncode
        stdout = x[0].decode('utf-8')
        stderr = x[1].decode('utf-8')

        return [rc, stdout, stderr]

    def find_run_tap_log_file(self, stdout, sync_engine=None):
        """Pipelinewise creates log file per running tap instances in a dynamically created directory:
            ~/.pipelinewise/<TARGET_ID>/<TAP_ID>/log
            
            Every log file matches the pattern:
            <TARGET_ID>-<TAP_ID>-<DATE>_<TIME>.<SYNC_ENGINE>.log.<STATUS>

            The generated full path is logged to STDOUT when tap starting"""
        if sync_engine:
            pattern = re.compile(r"Writing output into (.+\.{}\.log)".format(sync_engine))
        else:
            pattern = re.compile(r"Writing output into (.+\.log)")

        return pattern.search(stdout).group(1)

    def assert_command_success(self, rc, stdout, stderr, log_path=None):
        """Assert helper function to check if command finished successfully.
        In case of failure it logs stdout, stderr and content of the failed command log
        if exists"""
        if rc != 0 or stderr != "":
            failed_log = ""
            failed_log_path = f"{log_path}.failed"
            # Load failed log file if exists
            if os.path.isfile(failed_log_path):
                with open(failed_log_path, 'r') as file:
                    failed_log = file.read()

            print(f"STDOUT: {stdout}\nSTDERR: {stderr}\nFAILED LOG: {failed_log}")
            assert False

        # check success log file if log path defined
        success_log_path = f"{log_path}.success"
        if log_path and not os.path.isfile(success_log_path):
            assert False
        else:
            assert True

    def assert_state_file_valid(self, target_name, tap_name, log_path=None):
        """Assert helper function to check if state file exists for a certain tap
        for a certain target"""
        state_file = Path(f"{Path.home()}/.pipelinewise/{target_name}/{tap_name}/state.json").resolve()
        assert os.path.isfile(state_file)

        # Check if state file content equals to last emitted state in log
        if log_path:
            success_log_path = f"{log_path}.success"
            state_in_log = None
            with open(success_log_path, 'r') as log_f:
                state_log_pattern = re.search(r'\nINFO STATE emitted from target: (.+\n)', '\n'.join(log_f.readlines()))
                if state_log_pattern:
                    state_in_log = state_log_pattern.groups()[-1]

            # If the emitted state message exists in the log then compare it to the actual state file
            if state_in_log:
                with open(state_file, 'r') as state_f:
                    assert state_in_log == ''.join(state_f.readlines())

    def assert_run_tap_success(self, tap, target, sync_engines):
        """Run a specific tap and make sure that it's using the correct sync engine,
        finished successfully and state file created with the right content"""
        [rc, stdout, stderr] = self.run_command("pipelinewise run_tap --tap {} --target {}".format(tap, target))
        for sync_engine in sync_engines:
            log_file = self.find_run_tap_log_file(stdout, sync_engine)
            self.assert_command_success(rc, stdout, stderr, log_file)
            self.assert_state_file_valid(target, tap, log_file)

    @pytest.mark.dependency(name="import_config")
    def test_import_project(self):
        """Import the YAML project with taps and target and do discovery mode to write the JSON files for singer
        connectors """
        self.clean_target_postgres()
        self.clean_target_snowflake()
        [rc, stdout, stderr] = self.run_command("pipelinewise import_config --dir {}".format(self.project_dir))
        self.assert_command_success(rc, stdout, stderr)

    @pytest.mark.dependency(depends=["import_config"])
    def test_replicate_mariadb_to_postgres(self):
        """Replicate data from MariaDB to Postgres DWH, check if return code is zero and success log file created"""
        self.assert_run_tap_success('mariadb_source', 'postgres_dwh', ['singer'])

    @pytest.mark.dependency(depends=["import_config"])
    def test_replicate_postgres_to_postgres(self):
        """Replicate data from Postgres to Postgres DWH, check if return code is zero and success log file created"""
        self.assert_run_tap_success('postgres_source', 'postgres_dwh', ['singer'])

    @pytest.mark.dependency(depends=["import_config"])
    def test_replicate_mariadb_to_snowflake(self):
        """Replicate data from MariaDB to Snowflake DWH, check if return code is zero and success log file created"""
        tap, target = 'mariadb_to_sf', 'snowflake'

        # Run tap first time - both fastsync and a singer should be triggered
        self.assert_run_tap_success(tap, target, ['fastsync', 'singer'])

        # Run tap second time - only singer should be triggered
        self.assert_run_tap_success(tap, target, ['singer'])

    @pytest.mark.dependency(depends=["import_config"])
    def test_replicate_postgres_to_snowflake(self):
        """Replicate data from Postgres to Snowflake, check if return code is zero and success log file created"""
        tap, target = 'postgres_source_sf', 'snowflake'

        # Run tap first time - both fastsync and a singer should be triggered
        self.assert_run_tap_success(tap, target, ['fastsync', 'singer'])

        # Run tap second time - only singer should be triggered
        self.assert_run_tap_success(tap, target, ['singer'])

    @pytest.mark.dependency(depends=["import_config"])
    def test_replicate_s3_to_snowflake(self):
        """Replicate csv files from s3 to Snowflake, check if return code is zero and success log file created"""
        tap, target = 'csv_on_s3', 'snowflake'

        # Run tap first time - both fastsync and a singer should be triggered
        self.assert_run_tap_success(tap, target, ['fastsync', 'singer'])

        # Run tap second time - only singer should be triggered
        self.assert_run_tap_success(tap, target, ['singer'])
