import os
import re
import glob
import boto3
import shutil
import subprocess

from dotenv import load_dotenv
from . import db

USER_HOME = os.path.expanduser('~')
CONFIG_DIR = os.path.join(USER_HOME, '.pipelinewise')
DIR = os.path.dirname(os.path.realpath(__file__))


class E2EEnv:
    """Utilities class to run End to End tests

    This class provides functionalities to render tap and target YAML files,
    to run SQL queries on the supported databases and to run common assertions
    on the supported databases"""

    def __init__(self, project_dir):
        self._load_env()

        # Generate test project YAMLs from templates
        self._init_test_project_dir(project_dir)

    def _load_env(self):
        """Connector properties

        vars: Load environment variables in priority order:
            1: Existing environment variables
            2: Docker compose .env environment variables

        template_patterns:
            List of template file pattern where the env vars needs to be defined

        optional:
            Some connectors are mandatory and test database and test data included in the
            docker dev/test environment. Some connectors are optional, basically the ones
            which are not open sourced hence NOT included in the docker dev/test env.

        If optional connector properties are not defined in ../../../dev/project/.env then
        the related test cases will be skipped."""
        load_dotenv(
            dotenv_path=os.path.join(DIR, '..', '..', '..', 'dev-project', '.env')
        )
        self.env = {
            # ------------------------------------------------------------------
            # Tap Postgres is a REQUIRED test connector and test database with test data available
            # in the docker environment
            # ------------------------------------------------------------------
            'TAP_POSTGRES': {
                'template_patterns': ['tap_postgres'],
                'vars': {
                    'HOST': {
                        'value': os.environ.get('TAP_POSTGRES_HOST'),
                        'required': True,
                    },
                    'PORT': {
                        'value': os.environ.get('TAP_POSTGRES_PORT'),
                        'required': True,
                    },
                    'USER': {
                        'value': os.environ.get('TAP_POSTGRES_USER'),
                        'required': True,
                    },
                    'PASSWORD': {
                        'value': os.environ.get('TAP_POSTGRES_PASSWORD'),
                        'required': True,
                    },
                    'DB': {
                        'value': os.environ.get('TAP_POSTGRES_DB'),
                        'required': True,
                    },
                },
            },
            # ------------------------------------------------------------------
            # Tap MySQL is a REQUIRED test connector and test database with test data available
            # in the docker environment
            # ------------------------------------------------------------------
            'TAP_MYSQL': {
                'template_patterns': ['tap_mysql'],
                'vars': {
                    'HOST': {'value': os.environ.get('TAP_MYSQL_HOST')},
                    'PORT': {'value': os.environ.get('TAP_MYSQL_PORT')},
                    'USER': {'value': os.environ.get('TAP_MYSQL_USER')},
                    'PASSWORD': {'value': os.environ.get('TAP_MYSQL_PASSWORD')},
                    'DB': {'value': os.environ.get('TAP_MYSQL_DB')},
                },
            },
            # ------------------------------------------------------------------
            # Tap MongoDB is a REQUIRED test connector and test database with test data available
            # in the docker environment
            # ------------------------------------------------------------------
            'TAP_MONGODB': {
                'template_patterns': ['tap_postgres'],
                'vars': {
                    'HOST': {
                        'value': os.environ.get('TAP_MONGODB_HOST'),
                        'required': True,
                    },
                    'PORT': {
                        'value': os.environ.get('TAP_MONGODB_PORT'),
                        'required': True,
                    },
                    'USER': {
                        'value': os.environ.get('TAP_MONGODB_USER'),
                        'required': True,
                    },
                    'PASSWORD': {
                        'value': os.environ.get('TAP_MONGODB_PASSWORD'),
                        'required': True,
                    },
                    'DB': {'value': os.environ.get('TAP_MONGODB_DB'), 'required': True},
                    'AUTH_DB': {'value': 'admin', 'required': True},
                },
            },
            # ------------------------------------------------------------------
            # Tap S3 CSV is an OPTIONAL test connector and it requires credentials to a real S3 bucket.
            # To run the related tests add real S3 credentials to ../../../dev-project/.env
            # ------------------------------------------------------------------
            'TAP_S3_CSV': {
                'optional': True,
                'template_patterns': ['tap_s3_csv'],
                'vars': {
                    'AWS_KEY': {'value': os.environ.get('TAP_S3_CSV_AWS_KEY')},
                    'AWS_SECRET_ACCESS_KEY': {
                        'value': os.environ.get('TAP_S3_CSV_AWS_SECRET_ACCESS_KEY')
                    },
                    'BUCKET': {'value': os.environ.get('TAP_S3_CSV_BUCKET')},
                },
            },
            # ------------------------------------------------------------------
            # Target Postgres is a REQUIRED test connector and test database available in the docker environment
            # ------------------------------------------------------------------
            'TARGET_POSTGRES': {
                'template_patterns': ['target_postgres', 'to_pg'],
                'vars': {
                    'HOST': {'value': os.environ.get('TARGET_POSTGRES_HOST')},
                    'PORT': {'value': os.environ.get('TARGET_POSTGRES_PORT')},
                    'USER': {'value': os.environ.get('TARGET_POSTGRES_USER')},
                    'PASSWORD': {'value': os.environ.get('TARGET_POSTGRES_PASSWORD')},
                    'DB': {'value': os.environ.get('TARGET_POSTGRES_DB')},
                },
            },
            # ------------------------------------------------------------------
            # Target Snowflake is an OPTIONAL test connector because it's not open sourced and not part of
            # the docker environment.
            # To run the related test cases add real Snowflake credentials to ../../../dev-project/.env
            # ------------------------------------------------------------------
            'TARGET_SNOWFLAKE': {
                'optional': True,
                'template_patterns': ['target_snowflake', 'to_sf'],
                'vars': {
                    'ACCOUNT': {'value': os.environ.get('TARGET_SNOWFLAKE_ACCOUNT')},
                    'DBNAME': {'value': os.environ.get('TARGET_SNOWFLAKE_DBNAME')},
                    'USER': {'value': os.environ.get('TARGET_SNOWFLAKE_USER')},
                    'PASSWORD': {'value': os.environ.get('TARGET_SNOWFLAKE_PASSWORD')},
                    'WAREHOUSE': {
                        'value': os.environ.get('TARGET_SNOWFLAKE_WAREHOUSE')
                    },
                    'AWS_ACCESS_KEY': {
                        'value': os.environ.get('TARGET_SNOWFLAKE_AWS_ACCESS_KEY'),
                        'optional': True,
                    },
                    'AWS_SECRET_ACCESS_KEY': {
                        'value': os.environ.get(
                            'TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY'
                        ),
                        'optional': True,
                    },
                    'SESSION_TOKEN': {
                        'value': os.environ.get('TARGET_SNOWFLAKE_SESSION_TOKEN'),
                        'optional': True,
                    },
                    'S3_BUCKET': {
                        'value': os.environ.get('TARGET_SNOWFLAKE_S3_BUCKET')
                    },
                    'S3_KEY_PREFIX': {
                        'value': os.environ.get('TARGET_SNOWFLAKE_S3_KEY_PREFIX')
                    },
                    'S3_ACL': {
                        'value': os.environ.get('TARGET_SNOWFLAKE_S3_ACL'),
                        'optional': True,
                    },
                    'STAGE': {'value': os.environ.get('TARGET_SNOWFLAKE_STAGE')},
                    'FILE_FORMAT': {
                        'value': os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT')
                    },
                    'CLIENT_SIDE_ENCRYPTION_MASTER_KEY': {
                        'value': os.environ.get(
                            'TARGET_SNOWFLAKE_CLIENT_SIDE_ENCRYPTION_MASTER_KEY'
                        ),
                        'optional': True,
                    },
                    'SCHEMA_POSTFIX': {
                        'value': os.environ.get('TARGET_SNOWFLAKE_SCHEMA_POSTFIX', 'TEST'),
                        'optional': True,
                    }
                },
            },
            # ------------------------------------------------------------------
            # Target BigQuery is an OPTIONAL test connector because it's not open sourced and not part of
            # the docker environment.
            # To run the related test cases add real BigQuery credentials to ../../../dev-project/.env
            # ------------------------------------------------------------------
            'TARGET_BIGQUERY': {
                'optional': True,
                'template_patterns': ['target_bigquery', 'to_bq'],
                'vars': {
                    'PROJECT': {'value': os.environ.get('TARGET_BIGQUERY_PROJECT')},
                },
            },
            # ------------------------------------------------------------------
            # Target Redshift is an OPTIONAL test connector because it's not open sourced and not part of
            # the docker environment.
            # To run the related test cases add real Amazon Redshift credentials to ../../../dev-project/.env
            # ------------------------------------------------------------------
            'TARGET_REDSHIFT': {
                'optional': True,
                'template_patterns': ['target_redshift', 'to_rs'],
                'vars': {
                    'HOST': {'value': os.environ.get('TARGET_REDSHIFT_HOST')},
                    'PORT': {'value': os.environ.get('TARGET_REDSHIFT_PORT')},
                    'USER': {'value': os.environ.get('TARGET_REDSHIFT_USER')},
                    'PASSWORD': {'value': os.environ.get('TARGET_REDSHIFT_PASSWORD')},
                    'DBNAME': {'value': os.environ.get('TARGET_REDSHIFT_DBNAME')},
                    'AWS_ACCESS_KEY': {
                        'value': os.environ.get('TARGET_REDSHIFT_AWS_ACCESS_KEY'),
                        'optional': True,
                    },
                    'AWS_SECRET_ACCESS_KEY': {
                        'value': os.environ.get(
                            'TARGET_REDSHIFT_AWS_SECRET_ACCESS_KEY'
                        ),
                        'optional': True,
                    },
                    'SESSION_TOKEN': {
                        'value': os.environ.get('TARGET_REDSHIFT_SESSION_TOKEN'),
                        'optional': True,
                    },
                    'COPY_ROLE_ARN': {
                        'value': os.environ.get('TARGET_REDSHIFT_COPY_ROLE_ARN'),
                        'optional': True,
                    },
                    'S3_BUCKET': {'value': os.environ.get('TARGET_REDSHIFT_S3_BUCKET')},
                    'S3_KEY_PREFIX': {
                        'value': os.environ.get('TARGET_REDSHIFT_S3_KEY_PREFIX')
                    },
                    'S3_ACL': {
                        'value': os.environ.get('TARGET_REDSHIFT_S3_ACL'),
                        'optional': True,
                    },
                },
            },
        }

        # Add is_configured keys for every connector
        # Useful to skip certain test cases dynamically when specific tap
        # or target database is not configured
        self.env['TAP_POSTGRES']['is_configured'] = self._is_env_connector_configured(
            'TAP_POSTGRES'
        )
        self.env['TAP_MYSQL']['is_configured'] = self._is_env_connector_configured(
            'TAP_MYSQL'
        )
        self.env['TAP_S3_CSV']['is_configured'] = self._is_env_connector_configured(
            'TAP_S3_CSV'
        )
        self.env['TARGET_POSTGRES'][
            'is_configured'
        ] = self._is_env_connector_configured('TARGET_POSTGRES')
        self.env['TARGET_REDSHIFT'][
            'is_configured'
        ] = self._is_env_connector_configured('TARGET_REDSHIFT')
        self.env['TARGET_SNOWFLAKE'][
            'is_configured'
        ] = self._is_env_connector_configured('TARGET_SNOWFLAKE')
        self.env['TARGET_BIGQUERY'][
            'is_configured'
        ] = self._is_env_connector_configured('TARGET_BIGQUERY')

    def _get_conn_env_var(self, connector, key):
        """Get the value of a specific variable in the self.env dict"""
        return self.env[connector]['vars'][key]['value']

    def get_aws_session(self):
        """Get AWS session with using access from TARGET_SNOWFLAKE_ env vars"""
        if not self.env['TARGET_SNOWFLAKE']['is_configured']:
            raise Exception('TARGET_SNOWFLAKE is not configured')

        aws_access_key_id = os.environ.get('TARGET_SNOWFLAKE_AWS_ACCESS_KEY')
        aws_secret_access_key = os.environ.get('TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY')
        if aws_access_key_id is None or aws_secret_access_key is None:
            raise Exception(
                'Env vars TARGET_SNOWFLAKE_AWS_ACCESS_KEY and TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY are required'
            )

        return boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def _is_env_connector_configured(self, env_connector):
        """Detect if certain component(s) of env vars group is configured properly"""
        env_conns = []
        if isinstance(env_connector, str):
            env_conns.append(env_connector)
        elif isinstance(env_connector, list):
            env_conns = env_connector
        else:
            raise Exception('env_connector must be string or list')

        for env_conn in env_conns:
            for key, value in self.env[env_conn]['vars'].items():
                # If value not defined and is not optional
                if not value['value'] and not value.get('optional'):
                    # Value not defined but the entirely component is optional
                    if self.env[env_conn].get('optional'):
                        return False
                    # Value not defined but it's a required property
                    raise Exception(
                        f'{env_conn}_{key} env var is required but not defined.'
                    )
        return True

    def _find_env_conn_by_template_name(self, template_name):
        """Find env connectors by template filename patterns
        Returns list of self.env connector keys"""
        env_connectors = []
        for connector, props in self.env.items():
            for pattern in props['template_patterns']:
                if pattern in template_name:
                    env_connectors.append(connector)
        return env_connectors

    # pylint: disable=invalid-name
    def _all_env_vars_to_dict(self):
        """Transform self.env dict to a simple key-value dictionary
        From:
            {
                'TAP_X': {'vars': {'HOST': {'value': 'my_host_x'}}},
                'TAP_Y': {'vars': {'HOST': {'value': 'my_host_y'}}}
            }
        To:
            {
                'TAP_X_HOST': 'my_host_x',
                'TAP_Y_HOST': 'my_host_y'
            }"""
        all_env_vars = {}
        for k, v in self.env.items():
            c_vars = {}
            for x, y in v['vars'].items():
                c_vars[f'{k}_{x}'] = y['value']
            all_env_vars = {**all_env_vars, **c_vars}
        return all_env_vars

    def _init_test_project_dir(self, project_dir):
        """Load every YML template from test-project directory, but ONLY if env vars configured.
        Replace the environment variables to real values and save as consumable YAML files

        TODO: consider using a real template engine"""
        templates = glob.glob(f'{project_dir}/*.yml.template')
        for template_path in templates:
            # Replace env vars in template
            with open(template_path, 'r', encoding='utf-8') as f_template:
                yaml = f_template.read()

                # Detect if every env var configured for the template
                template = os.path.basename(template_path)
                yaml_path = template_path.replace('.template', '')
                env_connectors = self._find_env_conn_by_template_name(template)
                is_configured = self._is_env_connector_configured(env_connectors)

                # "Render" the template and save to file if env vars configured
                if is_configured:
                    template_vars = set(re.findall(r'\$\{(.+?)\}', yaml))
                    for var in template_vars:
                        yaml = yaml.replace(
                            f'${{{var}}}', self._all_env_vars_to_dict().get(var)
                        )

                    # Write the template replaced YAML file
                    with open(yaml_path, 'w+', encoding='utf-8') as f_render:
                        f_render.write(yaml)

                # Delete if exists but not configured
                else:
                    try:
                        os.remove(yaml_path)
                    except OSError:
                        pass

    @staticmethod
    def _run_command(args):
        """Run a command in a subprocess"""
        subprocess.run(args, check=True)

    # -------------------------------------------------------------------------
    # Database functions to run queries in source and target databases
    # -------------------------------------------------------------------------

    def run_query_tap_postgres(self, query):
        """Run and SQL query in tap postgres database"""
        return db.run_query_postgres(
            query,
            host=self._get_conn_env_var('TAP_POSTGRES', 'HOST'),
            port=self._get_conn_env_var('TAP_POSTGRES', 'PORT'),
            user=self._get_conn_env_var('TAP_POSTGRES', 'USER'),
            password=self._get_conn_env_var('TAP_POSTGRES', 'PASSWORD'),
            database=self._get_conn_env_var('TAP_POSTGRES', 'DB'),
        )

    def get_tap_mongodb_connection(self):
        """Create and returns tap mongodb database instance to run queries on"""
        return db.get_mongodb_connection(
            host=self._get_conn_env_var('TAP_MONGODB', 'HOST'),
            port=self._get_conn_env_var('TAP_MONGODB', 'PORT'),
            user=self._get_conn_env_var('TAP_MONGODB', 'USER'),
            password=self._get_conn_env_var('TAP_MONGODB', 'PASSWORD'),
            database=self._get_conn_env_var('TAP_MONGODB', 'DB'),
            auth_database=self._get_conn_env_var('TAP_MONGODB', 'AUTH_DB'),
        )

    def run_query_target_postgres(self, query: object) -> object:
        """Run and SQL query in target postgres database"""
        return db.run_query_postgres(
            query,
            host=self._get_conn_env_var('TARGET_POSTGRES', 'HOST'),
            port=self._get_conn_env_var('TARGET_POSTGRES', 'PORT'),
            user=self._get_conn_env_var('TARGET_POSTGRES', 'USER'),
            password=self._get_conn_env_var('TARGET_POSTGRES', 'PASSWORD'),
            database=self._get_conn_env_var('TARGET_POSTGRES', 'DB'),
        )

    def run_query_target_redshift(self, query):
        """Run an SQL query in target redshift database"""
        return db.run_query_redshift(
            query,
            host=self._get_conn_env_var('TARGET_REDSHIFT', 'HOST'),
            port=self._get_conn_env_var('TARGET_REDSHIFT', 'PORT'),
            user=self._get_conn_env_var('TARGET_REDSHIFT', 'USER'),
            password=self._get_conn_env_var('TARGET_REDSHIFT', 'PASSWORD'),
            database=self._get_conn_env_var('TARGET_REDSHIFT', 'DBNAME'),
        )

    # pylint: disable=unnecessary-pass
    def run_query_tap_s3_csv(self, file):
        """Get file from S3 and read into the file
        This function is not yet implemented"""
        pass

    def run_query_tap_mysql(self, query):
        """Run and SQL query in tap mysql database"""
        return db.run_query_mysql(
            query,
            host=self._get_conn_env_var('TAP_MYSQL', 'HOST'),
            port=int(self._get_conn_env_var('TAP_MYSQL', 'PORT')),
            user=self._get_conn_env_var('TAP_MYSQL', 'USER'),
            password=self._get_conn_env_var('TAP_MYSQL', 'PASSWORD'),
            database=self._get_conn_env_var('TAP_MYSQL', 'DB'),
        )

    def run_query_target_snowflake(self, query):
        """Run and SQL query in target snowflake database"""
        return db.run_query_snowflake(
            query,
            account=self._get_conn_env_var('TARGET_SNOWFLAKE', 'ACCOUNT'),
            database=self._get_conn_env_var('TARGET_SNOWFLAKE', 'DBNAME'),
            warehouse=self._get_conn_env_var('TARGET_SNOWFLAKE', 'WAREHOUSE'),
            user=self._get_conn_env_var('TARGET_SNOWFLAKE', 'USER'),
            password=self._get_conn_env_var('TARGET_SNOWFLAKE', 'PASSWORD'),
        )

    def delete_dataset_target_bigquery(self, dataset):
        """Run and SQL query in target bigquery database"""
        return db.delete_dataset_bigquery(
            dataset, project=self._get_conn_env_var('TARGET_BIGQUERY', 'PROJECT')
        )

    def run_query_target_bigquery(self, query):
        """Run and SQL query in target bigquery database"""
        return db.run_query_bigquery(
            query, project=self._get_conn_env_var('TARGET_BIGQUERY', 'PROJECT')
        )

    # -------------------------------------------------------------------------
    # Setup methods to initialise source and target databases and to make them
    # ready running the tests
    # -------------------------------------------------------------------------

    def setup_tap_mysql(self):
        """Clean mysql source database and prepare for test run
        Creating initial tables is defined in Docker entrypoint.sh"""
        db_script = os.path.join(DIR, '..', '..', 'db', 'tap_mysql_db.sh')
        self._run_command(db_script)

    # pylint: disable=unnecessary-pass
    def setup_tap_postgres(self):
        """Clean postgres source database and prepare for test run
        Creating initial tables is defined in Docker entrypoint.sh"""
        db_script = os.path.join(DIR, '..', '..', 'db', 'tap_postgres_db.sh')
        self._run_command(db_script)

    def setup_tap_mongodb(self):
        """Clean postgres source database and prepare for test run
        Creating initial tables is defined in Docker entrypoint.sh"""
        db_script = os.path.join(DIR, '..', '..', 'db', 'tap_mongodb.sh')
        self._run_command(db_script)

    def setup_tap_s3_csv(self):
        """Upload test input files to S3 to be prapared for test run"""
        mock_data_1 = os.path.join(
            DIR, '..', 'test-project', 's3_mock_data', 'mock_data_1.csv'
        )
        mock_data_2 = os.path.join(
            DIR, '..', 'test-project', 's3_mock_data', 'mock_data_2.csv'
        )

        bucket = self._get_conn_env_var('TAP_S3_CSV', 'BUCKET')
        s3 = boto3.client(
            's3',
            aws_access_key_id=self._get_conn_env_var('TAP_S3_CSV', 'AWS_KEY'),
            aws_secret_access_key=self._get_conn_env_var(
                'TAP_S3_CSV', 'AWS_SECRET_ACCESS_KEY'
            ),
        )

        s3.upload_file(mock_data_1, bucket, 'ppw_e2e_tap_s3_csv/mock_data_1.csv')
        s3.upload_file(mock_data_2, bucket, 'ppw_e2e_tap_s3_csv/mock_data_2.csv')

    def setup_target_postgres(self):
        """Clean postgres target database and prepare for test run"""
        self.run_query_target_postgres('CREATE EXTENSION IF NOT EXISTS pgcrypto')
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_public2 CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_logical1 CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_logical2 CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_mysql CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_s3_csv CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_mongodb CASCADE'
        )

        # Clean config directory
        shutil.rmtree(os.path.join(CONFIG_DIR, 'postgres_dwh'), ignore_errors=True)

    def setup_target_redshift(self):
        """Clean redshift target database and prepare for test run"""
        self.run_query_target_redshift(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres CASCADE'
        )
        self.run_query_target_redshift(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_public2 CASCADE'
        )
        self.run_query_target_redshift(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_logical1 CASCADE'
        )
        self.run_query_target_redshift(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_logical2 CASCADE'
        )
        self.run_query_target_redshift(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_mysql CASCADE'
        )
        self.run_query_target_redshift(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_s3_csv CASCADE'
        )
        self.run_query_target_redshift('DROP SCHEMA IF EXISTS ppw_e2e_helper CASCADE')
        self.run_query_target_redshift('CREATE SCHEMA ppw_e2e_helper')
        self.run_query_target_redshift(
            'CREATE TABLE ppw_e2e_helper.dual (dummy VARCHAR)'
        )
        self.run_query_target_redshift('INSERT INTO ppw_e2e_helper.dual VALUES (\'X\')')

        # Clean config directory
        shutil.rmtree(os.path.join(CONFIG_DIR, 'redshift'), ignore_errors=True)

    def setup_target_snowflake(self):
        """Clean snowflake target database and prepare for test run"""
        self.run_query_target_snowflake(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres CASCADE'
        )
        self.run_query_target_snowflake(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_public2 CASCADE'
        )
        self.run_query_target_snowflake(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_logical1 CASCADE'
        )
        self.run_query_target_snowflake(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_logical2 CASCADE'
        )
        self.run_query_target_snowflake(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_mysql CASCADE'
        )
        self.run_query_target_snowflake(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_s3_csv CASCADE'
        )
        self.run_query_target_snowflake(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_mongodb CASCADE'
        )

        # Clean config directory
        shutil.rmtree(os.path.join(CONFIG_DIR, 'snowflake'), ignore_errors=True)

    def setup_target_bigquery(self):
        """Clean bigquery target database and prepare for test run"""
        self.delete_dataset_target_bigquery('ppw_e2e_tap_postgres')
        self.delete_dataset_target_bigquery('ppw_e2e_tap_postgres_public2')
        self.delete_dataset_target_bigquery('ppw_e2e_tap_postgres_logical1')
        self.delete_dataset_target_bigquery('ppw_e2e_tap_postgres_logical2')
        self.delete_dataset_target_bigquery('ppw_e2e_tap_mysql')
        self.delete_dataset_target_bigquery('ppw_e2e_tap_s3_csv')
        self.delete_dataset_target_bigquery('ppw_e2e_tap_mongodb')
