import os
import re
import glob

import boto3
import shutil
import subprocess
import uuid
from pathlib import Path

from dotenv import load_dotenv
from . import db

USER_HOME = os.path.expanduser('~')
CONFIG_DIR = os.path.join(USER_HOME, '.pipelinewise')
DIR = os.path.dirname(os.path.realpath(__file__))
E2E_NAMESPACE_ENV = 'PIPELINEWISE_E2E_NAMESPACE'
TAP_S3_CSV_KEY_PREFIX = 'ppw_e2e_tap_s3_csv'
SNOWFLAKE_ARCHIVE_S3_PREFIX = 'archive_folder'


def _load_e2e_namespace():
    """Return an optional S3-safe namespace for concurrent E2E jobs."""
    namespace = os.environ.get(E2E_NAMESPACE_ENV, '')
    if namespace and re.fullmatch(r'[A-Za-z0-9_-]+', namespace) is None:
        raise ValueError(
            f'{E2E_NAMESPACE_ENV} must contain only letters, digits, underscores, '
            'and hyphens'
        )
    return namespace


def _namespaced_s3_path(base_prefix, namespace):
    """Append a namespace as one path segment without duplicating slashes."""
    return f'{base_prefix.rstrip("/")}/{namespace}'


# pylint: disable=too-many-public-methods
class E2EEnv:
    """Utilities class to run End to End tests

    This class provides functionalities to render tap and target YAML files,
    to run SQL queries on the supported databases and to run common assertions
    on the supported databases"""

    def __init__(self, project_dir):
        self.sf_schema_postfix = f'_{str(uuid.uuid4())[:8]}'
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
        schema_postfix_override = os.environ.get('TARGET_SNOWFLAKE_SCHEMA_POSTFIX')
        self.sf_schema_postfix_is_override = bool(schema_postfix_override)
        self.sf_schema_postfix = schema_postfix_override or self.sf_schema_postfix
        self.e2e_namespace = _load_e2e_namespace()
        tap_s3_csv_key_prefix = TAP_S3_CSV_KEY_PREFIX
        snowflake_archive_s3_prefix = SNOWFLAKE_ARCHIVE_S3_PREFIX
        snowflake_s3_key_prefix = os.environ.get('TARGET_SNOWFLAKE_S3_KEY_PREFIX')
        if self.e2e_namespace:
            tap_s3_csv_key_prefix = _namespaced_s3_path(
                tap_s3_csv_key_prefix,
                self.e2e_namespace,
            )
            snowflake_archive_s3_prefix = _namespaced_s3_path(
                snowflake_archive_s3_prefix,
                self.e2e_namespace,
            )
            if snowflake_s3_key_prefix:
                snowflake_s3_key_prefix = (
                    f'{_namespaced_s3_path(snowflake_s3_key_prefix, self.e2e_namespace)}/'
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
            # Tap YugabyteDB is a REQUIRED test connector and test database with test data
            # available in the docker environment
            # ------------------------------------------------------------------
            'TAP_YUGABYTE': {
                'template_patterns': ['tap_yugabyte'],
                'vars': {
                    'HOST': {
                        'value': os.environ.get('TAP_YUGABYTE_HOST'),
                        'required': True,
                    },
                    'PORT': {
                        'value': os.environ.get('TAP_YUGABYTE_PORT'),
                        'required': True,
                    },
                    'USER': {
                        'value': os.environ.get('TAP_YUGABYTE_USER'),
                        'required': True,
                    },
                    'PASSWORD': {
                        'value': os.environ.get('TAP_YUGABYTE_PASSWORD'),
                        'required': True,
                    },
                    'DB': {
                        'value': os.environ.get('TAP_YUGABYTE_DB'),
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
                    'DB_2': {'value': os.environ.get('TAP_MYSQL_REPLICA_DB')},
                    'REPLICA_HOST': {'value': os.environ.get('TAP_MYSQL_REPLICA_HOST')},
                    'REPLICA_PORT': {'value': os.environ.get('TAP_MYSQL_REPLICA_PORT')},
                    'REPLICA_USER': {'value': os.environ.get('TAP_MYSQL_REPLICA_USER')},
                    'REPLICA_PASSWORD': {'value': os.environ.get('TAP_MYSQL_REPLICA_PASSWORD')},
                    'REPLICA_DB': {'value': os.environ.get('TAP_MYSQL_REPLICA_DB')},
                },
            },
            # ------------------------------------------------------------------
            # Genuine Oracle MySQL is a REQUIRED source for engine-specific
            # tap-mysql compatibility coverage.
            # ------------------------------------------------------------------
            'TAP_ORACLE_MYSQL': {
                'template_patterns': ['tap_oracle_mysql'],
                'vars': {
                    'HOST': {
                        'value': os.environ.get('TAP_ORACLE_MYSQL_HOST'),
                        'required': True,
                    },
                    'PORT': {
                        'value': os.environ.get('TAP_ORACLE_MYSQL_PORT'),
                        'required': True,
                    },
                    'USER': {
                        'value': os.environ.get('TAP_ORACLE_MYSQL_USER'),
                        'required': True,
                    },
                    'PASSWORD': {
                        'value': os.environ.get('TAP_ORACLE_MYSQL_PASSWORD'),
                        'required': True,
                    },
                    'DB': {
                        'value': os.environ.get('TAP_ORACLE_MYSQL_DB'),
                        'required': True,
                    },
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
                    'KEY_PREFIX': {
                        'value': tap_s3_csv_key_prefix,
                        'optional': True,
                    },
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
            # PipelineWise backend is a REQUIRED operational database for
            # data-diff definitions, run evidence, and coverage state.
            # ------------------------------------------------------------------
            'PIPELINEWISE_BACKEND': {
                'template_patterns': ['config.yml'],
                'vars': {
                    'HOST': {
                        'value': os.environ.get('PIPELINEWISE_BACKEND_HOST'),
                        'required': True,
                    },
                    'PORT': {
                        'value': os.environ.get('PIPELINEWISE_BACKEND_PORT'),
                        'required': True,
                    },
                    'USER': {
                        'value': os.environ.get('PIPELINEWISE_BACKEND_USER'),
                        'required': True,
                    },
                    'PASSWORD': {
                        'value': os.environ.get('PIPELINEWISE_BACKEND_PASSWORD'),
                        'required': True,
                    },
                    'DB': {
                        'value': os.environ.get('PIPELINEWISE_BACKEND_DB'),
                        'required': True,
                    },
                    'DDL_USER': {
                        'value': os.environ.get('PIPELINEWISE_BACKEND_DDL_USER'),
                        'required': True,
                    },
                    'DDL_PASSWORD': {
                        'value': os.environ.get('PIPELINEWISE_BACKEND_DDL_PASSWORD'),
                        'required': True,
                    },
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
                    'PRIVATE_KEY': {'value': os.environ.get('TARGET_SNOWFLAKE_PRIVATE_KEY')},
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
                        'value': snowflake_s3_key_prefix
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
                        'value': self.sf_schema_postfix,
                        'optional': True,
                    },
                    'ARCHIVE_LOAD_FILES_S3_PREFIX': {
                        'value': snowflake_archive_s3_prefix,
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
        self.env['TAP_YUGABYTE']['is_configured'] = self._is_env_connector_configured(
            'TAP_YUGABYTE'
        )
        self.env['TAP_MYSQL']['is_configured'] = self._is_env_connector_configured(
            'TAP_MYSQL'
        )
        self.env['TAP_ORACLE_MYSQL']['is_configured'] = self._is_env_connector_configured(
            'TAP_ORACLE_MYSQL'
        )
        self.env['TAP_S3_CSV']['is_configured'] = self._is_env_connector_configured(
            'TAP_S3_CSV'
        )
        self.env['TAP_MONGODB']['is_configured'] = self._is_env_connector_configured(
            'TAP_MONGODB'
        )
        self.env['TARGET_POSTGRES'][
            'is_configured'
        ] = self._is_env_connector_configured('TARGET_POSTGRES')
        self.env['PIPELINEWISE_BACKEND'][
            'is_configured'
        ] = self._is_env_connector_configured('PIPELINEWISE_BACKEND')
        self.env['TARGET_SNOWFLAKE'][
            'is_configured'
        ] = self._is_env_connector_configured('TARGET_SNOWFLAKE')

    def get_conn_env_var(self, connector, key):
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
                    except FileNotFoundError:
                        pass

    @staticmethod
    def _run_command(args):
        """Run a command in a subprocess"""
        subprocess.run(args, check=True)

    # -------------------------------------------------------------------------
    # Database functions to run queries in source and target databases
    # -------------------------------------------------------------------------

    def run_query_tap_postgres(self, query, params=None):
        """Run and SQL query in tap postgres database"""
        return db.run_query_postgres(
            query,
            host=self.get_conn_env_var('TAP_POSTGRES', 'HOST'),
            port=self.get_conn_env_var('TAP_POSTGRES', 'PORT'),
            user=self.get_conn_env_var('TAP_POSTGRES', 'USER'),
            password=self.get_conn_env_var('TAP_POSTGRES', 'PASSWORD'),
            database=self.get_conn_env_var('TAP_POSTGRES', 'DB'),
            params=params,
        )

    def run_query_tap_yugabyte(self, query, params=None):
        """Run and SQL query in tap yugabyte database"""
        return db.run_query_postgres(
            query,
            host=self.get_conn_env_var('TAP_YUGABYTE', 'HOST'),
            port=self.get_conn_env_var('TAP_YUGABYTE', 'PORT'),
            user=self.get_conn_env_var('TAP_YUGABYTE', 'USER'),
            password=self.get_conn_env_var('TAP_YUGABYTE', 'PASSWORD'),
            database=self.get_conn_env_var('TAP_YUGABYTE', 'DB'),
            params=params,
        )

    def get_tap_mongodb_connection(self):
        """Create and returns tap mongodb database instance to run queries on"""
        return db.get_mongodb_connection(
            host=self.get_conn_env_var('TAP_MONGODB', 'HOST'),
            port=self.get_conn_env_var('TAP_MONGODB', 'PORT'),
            user=self.get_conn_env_var('TAP_MONGODB', 'USER'),
            password=self.get_conn_env_var('TAP_MONGODB', 'PASSWORD'),
            database=self.get_conn_env_var('TAP_MONGODB', 'DB'),
            auth_database=self.get_conn_env_var('TAP_MONGODB', 'AUTH_DB'),
        )

    def run_query_target_postgres(self, query: object) -> object:
        """Run and SQL query in target postgres database"""
        return db.run_query_postgres(
            query,
            host=self.get_conn_env_var('TARGET_POSTGRES', 'HOST'),
            port=self.get_conn_env_var('TARGET_POSTGRES', 'PORT'),
            user=self.get_conn_env_var('TARGET_POSTGRES', 'USER'),
            password=self.get_conn_env_var('TARGET_POSTGRES', 'PASSWORD'),
            database=self.get_conn_env_var('TARGET_POSTGRES', 'DB'),
        )

    def run_query_pipelinewise_backend(self, query: object) -> object:
        """Run a SQL query in the backend database as the application user.

        Deliberately the application user, not the DDL role: assertions then prove
        the migration granted it the access it needs.
        """
        return db.run_query_postgres(
            query,
            host=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'HOST'),
            port=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'PORT'),
            user=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'USER'),
            password=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'PASSWORD'),
            database=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'DB'),
        )

    def run_ddl_pipelinewise_backend(self, query: object) -> object:
        """Run DDL in the backend database as the schema-owning role."""
        return db.run_query_postgres(
            query,
            host=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'HOST'),
            port=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'PORT'),
            user=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'DDL_USER'),
            password=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'DDL_PASSWORD'),
            database=self.get_conn_env_var('PIPELINEWISE_BACKEND', 'DB'),
        )

    # pylint: disable=unnecessary-pass
    def run_query_tap_s3_csv(self, file):
        """Get file from S3 and read into the file
        This function is not yet implemented"""
        pass

    def run_query_tap_mysql(self, query, params=None):
        """Run and SQL query in tap mysql database"""
        return db.run_query_mysql(
            query,
            host=self.get_conn_env_var('TAP_MYSQL', 'HOST'),
            port=int(self.get_conn_env_var('TAP_MYSQL', 'PORT')),
            user=self.get_conn_env_var('TAP_MYSQL', 'USER'),
            password=self.get_conn_env_var('TAP_MYSQL', 'PASSWORD'),
            database=self.get_conn_env_var('TAP_MYSQL', 'DB'),
            params=params,
        )

    def run_query_tap_mysql_2(self, query):
        """Run and SQL query in tap mysql database"""
        return db.run_query_mysql(
            query,
            host=self.get_conn_env_var('TAP_MYSQL', 'HOST'),
            port=int(self.get_conn_env_var('TAP_MYSQL', 'PORT')),
            user=self.get_conn_env_var('TAP_MYSQL', 'USER'),
            password=self.get_conn_env_var('TAP_MYSQL', 'PASSWORD'),
            database=self.get_conn_env_var('TAP_MYSQL', 'DB_2'),
        )

    def run_query_tap_oracle_mysql(self, query, params=None):
        """Run a query against the genuine Oracle MySQL source."""
        return db.run_query_mysql(
            query,
            host=self.get_conn_env_var('TAP_ORACLE_MYSQL', 'HOST'),
            port=int(self.get_conn_env_var('TAP_ORACLE_MYSQL', 'PORT')),
            user=self.get_conn_env_var('TAP_ORACLE_MYSQL', 'USER'),
            password=self.get_conn_env_var('TAP_ORACLE_MYSQL', 'PASSWORD'),
            database=self.get_conn_env_var('TAP_ORACLE_MYSQL', 'DB'),
            params=params,
        )

    def run_query_target_snowflake(self, query):
        """Run and SQL query in target snowflake database"""
        return db.run_query_snowflake(
            query,
            account=self.get_conn_env_var('TARGET_SNOWFLAKE', 'ACCOUNT'),
            database=self.get_conn_env_var('TARGET_SNOWFLAKE', 'DBNAME'),
            warehouse=self.get_conn_env_var('TARGET_SNOWFLAKE', 'WAREHOUSE'),
            user=self.get_conn_env_var('TARGET_SNOWFLAKE', 'USER'),
            private_key=self.get_conn_env_var('TARGET_SNOWFLAKE', 'PRIVATE_KEY'),
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

    def setup_tap_oracle_mysql(self):
        """Reset the genuine Oracle MySQL source before discovery."""
        db_script = os.path.join(DIR, '..', '..', 'db', 'tap_oracle_mysql_db.sh')
        self._run_command(db_script)

    # pylint: disable=unnecessary-pass
    def setup_tap_postgres(self):
        """Clean postgres source database and prepare for test run
        Creating initial tables is defined in Docker entrypoint.sh"""
        db_script = os.path.join(DIR, '..', '..', 'db', 'tap_postgres_db.sh')
        self._run_command(db_script)

    def setup_tap_yugabyte(self):
        """Clean YugabyteDB source database and prepare for test run
        Creating initial tables is defined in Docker entrypoint.sh"""
        db_script = os.path.join(DIR, '..', '..', 'db', 'tap_yugabyte_db.sh')
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

        bucket = self.get_conn_env_var('TAP_S3_CSV', 'BUCKET')
        key_prefix = self.get_conn_env_var('TAP_S3_CSV', 'KEY_PREFIX')
        s3 = boto3.client(
            's3',
            aws_access_key_id=self.get_conn_env_var('TAP_S3_CSV', 'AWS_KEY'),
            aws_secret_access_key=self.get_conn_env_var(
                'TAP_S3_CSV', 'AWS_SECRET_ACCESS_KEY'
            ),
        )

        s3.upload_file(mock_data_1, bucket, f'{key_prefix}/mock_data_1.csv')
        s3.upload_file(mock_data_2, bucket, f'{key_prefix}/mock_data_2.csv')

    def cleanup_tap_s3_csv(self):
        """Delete this CI run's exact tap S3 CSV fixture objects."""
        if not self.e2e_namespace:
            return

        bucket = self.get_conn_env_var('TAP_S3_CSV', 'BUCKET')
        key_prefix = self.get_conn_env_var('TAP_S3_CSV', 'KEY_PREFIX')
        s3 = boto3.client(
            's3',
            aws_access_key_id=self.get_conn_env_var('TAP_S3_CSV', 'AWS_KEY'),
            aws_secret_access_key=self.get_conn_env_var(
                'TAP_S3_CSV', 'AWS_SECRET_ACCESS_KEY'
            ),
        )
        s3.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': [
                    {'Key': f'{key_prefix}/mock_data_1.csv'},
                    {'Key': f'{key_prefix}/mock_data_2.csv'},
                ],
                'Quiet': True,
            },
        )

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
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_yugabyte CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_yugabyte_public2 CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_yugabyte_logical1 CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_mysql CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_mysql_2 CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_s3_csv CASCADE'
        )
        self.run_query_target_postgres(
            'DROP SCHEMA IF EXISTS ppw_e2e_tap_mongodb CASCADE'
        )

        self.remove_dir_from_config_dir('postgres_dwh')

    def setup_pipelinewise_backend(self):
        """Remove data-diff control-plane state without touching target data."""
        # alembic_version is dropped, not emptied: on a fresh backend volume it
        # does not exist yet, and migrate() recreates it either way. Runs as the
        # DDL role because the application user has no DROP rights.
        self.run_ddl_pipelinewise_backend(
            'DROP TABLE IF EXISTS public.dd_watermark_events CASCADE; '
            'DROP TABLE IF EXISTS public.dd_watermark_state CASCADE; '
            'DROP TABLE IF EXISTS public.dd_run_slot_state CASCADE; '
            'DROP TABLE IF EXISTS public.dd_run_results CASCADE; '
            'DROP TABLE IF EXISTS public.dd_run_attempts CASCADE; '
            'DROP TABLE IF EXISTS public.dd_preflight_log CASCADE; '
            'DROP TABLE IF EXISTS public.dd_check_definitions CASCADE; '
            'DROP TABLE IF EXISTS public.alembic_version CASCADE'
        )

    def setup_target_snowflake(self):
        """Clean snowflake target database and prepare for test run"""

        if self.env['TARGET_SNOWFLAKE']['is_configured']:
            self.run_query_target_snowflake(
                f'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres{self.sf_schema_postfix} CASCADE'
            )
            self.run_query_target_snowflake(
                f'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_public2{self.sf_schema_postfix} CASCADE'
            )
            self.run_query_target_snowflake(
                f'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_logical1{self.sf_schema_postfix} CASCADE'
            )
            self.run_query_target_snowflake(
                f'DROP SCHEMA IF EXISTS ppw_e2e_tap_postgres_logical2{self.sf_schema_postfix} CASCADE'
            )
            self.run_query_target_snowflake(
                f'DROP SCHEMA IF EXISTS ppw_e2e_tap_mysql{self.sf_schema_postfix} CASCADE'
            )
            self.run_query_target_snowflake(
                f'DROP SCHEMA IF EXISTS ppw_e2e_tap_mysql_2{self.sf_schema_postfix} CASCADE'
            )
            self.run_query_target_snowflake(
                f'DROP SCHEMA IF EXISTS ppw_e2e_tap_s3_csv{self.sf_schema_postfix} CASCADE'
            )
            self.run_query_target_snowflake(
                f'DROP SCHEMA IF EXISTS ppw_e2e_tap_mongodb{self.sf_schema_postfix} CASCADE'
            )

        self.remove_dir_from_config_dir('snowflake')

    @staticmethod
    def remove_dir_from_config_dir(dir_path):
        """Remove generated config while surfacing failures other than absence."""
        try:
            shutil.rmtree(os.path.join(CONFIG_DIR, dir_path))
        except FileNotFoundError:
            pass

    def delete_record_from_target_snowflake(self, tap_type, table, where_clause):
        """Delete all records except the first one from the snowflake target"""
        source_type = E2EEnv._normalize_tap_type(tap_type)
        self.run_query_target_snowflake(
            f'DELETE from ppw_e2e_tap_{source_type}{self.sf_schema_postfix}.{table} {where_clause}'
        )

    def add_column_into_target_sf(self, tap_type, table, new_column):
        """Add a record into the target"""
        source_type = E2EEnv._normalize_tap_type(tap_type)
        self.run_query_target_snowflake(
            f'ALTER TABLE ppw_e2e_tap_{source_type}{self.sf_schema_postfix}.{table} ADD {new_column["name"]} int'
        )
        self.run_query_target_snowflake(
            f'UPDATE ppw_e2e_tap_{source_type}{self.sf_schema_postfix}.{table}'
            f' SET {new_column["name"]}={new_column["value"]} WHERE 1=1'
        )

    def add_column_into_source(self, tap_type, table, new_column):
        """Add a column into the source table"""
        run_query_method = E2EEnv._get_source_query_method(self, tap_type)
        run_query_method(
            f'ALTER TABLE {table} ADD {new_column["name"]} int'
        )
        run_query_method(
            f'UPDATE {table} set {new_column["name"]}={new_column["value"]} where 1=1'
        )

    def delete_record_from_source(self, tap_type, table, where_clause):
        """Delete a record from the source"""
        run_query_method = E2EEnv._get_source_query_method(self, tap_type)
        run_query_method(
            f'DELETE FROM {table} {where_clause}'
        )

    def _get_source_query_method(self, tap_type):
        """Resolve lowercase and canonical TAP_* source route identifiers."""
        source_type = E2EEnv._normalize_tap_type(tap_type)
        return getattr(self, f'run_query_tap_{source_type}')

    @staticmethod
    def _normalize_tap_type(tap_type):
        """Return the source suffix used in E2E method and schema names."""
        source_type = str(tap_type).lower()
        return source_type[4:] if source_type.startswith('tap_') else source_type

    def get_source_records_count(self, tap_type, table):
        """Getting count of records from the source"""
        run_query_method = E2EEnv._get_source_query_method(self, tap_type)
        result = run_query_method(f'SELECT count(1) FROM {table}')
        return result[0][0]

    def get_rows_from_source(
            self, tap_type, source_db, table, columns, primary_key,
            where_clause=None):
        """Get representative ordered rows from a source fixture table."""
        run_query_method = E2EEnv._get_source_query_method(self, tap_type)
        filter_sql = f' {where_clause.strip()}' if where_clause else ''
        return run_query_method(
            f'SELECT {", ".join(columns)} FROM {source_db}.{table}'
            f'{filter_sql} ORDER BY {primary_key}'
        )

    def get_rows_from_target_snowflake(
            self, tap_type, table, columns, primary_key, where_clause=None):
        """Get representative ordered rows from a Snowflake target table."""
        source_type = E2EEnv._normalize_tap_type(tap_type)
        filter_sql = f' {where_clause.strip()}' if where_clause else ''
        return self.run_query_target_snowflake(
            f'SELECT {", ".join(columns)} '
            f'FROM ppw_e2e_tap_{source_type}{self.sf_schema_postfix}.{table}'
            f'{filter_sql} ORDER BY "{primary_key.upper()}"'
        )

    def get_records_from_target_snowflake(self, tap_type, table, column, primary_key):
        """"Getting all records from a specific table of snowflake target"""
        return self.get_rows_from_target_snowflake(
            tap_type=tap_type,
            table=table,
            columns=[column],
            primary_key=primary_key,
        )

    @staticmethod
    def remove_all_state_files():
        """Clean up state files to ensure tests behave the same every time"""
        for state_file in Path(CONFIG_DIR).glob('**/state.json'):
            state_file.unlink()

    @staticmethod
    def clean_up_temp_dir():
        """Remove temporary files and directories between E2E runs."""
        entries = glob.glob(f'{CONFIG_DIR}/tmp/*')
        for entry in entries:
            try:
                if os.path.islink(entry) or not os.path.isdir(entry):
                    os.remove(entry)
                else:
                    shutil.rmtree(entry)
            except FileNotFoundError:
                pass
