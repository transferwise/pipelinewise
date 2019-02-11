import os
import json


def get_db_config():
    config = {}
    config['account'] = os.environ.get('TARGET_SNOWFLAKE_ACCOUNT')
    config['aws_access_key_id'] = os.environ.get('TARGET_SNOWFLAKE_AWS_ACCESS_KEY')
    config['aws_secret_access_key'] = os.environ.get('TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY')
    config['dbname'] = os.environ.get('TARGET_SNOWFLAKE_DBNAME')
    config['password'] = os.environ.get('TARGET_SNOWFLAKE_PASSWORD')
    config['s3_bucket'] = os.environ.get('TARGET_SNOWFLAKE_S3_BUCKET')
    config['s3_key_prefix'] = os.environ.get('TARGET_SNOWFLAKE_S3_KEY_PREFIX')
    config['user'] = os.environ.get('TARGET_SNOWFLAKE_USER')
    config['warehouse'] = os.environ.get('TARGET_SNOWFLAKE_WAREHOUSE')
    config['schema'] = os.environ.get("TARGET_SNOWFLAKE_SCHEMA")
    config['dynamic_schema_name'] = os.environ.get('TARGET_SNOWFLAKE_DYNAMIC_SCHEMA_NAME')
    config['dynamic_schema_name_postfix'] = os.environ.get('TARGET_SNOWFLAKE_DYNAMIC_SCHEMA_NAME_PREFIX')
    config['disable_table_cache'] = os.environ.get('DISABLE_TABLE_CACHE')

    return config


def get_test_config():
    db_config = get_db_config()

    return db_config


def get_test_tap_lines(filename):
    lines = []
    with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as tap_stdout:
        for line in tap_stdout.readlines():
            lines.append(line)

    return lines

