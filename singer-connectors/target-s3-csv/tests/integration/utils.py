import os


def get_config():
    config = {}

    # --------------------------------------------------------------------------
    # Default configuration settings for integration tests.
    # --------------------------------------------------------------------------
    # The following values needs to be defined in environment variables with
    # valid details to an S3 bucket
    # --------------------------------------------------------------------------
    # S3 bucket
    config['aws_access_key_id'] = os.environ.get('TARGET_S3_CSV_ACCESS_KEY_ID')
    config['aws_secret_access_key'] = os.environ.get('TARGET_S3_CSV_SECRET_ACCESS_KEY')
    config['s3_bucket'] = os.environ.get('TARGET_S3_CSV_BUCKET')
    config['s3_key_prefix'] = os.environ.get('TARGET_S3_CSV_KEY_PREFIX')

    # --------------------------------------------------------------------------
    # The following variables needs to be empty.
    # The tests cases will set them automatically whenever it's needed
    # --------------------------------------------------------------------------
    config['add_metadata_columns'] = None
    config['compression'] = None

    return config


def get_test_config():
    return get_config()


def get_test_tap_lines(filename):
    lines = []
    with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as tap_stdout:
        for line in tap_stdout.readlines():
            lines.append(line)

    return lines