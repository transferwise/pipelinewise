import os


def get_config():
    # --------------------------------------------------------------------------
    # Default configuration settings for integration tests.
    # --------------------------------------------------------------------------
    # The following values needs to be defined in environment variables with
    # valid details to an S3 bucket
    # --------------------------------------------------------------------------
    # S3 bucket
    config = {
        'aws_endpoint_url': os.environ.get('TAP_S3_CSV_ENDPOINT'),
        'aws_access_key_id': os.environ.get('TAP_S3_CSV_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('TAP_S3_CSV_SECRET_ACCESS_KEY'),
        'bucket': os.environ.get('TAP_S3_CSV_BUCKET'),
        'start_date': '2000-01-01',
        'tables': None
    }

    return config


def get_test_config():
    return get_config()
