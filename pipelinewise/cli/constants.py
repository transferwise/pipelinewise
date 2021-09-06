import enum


class ConnectorType(enum.Enum):
    """
    Enums for various Singer connector type names
    Todo: add more
    """

    TAP_MYSQL = 'tap-mysql'
    TAP_POSTGRES = 'tap-postgres'
    TAP_MONGODB = 'tap-mongodb'
    TAP_S3_CSV = 'tap-s3-csv'

    TARGET_BIGQUERY = 'target-bigquery'
    TARGET_POSTGRES = 'target-postgres'
    TARGET_SNOWFLAKE = 'target-snowflake'
    TARGET_REDSHIFT = 'target-redshift'
