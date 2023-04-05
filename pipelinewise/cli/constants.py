import enum


class ConnectorType(enum.Enum):
    """
    Enums for various Singer connector type names
    """

    TAP_GITHUB = 'tap-github'
    TAP_GOOGLE_ANALYTICS = 'tap-google-analytics'
    TAP_JIRA = 'tap-jira'
    TAP_KAFKA = 'tap-kafka'
    TAP_MIXPANEL = 'tap-mixpanel'
    TAP_MONGODB = 'tap-mongodb'
    TAP_MYSQL = 'tap-mysql'
    TAP_ORACLE = 'tap-oracle'
    TAP_POSTGRES = 'tap-postgres'
    TAP_S3_CSV = 'tap-s3-csv'
    TAP_SALESFORCE = 'tap-salesforce'
    TAP_SHOPIFY = 'tap-shopify'
    TAP_SLACK = 'tap-slack'
    TAP_SNOWFLAKE = 'tap-snowflake'
    TAP_TWILIO = 'tap-twilio'
    TAP_ZENDESK = 'tap-zendesk'
    TAP_ZUORA = 'tap-zuora'

    TARGET_BIGQUERY = 'target-bigquery'
    TARGET_POSTGRES = 'target-postgres'
    TARGET_SNOWFLAKE = 'target-snowflake'
    TARGET_REDSHIFT = 'target-redshift'
    TARGET_S3_CSV = 'target-s3-csv'

    TRANSFORM_FIELD = 'transform-field'
