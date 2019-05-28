#!/usr/bin/env python3

import backoff
import singer
import snowflake.connector

LOGGER = singer.get_logger()


def retry_pattern():
    return backoff.on_exception(backoff.expo,
                      (snowflake.connector.errors.OperationalError),
                      max_tries=5,
                      on_backoff=log_backoff_attempt,
                      factor=2)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Snowflake, triggering backoff: %d try", details.get("tries"))


def validate_config(config):
    errors = []
    required_config_keys = [
        'account',
        'dbname',
        'user',
        'password',
        'warehouse'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    return errors


class SnowflakeConnection:
    def __init__(self, connection_config):
        """
        connection_config:      Snowflake connection details
        """
        self.connection_config = connection_config
        config_errors = validate_config(connection_config)
        if len(config_errors) == 0:
            self.connection_config = connection_config
        else:
            LOGGER.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
            exit(1)


    def open_connection(self):
        return snowflake.connector.connect(
            user=self.connection_config['user'],
            password=self.connection_config['password'],
            account=self.connection_config['account'],
            database=self.connection_config['dbname'],
            warehouse=self.connection_config['warehouse'],
            insecure_mode=self.connection_config.get('insecure_mode', False)
            # Use insecure mode to avoid "Failed to get OCSP response" warnings
            #insecure_mode=True
        )


    @retry_pattern()
    def connect_with_backoff(self):
        return self.open_connection()


    def query(self, query, params=None):
        LOGGER.info("SNOWFLAKE - Running query: {}".format(query))
        with self.connect_with_backoff() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(
                    query,
                    params
                )

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []

