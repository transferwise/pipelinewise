#!/usr/bin/env python3
from typing import Union, List, Dict

import backoff
import singer
import sys
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector

LOGGER = singer.get_logger('tap_snowflake')


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records"""


def retry_pattern():
    """Retry pattern decorator used when connecting to snowflake
    """
    return backoff.on_exception(backoff.expo,
                                snowflake.connector.errors.OperationalError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=2)


def log_backoff_attempt(details):
    """Log backoff attempts used by retry_pattern
    """
    LOGGER.info('Error detected communicating with Snowflake, triggering backoff: %d try', details.get('tries'))


def validate_config(config):
    """Validate configuration dictionary"""
    errors = []
    required_config_keys = [
        'account',
        'dbname',
        'user',
        'warehouse',
        'tables'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append(f'Required key is missing from config: [{k}]')

    possible_authentication_keys =  [
      'password',
      'private_key_path'
    ]
    if not any(config.get(k, None) for k in possible_authentication_keys):
        errors.append(
            f'Required authentication key missing. Existing methods: {",".join(possible_authentication_keys)}')

    return errors


class SnowflakeConnection:
    """Class to manage connection to snowflake data warehouse"""

    def __init__(self, connection_config):
        """
        connection_config:      Snowflake connection details
        """
        self.connection_config = connection_config
        config_errors = validate_config(connection_config)
        if len(config_errors) == 0:
            self.connection_config = connection_config
        else:
            LOGGER.error('Invalid configuration:\n   * %s', '\n   * '.join(config_errors))
            sys.exit(1)

    def get_private_key(self):
        """
        Get private key from the right location
        """
        if self.connection_config.get('private_key_path'):
            try:
                encoded_passphrase = self.connection_config['private_key_passphrase'].encode()
            except KeyError:
                encoded_passphrase = None

            with open(self.connection_config['private_key_path'], 'rb') as key:
                p_key= serialization.load_pem_private_key(
                        key.read(),
                        password=encoded_passphrase,
                        backend=default_backend()
                    )

            pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption())
            return pkb

        return None

    def open_connection(self):
        """Connect to snowflake database"""
        return snowflake.connector.connect(
            user=self.connection_config['user'],
            password=self.connection_config.get('password', None),
            private_key=self.get_private_key(),
            account=self.connection_config['account'],
            database=self.connection_config['dbname'],
            warehouse=self.connection_config['warehouse'],
            role=self.connection_config.get('role', None),
            insecure_mode=self.connection_config.get('insecure_mode', False)
            # Use insecure mode to avoid "Failed to get OCSP response" warnings
            # insecure_mode=True
        )

    @retry_pattern()
    def connect_with_backoff(self):
        """Connect to snowflake database and retry automatically a few times if fails"""
        return self.open_connection()

    def query(self, query: Union[List[str], str], params: Dict = None, max_records=0):
        """Run a query in snowflake"""
        result = []

        if params is None:
            params = {}
        else:
            if 'LAST_QID' in params:
                LOGGER.warning('LAST_QID is a reserved prepared statement parameter name, '
                               'it will be overridden with each executed query!')

        with self.connect_with_backoff() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:

                # Run every query in one transaction if query is a list of SQL
                if isinstance(query, list):
                    cur.execute('START TRANSACTION')
                    queries = query
                else:
                    queries = [query]

                qid = None

                for sql in queries:
                    LOGGER.debug('Running query: %s', sql)

                    # update the LAST_QID
                    params['LAST_QID'] = qid

                    cur.execute(sql, params)
                    qid = cur.sfqid

                    # Raise exception if returned rows greater than max allowed records
                    if 0 < max_records < cur.rowcount:
                        raise TooManyRecordsException(
                            f'Query returned too many records. This query can return max {max_records} records')

                    if cur.rowcount > 0:
                        result = cur.fetchall()

        return result
