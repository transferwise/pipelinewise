from unittest import TestCase
from pipelinewise.fastsync.commons.type_mapping import (
    MYSQL_TO_POSTGRES_MAPPER, MYSQL_TO_SNOWFLAKE_MAPPER,
    MYSQL_TO_REDSHIFT_MAPPER, MYSQL_TO_BIGQUERY_MAPPER
)


class TestTypeMapping(TestCase):
    """
    Unit test for Type Mapping in FastSync components
    """
    def test_mysql_type_to_pg_mapper(self):
        """
        tests that mysql types are mapped correctly to postgres types
        """
        self.assertEqual('CHARACTER VARYING', MYSQL_TO_POSTGRES_MAPPER('binary', None))

        self.assertEqual('JSONB', MYSQL_TO_POSTGRES_MAPPER('geometry', None))

        self.assertEqual('BOOLEAN', MYSQL_TO_POSTGRES_MAPPER('bit', None))
        self.assertEqual('BOOLEAN', MYSQL_TO_POSTGRES_MAPPER('tinyint', 'tinyint(1)'))

        self.assertEqual('JSONB', MYSQL_TO_POSTGRES_MAPPER('json', None))

        self.assertEqual('INTEGER NULL', MYSQL_TO_POSTGRES_MAPPER('int', None))

        self.assertEqual('DOUBLE PRECISION', MYSQL_TO_POSTGRES_MAPPER('double', None))

        self.assertEqual('TIMESTAMP WITHOUT TIME ZONE', MYSQL_TO_POSTGRES_MAPPER('timestamp', None))

        self.assertEqual('TIME WITHOUT TIME ZONE', MYSQL_TO_POSTGRES_MAPPER('time', None))

        self.assertEqual('CHARACTER VARYING', MYSQL_TO_POSTGRES_MAPPER('longtext', None))

    def test_mysql_to_pg_with_undefined_type_returns_none(self):
        """test that an unsupported mysql datatype returns None"""
        self.assertIsNone(MYSQL_TO_POSTGRES_MAPPER('random-type', 'random-type'))

    def test_mysql_type_to_sf_mapper(self):
        """
        tests that mysql types are mapped correctly to snowflake types
        """
        self.assertEqual('BINARY', MYSQL_TO_SNOWFLAKE_MAPPER('binary', None))

        self.assertEqual('VARIANT', MYSQL_TO_SNOWFLAKE_MAPPER('geometry', None))

        self.assertEqual('BOOLEAN', MYSQL_TO_SNOWFLAKE_MAPPER('bit', None))
        self.assertEqual('BOOLEAN', MYSQL_TO_SNOWFLAKE_MAPPER('tinyint', 'tinyint(1)'))

        self.assertEqual('VARIANT', MYSQL_TO_SNOWFLAKE_MAPPER('json', None))

        self.assertEqual('NUMBER', MYSQL_TO_SNOWFLAKE_MAPPER('int', None))

        self.assertEqual('FLOAT', MYSQL_TO_SNOWFLAKE_MAPPER('double', None))

        self.assertEqual('TIMESTAMP_NTZ', MYSQL_TO_SNOWFLAKE_MAPPER('timestamp', None))

        self.assertEqual('TIME', MYSQL_TO_SNOWFLAKE_MAPPER('time', None))

        self.assertEqual('VARCHAR', MYSQL_TO_SNOWFLAKE_MAPPER('longtext', None))

    def test_mysql_to_sf_with_undefined_type_returns_none(self):
        """test that an unsupported mysql datatype returns None"""
        self.assertIsNone(MYSQL_TO_SNOWFLAKE_MAPPER('random-type', 'random-type'))

    def test_mysql_to_redshift_with_defined_type_returns_target_type(self):
        """
        tests that mysql types are mapped correctly to redshift types
        """
        self.assertEqual('CHARACTER VARYING(65535)', MYSQL_TO_REDSHIFT_MAPPER('binary', None))
        self.assertEqual('CHARACTER VARYING(10000)', MYSQL_TO_REDSHIFT_MAPPER('geometry', None))
        self.assertEqual('CHARACTER VARYING(10000)', MYSQL_TO_REDSHIFT_MAPPER('point', None))
        self.assertEqual('CHARACTER VARYING(10000)', MYSQL_TO_REDSHIFT_MAPPER('linestring', None))
        self.assertEqual('CHARACTER VARYING(10000)', MYSQL_TO_REDSHIFT_MAPPER('polygon', None))
        self.assertEqual('CHARACTER VARYING(10000)', MYSQL_TO_REDSHIFT_MAPPER('multipoint', None))
        self.assertEqual('CHARACTER VARYING(10000)', MYSQL_TO_REDSHIFT_MAPPER('multilinestring', None))
        self.assertEqual('CHARACTER VARYING(10000)', MYSQL_TO_REDSHIFT_MAPPER('multipolygon', None))
        self.assertEqual('CHARACTER VARYING(10000)', MYSQL_TO_REDSHIFT_MAPPER('geometrycollection', None))

    def test_mysql_to_redshift_with_undefined_type_returns_none(self):
        """test that an unsupported mysql datatype returns None"""
        self.assertIsNone(MYSQL_TO_REDSHIFT_MAPPER('random-type', 'random-type'))

    def test_mysql_to_bq_with_defined_type_returns_target_type(self):
        """test that supported mysql datatypes returns equivalent in Bigquery"""
        self.assertEqual('STRING', MYSQL_TO_BIGQUERY_MAPPER('binary', None))
        self.assertEqual('STRING', MYSQL_TO_BIGQUERY_MAPPER('geometry', None))
        self.assertEqual('STRING', MYSQL_TO_BIGQUERY_MAPPER('point', None))
        self.assertEqual('STRING', MYSQL_TO_BIGQUERY_MAPPER('linestring', None))
        self.assertEqual('STRING', MYSQL_TO_BIGQUERY_MAPPER('polygon', None))
        self.assertEqual('STRING', MYSQL_TO_BIGQUERY_MAPPER('multipoint', None))
        self.assertEqual('STRING', MYSQL_TO_BIGQUERY_MAPPER('multilinestring', None))
        self.assertEqual('STRING', MYSQL_TO_BIGQUERY_MAPPER('multipolygon', None))
        self.assertEqual('STRING', MYSQL_TO_BIGQUERY_MAPPER('geometrycollection', None))

    def test_mysql_to_bq_with_undefined_type_returns_none(self):
        """test that an unsupported mysql datatype returns None"""
        self.assertIsNone(MYSQL_TO_BIGQUERY_MAPPER('random-type', 'random-type'))
