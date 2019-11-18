import os
import pytest
from unittest import TestCase
from pipelinewise.fastsync.commons import utils

RESOURCES_DIR = "{}/resources".format(os.path.dirname(__file__))


class MySqlMock:
    def fetch_current_log_pos(self):
        return {
            "log_file": "mysqld-bin.000001",
            "log_pos": "123456",
            "version": 1
        }

    def fetch_current_incremental_key_pos(self, table, replication_key):
        return {
            "replication_key": replication_key,
            "replication_key_value": 123456,
            "version": 1
        }


class PostgresMock:
    def fetch_current_log_pos(self):
        return {
            "lsn": "16/B374D848",
            "version": 1
        }

    def fetch_current_incremental_key_pos(self, table, replication_key):
        return {
            "replication_key": replication_key,
            "replication_key_value": 123456,
            "version": 1
        }


class S3CsvMock:
    def fetch_current_incremental_key_pos(self, table, replication_key):
        return {
            "modified_since": '2019-11-15T07:39:44.171098'
        }

class TestFastSyncUtils(TestCase):
    def test_tablename_to_dict(self):
        """Test identifying schema and table names from fully qualified table names"""

        # Format: <CATALOG>.<SCHEMA>.<TABLE>
        assert utils.tablename_to_dict('my_catalog.my_schema.my_table') == \
               {
                   "catalog_name": "my_catalog",
                   "schema_name": "my_schema",
                   "table_name": "my_table",
                   "temp_table_name": "my_table_temp"
               }

        # Format: <SCHEMA>.<TABLE>
        assert utils.tablename_to_dict('my_schema.my_table') == \
               {
                   "catalog_name": None,
                   "schema_name": "my_schema",
                   "table_name": "my_table",
                   "temp_table_name": "my_table_temp"
               }

        # Format: <TABLE>
        assert utils.tablename_to_dict('my_table') == \
               {
                   "catalog_name": None,
                   "schema_name": None,
                   "table_name": "my_table",
                   "temp_table_name": "my_table_temp"
               }

        # Format: <CATALOG>.<SCHEMA>.<TABLE>.<SOMETHING>
        assert utils.tablename_to_dict('my_catalog.my_schema.my_table.foo') == \
               {
                   "catalog_name": "my_catalog",
                   "schema_name": "my_schema",
                   "table_name": "my_table_foo",
                   "temp_table_name": "my_table_foo_temp"
               }

        # Format: <CATALOG>.<SCHEMA>.<TABLE>.<SOMETHING>
        # Custom separator
        assert utils.tablename_to_dict('my_catalog-my_schema-my_table-foo', separator='-') == \
               {
                   "catalog_name": "my_catalog",
                   "schema_name": "my_schema",
                   "table_name": "my_table_foo",
                   "temp_table_name": "my_table_foo_temp"
               }

    def test_get_tables_from_properties(self):
        """Test getting selected tables from tap properties JSON"""
        # Load MySQL and Postgres properties JSON
        mysql_properties = utils.load_json("{}/properties_mysql.json".format(RESOURCES_DIR))
        postgres_properties = utils.load_json("{}/properties_postgres.json".format(RESOURCES_DIR))

        # Get list of selected tables
        # MySQL and Postgres schemas defined at different keys. get_tables_from_properties function
        # should detect and extract correctly
        mysql_tables = utils.get_tables_from_properties(mysql_properties)
        postgres_tables = utils.get_tables_from_properties(postgres_properties)

        # MySQL schema
        assert mysql_tables == \
               [
                   "mysql_source_db.address",
                   "mysql_source_db.order",
                   "mysql_source_db.weight_unit"
               ]

        assert postgres_tables == \
               [
                   "public.city",
                   "public.country"
               ]

    def test_get_tables_from_properties_for_s3_csv(self):
        properties = utils.load_json("{}/properties_s3_csv.json".format(RESOURCES_DIR))

        s3_csv_tables = utils.get_tables_from_properties(properties)

        # MySQL schema
        assert s3_csv_tables == \
               [
                   "applications",
                   "candidate_survey_questions",
                   "interviews",
               ]

    def test_get_bookmark_for_table_mysql(self):
        """Test bookmark extractors for MySQL taps"""
        # Load MySQL and Postgres properties JSON
        mysql_properties = utils.load_json("{}/properties_mysql.json".format(RESOURCES_DIR))

        # MySQL: mysql_source_db.order is LOG_BASED
        assert utils.get_bookmark_for_table("mysql_source_db.order", mysql_properties, MySqlMock()) == {
            "log_file": "mysqld-bin.000001",
            "log_pos": "123456",
            "version": 1
        }

        # MySQL: mysql_source_db.address is INCREMENTAL
        assert utils.get_bookmark_for_table("mysql_source_db.address", mysql_properties, MySqlMock()) == {
            "replication_key": "date_updated",
            "replication_key_value": 123456,
            "version": 1
        }

        # MySQL mysql_source_db.foo not exists
        assert utils.get_bookmark_for_table("mysql_source_db.foo", mysql_properties, MySqlMock()) == {}

    def test_get_bookmark_for_table_postgresl(self):
        """Test bookmark extractors for Postgres taps"""
        # Load Postgres properties JSON
        postgres_properties = utils.load_json("{}/properties_postgres.json".format(RESOURCES_DIR))

        # Postgres: public.countrylanguage is LOG_BASED
        assert utils.get_bookmark_for_table("public.countrylanguage", postgres_properties, PostgresMock()) == {
            "lsn": "16/B374D848",
            "version": 1
        }

        # Postgres: postgres_source_db.public.city is INCREMENTAL
        assert utils.get_bookmark_for_table("public.city", postgres_properties, PostgresMock(),
                                            dbname="postgres_source_db") == {
                   "replication_key": "id",
                   "replication_key_value": 123456,
                   "version": 1
               }

        # Postgres: postgres_source_db.public.foo not exists
        assert utils.get_bookmark_for_table("public.foo", postgres_properties, PostgresMock(),
                                            dbname="postgres_source_db") == {}

    def test_get_bookmark_for_table_tap_s3_csv(self):
        """Test bookmark extractors for S3 CSV taps"""
        # Load properties JSON
        properties = utils.load_json("{}/properties_s3_csv.json".format(RESOURCES_DIR))

        # applications is INCREMENTAL
        assert utils.get_bookmark_for_table("applications", properties, S3CsvMock()) == {
            "modified_since": '2019-11-15T07:39:44.171098',
        }

        # candidate_survey_questions is Full table
        assert utils.get_bookmark_for_table("candidate_survey_questions", properties, S3CsvMock()) == {}

        # foo not exists
        assert utils.get_bookmark_for_table("foo", properties, S3CsvMock()) == {}

    def test_get_target_schema(self):
        """Test target schema extractor from target config"""
        # No default_target_schema and schema_mapping should raise exception
        with pytest.raises(Exception):
            invalid_target_config = {}
            utils.get_target_schema(invalid_target_config, "foo.foo")

        # Empty default_target_schema should raise exception
        with pytest.raises(Exception):
            target_config_with_default = {"default_target_schema": ""}
            utils.get_target_schema(target_config_with_default, "foo.foo")

        # Default_target_schema should define the target_schema
        target_config_with_default = {"default_target_schema": "target_schema"}
        assert utils.get_target_schema(target_config_with_default, "foo.foo") == "target_schema"

        # Empty schema_mapping should raise exception
        with pytest.raises(Exception):
            target_config_with_empty_schema_mapping = {"schema_mapping": {}}
            utils.get_target_schema(target_config_with_empty_schema_mapping, "foo.foo")

        # Missing schema in schema_mapping should raise exception
        with pytest.raises(Exception):
            target_config_with_missing_schema_mapping = {"schema_mapping": {"foo2": {"target_schema": "foo2"}}}
            utils.get_target_schema(target_config_with_missing_schema_mapping, "foo.foo")

        # Target schema should be extracted from schema_mapping
        target_config_with_schema_mapping = {"schema_mapping": {"foo": {"target_schema": "foo"}}}
        assert utils.get_target_schema(target_config_with_schema_mapping, "foo.foo") == "foo"

        # If target schema exist in schema_mapping then should not use the default_target_schema
        target_config = {
            "default_target_schema": "target_schema",
            "schema_mapping": {"foo": {"target_schema": "foo"}}
        }
        assert utils.get_target_schema(target_config, "foo.foo") == "foo"

        # If target schema not exist in schema_mapping then should return the default_target_schema
        target_config = {
            "default_target_schema": "target_schema",
            "schema_mapping": {"foo2": {"target_schema": "foo2"}}
        }
        assert utils.get_target_schema(target_config, "foo.foo") == "target_schema"

    def test_get_grantees(self):
        """Test grantees extractor from target config"""
        # No default_target_schema_select_permissions and schema_mapping should return empty list
        target_config_with_empty_grantees = {}
        assert utils.get_grantees(target_config_with_empty_grantees, "foo.foo") == []

        # Empty default_target_schema_select_permissions should return empty list
        target_config_with_default_empty = {"default_target_schema_select_permissions": ""}
        assert utils.get_grantees(target_config_with_default_empty, "foo.foo") == []

        # default_target_schema_select_permissions as string should return list
        target_config_with_default_as_string = {"default_target_schema_select_permissions": "grantee"}
        assert utils.get_grantees(target_config_with_default_as_string, "foo.foo") == ["grantee"]

        # default_target_schema_select_permissions as list should return list
        target_config_with_default_as_list = {"default_target_schema_select_permissions": ["grantee1"]}
        assert utils.get_grantees(target_config_with_default_as_list, "foo.foo") == ["grantee1"]

        # default_target_schema_select_permissions as list should return list
        target_config_with_default_as_list = {"default_target_schema_select_permissions": ["grantee1", "grantee2"]}
        assert utils.get_grantees(target_config_with_default_as_list, "foo.foo") == ["grantee1", "grantee2"]

        # Empty schema_mapping should return empty list
        target_config_with_empty_schema_mapping = {"schema_mapping": {}}
        assert utils.get_grantees(target_config_with_empty_schema_mapping, "foo.foo") == []

        # Missing schema in schema_mapping should return empty list
        target_config_with_missing_schema_mapping = {
            "schema_mapping": {"foo2": {"target_schema_select_permissions": "grantee"}}}
        assert utils.get_grantees(target_config_with_missing_schema_mapping, "foo.foo") == []

        # Grantees as string should be extracted from schema_mapping
        target_config_with_missing_schema_mapping = {
            "schema_mapping": {"foo": {"target_schema_select_permissions": "grantee"}}}
        assert utils.get_grantees(target_config_with_missing_schema_mapping, "foo.foo") == ["grantee"]

        # Grantees as list should be extracted from schema_mapping
        target_config_with_missing_schema_mapping = {
            "schema_mapping": {"foo": {"target_schema_select_permissions": ["grantee1", "grantee2"]}}}
        assert utils.get_grantees(target_config_with_missing_schema_mapping, "foo.foo") == ["grantee1", "grantee2"]

        # If grantees exist in schema_mapping then should not use the default_target_schema_select_permissions
        target_config = {
            "default_target_schema_select_permissions": ["grantee1", "grantee2"],
            "schema_mapping": {"foo": {"target_schema_select_permissions": ["grantee3", "grantee4"]}}
        }
        assert utils.get_grantees(target_config, "foo.foo") == ["grantee3", "grantee4"]

        # If target schema not exist in schema_mapping then should return the default_target_schema_select_permissions
        target_config = {
            "default_target_schema_select_permissions": ["grantee1", "grantee2"],
            "schema_mapping": {"foo2": {"target_schema_select_permissions": ["grantee3", "grantee4"]}}
        }
        assert utils.get_grantees(target_config, "foo.foo") == ["grantee1", "grantee2"]

        # default_target_schema_select_permissions as dict with string should return dict
        target_config_with_default_as_dict = {"default_target_schema_select_permissions": {
            "users": "grantee_user1",
            "groups": "grantee_group1"
        }}
        assert utils.get_grantees(target_config_with_default_as_dict, "foo.foo") == {
            "users": ["grantee_user1"],
            "groups": ["grantee_group1"]
        }

        # default_target_schema_select_permissions as dict with list should return dict
        target_config_with_default_as_dict = {"default_target_schema_select_permissions": {
            "users": ["grantee_user1", "grantee_user2"],
            "groups": ["grantee_group1", "grantee_group2"]
        }}
        assert utils.get_grantees(target_config_with_default_as_dict, "foo.foo") == {
            "users": ["grantee_user1", "grantee_user2"],
            "groups": ["grantee_group1", "grantee_group2"]
        }

    def test_get_cpu_cores_should_succeed(self):
        assert utils.get_cpu_cores() >= 1

    def test_check_config_with_all_required_keys_present_should_succeed(self):
        config = {'key1': 1, 'key2': 2, 'key3': 3}
        required_keys = {'key1', 'key2', 'key3'}

        utils.check_config(config, required_keys)

    def test_check_config_with_some_required_keys_not_present_should_raise_exception(self):
        config = {'key1': 1, 'key2': 2, 'key3': 3}
        required_keys = {'key1', 'key4'}

        with pytest.raises(Exception):
            utils.check_config(config, required_keys)
