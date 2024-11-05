import unittest

import target_postgres


class TestUnit(unittest.TestCase):
    """
    Unit Tests
    """
    @classmethod
    def setUp(self):
        self.config = {}

    def test_config_validation(self):
        """Test configuration validator"""
        validator = target_postgres.db_sync.validate_config
        empty_config = {}
        minimal_config = {
            'host':                     "dummy-value",
            'port':                     5432,
            'user':                     "dummy-value",
            'password':                 "dummy-value",
            'dbname':                   "dummy-value",
            'default_target_schema':    "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors >= 0)
        self.assertGreater(len(validator(empty_config)),  0)

        # Minimal configuration should pass - (nr_of_errors == 0)
        self.assertEqual(len(validator(minimal_config)), 0)

        # Configuration without schema references - (nr_of_errors >= 0)
        config_with_no_schema = minimal_config.copy()
        config_with_no_schema.pop('default_target_schema')
        self.assertGreater(len(validator(config_with_no_schema)), 0)

        # Configuration with schema mapping - (nr_of_errors >= 0)
        config_with_schema_mapping = minimal_config.copy()
        config_with_schema_mapping.pop('default_target_schema')
        config_with_schema_mapping['schema_mapping'] = {
            "dummy_stream": {
                "target_schema": "dummy_schema"
            }
        }
        self.assertEqual(len(validator(config_with_schema_mapping)), 0)


    def test_column_type_mapping(self):
        """Test JSON type to Postgres column type mappings"""
        mapper = target_postgres.db_sync.column_type

        # Incoming JSON schema types
        json_str =          {"type": ["string"]             }
        json_str_or_null =  {"type": ["string", "null"]     }
        json_dt =           {"type": ["string"]             , "format": "date-time"}
        json_dt_or_null =   {"type": ["string", "null"]     , "format": "date-time"}
        json_date =         {"type": ["string"]             , "format": "date"}
        json_date_or_null = {"type": ["string", "null"]     , "format": "date"}
        json_t =            {"type": ["string"]             , "format": "time"}
        json_t_or_null =    {"type": ["string", "null"]     , "format": "time"}
        json_num =          {"type": ["number"]             }
        json_smallint =     {"type": ["integer"]            , "maximum": 32767, "minimum": -32768}
        json_int =          {"type": ["integer"]            , "maximum": 2147483647, "minimum": -2147483648}
        json_bigint =       {"type": ["integer"]            , "maximum": 9223372036854775807, "minimum": -9223372036854775808}
        json_nobound_int =  {"type": ["integer"]            }
        json_int_or_str =   {"type": ["integer", "string"]  }
        json_bool =         {"type": ["boolean"]            }
        json_obj =          {"type": ["object"]             }
        json_arr =          {"type": ["array"]              }
        
        # Mapping from JSON schema types to Postgres column types
        self.assertEqual(mapper(json_str)          , 'character varying')
        self.assertEqual(mapper(json_str_or_null)  , 'character varying')
        self.assertEqual(mapper(json_dt)           , 'timestamp without time zone')
        self.assertEqual(mapper(json_dt_or_null)   , 'timestamp without time zone')
        self.assertEqual(mapper(json_date)         , 'date')
        self.assertEqual(mapper(json_date_or_null) , 'date')
        self.assertEqual(mapper(json_t)            , 'time without time zone')
        self.assertEqual(mapper(json_t_or_null)    , 'time without time zone')
        self.assertEqual(mapper(json_num)          , 'double precision')
        self.assertEqual(mapper(json_smallint)     , 'smallint')
        self.assertEqual(mapper(json_int)          , 'integer')
        self.assertEqual(mapper(json_bigint)       , 'bigint')
        self.assertEqual(mapper(json_nobound_int)  , 'numeric')
        self.assertEqual(mapper(json_int_or_str)   , 'character varying')
        self.assertEqual(mapper(json_bool)         , 'boolean')
        self.assertEqual(mapper(json_obj)          , 'jsonb')
        self.assertEqual(mapper(json_arr)          , 'jsonb')

    def test_stream_name_to_dict(self):
        """Test identifying catalog, schema and table names from fully qualified stream and table names"""
        # Singer stream name format (Default '-' separator)
        assert \
            target_postgres.db_sync.stream_name_to_dict('my_table') == \
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"}

        # Singer stream name format (Default '-' separator)
        assert \
            target_postgres.db_sync.stream_name_to_dict('my_schema-my_table') == \
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"}

        # Singer stream name format (Default '-' separator)
        assert \
            target_postgres.db_sync.stream_name_to_dict('my_catalog-my_schema-my_table') == \
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"}

        # Redshift table format (Custom '.' separator)
        assert \
            target_postgres.db_sync.stream_name_to_dict('my_table', separator='.') == \
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"}

        # Redshift table format (Custom '.' separator)
        assert \
            target_postgres.db_sync.stream_name_to_dict('my_schema.my_table', separator='.') == \
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"}

        # Redshift table format (Custom '.' separator)
        assert \
            target_postgres.db_sync.stream_name_to_dict('my_catalog.my_schema.my_table', separator='.') == \
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"}


    def test_flatten_schema(self):
        """Test flattening of SCHEMA messages"""
        flatten_schema = target_postgres.db_sync.flatten_schema

        # Schema with no object properties should be empty dict
        schema_with_no_properties = {"type": "object"}
        assert flatten_schema(schema_with_no_properties) == {}

        not_nested_schema = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]}}}
        # NO FLATTENNING - Schema with simple properties should be a plain dictionary
        assert flatten_schema(not_nested_schema) == not_nested_schema['properties']

        nested_schema_with_no_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {"type": ["null", "object"]}}}
        # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
        assert flatten_schema(nested_schema_with_no_properties) == nested_schema_with_no_properties['properties']

        nested_schema_with_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {
                    "type": ["null", "object"],
                    "properties": {
                        "nested_prop1": {"type": ["null", "string"]},
                        "nested_prop2": {"type": ["null", "string"]},
                        "nested_prop3": {
                            "type": ["null", "object"],
                            "properties": {
                                "multi_nested_prop1": {"type": ["null", "string"]},
                                "multi_nested_prop2": {"type": ["null", "string"]}
                            }
                        }
                    }
                }
            }
        }
        # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
        # No flattening (default)
        assert flatten_schema(nested_schema_with_properties) == nested_schema_with_properties['properties']

        # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
        #   max_level: 0 : No flattening (default)
        assert flatten_schema(nested_schema_with_properties, max_level=0) == nested_schema_with_properties['properties']

        # FLATTENNING - Schema with object type property but without further properties should be a dict with flattened properties
        assert \
            flatten_schema(nested_schema_with_properties, max_level=1) == \
            {
                'c_pk': {'type': ['null', 'integer']},
                'c_varchar': {'type': ['null', 'string']},
                'c_int': {'type': ['null', 'integer']},
                'c_obj__nested_prop1': {'type': ['null', 'string']},
                'c_obj__nested_prop2': {'type': ['null', 'string']},
                'c_obj__nested_prop3': {
                    'type': ['null', 'object'],
                    "properties": {
                        "multi_nested_prop1": {"type": ["null", "string"]},
                        "multi_nested_prop2": {"type": ["null", "string"]}
                    }
                }
        }

        # FLATTENNING - Schema with object type property but without further properties should be a dict with flattened properties
        assert \
            flatten_schema(nested_schema_with_properties, max_level=10) == \
            {
                'c_pk': {'type': ['null', 'integer']},
                'c_varchar': {'type': ['null', 'string']},
                'c_int': {'type': ['null', 'integer']},
                'c_obj__nested_prop1': {'type': ['null', 'string']},
                'c_obj__nested_prop2': {'type': ['null', 'string']},
                'c_obj__nested_prop3__multi_nested_prop1': {'type': ['null', 'string']},
                'c_obj__nested_prop3__multi_nested_prop2': {'type': ['null', 'string']}
            }

    def test_flatten_record(self):
        """Test flattening of RECORD messages"""
        flatten_record = target_postgres.db_sync.flatten_record

        empty_record = {}
        # Empty record should be empty dict
        assert flatten_record(empty_record) == {}

        not_nested_record = {"c_pk": 1, "c_varchar": "1", "c_int": 1}
        # NO FLATTENNING - Record with simple properties should be a plain dictionary
        assert flatten_record(not_nested_record) == not_nested_record

        nested_record = {
            "c_pk": 1,
            "c_varchar": "1",
            "c_int": 1,
            "c_obj": {
                "nested_prop1": "value_1",
                "nested_prop2": "value_2",
                "nested_prop3": {
                    "multi_nested_prop1": "multi_value_1",
                    "multi_nested_prop2": "multi_value_2",
                }}}

        # NO FLATTENNING - No flattening (default)
        assert \
            flatten_record(nested_record) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
            }

        # NO FLATTENNING
        #   max_level: 0 : No flattening (default)
        assert \
            flatten_record(nested_record, max_level=0) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
            }

        # SEMI FLATTENNING
        #   max_level: 1 : Semi-flattening (default)
        assert \
            flatten_record(nested_record, max_level=1) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj__nested_prop1": "value_1",
                "c_obj__nested_prop2": "value_2",
                "c_obj__nested_prop3": '{"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}'
            }

        # FLATTENNING
        assert \
            flatten_record(nested_record, max_level=10) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj__nested_prop1": "value_1",
                "c_obj__nested_prop2": "value_2",
                "c_obj__nested_prop3__multi_nested_prop1": "multi_value_1",
                "c_obj__nested_prop3__multi_nested_prop2": "multi_value_2"
            }

    def test_flatten_record_with_flatten_schema(self):
        flatten_record = target_postgres.db_sync.flatten_record

        flatten_schema = {
            "id": {
                "type": [
                    "object",
                    "array",
                    "null"
                ]
            }
        }

        test_cases = [
            (
                True,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": "1",
                    "data": "xyz"
                }
            ),
            (
                False,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": 1,
                    "data": "xyz"
                }
            )
        ]

        for idx, (should_use_flatten_schema, record, expected_output) in enumerate(test_cases):
            output = flatten_record(record, flatten_schema if should_use_flatten_schema else None)
            assert output == expected_output
