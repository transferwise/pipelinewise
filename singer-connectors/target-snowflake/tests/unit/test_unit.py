import unittest
from nose.tools import assert_raises

import target_snowflake


class TestUnit(unittest.TestCase):
    """
    Unit Tests
    """
    @classmethod
    def setUp(self):
        self.config = {}


    def test_config_validation(self):
        """Test configuration validator"""
        validator = target_snowflake.db_sync.validate_config
        empty_config = {}
        minimal_config = {
            'account':                  "dummy-value",
            'dbname':                   "dummy-value",
            'user':                     "dummy-value",
            'password':                 "dummy-value",
            'warehouse':                "dummy-value",
            'aws_access_key_id':        "dummy-value",
            'aws_secret_access_key':    "dummy-value",
            's3_bucket':                "dummy-value",
            'schema':                   "dummy-value",
            'stage':                    "dummy-value",
            'file_format':              "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors >= 0)
        self.assertGreater(len(validator(empty_config)),  0)

        # Minimal configuratino should pass - (nr_of_errors == 0)
        self.assertEqual(len(validator(minimal_config)), 0)

        # Configuration without schema references - (nr_of_errors >= 0)
        config_with_no_schema = minimal_config.copy()
        config_with_no_schema.pop('schema')
        self.assertGreater(len(validator(config_with_no_schema)), 0)

        # Configuration without schema references - (nr_of_errors >= 0)
        config_with_no_schema = minimal_config.copy()
        config_with_no_schema.pop('schema')
        self.assertGreater(len(validator(config_with_no_schema)), 0)

        # Configuration with dynamic schema name - (nr_of_errors >= 0)
        config_with_dynamic_schema_name = minimal_config.copy()
        config_with_dynamic_schema_name.pop('schema')
        config_with_dynamic_schema_name['dynamic_schema_name'] = True
        self.assertEqual(len(validator(config_with_dynamic_schema_name)), 0)


    def test_column_type_mapping(self):
        """Test JSON type to Snowflake column type mappings"""
        mapper = target_snowflake.db_sync.column_type

        # Incoming JSON schema types
        json_str =          {"type": ["string"]             }
        json_str_or_null =  {"type": ["string", "null"]     }
        json_dt =           {"type": ["string"]             , "format": "date-time"}
        json_dt_or_null =   {"type": ["string", "null"]     , "format": "date-time"}
        json_num =          {"type": ["number"]             }
        json_int =          {"type": ["integer"]            }
        json_int_or_str =   {"type": ["integer", "string"]  }
        json_bool =         {"type": ["boolean"]            }
        json_obj =          {"type": ["object"]             }
        json_arr =          {"type": ["array"]              }
        
        # Mapping from JSON schema types ot Snowflake column types
        self.assertEquals(mapper(json_str)          , 'text')
        self.assertEquals(mapper(json_str_or_null)  , 'text')
        self.assertEquals(mapper(json_dt)           , 'timestamp_ntz')
        self.assertEquals(mapper(json_dt_or_null)   , 'timestamp_ntz')
        self.assertEquals(mapper(json_num)          , 'float')
        self.assertEquals(mapper(json_int)          , 'number')
        self.assertEquals(mapper(json_int_or_str)   , 'text')
        self.assertEquals(mapper(json_bool)         , 'boolean')
        self.assertEquals(mapper(json_obj)          , 'variant')
        self.assertEquals(mapper(json_arr)          , 'variant')

