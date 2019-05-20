import unittest
from nose.tools import assert_raises

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

        # Minimal configuratino should pass - (nr_of_errors == 0)
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
        """Test JSON type to Snowflake column type mappings"""
        mapper = target_postgres.db_sync.column_type

        # Incoming JSON schema types
        json_str =          {"type": ["string"]             }
        json_str_or_null =  {"type": ["string", "null"]     }
        json_dt =           {"type": ["string"]             , "format": "date-time"}
        json_dt_or_null =   {"type": ["string", "null"]     , "format": "date-time"}
        json_t =            {"type": ["string"]             , "format": "time"}
        json_t_or_null =    {"type": ["string", "null"]     , "format": "time"}
        json_num =          {"type": ["number"]             }
        json_int =          {"type": ["integer"]            }
        json_int_or_str =   {"type": ["integer", "string"]  }
        json_bool =         {"type": ["boolean"]            }
        json_obj =          {"type": ["object"]             }
        json_arr =          {"type": ["array"]              }
        
        # Mapping from JSON schema types ot Snowflake column types
        self.assertEquals(mapper(json_str)          , 'character varying')
        self.assertEquals(mapper(json_str_or_null)  , 'character varying')
        self.assertEquals(mapper(json_dt)           , 'timestamp without time zone')
        self.assertEquals(mapper(json_dt_or_null)   , 'timestamp without time zone')
        self.assertEquals(mapper(json_t)            , 'time')
        self.assertEquals(mapper(json_t_or_null)    , 'time')
        self.assertEquals(mapper(json_num)          , 'numeric')
        self.assertEquals(mapper(json_int)          , 'bigint')
        self.assertEquals(mapper(json_int_or_str)   , 'character varying')
        self.assertEquals(mapper(json_bool)         , 'boolean')
        self.assertEquals(mapper(json_obj)          , 'jsonb')
        self.assertEquals(mapper(json_arr)          , 'jsonb')

