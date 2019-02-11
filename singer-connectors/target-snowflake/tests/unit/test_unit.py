import unittest
import json
import datetime
import target_snowflake
from target_snowflake.db_sync import DbSync

from nose.tools import assert_raises 


class TestUnit(unittest.TestCase):


    @classmethod
    def setUp(self):
        self.config = {}


    def test_column_type_mapping(self):
        """Test JSON type to Snowflake column type mappings"""
        mapper = target_snowflake.db_sync.column_type

        # Incoming JSON schema types
        json_str = {"type": ["string"]}
        json_str_or_null = {"type": ["string", "null"]}
        json_dt = {"format": "date-time", "type": ["string", "null"]}
        json_dt_or_null = {"format": "date-time", "type": ["string", "null"]}
        json_num = {"type": ["number"]}
        json_int = {"type": ["integer"]}
        json_int_or_str = {"type": ["integer", "string"]}
        json_bool = {"type": ["boolean"]}
        json_obj = {"type": ["object"]}
        json_arr = {"type": ["array"]}
        
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
