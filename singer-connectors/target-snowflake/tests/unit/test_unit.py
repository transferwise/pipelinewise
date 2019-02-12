import unittest
import os
import json
import datetime
import binascii
from nose.tools import assert_raises

import target_snowflake
from target_snowflake.crypto import Crypto


class TestUnit(unittest.TestCase):
    """
    """
    @classmethod
    def setUp(self):
        self.config = {}

    @staticmethod
    def slurp_as_hex(file):
        with open(file, 'rb') as f:
            return binascii.hexlify(f.read())


    @staticmethod
    def spit(file, string):
        with open(file, 'w+b') as f:
            f.write(bytes(string, 'UTF-8'))


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


    def test_file_encryption_padded_string(self):
        content_padded = "THIS IS A FILE WITH SENSITIVE DATA AND NEEDS TO BE ENCRYPTED AND DECRYPTED\n"
        plain_filename = "sample-padded.csv"
        encrypted_filename = "sample-padded-encrypted.csv"
        decrypted_filename = "sample-padded-decrypted.csv"
        master_key = "0123456789abcdef"

        try:
            # Create an unencrypted input file
            self.spit(plain_filename, content_padded)

            crypto = Crypto(master_key)
            iv = crypto.encrypt_file(plain_filename, encrypted_filename)
            crypto.decrypt_file(encrypted_filename, iv, decrypted_filename)

            # Hex decoded original and decrypted file should match
            self.assertEquals(
                self.slurp_as_hex(decrypted_filename),
                self.slurp_as_hex(plain_filename)
            )

        finally:

            # Remove files at the end
            try:
                os.remove(plain_filename)
                os.remove(encrypted_filename)
                os.remove(decrypted_filename)

            # Ignore if file not exists
            except OSError:
                pass


    def test_file_encryption_not_padded_string(self):
        content_no_padding = "THIS IS A MULTI 16 BYTES LENGTH STRING WHERE NO AES CBC PADDING NEEDED---------\n"
        plain_filename = "sample-no-padding.csv"
        encrypted_filename = "sample-no-padding-encrypted.csv"
        decrypted_filename = "sample-no-padding-decrypted.csv"
        master_key = "0123456789abcdef"

        try:
            # Create an unencrypted input file
            self.spit(plain_filename, content_no_padding)

            crypto = Crypto(master_key)
            iv = crypto.encrypt_file(plain_filename, encrypted_filename)
            crypto.decrypt_file(encrypted_filename, iv, decrypted_filename)

            # Hex decoded original and decrypted file should match
            self.assertEquals(
                self.slurp_as_hex(decrypted_filename),
                self.slurp_as_hex(plain_filename)
            )

        finally:

            # Remove files at the end
            try:
                os.remove(plain_filename)
                os.remove(encrypted_filename)
                os.remove(decrypted_filename)

            # Ignore if file not exists
            except OSError:
                pass