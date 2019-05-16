import unittest
from nose.tools import assert_raises

import transform_field

import hashlib


class TestUnit(unittest.TestCase):
    """
    Unit Tests
    """
    @classmethod
    def setUp(self):
        self.config = {}


    def test_set_null(self):
        """TEST SET-NULL transformation"""
        self.assertEquals(
            transform_field.transform.do_transform({"col_1":"John"}, "col_1", "SET-NULL"),
            None
        )


    def test_hash(self):
        """Test HASH transformation"""
        self.assertEquals(
            transform_field.transform.do_transform({"col_1":"John"}, "col_1", "HASH"),
            hashlib.sha256("John".encode('utf-8')).hexdigest()
        )


    def test_mask_date(self):
        """Test MASK-DATE transformation"""
        self.assertEquals(
            transform_field.transform.do_transform({"col_1":"2019-05-21"}, "col_1", "MASK-DATE"),
            "2019-01-01T00:00:00"
        )

        # Mask date should keep the time elements
        self.assertEquals(
            transform_field.transform.do_transform({"col_1":"2019-05-21T13:34:11"}, "col_1", "MASK-DATE"),
            "2019-01-01T13:34:11"
        )

        # Mask date should keep the time elements
        self.assertEquals(
            transform_field.transform.do_transform({"col_1":"2019-05-21T13:34:99"}, "col_1", "MASK-DATE"),
            "2019-05-21T13:34:99"
        )


    def test_mask_number(self):
        """Test MASK-NUMBER transformation"""
        self.assertEquals(
            transform_field.transform.do_transform({"col_1":"1234567890"}, "col_1", "MASK-NUMBER"),
            0
        )


    def test_unknown_transformation_type(self):
        """Test not existing transformation type"""
        # Should return the original value
        self.assertEqual(
            transform_field.transform.do_transform({"col_1":"John"}, "col_1", "NOT-EXISTING-TRANSFORMATION-TYPE"),
            "John"
        )


    def test_conditions(self):
        """Test conditional transformations"""

        # Matching condition: Should transform to NULL
        self.assertEqual(
            transform_field.transform.do_transform(
              # Record:
              {"col_1":"com.transferwise.fx.user.User", "col_2":"passwordHash", "col_3":"lkj"},
              # Column to transform:
              "col_3",
              # Transform method:
              "SET-NULL",
              # Conditions when to transform:
              [
                {'column': 'col_1', 'equals': "com.transferwise.fx.user.User" },
                {'column': 'col_2', 'equals': "passwordHash" },
              ]
            ),

            # Expected output:
            None
        )

        # Not matching condition: Should keep the original value
        self.assertEqual(
            transform_field.transform.do_transform(
              # Record:
              {"col_1":"com.transferwise.fx.user.User", "col_2":"id", "col_3":"123456789"},
              # Column to transform:
              "col_3",
              # Transform method:
              "SET-NULL",
              # Conditions when to transform:
              [
                {'column': 'col_1', 'equals': "com.transferwise.fx.user.User" },
                {'column': 'col_2', 'equals': "passwordHash" },
              ]
            ),

            # Expected output:
            "123456789"
        )