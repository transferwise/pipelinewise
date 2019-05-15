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
            transform_field.transform.do_transform("John", "SET-NULL"),
            None
        )


    def test_hash(self):
        """Test HASH transformation"""
        self.assertEquals(
            transform_field.transform.do_transform("John", "HASH"),
            hashlib.sha256("John".encode('utf-8')).hexdigest()
        )


    def test_mask_date(self):
        """Test MASK-DATE transformation"""
        self.assertEquals(
            transform_field.transform.do_transform("2019-05-21", "MASK-DATE"),
            "2019-01-01T00:00:00"
        )

        # Mask date should keep the time elements
        self.assertEquals(
            transform_field.transform.do_transform("2019-05-21T13:34:11", "MASK-DATE"),
            "2019-01-01T13:34:11"
        )

        # Mask date should keep the time elements
        self.assertEquals(
            transform_field.transform.do_transform("2019-05-21T13:34:99", "MASK-DATE"),
            "2019-05-21T13:34:99"
        )


    def test_mask_number(self):
        """Test MASK-NUMBER transformation"""
        self.assertEquals(
            transform_field.transform.do_transform(1234567890, "MASK-NUMBER"),
            0
        )


    def test_inline_code(self):
        """Test inline code transformations"""
        transform_field.transform.do_transform("kaka", "INLINE-CODE")
        self.assertEquals(0, 0)


    def test_unknown_transformation_type(self):
        """Test not existing transformation type"""
        # Should return the original value
        self.assertEqual(
            transform_field.transform.do_transform("John", "NOT-EXISTING-TRANSFORMATION-TYPE"),
            "John"
        )
