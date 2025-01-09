import unittest
import hashlib

from transform_field import transform


class TestTransform(unittest.TestCase):
    """
    Unit Tests for the transform module
    """

    def setUp(self) -> None:
        self.config = {}

    def test_set_null(self):
        """TEST SET-NULL transformation"""
        self.assertEqual(
            transform.do_transform({"col_1": "John"}, "col_1", "SET-NULL"),
            None
        )

    def test_hash(self):
        """Test HASH transformation"""
        self.assertEqual(
            transform.do_transform({"col_1": "John"}, "col_1", "HASH"),
            hashlib.sha256("John".encode('utf-8')).hexdigest()
        )

    def test_mask_date(self):
        """Test MASK-DATE transformation"""
        self.assertEqual(
            transform.do_transform({"col_1": "2019-05-21"}, "col_1", "MASK-DATE"),
            "2019-01-01T00:00:00"
        )

        # Mask date should keep the time elements
        self.assertEqual(
            transform.do_transform({"col_1": "2019-05-21T13:34:11"}, "col_1", "MASK-DATE"),
            "2019-01-01T13:34:11"
        )

        # Mask date should keep the time elements, date is invalid
        self.assertEqual(
            transform.do_transform({"col_1": "2019-05-21T13:34:99"}, "col_1", "MASK-DATE"),
            "2019-05-21T13:34:99"
        )

    def test_mask_number(self):
        """Test MASK-NUMBER transformation"""
        self.assertEqual(
            transform.do_transform({"col_1": "1234567890"}, "col_1", "MASK-NUMBER"),
            0
        )

    def test_mask_hidden(self):
        """Test MASK-HIDDEN transformation"""
        self.assertEqual(
            transform.do_transform({"col_1": "abakadabra123"}, "col_1", "MASK-HIDDEN"),
            'hidden'
        )

    def test_mask_string_skip_ends_case1(self):
        """Test MASK-STRING-SKIP-ENDS transformation with n=3"""
        self.assertEqual(
            transform.do_transform({"col_1": "do!maskme!"}, "col_1", "MASK-STRING-SKIP-ENDS-3"),
            'do!****me!'
        )

    def test_mask_string_skip_ends_case2(self):
        """Test MASK-STRING-SKIP-ENDS transformation with n=2"""
        self.assertEqual(
            transform.do_transform({"col_1": "nomask"}, "col_1", "MASK-STRING-SKIP-ENDS-2"),
            'no**sk'
        )

    def test_mask_string_skip_ends_case3(self):
        """Test MASK-STRING-SKIP-ENDS transformation where string length equals to 2 * mask_length"""
        self.assertEqual(
            transform.do_transform({"col_1": "nomask"}, "col_1", "MASK-STRING-SKIP-ENDS-3"),
            '******'
        )

    def test_mask_string_skip_ends_case4(self):
        """Test MASK-STRING-SKIP-ENDS transformation where string length less than 2 * mask_length"""
        self.assertEqual(
            transform.do_transform({"col_1": "shortmask"}, "col_1", "MASK-STRING-SKIP-ENDS-5"),
            '*********'
        )

    def test_unknown_transformation_type(self):
        """Test not existing transformation type"""
        # Should return the original value
        self.assertEqual(
            transform.do_transform({"col_1": "John"}, "col_1", "NOT-EXISTING-TRANSFORMATION-TYPE"),
            "John"
        )

    def test_conditions(self):
        """Test conditional transformations"""

        # Matching condition: Should transform to NULL
        self.assertEqual(
            transform.do_transform(
                # Record:
                {"col_1": "random value", "col_2": "passwordHash", "col_3": "lkj"},
                # Column to transform:
                "col_3",
                # Transform method:
                "SET-NULL",
                # Conditions when to transform:
                [
                    {'column': 'col_1', 'equals': "random value"},
                    {'column': 'col_2', 'equals': "passwordHash"},
                ]
            ),

            # Expected output:
            None
        )

        # Not matching condition: Should keep the original value
        self.assertEqual(
            transform.do_transform(
                # Record:
                {"col_1": "random value", "col_2": "id", "col_3": "123456789"},
                # Column to transform:
                "col_3",
                # Transform method:
                "SET-NULL",
                # Conditions when to transform:
                [
                    {'column': 'col_1', 'equals': "random value"},
                    {'column': 'col_2', 'equals': "passwordHash"},
                ]
            ),

            # Expected output:
            "123456789"
        )

    def test_transform_field_in_json_col(self):
        """Test transformation of a field in a json column with no conditions"""

        expected_value = {'id': 1, 'info': {'last_name': 'hidden', 'first_name': 'John'}}

        return_value = transform.do_transform(
            # Record:
            {
                "col_1": "random value",
                "col_2": "passwordHash",
                "col_3": "lkj",
                'col_4': {'id': 1, 'info': {'last_name': 'Smith', 'first_name': 'John'}}
            },
            # Column to transform:
            "col_4",
            # Transform method:
            "MASK-HIDDEN",
            # Conditions when to transform:
            None,
            ['info/last_name']
        )

        self.assertDictEqual(expected_value, return_value)

    def test_transform_field_in_json_col_with_conditions(self):
        """Test transformation of a field in a json column with conditions"""

        expected_value = {'id': 1, 'info': {'last_name': 'hidden', 'first_name': 'John'}}

        return_value = transform.do_transform(
            # Record:
            {
                "col_1": "random value",
                "col_2": "passwordHash",
                "col_3": "lkj",
                'col_4': {'id': 1, 'info': {'last_name': 'Smith', 'first_name': 'John'}}
            },
            # Column to transform:
            "col_4",
            # Transform method:
            "MASK-HIDDEN",
            # Conditions when to transform:
            [
                {'column': 'col_2', 'equals': "passwordHash"},
            ],
            ['info/last_name']
        )

        self.assertDictEqual(expected_value, return_value)

    def test_transform_fields_in_json_col(self):
        """Test transformation of multiple fields in a json column with no conditions"""

        expected_value = {'id': 1, 'info': {'last_name': 'hidden', 'first_name': 'hidden', 'age': 25}}

        return_value = transform.do_transform(
            # Record:
            {
                "col_1": "random value",
                "col_2": "passwordHash",
                "col_3": "lkj",
                'col_4': {'id': 1, 'info': {'last_name': 'Smith', 'first_name': 'John', 'age': 25}}
            },
            # Column to transform:
            "col_4",
            # Transform method:
            "MASK-HIDDEN",
            # Conditions when to transform:
            None,
            ['info/last_name', 'info/first_name']
        )

        self.assertDictEqual(expected_value, return_value)

    def test_transform_col_with_condition_on_json_field(self):
        """Test transformation of a column with condition on a field in a json"""

        record = {
            "col_1": "random value",
            "col_2": "passwordHash",
            "col_3": "323df43983dfs",
            'col_4': {'id': 1, 'info': {'last_name': 'Smith', 'first_name': 'John', 'phone': '6573930'}}
        }

        self.assertEqual(
            'hidden',
            transform.do_transform(
                # Record:
                record,
                # Column to transform:
                "col_3",
                # Transform method:
                "MASK-HIDDEN",
                # Conditions when to transform:
                [
                    {'column': 'col_4', 'field_path': 'info/last_name', 'equals': 'Smith'},
                ]
            )
        )

    def test_transform_field_in_json_col_with_condition_on_field(self):
        """Test transformation of a field in a json column with condition on a field in json, condition will be met"""

        record = {
            "col_1": "random value",
            "col_2": "passwordHash",
            "col_3": "lkj",
            'col_4': {'id': 1, 'info': {'last_name': 'Smith', 'first_name': 'John', 'phone': '6573930'}}
        }

        self.assertDictEqual(
            {'id': 1, 'info': {'first_name': 'John', 'last_name': None, 'phone': '6573930'}},
            transform.do_transform(
                # Record:
                record,
                # Column to transform:
                "col_4",
                # Transform method:
                "SET-NULL",
                # Conditions when to transform:
                [
                    {'column': 'col_4', 'field_path': 'info/phone', 'equals': '6573930'},
                ],
                ['info/last_name']
            )
        )

    def test_transform_field_in_json_col_with_condition_on_field_2(self):
        """Test transformation of a field in a json column with condition on a field in json,
        the condition will not be met"""

        record = {
            "col_1": "random value",
            "col_2": "passwordHash",
            "col_3": "lkj",
            'col_4': {'id': 1, 'info': {'last_name': 'Smith', 'first_name': 'John', 'phone': '6573930'}}
        }

        # not transformed
        self.assertEqual(
            {'id': 1, 'info': {'last_name': 'Smith', 'first_name': 'John', 'phone': '6573930'}},
            transform.do_transform(
                # Record:
                record,
                # Column to transform:
                "col_4",
                # Transform method:
                "SET-NULL",
                # Conditions when to transform:
                [
                    {'column': 'col_4', 'field_path': 'info/phone', 'regex_match': '.*6573955.*'},
                ],
                ['info/last_name']
            )
        )

    def test_transform_multiple_conditions_all_success(self):
        """Test conditional transformation, all the conditions will be met and transformation should happen"""

        record = {
            "col_1": "random value",
            "col_2": "passwordHash",
            "col_3": "lkj",
            'col_4': {'id': 1, 'info': {'last_name': 'Smith', 'first_name': 'John', 'phone': '6573930'}},
            'col_5': '2021-11-30T16:40:07'
        }

        self.assertEqual(
            '2021-01-01T16:40:07',
            transform.do_transform(
                # Record:
                record,
                # Column to transform:
                "col_5",
                # Transform method:
                "MASK-DATE",
                # Conditions when to transform:
                [
                    {'column': 'col_4', 'field_path': 'info/last_name', 'equals': 'Smith'},
                    {'column': 'col_4', 'field_path': 'id', 'equals': 1},
                    {'column': 'col_3', 'regex_match': '.*lkj.*'},
                ]
            )
        )

    def test_transform_multiple_conditions_one_fails(self):
        """Test conditional transformation, one of the conditions will not be met and transformation should not happen"""

        record = {
            "col_1": "random value",
            "col_2": "passwordHash",
            "col_3": "lkj",
            'col_4': {'id': 1, 'info': {'last_name': 'Smith', 'first_name': 'John', 'phone': '6573930'}},
            'col_5': '2021-11-30T16:40:07'
        }

        # not transformed
        self.assertEqual(
            '2021-11-30T16:40:07',
            transform.do_transform(
                # Record:
                record,
                # Column to transform:
                "col_5",
                # Transform method:
                "MASK-DATE",
                # Conditions when to transform:
                [
                    {'column': 'col_4', 'field_path': 'info/last_name', 'equals': 'Smith'},
                    {'column': 'col_4', 'field_path': 'id', 'equals': 2},
                    {'column': 'col_3', 'regex_match': '.*lkj.*'},
                ]
            )
        )

