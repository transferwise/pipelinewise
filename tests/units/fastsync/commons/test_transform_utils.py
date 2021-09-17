import unittest

from pipelinewise.fastsync.commons.transform_utils import (
    TransformationHelper,
    SQLFlavor,
)


class TestTransformHelper(unittest.TestCase):
    """
    Unit tests for TransformationHelper class
    """

    def test_get_trans_in_sql_case1(self):
        """
        Test obfuscation where given transformations are empty and sql is Snowflake
        Test should pass
        """
        table_name = 'public-my_table'

        transformations = []

        trans = TransformationHelper.get_trans_in_sql_flavor(
            table_name, transformations, SQLFlavor('snowflake')
        )
        self.assertFalse(trans)

    def test_get_trans_in_sql_case2(self):
        """
        Test obfuscation where given transformations are empty and sql is Postgres
        Test should pass
        """
        table_name = 'public-my_table'

        transformations = []

        trans = TransformationHelper.get_trans_in_sql_flavor(
            table_name, transformations, SQLFlavor('postgres')
        )
        self.assertFalse(trans)

    def test_get_trans_in_sql_case3(self):
        """
        Test obfuscation where given transformations has an unsupported transformation type
        Test should fail
        """
        table_name = 'public-my_table'

        transformations = [
            {
                'field_id': 'col_7',
                'tap_stream_name': 'public-my_table',
                'type': 'RANDOM',
            },
            {
                'field_id': 'col_1',
                'tap_stream_name': 'public-my_other_table',
                'type': 'HASH',
            },
        ]

        with self.assertRaises(ValueError):
            TransformationHelper.get_trans_in_sql_flavor(
                table_name, transformations, SQLFlavor('snowflake')
            )

    def test_get_trans_in_sql_case4(self):
        """
        Test obfuscation where given transformations have no conditions and sql flavor is Snowflake
        Test should pass
        """
        table_name = 'public-my_table'

        transformations = [
            {
                'field_id': 'col_1',
                'tap_stream_name': 'public-my_table',
                'type': 'SET-NULL',
            },
            {
                'field_id': 'col_2',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-HIDDEN',
            },
            {
                'field_id': 'col_3',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-DATE',
            },
            {
                'field_id': 'col_4',
                'tap_stream_name': 'public-my_table',
                'safe_field_id': '"COL_4"',
                'type': 'MASK-NUMBER',
            },
            {'field_id': 'col_5', 'tap_stream_name': 'public-my_table', 'type': 'HASH'},
            {
                'field_id': 'col_1',
                'tap_stream_name': 'public-my_other_table',
                'type': 'HASH',
            },
            {
                'field_id': 'col_6',
                'tap_stream_name': 'public-my_table',
                'type': 'HASH-SKIP-FIRST-5',
            },
            {
                'field_id': 'col_7',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-STRING-SKIP-ENDS-3',
            },
        ]

        trans = TransformationHelper.get_trans_in_sql_flavor(
            table_name, transformations, SQLFlavor('snowflake')
        )

        self.assertListEqual(
            trans,
            [
                {'trans': '"COL_1" = NULL', 'conditions': None},
                {'trans': '"COL_2" = \'hidden\'', 'conditions': None},
                {
                    'trans': '"COL_3" = TIMESTAMP_NTZ_FROM_PARTS(DATE_FROM_PARTS(YEAR("COL_3"), 1, 1),'
                    'TO_TIME("COL_3"))',
                    'conditions': None,
                },
                {'trans': '"COL_4" = 0', 'conditions': None},
                {'trans': '"COL_5" = SHA2("COL_5", 256)', 'conditions': None},
                {
                    'trans': '"COL_6" = CONCAT(SUBSTRING("COL_6", 1, 5), SHA2(SUBSTRING("COL_6", 5 + 1), 256))',
                    'conditions': None,
                },
                {
                    'trans': '"COL_7" = CASE WHEN LENGTH("COL_7") > 2 * 3 THEN '
                        'CONCAT(SUBSTRING("COL_7", 1, 3), REPEAT(\'*\', LENGTH("COL_7")-(2 * 3)), '
                        'SUBSTRING("COL_7", LENGTH("COL_7")-3+1, 3)) '
                        'ELSE "COL_7" END',
                    'conditions': None,
                },
            ],
        )

    def test_get_trans_in_sql_case5(self):
        """
        Test obfuscation where given transformations have no conditions and sql flavor is Postgres
        Test should pass
        """
        table_name = 'public-my_table'

        transformations = [
            {
                'field_id': 'col_1',
                'tap_stream_name': 'public-my_table',
                'type': 'SET-NULL',
            },
            {
                'field_id': 'col_2',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-HIDDEN',
            },
            {
                'field_id': 'col_3',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-DATE',
            },
            {
                'field_id': 'col_4',
                'tap_stream_name': 'public-my_table',
                'safe_field_id': '"COL_4"',
                'type': 'MASK-NUMBER',
            },
            {'field_id': 'col_5', 'tap_stream_name': 'public-my_table', 'type': 'HASH'},
            {
                'field_id': 'col_1',
                'tap_stream_name': 'public-my_other_table',
                'type': 'HASH',
            },
            {
                'field_id': 'col_6',
                'tap_stream_name': 'public-my_table',
                'type': 'HASH-SKIP-FIRST-5',
            },
            {
                'field_id': 'col_7',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-STRING-SKIP-ENDS-3',
            },
        ]

        trans = TransformationHelper.get_trans_in_sql_flavor(
            table_name, transformations, SQLFlavor('postgres')
        )

        self.assertListEqual(
            trans,
            [
                {'trans': '"col_1" = NULL', 'conditions': None},
                {'trans': '"col_2" = \'hidden\'', 'conditions': None},
                {
                    'trans': '"col_3" = MAKE_TIMESTAMP('
                    'DATE_PART(\'year\', "col_3")::int, '
                    '1, '
                    '1, '
                    'DATE_PART(\'hour\', "col_3")::int, '
                    'DATE_PART(\'minute\', "col_3")::int, '
                    'DATE_PART(\'second\', "col_3")::double precision'
                    ')',
                    'conditions': None,
                },
                {'trans': '"col_4" = 0', 'conditions': None},
                {
                    'trans': '"col_5" = ENCODE(DIGEST("col_5", \'sha256\'), \'hex\')',
                    'conditions': None,
                },
                {
                    'trans': '"col_6" = CONCAT(SUBSTRING("col_6", 1, 5), '
                    'ENCODE(DIGEST(SUBSTRING("col_6", 5 + 1), \'sha256\'), \'hex\'))',
                    'conditions': None,
                },
                {
                    'trans': '"col_7" = CASE WHEN LENGTH("col_7") > 2 * 3 THEN '
                        'CONCAT(SUBSTRING("col_7", 1, 3), REPEAT(\'*\', LENGTH("col_7")-(2 * 3)), '
                        'SUBSTRING("col_7", LENGTH("col_7")-3+1, 3)) '
                        'ELSE "col_7" END',
                    'conditions': None,
                },
            ],
        )

    def test_get_trans_in_sql_case6(self):
        """
        Test obfuscation where given transformations have conditions and sql flavor is Snowflake
        Test should pass
        """
        table_name = 'public-my_table'

        transformations = [
            {
                'field_id': 'col_1',
                'tap_stream_name': 'public-my_table',
                'type': 'SET-NULL',
            },
            {
                'field_id': 'col_2',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-HIDDEN',
                'when': [
                    {'column': 'col_4', 'safe_column': '"COL_4"', 'equals': None},
                    {
                        'column': 'col_1',
                    },
                ],
            },
            {
                'field_id': 'col_3',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-DATE',
                'when': [{'column': 'col_5', 'equals': 'some_value'}],
            },
            {
                'field_id': 'col_4',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-NUMBER',
            },
            {'field_id': 'col_5', 'tap_stream_name': 'public-my_table', 'type': 'HASH'},
            {
                'field_id': 'col_10',
                'tap_stream_name': 'public-my_other_table',
                'type': 'HASH',
            },
            {
                'field_id': 'col_6',
                'tap_stream_name': 'public-my_table',
                'type': 'HASH-SKIP-FIRST-5',
                'when': [
                    {'column': 'col_1', 'equals': 30},
                    {'column': 'col_2', 'regex_match': r'[0-9]{3}\.[0-9]{3}'},
                ],
            },
            {
                'field_id': 'col_7',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-STRING-SKIP-ENDS-3',
                'when': [
                    {'column': 'col_1', 'equals': 30},
                    {'column': 'col_2', 'regex_match': r'[0-9]{3}\.[0-9]{3}'},
                    {'column': 'col_4', 'equals': None},
                ],
            },
        ]

        trans = TransformationHelper.get_trans_in_sql_flavor(
            table_name, transformations, SQLFlavor('snowflake')
        )

        self.assertListEqual(
            trans,
            [
                {
                    'trans': '"COL_1" = NULL',
                    'conditions': None,
                },
                {
                    'trans': '"COL_2" = \'hidden\'',
                    'conditions': '("COL_4" IS NULL)',
                },
                {
                    'trans': '"COL_3" = TIMESTAMP_NTZ_FROM_PARTS(DATE_FROM_PARTS(YEAR("COL_3"), 1, 1),'
                    'TO_TIME("COL_3"))',
                    'conditions': '("COL_5" = \'some_value\')',
                },
                {
                    'trans': '"COL_4" = 0',
                    'conditions': None,
                },
                {
                    'trans': '"COL_5" = SHA2("COL_5", 256)',
                    'conditions': None,
                },
                {
                    'trans': '"COL_6" = CONCAT(SUBSTRING("COL_6", 1, 5), SHA2(SUBSTRING("COL_6", 5 + 1), 256))',
                    'conditions': '("COL_1" = 30) AND ("COL_2" '
                        'REGEXP \'[0-9]{3}\.[0-9]{3}\')',  # pylint: disable=W1401  # noqa: W605
                },
                {
                    'trans': '"COL_7" = CASE WHEN LENGTH("COL_7") > 2 * 3 THEN '
                        'CONCAT(SUBSTRING("COL_7", 1, 3), REPEAT(\'*\', LENGTH("COL_7")-(2 * 3)), '
                        'SUBSTRING("COL_7", LENGTH("COL_7")-3+1, 3)) '
                        'ELSE "COL_7" END',
                    'conditions': '("COL_1" = 30) AND ("COL_2" '
                        'REGEXP \'[0-9]{3}\.[0-9]{3}\') AND ("COL_4" IS NULL)',  # pylint: disable=W1401  # noqa: W605
                },
            ],
        )

    def test_get_trans_in_sql_case7(self):
        """
        Test obfuscation where given transformations have conditions and sql flavor is Postgres
        Test should pass
        """
        table_name = 'public-my_table'

        transformations = [
            {
                'field_id': 'col_1',
                'tap_stream_name': 'public-my_table',
                'type': 'SET-NULL',
            },
            {
                'field_id': 'col_2',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-HIDDEN',
                'when': [
                    {'column': 'col_4', 'safe_column': '"COL_4"', 'equals': None},
                    {
                        'column': 'col_1',
                    },
                ],
            },
            {
                'field_id': 'col_3',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-DATE',
                'when': [{'column': 'col_5', 'equals': 'some_value'}],
            },
            {
                'field_id': 'col_4',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-NUMBER',
            },
            {'field_id': 'col_5', 'tap_stream_name': 'public-my_table', 'type': 'HASH'},
            {
                'field_id': 'col_10',
                'tap_stream_name': 'public-my_other_table',
                'type': 'HASH',
            },
            {
                'field_id': 'col_6',
                'tap_stream_name': 'public-my_table',
                'type': 'HASH-SKIP-FIRST-5',
                'when': [
                    {'column': 'col_1', 'equals': 30},
                    {'column': 'col_2', 'regex_match': r'[0-9]{3}\.[0-9]{3}'},
                ],
            },
            {
                'field_id': 'col_7',
                'tap_stream_name': 'public-my_table',
                'type': 'MASK-STRING-SKIP-ENDS-3',
                'when': [
                    {'column': 'col_1', 'equals': 30},
                    {'column': 'col_2', 'regex_match': r'[0-9]{3}\.[0-9]{3}'},
                    {'column': 'col_4', 'equals': None},
                ],
            },
        ]

        trans = TransformationHelper.get_trans_in_sql_flavor(
            table_name, transformations, SQLFlavor('postgres')
        )

        self.assertListEqual(
            trans,
            [
                {
                    'trans': '"col_1" = NULL',
                    'conditions': None,
                },
                {
                    'trans': '"col_2" = \'hidden\'',
                    'conditions': '("col_4" IS NULL)',
                },
                {
                    'trans': '"col_3" = MAKE_TIMESTAMP('
                    'DATE_PART(\'year\', "col_3")::int, '
                    '1, '
                    '1, '
                    'DATE_PART(\'hour\', "col_3")::int, '
                    'DATE_PART(\'minute\', "col_3")::int, '
                    'DATE_PART(\'second\', "col_3")::double precision'
                    ')',
                    'conditions': '("col_5" = \'some_value\')',
                },
                {
                    'trans': '"col_4" = 0',
                    'conditions': None,
                },
                {
                    'trans': '"col_5" = ENCODE(DIGEST("col_5", \'sha256\'), \'hex\')',
                    'conditions': None,
                },
                {
                    'trans': '"col_6" = CONCAT(SUBSTRING("col_6", 1, 5), ENCODE(DIGEST(SUBSTRING("col_6", 5 + 1), '
                    '\'sha256\'), \'hex\'))',
                    'conditions': '("col_1" = 30) AND ("col_2" ~ \'[0-9]{3}\.[0-9]{3}\')',  # pylint: disable=W1401  # noqa: W605, E501
                },
                {
                    'trans': '"col_7" = CASE WHEN LENGTH("col_7") > 2 * 3 THEN '
                        'CONCAT(SUBSTRING("col_7", 1, 3), REPEAT(\'*\', LENGTH("col_7")-(2 * 3)), '
                        'SUBSTRING("col_7", LENGTH("col_7")-3+1, 3)) '
                        'ELSE "col_7" END',
                    'conditions': '("col_1" = 30) AND ("col_2" ~ \'[0-9]{3}\.[0-9]{3}\') '  # pylint: disable=W1401  # noqa: W605, E501
                        'AND ("col_4" IS NULL)',
                },
            ],
        )
