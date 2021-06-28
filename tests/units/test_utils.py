import unittest

from pipelinewise import utils


class TestUtils(unittest.TestCase):
    """
    Unit Tests for PipelineWise common utils
    """
    def test_safe_column_name_case_1(self):
        """
        Given an all lower case word would be wrapped in double quotes and capitalized
        """
        input_name = 'group'

        self.assertEqual('"GROUP"', utils.safe_column_name(input_name))

    def test_safe_column_name_case_2(self):
        """
        Given an all lower case word would be wrapped in backticks and capitalized
        """
        input_name = 'group'

        self.assertEqual('`GROUP`', utils.safe_column_name(input_name, '`'))

    def test_safe_column_name_case_3(self):
        """
        Given a mixed-case word would be wrapped in double quotes and capitalized
        """
        input_name = 'CA se'

        self.assertEqual('"CA SE"', utils.safe_column_name(input_name))

    def test_safe_column_name_case_4(self):
        """
        Given a mixed-case word would be wrapped in backticks and capitalized
        """
        input_name = 'CA se'

        self.assertEqual('`CA SE`', utils.safe_column_name(input_name, '`'))

    def test_safe_column_name_is_null(self):
        """
        Given a null word, we should get null back
        """
        input_name = None

        self.assertIsNone(utils.safe_column_name(input_name))

if __name__ == '__main__':
    unittest.main()
