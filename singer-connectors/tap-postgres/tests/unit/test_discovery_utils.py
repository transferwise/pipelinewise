import unittest

from tap_postgres import discovery_utils


def _column(sql_data_type, is_primary_key=False):
    return discovery_utils.Column(
        column_name='test_column',
        is_primary_key=is_primary_key,
        sql_data_type=sql_data_type,
        character_maximum_length=None,
        numeric_precision=None,
        numeric_scale=None,
        is_array=False,
        is_enum=False,
    )


class TestSchemaForColumnDatatype(unittest.TestCase):
    maxDiff = None

    def test_ltree_maps_to_string(self):
        """ltree columns are discovered as nullable strings"""
        schema = discovery_utils.schema_for_column_datatype(_column('ltree'))
        self.assertEqual(schema, {'type': ['null', 'string']})

    def test_interval_maps_to_string(self):
        """interval columns are discovered as nullable strings"""
        schema = discovery_utils.schema_for_column_datatype(_column('interval'))
        self.assertEqual(schema, {'type': ['null', 'string']})

    def test_ltree_primary_key_is_not_nullable(self):
        """A primary-key ltree column is a non-nullable string"""
        schema = discovery_utils.schema_for_column_datatype(_column('ltree', is_primary_key=True))
        self.assertEqual(schema, {'type': ['string']})

    def test_interval_primary_key_is_not_nullable(self):
        """A primary-key interval column is a non-nullable string"""
        schema = discovery_utils.schema_for_column_datatype(_column('interval', is_primary_key=True))
        self.assertEqual(schema, {'type': ['string']})

    def test_interval_array_maps_to_string(self):
        """Array notation is stripped, so interval[] maps the same as interval"""
        schema = discovery_utils.schema_for_column_datatype(_column('interval[]'))
        self.assertEqual(schema, {'type': ['null', 'string']})
