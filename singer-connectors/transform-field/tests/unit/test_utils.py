import argparse
import unittest

from unittest.mock import patch
from singer import Catalog

from transform_field.utils import get_stream_schemas, parse_args


class TestUtils(unittest.TestCase):
    """
    Unit Tests for the utils
    """

    def test_get_stream_schemas(self):
        catalog = Catalog.from_dict({
            'streams': [
                {
                    'tap_stream_id': 'stream1',
                    'schema': {
                        'properties': {
                            'col_1': {}
                        }
                    },
                    'metadata': [
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'selected': True
                            }
                        }
                    ]
                },
                {
                    'tap_stream_id': 'stream2',
                    'schema': {
                        'properties': {
                            'col_2': {}
                        }
                    },
                    'metadata': [
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'selected': True
                            }
                        }
                    ]
                },
                {
                    'tap_stream_id': 'stream3',
                    'schema': {
                        'properties': {
                            'col_3': {}
                        }
                    },
                    'metadata': [
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'selected': False
                            }
                        }
                    ]
                }
            ]
        })

        output = get_stream_schemas(catalog)

        self.assertIn('stream1', output)
        self.assertIn('stream2', output)
        self.assertNotIn('stream3', output)

        self.assertEqual(len(output['stream1'].properties), 1)
        self.assertEqual(len(output['stream2'].properties), 1)

    @patch('transform_field.utils.Catalog.load')
    @patch('transform_field.utils.check_config')
    @patch('transform_field.utils.load_json')
    @patch('argparse.ArgumentParser.parse_args')
    def test_parse_args(self, parse_args_mock, load_json_mock, check_config_mock, catalog_load_mock):
        """
        test args parsing
        """
        check_config_mock.return_value = None
        load_json_mock.return_value = {}
        catalog_load_mock.return_value = {}

        parse_args_mock.return_value = argparse.Namespace(**{
            'config': './config.json',
            'catalog': './properties.json',
            'validate': False,
        })

        args = parse_args({'transformations'})

        load_json_mock.assert_called_once()
        catalog_load_mock.assert_called_once()
        check_config_mock.assert_called_once()

        self.assertEqual(args.config, {})
        self.assertEqual(args.catalog, {})
        self.assertEqual(args.validate, False)
