import unittest
from unittest.mock import patch

from singer import Catalog, Schema
from transform_field.errors import CatalogRequiredException, StreamNotFoundException, NoStreamSchemaException, \
    UnsupportedTransformationTypeException, InvalidTransformationException

from transform_field import TransformField, TransMeta


class TestTransformField(unittest.TestCase):
    """
    Unit Tests for the TransformField class
    """

    def setUp(self) -> None:
        self.config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "SET-NULL"
                },
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_2",
                    "type": "HASH",
                    "when": []
                },
                {
                    "tap_stream_name": "stream_2",
                    "field_id": "column_1",
                    "type": "MASK-DATE"
                },
            ]
        }

    def test_init(self):
        instance = TransformField(self.config)

        self.assertListEqual(instance.messages, [])
        self.assertEqual(instance.buffer_size_bytes, 0)
        self.assertIsNone(instance.state)
        self.assertIsNotNone(instance.time_last_batch_sent)
        self.assertDictEqual(instance.trans_config, self.config)
        self.assertDictEqual(instance.stream_meta, {})
        self.assertDictEqual(instance.trans_meta, {
            'stream_1': [
                TransMeta('column_1', 'SET-NULL', None, None),
                TransMeta('column_2', 'HASH', [], None),
            ],
            'stream_2': [TransMeta('column_1', 'MASK-DATE', None, None)],
        })

    def test_validate_without_catalog_fails(self):
        with self.assertRaises(CatalogRequiredException):
            TransformField(self.config).validate(None)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_missing_stream_fails(self, get_stream_schemas_mock):
        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_2': {'something'}
        }
        with self.assertRaises(StreamNotFoundException):
            TransformField(self.config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_empty_stream_schema_fails(self, get_stream_schemas_mock):
        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': {},
            'stream_2': {'something'}
        }
        with self.assertRaises(NoStreamSchemaException):
            TransformField(self.config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_unsupported_trans_type(self, get_stream_schemas_mock):
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "SET-RANDOM"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'string'
                    ]
                }
            }})
        }
        with self.assertRaises(UnsupportedTransformationTypeException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_set_null_trans_type_success(self, get_stream_schemas_mock):
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "SET-NULL"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'string'
                    ]
                }
            }})
        }
        TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_hash_fails_1(self, get_stream_schemas_mock):
        """
        Testing validation of HASH transformation when field has no type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "HASH"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {}
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_hash_fails_2(self, get_stream_schemas_mock):
        """
        Testing validation of HASH transformation when field has non-string type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "HASH"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'integer'
                    ]
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_hash_fails_3(self, get_stream_schemas_mock):
        """
        Testing validation of HASH transformation when field has string type but formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "HASH"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ],
                    'format': 'binary'
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_hash_success(self, get_stream_schemas_mock):
        """
        Testing validation of HASH transformation when field has string type but no format
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "HASH"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ]
                }
            }})
        }
        TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_hash_skip_first_fails_1(self, get_stream_schemas_mock):
        """
        Testing validation of HASH-SKIP-FIRST transformation when field has no type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "HASH-SKIP-FIRST-1"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {}
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_hash_skip_first_fails_2(self, get_stream_schemas_mock):
        """
        Testing validation of HASH-SKIP-FIRST transformation when field has non-string type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "HASH-SKIP-FIRST-1"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'integer'
                    ]
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_hash_skip_first_fails_3(self, get_stream_schemas_mock):
        """
        Testing validation of HASH-SKIP-FIRST-1 transformation when field has string type but formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "HASH-SKIP-FIRST-1"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ],
                    'format': 'binary'
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_hash_skip_first_success(self, get_stream_schemas_mock):
        """
        Testing validation of HASH-SKIP-FIRST-1 transformation when field has string type but not formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "HASH-SKIP-FIRST-1"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ]
                }
            }})
        }
        TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_hidden_fails_1(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-HIDDEN transformation when field has no type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-HIDDEN"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {}
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_hidden_fails_2(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-HIDDEN transformation when field has non-string type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-HIDDEN"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'integer'
                    ]
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_hidden_fails_3(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-HIDDEN transformation when field has string type but formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-HIDDEN"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ],
                    'format': 'binary'
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_hidden_success(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-HIDDEN transformation when field has string type but not formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-HIDDEN"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ]
                }
            }})
        }
        TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_date_fails_1(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-DATE transformation when field has no type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-DATE"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {}
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_date_fails_2(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-DATE transformation when field has string type but no format
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-DATE"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ]
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_date_fails_3(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-DATE transformation when field has non-string type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-DATE"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'integer'
                    ]
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_date_fails_4(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-DATE transformation when field has string type but not date formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-DATE"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ],
                    'format': 'binary'
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_date_success_1(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-DATE transformation when field has string type but is date formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-DATE"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ],
                    'format': 'date'
                }
            }})
        }
        TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_date_success_2(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-DATE transformation when field has string type but is date-time formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-DATE"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ],
                    'format': 'date-time'
                }
            }})
        }
        TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_number_fails_1(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-NUMBER transformation when field has no type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-NUMBER"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {}
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_number_fails_2(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-NUMBER transformation when field not have integer nor number type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-NUMBER"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ]
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_number_fails_3(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-NUMBER transformation when field has integer type but formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-NUMBER"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'integer'
                    ],
                    'format': 'something random'
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_number_fails_4(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-NUMBER transformation when field has number type but formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-DATE"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'number'
                    ],
                    'format': 'binary'
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_number_success_1(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-NUMBER transformation when field has integer type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-NUMBER"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'integer'
                    ]
                }
            }})
        }
        TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_number_success_2(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-NUMBER transformation when field has number type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-NUMBER"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'number'
                    ]
                }
            }})
        }
        TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_string_skip_ends_fails_1(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-STRING-SKIP-ENDS transformation when field has no type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-STRING-SKIP-ENDS-1"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {}
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_string_skip_ends_fails_2(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-STRING-SKIP-ENDS transformation when field has non-string type
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-STRING-SKIP-ENDS-1"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'integer'
                    ]
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_string_skip_ends_fails_3(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-STRING-SKIP-ENDS-1 transformation when field has string type but formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-STRING-SKIP-ENDS-1"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ],
                    'format': 'binary'
                }
            }})
        }
        with self.assertRaises(InvalidTransformationException):
            TransformField(config).validate(catalog)

    @patch('transform_field.utils.get_stream_schemas')
    def test_validate_with_mask_string_skip_ends_success(self, get_stream_schemas_mock):
        """
        Testing validation of MASK-STRING-SKIP-ENDS-1 transformation when field has string type but not formatted
        """
        config = {
            'transformations': [
                {
                    "tap_stream_name": "stream_1",
                    "field_id": "column_1",
                    "type": "MASK-STRING-SKIP-ENDS-1"
                },
            ]
        }

        catalog = Catalog.from_dict({'streams': []})

        get_stream_schemas_mock.return_value = {
            'stream_1': Schema.from_dict({'properties': {
                'column_1': {
                    'type': [
                        'null',
                        'string'
                    ]
                }
            }})
        }
        TransformField(config).validate(catalog)
