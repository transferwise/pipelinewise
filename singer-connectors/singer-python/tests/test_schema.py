import unittest

from singer.schema import Schema


class TestSchema(unittest.TestCase):

    # Raw data structures for several schema types
    string_dict = {
        'type': 'string',
        'maxLength': 32
    }

    integer_dict = {
        'type': 'integer',
        'maximum': 1000000
    }

    array_dict = {
        'type': 'array',
        'items': integer_dict
    }

    object_dict = {
        'type': 'object',
        'properties': {
            'a_string': string_dict,
            'an_array': array_dict
        },
        'inclusion': 'whatever',
        'additionalProperties': True,
    }

    # Schema object forms of the same schemas as above
    string_obj = Schema(type='string', maxLength=32)

    integer_obj = Schema(type='integer', maximum=1000000)

    array_obj = Schema(type='array', items=integer_obj)

    object_obj = Schema(type='object',
                        properties={'a_string': string_obj,
                                    'an_array': array_obj},
                        inclusion='whatever',
                        additionalProperties=True)

    def test_to_string(self):
        self.assertEqual('{"maxLength":32,"type":"string"}', str(self.string_obj))

    def test_string_to_dict(self):
        self.assertEqual(self.string_dict, self.string_obj.to_dict())

    def test_integer_to_dict(self):
        self.assertEqual(self.integer_dict, self.integer_obj.to_dict())

    def test_array_to_dict(self):
        self.assertEqual(self.array_dict, self.array_obj.to_dict())

    def test_object_to_dict(self):
        self.assertEqual(self.object_dict, self.object_obj.to_dict())

    def test_string_from_dict(self):
        self.assertEqual(self.string_obj, Schema.from_dict(self.string_dict))

    def test_integer_from_dict(self):
        self.assertEqual(self.integer_obj, Schema.from_dict(self.integer_dict))

    def test_array_from_dict(self):
        self.assertEqual(self.array_obj, Schema.from_dict(self.array_dict))

    def test_object_from_dict(self):
        self.assertEqual(self.object_obj, Schema.from_dict(self.object_dict))

    def test_repr_atomic(self):
        self.assertEqual(self.string_obj, eval(repr(self.string_obj)))

    def test_repr_recursive(self):
        self.assertEqual(self.object_obj, eval(repr(self.object_obj)))

    def test_object_from_dict_with_defaults(self):
        schema = Schema.from_dict(self.object_dict, inclusion='automatic')
        self.assertEqual('whatever', schema.inclusion,
                          msg='The schema value should override the default')
        self.assertEqual('automatic', schema.properties['a_string'].inclusion)
        self.assertEqual('automatic', schema.properties['an_array'].items.inclusion)
