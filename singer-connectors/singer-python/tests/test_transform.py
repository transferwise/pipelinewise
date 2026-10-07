import unittest
from singer import transform
from singer.transform import (
    NO_INTEGER_DATETIME_PARSING,
    UNIX_SECONDS_INTEGER_DATETIME_PARSING,
    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
    Transformer,
    resolve_schema_references,
    unix_seconds_to_datetime,
    unix_milliseconds_to_datetime,
)


class TestTransform(unittest.TestCase):
    def test_integer_transform(self):
        schema = {'type': 'integer'}
        self.assertEqual(123, transform(123, schema))
        self.assertEqual(123, transform('123', schema))
        self.assertEqual(1234, transform('1,234', schema))

    def test_nested_transform(self):
        schema =  {'type': 'object',
                   'properties': {'addrs': {'type': 'array',
                                            'items': {'type': 'object',
                                                      'properties': {'addr1': {'type': 'string'},
                                                                     'city': {'type': 'string'},
                                                                     'state': {'type': 'string'},
                                                                     'amount': {'type': 'integer'}}}}}}
        data = {'addrs': [{'amount': '123'}, {'amount': '456'}]}
        expected = {'addrs': [{'amount': 123}, {'amount': 456}]}
        self.assertDictEqual(expected, transform(data, schema))

    def test_multi_type_object_transform(self):
        schema =  {'type': ['null', 'object', 'string'],
                   'properties': {'whatever': {'type': 'date-time',
                                               'format': 'date-time'}}}
        data = {'whatever': '2017-01-01'}
        expected = {'whatever': '2017-01-01T00:00:00.000000Z'}
        self.assertDictEqual(expected, transform(data, schema))
        data = 'justastring'
        expected = 'justastring'
        self.assertEqual(expected, transform(data, schema))

    def test_multi_type_array_transform(self):
        schema =  {'type': ['null', 'array', 'integer'],
                   'items': {'type': 'date-time', 'format': 'date-time'}}
        data = ['2017-01-01']
        expected = ['2017-01-01T00:00:00.000000Z']
        self.assertEqual(expected, transform(data, schema))
        data = 23
        expected = 23
        self.assertEqual(expected, transform(data, schema))

    def test_null_transform(self):
        self.assertEqual('', transform('', {'type': ['null', 'string']}))
        self.assertEqual('', transform('', {'type': [ 'string', 'null']}))
        self.assertEqual(None, transform(None, {'type': [ 'string', 'null']}))
        self.assertEqual(None, transform('', {'type': ['null']}))
        self.assertEqual(None, transform(None, {'type': ['null']}))

    def test_datetime_transform(self):
        schema = {'type': 'string', 'format': 'date-time'}
        string_datetime = '2017-01-01T00:00:00Z'
        transformed_string_datetime = '2017-01-01T00:00:00.000000Z'
        self.assertEqual(transformed_string_datetime, transform(string_datetime, schema, NO_INTEGER_DATETIME_PARSING))
        self.assertEqual('1970-01-02T00:00:00.000000Z', transform(86400, schema, UNIX_SECONDS_INTEGER_DATETIME_PARSING))
        self.assertEqual(
            transformed_string_datetime, transform(string_datetime, schema, UNIX_SECONDS_INTEGER_DATETIME_PARSING))
        self.assertEqual(
            '1970-01-01T00:01:26.400000Z', transform(86400, schema, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING))
        self.assertEqual(
            transformed_string_datetime, transform(string_datetime, schema, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING))

        trans = Transformer(NO_INTEGER_DATETIME_PARSING)
        self.assertIsNone(trans._transform_datetime('cat'))
        self.assertIsNone(trans._transform_datetime(0))

        trans.integer_datetime_fmt = UNIX_SECONDS_INTEGER_DATETIME_PARSING
        self.assertIsNone(trans._transform_datetime('cat'))

    def test_datetime_string_with_timezone(self):
        schema = {'type': 'string', 'format': 'date-time'}
        string_datetime = '2017-03-18T07:00:05-0700'
        transformed_string_datetime = '2017-03-18T14:00:05.000000Z'
        self.assertEqual(transformed_string_datetime, transform(string_datetime, schema))

    def test_datetime_fractional_seconds_transform(self):
        schema = {'type': 'string', 'format': 'date-time'}
        string_datetime = '2017-01-01T00:00:00.123000Z'
        self.assertEqual(string_datetime, transform(string_datetime, schema, NO_INTEGER_DATETIME_PARSING))

    def test_anyof_datetime(self):
        schema = {'anyOf': [{'type': 'null'}, {'format': 'date-time', 'type': 'string'}]}
        string_datetime = '2016-03-10T18:47:20Z'
        transformed_string_datetime = '2016-03-10T18:47:20.000000Z'
        self.assertEqual(transformed_string_datetime, transform(string_datetime, schema))
        self.assertIsNone(transform(None, schema))

    def test_error_path(self):
        schema = {'type': 'object',
                  'properties': {'foo': {'type': 'integer'},
                                 'baz': {'type': 'integer'}}}
        data = {'foo': 'bar', 'baz': 1}
        trans = Transformer(NO_INTEGER_DATETIME_PARSING)
        success, data = trans.transform_recur(data, schema, [])
        self.assertFalse(success)
        self.assertIsNone(data)
        self.assertListEqual([[], ['foo']], sorted(e.path for e in trans.errors))

    def test_nested_error_path_throws(self):
        schema = {
            'type': 'object',
            'properties': {
                'key1': {
                    'type': 'object',
                    'properties': {
                        'key2': {
                            'type': 'object',
                            'properties': {
                                'key3': {
                                    'type': 'object',
                                    'properties': {
                                        'key4': {'type': 'integer'},
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
        data = {'key1': {'key2': {'key3': {'key4': 'not an integer'}}}}
        trans = Transformer()
        success, _ = trans.transform_recur(data, schema, [])
        self.assertFalse(success)
        expected = [
            [],
            ['key1'],
            ['key1', 'key2'],
            ['key1', 'key2', 'key3'],
            ['key1', 'key2', 'key3', 'key4'],
        ]
        self.assertListEqual(expected, sorted(e.path for e in trans.errors))

    def test_nested_error_path_no_throw(self):
        schema = {
            'type': 'object',
            'properties': {
                'key1': {
                    'type': 'object',
                    'properties': {
                        'key2': {
                            'type': 'object',
                            'properties': {
                                'key3': {
                                    'type': 'object',
                                    'properties': {
                                        'key4': {'type': 'string'},
                                        'key5': {'type': 'string'},
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
        data = {'key1': {'key2': {'key3': {'key4': None, 'key5': None}}}}
        trans = Transformer()
        success, data = trans.transform_recur(data, schema, [])
        self.assertFalse(success)
        self.assertIsNone(data)
        expected = [
            [],
            ['key1'],
            ['key1', 'key2'],
            ['key1', 'key2', 'key3'],
            ['key1', 'key2', 'key3', 'key4'],
            ['key1', 'key2', 'key3', 'key5'],
        ]
        self.assertListEqual(expected, sorted(e.path for e in trans.errors))

    def test_error_path_array(self):
        schema =  {'type': 'object',
                   'properties': {'integers': {'type': 'array',
                                               'items': {'type': 'integer'}}}}
        data = {'integers': [1, 2, 'not an integer', 4, 'also not an integer']}
        trans = Transformer()
        success, data = trans.transform_recur(data, schema, [])
        self.assertFalse(success)
        expected = [
            [],
            ['integers'],
            ['integers', 2],
            ['integers', 4],
        ]
        self.assertListEqual(expected, sorted(e.path for e in trans.errors))

    def test_nested_error_path_array(self):
        schema =  {'type': 'object',
                   'properties': {'lists_of_integers': {'type': 'array',
                                                        'items': {'type': 'array',
                                                                  'items': {'type': 'integer'}}}}}
        data = {'lists_of_integers': [[1, 'not an integer'], [2, 3], ['also not an integer', 4]]}
        trans = Transformer()
        success, transformed_data = trans.transform_recur(data, schema, [])
        self.assertFalse(success)
        expected = [
            [],
            ['lists_of_integers'],
            ['lists_of_integers', 0],
            ['lists_of_integers', 0, 1],
            ['lists_of_integers', 2],
            ['lists_of_integers', 2, 0],
        ]
        self.assertListEqual(expected, sorted(e.path for e in trans.errors))

    def test_error_path_datetime(self):
        schema = {'type': 'object',
                  'properties': {'good_datetime': {'type': 'string', 'format': 'date-time'},
                                 'bad_datetime1': {'type': 'string', 'format': 'date-time'},
                                 'bad_datetime2': {'type': 'string', 'format': 'date-time'}}}
        data = {'good_datetime': '2017-04-11T16:07:00Z',
                'bad_datetime1': 'not a datetime',
                'bad_datetime2': 1}
        trans = Transformer()
        success, transformed_data = trans.transform_recur(data, schema, [])
        self.assertFalse(success)
        expected = [
            [],
            ['bad_datetime1'],
            ['bad_datetime2'],
        ]
        self.assertListEqual(expected, sorted(e.path for e in trans.errors))

    def test_unexpected_object_properties(self):
        schema = {'type': 'object',
                  'properties': {'good_property': {'type': 'string'}}}
        data = {'good_property': 'expected data',
                'bad_property': 'unexpected data'}
        trans = Transformer()
        success, transformed_data = trans.transform_recur(data, schema, [])
        self.assertTrue(success)
        self.assertDictEqual({'good_property': 'expected data'}, transformed_data)
        self.assertSetEqual(set(['bad_property']), trans.removed)
        self.assertListEqual([], trans.errors)

    def test_unix_seconds_to_datetime(self):
        self.assertEqual(unix_seconds_to_datetime(0), '1970-01-01T00:00:00.000000Z')
        self.assertEqual(unix_seconds_to_datetime(1502722441), '2017-08-14T14:54:01.000000Z')

    def test_unix_milliseconds_to_datetime(self):
        self.assertEqual(unix_milliseconds_to_datetime(0), '1970-01-01T00:00:00.000000Z')
        self.assertEqual(unix_milliseconds_to_datetime(1502722441000), '2017-08-14T14:54:01.000000Z')

    def test_null_object_transform(self):
        schema =  {'type': 'object',
                   'properties': {'addrs': {'type': ['null', 'object'],
                                            'properties': {'city': {'type': 'string'}}}}}
        none_data = {'addrs': None}
        self.assertDictEqual(none_data, transform(none_data, schema))
        empty_data = {'addrs': {}}
        self.assertDictEqual(empty_data, transform(empty_data, schema))


class TestTransformsWithMetadata(unittest.TestCase):

    def test_drops_no_data_when_not_dict(self):
        schema = {'type': 'string'}
        metadata = {}
        string_value = 'hello'
        self.assertEqual(string_value, transform(string_value, schema, NO_INTEGER_DATETIME_PARSING, metadata=metadata))

    def test_keeps_selected_data_from_dicts(self):
        schema = {'type': 'object',
                  'properties': { 'name': {'type': 'string'}}}
        metadata = {('properties','name'): {'selected': True}}
        dict_value = {'name': 'chicken'}
        self.assertEqual(
            {'name': 'chicken'}, transform(dict_value, schema, NO_INTEGER_DATETIME_PARSING, metadata=metadata))

    def test_keeps_automatic_data_from_dicts(self):
        schema = {'type': 'object',
                  'properties': { 'name': {'type': 'string'}}}
        metadata = {('properties','name'): {'inclusion': 'automatic'}}
        dict_value = {'name': 'chicken'}
        self.assertEqual(
            {'name': 'chicken'}, transform(dict_value, schema, NO_INTEGER_DATETIME_PARSING, metadata=metadata))

    def test_keeps_fields_without_metadata(self):
        schema = {'type': 'object',
                  'properties': { 'name': {'type': 'string'}}}
        metadata = {('properties','age'): {'inclusion': 'automatic'}}
        dict_value = {'name': 'chicken'}
        self.assertEqual(
            {'name': 'chicken'}, transform(dict_value, schema, NO_INTEGER_DATETIME_PARSING, metadata=metadata))

    def test_drops_fields_which_are_unselected(self):
        schema = {'type': 'object',
                  'properties': { 'name': {'type': 'string'}}}
        metadata = {('properties','name'): {'selected': False}}
        dict_value = {'name': 'chicken'}
        self.assertEqual({}, transform(dict_value, schema, NO_INTEGER_DATETIME_PARSING, metadata=metadata))

    def test_drops_fields_which_are_unsupported(self):
        schema = {'type': 'object',
                  'properties': { 'name': {'type': 'string'}}}
        metadata = {('properties','name'): {'inclusion': 'unsupported'}}
        dict_value = {'name': 'chicken'}
        self.assertEqual({}, transform(dict_value, schema, NO_INTEGER_DATETIME_PARSING, metadata=metadata))


class TestResolveSchemaReferences(unittest.TestCase):
    def test_internal_refs_resolve(self):
        schema =  {'type': 'object',
                   'definitions': { 'string_type': {'type': 'string'}},
                   'properties': { 'name': {'$ref': '#/definitions/string_type'}}}
        result = resolve_schema_references(schema)
        self.assertEqual(result['properties']['name']['type'], 'string')

    def test_external_refs_resolve(self):
        schema =  {'type': 'object',
                   'properties': { 'name': {'$ref': 'references.json#/definitions/string_type'}}}
        refs =  {'references.json': {'definitions': { 'string_type': {'type': 'string'}}}}
        result = resolve_schema_references(schema, refs)
        self.assertEqual(result['properties']['name']['type'], 'string')

    def test_refs_resolve_pattern_properties(self):
        schema =  {'type': 'object',
                   'definitions': { 'string_type': {'type': 'string'}},
                   'patternProperties': {'.+': {'$ref': '#/definitions/string_type'}}}
        result = resolve_schema_references(schema)
        self.assertEqual(result['patternProperties']['.+']['type'], 'string')

    def test_refs_resolve_items(self):
        schema =  {'type': 'object',
                   'properties': { 'dogs':
                                   {'type': 'array',
                                    'items': {'$ref': 'doggie.json#/dogs'}}}}
        refs =  {'doggie.json': {'dogs': {
                                   'type': 'object',
                                   'properties': {
                                     'breed': {
                                       'type': 'string'
                                     },
                                     'name': {
                                       'type': 'string'}}}}}
        result = resolve_schema_references(schema, refs)
        self.assertEqual(result['properties']['dogs']['items']['properties']['breed'], {'type': 'string'})

    def test_refs_resolve_nested(self):
        schema = {'type': 'object',
                   'properties': {
                       'thing': {
                           'type': 'object',
                           'properties': {
                               'name': {'$ref': 'references.json#/definitions/string_type'}}}}}
        refs = {'references.json': {'definitions': { 'string_type': {'type': 'string'}}}}
        result = resolve_schema_references(schema, refs)
        self.assertEqual(result['properties']['thing']['properties']['name']['type'], 'string')

    def test_indirect_reference(self):
        schema =  {'type': 'object',
                   'properties': { 'name': {'$ref': 'references.json#/definitions/string_type'}}}
        refs =  {'references.json': {'definitions': { 'string_type': {'$ref': 'second_reference.json'}}},
                 'second_reference.json': {'type': 'string'}}
        result = resolve_schema_references(schema, refs)
        self.assertEqual(result['properties']['name']['type'], 'string')

    def test_refs_resolve_preserves_existing_fields(self):
        schema =  {'type': 'object',
                   'properties': { 'name': {'$ref': 'references.json#/definitions/string_type',
                                            'still_here': 'yep'}}}
        refs =  {'references.json': {'definitions': { 'string_type': {'type': 'string'}}}}
        result = resolve_schema_references(schema, refs)
        self.assertEqual(result['properties']['name']['type'], 'string')
        self.assertEqual(result['properties']['name']['still_here'], 'yep')


class TestPatternProperties(unittest.TestCase):
    def test_pattern_properties_match(self):
        schema = {'type': 'object',
                  'patternProperties': { '.+': {'type': 'string'}}}
        dict_value = {'name': 'chicken', 'unit_cost': '1.45', 'SKU': '123456'}
        expected = dict(dict_value)
        self.assertEqual(expected, transform(dict_value, schema))

    def test_pattern_properties_match_multiple(self):
        schema = {'type': 'object',
                  'patternProperties': { '.+?cost': {'type': 'number'},
                                         '.+(?<!cost)$': {'type': 'string'}}}
        dict_value = {'name': 'chicken', 'unit_cost': 1.45, 'SKU': '123456'}
        expected = dict(dict_value)
        self.assertEqual(expected, transform(dict_value, schema))
