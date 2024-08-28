"""
Test transformations
"""
from tap_twilio.transform import deserialise_jsons_in_dict, transform_json


def test_deserialise_jsons_in_dict():
    """JSON strings in dictionary should be converted to objects"""
    dict_with_json_string = {
        'non-jsons': 1,
        'jsons': '{"key1": "value1"}'
    }
    assert deserialise_jsons_in_dict(dict_with_json_string, ['jsons']) == {
        'non-jsons': 1,
        'jsons': {
            'key1': 'value1'
        }
    }


def test_deserialise_jsons_in_dict_with_wrong_key():
    """JSON strings in dictionary should not be converted to objects if the key not exists"""
    dict_with_json_string = {
        'non-jsons': 1,
        'jsons': '{"key1": "value1"}'
    }
    assert deserialise_jsons_in_dict(dict_with_json_string, ['NOT_EXISTING_KEY']) == {
        'non-jsons': 1,
        'jsons': '{"key1": "value1"}'
    }


def test_deserialise_jsons_in_dict_with_invalid_json_string():
    """Invalid JSON strings in dictionary should be converted to objects with the original string"""
    dict_with_json_string = {
        'non-jsons': 1,
        'jsons': 'THIS IS AN INVALID JSON'
    }
    assert deserialise_jsons_in_dict(dict_with_json_string, ['jsons']) == {
        'non-jsons': 1,
        'jsons': {
            'invalid_json': 'THIS IS AN INVALID JSON'
        }
    }


def test_deserialise_jsons_in_dict_with_objects():
    """JSON string keys which already objects should remain objects"""
    dict_with_json_string = {
        'non-jsons': 1,
        'jsons': {'hey': 'this is already an object already'}
    }
    assert deserialise_jsons_in_dict(dict_with_json_string, ['jsons']) == {
        'non-jsons': 1,
        'jsons': {
            'hey': 'this is already an object already'
        }
    }


def test_deserialise_jsons_in_dict_with_multiple_keys():
    """JSON strings in dictionary should NOT be converted to objects if the key not exists"""
    dict_with_json_string = {
        'non-jsons': 1,
        'jsons_with_object': '{"key1": "value1"}',
        'jsons_with_array': '[1, 2, 3, 4, 5]'
    }
    assert deserialise_jsons_in_dict(dict_with_json_string, ['jsons_with_object', 'jsons_with_array']) == {
        'non-jsons': 1,
        'jsons_with_object': {
            'key1': 'value1'
        },
        'jsons_with_array': [1, 2, 3, 4, 5]
    }


def test_transform_list_of_dicts():
    """Every dictionary item should be transformed"""
    data_dict = {
        'meta': 'foo',
        'my_data': [
            {
                'key_1': 'value_1',
                'key_2': 'value_2',
            },
            {
                'key_1': 'value_3',
                'key_2': 'value_4',
            }
        ]
    }
    assert transform_json(data_dict, data_key='my_data', jsons_keys=None) == [
        {
            'key_1': 'value_1',
            'key_2': 'value_2',
        },
        {
            'key_1': 'value_3',
            'key_2': 'value_4',
        }
    ]


def test_transform_list_with_jsons():
    """JSON strings in every dictionary item should be transformed to objects"""
    data_dict = {
        'meta': 'foo',
        'my_data': [
            # Stringified object should be converted to object
            {
                'key1': '{"dummy": "foo1"}',
                'key2': 'value1',
            },
            # Stringified array should be converted to list
            {
                'key1': '[1, 2, 3, 4, 5]',
                'key2': 'value2',
            },
            # Objects should remain object
            {
                'key1': {"dummy": "foo3"},
                'key2': 'value3',
            },
            # List should remain list
            {
                'key1': [1, 2, 3, 4, 5],
                'key2': 'value4',
            },
            # Invalid JSON should be converted to valid JSON using a custom error structure
            {
                'key1': 'THIS IS AN INVALID JSON',
                'key2': 'value5',
            }
        ]
    }
    assert transform_json(data_dict, data_key='my_data', jsons_keys=['key1']) == [
        {
            'key1': {'dummy': 'foo1'},
            'key2': 'value1',
        },
        {
            'key1': [1, 2, 3, 4, 5],
            'key2': 'value2',
        },
        {
            'key1': {'dummy': 'foo3'},
            'key2': 'value3',
        },
        {
            'key1': [1, 2, 3, 4, 5],
            'key2': 'value4',
        },
        {
            'key1': {'invalid_json': 'THIS IS AN INVALID JSON'},
            'key2': 'value5',
        }
    ]


def test_transform_list_with_subresource_json():
    """JSON strings as sub resources should be transformed to objects"""
    data_dict = {
        'meta': 'foo',
        'my_data': [
            {
                'key_1': {
                    'toTransform': '{"dummy": "foo1"}',
                    'alsoToTransform': '[1, 2, 3, 4, 5]'
                },
                'key_2': 'value_2',
            }
        ]
    }
    assert transform_json(data_dict, data_key='my_data', jsons_keys=['toTransform', 'alsoToTransform']) == [
        {
            'key_1': {
                'toTransform': {'dummy': 'foo1'},
                'alsoToTransform': [1, 2, 3, 4, 5]
            },
            'key_2': 'value_2',
        }
    ]


def test_transform_dict_with_subresource_json():
    """JSON strings as sub resources should be transformed to objects"""
    data_dict = {
        'meta': 'foo',
        'my_data':
            {
                'key_1': {
                    'toTransform': '{"dummy": "foo1"}',
                    'alsoToTransform': '[1, 2, 3, 4, 5]'
                },
                'key_2': 'value_2',
            }

    }
    assert transform_json(data_dict, data_key='my_data', jsons_keys=['toTransform', 'alsoToTransform']) == \
        {
            'key_1': {
                'toTransform': {'dummy': 'foo1'},
                'alsoToTransform': [1, 2, 3, 4, 5]
            },
            'key_2': 'value_2',
        }
