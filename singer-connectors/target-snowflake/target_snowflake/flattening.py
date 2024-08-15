import collections
import inflection
import itertools
import json
import re


def flatten_key(k, parent_key, sep):
    """

    Params:
        k:
        parent_key:
        sep:

    Returns:
    """
    full_key = parent_key + [k]
    inflected_key = full_key.copy()
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 255 and reducer_index < len(inflected_key):
        reduced_key = re.sub(r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)


# pylint: disable=invalid-name
def flatten_schema(d, parent_key=None, sep='__', level=0, max_level=0):
    """

    Params:
        k:
        parent_key:
        sep:

    Returns:
    """
    if parent_key is None:
        parent_key = []

    items = []
    if 'properties' not in d:
        return {}

    for k, v in d['properties'].items():
        new_key = flatten_key(k, parent_key, sep)
        if 'type' in v.keys():
            if 'object' in v['type'] and 'properties' in v and level < max_level:
                items.extend(flatten_schema(v, parent_key + [k], sep=sep, level=level + 1, max_level=max_level).items())
            else:
                items.append((new_key, v))
        elif len(v.values()) > 0:
            value_type = list(v.values())[0][0]['type']
            if value_type in ['string', 'array', 'object']:
                list(v.values())[0][0]['type'] = ['null', value_type]
                items.append((new_key, list(v.values())[0][0]))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError(f'Duplicate column name produced in schema: {k}')

    return dict(sorted_items)


def _should_json_dump_value(key, value, schema=None):
    """

    Params:
        k:
        parent_key:
        sep:

    Returns:
    """
    if isinstance(value, (dict, list)):
        return True

    if schema and key in schema and 'type' in schema[key] and set(
            schema[key]['type']) == {'null', 'object', 'array'}:
        return True

    return False


# pylint: disable-msg=invalid-name
def flatten_record(d, schema=None, parent_key=None, sep='__', level=0, max_level=0):
    """

    Params:
        k:
        parent_key:
        sep:

    Returns:
    """
    if parent_key is None:
        parent_key = []

    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.abc.MutableMapping) and level < max_level:
            items.extend(flatten_record(v, schema, parent_key + [k], sep=sep, level=level + 1,
                                        max_level=max_level).items())
        else:
            items.append((new_key, json.dumps(v) if _should_json_dump_value(k, v, schema) else v))

    return dict(items)
