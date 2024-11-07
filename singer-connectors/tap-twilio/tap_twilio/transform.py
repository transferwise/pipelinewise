import json
import singer

LOGGER = singer.get_logger()


def subresources_to_array(data_dict, data_key):
    new_dict = data_dict
    i = 0
    for record in data_dict[data_key]:
        subresources = record.get('subresource_uris', None) if isinstance(record, dict) else None
        if subresources:
            new_dict[data_key][i]['_subresource_uris'] = record.get('subresource_uris')
            subresource_mappings = []
            for subresource_name, subresource_uri in subresources.items():
                subresource_mappings.append(
                    {'subresource': subresource_name, 'uri': subresource_uri})
            new_dict[data_key][i]['subresource_uris'] = subresource_mappings
        i = i + 1

    return new_dict


def deserialise_jsons_in_dict(data_dict, jsons_keys):
    if isinstance(data_dict, dict):
        # Recursive deserialize JSON string child items
        for key in data_dict.keys():
            data_dict[key] = deserialise_jsons_in_dict(data_dict[key], jsons_keys)

        for jsons_key in jsons_keys:
            if jsons_key in data_dict:
                # Deserialise only string types
                if isinstance(data_dict[jsons_key], str):
                    try:
                        data_dict[jsons_key] = json.loads(data_dict[jsons_key])
                    except json.JSONDecodeError:
                        data_dict[jsons_key] = {'invalid_json': data_dict[jsons_key]}

    return data_dict


def deserialise_jsons(data, jsons_keys):
    if jsons_keys:
        if isinstance(data, list):
            data = [deserialise_jsons_in_dict(data_dict, jsons_keys) for data_dict in data]
        elif isinstance(data, dict):
            data = deserialise_jsons_in_dict(data, jsons_keys)

    return data


# Run all transforms: ...
def transform_json(data_dict, data_key, jsons_keys):
    transformed_dict = subresources_to_array(data_dict, data_key)
    transformed_dict_data = deserialise_jsons(transformed_dict[data_key], jsons_keys)

    return transformed_dict_data
