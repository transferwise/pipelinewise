import json

from tap_s3_csv.logger import LOGGER as logger
from voluptuous import Schema, Required, Any, Optional

CONFIG_CONTRACT = Schema({
    Required('aws_access_key_id'): str,
    Required('aws_secret_access_key'): str,
    Required('start_date'): str,
    Required('bucket'): str,
    Required('tables'): [{
        Required('name'): str,
        Required('pattern'): str,
        Required('key_properties'): [str],
        Required('format'): Any('csv', 'excel'),
        Optional('search_prefix'): str,
        Optional('field_names'): [str],
        Optional('worksheet_name'): str,
        Optional('schema_overrides'): {
            str: {
                Required('type'): Any(str, [str]),
                Required('_conversion_type'): Any('string',
                                                  'integer',
                                                  'number',
                                                  'date-time')
            }
        }
    }]
})


def load(filename):
    config = {}

    try:
        with open(filename) as handle:
            config = json.load(handle)
    except:
        logger.fatal("Failed to decode config file. Is it valid json?")
        raise RuntimeError

    CONFIG_CONTRACT(config)

    return config
