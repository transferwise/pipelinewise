#!/usr/bin/env python3
import time
import singer
import json
import re
import inflection

from decimal import Decimal
from datetime import datetime
from collections.abc import MutableMapping

logger = singer.get_logger('target_s3_csv')


def validate_config(config):
    """Validates config"""
    errors = []
    required_config_keys = [
        's3_bucket'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    return errors


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into
    double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = { 'type': ['null', 'string'], 'format': 'date-time' }
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = { 'type': ['null', 'string'] }
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = { 'type': ['null', 'string'], 'format': 'date-time' }
    extended_schema_message['schema']['properties']['_sdc_primary_key'] = {'type': ['null', 'string'] }
    extended_schema_message['schema']['properties']['_sdc_received_at'] = { 'type': ['null', 'string'], 'format': 'date-time' }
    extended_schema_message['schema']['properties']['_sdc_sequence'] = {'type': ['integer'] }
    extended_schema_message['schema']['properties']['_sdc_table_version'] = {'type': ['null', 'string'] }

    return extended_schema_message


def add_metadata_values_to_record(record_message, schema_message):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    """
    extended_record = record_message['record']
    extended_record['_sdc_batched_at'] = datetime.now().isoformat()
    extended_record['_sdc_deleted_at'] = record_message.get('record', {}).get('_sdc_deleted_at')
    extended_record['_sdc_extracted_at'] = record_message.get('time_extracted')
    extended_record['_sdc_primary_key'] = schema_message.get('key_properties')
    extended_record['_sdc_received_at'] = datetime.now().isoformat()
    extended_record['_sdc_sequence'] = int(round(time.time() * 1000))
    extended_record['_sdc_table_version'] = record_message.get('version')

    return extended_record


def remove_metadata_values_from_record(record_message):
    """Removes every metadata _sdc column from a given record message
    """
    cleaned_record = record_message['record']
    cleaned_record.pop('_sdc_batched_at', None)
    cleaned_record.pop('_sdc_deleted_at', None)
    cleaned_record.pop('_sdc_extracted_at', None)
    cleaned_record.pop('_sdc_primary_key', None)
    cleaned_record.pop('_sdc_received_at', None)
    cleaned_record.pop('_sdc_sequence', None)
    cleaned_record.pop('_sdc_table_version', None)

    return cleaned_record


# pylint: disable=unnecessary-comprehension
def flatten_key(k, parent_key, sep):
    """
    """
    full_key = parent_key + [k]
    inflected_key = [n for n in full_key]
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 255 and reducer_index < len(inflected_key):
        reduced_key = re.sub(r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)


def flatten_record(d, parent_key=None, sep='__'):
    """
    """

    if parent_key is None:
        parent_key = []

    items = []
    for k in sorted(d.keys()):
        v = d[k]
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, MutableMapping):
            items.extend(flatten_record(v, parent_key + [k], sep=sep).items())
        else:
            items.append((new_key, json.dumps(v) if type(v) is list else v))
    return dict(items)


def get_target_key(message, prefix=None, timestamp=None, naming_convention=None):
    """Creates and returns an S3 key for the message"""
    if not naming_convention:
        naming_convention = '{stream}-{timestamp}.csv' # o['stream'] + '-' + now + '.csv'
    if not timestamp:
        timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
    key = naming_convention
    
    # replace simple tokens
    for k, v in {
        '{stream}': message['stream'],
        '{timestamp}': timestamp,
        '{date}': datetime.now().strftime('%Y-%m-%d')
    }.items():
        if k in key:
            key = key.replace(k, v)

    # replace dynamic tokens
    # todo: replace dynamic tokens such as {date(<format>)} with the date formatted as requested in <format>

    if prefix:
        filename = key.split('/')[-1]
        key = key.replace(filename, f'{prefix}{filename}')
    return key
