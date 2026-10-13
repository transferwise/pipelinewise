import datetime
from .decimal_support import decimal_to_string, is_decimal_schema
import logging
import re
from jsonschema import RefResolver

import singer.metadata
from singer.logger import get_logger
from singer.utils import (strftime, strptime_to_utc)

LOGGER = get_logger()

NO_INTEGER_DATETIME_PARSING = 'no-integer-datetime-parsing'
UNIX_SECONDS_INTEGER_DATETIME_PARSING = 'unix-seconds-integer-datetime-parsing'
UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING = 'unix-milliseconds-integer-datetime-parsing'

VALID_DATETIME_FORMATS = [
    NO_INTEGER_DATETIME_PARSING,
    UNIX_SECONDS_INTEGER_DATETIME_PARSING,
    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
]


def string_to_datetime(value):
    try:
        return strftime(strptime_to_utc(value))
    except Exception as ex:
        LOGGER.warning('%s, (%s)', ex, value)
        return None


def unix_milliseconds_to_datetime(value):
    return strftime(datetime.datetime.fromtimestamp(float(value) / 1000.0, datetime.timezone.utc))


def unix_seconds_to_datetime(value):
    return strftime(datetime.datetime.fromtimestamp(int(value), datetime.timezone.utc))


class SchemaMismatch(Exception):
    def __init__(self, errors):
        if not errors:
            msg = 'An error occured during transform that was not a schema mismatch'

        else:
            estrs = [e.tostr() for e in errors]
            msg = f'Errors during transform:{", ".join(estrs)}'

        super().__init__(msg)


class SchemaKey:
    ref = '$ref'
    items = 'items'
    properties = 'properties'
    pattern_properties = 'patternProperties'
    any_of = 'anyOf'


class Error:
    def __init__(self, path, data, schema=None, logging_level=logging.INFO):
        self.path = path
        self.data = data
        self.schema = schema
        self.logging_level = logging_level

    def tostr(self):
        path = '.'.join(map(str, self.path))
        if self.schema:
            if self.logging_level >= logging.INFO:
                msg = f'data does not match {self.schema}'
            else:
                msg = f'does not match {self.schema}'
        else:
            msg = 'not in schema'

        if self.logging_level >= logging.INFO:
            output = f'{path}: {msg}'
        else:
            output = f'{path}: {self.data} {msg}'
        return output


class Transformer:
    def __init__(self, integer_datetime_fmt=NO_INTEGER_DATETIME_PARSING, pre_hook=None):
        self.integer_datetime_fmt = integer_datetime_fmt
        self.pre_hook = pre_hook
        self.removed = set()
        self.filtered = set()
        self.errors = []

    def log_warning(self):
        if self.filtered:
            LOGGER.debug('Filtered %s paths during transforms '
                         'as they were unsupported or not selected:\n\t%s',
                         len(self.filtered),
                         '\n\t'.join(sorted(self.filtered)))
            # Output list format to parse for reporting
            LOGGER.debug('Filtered paths list: %s',
                         sorted(self.filtered))

        if self.removed:
            LOGGER.debug('Removed %s paths during transforms:\n\t%s',
                         len(self.removed),
                         '\n\t'.join(sorted(self.removed)))
            # Output list format to parse for reporting
            LOGGER.debug('Removed paths list: %s', sorted(self.removed))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.log_warning()

    def filter_data_by_metadata(self, data, metadata):
        if isinstance(data, dict) and metadata:
            for field_name in list(data.keys()):
                selected = singer.metadata.get(metadata, ('properties', field_name), 'selected')
                inclusion = singer.metadata.get(metadata, ('properties', field_name), 'inclusion')
                if inclusion == 'automatic':
                    continue

                if selected is False:
                    data.pop(field_name, None)
                    # Track that a field was filtered because the customer
                    # didn't select it.
                    self.filtered.add(field_name)

                if inclusion == 'unsupported':
                    data.pop(field_name, None)
                    # Track that the field was filtered because the tap
                    # declared it as unsupported.
                    self.filtered.add(field_name)

        return data

    def transform(self, data, schema, metadata=None):
        data = self.filter_data_by_metadata(data, metadata)

        success, transformed_data = self.transform_recur(data, schema, [])
        if not success:
            raise SchemaMismatch(self.errors)

        return transformed_data

    def transform_recur(self, data, schema, path):
        if 'anyOf' in schema:
            return self._transform_anyof(data, schema, path)

        if 'type' not in schema:
            # indicates no typing information so don't bother transforming it
            return True, data

        types = schema['type']
        if not isinstance(types, list):
            types = [types]

        if 'null' in types:
            types.remove('null')
            types.append('null')

        for typ in types:
            success, transformed_data = self._transform(data, typ, schema, path)
            if success:
                return success, transformed_data
        else:
            # exhaused all types and didn't return, so we failed :-(
            self.errors.append(Error(path, data, schema, logging_level=LOGGER.level))
            return False, None

    def _transform_anyof(self, data, schema, path):
        subschemas = schema['anyOf']
        for subschema in subschemas:
            success, transformed_data = self.transform_recur(data, subschema, path)
            if success:
                return success, transformed_data
        else:
            # exhaused all schemas and didn't return, so we failed :-(
            self.errors.append(Error(path, data, schema, logging_level=LOGGER.level))
            return False, None

    def _transform_object(self, data, schema, path, pattern_properties):
        # We do not necessarily have a dict to transform here. The schema's
        # type could contain multiple possible values. Eg:
        #     ["null", "object", "string"]
        if not isinstance(data, dict):
            return False, data

        # Don't touch an empty schema
        if schema == {} and not pattern_properties:
            return True, data

        result = {}
        successes = []
        for key, value in data.items():
            # patternProperties are a map of {"pattern": { schema...}}
            pattern_schemas = [schema for pattern, schema
                               in (pattern_properties or {}).items()
                               if re.match(pattern, key)]
            if key in schema or pattern_schemas:
                sub_schema = schema.get(key, {'anyOf': pattern_schemas})
                success, subdata = self.transform_recur(value, sub_schema, path + [key])
                successes.append(success)
                result[key] = subdata
            else:
                # track that field has been removed because it wasn't
                # found in the schema. This likely indicates some problem
                # with discovery but rather than failing the run because
                # new data was added we'd rather continue the sync and
                # allow customers to indicate that they want the new data.
                self.removed.add('.'.join(map(str, path + [key])))

        return all(successes), result

    def _transform_array(self, data, schema, path):
        # We do not necessarily have a list to transform here. The schema's
        # type could contain multiple possible values. Eg:
        #     ["null", "array", "integer"]
        if not isinstance(data, list):
            return False, data
        result = []
        successes = []
        for i, row in enumerate(data):
            success, subdata = self.transform_recur(row, schema, path + [i])
            successes.append(success)
            result.append(subdata)

        return all(successes), result

    def _transform_datetime(self, value):
        if value is None or value == '':
            return None # Short circuit in the case of null or empty string

        if self.integer_datetime_fmt not in VALID_DATETIME_FORMATS:
            raise ValueError('Invalid integer datetime parsing option')

        if self.integer_datetime_fmt == NO_INTEGER_DATETIME_PARSING:
            return string_to_datetime(value)

        try:
            if self.integer_datetime_fmt == UNIX_SECONDS_INTEGER_DATETIME_PARSING:
                return unix_seconds_to_datetime(value)

            return unix_milliseconds_to_datetime(value)
        except Exception:
            return string_to_datetime(value)

    def _transform(self, data, typ, schema, path):
        if self.pre_hook:
            data = self.pre_hook(data, typ, schema)

        if is_decimal_schema(schema):
            return True, decimal_to_string(data, schema)

        if typ == 'null':
            if data is None or data == '':
                return True, None

            return False, None

        if schema.get('format') == 'date-time':
            data = self._transform_datetime(data)
            if data is None:
                return False, None

            return True, data

        if typ == 'object':
            # Objects do not necessarily specify properties
            return self._transform_object(data,
                                          schema.get('properties', {}),
                                          path,
                                          schema.get(SchemaKey.pattern_properties))

        if typ == 'array':
            return self._transform_array(data, schema['items'], path)

        return self._transform_scalar(data, typ)

    @staticmethod
    def _transform_scalar(data, typ):
        if typ == 'string':
            if data is not None:
                try:
                    return True, str(data)
                except Exception:
                    return False, None
            else:
                return False, None

        if typ == 'integer':
            if isinstance(data, str):
                data = data.replace(',', '')

            try:
                return True, int(data)
            except Exception:
                return False, None

        if typ == 'number':
            if isinstance(data, str):
                data = data.replace(',', '')

            try:
                return True, float(data)
            except Exception:
                return False, None

        if typ == 'boolean':
            if isinstance(data, str) and data.lower() == 'false':
                return True, False

            try:
                return True, bool(data)
            except Exception:
                return False, None

        return False, None


def transform(data, schema, integer_datetime_fmt=NO_INTEGER_DATETIME_PARSING,
              pre_hook=None, metadata=None):
    """
    Applies schema (and integer_datetime_fmt, if supplied) to data, transforming
    each field in data to the type specified in schema. If no type matches a
    data field, this throws an Exception.

    This applies types in order with the exception of 'null', which is always
    applied last.

    The valid types are: integer, number, boolean, array, object, null, string,
    and string with date-time format.

    If an integer_datetime_fmt is supplied, integer values in fields with date-
    time formats are appropriately parsed as unix seconds or unix milliseconds.

    The pre_hook should be a callable that takes data, type, and schema and
    returns the transformed data to be fed into the _transform function.
    """
    transformer = Transformer(integer_datetime_fmt, pre_hook)
    return transformer.transform(data, schema, metadata=metadata)


def _transform_datetime(value, integer_datetime_fmt=NO_INTEGER_DATETIME_PARSING):
    transformer = Transformer(integer_datetime_fmt)
    return transformer._transform_datetime(value)


def resolve_schema_references(schema, refs=None):
    """Resolves and replaces json-schema $refs with the appropriate dict.

    Recursively walks the given schema dict, converting every instance
    of $ref in a 'properties' structure with a resolved dict.

    This modifies the input schema and also returns it.

    Arguments:
        schema:
            the schema dict
        refs:
            a dict of <string, dict> which forms a store of referenced schemata

    Returns:
        schema
    """
    refs = refs or {}
    return _resolve_schema_references(schema, RefResolver('', schema, store=refs))


def _resolve_schema_references(schema, resolver):
    if SchemaKey.ref in schema:
        reference_path = schema.pop(SchemaKey.ref, None)
        resolved = resolver.resolve(reference_path)[1]
        schema.update(resolved)
        return _resolve_schema_references(schema, resolver)

    if SchemaKey.properties in schema:
        for k, val in schema[SchemaKey.properties].items():
            schema[SchemaKey.properties][k] = _resolve_schema_references(val, resolver)

    if SchemaKey.pattern_properties in schema:
        for k, val in schema[SchemaKey.pattern_properties].items():
            schema[SchemaKey.pattern_properties][k] = _resolve_schema_references(val, resolver)

    if SchemaKey.items in schema:
        schema[SchemaKey.items] = _resolve_schema_references(schema[SchemaKey.items], resolver)

    if SchemaKey.any_of in schema:
        for i, element in enumerate(schema[SchemaKey.any_of]):
            schema[SchemaKey.any_of][i] = _resolve_schema_references(element, resolver)

    return schema
