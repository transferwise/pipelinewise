
"""Provides an object model for JSON Schema"""

import orjson

# Retain JSON Schema fields and the explicit PipelineWise decimal annotation.
STANDARD_KEYS = [
    'selected',
    'inclusion',
    'description',
    'minimum',
    'maximum',
    'exclusiveMinimum',
    'exclusiveMaximum',
    'multipleOf',
    'maxLength',
    'minLength',
    'format',
    'type',
    'additionalProperties',
    'anyOf',
    'patternProperties',
    'decimal',
]


class Schema():
    """Object model for JSON Schema.

    Tap and Target authors may find this to be more convenient than
    working directly with JSON Schema data structures.

    """

    def __init__(self, type=None, format=None, properties=None, items=None,
                 selected=None, inclusion=None, description=None, minimum=None,
                 maximum=None, exclusiveMinimum=None, exclusiveMaximum=None,
                 multipleOf=None, maxLength=None, minLength=None, additionalProperties=None,
                 anyOf=None, patternProperties=None, decimal=None):

        self.type = type
        self.properties = properties
        self.items = items
        self.selected = selected
        self.inclusion = inclusion
        self.description = description
        self.minimum = minimum
        self.maximum = maximum
        self.exclusiveMinimum = exclusiveMinimum
        self.exclusiveMaximum = exclusiveMaximum
        self.multipleOf = multipleOf
        self.maxLength = maxLength
        self.minLength = minLength
        self.anyOf = anyOf
        self.format = format
        self.additionalProperties = additionalProperties
        self.patternProperties = patternProperties
        self.decimal = decimal

    def __str__(self):
        return orjson.dumps(self.to_dict()).decode('utf-8')

    def __repr__(self):
        pairs = [k + '=' + repr(v) for k, v in self.__dict__.items()]
        args = ', '.join(pairs)
        return 'Schema(' + args + ')'

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def to_dict(self):
        """Return the raw JSON Schema as a (possibly nested) dict."""
        result = {}

        if self.properties is not None:
            result['properties'] = {
                k: v.to_dict()
                for k, v
                in self.properties.items()
            }

        if self.items is not None:
            result['items'] = self.items.to_dict()

        for key in STANDARD_KEYS:
            if self.__dict__.get(key) is not None:
                result[key] = self.__dict__[key]

        return result

    @classmethod
    def from_dict(cls, data, **schema_defaults):
        """Initialize a Schema object based on the JSON Schema structure.

        :param schema_defaults: The default values to the Schema
        constructor."""
        kwargs = schema_defaults.copy()
        properties = data.get('properties')
        items = data.get('items')

        if properties is not None:
            kwargs['properties'] = {
                k: Schema.from_dict(v, **schema_defaults)
                for k, v in properties.items()
            }
        if items is not None:
            kwargs['items'] = Schema.from_dict(items, **schema_defaults)
        for key in STANDARD_KEYS:
            if key in data:
                kwargs[key] = data[key]
        return Schema(**kwargs)
