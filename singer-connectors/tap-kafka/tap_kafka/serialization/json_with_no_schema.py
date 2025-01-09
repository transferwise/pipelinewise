import orjson

from confluent_kafka.serialization import Deserializer
from confluent_kafka.serialization import Serializer
from confluent_kafka.serialization import SerializationError


# pylint: disable=R0903
class JSONSimpleSerializer(Serializer):
    """
    Serializes a Python object to JSON formatted string.
    """
    def __call__(self, obj, ctx):
        if obj is None:
            return None

        try:
            return orjson.dumps(obj)  # pylint: disable=E1101
        except orjson.JSONDecodeError as e:  # pylint: disable=E1101
            raise SerializationError(e)


# pylint: disable=R0903
class JSONSimpleDeserializer(Deserializer):
    """
    Deserializes a Python object from JSON formatted bytes.
    """
    def __call__(self, value, ctx):
        """
        Deserializes a Python object from JSON formatted bytes
        Args:
            value (bytes): bytes to be deserialized
            ctx (SerializationContext): Metadata pertaining to the serialization
                operation
        Raises:
            SerializerError if an error occurs during deserialization.
        Returns:
            Python object if data is not None, otherwise None
        """
        if value is None:
            return None

        try:
            return orjson.loads(value)  # pylint: disable=E1101
        except orjson.JSONDecodeError as e: # pylint: disable=E1101
            raise SerializationError(e)
