import io
import sys
import time
import singer

from typing import Union, Dict
from enum import Enum, unique
from collections import namedtuple
from decimal import Decimal
from jsonschema import FormatChecker, Draft7Validator
from singer import Catalog, Schema

from transform_field import transform
from transform_field import utils
from transform_field.timings import Timings

from transform_field.errors import CatalogRequiredException, StreamNotFoundException, InvalidTransformationException, \
    UnsupportedTransformationTypeException, NoStreamSchemaException


LOGGER = singer.get_logger('transform_field')
TIMINGS = Timings(LOGGER)
DEFAULT_MAX_BATCH_BYTES = 4000000
DEFAULT_MAX_BATCH_RECORDS = 20000
DEFAULT_BATCH_DELAY_SECONDS = 300.0
VALIDATE_RECORDS = False

StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])
TransMeta = namedtuple('TransMeta', ['field_id', 'type', 'when', 'field_paths'])

REQUIRED_CONFIG_KEYS = [
    "transformations"
]


@unique
class TransformationTypes(Enum):
    """
    List of supported transformation types
    """
    SET_NULL = 'SET-NULL'
    MASK_HIDDEN = 'MASK-HIDDEN'
    MASK_DATE = 'MASK-DATE'
    MASK_NUMBER = 'MASK-NUMBER'
    HASH = 'HASH'
    HASH_SKIP_FIRST = 'HASH-SKIP-FIRST'
    MASK_STRING_SKIP_ENDS = 'MASK-STRING-SKIP-ENDS'


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


class TransformFieldException(Exception):
    """A known exception for which we don't need to bring a stack trace"""


class TransformField:
    """
    Main Transformer class
    """

    def __init__(self, trans_config):
        self.trans_config = trans_config
        self.messages = []
        self.buffer_size_bytes = 0
        self.state = None

        # Time that the last batch was sent
        self.time_last_batch_sent = time.time()

        # Mapping from stream name to {'schema': ..., 'key_names': ..., 'bookmark_names': ... }
        self.stream_meta = {}

        # Mapping from transformation stream to {'stream': [ 'field_id': ..., 'type': ... ] ... }
        self.trans_meta = {}

        for trans in trans_config["transformations"]:
            # Naming differences in stream ids:
            #  1. properties.json and transformation_json using 'tap_stream_id'
            #  2. taps send in the 'stream' key in singer messages
            stream = trans["tap_stream_name"]
            if stream not in self.trans_meta:
                self.trans_meta[stream] = []

            self.trans_meta[stream].append(TransMeta(
                trans["field_id"],
                trans["type"],
                trans.get('when'),
                trans.get('field_paths')
            ))

    # pylint: disable=too-many-nested-blocks,too-many-branches
    # todo: simplify this method
    def flush(self):
        """Give batch to handlers to process"""

        if self.messages:
            stream = self.messages[0].stream
            stream_meta = self.stream_meta[stream]

            # Transform columns
            messages = self.messages
            schema = float_to_decimal(stream_meta.schema)
            key_properties = stream_meta.key_properties
            validator = Draft7Validator(schema, format_checker=FormatChecker())
            trans_meta = []
            if stream in self.trans_meta:
                trans_meta = self.trans_meta[stream]

            for i, message in enumerate(messages):
                if isinstance(message, singer.RecordMessage):

                    # Do transformation on every column where it is required
                    for trans in trans_meta:

                        if trans.field_id in message.record:
                            transformed = transform.do_transform(
                                message.record, trans.field_id, trans.type, trans.when, trans.field_paths
                            )
                            message.record[trans.field_id] = transformed

                    if VALIDATE_RECORDS:
                        # Validate the transformed columns
                        data = float_to_decimal(message.record)
                        try:
                            validator.validate(data)
                            if key_properties:
                                for k in key_properties:
                                    if k not in data:
                                        raise TransformFieldException(
                                            f'Message {i} is missing key property {k}')

                        except Exception as exc:
                            if type(exc).__name__ == "InvalidOperation":
                                raise TransformFieldException(
                                    f"Record does not pass schema validation. RECORD: {message.record}"
                                    "\n'multipleOf' validations that allows long precisions are not "
                                    "supported (i.e. with 15 digits or more). "
                                    f"Try removing 'multipleOf' methods from JSON schema.\n{exc}") from exc

                            raise TransformFieldException(
                                f"Record does not pass schema validation. RECORD: {message.record}\n{exc}") from exc

                    # Write the transformed message
                    singer.write_message(message)

            LOGGER.debug("Batch is valid with %s messages", len(messages))

            # Update stats
            self.time_last_batch_sent = time.time()
            self.messages = []
            self.buffer_size_bytes = 0

        if self.state:
            singer.write_message(singer.StateMessage(self.state))
            self.state = None

        TIMINGS.log_timings()

    def handle_line(self, line):
        """Takes a raw line from stdin and transforms it"""
        try:
            message = singer.parse_message(line)

            if not message:
                raise TransformFieldException('Unknown message type')
        except Exception as exc:
            raise TransformFieldException(f'Failed to process incoming message: {line}\n{exc}') from exc

        # If we got a Schema, set the schema and key properties for this
        # stream. Flush the batch, if there is one, in case the schema is
        # different
        if isinstance(message, singer.SchemaMessage):
            self.flush()

            self.stream_meta[message.stream] = StreamMeta(
                message.schema,
                message.key_properties,
                message.bookmark_properties)

            # if schema message, do validation of transformations using the schema to detect any
            # incompatibilities between the transformation and column types
            self.__validate_stream_trans(message.stream, message.schema)

            # Write the transformed message
            singer.write_message(message)

        elif isinstance(message, (singer.RecordMessage, singer.ActivateVersionMessage)):
            if self.messages and (
                    message.stream != self.messages[0].stream or
                    message.version != self.messages[0].version):
                self.flush()
            self.messages.append(message)
            self.buffer_size_bytes += len(line)

            num_bytes = self.buffer_size_bytes
            num_messages = len(self.messages)
            num_seconds = time.time() - self.time_last_batch_sent

            enough_bytes = num_bytes >= DEFAULT_MAX_BATCH_BYTES
            enough_messages = num_messages >= DEFAULT_MAX_BATCH_RECORDS
            enough_time = num_seconds >= DEFAULT_BATCH_DELAY_SECONDS
            if enough_bytes or enough_messages or enough_time:
                LOGGER.debug('Flushing %d bytes, %d messages, after %.2f seconds', num_bytes, num_messages, num_seconds)
                self.flush()

        elif isinstance(message, singer.StateMessage):
            self.state = message.value

    def consume(self, reader):
        """Consume all the lines from the queue, flushing when done."""
        for line in reader:
            self.handle_line(line)
        self.flush()

    def validate(self, catalog: Catalog):
        """
        Validate the transformations by checking if each transformation type is compatible with the column type
        :param catalog: the catalog of streams with their json schema
        """
        LOGGER.info('Starting validation of transformations...')

        if not catalog:
            raise CatalogRequiredException('Catalog missing! please provide catalog to run validation.')

        # get the schema of each stream
        schemas = utils.get_stream_schemas(catalog)

        for stream_id in self.trans_meta:
            self.__validate_stream_trans(stream_id, schemas.get(stream_id))

    def __validate_stream_trans(self, stream_id: str, stream_schema: Union[Schema, Dict]):
        """
        Validation of each stream's transformations
        :param stream_id: ID of the stream
        :param stream_schema: schema of the streams
        """

        if stream_id not in self.trans_meta:
            return

        # check if we even have schema for stream of this transformation
        if stream_schema is None:
            raise StreamNotFoundException(stream_id)

        # check if we stream has not empty schema
        if not stream_schema:
            raise NoStreamSchemaException(stream_id)

        for transformation in self.trans_meta[stream_id]:
            trans_type = transformation.type
            field_id = transformation.field_id

            if isinstance(stream_schema, Schema):
                field_type = stream_schema.properties[field_id].type
                field_format = stream_schema.properties[field_id].format
            else:
                field_type = stream_schema['properties'][field_id].get('type')
                field_format = stream_schema['properties'][field_id].get('format')

            # If the value we want to transform is a field in a JSON property
            # then no need to enforce rules below for now
            if field_type and \
                    ("object" in field_type or "array" in field_type) and \
                    transformation.field_paths is not None:
                continue

            if trans_type in (TransformationTypes.HASH.value, TransformationTypes.MASK_HIDDEN.value) or \
                    trans_type.startswith(TransformationTypes.HASH_SKIP_FIRST.value) or \
                    trans_type.startswith(TransformationTypes.MASK_STRING_SKIP_ENDS.value):
                if not (field_type is not None and 'string' in field_type and not field_format):
                    raise InvalidTransformationException(
                        f'Cannot apply `{trans_type}` transformation type to a non-string field `'
                        f'{field_id}` in stream `{stream_id}`')

            elif trans_type == TransformationTypes.MASK_DATE.value:
                if not (field_type is not None and 'string' in field_type and field_format in {'date-time', 'date'}):
                    raise InvalidTransformationException(
                        f'Cannot apply `{trans_type}` transformation type to a non-stringified date field'
                        f' `{field_id}` in stream `{stream_id}`')

            elif trans_type == TransformationTypes.MASK_NUMBER.value:
                if not (field_type is not None and (
                        'number' in field_type or 'integer' in field_type) and not field_format):
                    raise InvalidTransformationException(
                        f'Cannot apply `{trans_type}` transformation type to a non-numeric field '
                        f'`{field_id}` in stream `{stream_id}`')

            elif trans_type == TransformationTypes.SET_NULL.value:
                LOGGER.info('Transformation type is %s, no need to do any validation.', trans_type)

            else:
                raise UnsupportedTransformationTypeException(trans_type)


def main_impl():
    """
    Main implementation
    """
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    trans_config = {'transformations': args.config['transformations']}

    instance = TransformField(trans_config)

    if args.validate:
        instance.validate(args.catalog)
    else:
        reader = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
        instance.consume(reader)

    LOGGER.info("Exiting normally")


def main():
    """Main entry point"""
    try:
        main_impl()
    except TransformFieldException as exc:
        for line in str(exc).splitlines():
            LOGGER.critical(line)
        sys.exit(1)
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
