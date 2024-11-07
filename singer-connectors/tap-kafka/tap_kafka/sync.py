"""Sync functions that consumes and transforms kafka messages to singer messages"""
import time
import copy
import dpath.util
import dateutil
import datetime

import singer
import confluent_kafka

from confluent_kafka import KafkaException
from typing import List

from singer import utils, metadata
from tap_kafka.errors import InvalidBookmarkException
from tap_kafka.errors import InvalidConfigException
from tap_kafka.errors import InvalidTimestampException
from tap_kafka.errors import TimestampNotAvailableException
from tap_kafka.errors import PrimaryKeyNotFoundException
from tap_kafka.serialization.json_with_no_schema import JSONSimpleDeserializer
from tap_kafka.serialization.protobuf import ProtobufDictDeserializer
from tap_kafka.serialization.protobuf import proto_to_message_type

LOGGER = singer.get_logger('tap_kafka')

LOG_MESSAGES_PERIOD = 5000  # Print log messages to stderr after every nth messages
SEND_STATE_PERIOD = 5000    # Update and send bookmark to stdout after nth messages


def search_in_list_of_dict_by_key_value(d_list, key, value):
    """Search a specific value of a certain key in a list of dictionary.
    Returns the index of first matching index item in the list or -1 if not found"""
    for idx, dic in enumerate(d_list):
        if dic.get(key) == value:
            return idx
    return -1


def init_value_deserializer(kafka_config):
    """Initialise the value deserializer"""
    value_deserializer = None
    if kafka_config['message_format'] == 'json':
        value_deserializer = JSONSimpleDeserializer()

    elif kafka_config['message_format'] == 'protobuf':
        message_type = proto_to_message_type(kafka_config['proto_schema'],
                                             kafka_config['proto_classes_dir'],
                                             kafka_config['topic'])
        value_deserializer = ProtobufDictDeserializer(message_type, {
            'use.deprecated.format': False
        })

    if not value_deserializer:
        raise InvalidConfigException(f"Unknown message format: {kafka_config['message_format']}")

    return value_deserializer


def send_activate_version_message(state, tap_stream_id):
    """Generate and send singer ACTIVATE message"""
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')
    if stream_version is None:
        stream_version = int(time.time() * 1000)
    singer.write_message(singer.ActivateVersionMessage(
        stream=tap_stream_id,
        version=stream_version))


def send_schema_message(stream):
    """Generate and send singer SCHEMA message for the stream"""
    md_map = metadata.to_map(stream['metadata'])
    pks = md_map.get((), {}).get('table-key-properties', [])

    singer.write_message(singer.SchemaMessage(
        stream=stream['tap_stream_id'],
        schema=stream['schema'],
        key_properties=pks))


def update_bookmark(state, topic, message, comment = False):
    """Update bookmark with a new timestamp"""

    bookmark_key = f'partition_{message.partition()}'
    bookmark_value = {
        'partition': message.partition(),
        'offset': message.offset(),
        'timestamp': get_timestamp_from_timestamp_tuple(message.timestamp()),
        'start_time': epoch_to_iso_timestamp(get_timestamp_from_timestamp_tuple(message.timestamp()))
    }

    if comment : bookmark_value['_comment'] = 'order of precedence : offset, timestamp, start_time; only one will be used'

    return singer.write_bookmark(state, topic, bookmark_key, bookmark_value)


def iso_timestamp_to_epoch(iso_timestamp: str) -> int:
    """Convert an ISO 8601 formatted string to epoch in milliseconds"""
    try:
        return int(dateutil.parser.parse(iso_timestamp).timestamp() * 1000)
    except dateutil.parser.ParserError:
        raise InvalidTimestampException(f'{iso_timestamp} is not a valid ISO formatted string')


def epoch_to_iso_timestamp(epoch) -> str:
    """Convert an epoch to an ISO 8601 formatted string"""
    if len(str(epoch)) != 13 or type(epoch) != int:
        raise InvalidTimestampException(f'{epoch} is not a valid millisecond epoch integer')

    return datetime.datetime.utcfromtimestamp(epoch / 1000).isoformat(timespec='milliseconds')


def error_cb(err):
    """Error callback for kafka consumer"""
    LOGGER.info('An error occurred: %s', err)


def init_kafka_consumer(kafka_config):
    """Initialise kafka consumer"""

    LOGGER.info('Initialising Kafka Consumer...')

    consumer_conf = {
        # Required parameters
        'bootstrap.servers': kafka_config['bootstrap_servers'],
        'group.id': kafka_config['group_id'],

        # Optional parameters
        'session.timeout.ms': kafka_config['session_timeout_ms'],
        'heartbeat.interval.ms': kafka_config['heartbeat_interval_ms'],
        'max.poll.interval.ms': kafka_config['max_poll_interval_ms'],

        # Non-configurable parameters
        'enable.auto.commit': False,
        'value.deserializer': init_value_deserializer(kafka_config),
        'error_cb': error_cb,
    }

    if kafka_config['debug_contexts']:
        # https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#debug-contexts
        consumer_conf['debug'] = kafka_config['debug_contexts']

    consumer = confluent_kafka.DeserializingConsumer(consumer_conf)

    LOGGER.info('Kafka Consumer initialised successfully')

    return consumer


def get_timestamp_from_timestamp_tuple(kafka_ts: tuple) -> float:
    """Get the actual timestamp value from a kafka timestamp tuple"""
    if isinstance(kafka_ts, tuple):
        try:
            ts_type = kafka_ts[0]

            if ts_type == confluent_kafka.TIMESTAMP_NOT_AVAILABLE:
                raise TimestampNotAvailableException('Required timestamp not available in the kafka message.')

            if ts_type in [confluent_kafka.TIMESTAMP_CREATE_TIME, confluent_kafka.TIMESTAMP_LOG_APPEND_TIME]:
                try:
                    timestamp = int(kafka_ts[1])
                    if timestamp > 0:
                        return timestamp

                    raise InvalidTimestampException(f'Invalid timestamp tuple. '
                                                    f'Timestamp {timestamp} needs to be greater than zero.')
                except ValueError:
                    raise InvalidTimestampException(f'Invalid timestamp tuple. Timestamp {kafka_ts[1]} is not integer.')

            raise InvalidTimestampException(f'Invalid timestamp tuple. Timestamp type {ts_type} is not valid.')
        except IndexError:
            raise InvalidTimestampException(f'Invalid timestamp tuple. '
                                            f'Timestamp type {kafka_ts} should have two elements.')

    raise InvalidTimestampException(f'Invalid kafka timestamp. It needs to be a tuple but it is a {type(kafka_ts)}.')


def kafka_message_to_singer_record(message, primary_keys: dict, use_message_key: bool):
    """Transforms kafka message to singer record message"""
    # Create dictionary with base attributes
    record = {
        "message": message.value(),
        "message_partition": message.partition(),
        "message_offset": message.offset(),
        "message_timestamp": get_timestamp_from_timestamp_tuple(message.timestamp()),
    }

    # Add PKs to the record. In case custom PKs are defined, use them
    if primary_keys:
        for key, pk_selector in primary_keys.items():
            try:
                record[key] = dpath.get(message.value(), pk_selector)
            except KeyError:
                raise PrimaryKeyNotFoundException(f"Custom primary key not found in the message: '{pk_selector}'")
    elif use_message_key:
        if not message.key():
            raise PrimaryKeyNotFoundException("Kafka message key not found in the message")
        # message.key() can return string or bytes, so extra check to accommodate with either
        record['message_key'] = message.key() if isinstance(message.key(), str) else message.key().decode('utf-8')

    return record


def consume_kafka_message(message, topic, primary_keys, use_message_key):
    """Insert single kafka message into the internal store"""
    singer_record = kafka_message_to_singer_record(message, primary_keys, use_message_key)
    singer.write_message(singer.RecordMessage(stream=topic, record=singer_record, time_extracted=utils.now()))


def select_kafka_partitions(consumer, kafka_config) -> List[confluent_kafka.TopicPartition]:
    """Select partitions in topic"""

    LOGGER.info(f"Selecting partitions in topic '{kafka_config['topic']}'")

    topic = kafka_config['topic']
    partition_ids_requested = kafka_config['partitions']

    try:
        topic_meta = consumer.list_topics(topic, timeout=kafka_config['max_poll_interval_ms'] / 1000)
        partition_meta = topic_meta.topics[topic].partitions
    except KafkaException:
        LOGGER.exception(f"Unable to list partitions in topic '{topic}'", exc_info=True)
        raise

    if not partition_meta:
        raise InvalidConfigException(f"No partitions available in topic '{topic}'")

    # Get list of all partitions in topic
    partition_ids_available = []
    for partition in partition_meta:
        partition_ids_available.append(partition)

    if not partition_ids_requested:
        partition_ids = partition_ids_available
        LOGGER.info(f"Requesting all partitions in topic '{topic}'")
    else:
        LOGGER.info(f"Requesting partitions {partition_ids_requested} in topic '{topic}'")
        partition_ids = list(set(partition_ids_requested).intersection(partition_ids_available))
        partition_ids_not_available = list(set(partition_ids_requested).difference(partition_ids_available))
        if partition_ids_not_available: LOGGER.warning(f"Partitions {partition_ids_not_available} not available in topic '{topic}'")

    LOGGER.info(f"Selecting partitions {partition_ids} in topic '{topic}'")

    partitions = []
    for partition_id in partition_ids:
        partitions.append(confluent_kafka.TopicPartition(topic, partition_id))

    return partitions


def bookmarked_partition_offset(consumer, topic: str, partition_bookmark: dict) -> confluent_kafka.TopicPartition:
    """Transform a bookmarked partition to a kafka TopicPartition object"""

    try:
        if 'offset' in partition_bookmark:
            LOGGER.info(f"Partition [{partition_bookmark['partition']}] found in bookmark - setting offset to '{partition_bookmark['offset']}'")
            partition = confluent_kafka.TopicPartition(topic, partition_bookmark['partition'], partition_bookmark['offset'])
        elif 'timestamp' in partition_bookmark:
            epoch = partition_bookmark['timestamp']
            iso_timestamp = epoch_to_iso_timestamp(epoch)
            LOGGER.info(f"Partition [{partition_bookmark['partition']}] found in bookmark - setting offset to timestamp '{epoch}' ({iso_timestamp})")
            partition = confluent_kafka.TopicPartition(topic, partition_bookmark['partition'], epoch)
            partition = consumer.offsets_for_times([partition])[0]
        elif 'start_time' in partition_bookmark:
            start_time = partition_bookmark['start_time']
            epoch = iso_timestamp_to_epoch(start_time)
            LOGGER.info(f"Partition [{partition_bookmark['partition']}] found in bookmark - setting offset to start_time '{start_time}' ({epoch})")
            partition = confluent_kafka.TopicPartition(topic, partition_bookmark['partition'], epoch)
            partition = consumer.offsets_for_times([partition])[0]
        else:
            raise InvalidBookmarkException(f"Invalid bookmark. Bookmark does not include 'partition' and ('offset' or 'timestamp') keys.")
    except TypeError:
        raise InvalidBookmarkException(f"Invalid bookmark. One or more bookmark entries using invalid type(s).")
    except KeyError:
        raise InvalidBookmarkException(f"Invalid bookmark. One or more bookmark entries using invalid type(s).")

    return partition


def set_partition_offsets(consumer, partitions, kafka_config, state = {}):
    """Setting offsets to bookmarked state"""
    LOGGER.info(f"Setting offsets to bookmarked state")

    topic = kafka_config['topic']
    initial_start_time = kafka_config['initial_start_time']

    if state:
        bookmarked_partitions = state['bookmarks'][topic]
    else:
        bookmarked_partitions = {}

    partitions_to_set = []

    for partition in partitions:
        found_in_bookmark = False
        for bookmark in bookmarked_partitions:
            if partition.partition == bookmarked_partitions[bookmark]['partition']:
                partition = bookmarked_partition_offset(consumer, topic, bookmarked_partitions[bookmark])
                found_in_bookmark = True

        if not found_in_bookmark:
            if initial_start_time == 'beginning':
                LOGGER.info(f"Partition [{partition.partition}] not found in bookmark - setting offset to 'beginning'")
                partition.offset = consumer.get_watermark_offsets(partition)[0]
            elif initial_start_time == 'earliest':
                LOGGER.info(f"Partition [{partition.partition}] not found in bookmark - setting offset to 'earliest'")
                partition = consumer.committed([partition])[0]
                partition.offset = max(partition.offset, consumer.get_watermark_offsets(partition)[0])
            elif initial_start_time == 'latest':
                LOGGER.info(f"Partition [{partition.partition}] not found in bookmark - setting offset to 'latest'")
                partition.offset = consumer.get_watermark_offsets(partition)[1] - 1
            elif initial_start_time is not None:
                epoch = iso_timestamp_to_epoch(initial_start_time)
                LOGGER.info(f"Partition [{partition.partition}] not found in bookmark - setting offset to initial_start_time '{initial_start_time}' ({epoch})")
                partition.offset = epoch
                partition = consumer.offsets_for_times([partition])[0]
                partition.offset = max(partition.offset, consumer.get_watermark_offsets(partition)[0])

        partitions_to_set.append(partition)

    return partitions_to_set


def assign_kafka_partitions(consumer, partitions):
    """Assign and seek partitions to offsets"""
    LOGGER.info("Assigning partitions to consumer ...")

    consumer.assign(partitions)

    partitions_committed = partitions
    for partition in partitions_committed:
        partition.offset -= 1

    if all(partition.offset >= 0 for partition in partitions_committed):
        LOGGER.info("Committing partitions ")
        consumer.commit(offsets=partitions_committed)
    else:
        LOGGER.info("Partitions not committed because one or more offsets are less than zero")


def commit_consumer_to_bookmarked_state(consumer, topic, state):
    """Commit every bookmarked offset to kafka"""
    LOGGER.info("Committing bookmarked offsets to kafka ...")

    offsets_to_commit = []
    bookmarked_partitions = state.get('bookmarks', {}).get(topic, {})
    for partition in bookmarked_partitions:
        bookmarked_partition = bookmarked_partitions[partition]
        topic_partition = confluent_kafka.TopicPartition(topic,
                                                         bookmarked_partition['partition'],
                                                         bookmarked_partition['offset'])
        offsets_to_commit.append(topic_partition)

    consumer.commit(offsets=offsets_to_commit, asynchronous=False)

    LOGGER.info("Bookmarked offsets committed")


# pylint: disable=too-many-locals,too-many-statements
def read_kafka_messages(consumer, kafka_config, state):
    """Read kafka topic continuously and writing transformed singer messages to STDOUT"""

    LOGGER.info('Starting Kafka messages consumption...')

    topic = kafka_config['topic']
    primary_keys = kafka_config['primary_keys']
    use_message_key = kafka_config['use_message_key']
    max_runtime_ms = kafka_config['max_runtime_ms']
    commit_interval_ms = kafka_config['commit_interval_ms']
    consumed_messages = 0
    last_consumed_ts = 0
    start_time = 0
    last_commit_time = 0
    message = None

    # Send singer ACTIVATE message
    send_activate_version_message(state, topic)

    while True:
        polled_message = consumer.poll(timeout=kafka_config['consumer_timeout_ms'] / 1000)

        # Stop consuming more messages if no new message and consumer_timeout_ms exceeded
        if polled_message is None:
            LOGGER.info('No new message received in %s ms. Stop consuming more messages.',
                        kafka_config["consumer_timeout_ms"]
                        )
            break

        message = polled_message
        LOGGER.debug("topic=%s partition=%s offset=%s timestamp=%s key=%s value=<HIDDEN>" % (message.topic(),
                                                                                             message.partition(),
                                                                                             message.offset(),
                                                                                             message.timestamp(),
                                                                                             message.key()))

        # Initialise the start time after the first message
        if not start_time:
            start_time = time.time()

        # Initialise the last_commit_time after the first message
        if not last_commit_time:
            last_commit_time = time.time()

        # Generate singer message
        consume_kafka_message(message, topic, primary_keys, use_message_key)

        # Update bookmark after every consumed message
        state = update_bookmark(state, topic, message, comment=True)

        now = time.time()
        # Commit periodically
        if now - last_commit_time > commit_interval_ms / 1000:
            commit_consumer_to_bookmarked_state(consumer, topic, state)
            last_commit_time = time.time()

        # Log message stats periodically every LOG_MESSAGES_PERIOD
        consumed_messages += 1
        if consumed_messages % LOG_MESSAGES_PERIOD == 0:
            LOGGER.info("%d messages consumed... Last consumed timestamp: %f Partition: %d Offset: %d",
                        consumed_messages, last_consumed_ts, message.partition(), message.offset())

        # Send state message periodically every SEND_STATE_PERIOD
        if consumed_messages % SEND_STATE_PERIOD == 0:
            LOGGER.debug("%d messages consumed... Sending latest state: %s", consumed_messages, state)
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        # Stop consuming more messages if max runtime exceeded
        max_runtime_s = max_runtime_ms / 1000
        if now >= (start_time + max_runtime_s):
            LOGGER.info(f'Max runtime {max_runtime_s} seconds exceeded. Stop consuming more messages.')
            break

    # Update bookmark and send state at the last time
    if message:
        state = update_bookmark(state, topic, message, comment=True)
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
        commit_consumer_to_bookmarked_state(consumer, topic, state)


def do_sync(kafka_config, catalog, state):
    """Set up kafka consumer, start reading the topic"""
    topic = kafka_config['topic']

    # Only one stream
    streams = catalog.get('streams', [])
    topic_pos = search_in_list_of_dict_by_key_value(streams, 'tap_stream_id', topic)

    if topic_pos == -1:
        raise Exception(f'Invalid catalog object. Cannot find {topic} in catalog')

    # Send the initial schema message
    send_schema_message(streams[topic_pos])

    # Setup consumer
    consumer = init_kafka_consumer(kafka_config)

    try:
        partitions = select_kafka_partitions(consumer, kafka_config)

        partitions = set_partition_offsets(consumer, partitions, kafka_config, state)

        assign_kafka_partitions(consumer, partitions)

        # Start consuming messages from kafka
        read_kafka_messages(consumer, kafka_config, state)
    finally:
        # # Leave group and commit final offsets
        LOGGER.info('Explicitly closing Kafka consumer...')
        consumer.close()
