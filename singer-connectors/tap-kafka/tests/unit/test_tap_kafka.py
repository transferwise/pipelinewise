import os
import time
import json
import unittest

import singer
import tap_kafka
from tap_kafka import common
from tap_kafka import sync
from tap_kafka.errors import (
    DiscoveryException,
    InvalidConfigException,
    InvalidBookmarkException,
    InvalidTimestampException,
    TimestampNotAvailableException,
    PrimaryKeyNotFoundException,
)
import confluent_kafka

from tests.unit.helper.kafka_consumer_mock import KafkaConsumerMock, KafkaConsumerMessageMock


def _get_resource_from_json(filename):
    with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as json_resource:
        return json.load(json_resource)


def _message_to_singer_record(message):
    return {
        'message': message.get('value'),
        'message_timestamp': sync.get_timestamp_from_timestamp_tuple(message.get('timestamp')),
        'message_offset': message.get('offset'),
        'message_partition': message.get('partition')
    }


def _message_to_singer_state(message):
    return {
        'bookmarks': message
    }


def _delete_version_from_state_message(state):
    if 'bookmarks' in state:
        for key in state['bookmarks'].keys():
            if 'version' in state['bookmarks'][key]:
                del state['bookmarks'][key]['version']

    return state


def _dict_to_kafka_message(dict_m):
    return {
        **dict_m,
        **{
            'timestamp': tuple(dict_m.get('timestamp', []))
        }
    }


def _parse_stdout(stdout):
    stdout_messages = []

    # Process only json messages
    for s in stdout.split("\n"):
        try:
            stdout_messages.append(json.loads(s))
        except Exception as e:
            pass

    return stdout_messages


def _read_kafka_topic(config, state, kafka_messages):
    # Mock KafkaConsumer classes
    consumer = KafkaConsumerMock(kafka_messages)
    singer_messages = []

    # Store output singer messages in an array
    singer.write_message = lambda m: singer_messages.append(m.asdict())

    # Run sync_stream
    sync.read_kafka_topic(consumer, config, state)

    return singer_messages


def _assert_singer_messages_in_local_store_equal(local_store, topic, exp_records, exp_states):
    exp_singer_records = list(map(lambda x: _message_to_singer_record(x), exp_records))
    exp_singer_states = list(map(lambda x: _message_to_singer_state(x), exp_states))
    for msg in map(json.loads, local_store.messages):
        if msg['type'] == 'RECORD':
            assert msg['stream'] == topic
            record = msg['record']
            exp_singer_records.remove(record)

        if msg['type'] == 'STATE':
            state = _delete_version_from_state_message(msg['value'])
            exp_singer_states.remove(state)

    # All the fake kafka message that we generated in consumer have been observed as a part of the output
    assert len(exp_singer_records) == 0
    assert len(exp_singer_states) == 0


class TestSync(unittest.TestCase):
    """
    Unit Tests
    """

    maxDiff = None

    @classmethod
    def setup_class(self):
        self.config = {
            'topic': 'dummy_topic',
            'primary_keys': {},
            'use_message_key': False,
            'max_runtime_ms': tap_kafka.DEFAULT_MAX_RUNTIME_MS,
            'consumer_timeout_ms': tap_kafka.DEFAULT_CONSUMER_TIMEOUT_MS,
            'commit_interval_ms': tap_kafka.DEFAULT_COMMIT_INTERVAL_MS
        }

    def test_generate_config_with_defaults(self):
        """Should generate config dictionary with every required and optional parameter with defaults"""
        minimal_config = {
            'topic': 'my_topic',
            'group_id': 'my_group_id',
            'bootstrap_servers': 'server1,server2,server3'
        }
        self.assertDictEqual(tap_kafka.generate_config(minimal_config), {
            'topic': 'my_topic',
            'group_id': 'my_group_id',
            'bootstrap_servers': 'server1,server2,server3',
            'primary_keys': {},
            'use_message_key': True,
            'initial_start_time': tap_kafka.DEFAULT_INITIAL_START_TIME,
            'max_runtime_ms': tap_kafka.DEFAULT_MAX_RUNTIME_MS,
            'commit_interval_ms': tap_kafka.DEFAULT_COMMIT_INTERVAL_MS,
            'consumer_timeout_ms': tap_kafka.DEFAULT_CONSUMER_TIMEOUT_MS,
            'session_timeout_ms': tap_kafka.DEFAULT_SESSION_TIMEOUT_MS,
            'heartbeat_interval_ms': tap_kafka.DEFAULT_HEARTBEAT_INTERVAL_MS,
            'max_poll_records': tap_kafka.DEFAULT_MAX_POLL_RECORDS,
            'max_poll_interval_ms': tap_kafka.DEFAULT_MAX_POLL_INTERVAL_MS,
            'message_format': tap_kafka.DEFAULT_MESSAGE_FORMAT,
            'partitions': tap_kafka.DEFAULT_PARTITIONS,
            'proto_classes_dir': tap_kafka.DEFAULT_PROTO_CLASSES_DIR,
            'proto_schema': tap_kafka.DEFAULT_PROTO_SCHEMA,
            'debug_contexts': None
        })

    def test_generate_config_with_custom_parameters(self):
        """Should generate config dictionary with every required and optional parameter with custom values"""
        custom_config = {
            'topic': 'my_topic',
            'partitions': [2, 3],
            'group_id': 'my_group_id',
            'bootstrap_servers': 'server1,server2,server3',
            'primary_keys': {
                'id': '$.jsonpath.to.primary_key'
            },
            'max_runtime_ms': 1111,
            'commit_interval_ms': 10000,
            'batch_size_rows': 2222,
            'batch_flush_interval_ms': 3333,
            'consumer_timeout_ms': 1111,
            'session_timeout_ms': 2222,
            'heartbeat_interval_ms': 3333,
            'max_poll_records': 4444,
            'max_poll_interval_ms': 5555,
            'message_format': 'protobuf',
            'proto_classes_dir': '/tmp/proto-classes',
            'proto_schema': 'proto-schema',
            'debug_contexts': 'topic,cgrp'
        }
        self.assertDictEqual(tap_kafka.generate_config(custom_config), {
            'topic': 'my_topic',
            'partitions': [2, 3],
            'group_id': 'my_group_id',
            'bootstrap_servers': 'server1,server2,server3',
            'primary_keys': {
                'id': '$.jsonpath.to.primary_key'
            },
            'use_message_key': True,
            'initial_start_time': 'latest',
            'max_runtime_ms': 1111,
            'commit_interval_ms': 10000,
            'consumer_timeout_ms': 1111,
            'session_timeout_ms': 2222,
            'heartbeat_interval_ms': 3333,
            'max_poll_records': 4444,
            'max_poll_interval_ms': 5555,
            'message_format': 'protobuf',
            'proto_classes_dir': '/tmp/proto-classes',
            'proto_schema': 'proto-schema',
            'debug_contexts': 'topic,cgrp'
        })

    def test_validate_config(self):
        """Make sure if config dict can be validated correctly"""
        # Should raise an exception if a required key (bootstrap_servers) not exists in the config
        with self.assertRaises(InvalidConfigException):
            tap_kafka.validate_config({'topic': 'my_topic',
                                       'group_id': 'my_group_id'})

        # Should raise an exception if initial_start_time is not valid
        with self.assertRaises(InvalidConfigException):
            tap_kafka.validate_config({'topic': 'my_topic',
                                       'group_id': 'my_group_id',
                                       'bootstrap_servers': 'server1,server2,server3',
                                       'message_format': 'json',
                                       'initial_start_time': 'ssssss'})

        # Partitions are in a list
        self.assertIsNone(tap_kafka.validate_config({'topic': 'my_topic',
                                                     'partitions': [1, 2, 2, 2],
                                                     'group_id': 'my_group_id',
                                                     'bootstrap_servers': 'server1,server2,server3',
                                                     'message_format': 'json',
                                                     'initial_start_time': 'latest'}))

        # Initial start time is a reserved word (beginning)
        self.assertIsNone(tap_kafka.validate_config({'topic': 'my_topic',
                                                     'partitions': [],
                                                     'group_id': 'my_group_id',
                                                     'bootstrap_servers': 'server1,server2,server3',
                                                     'message_format': 'json',
                                                     'initial_start_time': 'beginning'}))

        # Initial start time is a reserved word (latest)
        self.assertIsNone(tap_kafka.validate_config({'topic': 'my_topic',
                                                     'partitions': [],
                                                     'group_id': 'my_group_id',
                                                     'bootstrap_servers': 'server1,server2,server3',
                                                     'message_format': 'json',
                                                     'initial_start_time': 'latest'}))

        # Initial start time is a reserved word (earliset)
        self.assertIsNone(tap_kafka.validate_config({'topic': 'my_topic',
                                                     'partitions': [],
                                                     'group_id': 'my_group_id',
                                                     'bootstrap_servers': 'server1,server2,server3',
                                                     'message_format': 'json',
                                                     'initial_start_time': 'earliest'}))

        # Initial start time is an ISO timestamp
        self.assertIsNone(tap_kafka.validate_config({'topic': 'my_topic',
                                                     'partitions': [],
                                                     'group_id': 'my_group_id',
                                                     'bootstrap_servers': 'server1,server2,server3',
                                                     'message_format': 'json',
                                                     'initial_start_time': '2022-12-03T11:39:53'}))

        # Should raise an Exception if initial_start_time is not ISO timestamp
        with self.assertRaises(InvalidConfigException):
            tap_kafka.validate_config({'topic': 'my_topic',
                                                     'partitions': [],
                                                     'group_id': 'my_group_id',
                                                     'bootstrap_servers': 'server1,server2,server3',
                                                     'message_format': 'json',
                                                     'initial_start_time': 'not-ISO-sorry'})

        # Should raise an exception if message format is protobuf but proto schema is not provided
        with self.assertRaises(InvalidConfigException):
            tap_kafka.validate_config({'topic': 'my_topic',
                                       'partitions': [],
                                       'group_id': 'my_group_id',
                                       'bootstrap_servers': 'server1,server2,server3',
                                       'message_format': 'protobuf',
                                       'initial_start_time': 'earliest'})

        self.assertIsNone(tap_kafka.validate_config({'topic': 'my_topic',
                                                     'partitions': [],
                                                     'group_id': 'my_group_id',
                                                     'bootstrap_servers': 'server1,server2,server3',
                                                     'message_format': 'protobuf',
                                                     'proto_schema': 'proto-schema',
                                                     'initial_start_time': 'latest'}))

    def test_generate_schema_with_no_pk(self):
        """Should not add extra column when no PK defined"""
        self.assertDictEqual(common.generate_schema([]),
            {
                "type": "object",
                "properties": {
                    "message_timestamp": {"type": ["integer", "string", "null"]},
                    "message_offset": {"type": ["integer", "null"]},
                    "message_partition": {"type": ["integer", "null"]},
                    "message": {"type": ["object", "array", "string", "null"]}
                }
            })

    def test_generate_schema_with_pk(self):
        """Should add one extra column if PK defined"""
        self.assertDictEqual(common.generate_schema(["id"]),
            {
                "type": "object",
                "properties": {
                    "id": {"type": ["string"]},
                    "message_timestamp": {"type": ["integer", "string", "null"]},
                    "message_offset": {"type": ["integer", "null"]},
                    "message_partition": {"type": ["integer", "null"]},
                    "message": {"type": ["object", "array", "string", "null"]}
                }
            })

    def test_generate_schema_with_composite_pk(self):
        """Should add multiple extra columns if composite PK defined"""
        self.assertDictEqual(common.generate_schema(["id", "version"]),
            {
                "type": "object",
                "properties": {
                    "id": {"type": ["string"]},
                    "version": {"type": ["string"]},
                    "message_timestamp": {"type": ["integer", "string", "null"]},
                    "message_offset": {"type": ["integer", "null"]},
                    "message_partition": {"type": ["integer", "null"]},
                    "message": {"type": ["object", "array", "string", "null"]}
                }
            })

    def test_generate_catalog_with_no_pk(self):
        """table-key-properties cannot be empty when custom PK is not defined and default config is used"""
        self.assertListEqual(common.generate_catalog({"topic": "dummy_topic", 'use_message_key': True}),
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": ['message_key']}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                'message_key': {'type': ['string']},
                                "message_timestamp": {"type": ["integer", "string", "null"]},
                                "message_offset": {"type": ["integer", "null"]},
                                "message_partition": {"type": ["integer", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ])

    def test_generate_catalog_with_no_keys(self):
        """table-key-properties should be empty when custom PK is not defined and default config overridden"""
        self.assertListEqual(common.generate_catalog({"topic": "dummy_topic", 'use_message_key': False}),
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                               "metadata": {"table-key-properties": []}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                "message_timestamp": {"type": ["integer", "string", "null"]},
                                "message_offset": {"type": ["integer", "null"]},
                                "message_partition": {"type": ["integer", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ])

    def test_generate_catalog_with_pk(self):
        """table-key-properties should be a list with single item when PK defined"""
        self.assertListEqual(common.generate_catalog({"topic": "dummy_topic", "primary_keys": {"id": "^.dummyJson.id"}}),
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": ["id"]}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                "id": {"type": ["string"]},
                                "message_timestamp": {"type": ["integer", "string", "null"]},
                                "message_offset": {"type": ["integer", "null"]},
                                "message_partition": {"type": ["integer", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ])

    def test_generate_catalog_with_composite_pk(self):
        """table-key-properties should be a list with two items when composite PK defined"""
        self.assertListEqual(common.generate_catalog({"topic": "dummy_topic",
                                                  "primary_keys":{
                                                      "id": "dummyJson.id", "version": "dummyJson.version"}
                                                  }),
               [
                   {
                       "metadata": [
                           {
                               "breadcrumb": (),
                                "metadata": {"table-key-properties": ["id", "version"]}
                           }
                       ],
                       "schema": {
                           "type": "object",
                           "properties": {
                                "id": {"type": ["string"]},
                                "version": {"type": ["string"]},
                                "message_timestamp": {"type": ["integer", "string", "null"]},
                                "message_offset": {"type": ["integer", "null"]},
                                "message_partition": {"type": ["integer", "null"]},
                                "message": {"type": ["object", "array", "string", "null"]}
                           }
                       },
                       "tap_stream_id": "dummy_topic"
                   }
               ])

    def test_get_timestamp_from_timestamp_tuple__invalid_tuple(self):
        """Argument needs to be a tuple"""
        # Passing number should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple(0)

        # String should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple("not-a-tuple")

        # List should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple([])

        # Valid timestamp but as list should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple([confluent_kafka.TIMESTAMP_CREATE_TIME, 123456789])

        # Dict should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple({})

        # Empty tuple should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple(())

        # Tuple with one element should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple(tuple([confluent_kafka.TIMESTAMP_CREATE_TIME]))

        # Zero timestamp should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple((confluent_kafka.TIMESTAMP_CREATE_TIME, 0))

        # Negative timestamp should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple((confluent_kafka.TIMESTAMP_CREATE_TIME, -9876))

    def test_get_timestamp_from_timestamp_tuple__valid_tuple(self):
        """Argument needs to be a tuple"""
        self.assertEqual(sync.get_timestamp_from_timestamp_tuple((confluent_kafka.TIMESTAMP_CREATE_TIME, 9876)), 9876)

    def test_search_in_list_of_dict_by_key_value(self):
        """Search in list of dictionaries by key and value"""
        # No match should return -1
        list_of_dict = [{}, {'search_key': 'search_val_X'}]
        self.assertEqual(sync.search_in_list_of_dict_by_key_value(list_of_dict, 'search_key', 'search_val'), -1)

        # Should return second position (1)
        list_of_dict = [{}, {'search_key': 'search_val'}]
        self.assertEqual(sync.search_in_list_of_dict_by_key_value(list_of_dict, 'search_key', 'search_val'), 1)

        # Multiple match should return the first match postiong (0)
        list_of_dict = [{'search_key': 'search_val'}, {'search_key': 'search_val'}]
        self.assertEqual(sync.search_in_list_of_dict_by_key_value(list_of_dict, 'search_key', 'search_val'), 0)

    def test_send_activate_version_message(self):
        """ACTIVATE_VERSION message should be generated from bookmark"""
        singer_messages = []

        # Store output singer messages in an array
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # If no bookmarked version then it should generate a timestamp
        state = _get_resource_from_json('state-with-bookmark-with-version.json')
        sync.send_activate_version_message(state, 'dummy_topic')
        self.assertListEqual(singer_messages, [
            {
                'stream': 'dummy_topic',
                'type': 'ACTIVATE_VERSION',
                'version': 9999
            }
        ])

        # If no bookmarked version then it should generate a timestamp
        singer_messages = []
        now = int(time.time() * 1000)
        state = _get_resource_from_json('state-with-bookmark.json')
        sync.send_activate_version_message(state, 'dummy_topic')
        self.assertGreaterEqual(singer_messages[0]['version'], now)
        self.assertListEqual(singer_messages, [
            {
                'stream': 'dummy_topic',
                'type': 'ACTIVATE_VERSION',
                'version': singer_messages[0]['version']
            }
        ])

    def test_send_schema_message(self):
        """SCHEME message should be generated from catalog"""
        singer_messages = []

        # Store output singer messages in an array
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        catalog = _get_resource_from_json('catalog.json')
        streams = catalog.get('streams', [])
        topic_pos = sync.search_in_list_of_dict_by_key_value(streams, 'tap_stream_id', 'dummy_topic')
        stream = streams[topic_pos]

        sync.send_schema_message(stream)
        self.assertListEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': 'dummy_topic',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'id': {'type': ['string']},
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']}
                    }
                },
                'key_properties': ['id']
            }
        ])

    def test_update_bookmark__on_empty_state(self):
        """Updating empty state should generate a new bookmark"""
        topic = 'test-topic'
        input_state = {}
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=1234,
                                           partition=0)
        self.assertDictEqual(sync.update_bookmark(input_state, topic, message),
            {'bookmarks': {'test-topic': {'partition_0': {
                'partition': 0,
                'offset': 1234,
                'start_time': '2009-02-13T23:31:30.123',
                'timestamp': 1234567890123
            }}}})

    def test_update_bookmark__update_stream(self):
        """Updating existing bookmark in state should update at every property"""
        topic = 'test-topic-updated'
        input_state = {'bookmarks': {'test-topic-updated': {'partition_0': {'partition': 0,
                                                                            'offset': 1234,
                                                                            'timestamp': 111}}}}
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=999,
                                           partition=0)

        self.assertDictEqual(sync.update_bookmark(input_state, topic, message),
            {'bookmarks': {'test-topic-updated': {'partition_0': {
                'partition': 0,
                'offset': 999,
                'start_time': '2009-02-13T23:31:30.123',
                'timestamp': 1234567890123
            }}}})

    def test_update_bookmark__comment(self):
        """Updating existing bookmark in state should update at every property"""
        topic = 'test-topic-updated'
        input_state = {'bookmarks': {'test-topic-updated': {'partition_0': {'partition': 0,
                                                                            'offset': 1234,
                                                                            'timestamp': 111}}}}
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=111,
                                           partition=1)

        self.assertDictEqual(sync.update_bookmark(input_state, topic, message, comment=True), {'bookmarks': {'test-topic-updated': {
                                'partition_0': {
                                    'partition': 0,
                                    'offset': 1234,
                                    'timestamp': 111},
                                'partition_1': {
                                    'partition': 1,
                                    'offset': 111,
                                    'timestamp': 1234567890123,
                                    'start_time': '2009-02-13T23:31:30.123',
                                    '_comment': 'order of precedence : offset, timestamp, start_time; only one will be used'
                                }}}})


    def test_update_bookmark__no_comment(self):
        """Updating existing bookmark in state should update at every property"""
        topic = 'test-topic-updated'
        input_state = {'bookmarks': {'test-topic-updated': {'partition_0': {'partition': 0,
                                                                            'offset': 1234,
                                                                            'timestamp': 111}}}}
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=111,
                                           partition=1)

        self.assertDictEqual(sync.update_bookmark(input_state, topic, message), {'bookmarks': {'test-topic-updated': {
                                'partition_0': {
                                    'partition': 0,
                                    'offset': 1234,
                                    'timestamp': 111},
                                'partition_1': {
                                    'partition': 1,
                                    'offset': 111,
                                    'timestamp': 1234567890123,
                                    'start_time': '2009-02-13T23:31:30.123'
                                }}}})


    def test_update_bookmark__add_new_partition(self):
        """Updating existing bookmark in state should update at every property"""
        topic = 'test-topic-updated'
        input_state = {'bookmarks': {'test-topic-updated': {'partition_0': {'partition': 0,
                                                                            'offset': 1234,
                                                                            'timestamp': 111}}}}
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=111,
                                           partition=1)

        self.assertDictEqual(sync.update_bookmark(input_state, topic, message), {'bookmarks': {'test-topic-updated': {
                                'partition_0': {
                                    'partition': 0,
                                    'offset': 1234,
                                    'timestamp': 111},
                                'partition_1': {
                                    'partition': 1,
                                    'offset': 111,
                                    'timestamp': 1234567890123,
                                    'start_time': '2009-02-13T23:31:30.123'
                                }}}})


    def test_update_bookmark__update_partition(self):
        """Updating existing bookmark in state should update at every property"""
        topic = 'test-topic-updated'
        input_state = {'bookmarks': {'test-topic-updated': {'partition_0': {'partition': 0,
                                                                            'offset': 1234,
                                                                            'timestamp': 111},
                                                            'partition_1': {'partition': 0,
                                                                            'offset': 1234,
                                                                            'timestamp': 111}}}}
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=111,
                                           partition=1)

        self.assertDictEqual(sync.update_bookmark(input_state, topic, message),{'bookmarks': {'test-topic-updated': {
                                'partition_0': {
                                    'partition': 0,
                                    'offset': 1234,
                                    'timestamp': 111},
                                'partition_1': {
                                    'partition': 1,
                                    'offset': 111,
                                    'timestamp': 1234567890123,
                                    'start_time': '2009-02-13T23:31:30.123'
                                }}}})


    def test_update_bookmark__add_new_stream(self):
        """Updating a not existing stream id should be appended to the bookmarks dictionary"""
        input_state = {'bookmarks': {'test-topic-0': {'partition_0': {'partition': 0,
                                                                      'offset': 1234,
                                                                      'timestamp': 111},
                                                      'partition_1': {'partition': 1,
                                                                      'offset': 111,
                                                                      'timestamp': 1234}}}}
        message = KafkaConsumerMessageMock(topic='test-topic-1',
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=111,
                                           partition=0)

        self.assertDictEqual(sync.update_bookmark(input_state, 'test-topic-1', message),
            {'bookmarks': {'test-topic-0': {'partition_0': {'partition': 0,
                                                            'offset': 1234,
                                                            'timestamp': 111},
                                            'partition_1': {'partition': 1,
                                                            'offset': 111,
                                                            'timestamp': 1234}},
                           'test-topic-1': {'partition_0': {'partition': 0,
                                                            'offset': 111,
                                                            'timestamp': 1234567890123,
                                                            'start_time': '2009-02-13T23:31:30.123'
                                }}}})

    def test_update_bookmark__not_integer(self):
        """Timestamp in the bookmark should be auto-converted to int whenever it's possible"""
        topic = 'test-topic-updated'
        input_state = {'bookmarks': {topic: {'partition_0': {'partition': 0,
                                                             'offset': 1234,
                                                             'timestamp': 111}}}}

        # Timestamp should be converted from string to int
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, "1234567890123"),
                                           offset=111,
                                           partition=0)
        self.assertDictEqual(sync.update_bookmark(input_state, topic, message),
            {'bookmarks': {'test-topic-updated': {'partition_0': {'partition': 0,
                                                                  'offset': 111,
                                                                  'timestamp': 1234567890123,
                                                                  'start_time': '2009-02-13T23:31:30.123'
                                }}}})

        # Timestamp that cannot be converted to int should raise exception
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, "this-is-not-numeric"),
                                           offset=111,
                                           partition=0)
        with self.assertRaises(InvalidTimestampException):
            sync.update_bookmark(input_state, topic, message)


    def test_kafka_message_to_singer_record(self):
        """Validate if kafka messages converted to singer messages correctly"""
        topic = 'test-topic'

        # Converting without custom primary or message key
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=1234,
                                           partition=0)
        primary_keys = {}
        self.assertDictEqual(sync.kafka_message_to_singer_record(message, primary_keys, use_message_key=False), {
            'message': {'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
            'message_timestamp': 1234567890123,
            'message_offset': 1234,
            'message_partition': 0
        })

        # Converting with message key
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=1234,
                                           partition=0,
                                           key='1')

        primary_keys = {}
        self.assertDictEqual(sync.kafka_message_to_singer_record(message, primary_keys, use_message_key=True), {
            'message': {'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
            'message_timestamp': 1234567890123,
            'message_offset': 1234,
            'message_partition': 0,
            'message_key': '1'
        })

        # Converting with custom primary key and default setting for message key
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=1234,
                                           partition=0)
        primary_keys = {'id': '/id'}
        self.assertDictEqual(sync.kafka_message_to_singer_record(message, primary_keys, use_message_key=True), {
            'message': {'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
            'id': 1,
            'message_timestamp': 1234567890123,
            'message_offset': 1234,
            'message_partition': 0
        })

        # Converting with nested and multiple custom primary keys and default setting for message key
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=1234,
                                           partition=0)
        primary_keys = {'id': '/id', 'y': '/data/y'}
        self.assertDictEqual(sync.kafka_message_to_singer_record(message, primary_keys, use_message_key=True), {
            'message': {'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
            'id': 1,
            'y': 'value-y',
            'message_timestamp': 1234567890123,
            'message_offset': 1234,
            'message_partition': 0
        })

        # Converting with not existing custom primary keys
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=1234,
                                           partition=0)
        primary_keys = {'id': '/id', 'not-existing-key': '/path/not/exists'}

        with self.assertRaises(PrimaryKeyNotFoundException):
            sync.kafka_message_to_singer_record(message, primary_keys, use_message_key=False)

        # Converting without custom PK and absent message key with default settings
        message = KafkaConsumerMessageMock(topic=topic,
                                           value={'id': 1, 'data': {'x': 'value-x', 'y': 'value-y'}},
                                           timestamp=(confluent_kafka.TIMESTAMP_CREATE_TIME, 1234567890123),
                                           offset=1234,
                                           partition=0)
        primary_keys = {}

        with self.assertRaises(PrimaryKeyNotFoundException):
            sync.kafka_message_to_singer_record(message, primary_keys, use_message_key=True)

    def test_commit_consumer_to_bookmarked_state(self):
        """Commit should commit every partition in the bookmark state"""
        topic = 'test_topic'

        # If one partition bookmarked then need to commit one offset
        state = {'bookmarks': {topic: {'partition_0': {'partition': 0,
                                                       'offset': 1234,
                                                       'timestamp': 1234567890123}}}}
        consumer = KafkaConsumerMock(fake_messages=[])
        sync.commit_consumer_to_bookmarked_state(consumer, topic, state)
        self.assertListEqual(consumer.committed_offsets, [
            confluent_kafka.TopicPartition(topic=topic, partition=0, offset=1234)
        ])

        # If multiple partitions bookmarked then need to commit every offset
        state = {'bookmarks': {topic: {'partition_0': {'partition': 0,
                                                       'offset': 1234,
                                                       'timestamp': 1234567890123},
                                       'partition_1': {'partition': 1,
                                                       'offset': 2345,
                                                       'timestamp': 1234567890123},
                                       'partition_2': {'partition': 2,
                                                       'offset': 3456,
                                                       'timestamp': 1234567890123}
                                       }}}
        consumer = KafkaConsumerMock(fake_messages=[])
        sync.commit_consumer_to_bookmarked_state(consumer, topic, state)
        self.assertListEqual(consumer.committed_offsets, [
            confluent_kafka.TopicPartition(topic=topic, partition=0, offset=1234),
            confluent_kafka.TopicPartition(topic=topic, partition=1, offset=2345),
            confluent_kafka.TopicPartition(topic=topic, partition=2, offset=3456)
        ])


    def test_bookmarked_partition_offset(self):
        """Transform a bookmarked partition to a kafka TopicPartition object"""
        topic = 'test_topic'
        consumer = KafkaConsumerMock(fake_messages=[])
        partition_bookmark = {'partition': 0, 'offset': 1234, 'timestamp': 1638132327000}

        # By default TopicPartition offset needs to be bookmarked offset
        topic_partition = sync.bookmarked_partition_offset(consumer, topic, partition_bookmark)
        self.assertEqual(topic_partition.topic, topic)
        self.assertEqual(topic_partition.partition, 0)
        self.assertEqual(topic_partition.offset, 1234)

        partition_bookmark = {'partition': 0, 'timestamp': 1638132327000}

        # Assigning by bookmark without offset should use timestamp to calculate offset
        topic_partition = sync.bookmarked_partition_offset(consumer, topic, partition_bookmark)
        self.assertEqual(topic_partition.topic, topic)
        self.assertEqual(topic_partition.partition, 0)
        self.assertEqual(topic_partition.offset, 1234)


    def test_bookmarked_partition_to_next_position__invalid_options(self):
        """Transform a bookmarked partition to a kafka TopicPartition object"""
        topic = 'test_topic'
        consumer = KafkaConsumerMock(fake_messages=[])

        # Empty bookmark should raise exception
        partition_bookmark = {}
        with self.assertRaises(InvalidBookmarkException):
            sync.bookmarked_partition_offset(consumer, topic, partition_bookmark)

        # Partially provided bookmark - no partition
        partition_bookmark = {'offset': 1234, 'timestamp': 1638132327000}
        with self.assertRaises(InvalidBookmarkException):
            sync.bookmarked_partition_offset(consumer, topic, partition_bookmark)

        # Should raise an exception if partition is not int
        partition_bookmark = {'partition': '0', 'offset': 1234, 'timestamp': 1638132327000}
        with self.assertRaises(InvalidBookmarkException):
            sync.bookmarked_partition_offset(consumer, topic, partition_bookmark)

        # Should raise an exception if offset is not int
        partition_bookmark = {'partition': 0, 'offset': '1234', 'timestamp': 1638132327000}
        with self.assertRaises(InvalidBookmarkException):
            sync.bookmarked_partition_offset(consumer, topic, partition_bookmark)

        # Should raise an exception if start_time is not ISO timestamp
        partition_bookmark = {'partition': 0, 'start_time': 'not-ISO-sorry'}
        with self.assertRaises(InvalidTimestampException):
            sync.bookmarked_partition_offset(consumer, topic, partition_bookmark)

    def test_do_disovery_failure(self):
        """Validate if kafka messages converted to singer messages correctly"""
        minimal_config = {
            'topic': 'not_existing_topic',
            'group_id': 'my_group_id',
            'bootstrap_servers': 'not-existing-server1,not-existing-server2',
            'session_timeout_ms': 1000,
        }
        config = tap_kafka.generate_config(minimal_config)

        with self.assertRaises(DiscoveryException):
            tap_kafka.do_discovery(config)


    def test_iso_timestamp_to_epoch(self):
        """Validate converting ISO timestamps to epoch milliseconds"""
        # Using space as date and time delimiter
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01 23:01:11'), 1635807671000)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01 23:01:11.123'), 1635807671123)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01 23:01:11.123456'), 1635807671123)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01 23:01:11.123987'), 1635807671123)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01 23:01:11.123987+00:00'), 1635807671123)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-02 02:01:11.123987+03:00'), 1635807671123)

        # Using T as date and time delimiter
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01T23:01:11'), 1635807671000)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01T23:01:11.123'), 1635807671123)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01T23:01:11.123456'), 1635807671123)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01T23:01:11.123987'), 1635807671123)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-01T23:01:11.123987+00:00'), 1635807671123)
        self.assertEqual(sync.iso_timestamp_to_epoch('2021-11-02T02:01:11.123987+03:00'), 1635807671123)

        # Invalid ISO 8601 format should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.iso_timestamp_to_epoch('invalid-timestamp')

    def test_epoch_to_iso_timestamp(self):
        """Validate converting epoch milliseconds to ISO timestamp"""
        self.assertEqual(sync.epoch_to_iso_timestamp(1635807671000),'2021-11-01T23:01:11.000')
        self.assertEqual(sync.epoch_to_iso_timestamp(1635807671123),'2021-11-01T23:01:11.123')

        # Invalid epoch should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.epoch_to_iso_timestamp(None)

        # Invalid epoch should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.epoch_to_iso_timestamp('sss')

        # too short epoch should raise exception
        with self.assertRaises(InvalidTimestampException):
            sync.epoch_to_iso_timestamp(163580767100)

        # too long epoch should raise exception
        # If someone is fixing this on 'Sat, 20 Nov 2286 17:46:40 GMT', I'm sorry, but also a very proud ghost.
        with self.assertRaises(InvalidTimestampException):
            sync.epoch_to_iso_timestamp(16358076710000)


    def test_get_timestamp_from_timestamp_tuple(self):
        """Validate if the actual timestamp can be extracted from a kafka timestamp"""
        # Timestamps as tuples
        self.assertEqual(sync.get_timestamp_from_timestamp_tuple((confluent_kafka.TIMESTAMP_CREATE_TIME, 1234)), 1234)
        self.assertEqual(sync.get_timestamp_from_timestamp_tuple((confluent_kafka.TIMESTAMP_LOG_APPEND_TIME, 1234)), 1234)

        # Timestamp not available
        with self.assertRaises(TimestampNotAvailableException):
            sync.get_timestamp_from_timestamp_tuple((confluent_kafka.TIMESTAMP_NOT_AVAILABLE, 1234))

        # Invalid timestamp type
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple(([confluent_kafka.TIMESTAMP_CREATE_TIME, 1234], 1234))

        # Invalid timestamp type
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple((9999, 1234))

        # Invalid timestamp type
        with self.assertRaises(InvalidTimestampException):
            sync.get_timestamp_from_timestamp_tuple("not_a_tuple_or_list")

if __name__ == '__main__':
    unittest.main()
