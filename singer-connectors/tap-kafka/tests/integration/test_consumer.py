import unittest
import time
import singer

from confluent_kafka import KafkaException
from datetime import datetime

import tap_kafka.serialization
import tests.integration.utils as test_utils

from tap_kafka import sync
from tap_kafka.errors import DiscoveryException
from tap_kafka.errors import InvalidConfigException
from tap_kafka.serialization.json_with_no_schema import JSONSimpleSerializer

SINGER_MESSAGES = []


def accumulate_singer_messages(message):
    singer_message = singer.parse_message(message)
    SINGER_MESSAGES.append(singer_message)


def message_types(messages):
    message_types_set = set()
    for message in messages:
        message_types_set.add(type(message))
    return message_types_set


class TestKafkaConsumer(unittest.TestCase):

    def test_tap_kafka_discovery(self):
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic_name = test_utils.create_topic(kafka_config['bootstrap_servers'],
                                             'test-topic-for-discovery',
                                             num_partitions=4)

        # Consume test messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic_name,
            'group_id': 'tap_kafka_integration_test',
        })

        catalog_streams = []
        tap_kafka.dump_catalog = lambda c: catalog_streams.extend(c)
        tap_kafka.do_discovery(tap_kafka_config)

        self.assertListEqual(catalog_streams, [
            {
                'tap_stream_id': catalog_streams[0]['tap_stream_id'],
                'metadata': [
                    {'breadcrumb': (), 'metadata': {'table-key-properties': ['message_key']}}
                ],
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_partition': {'type': ['integer', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']},
                        'message_key': {'type': ['string']}
                    }
                }
            }
        ])

    def test_tap_kafka_discovery_failure(self):
        kafka_config = test_utils.get_kafka_config()

        # Trying to discover topic
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': 'not-existing-topic',
            'group_id': 'tap_kafka_integration_test',
            'session_timeout_ms': 1000,
        })

        with self.assertRaises(DiscoveryException):
            tap_kafka.do_discovery(tap_kafka_config)

    def test_tap_kafka_consumer_brokers_down(self):
        # Consume test messages from not existing broker
        topic = 'fake'
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': 'localhost:12345',
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'session_timeout_ms': 1,
            'max_poll_interval_ms': 10
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        with self.assertRaises(KafkaException):
            sync.do_sync(tap_kafka_config, catalog, state={'bookmarks': {topic: {}}})

    def test_tap_kafka_consumer(self):
        kafka_config = test_utils.get_kafka_config()

        start_time = int(time.time() * 1000) - 60000

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-one', num_partitions=1)
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Consume test messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            "initial_start_time": "latest",
            'primary_keys': {
                'id': '/id'
            }
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # Should only receive one RECORD messages because we start consuming from latest
        sync.do_sync(tap_kafka_config, catalog, state={'bookmarks': {topic: {}}})
        self.assertEqual(len(singer_messages), 4)
        self.assertListEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']},
                        'id': {'type': ['string']}
                    }
                },
                'key_properties': ['id']
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message': {'id': 3, 'value': 'initial id 3'},
                    'message_partition': 0,
                    'message_offset': 4,
                    'message_timestamp': singer_messages[2]['record']['message_timestamp'],
                    'id': 3
                },
                'time_extracted': singer_messages[2]['time_extracted']
            },
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {
                        topic: singer_messages[3]['value']['bookmarks'][topic]
                    }
                }
            }
        ])

        # Position to the time when the test started
        singer_messages = []
        sync.do_sync(tap_kafka_config, catalog, state={
            'bookmarks': {
                topic: {'partition_0': {'partition': 0,
                                        'offset': 0,
                                        'timestamp': start_time}}}})

        self.assertEqual(len(singer_messages), 8)
        self.assertListEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']},
                        'id': {'type': ['string']}
                    }
                },
                'key_properties': ['id']
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 0,
                    'message_timestamp': singer_messages[2]['record']['message_timestamp'],
                    'message': {'id': 1, 'value': 'initial id 1'},
                    'id': 1
                },
                'time_extracted': singer_messages[2]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 1,
                    'message_timestamp': singer_messages[3]['record']['message_timestamp'],
                    'message': {'id': 1, 'value': 'updated id 1'},
                    'id': 1
                },
                'time_extracted': singer_messages[3]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 2,
                    'message_timestamp': singer_messages[4]['record']['message_timestamp'],
                    'message': {'id': 2, 'value': 'initial id 2'},
                    'id': 2
                },
                'time_extracted': singer_messages[4]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 3,
                    'message_timestamp': singer_messages[5]['record']['message_timestamp'],
                    'message': {'id': 2, 'value': 'updated id 2'},
                    'id': 2
                },
                'time_extracted': singer_messages[5]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 4,
                    'message_timestamp': singer_messages[6]['record']['message_timestamp'],
                    'message': {'id': 3, 'value': 'initial id 3'},
                    'id': 3
                },
                'time_extracted': singer_messages[6]['time_extracted']
            },
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {
                        topic: singer_messages[7]['value']['bookmarks'][topic]
                    }
                }
            }
        ])

        # Save state with bookmarks
        state = singer_messages[7]['value']

        # Produce some new messages
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce_2.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Position to the time in the state
        singer_messages = []
        sync.do_sync(tap_kafka_config, catalog, state=state)

        self.assertEqual(len(singer_messages), 7)

        self.assertListEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']},
                        'id': {'type': ['string']}
                    }
                },
                'key_properties': ['id']
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 4,
                    'message_timestamp': singer_messages[2]['record']['message_timestamp'],
                    'message': {'id': 3, 'value': 'initial id 3'},
                    'id': 3
                },
                'time_extracted': singer_messages[2]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 5,
                    'message_timestamp': singer_messages[3]['record']['message_timestamp'],
                    'message': {'id': 3, 'value': 'updated id 3'},
                    'id': 3
                },
                'time_extracted': singer_messages[3]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 6,
                    'message_timestamp': singer_messages[4]['record']['message_timestamp'],
                    'message': {'id': 4, 'value': 'initial id 4'},
                    'id': 4
                },
                'time_extracted': singer_messages[4]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 7,
                    'message_timestamp': singer_messages[5]['record']['message_timestamp'],
                    'message': {'id': 4, 'value': 'updated id 4'},
                    'id': 4
                },
                'time_extracted': singer_messages[5]['time_extracted']
            },
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {
                        topic: singer_messages[6]['value']['bookmarks'][topic]
                    }
                }
            }
        ])

    def test_tap_kafka_consumer_initial_start_time_beginning(self):
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-start', num_partitions=1)
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Consume test messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'initial_start_time': 'beginning'
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # Should receive all RECORD and STATE messages because we start consuming from the earliest
        sync.do_sync(tap_kafka_config, catalog, state={})
        self.assertEqual(len(singer_messages), 8)


    def test_tap_kafka_consumer_initial_start_time_earliest(self):
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-earliest', num_partitions=1)
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Consume test messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'initial_start_time': 'earliest'
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # Should receive all RECORD and STATE messages because we start consuming from the earliest
        sync.do_sync(tap_kafka_config, catalog, state={})
        self.assertEqual(len(singer_messages), 8)


    def test_tap_kafka_consumer_initial_start_time_latest(self):
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-latest', num_partitions=1)
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Consume test messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'initial_start_time': 'latest'
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # Should not receive any RECORD and STATE messages because we start consuming from latest
        sync.do_sync(tap_kafka_config, catalog, state={})
        self.assertEqual(len(singer_messages), 4)


    def test_tap_kafka_consumer_initial_start_time_iso_timestamp(self):
        kafka_config = test_utils.get_kafka_config()

        # Produce test messages
        topic = test_utils.create_topic(kafka_config['bootstrap_servers'], 'test-topic-init-start-iso-timestamp', num_partitions=1)
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Wait a couple of seconds before producing some more test messages
        time.sleep(2)

        # We will start consuming message starting from this moment
        initial_start_time = datetime.now().isoformat()

        # Produce some more test messages
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': JSONSimpleSerializer()},
            topic,
            test_utils.get_file_lines('json_messages_to_produce_2.json'),
            test_message_transformer={'func': test_utils.test_message_to_string},
        )

        # Consume test messages from a given timestamp
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'initial_start_time': initial_start_time
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        # Should receive RECORD and STATE messages only from json_messages_to_produce_2.json
        sync.do_sync(tap_kafka_config, catalog, state={})
        self.assertEqual(len(singer_messages), 6)
        self.assertListEqual(singer_messages, [
            {
                'type': 'SCHEMA',
                'stream': topic,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'message_key': {'type': ['string']},
                        'message_partition': {'type': ['integer', 'null']},
                        'message_offset': {'type': ['integer', 'null']},
                        'message_timestamp': {'type': ['integer', 'string', 'null']},
                        'message': {'type': ['object', 'array', 'string', 'null']}
                    }
                },
                'key_properties': ['message_key']
            },
            {
                'type': 'ACTIVATE_VERSION',
                'stream': topic,
                'version': singer_messages[1]['version']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 5,
                    'message_timestamp': singer_messages[2]['record']['message_timestamp'],
                    'message': {'id': 3, 'value': 'updated id 3'},
                    'message_key': '3'
                },
                'time_extracted': singer_messages[2]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 6,
                    'message_timestamp': singer_messages[3]['record']['message_timestamp'],
                    'message': {'id': 4, 'value': 'initial id 4'},
                    'message_key': '4'
                },
                'time_extracted': singer_messages[3]['time_extracted']
            },
            {
                'type': 'RECORD',
                'stream': topic,
                'record': {
                    'message_partition': 0,
                    'message_offset': 7,
                    'message_timestamp': singer_messages[4]['record']['message_timestamp'],
                    'message': {'id': 4, 'value': 'updated id 4'},
                    'message_key': '4'
                },
                'time_extracted': singer_messages[4]['time_extracted']
            },
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {
                        topic: singer_messages[5]['value']['bookmarks'][topic]
                    }
                }
            }
        ])


    def test_tap_kafka_select_partitions(self):
        tap_kafka_config = test_utils.get_kafka_config()

        # Should throw exception if topic not found on brokers
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': tap_kafka_config['bootstrap_servers'],
            'topic': 'fake-topic-name',
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'session_timeout_ms': 1000,
            'max_poll_interval_ms': 1000,
            'initial_start_time': 'beginning'
        })
        consumer = sync.init_kafka_consumer(tap_kafka_config)

        with self.assertRaises(InvalidConfigException):
            sync.select_kafka_partitions(consumer, tap_kafka_config)


        # Create topic of selection tests
        topic = test_utils.create_topic(tap_kafka_config['bootstrap_servers'], 'test-topic-select', num_partitions=3)


        # No selection Should return all 3 partitions
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': tap_kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'session_timeout_ms': 1000,
            'max_poll_interval_ms': 1000,
            'initial_start_time': 'beginning'
        })
        consumer = sync.init_kafka_consumer(tap_kafka_config)
        partitions = sync.select_kafka_partitions(consumer, tap_kafka_config)

        self.assertEqual(len(partitions), 3)


        # Specifying only partition 2 should return only partition 2
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': tap_kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'session_timeout_ms': 1000,
            'max_poll_interval_ms': 1000,
            'initial_start_time': 'beginning',
            'partitions': [2]
        })
        consumer = sync.init_kafka_consumer(tap_kafka_config)
        partitions = sync.select_kafka_partitions(consumer, tap_kafka_config)

        self.assertEqual(len(partitions), 1)
        self.assertEqual(partitions[0].partition, 2)


        # Specifying only partition 4 should return no partitions
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': tap_kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'session_timeout_ms': 1000,
            'max_poll_interval_ms': 1000,
            'initial_start_time': 'beginning',
            'partitions': [4]
        })
        consumer = sync.init_kafka_consumer(tap_kafka_config)
        partitions = sync.select_kafka_partitions(consumer, tap_kafka_config)

        self.assertEqual(len(partitions), 0)


        # Specifying partitions 1 and 5 should only return partition 1
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': tap_kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'session_timeout_ms': 1000,
            'max_poll_interval_ms': 1000,
            'initial_start_time': 'beginning',
            'partitions': [1,5]
        })
        consumer = sync.init_kafka_consumer(tap_kafka_config)
        partitions = sync.select_kafka_partitions(consumer, tap_kafka_config)

        self.assertEqual(len(partitions), 1)
        self.assertEqual(partitions[0].partition, 1)


        # Specifying partitions 0 and 2 should return partition 0 and 2
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': tap_kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'session_timeout_ms': 1000,
            'max_poll_interval_ms': 1000,
            'initial_start_time': 'beginning',
            'partitions': [0,2]
        })
        consumer = sync.init_kafka_consumer(tap_kafka_config)
        partitions = sync.select_kafka_partitions(consumer, tap_kafka_config)

        self.assertEqual(len(partitions), 2)

        partition_ids = []
        for partition in partitions:
            partition_ids.append(partition.partition)
        self.assertEqual(partition_ids, [0, 2])
