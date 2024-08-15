import os
import json
import unittest
import tap_kafka
import confluent_kafka
import singer

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

from tap_kafka import sync
from tap_kafka.errors import ProtobufCompilerException
from tap_kafka.serialization.protobuf import proto_to_message_type
from tap_kafka.serialization.protobuf import ProtobufDictDeserializer

import tests.integration.utils as test_utils


def message_value_to_protobuf(test_message_value: dict, message_type):
    return message_type(name=test_message_value['name'],
                        favourite_number=test_message_value['favourite_number'],
                        favourite_color=test_message_value['favourite_color'])


def create_protobuf_topic(kafka_config: dict, schema_path: str, num_partitions: int = 1):
    topic_name = test_utils.create_topic(kafka_config['bootstrap_servers'],
                                         'test-topic-protobuf.dummy.camelCase',
                                         num_partitions=num_partitions)
    protobuf_classes_dir = os.path.join(os.getcwd(), kafka_config['proto_classes_dir'])

    # Generate proto class message type from .proto schema
    schema = ''.join(test_utils.get_file_lines(schema_path))
    message_type = proto_to_message_type(schema, protobuf_classes_dir, topic_name)

    return {
        'topic_name': topic_name,
        'message_type': message_type,
        'protobuf_classes_dir': protobuf_classes_dir,
    }

class TestProtobuf(unittest.TestCase):
    def test_tap_kafka_protobuf(self):
        kafka_config = test_utils.get_kafka_config(extra_config={
            'proto_classes_dir': tap_kafka.DEFAULT_PROTO_CLASSES_DIR
        })
        protobuf_topic = create_protobuf_topic(kafka_config, schema_path='protobuf/user.proto')
        schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
        topic = protobuf_topic['topic_name']

        # Produce protobuf messages
        test_utils.produce_messages(
            {'bootstrap.servers': kafka_config['bootstrap_servers'],
             'value.serializer': ProtobufSerializer(protobuf_topic['message_type'],
                                                    schema_registry_client,
                                                    {
                                                        'use.deprecated.format': False
                                                    })},
            topic,
            test_utils.get_file_lines('protobuf/messages_to_produce.json'),
            test_message_transformer={'func': test_utils.test_message_to_protobuf,
                                      'conf': {
                                          'proto_fn': message_value_to_protobuf,
                                          'message_type': protobuf_topic['message_type'],
                                      }},
        )

        # Consume protobuf messages
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'consumer_timeout_ms': 1000,
            'initial_start_time': 'beginning',

            # set protobuf message_format and proto_schema
            'message_format': 'protobuf',
            'proto_schema': ''.join(test_utils.get_file_lines('protobuf/user.proto')),
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}

        # Mock items
        singer_messages = []
        singer.write_message = lambda m: singer_messages.append(m.asdict())

        sync.do_sync(tap_kafka_config, catalog, state={})
        self.assertEqual(len(singer_messages), 6)
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
                        'message_key': {'type': ['string']}
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
                    'message_offset': 0,
                    'message_timestamp': singer_messages[2]['record']['message_timestamp'],
                    'message': {
                        'name': 'Sir Lancelot of Camelot',
                        'favourite_number': '3',
                        'favourite_color': 'Blue'
                    },
                    'message_key': '1'
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
                    'message': {
                        'name': 'Sir Galahad of Camelot',
                        'favourite_number': '8',
                        'favourite_color': 'Yellow'
                    },
                    'message_key': '2'
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
                    'message': {
                        'name': 'Arthur, King of the Britons',
                        'favourite_number': '12',
                        'favourite_color': ''
                    },
                    'message_key': '3'
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

        # Invalid proto schema should raise exception
        tap_kafka_config = tap_kafka.generate_config({
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': topic,
            'group_id': test_utils.generate_unique_consumer_group(),
            'initial_start_time': 'earliest',

            # set protobuf message_format and proto_schema
            'message_format': 'protobuf',
            'proto_schema': 'invalid-proto-schema',
        })
        catalog = {'streams': tap_kafka.common.generate_catalog(tap_kafka_config)}
        with self.assertRaises(ProtobufCompilerException):
            sync.do_sync(tap_kafka_config, catalog, state={'bookmarks': {topic: {}}})
