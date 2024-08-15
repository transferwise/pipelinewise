import os
import json
from datetime import datetime
from typing import Dict, List

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic


def get_file_lines(filename: str) -> List:
    with open(f'{os.path.dirname(__file__)}/resources/{filename}') as f_lines:
        return f_lines.readlines()


def get_kafka_config(extra_config: Dict = None) -> Dict:
    default_config = {
        'bootstrap_servers': os.environ['TAP_KAFKA_BOOTSTRAP_SERVERS']
    }

    if extra_config:
        return {**default_config, **extra_config}

    return default_config


def delete_topic(
        bootstrap_servers: str,
        topic_name: str
) -> None:
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    fs = admin_client.delete_topics([topic_name], operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print('Topic {} deleted'.format(topic))
        except Exception as exc:
            print('Failed to delete topic {}: {}'.format(topic, exc))
            raise exc


def create_topic(
        bootstrap_servers: str,
        topic_name: str,
        num_partitions: int = 1,
        replica_factor: int = 1
) -> str:
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    now = datetime.now().strftime('%Y%m%d_%H%M%S')
    full_topic_name = f'{topic_name}_{now}'
    fs = admin_client.create_topics([NewTopic(full_topic_name, num_partitions, replica_factor)])

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f'Topic {topic} created')
            return topic
        except Exception as exc:
            print(f'Failed to create topic {topic}: {exc}')
            raise exc


def generate_unique_consumer_group(prefix='tap_kafka_integration_test'):
    return f"{prefix}{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def test_message_to_string(message: str, conf: dict = {}) -> object:
    message_dict = json.loads(message)
    message_key = str(message_dict['message_key'])
    message_value = message_dict['message']

    return {'key': message_key,
            'value': message_value}


def test_message_to_protobuf(message: str, conf: dict) -> object:
    message_dict = json.loads(message)
    message_key = message_dict['message_key']
    message_value = conf['proto_fn'](message_dict['message'], conf['message_type'])

    return {'key': message_key,
            'value': message_value}


def produce_messages(
        producer_config: dict,
        topic_name: str,
        test_messages: List[str],
        test_message_transformer
) -> None:
    p = SerializingProducer(producer_config)

    def delivery_report(err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    # Transformer from JSON test messages to desired types
    transformer_func = test_message_transformer['func']
    transformer_conf = test_message_transformer.get('conf', {})

    for test_message in test_messages:
        test_message_dict = transformer_func(test_message, transformer_conf)

        kafka_msg_key = test_message_dict['key']
        kafka_msg_value = test_message_dict['value']

        p.poll(0)
        p.produce(topic_name, key=kafka_msg_key, value=kafka_msg_value, on_delivery=delivery_report)

    p.flush()
