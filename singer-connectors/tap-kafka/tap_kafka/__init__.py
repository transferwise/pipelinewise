"""pipelinewise-tap-kafka"""
import os
import sys
import json
import singer

from singer import utils
from confluent_kafka import Consumer, KafkaException

from tap_kafka import sync
from tap_kafka import common

from .errors import InvalidTimestampException, InvalidConfigException, DiscoveryException

LOGGER = singer.get_logger('tap_kafka')

REQUIRED_CONFIG_KEYS = [
    'bootstrap_servers',
    'group_id',
    'topic'
]

DEFAULT_INITIAL_START_TIME = 'latest'
DEFAULT_PARTITIONS = []
DEFAULT_MAX_RUNTIME_MS = 300000
DEFAULT_COMMIT_INTERVAL_MS = 5000
DEFAULT_CONSUMER_TIMEOUT_MS = 10000
DEFAULT_SESSION_TIMEOUT_MS = 30000
DEFAULT_HEARTBEAT_INTERVAL_MS = 10000
DEFAULT_MAX_POLL_INTERVAL_MS = 300000
DEFAULT_MAX_POLL_RECORDS = 500
DEFAULT_MESSAGE_FORMAT = 'json'
DEFAULT_PROTO_SCHEMA = None
DEFAULT_PROTO_CLASSES_DIR = os.path.join(os.getcwd(), 'tap-kafka-proto-classes')


def dump_catalog(all_streams):
    """Dump every stream catalog as JSON to STDOUT"""
    json.dump({'streams': all_streams}, sys.stdout, indent=2)


def do_discovery(config):
    """Discover kafka topic by trying to connect to the topic and generate singer schema
    according to the config"""
    consumer = Consumer({
        'bootstrap.servers': config['bootstrap_servers'],
        'group.id': config['group_id'],
        'auto.offset.reset': 'earliest',
    })

    try:
        topic = config['topic']

        LOGGER.info(f"Discovering {topic} topic...")
        cluster_md = consumer.list_topics(topic=topic, timeout=config['session_timeout_ms'] / 1000)
        topic_md = cluster_md.topics[topic]

        if topic_md.error:
            raise KafkaException(topic_md.error)

    except KafkaException as exc:
        LOGGER.warning("Unable to view topic %s. bootstrap_servers: %s, topic: %s, group_id: %s",
                       config['topic'],
                       config['bootstrap_servers'], config['topic'], config['group_id'])

        consumer.close()
        raise DiscoveryException('Unable to view topic {} - {}'.format(config['topic'], exc))

    dump_catalog(common.generate_catalog(config))
    consumer.close()


def get_args():
    return utils.parse_args(REQUIRED_CONFIG_KEYS)


def validate_config(config) -> None:
    """Validate configuration"""
    for required_key in REQUIRED_CONFIG_KEYS:
        if required_key not in config.keys():
            raise InvalidConfigException(f'Invalid config. {required_key} not found in config.')

    initial_start_time = config.get('initial_start_time')
    if initial_start_time and initial_start_time not in ['beginning', 'latest', 'earliest']:
        try:
            sync.iso_timestamp_to_epoch(config.get('initial_start_time'))
        except InvalidTimestampException:
            raise InvalidConfigException("Invalid config. initial_start_time needs to be one of 'beginning', 'earliest', 'latest' or an ISO-8601 formatted timestamp string")

    if not isinstance(config.get('partitions'), list):
        raise InvalidConfigException(f"Invalid config. 'partitions' must be a python 'list', not a {type(config.get('partitions'))}")

    if config.get('message_format') not in ['json', 'protobuf']:
        raise InvalidConfigException("Invalid config. 'message_format' needs to be one of 'json' or 'protobuf'")

    if config.get('message_format') == 'protobuf' and not config.get('proto_schema'):
        raise InvalidConfigException("Invalid config. Cannot find required proto_schema for protobuf message type")


def generate_config(args_config):
    config = {
        # Add required parameters
        'topic': args_config['topic'],
        'group_id': args_config['group_id'],
        'bootstrap_servers': args_config['bootstrap_servers'],

        # Add optional parameters with defaults
        'primary_keys': args_config.get('primary_keys', {}),
        'use_message_key': args_config.get('use_message_key', True),
        'initial_start_time': args_config.get('initial_start_time', DEFAULT_INITIAL_START_TIME),
        'partitions': args_config.get('partitions', DEFAULT_PARTITIONS),
        'max_runtime_ms': args_config.get('max_runtime_ms', DEFAULT_MAX_RUNTIME_MS),
        'commit_interval_ms': args_config.get('commit_interval_ms', DEFAULT_COMMIT_INTERVAL_MS),
        'consumer_timeout_ms': args_config.get('consumer_timeout_ms', DEFAULT_CONSUMER_TIMEOUT_MS),
        'session_timeout_ms': args_config.get('session_timeout_ms', DEFAULT_SESSION_TIMEOUT_MS),
        'heartbeat_interval_ms': args_config.get('heartbeat_interval_ms', DEFAULT_HEARTBEAT_INTERVAL_MS),
        'max_poll_records': args_config.get('max_poll_records', DEFAULT_MAX_POLL_RECORDS),
        'max_poll_interval_ms': args_config.get('max_poll_interval_ms', DEFAULT_MAX_POLL_INTERVAL_MS),
        'message_format': args_config.get('message_format', DEFAULT_MESSAGE_FORMAT),
        'proto_schema': args_config.get('proto_schema', DEFAULT_PROTO_SCHEMA),
        'proto_classes_dir': args_config.get('proto_classes_dir', DEFAULT_PROTO_CLASSES_DIR),
        'debug_contexts': args_config.get('debug_contexts'),
    }

    validate_config(config)
    return config


def main_impl():
    """Main tap-kafka implementation"""
    args = get_args()
    kafka_config = generate_config(args.config)

    if args.discover:
        do_discovery(kafka_config)
    elif args.properties:
        state = args.state or {}
        sync.do_sync(kafka_config, args.properties, state)
    else:
        LOGGER.info("No properties were selected")


def main():
    """Main entry point"""
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
