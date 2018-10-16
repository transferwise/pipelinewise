import singer
from singer import utils
from kafka import KafkaConsumer
import json
import pdb
import sys
import tap_kafka.sync as sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'group_id',
    'bootstrap_servers',
    'topic'
    # 'schema',
    # 'primary_keys',
    # 'message_serialization'
]

def dump_catalog(all_streams):
    json.dump({'streams' : all_streams}, sys.stdout, indent=2)

def do_discovery(config):
    try:
        consumer = KafkaConsumer(config['topic'],
                                 group_id=config['group_id'],
                                 enable_auto_commit=False,
                                 consumer_timeout_ms=10000,
                                 #value_deserializer=lambda m: json.loads(m.decode('ascii'))
                                 bootstrap_servers=config['bootstrap_servers'].split(','))
    except Exception as ex:
        LOGGER.warn("Unable to connect to kafka. bootstrap_servers: %s, topic: %s, group_id: %s", config['bootstrap_servers'].split(','), config['topic'], config['group_id'])
        LOGGER.warn(ex)
        raise ex

    if config['topic'] not in consumer.topics():
        LOGGER.warn("Unable to view topic %s. bootstrap_servers: %s, topic: %s, group_id: %s", config['topic'], config['bootstrap_servers'].split(','), config['topic'], config['group_id'])
        raise Exception('Unable to view topic {}'.format(config['topic']))

    dump_catalog(common.default_streams(config))



def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    kafka_config = {'topic' : args.config['topic'],
                    'group_id' : args.config['group_id'],
                    'reject_topic': args.config.get('reject_topic'),
                    'bootstrap_servers': args.config['bootstrap_servers'].split(',')}

    if args.discover:
        do_discovery(args.config)
    elif args.properties:
        state = args.state or {}
        # streams = args.properties or {'streams' : common.default_streams(kafka_config)}
        sync.do_sync(kafka_config, args.properties, state)
    else:
        LOGGER.info("No properties were selected")

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == "__main__":
    main()
