import sys
import json
import singer
from slack_sdk import WebClient

from tap_slack.client import SlackClient
from tap_slack.streams import AVAILABLE_STREAMS
from tap_slack.catalog import generate_catalog

LOGGER = singer.get_logger(__name__)


def auto_join(client, config):

    if "channels" in config:
        conversations = config.get("channels")

        for conversation_id in conversations:
            join_response = client.join_channel(channel=conversation_id)
            if not join_response.get("ok", False):
                error = join_response.get("error", "Unspecified Error")
                LOGGER.error('Error joining {}, Reason: {}'.format(conversation_id, error))
                raise Exception('{}: {}'.format(conversation_id, error))
    else:
        response = client.get_all_channels(types="public_channel", exclude_archived="true")
        conversations = response.get("channels", [])

        for conversation in conversations:
            conversation_id = conversation.get("id", None)
            conversation_name = conversation.get("name", None)
            join_response = client.join_channel(channel=conversation_id)
            if not join_response.get("ok", False):
                error = join_response.get("error", "Unspecified Error")
                LOGGER.error('Error joining {}, Reason: {}'.format(conversation_name, error))
                raise Exception('{}: {}'.format(conversation_name, error))


def discover(client):
    LOGGER.info('Starting Discovery..')
    streams = [stream_class(client) for _, stream_class in AVAILABLE_STREAMS.items()]
    catalog = generate_catalog(streams)
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished Discovery..")


def sync(client, config, catalog, state):
    LOGGER.info('Starting Sync..')
    selected_streams = catalog.get_selected_streams(state)

    streams = []
    stream_keys = []
    for catalog_entry in selected_streams:
        streams.append(catalog_entry)
        stream_keys.append(catalog_entry.stream)

    if "threads" in stream_keys and "messages" not in stream_keys:
        sync_messages = False
        streams.append(catalog.get_stream("messages"))
    elif "messages" in stream_keys:
        sync_messages = True
    else:
        sync_messages = False

    for catalog_entry in streams:
        if "threads" != catalog_entry.stream:
            if "messages" == catalog_entry.stream:
                stream = AVAILABLE_STREAMS[catalog_entry.stream](client=client, config=config,
                                                                 catalog=catalog,
                                                                 state=state,
                                                                 write_to_singer=sync_messages)
            else:
                stream = AVAILABLE_STREAMS[catalog_entry.stream](client=client, config=config,
                                                                 catalog=catalog,
                                                                 state=state)
            LOGGER.info('Syncing stream: %s', catalog_entry.stream)
            stream.write_schema()
            stream.sync(catalog_entry.metadata)
            stream.write_state()

    LOGGER.info('Finished Sync..')


def main():
    args = singer.utils.parse_args(required_config_keys=['token', 'start_date'])

    webclient = WebClient(token=args.config.get("token"))
    client = SlackClient(webclient=webclient, config=args.config)

    if args.discover:
        discover(client=client)
    elif args.catalog:
        if args.config.get("join_public_channels", "false") == "true":
            auto_join(client=client, config=args.config)
        sync(client=client, config=args.config, catalog=args.catalog, state=args.state)


if __name__ == '__main__':
    main()
