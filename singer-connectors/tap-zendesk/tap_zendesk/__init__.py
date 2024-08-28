#!/usr/bin/env python3
# pylint: disable=invalid-name,missing-function-docstring,missing-class-docstring,unused-argument
import json
import os
import sys
import itertools
import requests
import singer

from zenpy import Zenpy
from requests import Session
from requests.adapters import HTTPAdapter
from singer import metadata, metrics as singer_metrics
from tap_zendesk import metrics as zendesk_metrics
from tap_zendesk.discover import discover_streams
from tap_zendesk.streams import STREAMS
from tap_zendesk.sync import sync_stream

LOGGER = singer.get_logger('tap_zendesk')

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "subdomain",
]

# default authentication
OAUTH_CONFIG_KEYS = [
    "access_token",
]

# email + api_token authentication
API_TOKEN_CONFIG_KEYS = [
    "email",
    "api_token",
]

# patch Session.request to record HTTP request metrics
request = Session.request


def request_metrics_patch(self, method, url, **kwargs):
    with singer_metrics.http_request_timer(None):
        return request(self, method, url, **kwargs)


Session.request = request_metrics_patch


# end patch

def do_discover(client):
    LOGGER.info("Starting discover")
    catalog = {"streams": discover_streams(client)}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def get_selected_streams(catalog):
    selected_stream_names = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_names.append(stream.tap_stream_id)
    return selected_stream_names


SUB_STREAMS = {
    'tickets': ['ticket_audits', 'ticket_metrics', 'ticket_comments']
}


def get_sub_stream_names():
    """
    Get all sub_streams as one list
    """
    return list(itertools.chain(*SUB_STREAMS.values()))


class DependencyException(Exception):
    pass


def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")
    for parent_stream_name, sub_stream_names in SUB_STREAMS.items():
        for sub_stream_name in sub_stream_names:
            if sub_stream_name in selected_stream_ids and parent_stream_name not in selected_stream_ids:
                errs.append(msg_tmpl.format(sub_stream_name, parent_stream_name))

    if errs:
        raise DependencyException(" ".join(errs))


def populate_class_schemas(catalog, selected_stream_names):
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_stream_names:
            STREAMS[stream.tap_stream_id].stream = stream


# pylint: disable=too-many-locals
def do_sync(client, catalog, state, start_date):
    selected_stream_names = get_selected_streams(catalog)
    validate_dependencies(selected_stream_names)
    populate_class_schemas(catalog, selected_stream_names)
    all_sub_stream_names = get_sub_stream_names()

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)
        if stream_name not in selected_stream_names:
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        # if starting_stream:
        #     if starting_stream == stream_name:
        #         LOGGER.info("%s: Resuming", stream_name)
        #         starting_stream = None
        #     else:
        #         LOGGER.info("%s: Skipping - already synced", stream_name)
        #         continue
        # else:
        #     LOGGER.info("%s: Starting", stream_name)

        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        sub_stream_names = SUB_STREAMS.get(stream_name)
        if sub_stream_names:
            for sub_stream_name in sub_stream_names:
                if sub_stream_name not in selected_stream_names:
                    continue
                sub_stream = STREAMS[sub_stream_name].stream
                sub_mdata = metadata.to_map(sub_stream.metadata)
                sub_key_properties = metadata.get(sub_mdata, (), 'table-key-properties')
                singer.write_schema(sub_stream.tap_stream_id, sub_stream.schema.to_dict(), sub_key_properties)

        # parent stream will sync sub stream
        if stream_name in all_sub_stream_names:
            continue

        LOGGER.info("%s: Starting sync", stream_name)
        instance = STREAMS[stream_name](client)
        counter_value = sync_stream(state, start_date, instance)
        singer.write_state(state)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)
        zendesk_metrics.log_aggregate_rates()

    singer.write_state(state)
    LOGGER.info("Finished sync")
    zendesk_metrics.log_aggregate_rates()


def oauth_auth(args):
    if not set(OAUTH_CONFIG_KEYS).issubset(args.config.keys()):
        LOGGER.debug("OAuth authentication unavailable.")
        return None

    LOGGER.info("Using OAuth authentication.")
    return {
        "subdomain": args.config['subdomain'],
        "oauth_token": args.config['access_token'],
    }


def api_token_auth(args):
    if not set(API_TOKEN_CONFIG_KEYS).issubset(args.config.keys()):
        LOGGER.debug("API Token authentication unavailable.")
        return None

    LOGGER.info("Using API Token authentication.")
    return {
        "subdomain": args.config['subdomain'],
        "email": args.config['email'],
        "token": args.config['api_token']
    }


def convert_x_rate_limit_remaining_to_int(response, *args, **kwargs):
    if 'X-Rate-Limit-Remaining' in response.headers and isinstance(response.headers['X-Rate-Limit-Remaining'], str):
        response.headers['X-Rate-Limit-Remaining'] = int(response.headers['X-Rate-Limit-Remaining'])

    return response


def add_session_hooks(session):
    # This is due version conflict between singer-python and ZenPy
    # Link: https://github.com/singer-io/singer-python/issues/114
    session.hooks['response'].append(convert_x_rate_limit_remaining_to_int)


def get_session(config):
    """ Add partner information to requests Session object if specified in the config. """
    if not all(k in config for k in ["marketplace_name",
                                     "marketplace_organization_id",
                                     "marketplace_app_id"]):
        return None
    session = requests.Session()

    # Using Zenpy's default adapter args, following the method outlined here:
    # https://github.com/facetoe/zenpy/blob/master/docs/zenpy.rst#usage
    session.mount("https://", HTTPAdapter(**Zenpy.http_adapter_kwargs()))
    session.headers["X-Zendesk-Marketplace-Name"] = config.get("marketplace_name", "")
    session.headers["X-Zendesk-Marketplace-Organization-Id"] = str(config.get("marketplace_organization_id", ""))
    session.headers["X-Zendesk-Marketplace-App-Id"] = str(config.get("marketplace_app_id", ""))
    return session


def get_default_config():
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'default_config.json')) as f:
        config = json.loads(f.read())

    return config


def get_internal_config(user_config, default_config):
    config = {}
    for key in default_config.keys():
        config[key] = user_config.get(key, default_config[key])

    return config


@singer.utils.handle_top_exception(LOGGER)
def main():
    default_config = get_default_config()
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    internal_config = get_internal_config(parsed_args.config, default_config)

    # OAuth has precedence
    creds = oauth_auth(parsed_args) or api_token_auth(parsed_args)
    session = get_session(parsed_args.config)
    client = Zenpy(session=session, ratelimit=internal_config['rate_limit'], **creds)
    client.internal_config = internal_config

    add_session_hooks(client.tickets.session)

    if not client:
        LOGGER.error("""No suitable authentication keys provided.""")

    if parsed_args.discover:
        do_discover(client)
    elif parsed_args.catalog:
        state = parsed_args.state
        do_sync(client, parsed_args.catalog, state, parsed_args.config['start_date'])
