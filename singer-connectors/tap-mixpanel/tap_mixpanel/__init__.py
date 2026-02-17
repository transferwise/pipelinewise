#!/usr/bin/env python3

import json
import sys
from datetime import timedelta

import singer
from singer import utils
from singer.utils import strftime, strptime_to_utc

from tap_mixpanel.client import MixpanelClient
from tap_mixpanel.discover import discover as _discover
from tap_mixpanel.sync import sync as _sync

LOGGER = singer.get_logger()

REQUEST_TIMEOUT = 300
REQUIRED_CONFIG_KEYS = [
    "project_timezone",
    "api_secret",
    "date_window_size",
    "attribution_window",
    "start_date",
    "user_agent",
]


def _is_true(value):
    return str(value).lower() == "true"


def do_discover(client, properties_flag):
    """Call the discovery function.

    Args:
        client (MixpanelClient): Client object to make http calls.
        properties_flag (str): Setting this argument to `true` ensures that new properties on
                               events and engage records are captured.
    """
    LOGGER.info("Starting discover")
    catalog = _discover(client, properties_flag)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():
    """
    Run discover mode or sync mode.
    """
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    start_date = parsed_args.config["start_date"]
    # Set request timeout to config param `request_timeout` value.
    # If value is 0, "0", "" or not passed then it sets default to 300 seconds.
    config_request_timeout = parsed_args.config.get("request_timeout")
    if config_request_timeout and float(config_request_timeout):
        request_timeout = float(config_request_timeout)
    else:
        request_timeout = REQUEST_TIMEOUT

    start_dttm = strptime_to_utc(start_date)
    now_dttm = utils.now()
    if parsed_args.config.get("end_date"):
        now_dttm = strptime_to_utc(parsed_args.config.get("end_date"))
    delta_days = (now_dttm - start_dttm).days
    if delta_days >= 365:
        delta_days = 365
        start_date = strftime(now_dttm - timedelta(days=delta_days))
        LOGGER.warning("start_date greater than 1 year maximum for API.")
        LOGGER.warning("Setting start_date to 1 year ago, {}".format(start_date))

    # Check support for EU endpoints
    eu_residency = _is_true(parsed_args.config.get("eu_residency")) or _is_true(
        parsed_args.config.get("eu_residency_server")
    )
    if eu_residency:
        api_domain = "eu.mixpanel.com"
    else:
        api_domain = "mixpanel.com"

    with MixpanelClient(
        parsed_args.config["api_secret"],
        api_domain,
        request_timeout,
        parsed_args.config["user_agent"],
    ) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        config = parsed_args.config
        properties_flag = config.get("select_properties_by_default")

        if parsed_args.discover:
            do_discover(client, properties_flag)
        else:
            catalog = parsed_args.catalog
            if not catalog:
                catalog = _discover(client, properties_flag)
            _sync(
                client=client,
                config=config,
                catalog=catalog,
                state=state,
                start_date=start_date,
            )


if __name__ == "__main__":
    main()
