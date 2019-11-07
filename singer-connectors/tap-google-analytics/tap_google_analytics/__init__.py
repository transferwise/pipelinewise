#!/usr/bin/env python3
import datetime
import json
import sys

from pathlib import Path

import singer
from singer import utils, metadata

from tap_google_analytics.ga_client import GAClient
from tap_google_analytics.reports_helper import ReportsHelper
from tap_google_analytics.error import *

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "view_id"
]

LOGGER = singer.get_logger()

def discover(config):
    # Load the reports json file
    default_reports = Path(__file__).parent.joinpath('defaults', 'default_report_definition.json')

    report_def_file = config.get('reports', default_reports)
    if Path(report_def_file).is_file():
        try:
            reports_definition = load_json(report_def_file)
        except ValueError:
            LOGGER.critical("tap-google-analytics: The JSON definition in '{}' has errors".format(report_def_file))
            sys.exit(1)
    else:
        LOGGER.critical("tap-google-analytics: '{}' file not found".format(report_def_file))
        sys.exit(1)

    # validate the definition
    reports_helper = ReportsHelper(config, reports_definition)
    reports_helper.validate()

    # Generate and return the catalog
    return reports_helper.generate_catalog()

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks for an empty breadcrumb
    and metadata with a 'selected' or an 'inclusion' == automatic entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = metadata.to_map(stream['metadata'])

        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected") \
          or metadata.get(stream_metadata, (), "inclusion") == 'automatic':
            selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def sync(config, state, catalog):
    errors_encountered = False

    selected_stream_ids = get_selected_streams(catalog)

    client = GAClient(config)

    # Loop over streams in catalog
    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']

        stream_metadata = metadata.to_map(stream['metadata'])
        key_properties = metadata.get(stream_metadata, (), "table-key-properties")

        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream: ' + stream_id)

            try:
                report_definition = ReportsHelper.get_report_definition(stream)
                results = client.process_stream(report_definition)

                # we write the schema message after we are sure that we could
                #  fetch records without errors
                singer.write_schema(stream_id, stream_schema, key_properties)
                singer.write_records(stream_id, results)
            except TapGaInvalidArgumentError as e:
                errors_encountered = True
                LOGGER.error("Skipping stream: '{}' due to invalid report definition.".format(stream_id))
                LOGGER.debug("Error: '{}'.".format(e))
            except TapGaRateLimitError as e:
                errors_encountered = True
                LOGGER.error("Skipping stream: '{}' due to Rate Limit Errors.".format(stream_id))
                LOGGER.debug("Error: '{}'.".format(e))
            except TapGaQuotaExceededError as e:
                errors_encountered = True
                LOGGER.error("Skipping stream: '{}' due to Quota Exceeded Errors.".format(stream_id))
                LOGGER.debug("Error: '{}'.".format(e))
            except TapGaAuthenticationError as e:
                LOGGER.error("Stopping execution while processing '{}' due to Authentication Errors.".format(stream_id))
                LOGGER.debug("Error: '{}'.".format(e))
                sys.exit(1)
            except TapGaUnknownError as e:
                LOGGER.error("Stopping execution while processing '{}' due to Unknown Errors.".format(stream_id))
                LOGGER.debug("Error: '{}'.".format(e))
                sys.exit(1)
        else:
            LOGGER.info('Skipping unselected stream: ' + stream_id)

    # If we encountered errors, exit with 1
    if errors_encountered:
        sys.exit(1)

    return

def load_json(path):
    with open(path) as f:
        return json.load(f)

def process_args():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Check for errors on the provided config params that utils.parse_args is letting through
    if not args.config.get('start_date'):
        LOGGER.critical("tap-google-analytics: a valid start_date must be provided.")
        sys.exit(1)

    if not args.config.get('view_id'):
        LOGGER.critical("tap-google-analytics: a valid view_id must be provided.")
        sys.exit(1)

    if not args.config.get('key_file_location') and not args.config.get('oauth_credentials'):
        LOGGER.critical("tap-google-analytics: a valid key_file_location string or oauth_credentials object must be provided.")
        sys.exit(1)

    # Remove optional args that have empty strings as values
    if 'reports' in args.config and not args.config.get('reports'):
        del args.config['reports']

    if 'end_date' in args.config and not args.config.get('end_date'):
        del args.config['end_date']

    # Process the [start_date, end_date) so that they define an open date window
    # that ends yesterday if end_date is not defined
    start_date = utils.strptime_to_utc(args.config['start_date'])
    args.config['start_date'] = utils.strftime(start_date,'%Y-%m-%d')

    end_date = args.config.get('end_date', utils.strftime(utils.now()))
    end_date = utils.strptime_to_utc(end_date) - datetime.timedelta(days=1)
    args.config['end_date'] = utils.strftime(end_date ,'%Y-%m-%d')

    if end_date < start_date:
        LOGGER.critical("tap-google-analytics: start_date '{}' > end_date '{}'".format(start_date, end_date))
        sys.exit(1)

    # If using a service account, validate that the client_secrets.json file exists and load it
    if args.config.get('key_file_location'):
        if Path(args.config['key_file_location']).is_file():
            try:
                args.config['client_secrets'] = load_json(args.config['key_file_location'])
            except ValueError:
                LOGGER.critical("tap-google-analytics: The JSON definition in '{}' has errors".format(args.config['key_file_location']))
                sys.exit(1)
        else:
            LOGGER.critical("tap-google-analytics: '{}' file not found".format(args.config['key_file_location']))
            sys.exit(1)
    else:
        # If using oauth credentials, verify that all required keys are present
        credentials = args.config['oauth_credentials']

        if not credentials.get('access_token'):
            LOGGER.critical("tap-google-analytics: a valid access_token for the oauth_credentials must be provided.")
            sys.exit(1)

        if not credentials.get('refresh_token'):
            LOGGER.critical("tap-google-analytics: a valid refresh_token for the oauth_credentials must be provided.")
            sys.exit(1)

        if not credentials.get('client_id'):
            LOGGER.critical("tap-google-analytics: a valid client_id for the oauth_credentials must be provided.")
            sys.exit(1)

        if not credentials.get('client_secret'):
            LOGGER.critical("tap-google-analytics: a valid client_secret for the oauth_credentials must be provided.")
            sys.exit(1)

    return args

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = process_args()

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(args.config)
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog.to_dict()
        else:
            catalog = discover(args.config)

        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
