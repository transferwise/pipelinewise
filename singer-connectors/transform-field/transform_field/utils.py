import argparse

from typing import Dict
from singer import Catalog, get_logger, Schema
from singer.utils import check_config, load_json


LOGGER = get_logger('transform_field')


def parse_args(required_config_keys):
    """
    Parse standard command-line args.

    Parses the command-line arguments mentioned in the SPEC and the BEST_PRACTICES documents:

    -c,--config     Config file
    --validate     flag  to validate the transformations
    --catalog       Catalog file

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, catalog), we will automatically
    load and parse the JSON file.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '--validate',
        help='Flag to trigger one-off validation of transformations in config file using the catalog',
        default=False,
        action='store_true'
    )

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    args = parser.parse_args()

    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    if args.catalog:
        setattr(args, 'catalog_path', args.catalog)
        args.catalog = Catalog.load(args.catalog)

    check_config(args.config, required_config_keys)

    return args


def get_stream_schemas(catalog: Catalog) -> Dict[str, Schema]:
    """
    Build a map of streams with their schemas
    :param catalog:
    :return: Dictionary mapping stream ID to its schema
    """
    return {
        stream.tap_stream_id: stream.schema
        for stream in catalog.streams if stream.is_selected()
    }
