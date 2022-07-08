import argparse
import os
import re

from typing import Dict, Tuple, List

from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake


def upload_to_s3(snowflake: FastSyncTargetSnowflake, file_parts: List, temp_dir: str) -> Tuple[List, str]:
    """Upload exported data into S3"""

    s3_keys = []
    for file_part in file_parts:
        s3_keys.append(snowflake.upload_to_s3(file_part, tmp_dir=temp_dir))
        os.remove(file_part)

    # Create a pattern that match all file parts by removing multipart suffix
    s3_key_pattern = (
        re.sub(r'\.part\d*$', '', s3_keys[0])
        if len(s3_keys) > 0
        else 'NO_FILES_TO_LOAD'
    )
    return s3_keys, s3_key_pattern


def load_into_snowflake(
        snowflake: FastSyncTargetSnowflake,
        args: argparse.Namespace,
        s3_keys: List, s3_key_pattern: str, size_bytes: int) -> None:
    """load data into Snowflake"""

    # delete partial data from the table
    target_schema = common_utils.get_target_schema(args.target, args.table)
    table_dict = common_utils.tablename_to_dict(args.table)
    target_table = table_dict.get('table_name')
    where_clause = f'WHERE {args.column} >= \'{args.start_value}\''
    if args.end_value:
        where_clause += f' AND {args.column} <= \'{args.end_value}\''

    snowflake.query(f'DELETE FROM {target_schema}."{target_table.upper()}" {where_clause}')
    # copy partial data into the table
    archive_load_files = args.target.get('archive_load_files', False)
    tap_id = args.target.get('tap_id')

    # Load into Snowflake table
    snowflake.copy_to_table(
        s3_key_pattern, target_schema, args.table, size_bytes, is_temporary=False
    )

    for s3_key in s3_keys:
        if archive_load_files:
            # Copy load file to archive
            snowflake.copy_to_archive(s3_key, tap_id, args.table)

        # Delete all file parts from s3
        snowflake.s3.delete_object(Bucket=args.target.get('s3_bucket'), Key=s3_key)


def update_state_file(args: argparse.Namespace, bookmark: Dict) -> None:
    """Update state file"""
    # Save bookmark to singer state file
    if not args.end_value:
        common_utils.save_state_file(args.state, args.table, bookmark)


def parse_args_for_partial_sync(required_config_keys: Dict) -> argparse.Namespace:
    """Parsing arguments for partial sync"""

    parser = _get_args_parser_for_partialsync()

    parser.add_argument('--table', help='Partial sync table')
    parser.add_argument('--column', help='Column for partial sync table')
    parser.add_argument('--start_value', help='Start value for partial sync table')
    parser.add_argument('--end_value', help='End value for partial sync table')

    args: argparse.Namespace = parser.parse_args()

    if args.tap:
        args.tap = common_utils.load_json(args.tap)

    if args.properties:
        args.properties = common_utils.load_json(args.properties)

    if args.target:
        args.target = common_utils.load_json(args.target)

    if args.transform:
        args.transform = common_utils.load_json(args.transform)
    else:
        args.transform = {}

    if not args.temp_dir:
        args.temp_dir = os.path.realpath('.')

    common_utils.check_config(args.tap, required_config_keys['tap'])
    common_utils.check_config(args.target, required_config_keys['target'])

    return args


def _get_args_parser_for_partialsync():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tap', help='Tap Config file', required=True)
    parser.add_argument('--state', help='State file')
    parser.add_argument('--properties', help='Properties file')
    parser.add_argument('--target', help='Target Config file', required=True)
    parser.add_argument('--transform', help='Transformations Config file')
    parser.add_argument(
        '--temp_dir', help='Temporary directory required for CSV exports'
    )
    parser.add_argument(
        '--drop_pg_slot',
        help='Drop pg replication slot before starting resync',
        action='store_true',
    )

    return parser
