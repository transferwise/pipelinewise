import os
import re

from pipelinewise.fastsync.commons import utils


def upload_to_s3(snowflake, file_parts, temp_dir):
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


def load_into_snowflake(snowflake, args, s3_keys, s3_key_pattern, size_bytes):
    """load data into Snowflake"""

    # delete partial data from the table
    target_schema = utils.get_target_schema(args.target, args.table)
    table_dict = utils.tablename_to_dict(args.table)
    target_table = table_dict.get('table_name')
    where_clause = f'WHERE {args.column} >= {args.start_value}'
    if args.end_value:
        where_clause += f' AND {args.column} <= {args.end_value}'

    snowflake.query(f'DELETE FROM {target_schema}.{target_table} {where_clause}')
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


def update_state_file(args, bookmark, lock):
    """Update state file"""
    # Save bookmark to singer state file
    # Lock to ensure that only one process writes the same state file at a time
    if not args.end_value:
        lock.acquire()
        try:
            utils.save_state_file(args.state, args.table, bookmark)
        finally:
            lock.release()
