import argparse
from contextlib import contextmanager
import fcntl
import json
import multiprocessing
import os
import logging
import datetime
import re
import stat
import tempfile

from typing import Callable, Dict, List, Optional, Tuple
from pipelinewise.cli.utils import generate_random_string

LOGGER = logging.getLogger(__name__)

SDC_EXTRACTED_AT = '_SDC_EXTRACTED_AT'
SDC_BATCHED_AT = '_SDC_BATCHED_AT'
SDC_DELETED_AT = '_SDC_DELETED_AT'


class StagingUploadError(RuntimeError):
    """An upload failed and its successfully uploaded parts still need cleanup."""

    def __init__(self, upload_error, cleanup_error, s3_keys):
        super().__init__(
            f'{upload_error}; staging upload rollback failed: {cleanup_error}'
        )
        self.s3_keys = list(s3_keys)


def delete_s3_objects(
    snowflake,
    s3_keys: List[str],
    bucket: str,
    cleanup_context='FastSync staging cleanup',
    max_attempts=3,
) -> None:
    """Delete every staging object with bounded retries and report any debt."""
    failures = []
    for s3_key in s3_keys:
        for attempt in range(1, max_attempts + 1):
            try:
                snowflake.s3.delete_object(Bucket=bucket, Key=s3_key)
                break
            except Exception as exc:
                if attempt == max_attempts:
                    failures.append((s3_key, exc))
                else:
                    LOGGER.warning(
                        '%s retry %s/%s failed for s3://%s/%s: %s',
                        cleanup_context,
                        attempt,
                        max_attempts,
                        bucket,
                        s3_key,
                        exc,
                    )

    if failures:
        details = '; '.join(
            f's3://{bucket}/{s3_key}: {exc}' for s3_key, exc in failures
        )
        raise RuntimeError(f'{cleanup_context} failed after {max_attempts} attempts: {details}')


def cleanup_staging(
    snowflake,
    s3_keys,
    bucket,
    target_schema=None,
    table=None,
    temp_created=False,
) -> None:
    """Attempt all S3 and Snowflake staging cleanup before reporting failures."""
    failures = []
    if s3_keys:
        try:
            delete_s3_objects(
                snowflake,
                s3_keys,
                bucket,
                cleanup_context='Failed FastSync rollback',
            )
        except Exception as exc:
            failures.append(f'S3 objects: {exc}')

    if temp_created and target_schema and table:
        try:
            snowflake.drop_table(
                target_schema,
                table,
                is_temporary=True,
                max_attempts=3,
            )
        except Exception as exc:
            failures.append(f'Snowflake staging table: {exc}')

    if failures:
        raise RuntimeError('; '.join(failures))


def apply_snowflake_table_grants(
    snowflake,
    target_config,
    target_schema,
    table,
    is_temporary=False,
) -> None:
    """Grant schema usage and SELECT on one obfuscated staging or live table."""
    grantees = get_grantees(target_config, table)

    def grant_select_on_live_table(schema, role, to_group=False):
        snowflake.grant_select_on_table(
            schema,
            table,
            role,
            is_temporary=is_temporary,
            to_group=to_group,
        )

    run_post_publication_actions([
        (
            'schema usage grant',
            lambda: grant_privilege(
                target_schema, grantees, snowflake.grant_usage_on_schema
            ),
        ),
        (
            'live table select grant',
            lambda: grant_privilege(
                target_schema, grantees, grant_select_on_live_table
            ),
        ),
    ])


def retry_snowflake_table_grants(
    snowflake,
    target_config,
    target_schema,
    table,
    is_temporary=False,
    max_attempts=3,
) -> None:
    """Retry idempotent grants so a transient failure does not force republish."""
    for attempt in range(1, max_attempts + 1):
        try:
            apply_snowflake_table_grants(
                snowflake,
                target_config,
                target_schema,
                table,
                is_temporary=is_temporary,
            )
            return
        except Exception as exc:
            if attempt == max_attempts:
                raise
            LOGGER.warning(
                'Snowflake grant retry %s/%s failed for %s.%s: %s',
                attempt,
                max_attempts,
                target_schema,
                table,
                exc,
            )


def finalize_snowflake_fullsync(
    snowflake,
    s3_keys,
    bucket,
    target_config,
    target_schema,
    table,
    publication_error=None,
) -> None:
    """Attempt finalization without masking an earlier publication failure."""
    try:
        run_post_publication_actions([
            (
                'grant application',
                lambda: apply_snowflake_table_grants(
                    snowflake, target_config, target_schema, table
                ),
            ),
            (
                'S3 staging cleanup',
                lambda: delete_s3_objects(
                    snowflake,
                    s3_keys,
                    bucket,
                    cleanup_context='Successful FullSync staging cleanup',
                ),
            ),
            (
                'Snowflake staging cleanup',
                lambda: snowflake.drop_table(
                    target_schema,
                    table,
                    is_temporary=True,
                    max_attempts=3,
                ),
            ),
        ])
    except Exception as finalization_error:
        if publication_error is not None:
            raise RuntimeError(
                f'{publication_error}; post-publication finalization failed: '
                f'{finalization_error}'
            ) from publication_error
        raise


def staging_failure_result(
    snowflake,
    s3_keys,
    bucket,
    target_schema,
    table,
    temp_created,
    operation_error,
) -> str:
    """Roll back staging and preserve both the operation and cleanup errors."""
    result = f'{table}: {operation_error}'
    if not s3_keys and not temp_created:
        return result

    try:
        cleanup_staging(
            snowflake,
            s3_keys,
            bucket,
            target_schema=target_schema,
            table=table,
            temp_created=temp_created,
        )
    except Exception as cleanup_error:
        LOGGER.exception('Failed to clean up FastSync staging')
        result = f'{result}; staging cleanup failed: {cleanup_error}'
    return result


def partial_sync_failure_result(
    snowflake,
    target_config,
    source_table,
    target_schema,
    target_table,
    staging,
    operation_error,
) -> str:
    """Repair published grants, roll back staging, and preserve every error."""
    if staging['publication_attempted'] and not staging['grants_attempted']:
        try:
            retry_snowflake_table_grants(
                snowflake, target_config, target_schema, source_table
            )
        except Exception as grant_error:
            LOGGER.exception('Failed to repair PartialSync grants')
            operation_error = RuntimeError(
                f'{operation_error}; grant application failed: {grant_error}'
            )

    return staging_failure_result(
        snowflake,
        staging['s3_keys'],
        target_config.get('s3_bucket'),
        target_schema,
        target_table or source_table,
        staging['temp_created'],
        operation_error,
    )


def get_expected_s3_key(snowflake, file_part):
    """Resolve a deterministic key so ambiguous upload outcomes can be cleaned."""
    key_resolver = getattr(snowflake, '_get_s3_key', None)
    if not callable(key_resolver):
        return None
    expected_key = key_resolver(file_part)
    return expected_key if isinstance(expected_key, str) else None


def get_expected_s3_keys(snowflake, file_parts: List[str]) -> List[str]:
    """Resolve and validate every deterministic staging key before upload."""
    s3_keys = []
    seen = set()
    has_missing = False
    has_duplicate = False
    for file_part in file_parts:
        s3_key = get_expected_s3_key(snowflake, file_part)
        s3_keys.append(s3_key)
        if not s3_key:
            has_missing = True
        elif s3_key in seen:
            has_duplicate = True
        else:
            seen.add(s3_key)

    if has_missing:
        raise ValueError('Iceberg staging requires deterministic S3 keys')
    if has_duplicate:
        raise ValueError('Iceberg staging S3 keys must be unique')
    return s3_keys


def upload_files_to_s3(
    snowflake,
    file_parts: List[str],
    temp_dir: str,
    bucket: str,
    planned_s3_keys: Optional[List[str]] = None,
) -> Tuple[List[str], str]:
    """Upload every part before removing local files and roll back failed staging."""
    if planned_s3_keys is None:
        s3_keys = []
    else:
        s3_keys = list(planned_s3_keys)
        if s3_keys != get_expected_s3_keys(snowflake, file_parts):
            raise ValueError('Planned Iceberg staging S3 keys changed before upload')

    try:
        for index, file_part in enumerate(file_parts):
            expected_key = get_expected_s3_key(snowflake, file_part)
            if planned_s3_keys is None and expected_key:
                s3_keys.append(expected_key)
            uploaded_key = snowflake.upload_to_s3(file_part, tmp_dir=temp_dir)
            if planned_s3_keys is not None:
                if uploaded_key != s3_keys[index]:
                    raise RuntimeError(
                        'Uploaded Iceberg staging key does not match its persisted plan'
                    )
            elif expected_key:
                s3_keys[-1] = uploaded_key
            else:
                s3_keys.append(uploaded_key)
        for file_part in file_parts:
            os.remove(file_part)
    except Exception as upload_error:
        try:
            delete_s3_objects(
                snowflake,
                s3_keys,
                bucket,
                cleanup_context='Failed FastSync upload rollback',
            )
        except Exception as cleanup_error:
            LOGGER.exception('Failed to fully roll back uploaded FastSync staging objects')
            raise StagingUploadError(
                upload_error, cleanup_error, s3_keys
            ) from upload_error
        raise

    s3_key_pattern = (
        re.sub(r'\.part\d*$', '', s3_keys[0])
        if s3_keys
        else 'NO_FILES_TO_LOAD'
    )
    return s3_keys, s3_key_pattern


def run_post_publication_actions(
    actions: List[Tuple[str, Callable[[], None]]],
) -> None:
    """Attempt every required post-publication action before reporting failures."""
    failures = []
    for action_name, action in actions:
        try:
            action()
        except Exception as exc:
            LOGGER.exception('Post-publication %s failed', action_name)
            failures.append((action_name, exc))

    if failures:
        details = '; '.join(f'{action_name}: {exc}' for action_name, exc in failures)
        raise RuntimeError(f'Post-publication actions failed: {details}') from failures[0][1]


class NotSelectedTableException(Exception):
    """
    Exception to raise when a table is not selected for resync
    """

    def __init__(self, table_name, selected_tables):
        self.message = f'Cannot Resync unselected table "{table_name}"! Selected tables are: {selected_tables}'
        super().__init__(self, self.message)


# pylint: disable=missing-function-docstring
def get_cpu_cores():
    """Get CPU cores for multiprocessing"""
    try:
        return multiprocessing.cpu_count()
    # Defaults to 1 core in case of any exception
    except Exception:
        return 1


def load_json(path):
    with open(path, encoding='utf-8') as fil:
        return json.load(fil)


def _fsync_directory(path):
    """Persist a renamed state-file directory entry across host crashes."""
    directory_descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def save_dict_to_json(path, data):
    LOGGER.info('Saving new state file to %s', path)
    path = os.path.realpath(path)
    directory = os.path.dirname(path)
    file_mode = stat.S_IMODE(os.stat(path).st_mode) if os.path.exists(path) else None
    file_descriptor, temp_path = tempfile.mkstemp(
        dir=directory,
        prefix=f'.{os.path.basename(path)}.',
        suffix='.tmp',
    )

    try:
        with os.fdopen(file_descriptor, 'w', encoding='utf-8') as fil:
            json.dump(data, fil, indent=4, sort_keys=True)
            fil.flush()
            os.fsync(fil.fileno())
        if file_mode is not None:
            os.chmod(temp_path, file_mode)
        os.replace(temp_path, path)
        _fsync_directory(directory)
    except Exception:
        try:
            os.remove(temp_path)
        except FileNotFoundError:
            pass
        raise


@contextmanager
def _state_file_lock(path):
    """Serialize state updates across independently started FastSync processes."""
    state_path = os.path.realpath(path)
    with open(f'{state_path}.lock', 'a', encoding='utf-8') as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield state_path
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception('Config is missing required keys: {}'.format(missing_keys))


def tablename_to_dict(table, separator='.'):
    """Derive catalog, schema and table names from fully qualified table names"""
    catalog_name = None
    schema_name = None
    table_name = table

    split_parts = table.split(separator)
    if len(split_parts) == 2:
        schema_name = split_parts[0]
        table_name = split_parts[1]
    if len(split_parts) > 2:
        catalog_name = split_parts[0]
        schema_name = split_parts[1]
        table_name = '_'.join(split_parts[2:])

    return {
        'catalog_name': catalog_name,
        'schema_name': schema_name,
        'table_name': table_name,
        'temp_table_name': '{}_temp'.format(table_name),
    }


def get_tables_from_properties(properties: Dict) -> set:
    """Get list of selected tables with schema names from properties json
    The output is used to generate list of tables to sync
    """
    tables = set()

    for stream in properties.get('streams', tables):
        metadata = stream.get('metadata', [])
        table_name = stream.get('table_name', stream['stream'])

        table_meta = next(
            (
                i
                for i in metadata
                if isinstance(i, dict) and len(i.get('breadcrumb', [])) == 0
            ),
            {},
        ).get('metadata')
        selected = table_meta.get('selected', False)
        schema_name = table_meta.get('schema-name')
        db_name = table_meta.get('database-name')

        if table_name and selected:
            if schema_name is not None or db_name is not None:
                tables.add('{}.{}'.format(schema_name or db_name, table_name))
            else:
                # Some tap types don't have db name nor schema name
                tables.add(table_name)

    return tables


def get_bookmark_for_table(table, properties, db_engine, dbname=None):
    """Get actual bookmark for a specific table used for LOG_BASED or INCREMENTAL
    replications
    """
    bookmark = {}

    # Find table from properties and get bookmark based on replication method
    for stream in properties.get('streams', []):
        metadata = stream.get('metadata', [])
        table_name = stream.get('table_name', stream['stream'])

        # Get table specific metadata i.e. replication method, replication key, etc.
        table_meta = next(
            (
                i
                for i in metadata
                if isinstance(i, dict) and len(i.get('breadcrumb', [])) == 0
            ),
            {},
        ).get('metadata')
        db_name = table_meta.get('database-name')
        schema_name = table_meta.get('schema-name')
        replication_method = table_meta.get('replication-method')
        replication_key = table_meta.get('replication-key')

        fully_qualified_table_name = (
            '{}.{}'.format(schema_name or db_name, table_name)
            if schema_name is not None or db_name is not None
            else table_name
        )

        if (
            dbname is None or db_name == dbname
        ) and fully_qualified_table_name == table:
            # Log based replication: get mysql binlog position
            if replication_method == 'LOG_BASED':
                bookmark = db_engine.fetch_current_log_pos()

            # Key based incremental replication: Get max replication key from source
            elif replication_method == 'INCREMENTAL':
                bookmark = db_engine.fetch_current_incremental_key_pos(
                    fully_qualified_table_name, replication_key
                )

            break

    return bookmark


def get_target_schema(target_config, table):
    """Target schema name can be defined in multiple ways:

    1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
                                      not specified explicitly for a given stream in
                                      the `schema_mapping` object
    2: 'schema_mapping' key         : Target schema defined explicitly for a given stream.
                                      Example config.json:
                                            "schema_mapping": {
                                                "my_tap_stream_id": {
                                                    "target_schema": "my_target_schema",
                                                }
                                            }
    """
    target_schema = None
    config_default_target_schema = target_config.get(
        'default_target_schema', ''
    ).strip()
    config_schema_mapping = target_config.get('schema_mapping', {})

    table_dict = tablename_to_dict(table)
    table_schema = table_dict['schema_name']
    if config_schema_mapping and table_schema in config_schema_mapping:
        target_schema = config_schema_mapping[table_schema].get('target_schema')
    elif config_default_target_schema:
        target_schema = config_default_target_schema

    if not target_schema:
        raise Exception(
            "Target schema name not defined in config. Neither 'default_target_schema' (string) nor 'schema_mapping' "
            '(object) defines target schema for {} stream. '.format(table)
        )

    return target_schema


# pylint: disable=invalid-name
def get_target_schemas(target_config, tables):
    """Get list of target schemas"""
    target_schemas = []
    for trans in tables:
        target_schemas.append(get_target_schema(target_config, trans))

    return list(dict.fromkeys(target_schemas))


# pylint: disable=invalid-name
def get_grantees(target_config, table):
    """Grantees can be defined in multiple ways:

    1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on every table to
                                                         a given role for every incoming stream if not specified
                                                         explicitly in the `schema_mapping` object
    2: 'target_schema_select_permissions' key          : Roles to grant USAGE and SELECT privileges defined explicitly
                                                        for a given stream.
                                                        Example config.json:
                                                        "schema_mapping": {
                                                            "my_tap_stream_id": {
                                                                "target_schema_select_permissions": [
                                                                    "role_with_select_privs"
                                                                ]
                                                            }
                                                        }
    """
    grantees = []
    config_default_target_schema_select_permissions = target_config.get(
        'default_target_schema_select_permissions', []
    )
    config_schema_mapping = target_config.get('schema_mapping', {})

    table_dict = tablename_to_dict(table)
    table_schema = table_dict['schema_name']
    if config_schema_mapping and table_schema in config_schema_mapping:
        grantees = config_schema_mapping[table_schema].get(
            'target_schema_select_permissions', []
        )
    elif config_default_target_schema_select_permissions:
        grantees = config_default_target_schema_select_permissions

    # Grantees can be string
    if isinstance(grantees, str):
        grantees = [grantees]
    # Grantees can be a dict with string/list of users and groups
    elif isinstance(grantees, dict):
        users = grantees.get('users')
        groups = grantees.get('groups')

        grantees = {
            'users': [users] if isinstance(users, str) else users,
            'groups': [groups] if isinstance(groups, str) else groups,
        }
    # Convert anything else that not list empty list
    elif not isinstance(grantees, list):
        grantees = []

    return grantees


def _grantee_entries(grantees, to_group=False):
    """Normalize configured grantees while preserving user/group semantics."""
    if isinstance(grantees, str):
        return [(grantees, to_group)]
    if isinstance(grantees, list):
        return [(grantee, to_group) for grantee in grantees]
    if isinstance(grantees, dict):
        return (
            _grantee_entries(grantees.get('users'))
            + _grantee_entries(grantees.get('groups'), to_group=True)
        )
    return []


def grant_privilege(schema, grantees, grant_method, to_group=False):
    """Attempt a privilege grant for every configured grantee."""
    failures = []
    for grantee, grantee_is_group in _grantee_entries(grantees, to_group):
        try:
            grant_method(schema, grantee, grantee_is_group)
        except Exception as exc:
            failures.append((grantee, exc))

    if failures:
        details = '; '.join(f'{grantee}: {exc}' for grantee, exc in failures)
        raise RuntimeError(f'Privilege grants failed: {details}') from failures[0][1]


def save_state_file(path, table, bookmark, dbname=None):
    table_dict = tablename_to_dict(table)
    if dbname:
        stream_id = '{}-{}-{}'.format(
            dbname, table_dict.get('schema_name'), table_dict.get('table_name')
        )
    elif table_dict['schema_name']:
        stream_id = '{}-{}'.format(
            table_dict['schema_name'], table_dict.get('table_name')
        )
    else:
        stream_id = table_dict['table_name']

    # Do nothing if state path not defined
    if not path:
        return

    with _state_file_lock(path) as state_path:
        # Load the current state file
        state = {}
        if os.path.exists(state_path):
            state = load_json(state_path)

        # Find the current table position
        bookmarks = state.get('bookmarks', {})

        # Update the state file with the new values at the right place
        state['currently_syncing'] = None
        state['bookmarks'] = bookmarks
        state['bookmarks'][stream_id] = bookmark

        # Save the new state file
        save_dict_to_json(state_path, state)
        LOGGER.info('FastSync state updated for stream: %s', stream_id)


def parse_args(required_config_keys: Dict) -> argparse.Namespace:
    """Parse standard command-line args.

    --tap               Tap Config file
    --state             State file
    --properties        Properties file
    --target            Target Config file
    --transform         Transformations Config file
    --tables            Tables to sync. (Separated by comma)
    --temp_dir          Directory to create temporary csv exports. Defaults to current work dir.
    --drop_pg_slot      flag to drop or not the Postgres replication slot before starting the resync

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (tap, state, properties, target, transform),
    we will automatically load and parse the JSON file.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--tap', help='Tap Config file', required=True)
    parser.add_argument('--state', help='State file')
    parser.add_argument('--properties', help='Properties file')
    parser.add_argument('--target', help='Target Config file', required=True)
    parser.add_argument('--transform', help='Transformations Config file')
    parser.add_argument('--tables', help='Sync only specific tables')
    parser.add_argument(
        '--temp_dir', help='Temporary directory required for CSV exports'
    )
    parser.add_argument(
        '--drop_pg_slot',
        help='Drop pg replication slot before starting resync',
        action='store_true',
    )
    parser.add_argument('--autoresync_size', help='maximum value for table size to resync', )

    args: argparse.Namespace = parser.parse_args()

    if args.tap:
        args.tap = load_json(args.tap)

    if args.properties:
        args.properties = load_json(args.properties)

    if args.target:
        args.target = load_json(args.target)

    if args.transform:
        args.transform = load_json(args.transform)
    else:
        args.transform = {}

    # get all selected tables from json schema
    all_selected_tables = get_tables_from_properties(args.properties)

    if args.tables:
        # prevent duplicates
        unique_tables_list = set(args.tables.split(','))

        # check if all the given tables are actually selected
        for table in unique_tables_list:
            if table not in all_selected_tables:
                raise NotSelectedTableException(table, all_selected_tables)

        args.tables = unique_tables_list
    else:
        args.tables = all_selected_tables

    if not args.temp_dir:
        args.temp_dir = os.path.realpath('.')

    check_config(args.tap, required_config_keys['tap'])
    check_config(args.target, required_config_keys['target'])

    return args


# pylint: disable=import-outside-toplevel
def retry_pattern():
    import backoff
    from botocore.exceptions import ClientError

    return backoff.on_exception(
        backoff.expo,
        ClientError,
        max_tries=5,
        on_backoff=log_backoff_attempt,
        factor=10,
    )


def log_backoff_attempt(details):
    LOGGER.error(
        'Error detected communicating with Amazon, triggering backoff: %s try',
        details.get('tries'),
    )


def get_pool_size(tap: Dict) -> int:
    """
    Get the pool size to use in FastSync
    Args:
        tap: tap config, a dictionary with optional key "fastsync_parallelism"

    Returns: pool size as int

    """
    cpu_cores = get_cpu_cores()
    fastsync_parallelism = tap.get('fastsync_parallelism', None)

    if fastsync_parallelism is None:
        return cpu_cores

    return min(fastsync_parallelism, cpu_cores)


def gen_export_filename(
    tap_id: str, table: str, suffix: str = None, postfix: str = None, ext: str = None, sync_type: str = 'fastsync'
) -> str:
    """
    Generates a unique filename used for exported fastsync data that avoids file name collision

    Default pattern:
        pipelinewise_<tap_id>_<table>_<timestamp_with_ms>_fastsync_<random_string>.csv.gz

    Args:
        tap_id: Unique tap id
        table: Name of the table to export
        suffix: Generated filename suffix. Defaults to current timestamp in milliseconds
        postfix: Generated filename postfix. Defaults to a random 8 character length string
        ext: Filename extension. Defaults to .csv.gz

    Returns:
        Unique filename as a string
    """
    if not suffix:
        suffix = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')

    if not postfix:
        postfix = generate_random_string()

    if not ext:
        ext = 'csv.gz'

    return f'pipelinewise_{tap_id}_{table}_{suffix}_{sync_type}_{postfix}.{ext}'
