"""Shared route ordering for Snowflake Iceberg publication attempts."""

from . import utils
from .tap_mysql import (
    DEFAULT_CHARSET,
    DEFAULT_SESSION_SQLS,
    DEFAULT_USE_GTID,
)
from .snowflake_iceberg import (
    DEFAULT_QUERY_HISTORY_POLL_INTERVAL_SECONDS,
    DEFAULT_QUERY_HISTORY_POLL_TIMEOUT_SECONDS,
    RECOVERY_FINALIZE,
    RECOVERY_PUBLISH,
    RECOVERY_RESTART_STAGING,
    RECOVERY_STATE_HANDOFF,
    IcebergTableSpec,
    SnowflakeIcebergPublisher,
    SnowflakeObjectName,
    TABLE_FORMAT_MISSING,
    TABLE_FORMAT_NATIVE,
    TableCompatibilityError,
)
from .snowflake_iceberg_recovery import (
    FINALIZATION_GRANTS,
    FINALIZATION_METADATA,
    FINALIZATION_S3_CLEANUP,
    FINALIZATION_STAGING_CLEANUP,
    RetryableQueryHistoryRecoveryError,
)
from .snowflake_iceberg_versions import (
    build_recovery_identity,
    is_supported_managed_iceberg_version,
)
from .snowflake_iceberg_coordination import target_runtime_dir


_STAGING_CONFIG_KEYS = ('s3_bucket', 's3_key_prefix', 'stage', 'file_format')


def publication_failure_result(logger, table, error):
    """Format Iceberg route failures without hiding unexpected tracebacks."""
    if isinstance(error, RetryableQueryHistoryRecoveryError):
        logger.error('%s: %s', table, error)
    else:
        logger.exception(error)
    return f'{table}: {error}'


def _is_exact_int(value):
    """Exclude booleans while accepting integer configuration values."""
    return isinstance(value, int) and not isinstance(value, bool)


def query_history_poll_timeout_seconds(target_config):
    """Return the validated target-level query-history recovery budget."""
    timeout_seconds = target_config.get(
        'iceberg_query_history_poll_timeout_seconds',
        DEFAULT_QUERY_HISTORY_POLL_TIMEOUT_SECONDS,
    )
    if not _is_exact_int(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError(
            'iceberg_query_history_poll_timeout_seconds must be a positive integer'
        )
    return timeout_seconds


def validate_route_config(target_config):
    """Reject unsupported direct FastSync Iceberg configurations."""
    query_history_poll_timeout_seconds(target_config)
    if 'iceberg_create' in target_config:
        raise ValueError(
            'Snowflake FastSync no longer supports iceberg_create; configure '
            'target_table_format and iceberg_version on the tap'
        )

    format_is_set = 'target_table_format' in target_config
    version_is_set = 'iceberg_version' in target_config
    target_format = target_config.get('target_table_format')
    if not format_is_set:
        if version_is_set:
            raise ValueError(
                'Snowflake Iceberg FastSync requires target_table_format with iceberg_version'
            )
        return None
    if target_format not in ('native', 'iceberg'):
        raise ValueError(
            'Snowflake FastSync target_table_format must be native or iceberg'
        )
    if target_format == 'native':
        if version_is_set:
            raise ValueError(
                'Snowflake native FastSync does not accept iceberg_version'
            )
        return None

    iceberg_version = target_config.get('iceberg_version')
    if (
        not is_supported_managed_iceberg_version(iceberg_version)
    ):
        raise ValueError(
            'Snowflake Iceberg FastSync requires a supported integer iceberg_version'
        )
    if target_config.get('hard_delete') is not True:
        raise ValueError(
            'Snowflake Iceberg FastSync requires hard_delete to be true'
        )
    flattening_level = target_config.get('data_flattening_max_level')
    if not _is_exact_int(flattening_level) or flattening_level != 0:
        raise ValueError(
            'Snowflake Iceberg FastSync requires data_flattening_max_level to be integer 0'
        )
    return iceberg_version


def create_publisher(snowflake, args):
    """Create a publisher in the target-scoped recovery directory."""
    iceberg_requested = validate_route_config(getattr(args, 'target', {})) is not None
    runtime_dir = target_runtime_dir(
        args.state,
        args.temp_dir,
        require_target_scope=iceberg_requested,
    )
    return SnowflakeIcebergPublisher(
        snowflake,
        runtime_dir,
        history_poll_interval_seconds=DEFAULT_QUERY_HISTORY_POLL_INTERVAL_SECONDS,
        history_poll_timeout_seconds=query_history_poll_timeout_seconds(
            getattr(args, 'target', {})
        ),
    )


def require_native_target_format(
    snowflake,
    args,
    target_schema,
    source_table,
    *,
    allow_missing,
):
    """Require the physical target format expected by native FastSync."""
    target = target_name(args, target_schema, source_table)
    table_format = create_publisher(snowflake, args).discover_table_format(
        target.schema, target.table
    )
    allowed_formats = (
        (TABLE_FORMAT_MISSING, TABLE_FORMAT_NATIVE)
        if allow_missing
        else (TABLE_FORMAT_NATIVE,)
    )
    if table_format not in allowed_formats:
        raise TableCompatibilityError(
            f'Native Snowflake FastSync requires {target.quoted} to be '
            f'{"missing or native" if allow_missing else "native"}; '
            f'found {table_format}'
        )
    return table_format


def staging_config_identity(target_config):
    """Return the non-secret staging identity bound to a recovery attempt."""
    return {key: target_config.get(key) for key in _STAGING_CONFIG_KEYS}


def fastsync_recovery_identity(
    args,
    source_table,
    source_route,
    source_engine,
    staging_config,
    iceberg_version,
    partial_boundary=None,
):
    """Bind recovery to one credential-free FastSync execution identity."""
    source_config = args.tap
    target_config = args.target
    engine = source_engine.lower()
    target_schema = utils.get_target_schema(target_config, source_table).upper()
    target_table = utils.tablename_to_dict(source_table)['table_name'].upper()
    if (
        target_config.get('target_table_format') != 'iceberg'
        or target_config.get('iceberg_version') != iceberg_version
        or not is_supported_managed_iceberg_version(iceberg_version)
    ):
        raise ValueError(
            'FastSync recovery identity requires the validated Iceberg format contract'
        )
    source_identity = {
        'route': source_route,
        'engine': engine,
        'host': source_config.get('host'),
        'port': source_config.get('port'),
        'database': source_config.get('dbname'),
        'user': source_config.get('user'),
        'replica_host': source_config.get('replica_host'),
        'replica_port': source_config.get('replica_port'),
        'replica_user': source_config.get('replica_user'),
        'table': source_table,
    }
    if engine in ('mysql', 'mariadb'):
        source_identity.update({
            'charset': source_config.get('charset', DEFAULT_CHARSET),
            'session_sqls': list(
                source_config.get('session_sqls', DEFAULT_SESSION_SQLS)
            ),
            'use_gtid': source_config.get('use_gtid', DEFAULT_USE_GTID),
        })
    elif engine == 'postgres':
        source_identity['ssl'] = source_config.get('ssl')

    identity = {
        'source': source_identity,
        'target': {
            'account': target_config.get('account'),
            'database': target_config.get('dbname'),
            'schema': target_schema,
            'table': target_table,
            'user': target_config.get('user'),
            'role': target_config.get('role'),
            'target_table_format': target_config['target_table_format'],
            'iceberg_version': iceberg_version,
        },
        'staging': dict(staging_config),
    }
    if partial_boundary is not None:
        identity['partial_boundary'] = dict(partial_boundary)
    return build_recovery_identity(
        'fastsync',
        identity,
        transformation_config=args.transform or {},
        stream_identity={
            'tap_id': target_config.get('tap_id'),
            'route': source_route,
            'table': source_table,
        },
        target_table_format=target_config['target_table_format'],
        iceberg_version=iceberg_version,
    )


def create_spec(args, target_schema, table, columns, primary_key):
    """Build the canonical target specification for one FastSync stream."""
    target = target_name(args, target_schema, table)
    return IcebergTableSpec.from_fastsync(
        target.database,
        target.schema,
        target.table,
        columns,
        primary_key,
        args.target['iceberg_version'],
    )


def target_name(args, target_schema, table):
    """Return the canonical FastSync target without source introspection."""
    target_table = utils.tablename_to_dict(table)['table_name']
    return SnowflakeObjectName(
        args.target['dbname'].upper(),
        target_schema.upper(),
        target_table.upper(),
    )


def require_partial_sync_primary_key(primary_key, table):
    """Reject PartialSync before export when deterministic MERGE is impossible."""
    if not primary_key:
        raise ValueError(f'Iceberg PartialSync requires a primary key for {table}')


def plan_staging_uploads(publisher, attempt, snowflake, file_parts):
    """Persist deterministic S3 keys before an Iceberg upload can start."""
    s3_keys = utils.get_expected_s3_keys(snowflake, file_parts)
    publisher.record_planned_uploads(attempt, s3_keys)
    return s3_keys


def validate_recovery_source_spec(
    persisted_spec: IcebergTableSpec,
    current_spec: IcebergTableSpec,
):
    """Ensure a fresh source export can satisfy the persisted publication contract."""
    if current_spec.name != persisted_spec.name:
        raise TableCompatibilityError(
            'The current source mapping resolves to a different Iceberg target'
        )

    current_columns = {column.name: column for column in current_spec.columns}
    persisted_columns = {column.name: column for column in persisted_spec.columns}
    missing = [
        column.name
        for column in persisted_spec.columns
        if column.name not in current_columns
    ]
    incompatible = [
        column.name
        for column in persisted_spec.columns
        if column.name in current_columns
        and current_columns[column.name] != column
    ]
    additional = [
        column.name
        for column in current_spec.columns
        if column.name not in persisted_columns
    ]
    order_changed = (
        not missing
        and not additional
        and not incompatible
        and tuple(column.name for column in current_spec.columns)
        != tuple(column.name for column in persisted_spec.columns)
    )
    if missing or incompatible or additional or order_changed:
        details = []
        if missing:
            details.append(f'missing columns: {", ".join(missing)}')
        if incompatible:
            details.append(f'changed columns: {", ".join(incompatible)}')
        if additional:
            details.append(f'added columns: {", ".join(additional)}')
        if order_changed:
            details.append('column order changed')
        raise TableCompatibilityError(
            'The current source schema cannot resume the persisted Iceberg '
            f'publication contract ({"; ".join(details)})'
        )

    if current_spec.primary_key != persisted_spec.primary_key:
        raise TableCompatibilityError(
            'The current source primary key cannot resume the persisted Iceberg '
            'publication contract'
        )


def finalize_attempt(
    publisher,
    snowflake,
    target_config,
    target_schema,
    table,
    attempt,
    cleanup_context,
):
    """Finish every post-publication action before marking an attempt final."""
    completed = getattr(attempt, 'finalization', {})

    def durable_action(name, action):
        def run():
            if completed.get(name) is True:
                return
            action()
            publisher.record_finalization_action(attempt, name)

        return run

    actions = []
    if attempt.manifest_payload.replacement_metadata is not None:
        actions.append((
            'metadata restoration',
            durable_action(
                FINALIZATION_METADATA,
                lambda: publisher.restore_metadata(attempt),
            ),
        ))
    actions.extend([
        (
            'grant application',
            durable_action(
                FINALIZATION_GRANTS,
                lambda: utils.retry_snowflake_table_grants(
                    snowflake, target_config, target_schema, table
                ),
            ),
        ),
        (
            'S3 staging cleanup',
            durable_action(
                FINALIZATION_S3_CLEANUP,
                lambda: utils.delete_s3_objects(
                    snowflake,
                    attempt.s3_keys,
                    target_config.get('s3_bucket'),
                    cleanup_context=cleanup_context,
                ),
            ),
        ),
        (
            'Snowflake staging cleanup',
            durable_action(
                FINALIZATION_STAGING_CLEANUP,
                lambda: snowflake.drop_table(
                    target_schema,
                    table,
                    is_temporary=True,
                    max_attempts=3,
                    staging_table_name=attempt.staging_table,
                ),
            ),
        ),
    ])
    utils.run_post_publication_actions(actions)
    publisher.mark_finalized(attempt)


def complete_state_handoff(publisher, attempt, state_writer):
    """Remove recovery state only after the route's Singer state is durable."""
    state_writer(attempt.source_bookmark)
    publisher.complete_state_handoff(attempt)


def complete_partial_state_handoff(publisher, attempt, state_path, table):
    """Advance unbounded PartialSync state from the persisted attempt range."""
    if attempt.manifest_payload.end_is_unbounded:
        utils.save_state_file(state_path, table, attempt.source_bookmark)
    publisher.complete_state_handoff(attempt)


def restart_staging(
    publisher,
    snowflake,
    target_config,
    target_schema,
    table,
    attempt,
):
    """Clean an incomplete native stage before replaying its saved boundary."""
    utils.run_post_publication_actions([
        (
            'S3 staging cleanup',
            lambda: utils.delete_s3_objects(
                snowflake,
                attempt.s3_keys,
                target_config.get('s3_bucket'),
                cleanup_context='Incomplete Iceberg staging cleanup',
            ),
        ),
        (
            'Snowflake staging cleanup',
            lambda: snowflake.drop_table(
                target_schema,
                table,
                is_temporary=True,
                max_attempts=3,
                staging_table_name=attempt.staging_table,
            ),
        ),
    ])
    publisher.reset_staging(attempt)


def resume_attempt(
    publisher,
    attempt,
    spec,
    restart,
    publish,
    finalize,
    state_handoff,
):
    """Dispatch the publisher's durable recovery decision in route order."""
    outcome = publisher.reconcile(attempt, spec)
    if outcome.action == RECOVERY_RESTART_STAGING:
        restart()
        return False
    if outcome.action == RECOVERY_PUBLISH:
        publish()
        finalize()
    elif outcome.action == RECOVERY_FINALIZE:
        finalize()
    elif outcome.action != RECOVERY_STATE_HANDOFF:
        raise RuntimeError(f'Unsupported Iceberg recovery action: {outcome.action}')

    state_handoff()
    return True
