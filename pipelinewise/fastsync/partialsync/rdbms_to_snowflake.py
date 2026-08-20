"""Shared MySQL/PostgreSQL PartialSync lifecycle for Snowflake targets."""
import copy
import os

from contextlib import ExitStack
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from pipelinewise.fastsync.commons import snowflake_iceberg_routes as iceberg_routes
from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.commons.rdbms_source import RdbmsSnowflakeSource
from pipelinewise.fastsync.commons.snowflake_iceberg import PartialSyncBoundary
from pipelinewise.fastsync.partialsync import utils


@dataclass
class _PartialSyncRun:  # pylint: disable=too-many-instance-attributes
    """Mutable state passed between ordered PartialSync phases."""

    table: tuple
    args: Any
    source_adapter: RdbmsSnowflakeSource
    snowflake: Any
    logger: Any
    iceberg_version: Optional[int]
    iceberg_requested: bool
    iceberg_operation: ExitStack = field(default_factory=ExitStack)
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    has_dynamic_boundary: bool = False
    target_schema: Optional[str] = None
    target_table: Optional[str] = None
    table_dict: Dict[str, Any] = field(default_factory=dict)
    source: Any = None
    publisher: Any = None
    attempt: Any = None
    spec: Any = None
    staging_config: Any = None
    recovery_identity: Any = None
    bookmark: Any = None
    where_clause_sql: Optional[str] = None
    source_columns: Any = None
    primary_keys: Any = None
    file_parts: List[str] = field(default_factory=list)
    size_bytes: int = 0
    s3_keys: List[str] = field(default_factory=list)
    s3_key_pattern: Optional[str] = None
    temp_created: bool = False
    publication_status: Dict[str, bool] = field(
        default_factory=lambda: {'attempted': False}
    )
    grants_attempted: bool = False
    target_sf: Any = None


def partial_sync_table(
    table: tuple,
    args,
    source_adapter: RdbmsSnowflakeSource,
    target_factory,
    logger,
) -> Union[bool, str]:
    """Run one RDBMS-to-Snowflake PartialSync with shared publication ordering."""
    iceberg_version = iceberg_routes.validate_route_config(args.target)
    run = _PartialSyncRun(
        table=table,
        args=args,
        source_adapter=source_adapter,
        snowflake=target_factory(args.target, args.transform),
        logger=logger,
        iceberg_version=iceberg_version,
        iceberg_requested=iceberg_version is not None,
    )

    try:
        if _prepare_partial_run(run):
            return True
        if not _export_partial_source(run):
            return True
        _stage_partial_export(run)
        if run.iceberg_requested:
            return _publish_partial_iceberg(run)
        return _publish_partial_native(run)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return _partial_failure_result(run, exc)
    finally:
        run.iceberg_operation.close()


def _prepare_partial_run(run: _PartialSyncRun) -> bool:
    run.table_name = run.table[0]
    run.staging_config = (
        iceberg_routes.staging_config_identity(run.args.target)
        if run.iceberg_requested
        else None
    )
    run.recovery_identity = (
        iceberg_routes.fastsync_recovery_identity(
            run.args,
            run.table_name,
            source_route=run.source_adapter.route_name,
            source_engine=run.source_adapter.source_engine(run.args),
            staging_config=run.staging_config,
            iceberg_version=run.iceberg_version,
        )
        if run.iceberg_requested
        else None
    )
    run.column_name = run.table[1]['column']
    run.args = copy.copy(run.args)
    run.args.drop_target_table = run.table[1]['drop_target_table']
    run.args.table = run.table_name
    run.has_dynamic_boundary = any(
        isinstance(value, str) and value.startswith('<D>')
        for value in (
            run.table[1]['start_value'],
            run.table[1]['end_value'],
        )
    )

    if run.iceberg_requested or not run.has_dynamic_boundary:
        _resolve_partial_target(run)
    if run.iceberg_requested:
        return _recover_partial_attempt(run)
    if not run.has_dynamic_boundary:
        _require_native_partial_target(run)
    return False


def _resolve_partial_target(run: _PartialSyncRun) -> None:
    run.target_schema = common_utils.get_target_schema(
        run.args.target, run.table_name
    )
    run.table_dict = common_utils.tablename_to_dict(run.table_name)
    run.target_table = run.table_dict.get('table_name')


def _require_native_partial_target(run: _PartialSyncRun) -> None:
    iceberg_routes.require_native_target_format(
        run.snowflake,
        run.args,
        run.target_schema,
        run.table_name,
        allow_missing=True,
    )


def _recover_partial_attempt(run: _PartialSyncRun) -> bool:
    target_name = iceberg_routes.target_name(
        run.args, run.target_schema, run.table_name
    )
    run.publisher = iceberg_routes.create_publisher(run.snowflake, run.args)
    run.iceberg_operation.enter_context(
        run.publisher.table_lock(target_name, run.recovery_identity)
    )
    run.attempt = run.publisher.load_attempt(
        target_name,
        expected_kind='partial',
        recovery_identity=run.recovery_identity,
        staging_config=run.staging_config,
    )
    run.snowflake.create_schema(run.target_schema)
    if run.attempt is None:
        return False

    run.spec = run.attempt.table_spec
    return iceberg_routes.resume_attempt(
        run.publisher,
        run.attempt,
        run.spec,
        restart=lambda: iceberg_routes.restart_staging(
            run.publisher,
            run.snowflake,
            run.args.target,
            run.target_schema,
            run.table_name,
            run.attempt,
        ),
        publish=lambda: run.publisher.publish_partial_sync(
            run.attempt, run.spec
        ),
        finalize=lambda: iceberg_routes.finalize_attempt(
            run.publisher,
            run.snowflake,
            run.args.target,
            run.target_schema,
            run.table_name,
            run.attempt,
            'Successful Iceberg PartialSync staging cleanup',
        ),
        state_handoff=lambda: iceberg_routes.complete_partial_state_handoff(
            run.publisher, run.attempt, run.args.state, run.table_name
        ),
    )


def _export_partial_source(run: _PartialSyncRun) -> bool:
    run.source = run.source_adapter.create(run.args, run.iceberg_requested)
    try:
        run.source_adapter.open(run.source)
        ready = (
            _prepare_iceberg_partial_export(run)
            if run.iceberg_requested
            else _prepare_native_partial_export(run)
        )
        if not ready:
            return False

        run.file_parts = run.source.export_source_table_data(
            run.args,
            run.args.target.get('tap_id'),
            run.where_clause_sql,
        )
        if run.iceberg_requested:
            _validate_partial_export(run)
        return True
    finally:
        run.source_adapter.close_partial(run.source)


def _prepare_iceberg_partial_export(run: _PartialSyncRun) -> bool:
    snowflake_types = run.source.map_column_types_to_target(run.table_name)
    run.source_columns = snowflake_types.get('columns', [])
    run.primary_keys = snowflake_types.get('primary_key')
    current_spec = iceberg_routes.create_spec(
        run.args,
        run.target_schema,
        run.table_name,
        run.source_columns,
        run.primary_keys,
    )
    if run.attempt is not None:
        run.spec = run.attempt.table_spec
        iceberg_routes.validate_recovery_source_spec(run.spec, current_spec)
        run.bookmark = run.attempt.source_bookmark
        run.where_clause_sql = run.attempt.manifest_payload.where_clause_sql
        run.publisher.plan_partial_sync(run.attempt, run.spec)
        return True

    run.spec = current_spec
    iceberg_routes.require_partial_sync_primary_key(
        run.primary_keys, run.table_name
    )
    boundary_values = _resolve_partial_boundary(run)
    if boundary_values is None:
        return False
    start_value, end_value = boundary_values
    run.bookmark = common_utils.get_bookmark_for_table(
        run.table_name,
        run.args.properties,
        run.source,
        **run.source_adapter.bookmark_kwargs(run.args),
    )
    run.where_clause_sql = _where_clause(
        run.column_name, start_value, end_value
    )
    run.attempt = run.publisher.prepare_partial_sync(
        run.spec,
        run.bookmark,
        PartialSyncBoundary(
            run.where_clause_sql,
            start_value=start_value,
            end_value=end_value,
            drop_target=run.args.drop_target_table,
        ),
        recovery_identity=run.recovery_identity,
        staging_config=run.staging_config,
    )
    run.publisher.plan_partial_sync(run.attempt, run.spec)
    return True


def _prepare_native_partial_export(run: _PartialSyncRun) -> bool:
    boundary_values = _resolve_partial_boundary(run)
    if boundary_values is None:
        return False
    start_value, end_value = boundary_values

    if run.has_dynamic_boundary:
        _resolve_partial_target(run)
        _require_native_partial_target(run)

    run.bookmark = common_utils.get_bookmark_for_table(
        run.table_name,
        run.args.properties,
        run.source,
        **run.source_adapter.bookmark_kwargs(run.args),
    )
    snowflake_types = run.source.map_column_types_to_target(run.table_name)
    run.source_columns = snowflake_types.get('columns', [])
    run.primary_keys = snowflake_types.get('primary_key')
    run.where_clause_sql = _where_clause(
        run.column_name, start_value, end_value
    )
    return True


def _resolve_partial_boundary(run: _PartialSyncRun):
    start_value = utils.validate_boundary_value(
        run.source.query, run.table[1]['start_value']
    )
    end_value = utils.validate_boundary_value(
        run.source.query, run.table[1]['end_value']
    )
    if (
        start_value is utils.DYNAMIC_BOUNDARY_NOT_READY
        or end_value is utils.DYNAMIC_BOUNDARY_NOT_READY
    ):
        run.logger.info(
            'Dynamic boundary returned no value for %s; skipping PartialSync',
            run.table_name,
        )
        return None
    return start_value, end_value


def _validate_partial_export(run: _PartialSyncRun) -> None:
    exported_types = run.source.map_column_types_to_target(run.table_name)
    exported_spec = iceberg_routes.create_spec(
        run.args,
        run.target_schema,
        run.table_name,
        exported_types.get('columns', []),
        exported_types.get('primary_key'),
    )
    iceberg_routes.validate_recovery_source_spec(run.spec, exported_spec)


def _stage_partial_export(run: _PartialSyncRun) -> None:
    run.target_sf = {
        'sf_object': run.snowflake,
        'schema': run.target_schema,
        'table': run.target_table,
        'temp': run.table_dict.get('temp_table_name'),
        'publication_status': run.publication_status,
    }
    if not run.iceberg_requested:
        run.snowflake.create_schema(run.target_schema)

    staging_table_name = (
        run.attempt.staging_table if run.attempt is not None else None
    )
    run.size_bytes = sum(os.path.getsize(path) for path in run.file_parts)
    if run.iceberg_requested:
        _stage_partial_iceberg(run, staging_table_name)
    else:
        _stage_partial_native(run)


def _stage_partial_iceberg(run: _PartialSyncRun, staging_table_name: str) -> None:
    planned_s3_keys = iceberg_routes.plan_staging_uploads(
        run.publisher, run.attempt, run.snowflake, run.file_parts
    )
    try:
        run.s3_keys, run.s3_key_pattern = utils.upload_to_s3(
            run.snowflake,
            run.file_parts,
            run.args.temp_dir,
            planned_s3_keys=planned_s3_keys,
        )
    except common_utils.StagingUploadError as exc:
        run.publisher.record_uploaded(run.attempt, exc.s3_keys)
        raise
    run.publisher.record_uploaded(run.attempt, run.s3_keys)
    run.snowflake.create_table(
        run.target_schema,
        run.target_table,
        run.source_columns,
        run.primary_keys,
        is_temporary=True,
        staging_table_name=staging_table_name,
    )
    run.publisher.record_staging_created(run.attempt)


def _stage_partial_native(run: _PartialSyncRun) -> None:
    run.temp_created = True
    run.snowflake.create_table(
        run.target_schema,
        run.target_table,
        run.source_columns,
        run.primary_keys,
        is_temporary=True,
    )
    run.s3_keys, run.s3_key_pattern = utils.upload_to_s3(
        run.snowflake, run.file_parts, run.args.temp_dir
    )


def _publish_partial_iceberg(run: _PartialSyncRun) -> bool:
    inserted_rows = run.snowflake.copy_to_table(
        run.s3_key_pattern,
        run.target_schema,
        run.target_table,
        run.size_bytes,
        is_temporary=True,
        staging_table_name=run.attempt.staging_table,
    )
    run.snowflake.obfuscate_columns(
        run.target_schema,
        run.table_name,
        staging_table_name=run.attempt.staging_table,
    )
    staged_row_count, staged_fingerprint = run.publisher.staging_evidence(
        run.attempt, run.spec, inserted_rows
    )
    run.publisher.record_staged(
        run.attempt,
        row_count=staged_row_count,
        row_fingerprint=staged_fingerprint,
    )
    run.publisher.publish_partial_sync(run.attempt, run.spec)
    iceberg_routes.finalize_attempt(
        run.publisher,
        run.snowflake,
        run.args.target,
        run.target_schema,
        run.table_name,
        run.attempt,
        'Successful Iceberg PartialSync staging cleanup',
    )
    iceberg_routes.complete_partial_state_handoff(
        run.publisher, run.attempt, run.args.state, run.table_name
    )
    return True


def _publish_partial_native(run: _PartialSyncRun) -> bool:
    utils.load_into_snowflake(
        run.target_sf,
        run.args,
        run.source_columns,
        run.primary_keys,
        run.s3_key_pattern,
        run.size_bytes,
        run.where_clause_sql,
    )
    run.publication_status['attempted'] = True
    run.temp_created = False
    run.grants_attempted = True
    common_utils.retry_snowflake_table_grants(
        run.snowflake,
        run.args.target,
        run.target_schema,
        run.table_name,
    )
    utils.delete_s3_objects(
        run.snowflake, run.s3_keys, run.args.target.get('s3_bucket')
    )
    run.s3_keys = []
    utils.update_state_file(run.args, run.bookmark)
    return True


def _partial_failure_result(run: _PartialSyncRun, exc: Exception) -> str:
    if run.iceberg_requested:
        return iceberg_routes.publication_failure_result(
            run.logger, run.table_name, exc
        )
    run.logger.exception(exc)
    return common_utils.partial_sync_failure_result(
        run.snowflake,
        run.args.target,
        run.table_name,
        run.target_schema,
        run.target_table,
        {
            's3_keys': getattr(exc, 's3_keys', run.s3_keys),
            'temp_created': run.temp_created,
            'publication_attempted': run.publication_status['attempted'],
            'grants_attempted': run.grants_attempted,
        },
        exc,
    )


def _where_clause(column_name, start_value, end_value) -> str:
    start_value_for_query = (
        start_value if start_value == 'NULL' else f'\'{start_value}\''
    )
    where_clause_sql = f' WHERE {column_name} >= {start_value_for_query}'
    if end_value is not None:
        where_clause_sql += f' AND {column_name} <= \'{end_value}\''
    return where_clause_sql
