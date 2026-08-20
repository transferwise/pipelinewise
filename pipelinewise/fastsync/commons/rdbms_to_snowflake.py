"""Shared MySQL/PostgreSQL FullSync lifecycle for Snowflake targets."""
import glob
import os

from contextlib import ExitStack
from dataclasses import dataclass, field
from typing import Any, List, Optional, Union

from pipelinewise.fastsync.commons import snowflake_iceberg_routes as iceberg_routes
from pipelinewise.fastsync.commons.rdbms_source import RdbmsSnowflakeSource
from pipelinewise.fastsync.commons.utils import StagingUploadError


@dataclass
class _FullSyncRun:  # pylint: disable=too-many-instance-attributes
    """Mutable state passed between ordered FullSync phases."""

    table: str
    args: Any
    source_adapter: RdbmsSnowflakeSource
    snowflake: Any
    logger: Any
    route_utils: Any
    iceberg_version: Optional[int]
    iceberg_requested: bool
    iceberg_operation: ExitStack = field(default_factory=ExitStack)
    source: Any = None
    publisher: Any = None
    attempt: Any = None
    spec: Any = None
    staging_config: Any = None
    recovery_identity: Any = None
    target_schema: Optional[str] = None
    filepath: Optional[str] = None
    bookmark: Any = None
    snowflake_columns: Any = None
    primary_key: Any = None
    file_parts: List[str] = field(default_factory=list)
    size_bytes: int = 0
    s3_keys: List[str] = field(default_factory=list)
    s3_key_pattern: Optional[str] = None
    temp_created: bool = False
    inserted_rows: Any = None


def sync_table(
    table: str,
    args,
    source_adapter: RdbmsSnowflakeSource,
    target_factory,
    logger,
    route_utils,
) -> Union[bool, str]:
    """Run one RDBMS-to-Snowflake FullSync with shared publication ordering."""
    iceberg_version = iceberg_routes.validate_route_config(args.target)
    run = _FullSyncRun(
        table=table,
        args=args,
        source_adapter=source_adapter,
        snowflake=target_factory(args.target, args.transform),
        logger=logger,
        route_utils=route_utils,
        iceberg_version=iceberg_version,
        iceberg_requested=iceberg_version is not None,
    )

    try:
        if _prepare_full_run(run):
            return True
        _export_full_source(run)
        _stage_full_export(run)
        if run.iceberg_requested:
            return _publish_full_iceberg(run)
        return _publish_full_native(run)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return _full_failure_result(run, exc)
    finally:
        try:
            if run.source is not None:
                run.source_adapter.close_finally(run.source)
        finally:
            run.iceberg_operation.close()


def _prepare_full_run(run: _FullSyncRun) -> bool:
    run.staging_config = (
        iceberg_routes.staging_config_identity(run.args.target)
        if run.iceberg_requested
        else None
    )
    run.recovery_identity = (
        iceberg_routes.fastsync_recovery_identity(
            run.args,
            run.table,
            source_route=run.source_adapter.route_name,
            source_engine=run.source_adapter.source_engine(run.args),
            staging_config=run.staging_config,
            iceberg_version=run.iceberg_version,
        )
        if run.iceberg_requested
        else None
    )
    filename = run.route_utils.gen_export_filename(
        tap_id=run.args.target.get('tap_id'), table=run.table
    )
    run.filepath = os.path.join(run.args.temp_dir, filename)
    run.target_schema = run.route_utils.get_target_schema(
        run.args.target, run.table
    )

    if not run.iceberg_requested:
        iceberg_routes.require_native_target_format(
            run.snowflake,
            run.args,
            run.target_schema,
            run.table,
            allow_missing=True,
        )

    run.source = run.source_adapter.create(run.args, run.iceberg_requested)
    return _recover_full_attempt(run) if run.iceberg_requested else False


def _recover_full_attempt(run: _FullSyncRun) -> bool:
    target_name = iceberg_routes.target_name(
        run.args, run.target_schema, run.table
    )
    run.publisher = iceberg_routes.create_publisher(run.snowflake, run.args)
    run.iceberg_operation.enter_context(
        run.publisher.table_lock(target_name, run.recovery_identity)
    )
    run.attempt = run.publisher.load_attempt(
        target_name,
        expected_kind='full',
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
            run.table,
            run.attempt,
        ),
        publish=lambda: run.publisher.publish_full_sync(run.attempt, run.spec),
        finalize=lambda: iceberg_routes.finalize_attempt(
            run.publisher,
            run.snowflake,
            run.args.target,
            run.target_schema,
            run.table,
            run.attempt,
            'Successful Iceberg FullSync staging cleanup',
        ),
        state_handoff=lambda: iceberg_routes.complete_state_handoff(
            run.publisher,
            run.attempt,
            lambda source_bookmark: run.route_utils.save_state_file(
                run.args.state, run.table, source_bookmark
            ),
        ),
    )


def _export_full_source(run: _FullSyncRun) -> None:
    run.source_adapter.open(run.source)
    if run.iceberg_requested:
        _plan_full_iceberg_export(run)
    else:
        run.bookmark = run.route_utils.get_bookmark_for_table(
            run.table,
            run.args.properties,
            run.source,
            **run.source_adapter.bookmark_kwargs(run.args),
        )

    run.source.copy_table(
        run.table,
        run.filepath,
        split_large_files=run.args.target.get('split_large_files'),
        split_file_chunk_size_mb=run.args.target.get(
            'split_file_chunk_size_mb'
        ),
        split_file_max_chunks=run.args.target.get('split_file_max_chunks'),
    )
    snowflake_types, run.file_parts, run.size_bytes = (
        run.source_adapter.complete_full_export(
            run.source,
            run.table,
            run.iceberg_requested,
            lambda exported_types: _validate_full_export(run, exported_types),
            lambda: _inspect_full_export(run),
        )
    )
    if not run.iceberg_requested:
        run.snowflake_columns = snowflake_types.get('columns', [])
        run.primary_key = snowflake_types.get('primary_key')


def _plan_full_iceberg_export(run: _FullSyncRun) -> None:
    snowflake_types = run.source.map_column_types_to_target(run.table)
    run.snowflake_columns = snowflake_types.get('columns', [])
    run.primary_key = snowflake_types.get('primary_key')
    current_spec = iceberg_routes.create_spec(
        run.args,
        run.target_schema,
        run.table,
        run.snowflake_columns,
        run.primary_key,
    )
    if run.attempt is not None:
        iceberg_routes.validate_recovery_source_spec(run.spec, current_spec)
        run.bookmark = run.attempt.source_bookmark
        run.publisher.plan_full_sync(run.attempt, run.spec)
        return

    run.spec = current_spec
    run.bookmark = run.route_utils.get_bookmark_for_table(
        run.table,
        run.args.properties,
        run.source,
        **run.source_adapter.bookmark_kwargs(run.args),
    )
    run.attempt = run.publisher.prepare_full_sync(
        run.spec,
        run.bookmark,
        recovery_identity=run.recovery_identity,
        staging_config=run.staging_config,
    )
    run.publisher.plan_full_sync(run.attempt, run.spec)


def _validate_full_export(run: _FullSyncRun, snowflake_types) -> None:
    if not run.iceberg_requested:
        return
    exported_spec = iceberg_routes.create_spec(
        run.args,
        run.target_schema,
        run.table,
        snowflake_types.get('columns', []),
        snowflake_types.get('primary_key'),
    )
    iceberg_routes.validate_recovery_source_spec(run.spec, exported_spec)


def _inspect_full_export(run: _FullSyncRun):
    file_exists = os.path.exists(run.filepath)
    file_parts = glob.glob(f'{run.filepath}*')
    if not file_parts and file_exists:
        run.logger.warning('DATA LOSS! -> %s', run.filepath)
    return file_parts, sum(os.path.getsize(path) for path in file_parts)


def _upload_full_export(run: _FullSyncRun) -> None:
    upload_options = {}
    if run.iceberg_requested:
        upload_options['planned_s3_keys'] = iceberg_routes.plan_staging_uploads(
            run.publisher, run.attempt, run.snowflake, run.file_parts
        )
    try:
        run.s3_keys, run.s3_key_pattern = run.route_utils.upload_files_to_s3(
            run.snowflake,
            run.file_parts,
            run.args.temp_dir,
            run.args.target.get('s3_bucket'),
            **upload_options,
        )
    except StagingUploadError as exc:
        if run.iceberg_requested:
            run.publisher.record_uploaded(run.attempt, exc.s3_keys)
        raise
    if run.iceberg_requested:
        run.publisher.record_uploaded(run.attempt, run.s3_keys)


def _stage_full_export(run: _FullSyncRun) -> None:
    _upload_full_export(run)
    if not run.iceberg_requested:
        run.snowflake.create_schema(run.target_schema)

    run.temp_created = True
    staging_table_name = (
        run.attempt.staging_table if run.attempt is not None else None
    )
    staging_options = (
        {'staging_table_name': staging_table_name}
        if run.iceberg_requested
        else {}
    )
    run.snowflake.create_table(
        run.target_schema,
        run.table,
        run.snowflake_columns,
        run.primary_key,
        is_temporary=True,
        **staging_options,
    )
    if run.iceberg_requested:
        run.publisher.record_staging_created(run.attempt)

    run.inserted_rows = run.snowflake.copy_to_table(
        run.s3_key_pattern,
        run.target_schema,
        run.table,
        run.size_bytes,
        is_temporary=True,
        **staging_options,
    )
    if run.args.target.get('archive_load_files', False):
        for s3_key in run.s3_keys:
            run.snowflake.copy_to_archive(
                s3_key, run.args.target.get('tap_id'), run.table
            )
    run.snowflake.obfuscate_columns(
        run.target_schema,
        run.table,
        **staging_options,
    )


def _publish_full_iceberg(run: _FullSyncRun) -> bool:
    staged_row_count, staged_fingerprint = run.publisher.staging_evidence(
        run.attempt, run.spec, run.inserted_rows
    )
    run.publisher.record_staged(
        run.attempt,
        row_count=staged_row_count,
        row_fingerprint=staged_fingerprint,
    )
    run.publisher.publish_full_sync(run.attempt, run.spec)
    iceberg_routes.finalize_attempt(
        run.publisher,
        run.snowflake,
        run.args.target,
        run.target_schema,
        run.table,
        run.attempt,
        'Successful Iceberg FullSync staging cleanup',
    )
    iceberg_routes.complete_state_handoff(
        run.publisher,
        run.attempt,
        lambda source_bookmark: run.route_utils.save_state_file(
            run.args.state, run.table, source_bookmark
        ),
    )
    return True


def _publish_full_native(run: _FullSyncRun) -> bool:
    run.snowflake.create_table(
        run.target_schema,
        run.table,
        run.snowflake_columns,
        run.primary_key,
        allow_replace_table=False,
        normalize_primary_keys=False,
    )
    iceberg_routes.require_native_target_format(
        run.snowflake,
        run.args,
        run.target_schema,
        run.table,
        allow_missing=False,
    )
    run.route_utils.apply_snowflake_table_grants(
        run.snowflake,
        run.args.target,
        run.target_schema,
        run.table,
        is_temporary=True,
    )
    publication_error = None
    try:
        run.snowflake.swap_tables(
            run.target_schema, run.table, cleanup_old_table=False
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        publication_error = exc

    run.route_utils.finalize_snowflake_fullsync(
        run.snowflake,
        run.s3_keys,
        run.args.target.get('s3_bucket'),
        run.args.target,
        run.target_schema,
        run.table,
        publication_error=publication_error,
    )
    run.s3_keys = []
    run.temp_created = False
    if publication_error:
        raise publication_error

    run.route_utils.save_state_file(run.args.state, run.table, run.bookmark)
    return True


def _full_failure_result(run: _FullSyncRun, exc: Exception) -> str:
    if run.iceberg_requested:
        return iceberg_routes.publication_failure_result(
            run.logger, run.table, exc
        )
    run.logger.exception(exc)
    return run.route_utils.staging_failure_result(
        run.snowflake,
        getattr(exc, 's3_keys', run.s3_keys),
        run.args.target.get('s3_bucket'),
        run.target_schema,
        run.table,
        run.temp_created,
        exc,
    )
