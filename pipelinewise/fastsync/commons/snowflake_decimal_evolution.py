"""Plan resumable decimal column versioning for managed Iceberg PartialSync."""

from dataclasses import replace

from .snowflake_column_versioning import (
    has_column_versions, is_decimal_version_change, is_retained_legacy_decimal_float, is_versioned_column,
    versioned_column_name,
)
from .snowflake_iceberg_model import IcebergColumn, quote_identifier
from .snowflake_iceberg_recovery import RecoveryManifestError, TableCompatibilityError


def partial_compatibility(
    expected, actual, allow_versions=True, boundary_column=None, historical_columns=None, decimal_columns=(),
    force_precision_columns=False,
):
    """Allow nullable historical columns while retaining source and key checks."""
    if actual is None or expected.primary_key != actual.primary_key:
        return 'incompatible', ()
    expected_columns = {column.name: column for column in expected.columns}
    actual_columns = {column.name: column for column in actual.columns}
    decimal_columns = {name.upper() for name in decimal_columns or ()}
    if historical_columns is not None and set(historical_columns).intersection(expected_columns):
        return 'incompatible', ()
    if boundary_column and has_column_versions(boundary_column.upper(), expected_columns, actual_columns):
        raise TableCompatibilityError(
            f'PartialSync boundary column {boundary_column} has historical versions; '
            'run FullSync to recreate the target'
        )
    has_versions = False
    for name, column in actual_columns.items():
        desired = expected_columns.get(name)
        if desired == column:
            continue
        if desired is None and column.nullable and is_versioned_column(name, expected_columns):
            if historical_columns is None or historical_columns.get(name) == column.data_type:
                continue
        if (
            desired is not None
            and name in decimal_columns
            and (not force_precision_columns or name in expected.primary_key)
            and is_retained_legacy_decimal_float(column.data_type, desired.data_type)
        ):
            continue
        if (desired is not None and name in decimal_columns and
                is_decimal_version_change(column.data_type, desired.data_type)):
            if name in expected.primary_key:
                raise TableCompatibilityError(f'Cannot version primary-key column {name}')
            _reject_boundary_version(name, boundary_column)
            if allow_versions and column.nullable and desired.nullable:
                has_versions = True
                continue
        return 'incompatible', ()
    if historical_columns is not None and not set(historical_columns).issubset(actual_columns):
        return 'incompatible', ()
    additions = tuple(column for column in expected.columns if column.name not in actual_columns)
    if any(not column.nullable for column in additions):
        return 'incompatible', ()
    return ('versioning' if has_versions else 'additive' if additions else 'exact'), additions


def plan_column_versions(
    expected, actual, boundary_column=None, decimal_columns=(), force_precision_columns=False,
):
    """Persist archive identities before any independently committed DDL."""
    compatibility, _ = partial_compatibility(
        expected, actual, boundary_column=boundary_column, decimal_columns=decimal_columns,
        force_precision_columns=force_precision_columns,
    )
    if compatibility == 'incompatible':
        raise TableCompatibilityError('Existing Iceberg table is incompatible with PartialSync')
    expected_columns = {column.name: column for column in expected.columns}
    names = {column.name for column in actual.columns} | set(expected_columns)
    versions = {}
    for column in actual.columns:
        desired = expected_columns.get(column.name)
        if desired is None or column.data_type == desired.data_type:
            continue
        if (
            column.name in decimal_columns
            and (not force_precision_columns or column.name in expected.primary_key)
            and is_retained_legacy_decimal_float(column.data_type, desired.data_type)
        ):
            continue
        archived_name = versioned_column_name(column.name)
        if archived_name in names:
            raise TableCompatibilityError(f'Historical column already exists: {archived_name}')
        versions[column.name] = {'archived_name': archived_name, 'data_type': column.data_type}
        names.add(archived_name)
    return versions


def with_retained_decimal_types(
    expected, actual, decimal_columns=(), force_precision_columns=False,
):
    """Use existing FLOAT staging types for retained legacy decimal columns."""
    if actual is None:
        return expected
    decimal_columns = {name.upper() for name in decimal_columns or ()}
    actual_columns = {column.name: column for column in actual.columns}
    columns = tuple(
        replace(column, data_type=actual_columns[column.name].data_type)
        if (
            column.name in decimal_columns
            and column.name in actual_columns
            and (not force_precision_columns or column.name in expected.primary_key)
            and is_retained_legacy_decimal_float(
                actual_columns[column.name].data_type,
                column.data_type,
            )
        )
        else column
        for column in expected.columns
    )
    return replace(expected, columns=columns)


def partial_preparation(
    expected, actual, versions, boundary_column=None, historical_columns=None,
    decimal_columns=(), force_precision_columns=False,
):
    """Resume before or after each rename/add without repeating a rename."""
    columns = {column.name: column for column in actual.columns}
    desired_columns = {column.name: column for column in expected.columns}
    statements = []
    for name, version in versions.items():
        desired = desired_columns.get(name)
        if desired is None or name in expected.primary_key:
            raise RecoveryManifestError('Invalid decimal column version in PartialSync recovery')
        _reject_boundary_version(name, boundary_column)
        old = IcebergColumn(name, version['data_type'], True, desired.iceberg_version)
        archived = replace(old, name=version['archived_name'])
        _resume_column_version(expected, columns, old, archived, desired, statements)
    evolved = replace(actual, columns=tuple(columns.values()))
    compatibility, additions = partial_compatibility(
        expected, evolved, allow_versions=False, boundary_column=boundary_column,
        historical_columns=retained_column_types(historical_columns, versions),
        decimal_columns=decimal_columns,
        force_precision_columns=force_precision_columns,
    )
    if compatibility == 'incompatible':
        raise RecoveryManifestError('Iceberg target changed outside the planned decimal evolution')
    statements.extend(
        f'ALTER ICEBERG TABLE {expected.name.quoted} ADD COLUMN {column.definition}'
        for column in additions
    )
    return tuple(statements)


def retained_column_types(historical_columns, versions):
    """Combine the persisted original archives and the planned new archives."""
    retained = dict(historical_columns or {})
    for version in versions.values():
        name = version['archived_name']
        if name in retained:
            raise RecoveryManifestError('Planned decimal archive collides with historical data')
        retained[name] = version['data_type']
    return retained


def _reject_boundary_version(name, boundary_column):
    if boundary_column and name.upper() == boundary_column.upper():
        raise TableCompatibilityError(
            f'Cannot version PartialSync boundary column {name}; run FullSync to recreate the target'
        )


def _resume_column_version(expected, columns, old, archived, desired, statements):
    existing_archive = columns.get(archived.name)
    current = columns.get(old.name)
    if existing_archive is None:
        if current != old:
            raise RecoveryManifestError('Decimal column changed before its planned rename')
        statements.append(
            f'ALTER ICEBERG TABLE {expected.name.quoted} RENAME COLUMN '
            f'{quote_identifier(old.name)} TO {quote_identifier(archived.name)}'
        )
        columns[archived.name] = archived
        del columns[old.name]
        current = None
    elif existing_archive != archived:
        raise RecoveryManifestError('Historical decimal column changed during recovery')
    if current is None:
        statements.append(f'ALTER ICEBERG TABLE {expected.name.quoted} ADD COLUMN {desired.definition}')
        columns[desired.name] = desired
    elif current != desired:
        raise RecoveryManifestError('Replacement decimal column changed during recovery')
