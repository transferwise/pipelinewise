"""Source-export restrictions are checked offline by validate and import_config."""

import json

import pytest

from pipelinewise.cli.config import Config
from pipelinewise.cli.errors import InvalidTransformationException
from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs


def _write_project(tmp_path, *, tap_type='tap-postgres', target_type='target-snowflake', iceberg=False, **table):
    tap = {
        'id': 'source', 'name': 'source', 'type': tap_type, 'target': 'target', 'db_conn': {},
        'schemas': [{'source_schema': 'public', 'target_schema': 'public', 'tables': [{
            'table_name': 'events', 'replication_method': 'FULL_TABLE', **table,
        }]}],
    }
    if iceberg:
        tap.update(target_table_format='iceberg', iceberg_version=3)
    target = {
        'id': 'target', 'name': 'target', 'type': target_type,
        'db_conn': {
            'account': 'account', 'dbname': 'database', 'user': 'user', 'private_key': 'private-key',
            'warehouse': 'warehouse', 's3_bucket': 'bucket', 's3_key_prefix': 'prefix/',
            'stage': 'schema.stage', 'file_format': 'file-format',
        },
    }
    (tmp_path / 'tap_test.yml').write_text(json.dumps(tap), encoding='utf-8')
    (tmp_path / 'target_test.yml').write_text(json.dumps(target), encoding='utf-8')


def _validate_project(tmp_path, command):
    runtime_dir = str(tmp_path / 'runtime')
    if command == 'validate':
        PipelineWise(CliArgs(dir=str(tmp_path)), runtime_dir, '/unused').validate()
    else:
        # import_config calls this before saving generated config or performing discovery.
        Config.from_yamls(runtime_dir, str(tmp_path))
    assert not (tmp_path / 'runtime').exists()


@pytest.mark.parametrize('command', ['validate', 'import_config'])
@pytest.mark.parametrize('tap_type', ['tap-postgres', 'tap-mysql'])
@pytest.mark.parametrize('iceberg', [False, True])
@pytest.mark.parametrize('condition,error', [
    ({'regex_match': r'\d+'}, 'Backslash regex'),
    ({'regex_match': 'a^b'}, 'anchors'),
    ({'regex_match': '[a&&b]'}, 'set operators'),
    ({'equals': r'C:\path'}, 'Backslash equality'),
    ({'equals': '\0'}, 'NUL'),
])
def test_invalid_source_conditions_fail_offline(tmp_path, command, tap_type, iceberg, condition, error):
    _write_project(tmp_path, tap_type=tap_type, iceberg=iceberg, transformations=[{
        'column': 'secret', 'type': 'HASH', 'when': [{'column': 'secret', **condition}],
    }])
    with pytest.raises(InvalidTransformationException, match=error):
        _validate_project(tmp_path, command)
    assert not (tmp_path / 'runtime').exists()


@pytest.mark.parametrize('command', ['validate', 'import_config'])
def test_duplicate_unconditional_rules_fail_offline(tmp_path, command):
    _write_project(tmp_path, transformations=[
        {'column': 'secret', 'type': 'HASH'}, {'column': 'SECRET', 'type': 'SET-NULL'},
    ])
    with pytest.raises(InvalidTransformationException, match='Duplicate unconditional'):
        _validate_project(tmp_path, command)


@pytest.mark.parametrize('command', ['validate', 'import_config'])
@pytest.mark.parametrize('tap_type', ['tap-postgres', 'tap-mysql'])
def test_transformed_incremental_key_fails_offline(tmp_path, command, tap_type):
    _write_project(
        tmp_path, tap_type=tap_type, replication_method='INCREMENTAL', replication_key='id',
        transformations=[{'column': 'ID', 'type': 'MASK-NUMBER'}],
    )
    with pytest.raises(InvalidTransformationException, match='INCREMENTAL replication key'):
        _validate_project(tmp_path, command)


@pytest.mark.parametrize('command', ['validate', 'import_config'])
def test_valid_configuration_leaves_metadata_dependent_checks_for_preflight(tmp_path, command):
    _write_project(tmp_path, transformations=[{
        'column': 'unknown_until_discovery', 'type': 'MASK-DATE',
        'when': [{'column': 'also_unknown', 'regex_match': '^[0-9]+$'}],
    }])
    _validate_project(tmp_path, command)


@pytest.mark.parametrize('command', ['validate', 'import_config'])
@pytest.mark.parametrize('tap_type,target_type', [
    ('tap-mongodb', 'target-snowflake'), ('tap-postgres', 'target-postgres'),
    ('tap-mysql', 'target-postgres'),
])
def test_other_routes_keep_their_existing_condition_syntax(tmp_path, command, tap_type, target_type):
    _write_project(tmp_path, tap_type=tap_type, target_type=target_type, transformations=[{
        'column': 'secret', 'type': 'HASH', 'when': [{'column': 'secret', 'regex_match': r'\d+'}],
    }])
    _validate_project(tmp_path, command)
