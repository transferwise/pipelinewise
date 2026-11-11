"""Discovery and runs round MariaDB/MySQL FLOAT equality literals to the value the tap now extracts."""

import copy
import json
import logging
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

from pipelinewise.cli import mysql_float_conditions
from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs


CONFIG_DIR = 'tests/units/cli/resources/sample_json_config'
VIRTUALENVS_DIR = './virtualenvs-dummy'


def property_metadata(name, schema=None, **metadata):
    if schema is None:
        schema = {'type': ['null', 'string']} if metadata.get('datatype') == 'varchar' else {'type': ['null', 'number']}
    return {'breadcrumb': ['properties', name], 'metadata': {'selected-by-default': True, **metadata}, 'schema': schema}


def stream(tap_stream_id, *properties):
    return {
        'tap_stream_id': tap_stream_id,
        'stream': tap_stream_id.split('-', 1)[1],
        'schema': {'type': 'object', 'properties': {entry['breadcrumb'][1]: entry['schema'] for entry in properties}},
        'metadata': [{'breadcrumb': [], 'metadata': {'database-name': 'db'}},
                     *({key: value for key, value in entry.items() if key != 'schema'} for entry in properties)],
    }


CATALOG = {'streams': [
    stream(
        'db-orders',
        property_metadata('weight', datatype='float', **{'sql-datatype': 'float unsigned'}),
        property_metadata('ratio', datatype='double', **{'sql-datatype': 'double'}),
        property_metadata('code', datatype='varchar', **{'sql-datatype': 'varchar(10)'}),
        property_metadata('legacy_weight', **{'sql-datatype': 'float(7,4)'}),
        property_metadata('legacy_ratio', **{'sql-datatype': 'double'}),
        property_metadata(
            'text_weight', schema={'type': ['null', 'string'], 'format': 'singer.decimal'}, datatype='float',
        ),
        property_metadata('unschematised_weight', schema={}, datatype='float'),
    ),
    stream('db-other', property_metadata('ratio', datatype='float', **{'sql-datatype': 'float'})),
]}


def rule(when, tap_stream_name='db-orders', field_id='code', **extra):
    return {'tap_stream_name': tap_stream_name, 'field_id': field_id, 'type': 'SET-NULL', 'when': when, **extra}


def rounded(conditions, tap_stream_name='db-orders'):
    config = {'transformations': [rule(copy.deepcopy(conditions), tap_stream_name)]}
    changed = mysql_float_conditions.round_float_equals(config, CATALOG)
    return changed, config['transformations'][0]['when']


@pytest.mark.parametrize('column,value,expected', [
    ('weight', 0.3, 0.30000001192092896),
    ('weight', 16777217, 16777216.0),
    ('weight', 1e-50, 0.0),
])
def test_float_equality_literals_round_to_the_nearest_float(column, value, expected):
    changed, when = rounded([{'column': column, 'equals': value}])
    assert changed
    assert when == [{'column': column, 'equals': expected}]


@pytest.mark.parametrize('condition', [
    {'column': 'ratio', 'equals': 0.3},
    {'column': 'legacy_ratio', 'equals': 0.3},
    # tap-mysql promotes only discovery's FLOAT datatype with a plain number schema, so neither is rounded.
    {'column': 'legacy_weight', 'equals': 0.1},
    {'column': 'text_weight', 'equals': 0.1},
    {'column': 'unschematised_weight', 'equals': 0.1},
    {'column': 'code', 'equals': 0.3},
    {'column': 'missing', 'equals': 0.3},
    {'column': 'weight', 'field_path': 'nested/value', 'equals': 0.3},
    {'column': 'weight', 'regex_match': '0[.]3'},
    {'column': 'weight', 'equals': '0.3'},
    {'column': 'weight', 'equals': True},
    {'column': 'weight', 'equals': None},
    {'column': 'weight', 'equals': 0.5},
    {'column': 'weight', 'equals': 16777216},
    {'column': 'weight', 'equals': 1e39},
    {'column': 'weight', 'equals': 10 ** 40},
    {'column': 'weight', 'equals': float('nan')},
])
def test_conditions_that_float_rounding_does_not_change_are_untouched(condition):
    changed, when = rounded([condition])
    assert not changed
    assert repr(when) == repr([condition])


def test_rounding_uses_the_condition_streams_column_types():
    changed, when = rounded([{'column': 'ratio', 'equals': 0.3}], tap_stream_name='db-other')
    assert changed
    assert when == [{'column': 'ratio', 'equals': 0.30000001192092896}]
    assert rounded([{'column': 'weight', 'equals': 0.3}], tap_stream_name='db-unknown') == (
        False, [{'column': 'weight', 'equals': 0.3}],
    )


def test_condition_columns_match_float_columns_case_insensitively():
    catalog = {'streams': [stream('db-orders', property_metadata('Weight', datatype='float'))]}
    config = {'transformations': [rule([{'column': 'WEIGHT', 'equals': 0.3}, {'column': 'weight', 'equals': 0.1}])]}

    assert mysql_float_conditions.float_columns_by_stream(catalog) == {'db-orders': {'weight'}}
    assert mysql_float_conditions.round_float_equals(config, catalog)
    assert config['transformations'][0]['when'] == [
        {'column': 'WEIGHT', 'equals': 0.30000001192092896}, {'column': 'weight', 'equals': 0.10000000149011612},
    ]


def test_rule_streams_prefer_the_exact_stream_then_match_case_insensitively():
    """transform-field applies a rule to the exact stream; FastSync-to-PostgreSQL to streams differing by case."""
    catalog = {'streams': [
        stream('db-Events', property_metadata('weight', datatype='float')),
        stream('db-events', property_metadata('weight', datatype='double')),
        stream('db-Mixed', property_metadata('weight', datatype='float')),
    ]}
    config = {'transformations': [
        rule([{'column': 'weight', 'equals': 0.3}], tap_stream_name=name)
        for name in ('db-events', 'db-mixed', 'DB-EVENTS', 'db-missing')
    ]}

    assert mysql_float_conditions.round_float_equals(config, catalog)
    assert [transformation['when'][0]['equals'] for transformation in config['transformations']] == [
        0.3, 0.30000001192092896, 0.30000001192092896, 0.3,
    ]


def test_rounding_is_idempotent_and_keeps_other_conditions():
    config = {'transformations': [
        rule([{'column': 'code', 'regex_match': 'A.*'}, {'column': 'weight', 'equals': 0.3}]),
        rule(None, field_id='weight'),
    ]}
    assert mysql_float_conditions.round_float_equals(config, CATALOG)
    first = copy.deepcopy(config)

    assert not mysql_float_conditions.round_float_equals(config, CATALOG)
    assert config == first
    assert config['transformations'][0]['when'] == [
        {'column': 'code', 'regex_match': 'A.*'}, {'column': 'weight', 'equals': 0.30000001192092896},
    ]
    assert config['transformations'][1]['when'] is None


@pytest.mark.parametrize('config', [
    None, {}, {'transformations': None}, {'transformations': [None, rule('bad')]},
    {'transformations': [rule([{'column': 'weight', 'equals': 0.3}], tap_stream_name=['db-orders'])]},
    {'transformations': [rule([{'column': ['weight'], 'equals': 0.3}, None])]},
])
def test_malformed_transformation_config_is_left_for_validation(config):
    assert not mysql_float_conditions.round_float_equals(copy.deepcopy(config), CATALOG)


def test_transformation_file_is_rewritten_only_when_a_literal_changes(tmp_path):
    path = tmp_path / 'transformation.json'
    exact = {'transformations': [rule([{'column': 'weight', 'equals': 0.5}])]}
    path.write_text(json.dumps(exact), encoding='utf-8')

    assert not mysql_float_conditions.round_transformation_file(str(path), CATALOG)
    assert path.read_text(encoding='utf-8') == json.dumps(exact)

    path.write_text(json.dumps({'transformations': [rule([{'column': 'weight', 'equals': 0.3}])]}), encoding='utf-8')
    assert mysql_float_conditions.round_transformation_file(str(path), CATALOG)
    saved = json.loads(path.read_text(encoding='utf-8'))
    assert saved['transformations'][0]['when'] == [{'column': 'weight', 'equals': 0.30000001192092896}]
    assert not mysql_float_conditions.round_transformation_file(str(path), CATALOG)
    assert not mysql_float_conditions.round_transformation_file(str(tmp_path / 'missing.json'), CATALOG)
    assert not mysql_float_conditions.round_transformation_file(None, CATALOG)


def _pipelinewise(tmp_path):
    instance = object.__new__(PipelineWise)
    instance.logger = logging.getLogger('test_mysql_float_conditions')
    instance.profiling_mode = False
    instance.venv_dir = str(tmp_path / 'venv')
    return instance


def _tap(tmp_path, tap_type):
    files = {name: str(tmp_path / f'{name}.json') for name in ('config', 'properties', 'selection', 'transformation')}
    (tmp_path / 'transformation.json').write_text(
        json.dumps({'transformations': [rule([{'column': 'weight', 'equals': 0.3}])]}), encoding='utf-8',
    )
    return {'id': 'tap_one', 'type': tap_type, 'files': files}


def _discover(tmp_path, tap_type, returncode=0, post_import_errors=()):
    tap = _tap(tmp_path, tap_type)
    discovered = (returncode, json.dumps(CATALOG), 'discovery output')
    with patch('pipelinewise.cli.pipelinewise.commands.run_command', return_value=discovered), \
            patch.object(PipelineWise, '_run_post_import_tap_checks', return_value=list(post_import_errors)):
        error = _pipelinewise(tmp_path)._discover_tap(tap, {'id': 'target_one', 'type': 'target-snowflake'})
    return error, json.loads((tmp_path / 'transformation.json').read_text(encoding='utf-8'))


def test_mysql_discovery_rounds_float_equality_literals(tmp_path):
    error, transformations = _discover(tmp_path, 'tap-mysql')
    assert error is None
    assert transformations['transformations'][0]['when'] == [{'column': 'weight', 'equals': 0.30000001192092896}]
    assert json.loads((tmp_path / 'properties.json').read_text(encoding='utf-8')) == CATALOG


@pytest.mark.parametrize('tap_type', ['tap-postgres', 'tap-yugabyte', 'tap-mongodb'])
def test_other_taps_keep_their_transformation_literals(tmp_path, tap_type):
    error, transformations = _discover(tmp_path, tap_type)
    assert error is None
    assert transformations['transformations'][0]['when'] == [{'column': 'weight', 'equals': 0.3}]


@pytest.mark.parametrize('returncode,post_import_errors', [(1, ()), (0, ('No primary key set',))])
def test_failed_mysql_discovery_keeps_transformation_literals(tmp_path, returncode, post_import_errors):
    error, transformations = _discover(tmp_path, 'tap-mysql', returncode, post_import_errors)
    assert error
    assert transformations['transformations'][0]['when'] == [{'column': 'weight', 'equals': 0.3}]


def test_transformation_save_failure_fails_mysql_discovery(tmp_path):
    with patch.object(mysql_float_conditions.utils, 'save_json', side_effect=[None, Exception('disk full')]):
        error, transformations = _discover(tmp_path, 'tap-mysql')
    assert error == 'Cannot update transformation file. disk full'
    assert transformations['transformations'][0]['when'] == [{'column': 'weight', 'equals': 0.3}]


def _command_pipelinewise(tmp_path, tap_type='tap-mysql'):
    """Build the sample tap with its generated files in tmp_path."""
    instance = PipelineWise(CliArgs(target='target_one', tap='tap_one'), CONFIG_DIR, VIRTUALENVS_DIR)
    instance.tap['type'] = tap_type
    files = instance.tap['files']
    files.update({name: str(tmp_path / f'{name}.json') for name in ('properties', 'transformation', 'state')})
    files['pidfile'] = str(tmp_path / 'tap.pid')
    Path(files['properties']).write_text(json.dumps(CATALOG), encoding='utf-8')
    Path(files['transformation']).write_text(
        json.dumps({'transformations': [rule([{'column': 'weight', 'equals': 0.3}])]}), encoding='utf-8',
    )
    return instance


def _conditions(instance):
    saved = json.loads(Path(instance.tap['files']['transformation']).read_text(encoding='utf-8'))
    return saved['transformations'][0]['when']


@contextmanager
def _patched_run_tap(instance, tmp_path):
    """Replace FastSync and Singer in run_tap with probes that record the conditions they would read."""
    filtered = tmp_path / 'filtered_properties.json'
    filtered.write_text('{"streams": []}', encoding='utf-8')
    seen = []
    with patch.object(
        instance, 'create_filtered_tap_properties',
        return_value=(str(filtered), ['db-orders'], str(filtered), ['db-other']),
    ), patch.object(
        instance, 'do_sync_tables', side_effect=lambda *_, **__: seen.append(('fastsync', _conditions(instance))),
    ), patch.object(
        instance, 'run_tap_singer', side_effect=lambda **_: seen.append(('singer', _conditions(instance))),
    ), patch.object(instance, 'send_alert') as send_alert, patch.object(
        instance, '_print_tap_run_summary',
    ) as summary:
        yield seen, send_alert, summary


def test_run_tap_rounds_float_literals_before_fastsync_and_singer_start(tmp_path):
    """A run after upgrading, or after a failed import, must not leave FLOAT-conditioned masks unapplied."""
    instance = _command_pipelinewise(tmp_path)

    with _patched_run_tap(instance, tmp_path) as (seen, send_alert, summary):
        instance.run_tap()

    rounded_when = [{'column': 'weight', 'equals': 0.30000001192092896}]
    assert seen == [('fastsync', rounded_when), ('singer', rounded_when)]
    send_alert.assert_not_called()
    assert summary.call_args.args[0] == PipelineWise.STATUS_SUCCESS


def test_fast_sync_rounds_float_literals_before_syncing(tmp_path):
    instance = _command_pipelinewise(tmp_path)
    seen = []

    with patch.object(instance, 'do_sync_tables', side_effect=lambda **_: seen.append(_conditions(instance))):
        instance.fast_sync()

    assert seen == [[{'column': 'weight', 'equals': 0.30000001192092896}]]


def test_partial_sync_table_rounds_float_literals_before_syncing(tmp_path):
    """PartialSync must hash the same rounded rules as run_tap and fast_sync into its Iceberg recovery identity."""
    instance = _command_pipelinewise(tmp_path)
    seen = []

    with patch.object(instance, 'sync_tables_partial_sync', side_effect=lambda: seen.append(_conditions(instance))):
        instance.partial_sync_table()

    assert seen == [[{'column': 'weight', 'equals': 0.30000001192092896}]]


@pytest.mark.parametrize('tap_type', ['tap-postgres', 'tap-yugabyte'])
def test_runs_of_other_taps_keep_their_transformation_literals(tmp_path, tap_type):
    instance = _command_pipelinewise(tmp_path, tap_type)
    saved = Path(instance.tap['files']['transformation']).read_text(encoding='utf-8')

    with _patched_run_tap(instance, tmp_path) as (seen, _, _):
        instance.run_tap()
    with patch.object(instance, '_should_reset_source_slot_for_fast_sync', return_value=False), \
            patch.object(instance, 'do_sync_tables'):
        instance.fast_sync()

    unchanged_when = [{'column': 'weight', 'equals': 0.3}]
    assert seen == [('fastsync', unchanged_when), ('singer', unchanged_when)]
    assert Path(instance.tap['files']['transformation']).read_text(encoding='utf-8') == saved


def test_run_tap_stops_before_syncing_when_literals_cannot_be_rounded(tmp_path):
    instance = _command_pipelinewise(tmp_path)
    error = Exception('read-only file system')
    save_json = mysql_float_conditions.utils.save_json

    transformation_dir = Path(instance.tap['files']['transformation']).parent

    def fail_transformation_save(data, path):
        # The rewrite saves to a temporary file beside transformation.json, then replaces it.
        if Path(path).parent == transformation_dir and Path(path).name.startswith('.transformation.'):
            raise error
        return save_json(data, path)

    with _patched_run_tap(instance, tmp_path) as (seen, send_alert, summary), patch.object(
        mysql_float_conditions.utils, 'save_json', side_effect=fail_transformation_save,
    ):
        with pytest.raises(Exception, match='read-only file system'):
            instance.run_tap()

    assert seen == []
    send_alert.assert_called_once_with(message='tap_one tap failed', exc=error)
    assert summary.call_args.args[0] == PipelineWise.STATUS_FAILED
    assert _conditions(instance) == [{'column': 'weight', 'equals': 0.3}]
    assert not list(transformation_dir.glob('.transformation.*'))
