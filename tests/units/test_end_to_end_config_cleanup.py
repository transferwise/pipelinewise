"""Unit coverage for generated E2E runtime and inventory cleanup."""

import json
from unittest import mock

import pytest

from tests.end_to_end.helpers import config_cleanup


def _write_root_config(config_dir, root_config):
    config_dir.mkdir(parents=True, exist_ok=True)
    root_config_path = config_dir / 'config.json'
    root_config_path.write_text(json.dumps(root_config), encoding='utf-8')
    return root_config_path


def test_target_cleanup_preserves_global_keys_siblings_and_cleanup_journal(tmp_path):
    config_dir = tmp_path / 'runtime'
    runtime_dir = config_dir / 'warehouse'
    runtime_dir.mkdir(parents=True)
    (runtime_dir / 'config.json').write_text('{}', encoding='utf-8')
    root_config = {
        'format_version': 7,
        'global_setting': {'enabled': True},
        'targets': [
            {
                'id': 'warehouse',
                'name': 'Warehouse',
                'taps': [{'id': 'orders'}, {'id': 'customers'}],
            },
            {
                'id': 'archive',
                'name': 'Archive',
                'taps': [{'id': 'events'}],
            },
        ],
    }
    root_config_path = _write_root_config(config_dir, root_config)
    journal_path = config_dir / '.deleted-config-cleanup.json'
    journal_contents = '{"pending": true}'
    journal_path.write_text(journal_contents, encoding='utf-8')

    config_cleanup.remove_runtime_config(config_dir, 'warehouse')

    assert not runtime_dir.exists()
    assert json.loads(root_config_path.read_text(encoding='utf-8')) == {
        'format_version': 7,
        'global_setting': {'enabled': True},
        'targets': [root_config['targets'][1]],
    }
    assert journal_path.read_text(encoding='utf-8') == journal_contents


def test_missing_tap_runtime_still_prunes_only_that_tap(tmp_path):
    config_dir = tmp_path / 'runtime'
    root_config = {
        'global_setting': 'preserve-me',
        'targets': [
            {
                'id': 'warehouse',
                'name': 'Warehouse',
                'status': 'ready',
                'taps': [
                    {'id': 'orders', 'name': 'Orders'},
                    {'id': 'customers', 'name': 'Customers'},
                ],
            },
            {'id': 'archive', 'taps': [{'id': 'events'}]},
        ],
    }
    root_config_path = _write_root_config(config_dir, root_config)

    config_cleanup.remove_runtime_config(config_dir, 'warehouse/orders')

    assert json.loads(root_config_path.read_text(encoding='utf-8')) == {
        'global_setting': 'preserve-me',
        'targets': [
            {
                'id': 'warehouse',
                'name': 'Warehouse',
                'status': 'ready',
                'taps': [{'id': 'customers', 'name': 'Customers'}],
            },
            root_config['targets'][1],
        ],
    }


def test_missing_root_config_is_idempotent(tmp_path):
    config_dir = tmp_path / 'runtime'
    runtime_dir = config_dir / 'warehouse'
    runtime_dir.mkdir(parents=True)

    config_cleanup.remove_runtime_config(config_dir, 'warehouse')
    config_cleanup.remove_runtime_config(config_dir, 'warehouse')

    assert not runtime_dir.exists()
    assert not (config_dir / 'config.json').exists()


def test_missing_root_owner_does_not_rewrite_inventory(tmp_path):
    config_dir = tmp_path / 'runtime'
    root_config_path = _write_root_config(
        config_dir,
        {'global_setting': True, 'targets': [{'id': 'archive', 'taps': []}]},
    )
    original_contents = root_config_path.read_bytes()

    config_cleanup.remove_runtime_config(config_dir, 'warehouse/orders')

    assert root_config_path.read_bytes() == original_contents


@pytest.mark.parametrize(
    'relative_path',
    [
        '',
        '/absolute',
        './warehouse',
        '../warehouse',
        'warehouse/.',
        'warehouse/..',
        'warehouse/../archive',
        'warehouse/orders/extra',
        'warehouse\\orders',
    ],
)
def test_cleanup_rejects_paths_outside_one_owner(relative_path, tmp_path):
    with mock.patch.object(config_cleanup.shutil, 'rmtree') as rmtree:
        with pytest.raises(ValueError, match='target or target/tap relative path'):
            config_cleanup.remove_runtime_config(tmp_path, relative_path)

    rmtree.assert_not_called()


@pytest.mark.parametrize(
    'root_contents',
    [
        '{not-json',
        '[]',
        '{}',
        '{"targets": {}}',
        '{"targets": [{"id": "warehouse"}]}',
        '{"targets": [{"id": "warehouse", "taps": {}}]}',
    ],
)
def test_malformed_root_config_surfaces_after_runtime_cleanup(
    root_contents,
    tmp_path,
):
    config_dir = tmp_path / 'runtime'
    runtime_dir = config_dir / 'warehouse'
    runtime_dir.mkdir(parents=True)
    (config_dir / 'config.json').write_text(root_contents, encoding='utf-8')

    with pytest.raises(ValueError):
        config_cleanup.remove_runtime_config(config_dir, 'warehouse')

    assert not runtime_dir.exists()


def test_runtime_removal_failure_surfaces_before_root_pruning(tmp_path):
    config_dir = tmp_path / 'runtime'
    root_config_path = _write_root_config(
        config_dir,
        {'targets': [{'id': 'warehouse', 'taps': []}]},
    )
    original_contents = root_config_path.read_bytes()

    with mock.patch.object(
        config_cleanup.shutil,
        'rmtree',
        side_effect=PermissionError('runtime cleanup failed'),
    ), pytest.raises(PermissionError, match='runtime cleanup failed'):
        config_cleanup.remove_runtime_config(config_dir, 'warehouse')

    assert root_config_path.read_bytes() == original_contents


def test_atomic_root_write_failure_surfaces_after_runtime_cleanup(tmp_path):
    config_dir = tmp_path / 'runtime'
    runtime_dir = config_dir / 'warehouse'
    runtime_dir.mkdir(parents=True)
    root_config = {'targets': [{'id': 'warehouse', 'taps': []}]}
    root_config_path = _write_root_config(config_dir, root_config)

    with mock.patch.object(
        config_cleanup.fastsync_utils.os,
        'replace',
        side_effect=OSError('atomic replace failed'),
    ), pytest.raises(OSError, match='atomic replace failed'):
        config_cleanup.remove_runtime_config(config_dir, 'warehouse')

    assert not runtime_dir.exists()
    assert json.loads(root_config_path.read_text(encoding='utf-8')) == root_config
