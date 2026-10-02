"""Shared generated-config cleanup for isolated E2E fixtures."""

import os
import shutil
from pathlib import Path

from pipelinewise.fastsync.commons import utils as fastsync_utils


def _owner_parts(relative_path):
    relative_path = os.fspath(relative_path)
    if not isinstance(relative_path, str):
        raise ValueError(
            'Generated-config cleanup requires a target or target/tap relative path'
        )
    parts = relative_path.split('/')
    if (
        Path(relative_path).is_absolute()
        or '\\' in relative_path
        or len(parts) not in (1, 2)
        or any(part in {'', '.', '..'} for part in parts)
    ):
        raise ValueError(
            'Generated-config cleanup requires a target or target/tap relative path'
        )
    return parts


def _root_targets(root_config, root_config_path):
    if not isinstance(root_config, dict):
        raise ValueError(f'Malformed generated root config at {root_config_path}')

    targets = root_config.get('targets')
    if not isinstance(targets, list):
        raise ValueError(f'Malformed generated root config at {root_config_path}')

    target_ids = set()
    for target in targets:
        if (
            not isinstance(target, dict)
            or not isinstance(target.get('id'), str)
            or not target['id']
            or target['id'] in target_ids
        ):
            raise ValueError(f'Malformed generated root config at {root_config_path}')
        target_ids.add(target['id'])

        taps = target.get('taps')
        if not isinstance(taps, list):
            raise ValueError(f'Malformed generated root config at {root_config_path}')
        tap_ids = set()
        for tap in taps:
            if (
                not isinstance(tap, dict)
                or not isinstance(tap.get('id'), str)
                or not tap['id']
                or tap['id'] in tap_ids
            ):
                raise ValueError(f'Malformed generated root config at {root_config_path}')
            tap_ids.add(tap['id'])

    return targets


def remove_runtime_config(config_dir, relative_path):
    """Remove one E2E runtime owner and its entry in the root inventory."""
    owner_parts = _owner_parts(relative_path)
    config_dir = Path(config_dir)

    try:
        shutil.rmtree(config_dir.joinpath(*owner_parts))
    except FileNotFoundError:
        pass

    root_config_path = config_dir / 'config.json'
    try:
        root_config = fastsync_utils.load_json(root_config_path)
    except FileNotFoundError:
        return
    targets = _root_targets(root_config, root_config_path)

    target_id = owner_parts[0]
    if len(owner_parts) == 1:
        remaining_targets = [
            target for target in targets if target['id'] != target_id
        ]
        if len(remaining_targets) == len(targets):
            return
        updated_root = dict(root_config)
        updated_root['targets'] = remaining_targets
    else:
        tap_id = owner_parts[1]
        updated_targets = list(targets)
        for index, target in enumerate(targets):
            if target['id'] != target_id:
                continue
            remaining_taps = [
                tap for tap in target['taps'] if tap['id'] != tap_id
            ]
            if len(remaining_taps) == len(target['taps']):
                return
            updated_target = dict(target)
            updated_target['taps'] = remaining_taps
            updated_targets[index] = updated_target
            break
        else:
            return

        updated_root = dict(root_config)
        updated_root['targets'] = updated_targets

    fastsync_utils.save_dict_to_json(root_config_path, updated_root)
