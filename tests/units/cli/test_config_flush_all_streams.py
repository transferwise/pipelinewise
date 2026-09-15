"""Saved Singer configuration defaults and explicit flush overrides."""

import pytest

from pipelinewise.cli import utils
from pipelinewise.cli.config import Config


@pytest.mark.parametrize('target_type', ['target-postgres', 'target-snowflake'])
@pytest.mark.parametrize(
    ('settings', 'expected'),
    [({}, True), ({'flush_all_streams': True}, True), ({'flush_all_streams': False}, False)],
)
def test_save_config_flush_all_streams(tmp_path, target_type, settings, expected):
    """Generated Singer config enables flushing all streams unless explicitly disabled."""
    config = Config(str(tmp_path))
    tap = {
        'id': 'test_tap',
        'name': 'Test tap',
        'type': 'tap-postgres',
        'db_conn': {},
        'schemas': [],
        **settings,
    }
    target = {
        'id': 'test_target',
        'name': 'Test target',
        'type': target_type,
        'db_conn': {},
        'taps': [tap],
    }
    config.targets = {target['id']: target}

    config.save()

    inheritable = utils.load_json(
        str(tmp_path / target['id'] / tap['id'] / 'inheritable_config.json')
    )
    assert inheritable['flush_all_streams'] is expected
