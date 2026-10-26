import json
import os
import shutil
import stat
from io import StringIO
from pathlib import Path
from unittest import mock

import pytest

from pipelinewise import cli
from pipelinewise.cli import utils
from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs

SAMPLE_CONFIG_DIR = Path(__file__).parent / 'resources' / 'sample_json_config'
VIRTUALENVS_DIR = './virtualenvs-dummy'
TRICKY_SECRET = 'p@ss"w\\rd\' <>&: #{{ not_jinja }}'


@pytest.fixture(name='config_dir')
def fixture_config_dir(tmp_path, monkeypatch):
    """A copy of the sample config whose tap, target and main configs keep secrets as env_var lookups."""
    config_dir = tmp_path / 'config'
    shutil.copytree(SAMPLE_CONFIG_DIR, config_dir)
    _with_lookups(config_dir, 'target_one/tap_one/config.json', password='PPW_TAP_PASSWORD')
    _with_lookups(config_dir, 'target_one/config.json', password='PPW_TARGET_PASSWORD')
    main_config = json.loads((config_dir / 'config.json').read_text(encoding='utf-8'))
    main_config['alert_handlers'] = {'slack': {'token': '__lookup__'}}
    (config_dir / 'config.json').write_text(
        json.dumps(main_config).replace('"__lookup__"', "{{ env_var['PPW_SLACK_TOKEN'] | tojson }}"),
        encoding='utf-8',
    )
    monkeypatch.setenv('PPW_TAP_PASSWORD', TRICKY_SECRET)
    monkeypatch.setenv('PPW_TARGET_PASSWORD', 'target-secret')
    monkeypatch.setenv('PPW_SLACK_TOKEN', 'slack-token')
    return config_dir


def _with_lookups(config_dir: Path, relative_path: str, **env_names: str) -> None:
    """Write the sample connector config into config_dir with the given keys as env_var lookups."""
    config = json.loads((SAMPLE_CONFIG_DIR / relative_path).read_text(encoding='utf-8'))
    text = json.dumps({**config, **{key: f'__lookup_{key}__' for key in env_names}}, indent=4)
    for key, env_name in env_names.items():
        text = text.replace(f'"__lookup_{key}__"', "{{ env_var['%s'] | tojson }}" % env_name)
    (config_dir / relative_path).write_text(text, encoding='utf-8')


def _pipelinewise(config_dir: Path) -> PipelineWise:
    with mock.patch('pipelinewise.cli.pipelinewise.signal.signal'):
        return PipelineWise(CliArgs(target='target_one', tap='tap_one'), str(config_dir), VIRTUALENVS_DIR)


class TestRenderEnvVarLookups:
    def test_renders_lookups_from_the_environment(self, monkeypatch):
        monkeypatch.setenv('PPW_SECRET', TRICKY_SECRET)
        text = '{"password": {{ env_var[\'PPW_SECRET\'] | tojson }}}'
        assert json.loads(utils.render_env_var_lookups(text)) == {'password': TRICKY_SECRET}

    def test_text_without_lookups_is_unchanged(self):
        text = '{"query_tag": "{\\"database\\": \\"{{database}}\\"}"}'
        assert utils.render_env_var_lookups(text) is text

    def test_a_missing_variable_is_an_error(self, monkeypatch):
        monkeypatch.delenv('PPW_MISSING', raising=False)
        with pytest.raises(Exception):
            utils.render_env_var_lookups("{{ env_var['PPW_MISSING'] }}")

    def test_load_json_with_env_vars(self, tmp_path, monkeypatch):
        monkeypatch.setenv('PPW_SECRET', 'value')
        path = tmp_path / 'config.json'
        path.write_text('{"password": {{ env_var[\'PPW_SECRET\'] | tojson }}, "port": 5432}', encoding='utf-8')
        assert utils.load_json_with_env_vars(str(path)) == {'password': 'value', 'port': 5432}
        assert utils.load_json_with_env_vars(str(tmp_path / 'missing.json')) is None


class TestPipelineWiseConnectorConfigs:
    def test_tap_and_target_configs_point_at_private_rendered_copies(self, config_dir):
        pipelinewise = _pipelinewise(config_dir)
        tap_config = Path(pipelinewise.tap['files']['config'])
        target_config = Path(pipelinewise.target['files']['config'])

        assert tap_config.parent == Path(pipelinewise.get_temp_dir())
        assert json.loads(tap_config.read_text(encoding='utf-8'))['password'] == TRICKY_SECRET
        assert json.loads(target_config.read_text(encoding='utf-8'))['password'] == 'target-secret'
        assert pipelinewise.tap['target']['files']['config'] == str(target_config)
        for path in (tap_config, target_config):
            assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
        original = config_dir / 'target_one' / 'tap_one' / 'config.json'
        assert "env_var['PPW_TAP_PASSWORD']" in original.read_text(encoding='utf-8')

        pipelinewise._remove_rendered_connector_configs()
        assert not tap_config.exists() and not target_config.exists()

    def test_main_config_lookups_are_rendered(self, config_dir):
        assert _pipelinewise(config_dir).config['alert_handlers']['slack']['token'] == 'slack-token'

    def test_configs_without_lookups_are_used_as_they_are(self):
        pipelinewise = _pipelinewise(SAMPLE_CONFIG_DIR)
        assert pipelinewise.tap['files']['config'] == f'{SAMPLE_CONFIG_DIR}/target_one/tap_one/config.json'
        assert pipelinewise.target['files']['config'] == f'{SAMPLE_CONFIG_DIR}/target_one/config.json'

    def test_a_missing_variable_fails_naming_the_config(self, config_dir, monkeypatch):
        monkeypatch.delenv('PPW_TARGET_PASSWORD')
        with pytest.raises(Exception, match='Cannot render env_var lookups in .*target_one/config.json'):
            _pipelinewise(config_dir)

    def test_tap_info_reads_the_rendered_tap_config(self, config_dir, monkeypatch):
        monkeypatch.setenv('PPW_TAP_HOST', 'db.internal')
        _with_lookups(config_dir, 'target_one/tap_one/config.json', password='PPW_TAP_PASSWORD', host='PPW_TAP_HOST')
        with (
            mock.patch('pipelinewise.cli.pipelinewise.get_source_table_details', return_value={}) as details,
            mock.patch('sys.argv', ['pipelinewise', 'tap_info', '--tap', 'tap_one']),
            mock.patch('sys.stdout', new_callable=StringIO) as stdout,
            mock.patch.object(cli, '__init_logger'),
            mock.patch.object(cli, 'CONFIG_DIR', str(config_dir)),
            mock.patch('pipelinewise.cli.pipelinewise.signal.signal'),
        ):
            cli.main()

        assert details.call_args.args[1]['password'] == TRICKY_SECRET
        assert json.loads(stdout.getvalue())['tap_information']['source_database'] == 'db.internal'
