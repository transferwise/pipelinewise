"""Exercise managed PostgreSQL final ACK persistence and EOF coordination."""

import json
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pipelinewise.cli import commands
from pipelinewise.cli.pipelinewise import PipelineWise
from pipelinewise.fastsync.commons import utils as fastsync_utils


def _state(lsn: int) -> dict:
    return {'bookmarks': {'public-items': {'lsn': lsn}}}


def _runner(tmp_path: Path) -> PipelineWise:
    runner = object.__new__(PipelineWise)
    runner.tap_run_log_file = str(tmp_path / 'run.log')
    runner.profiling_mode = False
    runner.profiling_dir = None
    runner.extra_log = False
    runner.logger = Mock()
    return runner


def _params(
    tmp_path: Path,
    tap_type: str = 'tap-postgres',
    target_type: str = 'target-snowflake',
    transformations: bool = True,
) -> tuple[commands.TapParams, commands.TargetParams, commands.TransformParams]:
    config = tmp_path / 'config.json'
    config.write_text('{}', encoding='utf-8')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{}', encoding='utf-8')
    state = tmp_path / 'state.json'
    state.write_text(json.dumps(_state(0)), encoding='utf-8')
    transform_config = tmp_path / 'transform.json'
    transform_config.write_text(json.dumps({'transformations': [{}] if transformations else []}), encoding='utf-8')
    return (
        commands.TapParams('source', tap_type, '/tap', sys.executable, str(config), str(catalog), str(state)),
        commands.TargetParams('destination', target_type, '/target', sys.executable, str(config)),
        commands.TransformParams('/transform', sys.executable, str(transform_config), 'source', 'destination'),
    )


@pytest.mark.parametrize('profiling', [False, True])
@pytest.mark.parametrize('tap_type,target_type,enabled', [
    ('tap-postgres', 'target-snowflake', True),
    ('tap-postgres', 'target-postgres', False),
    ('tap-mysql', 'target-snowflake', False),
    ('tap-yugabyte', 'target-snowflake', False),
])
def test_feedback_environment_applies_only_to_managed_postgres_tap(
    tmp_path, profiling, tap_type, target_type, enabled,
):
    tap, target, transform = _params(tmp_path, tap_type, target_type)
    command = commands.build_singer_command(
        tap, target, transform, stream_buffer_size=10,
        profiling_mode=profiling, profiling_dir=str(tmp_path),
    )
    stages = command.split(' | ')
    prefix = f'{commands.POSTGRES_FINAL_FEEDBACK_ENV}=1 '
    assert stages[0].startswith(prefix) is enabled
    assert all(commands.POSTGRES_FINAL_FEEDBACK_ENV not in stage for stage in stages[1:])
    assert commands.POSTGRES_FINAL_FEEDBACK_ENV not in commands.build_tap_command(tap)
    if enabled and profiling:
        assert stages[0].startswith(f'{prefix}{sys.executable} -m cProfile ')


@pytest.mark.parametrize('state_path', [None, 'absent.json'])
def test_feedback_wait_is_disabled_when_tap_has_no_readable_state_path(tmp_path, state_path):
    tap, target, transform = _params(tmp_path)
    tap.state = str(tmp_path / state_path) if state_path is not None else None
    command = commands.build_singer_command(tap, target, transform)
    assert commands.POSTGRES_FINAL_FEEDBACK_ENV not in command
    assert '--state ' not in command


@pytest.mark.parametrize('tap_type,target_type,immediate', [
    ('tap-postgres', 'target-snowflake', True),
    ('tap-postgres', 'target-postgres', False),
    ('tap-mysql', 'target-snowflake', False),
    ('tap-yugabyte', 'target-snowflake', False),
])
def test_final_ack_within_two_seconds_is_saved_before_managed_pipeline_exits(
    tmp_path, tap_type, target_type, immediate,
):
    tap, target, transform = _params(tmp_path, tap_type, target_type)
    state_path = Path(tap.state)

    def running_command(_command, _log_file, line_callback):
        line_callback(json.dumps(_state(1)))
        assert json.loads(state_path.read_text()) == _state(1)
        line_callback(json.dumps(_state(2)))
        assert json.loads(state_path.read_text()) == _state(2 if immediate else 1)

    with patch.object(commands, 'run_command', side_effect=running_command), \
            patch('pipelinewise.cli.pipelinewise.time', return_value=0):
        _runner(tmp_path).run_tap_singer(tap, target, transform)

    assert json.loads(state_path.read_text()) == _state(2)


def test_failed_final_state_publication_propagates_and_preserves_prior_ack(tmp_path):
    tap, target, transform = _params(tmp_path)
    state_path = Path(tap.state)

    def running_command(_command, _log_file, line_callback):
        line_callback(json.dumps(_state(1)))
        with patch.object(fastsync_utils.os, 'replace', side_effect=OSError('state publication failed')):
            line_callback(json.dumps(_state(2)))

    with patch.object(commands, 'run_command', side_effect=running_command), \
            patch('pipelinewise.cli.pipelinewise.time', return_value=0), \
            pytest.raises(OSError, match='state publication failed'):
        _runner(tmp_path).run_tap_singer(tap, target, transform)

    assert json.loads(state_path.read_text()) == _state(1)
    assert list(tmp_path.glob('.state.json.*.tmp')) == []


_TAP_SCRIPT = '''import argparse
import json
import os
from pathlib import Path
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('--config')
parser.add_argument('--state')
args, _ = parser.parse_known_args()
config = json.loads(Path(args.config).read_text())
assert os.environ.get('PIPELINEWISE_POSTGRES_FINAL_FEEDBACK') == '1'
Path(config['pid_path']).write_text(str(os.getpid()))
print(json.dumps({'type': 'STATE', 'value': {'bookmarks': {'public-items': {'lsn': 2}}}}), flush=True)
# Release the actual pipe, retaining a writable stdout for interpreter shutdown.
with open(os.devnull, 'w') as sink:
    os.dup2(sink.fileno(), sys.stdout.fileno())
deadline = time.monotonic() + config['timeout']
while time.monotonic() < deadline:
    state = json.loads(Path(args.state).read_text())
    if state['bookmarks']['public-items']['lsn'] == 2:
        Path(config['observed_path']).write_text('final ACK observed before tap exit')
        sys.exit(0)
    time.sleep(0.01)
sys.exit(42)
'''

_TARGET_SCRIPT = '''import argparse
import json
import os
from pathlib import Path
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--config')
args = parser.parse_args()
config = json.loads(Path(args.config).read_text())
assert 'PIPELINEWISE_POSTGRES_FINAL_FEEDBACK' not in os.environ
messages = [json.loads(line) for line in sys.stdin]
# EOF must reach the target while the source session is still alive.
os.kill(int(Path(config['pid_path']).read_text()), 0)
if config['target_fails']:
    sys.exit(9)
print(json.dumps({'bookmarks': {'public-items': {'lsn': 1}}}), flush=True)
print(json.dumps(messages[-1]['value']), flush=True)
'''

_TRANSFORM_SCRIPT = '''import os
import sys

assert 'PIPELINEWISE_POSTGRES_FINAL_FEEDBACK' not in os.environ
for line in sys.stdin:
    sys.stdout.write(line)
sys.stdout.flush()
'''


@pytest.mark.parametrize('transformations', [False, True])
@pytest.mark.parametrize('target_fails', [False, True])
def test_real_pipeline_flushes_at_eof_while_tap_waits_for_durable_final_ack(
    tmp_path, monkeypatch, transformations, target_fails,
):
    monkeypatch.delenv(commands.POSTGRES_FINAL_FEEDBACK_ENV, raising=False)
    tap, target, transform = _params(tmp_path, transformations=transformations)
    config = {
        'pid_path': str(tmp_path / 'tap.pid'),
        'observed_path': str(tmp_path / 'observed.txt'),
        'timeout': 2 if target_fails else 5,
        'target_fails': target_fails,
    }
    Path(tap.config).write_text(json.dumps(config), encoding='utf-8')
    for name, body in [('tap', _TAP_SCRIPT), ('target', _TARGET_SCRIPT), ('transform', _TRANSFORM_SCRIPT)]:
        script = tmp_path / f'{name}.py'
        script.write_text(body, encoding='utf-8')
        component = {'tap': tap, 'target': target, 'transform': transform}[name]
        component.bin = f'{sys.executable} {script}'

    with patch('pipelinewise.cli.pipelinewise.time', return_value=0):
        if target_fails:
            with pytest.raises(commands.RunCommandException, match='Return code: 9'):
                _runner(tmp_path).run_tap_singer(tap, target, transform)
        else:
            _runner(tmp_path).run_tap_singer(tap, target, transform)

    assert json.loads(Path(tap.state).read_text()) == _state(0 if target_fails else 2)
    assert Path(config['observed_path']).exists() is not target_fails
    assert (tmp_path / f'run.log.{"failed" if target_fails else "success"}').exists()
