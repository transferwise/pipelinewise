import os
import subprocess
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
REQUIRE_ENV = REPOSITORY_ROOT / 'scripts' / 'ci_require_env.sh'


def _run_require_env(*names, environment=None):
    return subprocess.run(
        [str(REQUIRE_ENV), *names],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )


def test_configured_env_hides_values():
    """Configured values must never be written to CI output."""
    environment = os.environ.copy()
    environment['TEST_REQUIRED_SECRET'] = 'do-not-print-this-value'

    result = _run_require_env('TEST_REQUIRED_SECRET', environment=environment)

    assert result.returncode == 0
    assert result.stdout == 'All required environment variables are configured\n'
    assert 'do-not-print-this-value' not in result.stdout + result.stderr


def test_missing_env_reports_names():
    """Failures identify missing variables without printing values."""
    environment = os.environ.copy()
    environment.pop('TEST_MISSING_SECRET', None)
    environment['TEST_EMPTY_SECRET'] = ''

    result = _run_require_env(
        'TEST_MISSING_SECRET',
        'TEST_EMPTY_SECRET',
        environment=environment,
    )

    assert result.returncode == 1
    assert result.stdout == ''
    assert result.stderr == (
        'Missing required environment variables: '
        'TEST_MISSING_SECRET TEST_EMPTY_SECRET\n'
    )


def test_no_env_names_is_invalid():
    """Calling the guard without a contract is an invocation error."""
    result = _run_require_env()

    assert result.returncode == 2
    assert result.stderr == 'No required environment variables were specified\n'
