import os
import subprocess
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DETECTOR = REPOSITORY_ROOT / 'scripts' / 'ci_check_no_file_changes.sh'

FAKE_CURL = r'''#!/usr/bin/env bash
set -u

page=''
while (( $# > 0 )); do
  case "$1" in
    --data-urlencode)
      if [[ $2 == page=* ]]; then
        page=${2#page=}
      fi
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

printf '%s\n' "${page}" >> "${FAKE_CURL_LOG}"

write_full_docs_page() {
  printf '['
  for ((index = 1; index <= 100; index += 1)); do
    if (( index > 1 )); then
      printf ','
    fi
    printf '{"filename":"docs/page-%s.rst"}' "${index}"
  done
  printf ']'
}

case "${FAKE_CURL_SCENARIO}:${page}" in
  second_page_python:1)
    write_full_docs_page
    ;;
  second_page_python:2)
    printf '[{"filename":"pipelinewise/cli/pipelinewise.py"}]'
    ;;
  docs_only:1)
    printf '[{"filename":"docs/index.rst"}]'
    ;;
  config_only:1)
    printf '[{"filename":"dev-project/pipelinewise-config/config.yml"}]'
    ;;
  detector_only:1)
    printf '[{"filename":"scripts/ci_check_no_file_changes.sh"}]'
    ;;
  api_failure:1)
    printf 'API unavailable\n' >&2
    exit 22
    ;;
  invalid_response:1)
    printf '{"message":"rate limit exceeded"}'
    ;;
  *)
    printf 'Unexpected scenario or page: %s:%s\n' "${FAKE_CURL_SCENARIO}" "${page}" >&2
    exit 2
    ;;
esac
'''

FAKE_JQ = r'''#!/usr/bin/env python3
import json
import sys


try:
    payload = json.load(sys.stdin)
except (json.JSONDecodeError, OSError):
    sys.exit(1)

filenames_are_valid = isinstance(payload, list) and all(
    isinstance(item, dict) and isinstance(item.get('filename'), str)
    for item in payload
)
expression = sys.argv[-1]

if expression.startswith('type =='):
    sys.exit(0 if filenames_are_valid else 1)
if not filenames_are_valid:
    sys.exit(1)
if expression == 'length':
    print(len(payload))
elif expression == '.[].filename':
    for item in payload:
        print(item['filename'])
else:
    sys.exit(2)
'''


def run_detector(tmp_path, scenario, *checks):
    """Run the detector with deterministic GitHub API command doubles."""
    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    fake_curl = fake_bin / 'curl'
    fake_curl.write_text(FAKE_CURL, encoding='utf-8')
    fake_curl.chmod(0o755)
    fake_jq = fake_bin / 'jq'
    fake_jq.write_text(FAKE_JQ, encoding='utf-8')
    fake_jq.chmod(0o755)

    request_log = tmp_path / 'curl-pages.log'
    environment = os.environ.copy()
    environment.update(
        {
            'FAKE_CURL_LOG': str(request_log),
            'FAKE_CURL_SCENARIO': scenario,
            'GITHUB_REPO': 'transferwise/pipelinewise',
            'PATH': f'{fake_bin}:{environment["PATH"]}',
            'PR_NUMBER': '1321',
        }
    )

    result = subprocess.run(
        [str(DETECTOR), *checks],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )
    pages = request_log.read_text(encoding='utf-8').splitlines()
    return result, pages


def test_second_page_python_change_runs_ci(tmp_path):
    """A relevant file after page one must run CI."""
    result, pages = run_detector(tmp_path, 'second_page_python', 'python', 'config')

    assert result.returncode == 1
    assert pages == ['1', '2']
    assert 'Detected changes in following file: pipelinewise/cli/pipelinewise.py' in result.stdout


def test_docs_only_skips_python_config(tmp_path):
    """Documentation-only changes may skip Python and config checks."""
    result, pages = run_detector(tmp_path, 'docs_only', 'python', 'config')

    assert result.returncode == 0
    assert pages == ['1']
    assert 'No changes detected... Exiting with SUCCESS code' in result.stdout


def test_config_change_triggers_checks(tmp_path):
    """A dev-project change must run Python and config checks."""
    result, pages = run_detector(tmp_path, 'config_only', 'python', 'config')

    assert result.returncode == 1
    assert pages == ['1']
    assert 'Detected changes in following file: dev-project/pipelinewise-config/config.yml' in result.stdout


def test_detector_change_triggers_checks(tmp_path):
    """A change to the detector must run its own regression tests."""
    result, pages = run_detector(tmp_path, 'detector_only', 'python')

    assert result.returncode == 1
    assert pages == ['1']
    assert 'Detected changes in following file: scripts/ci_check_no_file_changes.sh' in result.stdout


def test_api_failure_triggers_checks(tmp_path):
    """An API failure must run CI rather than produce a false green."""
    result, pages = run_detector(tmp_path, 'api_failure', 'python')

    assert result.returncode == 1
    assert pages == ['1']
    assert 'Unable to determine changed files; Exiting with FAILURE code' in result.stdout
    assert 'Failed to fetch changed files from GitHub' in result.stderr


def test_invalid_response_triggers_checks(tmp_path):
    """A malformed API response must run CI rather than skip it."""
    result, pages = run_detector(tmp_path, 'invalid_response', 'python')

    assert result.returncode == 1
    assert pages == ['1']
    assert 'Unable to determine changed files; Exiting with FAILURE code' in result.stdout
    assert 'GitHub changed-files response is invalid' in result.stderr
