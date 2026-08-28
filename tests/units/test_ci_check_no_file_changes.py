import os
import subprocess
from pathlib import Path

import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DETECTOR = REPOSITORY_ROOT / 'scripts' / 'ci_check_no_file_changes.sh'
E2E_WORKFLOW = REPOSITORY_ROOT / '.github' / 'workflows' / 'e2e_tests.yml'
TW_RULES = REPOSITORY_ROOT / '.github' / 'tw-rules.yaml'

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
  e2e_workflow:1)
    printf '[{"filename":".github/workflows/e2e_tests.yml"}]'
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


def test_e2e_workflow_triggers_checks(tmp_path):
    """An E2E workflow-only change must run the E2E jobs it defines."""
    result, pages = run_detector(tmp_path, 'e2e_workflow', 'python', 'config', 'e2e')

    assert result.returncode == 1
    assert pages == ['1']
    assert 'Detected changes in following file: .github/workflows/e2e_tests.yml' in result.stdout


def test_snowflake_e2e_matrix_contract():
    """Snowflake shards cover every route exactly once with full parallelism."""
    workflow = yaml.safe_load(E2E_WORKFLOW.read_text(encoding='utf-8'))
    jobs = workflow['jobs']
    job = jobs['e2e_tests_snowflake']
    strategy = job['strategy']
    shards = strategy['matrix']['include']

    assert workflow['concurrency']['group'] == (
        'e2e_tests-${{ github.event.pull_request.number || github.ref_name }}'
    )
    assert 'needs' not in job
    assert job['name'] == '${{ matrix.check_name }}'
    assert strategy['fail-fast'] is False
    assert 'max-parallel' not in strategy
    assert len(shards) == 8

    expected_shards = {
        'conversion': (
            'e2e_tests_sf_conversion',
            (
                'tests/end_to_end/target_snowflake/test_native_to_iceberg_converter.py',
                'tests/end_to_end/data_diff/test_mysql_to_snowflake.py',
            ),
        ),
        'publication': (
            'e2e_tests_sf_publication',
            (
                'tests/end_to_end/target_snowflake/tap_postgres/test_snowflake_iceberg_publisher.py',
                'tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_replica_to_sf.py',
            ),
        ),
        'pg-partial': (
            'e2e_tests_sf_pg_partial',
            (
                'tests/end_to_end/target_snowflake/tap_postgres/test_partial_sync_pg_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_to_sf_with_custom_buffer_size.py',
                'tests/end_to_end/data_diff/test_postgres_to_snowflake.py',
            ),
        ),
        'mariadb-partial': (
            'e2e_tests_sf_mariadb_partial',
            (
                'tests/end_to_end/target_snowflake/tap_mariadb/test_partial_sync_mariadb_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_postgres/test_defined_partial_sync_pg_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_postgres/test_resync_pg_to_sf_with_split_large_files.py',
            ),
        ),
        'pg-iceberg': (
            'e2e_tests_sf_pg_iceberg',
            (
                'tests/end_to_end/target_snowflake/tap_postgres/test_iceberg_v3_postgres_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_postgres/test_replicate_pg_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_s3/test_replicate_s3_to_sf.py',
            ),
        ),
        'mariadb-iceberg': (
            'e2e_tests_sf_mariadb_iceberg',
            (
                'tests/end_to_end/target_snowflake/tap_mariadb/test_iceberg_v3_mariadb_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_to_sf_soft_delete.py',
                'tests/end_to_end/target_snowflake/tap_mongodb/test_replicate_mongodb_to_sf.py',
            ),
        ),
        'mysql-iceberg': (
            'e2e_tests_sf_mysql_iceberg',
            (
                'tests/end_to_end/target_snowflake/tap_mysql/test_iceberg_v3_mysql_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_postgres/test_resync_pg_to_sf_table_size_check.py',
                'tests/end_to_end/target_snowflake/tap_mariadb/test_resync_mariadb_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_postgres/test_replicate_pg_to_sf_with_archive_load_files.py',
            ),
        ),
        'mariadb-native': (
            'e2e_tests_sf_mariadb_native',
            (
                'tests/end_to_end/target_snowflake/tap_mariadb/test_resync_mariadb_to_sf_table_size_check.py',
                'tests/end_to_end/target_snowflake/tap_mariadb/test_replicate_mariadb_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_mariadb/test_defined_partial_sync_mariadb_to_sf.py',
                'tests/end_to_end/target_snowflake/tap_mariadb/test_resync_mariadb_to_sf_with_split_large_files.py',
            ),
        ),
    }
    actual_shards = {
        shard['shard']: (shard['check_name'], tuple(shard['test_paths'].split()))
        for shard in shards
    }
    assert actual_shards == expected_shards

    configured_paths = [
        test_path
        for _, test_paths in actual_shards.values()
        for test_path in test_paths
    ]
    expected_paths = {
        str(path.relative_to(REPOSITORY_ROOT))
        for path in (
            REPOSITORY_ROOT / 'tests' / 'end_to_end' / 'target_snowflake'
        ).rglob('test_*.py')
    }
    expected_paths.update(
        {
            'tests/end_to_end/data_diff/test_mysql_to_snowflake.py',
            'tests/end_to_end/data_diff/test_postgres_to_snowflake.py',
        }
    )
    assert len(configured_paths) == len(set(configured_paths))
    assert set(configured_paths) == expected_paths

    commands = '\n'.join(step.get('run', '') for step in job['steps'])
    assert job['env']['E2E_TEST_PATHS'] == '${{ matrix.test_paths }}'
    assert 'pipelinewise pytest $E2E_TEST_PATHS' in commands
    assert '-e PIPELINEWISE_E2E_NAMESPACE=$PIPELINEWISE_E2E_NAMESPACE' in commands
    assert '${{ github.run_id }}' in job['env']['PIPELINEWISE_E2E_NAMESPACE']
    assert '${{ github.run_attempt }}' in job['env']['PIPELINEWISE_E2E_NAMESPACE']
    assert '${{ matrix.shard }}' in job['env']['PIPELINEWISE_E2E_NAMESPACE']

    retired_jobs = {
        'e2e_tests_mariadb_to_sf',
        'e2e_tests_mysql_to_sf',
        'e2e_tests_pg_to_sf',
        'e2e_tests_mg_to_sf',
        'e2e_tests_s3_to_sf',
    }
    assert retired_jobs.isdisjoint(jobs)

    target_pg_job = jobs['e2e_tests_target_pg']
    target_pg_commands = '\n'.join(
        step.get('run', '') for step in target_pg_job['steps']
    )
    assert '${{ github.run_id }}' in target_pg_job['env']['PIPELINEWISE_E2E_NAMESPACE']
    assert '${{ github.run_attempt }}' in target_pg_job['env']['PIPELINEWISE_E2E_NAMESPACE']
    assert (
        '-e PIPELINEWISE_E2E_NAMESPACE=$PIPELINEWISE_E2E_NAMESPACE'
        in target_pg_commands
    )
    readiness_steps = [
        step
        for configured_job in jobs.values()
        for step in configured_job['steps']
        if step.get('name') == 'Wait for test containers to be ready'
    ]
    assert len(readiness_steps) == 2
    for readiness_step in readiness_steps:
        assert 'docker logs --tail 50 pipelinewise' in readiness_step['run']
        assert 'sleep 5' in readiness_step['run']
        assert 'sleep 30' not in readiness_step['run']
        assert (
            "container_status=$(docker inspect --format '{{.State.Status}}' pipelinewise)"
            in readiness_step['run']
        )
        assert 'PipelineWise container stopped with status' in readiness_step['run']
        assert 'exit 1' in readiness_step['run']


def test_required_e2e_status_contract():
    """Repository rules require every current Snowflake shard exactly once."""
    workflow = yaml.safe_load(E2E_WORKFLOW.read_text(encoding='utf-8'))
    rules = yaml.safe_load(TW_RULES.read_text(encoding='utf-8'))
    shards = workflow['jobs']['e2e_tests_snowflake']['strategy']['matrix']['include']
    expected_statuses = {shard['check_name'] for shard in shards}
    configured_checks = rules['actions']['branch-protection-settings']['branches'][0]['checks']
    configured_names = [check['name'] for check in configured_checks]
    required_statuses = {
        name for name in configured_names if name.startswith('e2e_tests_sf_')
    }

    assert len(configured_names) == len(set(configured_names))
    assert required_statuses == expected_statuses
    assert not {
        'e2e_tests_mariadb_to_sf',
        'e2e_tests_mg_to_sf',
        'e2e_tests_pg_to_sf',
        'e2e_tests_s3_to_sf',
    }.intersection(configured_names)


def test_snowflake_e2e_matrix_preflight_once():
    """Every Snowflake shard runs one complete preflight command."""
    workflow = yaml.safe_load(E2E_WORKFLOW.read_text(encoding='utf-8'))
    job = workflow['jobs']['e2e_tests_snowflake']
    preflight_steps = [
        step
        for step in job['steps']
        if step.get('name')
        == 'Validate required Snowflake end-to-end configuration'
    ]

    assert len(preflight_steps) == 1
    assert preflight_steps[0]['if'] == "steps.check.outcome == 'failure'"
    assert preflight_steps[0]['run'].count('./scripts/ci_require_env.sh') == 1


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
