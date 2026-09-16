import ast
import fnmatch
import os
import re
import tomllib
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
NON_RUFF_TOOL_PATTERN = re.compile(
    r'\b(?:bandit|black|flake8|isort|mypy|pylint|pyright|unify|yapf)\b',
    re.IGNORECASE,
)
NON_RUFF_DIRECTIVE_PATTERN = re.compile(
    r'#.*(?:\b(?:flake8|isort|mypy|pylint|pyright|yapf)\s*:)',
    re.IGNORECASE,
)
NON_RUFF_CONFIG_NAMES = (
    '.bandit',
    '.flake8',
    '.isort.cfg',
    '.mypy.ini',
    '.pylintrc',
    '.style.yapf',
    '.yapfignore',
    'mypy.ini',
    'pylintrc',
    'pyrightconfig.json',
)


def _active_tooling_files():
    files = [
        REPOSITORY_ROOT / '.pre-commit-config.yaml',
        REPOSITORY_ROOT / '.dockerignore',
        REPOSITORY_ROOT / 'AGENTS.md',
        REPOSITORY_ROOT / 'CONTRIBUTING.md',
        REPOSITORY_ROOT / 'Makefile',
        REPOSITORY_ROOT / 'docs/Makefile',
        REPOSITORY_ROOT / 'docs/project/contribution.rst',
        REPOSITORY_ROOT / 'pyproject.toml',
        REPOSITORY_ROOT / 'setup.py',
        REPOSITORY_ROOT / 'singer-connectors/AGENTS.md',
    ]
    files.extend(sorted((REPOSITORY_ROOT / '.github/workflows').glob('*.y*ml')))
    files.extend(sorted(REPOSITORY_ROOT.glob('*requirements*.txt')))
    files.extend(sorted((REPOSITORY_ROOT / 'scripts').rglob('*.sh')))

    for connector_dir in sorted((REPOSITORY_ROOT / 'singer-connectors').iterdir()):
        if not connector_dir.is_dir():
            continue
        files.extend(
            path
            for name in ('Makefile', 'README.md', 'pyproject.toml', 'setup.cfg', 'setup.py', 'tox.ini')
            if (path := connector_dir / name).is_file()
        )
        files.extend(sorted(connector_dir.glob('*requirements*.txt')))

    return files


def test_active_python_tooling_uses_only_ruff():
    matches = []
    for path in _active_tooling_files():
        for line_number, line in enumerate(path.read_text().splitlines(), start=1):
            if line.lstrip().startswith('#'):
                continue
            if path.name == 'ci_check_no_file_changes.sh' and 'REGEX+=' in line:
                continue
            if match := NON_RUFF_TOOL_PATTERN.search(line):
                matches.append(f'{path.relative_to(REPOSITORY_ROOT)}:{line_number}: {match.group(0)}')

    assert not matches, 'Non-Ruff Python tooling remains active:\n' + '\n'.join(matches)


def test_non_ruff_configuration_files_are_removed():
    config_roots = [REPOSITORY_ROOT]
    config_roots.extend(
        path for path in (REPOSITORY_ROOT / 'singer-connectors').iterdir() if path.is_dir()
    )
    legacy_configs = [root / name for root in config_roots for name in NON_RUFF_CONFIG_NAMES if (root / name).exists()]

    assert not legacy_configs, 'Legacy linter configuration remains:\n' + '\n'.join(
        str(path.relative_to(REPOSITORY_ROOT)) for path in legacy_configs
    )


def test_python_sources_do_not_contain_other_linter_directives():
    matches = []
    for root in ('pipelinewise', 'tests', 'singer-connectors'):
        for directory, directories, filenames in os.walk(REPOSITORY_ROOT / root):
            directories[:] = [name for name in directories if name not in {'.venv', '__pycache__', 'venv'}]
            for filename in filenames:
                if not filename.endswith('.py'):
                    continue
                path = Path(directory) / filename
                for line_number, line in enumerate(path.read_text().splitlines(), start=1):
                    if match := NON_RUFF_DIRECTIVE_PATTERN.search(line):
                        matches.append(f'{path.relative_to(REPOSITORY_ROOT)}:{line_number}: {match.group(0)}')

    assert not matches, 'Non-Ruff directives remain in Python sources:\n' + '\n'.join(matches)


def test_root_ci_dependencies_and_policy_use_ruff():
    workflow = (REPOSITORY_ROOT / '.github/workflows/lint_unit_tests.yml').read_text()
    assert 'ruff check .' in {
        line.strip() for line in workflow.splitlines()
    }

    pre_commit = (REPOSITORY_ROOT / '.pre-commit-config.yaml').read_text()
    assert 'id: ruff-check' in pre_commit
    assert '--exclude' not in pre_commit
    assert not re.search(r'^\s+(?:exclude|files):', pre_commit, re.MULTILINE)

    ruff_config = tomllib.loads((REPOSITORY_ROOT / 'pyproject.toml').read_text())['tool']['ruff']
    assert ruff_config['line-length'] == 120
    assert ruff_config['force-exclude'] is True
    exclusions = set(ruff_config['extend-exclude'])

    connector_workflow = (REPOSITORY_ROOT / '.github/workflows/connectors.yml').read_text()
    ci_tested_connectors = set(re.findall(r'^\s+- connector: ([\w-]+)$', connector_workflow, re.MULTILINE))
    assert ci_tested_connectors == {'tap-mysql', 'tap-postgres', 'target-snowflake'}

    connector_test_dirs = {
        str(path.relative_to(REPOSITORY_ROOT))
        for connector_dir in (REPOSITORY_ROOT / 'singer-connectors').iterdir()
        if connector_dir.is_dir()
        for name in ('spikes', 'test', 'tests')
        if (path := connector_dir / name).is_dir()
    }
    for test_dir in connector_test_dirs:
        is_excluded = any(fnmatch.fnmatchcase(test_dir, pattern) for pattern in exclusions)
        connector = Path(test_dir).parts[1]
        if connector in ci_tested_connectors and test_dir.endswith('/tests'):
            unit_dir = f'{test_dir}/unit'
            integration_dir = f'{test_dir}/integration'
            assert not any(fnmatch.fnmatchcase(unit_dir, pattern) for pattern in exclusions)
            if (REPOSITORY_ROOT / integration_dir).is_dir():
                assert any(fnmatch.fnmatchcase(integration_dir, pattern) for pattern in exclusions)
        else:
            assert is_excluded

    for connector in ci_tested_connectors:
        makefile = (REPOSITORY_ROOT / f'singer-connectors/{connector}/Makefile').read_text()
        assert f'singer-connectors/{connector}/tests/unit/' in makefile

    assert set(ruff_config['lint']['select']) == {'C90', 'E', 'F', 'PLE', 'Q002', 'W'}
    assert ruff_config['lint']['mccabe']['max-complexity'] == 15
    connector_source_rules = {'C901', 'E501', 'E731', 'Q002'}
    for pattern, ignored_rules in ruff_config['lint']['per-file-ignores'].items():
        if pattern.startswith('singer-connectors/'):
            assert connector_source_rules.isdisjoint(ignored_rules)

    setup_tree = ast.parse((REPOSITORY_ROOT / 'setup.py').read_text())
    dependencies = {
        node.value.split('=', maxsplit=1)[0]
        for node in ast.walk(setup_tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert 'ruff' in dependencies

    connector_setups = sorted((REPOSITORY_ROOT / 'singer-connectors').glob('*/setup.py'))
    assert connector_setups
    assert all('ruff==0.16.1' in path.read_text() for path in connector_setups)
