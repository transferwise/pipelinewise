#!/usr/bin/env python3
"""Check documentation examples and implementation-backed reference coverage."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_ROOT = REPO_ROOT / 'docs'


class DocumentationLoader(yaml.SafeLoader):
    """YAML loader that accepts Ansible Vault scalar examples."""


DocumentationLoader.add_constructor(
    '!vault', lambda loader, node: loader.construct_scalar(node)
)


def yaml_blocks(path: Path):
    """Yield line number and text for YAML/YML code blocks in one RST file."""
    lines = path.read_text(encoding='utf-8').splitlines()
    index = 0
    while index < len(lines):
        match = re.match(r'^(\s*)\.\. code-block:: ya?ml\s*$', lines[index])
        if not match:
            index += 1
            continue

        directive_indent = len(match.group(1))
        index += 1
        while index < len(lines):
            line = lines[index]
            if not line.strip() or line.lstrip().startswith(':'):
                index += 1
                continue
            break
        start = index
        block = []
        while index < len(lines):
            line = lines[index]
            if line.strip() and len(line) - len(line.lstrip()) <= directive_indent:
                break
            block.append(line)
            index += 1

        non_empty_indents = [
            len(line) - len(line.lstrip()) for line in block if line.strip()
        ]
        if non_empty_indents:
            block_indent = min(non_empty_indents)
            yield start + 1, '\n'.join(
                line[block_indent:] if line.strip() else '' for line in block
            ).rstrip()


def check_yaml_examples() -> list[str]:
    """Return failures for YAML examples that do not parse."""
    failures = []
    for path in sorted(DOCS_ROOT.rglob('*.rst')):
        for line_number, block in yaml_blocks(path):
            try:
                yaml.load(block, Loader=DocumentationLoader)
            except yaml.YAMLError as exc:
                relative = path.relative_to(REPO_ROOT)
                failures.append(f'{relative}:{line_number}: invalid YAML: {exc}')
    return failures


def check_explicit_code_blocks() -> list[str]:
    """Reject untyped RST literal blocks that bypass language checks."""
    failures = []
    for path in sorted(DOCS_ROOT.rglob('*.rst')):
        for line_number, line in enumerate(
            path.read_text(encoding='utf-8').splitlines(), start=1
        ):
            if re.match(r'^(?!\s*\.\.).*::$', line):
                relative = path.relative_to(REPO_ROOT)
                failures.append(
                    f'{relative}:{line_number}: use an explicit code-block language'
                )
    return failures


def assigned_string_list(path: Path, name: str) -> list[str]:
    """Read a literal string-list assignment without importing the module."""
    tree = ast.parse(path.read_text(encoding='utf-8'))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise ValueError(f'{name} not found in {path}')


def cli_options(path: Path) -> set[str]:
    """Read long options passed to argparse without importing the CLI."""
    tree = ast.parse(path.read_text(encoding='utf-8'))
    options = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != 'add_argument':
            continue
        options.update(
            argument.value
            for argument in node.args
            if isinstance(argument, ast.Constant)
            and isinstance(argument.value, str)
            and argument.value.startswith('--')
        )
    return options


def check_cli_reference() -> list[str]:
    """Ensure canonical commands have sections and documented options exist."""
    cli_path = REPO_ROOT / 'pipelinewise/cli/__init__.py'
    commands = assigned_string_list(cli_path, 'COMMANDS')
    canonical_commands = set(commands) - {'import', 'sync_tables'}
    cli_text = (DOCS_ROOT / 'user_guide/cli.rst').read_text(encoding='utf-8')
    failures = [
        f'docs/user_guide/cli.rst: missing canonical command {command}'
        for command in sorted(canonical_commands)
        if not re.search(rf'^``{re.escape(command)}``\s*$', cli_text, re.MULTILINE)
    ]
    documented_options = set(
        re.findall(r'(?<![A-Za-z0-9_-])--[A-Za-z][A-Za-z0-9_-]*', cli_text)
    )
    failures.extend(
        f'docs/user_guide/cli.rst: unknown CLI option {option}'
        for option in sorted(documented_options - cli_options(cli_path))
    )
    return failures


def contains_key(value, keys: set[str]) -> bool:
    """Return whether a nested structure contains one of the supplied keys."""
    if isinstance(value, dict):
        return bool(keys.intersection(value)) or any(
            contains_key(item, keys) for item in value.values()
        )
    if isinstance(value, list):
        return any(contains_key(item, keys) for item in value)
    return False


def check_transformation_examples() -> list[str]:
    """Reject copyable nested-field examples without full route context."""
    path = DOCS_ROOT / 'user_guide/transformations.rst'
    failures = []
    for line_number, block in yaml_blocks(path):
        try:
            value = yaml.load(block, Loader=DocumentationLoader)
        except yaml.YAMLError:
            continue
        if contains_key(value, {'field_path', 'field_paths'}):
            failures.append(
                f'docs/user_guide/transformations.rst:{line_number}: nested-field '
                'YAML is unsafe on FastSync-eligible routes; document it with '
                'explicit full route context'
            )
    return failures


def packaged_connectors() -> set[str]:
    """Read connector names from the Makefile ALL_CONNECTORS definition."""
    makefile = (REPO_ROOT / 'Makefile').read_text(encoding='utf-8')
    match = re.search(r'define ALL_CONNECTORS\n(.*?)\nendef', makefile, re.DOTALL)
    if not match:
        raise ValueError('ALL_CONNECTORS not found in Makefile')
    return set(re.findall(r'(?:tap|target)-[a-z0-9-]+|transform-field', match.group(1)))


def check_connector_inventories() -> list[str]:
    """Ensure packaged connectors appear in inventories and navigation."""
    taps_text = (DOCS_ROOT / 'connectors/taps.rst').read_text(encoding='utf-8')
    targets_text = (DOCS_ROOT / 'connectors/targets.rst').read_text(encoding='utf-8')
    installation_text = (
        DOCS_ROOT / 'installation_guide/installation.rst'
    ).read_text(encoding='utf-8')
    failures = []
    for connector in sorted(packaged_connectors()):
        if connector.startswith('tap-'):
            connector_doc = f"taps/{connector.removeprefix('tap-').replace('-', '_')}"
            if connector not in taps_text:
                failures.append(f'docs/connectors/taps.rst: missing packaged {connector}')
            if not re.search(rf'^\s+{re.escape(connector_doc)}\s*$', taps_text, re.MULTILINE):
                failures.append(
                    f'docs/connectors/taps.rst: navigation missing {connector_doc}'
                )
        if connector.startswith('target-'):
            connector_doc = (
                f"targets/{connector.removeprefix('target-').replace('-', '_')}"
            )
            if connector not in targets_text:
                failures.append(f'docs/connectors/targets.rst: missing packaged {connector}')
            if not re.search(
                rf'^\s+{re.escape(connector_doc)}\s*$', targets_text, re.MULTILINE
            ):
                failures.append(
                    f'docs/connectors/targets.rst: navigation missing {connector_doc}'
                )
        if connector not in installation_text:
            failures.append(
                f'docs/installation_guide/installation.rst: missing packaged {connector}'
            )
    return failures


def check_required_config_fields() -> list[str]:
    """Ensure schema-required fields and global keys appear in the YAML guide."""
    yaml_guide = (DOCS_ROOT / 'user_guide/yaml_config.rst').read_text(encoding='utf-8')
    required_fields = set()
    for filename in ('tap.json', 'target.json'):
        schema = json.loads(
            (REPO_ROOT / f'pipelinewise/cli/schemas/{filename}').read_text(
                encoding='utf-8'
            )
        )
        required_fields.update(schema['required'])

    config_schema = json.loads(
        (REPO_ROOT / 'pipelinewise/cli/schemas/config.json').read_text(encoding='utf-8')
    )
    required_fields.update(config_schema['properties'])
    return [
        f'docs/user_guide/yaml_config.rst: missing schema field {field}'
        for field in sorted(required_fields)
        if f'``{field}``' not in yaml_guide
    ]


def schema_property_names(value) -> set[str]:
    """Collect property names recursively from a JSON Schema structure."""
    if isinstance(value, dict):
        names = {
            name.removesuffix(':') for name in value.get('properties', {})
        }
        for item in value.values():
            names.update(schema_property_names(item))
        return names
    if isinstance(value, list):
        names = set()
        for item in value:
            names.update(schema_property_names(item))
        return names
    return set()


def check_schema_field_reference() -> list[str]:
    """Ensure every schema property is named somewhere in public docs."""
    fields = set()
    for path in sorted((REPO_ROOT / 'pipelinewise/cli/schemas').glob('*.json')):
        fields.update(schema_property_names(json.loads(path.read_text(encoding='utf-8'))))
    docs_text = '\n'.join(
        path.read_text(encoding='utf-8') for path in sorted(DOCS_ROOT.rglob('*.rst'))
    )
    return [
        f'docs: schema field is undocumented: {field}'
        for field in sorted(fields)
        if not re.search(
            rf'(?<![A-Za-z0-9_]){re.escape(field)}(?![A-Za-z0-9_])', docs_text
        )
    ]


def main() -> int:
    """Run all documentation reference checks."""
    failures = []
    failures.extend(check_yaml_examples())
    failures.extend(check_explicit_code_blocks())
    failures.extend(check_cli_reference())
    failures.extend(check_transformation_examples())
    failures.extend(check_connector_inventories())
    failures.extend(check_required_config_fields())
    failures.extend(check_schema_field_reference())
    if failures:
        print('\n'.join(failures))
        return 1
    print('Documentation reference checks passed.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
