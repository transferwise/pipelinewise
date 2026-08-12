"""Focused tests for documentation reference checks."""

import tempfile
import unittest

from pathlib import Path
from unittest import mock

import check_docs


class DocumentationChecksTest(unittest.TestCase):
    """Verify parsers and semantic guards used by ``make check``."""

    def test_yaml_blocks_supports_yaml_yml_and_directive_options(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'example.rst'
            path.write_text(
                '.. code-block:: yaml\n\n   first: 1\n\n'
                '.. code-block:: yml\n   :caption: Example\n\n   second: 2\n',
                encoding='utf-8',
            )

            blocks = list(check_docs.yaml_blocks(path))

        self.assertEqual([(3, 'first: 1'), (8, 'second: 2')], blocks)

    def test_cli_options_reads_only_long_argparse_options(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'cli.py'
            path.write_text(
                "parser.add_argument('--target', '-t')\n"
                "parser.add_argument('command')\n",
                encoding='utf-8',
            )

            options = check_docs.cli_options(path)

        self.assertEqual({'--target'}, options)

    def test_explicit_code_block_check_rejects_plain_literal_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_root = Path(directory)
            path = docs_root / 'example.rst'
            path.write_text('Example::\n\n   unsafe: block\n', encoding='utf-8')
            with mock.patch.object(
                check_docs, 'DOCS_ROOT', docs_root
            ), mock.patch.object(check_docs, 'REPO_ROOT', docs_root):
                failures = check_docs.check_explicit_code_blocks()

        self.assertEqual(1, len(failures))
        self.assertIn('explicit code-block language', failures[0])

    def test_contains_key_finds_nested_transformation_fields(self):
        value = {'transformations': [{'when': [{'field_path': 'user/id'}]}]}

        self.assertTrue(check_docs.contains_key(value, {'field_path'}))
        self.assertFalse(check_docs.contains_key(value, {'field_paths'}))

    def test_transformation_check_rejects_copyable_nested_field_yaml(self):
        with tempfile.TemporaryDirectory() as directory:
            docs_root = Path(directory)
            guide = docs_root / 'user_guide'
            guide.mkdir()
            (guide / 'transformations.rst').write_text(
                '.. code-block:: yaml\n\n'
                '   transformations:\n'
                '     - column: payload\n'
                '       field_paths: [user/id]\n',
                encoding='utf-8',
            )
            with mock.patch.object(check_docs, 'DOCS_ROOT', docs_root):
                failures = check_docs.check_transformation_examples()

        self.assertEqual(1, len(failures))
        self.assertIn('FastSync-eligible routes', failures[0])

    def test_schema_property_names_recurses_and_normalizes_colon_typos(self):
        schema = {
            'properties': {'id': {'type': 'string'}},
            'definitions': {
                'target': {'properties': {'file_format:': {'type': 'string'}}}
            },
        }

        self.assertEqual({'id', 'file_format'}, check_docs.schema_property_names(schema))


if __name__ == '__main__':
    unittest.main()
