import json
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, mock

from pipelinewise import cli


class TapInfoTestCase(TestCase):

    def test_tap_info_output(self):
        """The tap_info JSON groups selected tables and replication methods by source schema."""
        config_dir = Path(__file__).parent / 'resources' / 'sample_json_config'
        tap_id = 'tap_one'
        target_id = 'target_one'
        config = json.loads((config_dir / 'config.json').read_text(encoding='utf-8'))
        target = next(item for item in config['targets'] if item['id'] == target_id)
        tap = next(item for item in target['taps'] if item['id'] == tap_id)
        tap_config_path = config_dir / target_id / tap_id / 'config.json'
        tap_config = json.loads(tap_config_path.read_text(encoding='utf-8'))
        tap_dir = config_dir / target_id / tap_id
        selection = json.loads((tap_dir / 'selection.json').read_text(encoding='utf-8'))
        catalog = json.loads((tap_dir / 'properties.json').read_text(encoding='utf-8'))
        streams = {stream['tap_stream_id']: stream for stream in catalog['streams']}
        selected_tables = {}
        for item in selection['selection']:
            stream = streams[item['tap_stream_id']]
            schema_name = stream['metadata'][0]['metadata']['database-name']
            selected_tables.setdefault(schema_name, {})[stream['table_name']] = {
                'replication_method': item['replication_method'],
                'size_bytes': 4096,
                'indexes': [], 'primary_key': None,
            }
        details = {
            (schema, table): {key: value for key, value in info.items() if key != 'replication_method'}
            for schema, tables in selected_tables.items() for table, info in tables.items()
        }
        with (
            mock.patch('pipelinewise.cli.pipelinewise.get_source_table_details', return_value=details),
            mock.patch('sys.argv', ['pipelinewise', 'tap_info', '--tap', tap_id]),
            mock.patch('sys.stdout', new_callable=StringIO) as stdout,
            mock.patch.object(cli, '__init_logger'),
            mock.patch.object(cli, 'CONFIG_DIR', str(config_dir)),
            mock.patch('pipelinewise.cli.pipelinewise.signal.signal'),
        ):
            cli.main()

        expected_output_json = {
            'tap_information': {
                'tap_id': tap_id,
                'target_id': target_id,
                'tap_type': tap['type'],
                'source_database': tap_config['host'],
                'selected_tables': selected_tables,
            }
        }
        self.assertDictEqual(json.loads(stdout.getvalue()), expected_output_json)

    def test_tap_info_without_tap_id_outputs_error(self):
        """The tap_info command prints Error when no tap ID is provided."""
        with (
            mock.patch('sys.argv', ['pipelinewise', 'tap_info', '--target', 'target_one']),
            mock.patch('sys.stdout', new_callable=StringIO),
            mock.patch.object(cli, '__init_logger'),
            mock.patch.object(cli.PipelineWise, '__init__', return_value=None),
        ):
            with self.assertRaises(SystemExit) as system_exit:
                cli.main()
            self.assertEqual(system_exit.exception.code, 1)

    def test_tap_info_with_unknown_tap_id_exits_with_error(self):
        """Reject an unconfigured tap ID under an existing target."""
        config_dir = Path(__file__).parent / 'resources' / 'sample_json_config'
        with (
            mock.patch('sys.argv', ['pipelinewise', 'tap_info', '--tap', 'unknown_tap', '--target', 'target_one']),
            mock.patch('sys.stdout', new_callable=StringIO),
            mock.patch.object(cli, 'CONFIG_DIR', str(config_dir)),
            mock.patch.object(cli, '__init_logger'),
            mock.patch('pipelinewise.cli.pipelinewise.signal.signal'),
        ):
            with self.assertRaises(SystemExit) as system_exit:
                cli.main()

        self.assertEqual(system_exit.exception.code, 1)

    def test_tap_info_with_duplicate_tap_ids_requires_target(self):
        """List every matching tap and target before reading source configuration."""
        instance = cli.PipelineWise.__new__(cli.PipelineWise)
        instance.args = SimpleNamespace(tap='tap_one', target='*')
        instance.logger = mock.Mock()
        targets = [
            {'id': target, 'taps': [{'id': 'tap_one', 'type': 'tap-mysql'}]}
            for target in ['target_one', 'target_two', 'target_three']
        ]
        with (
            mock.patch.object(instance, 'get_targets', return_value=targets),
            mock.patch('pipelinewise.cli.utils.LOGGER') as logger,
            mock.patch('builtins.open') as open_file,
        ):
            with self.assertRaises(SystemExit) as system_exit:
                instance.tap_info()

        self.assertEqual(system_exit.exception.code, 1)
        open_file.assert_not_called()
        error_args = logger.error.call_args.args
        message = error_args[0] % error_args[1:]
        for target in targets:
            self.assertIn(f'--tap tap_one --target {target["id"]}', message)
        self.assertIn('Use --target', message)

    def test_tap_info_with_unknown_tap_without_target_exits_with_error(self):
        """Reject an unknown tap when searching across targets."""
        instance = cli.PipelineWise.__new__(cli.PipelineWise)
        instance.args = SimpleNamespace(tap='unknown_tap', target='*')
        instance.logger = mock.Mock()
        with mock.patch.object(instance, 'get_targets', return_value=[{'id': 'target_one', 'taps': []}]):
            with self.assertRaises(SystemExit) as system_exit:
                instance.tap_info()
        self.assertEqual(system_exit.exception.code, 1)

    def test_tap_info_with_tap_under_another_target_exits_with_error(self):
        """Reject a tap that exists only under another target."""
        config_dir = Path(__file__).parent / 'resources' / 'sample_json_config'
        with (
            mock.patch('sys.argv', ['pipelinewise', 'tap_info', '--tap', 'tap_three', '--target', 'target_one']),
            mock.patch('sys.stdout', new_callable=StringIO),
            mock.patch.object(cli, 'CONFIG_DIR', str(config_dir)),
            mock.patch.object(cli, '__init_logger'),
            mock.patch('pipelinewise.cli.pipelinewise.signal.signal'),
        ):
            with self.assertRaises(SystemExit) as system_exit:
                cli.main()

        self.assertEqual(system_exit.exception.code, 1)

    def test_tap_info_with_unknown_target_id_exits_with_error(self):
        """Reject an unknown target even when the tap exists elsewhere."""
        config_dir = Path(__file__).parent / 'resources' / 'sample_json_config'
        with (
            mock.patch('sys.argv', ['pipelinewise', 'tap_info', '--tap', 'tap_one', '--target', 'unknown_target']),
            mock.patch('sys.stdout', new_callable=StringIO),
            mock.patch.object(cli, 'CONFIG_DIR', str(config_dir)),
            mock.patch.object(cli, '__init_logger'),
            mock.patch('pipelinewise.cli.pipelinewise.signal.signal'),
        ):
            with self.assertRaises(SystemExit) as system_exit:
                cli.main()

        self.assertEqual(system_exit.exception.code, 1)

    def test_tap_info_groups_same_table_name_under_distinct_schemas(self):
        """Keep same-named tables separate and prefer source schema over database."""
        instance = cli.PipelineWise.__new__(cli.PipelineWise)
        instance.args = SimpleNamespace(tap='tap_one', target='target_one')
        instance.config_dir = '/unused'
        catalog = {
            'streams': [
                {
                    'tap_stream_id': f'{schema}-orders',
                    'table_name': 'orders',
                    'metadata': [{
                        'breadcrumb': [],
                        'metadata': {'schema-name': schema, 'database-name': 'source_db'},
                    }],
                }
                for schema in ['schema_one', 'schema_two']
            ]
        }
        selection = {'selection': [
            {'tap_stream_id': 'schema_one-orders', 'replication_method': 'LOG_BASED'},
            {'tap_stream_id': 'schema_two-orders', 'replication_method': 'INCREMENTAL'},
        ]}
        with (
            mock.patch('pipelinewise.cli.pipelinewise.get_source_table_details', return_value={
                ('schema_one', 'orders'): {'size_bytes': 4096, 'indexes': [{
                    'name': 'orders_idx', 'is_unique': True, 'type': 'btree', 'definition': 'CREATE INDEX ...',
                    'columns': [
                        {'name': 'tenant_id', 'order': 'DESC', 'prefix_length': None},
                        {'name': 'order_id', 'is_included': True},
                        {'name': None, 'expression': 'lower(email)'},
                    ],
                }], 'primary_key': {
                    'name': 'orders_pkey', 'columns': ['tenant_id', 'order_id'],
                    'is_deferrable': False, 'initially_deferred': False, 'is_validated': True,
                }, 'unique_constraints': [{'name': 'orders_unique', 'columns': ['order_id']}],
                    'foreign_keys': [{'name': 'tenant_fk', 'columns': ['tenant_id']}],
                },
                ('schema_two', 'orders'): {
                    'size_bytes': 8192, 'indexes': [], 'primary_key': None,
                    'unique_constraints': [], 'foreign_keys': [],
                },
            }),
            mock.patch.object(instance, 'get_targets', return_value=[{
                'id': 'target_one', 'taps': [{'id': 'tap_one', 'type': 'tap-postgres'}],
            }, {
                'id': 'target_two', 'taps': [{'id': 'tap_one', 'type': 'tap-mysql'}],
            }]),
            mock.patch('builtins.open', side_effect=[
                StringIO(json.dumps({'host': 'localhost'})),
                StringIO(json.dumps(selection)),
                StringIO(json.dumps(catalog)),
            ]),
            mock.patch('sys.stdout', new_callable=StringIO) as stdout,
        ):
            instance.tap_info()

        self.assertEqual(json.loads(stdout.getvalue())['tap_information']['selected_tables'], {
            'schema_one': {'orders': {
                'replication_method': 'LOG_BASED', 'size_bytes': 4096,
                'indexes': [{'name': 'orders_idx', 'columns': ['tenant_id', 'order_id', None]}],
                'primary_key': {'name': 'orders_pkey', 'columns': ['tenant_id', 'order_id']},
            }},
            'schema_two': {'orders': {
                'replication_method': 'INCREMENTAL', 'size_bytes': 8192, 'indexes': [], 'primary_key': None,
            }},
        })

    def test_tap_info_with_missing_selected_stream_exits_with_error(self):
        """Reject a stale selection without printing incomplete table information."""
        instance = cli.PipelineWise.__new__(cli.PipelineWise)
        instance.args = SimpleNamespace(tap='tap_one', target='target_one')
        instance.config_dir = '/unused'
        instance.logger = mock.Mock()
        missing_stream_id = 'foo_schema-foo_table_1'
        selection = {'selection': [
            {'tap_stream_id': 'source-existing', 'replication_method': 'LOG_BASED'},
            {'tap_stream_id': missing_stream_id, 'replication_method': 'INCREMENTAL'},
        ]}
        catalog = {'streams': [{'tap_stream_id': 'source-existing'}]}
        with (
            mock.patch.object(instance, 'get_targets', return_value=[{
                'id': 'target_one', 'taps': [{'id': 'tap_one', 'type': 'tap-mysql'}],
            }]),
            mock.patch('builtins.open', side_effect=[
                StringIO(json.dumps({'host': 'localhost'})),
                StringIO(json.dumps(selection)),
                StringIO(json.dumps(catalog)),
            ]),
            mock.patch('sys.stdout', new_callable=StringIO) as stdout,
        ):
            with self.assertRaises(SystemExit) as system_exit:
                instance.tap_info()

        self.assertEqual(system_exit.exception.code, 1)
        self.assertEqual(stdout.getvalue(), '')
        error_args = instance.logger.error.call_args.args
        error_message = error_args[0] % error_args[1:]
        self.assertIn(missing_stream_id, error_message)
        self.assertIn('discover_tap --tap tap_one --target target_one', error_message)

    def test_tap_info_with_unsupported_tap_type_exits_with_error(self):
        """Reject unsupported connectors before reading their generated configuration."""
        for tap_type in ['tap-mongodb', 'tap-salesforce', 'tap-github', None]:
            with self.subTest(tap_type=tap_type):
                instance = cli.PipelineWise.__new__(cli.PipelineWise)
                instance.args = SimpleNamespace(tap='tap_one', target='target_one')
                instance.logger = mock.Mock()
                with (
                    mock.patch.object(instance, 'get_targets', return_value=[{
                        'id': 'target_one', 'taps': [{'id': 'tap_one', 'type': tap_type}],
                    }]),
                    mock.patch('builtins.open') as open_file,
                    mock.patch('sys.stdout', new_callable=StringIO) as stdout,
                ):
                    with self.assertRaises(SystemExit) as system_exit:
                        instance.tap_info()

                self.assertEqual(system_exit.exception.code, 1)
                self.assertEqual(stdout.getvalue(), '')
                open_file.assert_not_called()
                instance.logger.error.assert_called_once_with(
                    'This command only supports mysql and postgres taps'
                )
