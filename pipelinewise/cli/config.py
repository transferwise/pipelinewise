"""
PipelineWise CLI - Configuration class
"""
import logging
import os
import sys
import json

from typing import Dict, List

from pipelinewise.utils import safe_column_name
from . import utils


class Config:
    """PipelineWise Configuration Class"""

    def __init__(self, config_dir, temp_dir=None):
        """
        Class Constructor

        Initialising a configuration with an empty list of data flows
        """
        self.logger = logging.getLogger(__name__)
        self.config_dir = config_dir
        self.config_path = os.path.join(self.config_dir, 'config.json')
        self.temp_dir = temp_dir
        if self.temp_dir is None:
            self.temp_dir = os.path.join(self.config_dir, 'tmp')
        self.global_config = {}
        self.targets = {}

    @classmethod
    # pylint: disable=too-many-locals
    def from_yamls(cls, config_dir, yaml_dir='.', vault_secret=None, temp_dir=None):
        """
        Class Constructor

        Initialising a configuration from YAML files.

        Pipelinewise can import and generate singer configurations files
        from human friendly easy to understand YAML files.
        """
        config = cls(config_dir, temp_dir=temp_dir)
        targets = {}
        taps = {}

        config.logger.info('Searching YAML config files in %s', yaml_dir)
        global_config_yaml = os.path.join(yaml_dir, 'config.yml')
        tap_yamls, target_yamls = utils.get_tap_target_names(yaml_dir)

        global_config_schema = utils.load_schema('config')
        target_schema = utils.load_schema('target')
        tap_schema = utils.load_schema('tap')

        # Load global config yaml
        if os.path.exists(global_config_yaml):
            global_config = utils.load_yaml(global_config_yaml, vault_secret)
            utils.validate(instance=global_config, schema=global_config_schema)
            config.global_config = global_config or {}

        # pylint: disable=E1136,E1137  # False positive when loading vault encrypted YAML
        # Load every target yaml into targets dictionary
        for yaml_file in target_yamls:
            config.logger.info('LOADING TARGET: %s', yaml_file)
            target_data = utils.load_yaml(
                os.path.join(yaml_dir, yaml_file), vault_secret
            )
            utils.validate(instance=target_data, schema=target_schema)

            # Add generated extra keys that not available in the YAML
            target_id = target_data['id']

            # Check if a target with same ID already exists
            # exit with error, otherwise, we would override the previous one and that would go unnoticed.
            if target_id in targets:
                config.logger.error('Duplicate target found "%s"', target_id)
                sys.exit(1)

            target_data['files'] = config.get_connector_files(
                config.get_target_dir(target_id)
            )
            target_data['taps'] = []

            # Add target to list
            targets[target_id] = target_data

        # Load every tap yaml into targets dictionary
        for yaml_file in tap_yamls:
            config.logger.info('LOADING TAP: %s', yaml_file)
            tap_data = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            utils.validate(instance=tap_data, schema=tap_schema)

            tap_id = tap_data['id']

            # Check if a tap with same ID already exists
            # exit with error, otherwise, we would override the previous one and that would go unnoticed.
            if tap_id in taps:
                config.logger.error('Duplicate tap found "%s"', tap_id)
                sys.exit(1)

            target_id = tap_data['target']
            if target_id not in targets:
                config.logger.error(
                    "Can't find the target with the ID \"%s\" but it's referenced in %s",
                    target_id,
                    yaml_file,
                )
                sys.exit(1)

            # Add generated extra keys that not available in the YAML
            tap_data['files'] = config.get_connector_files(
                config.get_tap_dir(target_id, tap_id)
            )

            # Add tap to list
            taps[tap_id] = tap_data

        # Link taps to targets
        for target_key, target in targets.items():
            target['taps'] = [
                tap for tap in taps.values() if tap['target'] == target_key
            ]

        # Final structure is ready
        config.targets = targets

        return config

    def get_target_dir(self, target_id):
        """
        Returns the absolute path of a target configuration directory
        """
        return os.path.join(self.config_dir, target_id)

    def get_tap_dir(self, target_id, tap_id):
        """
        Returns the absolute path of a tap configuration directory
        """
        return os.path.join(self.config_dir, target_id, tap_id)

    @staticmethod
    def get_connector_files(connector_dir: str) -> Dict:
        """
        Returns the absolute paths of a tap/target configuration files
        """
        return {
            'config': os.path.join(connector_dir, 'config.json'),
            'inheritable_config': os.path.join(
                connector_dir, 'inheritable_config.json'
            ),
            'properties': os.path.join(connector_dir, 'properties.json'),
            'state': os.path.join(connector_dir, 'state.json'),
            'transformation': os.path.join(connector_dir, 'transformation.json'),
            'selection': os.path.join(connector_dir, 'selection.json'),
            'pidfile': os.path.join(connector_dir, 'pipelinewise.pid'),
        }

    def save(self):
        """
        Generating pipelinewise configuration directory layout on the disk.

        The pipelinewise configuration is a group of JSON files organised
        into a common directory structure and usually deployed into
        ~/.pipelinewise
        """
        self.logger.info('SAVING CONFIG')
        self.save_main_config_json()

        # Save every target config json
        for target in self.targets.values():
            self.save_target_jsons(target)

            # Save every tap JSON files
            for tap in target['taps']:
                extra_config_keys = utils.get_tap_extra_config_keys(
                    tap, self.temp_dir
                )
                self.save_tap_jsons(target, tap, extra_config_keys)

    def save_main_config_json(self):
        """
        Generating pipelinewise main config.json file

        This is in the main config directory, usually at ~/.pipelinewise/config.json
        and has the list of targets and its taps with some basic information.
        """
        self.logger.info('SAVING MAIN CONFIG JSON to %s', self.config_path)
        targets = []

        # Generate dictionary for config.json
        for target_tuple in self.targets.items():
            target = target_tuple[1]
            taps = []
            for tap in target.get('taps'):
                taps.append(
                    {
                        'id': tap.get('id'),
                        'name': tap.get('name'),
                        'type': tap.get('type'),
                        'owner': tap.get('owner'),
                        'stream_buffer_size': tap.get('stream_buffer_size'),
                        'send_alert': tap.get('send_alert', True),
                        'enabled': True,
                    }
                )

            targets.append(
                {
                    'id': target.get('id'),
                    'name': target.get('name'),
                    'status': 'ready',
                    'type': target.get('type'),
                    'taps': taps,
                }
            )
        main_config = {**self.global_config, **{'targets': targets}}

        # Create config dir if not exists
        if not os.path.exists(self.config_dir):
            os.mkdir(self.config_dir)

        # Save to JSON
        utils.save_json(main_config, self.config_path)

    def save_target_jsons(self, target):
        """
        Generating JSON config files for a singer target connector:
            1. config.json             :(Singer spec):  Tap connection details
        """
        target_dir = self.get_target_dir(target.get('id'))
        target_config_path = os.path.join(target_dir, 'config.json')
        self.logger.info('SAVING TARGET JSONS to %s', target_config_path)

        # Create target dir if not exists
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

        # Save target config.json
        utils.save_json(target.get('db_conn'), target_config_path)

    # pylint: disable=too-many-locals
    def save_tap_jsons(self, target, tap, extra_config_keys=None):
        """
        Generating JSON config files for a singer tap connector:
            1. config.json             :(Singer spec):  Tap connection details

            4. selection.json          :(Pipelinewise): List of streams/tables to replicate
            5. inheritable_config.json :(Pipelinewise): Extra config keys for the linked
                                                        singer target connector that
                                                        pipelinewise will pass at run time
            6. transformation.json     :(Pipelinewise): Column transformations between the
                                                        tap and target
        """
        if extra_config_keys is None:
            extra_config_keys = {}

        # Generate tap config dict
        tap_config = self.generate_tap_connection_config(tap, extra_config_keys)

        # Generate tap selection
        tap_selection = {'selection': self.generate_selection(tap)}

        # Generate tap transformation
        tap_transformation = {'transformations': self.generate_transformations(tap)}

        # Generate tap inheritable_config dict
        tap_inheritable_config = self.generate_inheritable_config(tap)

        tap_dir = self.get_tap_dir(target.get('id'), tap.get('id'))
        self.logger.info('SAVING TAP JSONS to %s', tap_dir)

        # Define tap JSON file paths
        tap_config_path = os.path.join(tap_dir, 'config.json')
        tap_selection_path = os.path.join(tap_dir, 'selection.json')
        tap_transformation_path = os.path.join(tap_dir, 'transformation.json')
        tap_inheritable_config_path = os.path.join(tap_dir, 'inheritable_config.json')

        # Create tap dir if not exists
        if not os.path.exists(tap_dir):
            os.mkdir(tap_dir)

        # Save the generated JSON files
        utils.save_json(tap_config, tap_config_path)
        utils.save_json(tap_inheritable_config, tap_inheritable_config_path)
        utils.save_json(tap_transformation, tap_transformation_path)
        utils.save_json(tap_selection, tap_selection_path)

    @classmethod
    def generate_tap_connection_config(cls, tap: Dict, extra_config_keys: Dict) -> Dict:
        """
        Generate tap connection config which is a merged dictionary of db_connection and optional extra_keys
        Args:
            tap: tap config
            extra_config_keys:  extra keys to add to the db conn config
        Returns: Dictionary of tap connection config
        """
        return {**tap.get('db_conn'), **extra_config_keys}

    @classmethod
    def generate_selection(cls, tap: Dict) -> List[Dict]:
        """
        Generate the selection data which is the list of selected streams and their replication method
        Args:
            tap: the tap config dictionary

        Returns: List of dictionaries of selected streams
        """
        selection = []

        for schema in tap.get('schemas', []):
            schema_name = schema.get('source_schema')
            for table in schema.get('tables', []):
                table_name = table.get('table_name')
                replication_method = table.get(
                    'replication_method', utils.get_tap_default_replication_method(tap)
                )
                selection.append(
                    utils.delete_empty_keys(
                        {
                            'tap_stream_id': utils.get_tap_stream_id(
                                tap, tap['db_conn'].get('dbname'), schema_name, table_name
                            ),
                            'replication_method': replication_method,
                            # Add replication_key only if replication_method is INCREMENTAL
                            'replication_key': table.get('replication_key')
                            if replication_method == 'INCREMENTAL' else None,
                        }
                    )
                )

        return selection

    @classmethod
    def generate_transformations(cls, tap: Dict) -> List[Dict]:
        """
        Generate the transformations data from the given tap config
        Args:
            tap: the tap config dictionary

        Returns: List of transformations
        """
        transformations = []

        for schema in tap.get('schemas', []):
            schema_name = schema.get('source_schema')
            for table in schema.get('tables', []):
                table_name = table.get('table_name')
                for trans in table.get('transformations', []):
                    transformations.append(
                        {
                            'tap_stream_name': utils.get_tap_stream_name(
                                tap, tap['db_conn'].get('dbname'), schema_name, table_name),
                            'field_id': trans['column'],
                            # Make column name safe by wrapping it in quotes, it's useful when a field_id is a reserved
                            # word to be used by target snowflake in fastsync
                            'safe_field_id': safe_column_name(trans['column']),
                            'field_paths': trans.get('field_paths'),
                            'type': trans['type'],
                            'when': trans.get('when'),
                        }
                    )

        return transformations

    def generate_inheritable_config(self, tap: Dict) -> Dict:
        """
        Generate the inheritable config which is the custom config that should be fed to the target at runtime
        Args:
            tap: tap config

        Returns: Dictionary of config
        """
        schema_mapping = {}

        for schema in tap.get('schemas', []):
            source_schema = schema.get('source_schema')
            target_schema = schema.get('target_schema')
            target_schema_select_perms = schema.get(
                'target_schema_select_permissions', []
            )

            schema_mapping[source_schema] = {
                'target_schema': target_schema,
                'target_schema_select_permissions': target_schema_select_perms,
            }

            # Schema mapping can include list of indices to create. Some target components
            # like target-postgres create indices automatically
            indices = {}
            for table in schema.get('tables', []):
                table_name = table.get('table_name')
                table_indices = table.get('indices')
                if table_indices:
                    indices[table_name] = table_indices

            # Add indices map to schema mapping
            if indices:
                schema_mapping[source_schema]['indices'] = indices

        # Generate tap inheritable_config dict
        tap_inheritable_config = utils.delete_empty_keys(
            {
                'temp_dir': self.temp_dir,
                'tap_id': tap.get('id'),
                'query_tag': json.dumps(
                    {
                        'ppw_component': tap.get('type'),
                        'tap_id': tap.get('id'),
                        'database': '{{database}}',
                        'schema': '{{schema}}',
                        'table': '{{table}}',
                    }
                ),
                'batch_size_rows': tap.get('batch_size_rows', 20000),
                'batch_wait_limit_seconds': tap.get('batch_wait_limit_seconds', None),
                'parallelism': tap.get('parallelism', 0),
                'parallelism_max': tap.get('parallelism_max', 4),
                'hard_delete': tap.get('hard_delete', True),
                'append_only': tap.get('append_only', False),
                'flush_all_streams': tap.get('flush_all_streams', False),
                'primary_key_required': tap.get('primary_key_required', True),
                'default_target_schema': tap.get('default_target_schema'),
                'default_target_schema_select_permissions': tap.get(
                    'default_target_schema_select_permissions'
                ),
                'schema_mapping': schema_mapping,
                # data_flattening_max_level
                # -------------------------
                #
                # 'data_flattening_max_level' is an optional parameter in some target connectors that specifies
                # how to load nested object into destination.
                #
                # We can load the original object represented as JSON or string (data flattening off) or we can
                # flatten the schema and data by creating columns automatically. When 'data_flattening_max_level'
                # is set to 0 then flattening functionality is turned off.
                #
                # The value can be set in multiple place and evaluated in the following order:
                # ------------
                #   1: First we try to find it in the tap YAML
                #   2: Second we try to get the tap type specific default value
                #   3: Otherwise we set flattening level to 0 (disabled)
                'data_flattening_max_level': tap.get(
                    'data_flattening_max_level',
                    utils.get_tap_property(tap, 'default_data_flattening_max_level') or 0,
                    ),
                'validate_records': tap.get('validate_records', False),
                'add_metadata_columns': tap.get('add_metadata_columns', False),
                'split_large_files': tap.get('split_large_files', False),
                'split_file_chunk_size_mb': tap.get('split_file_chunk_size_mb', 1000),
                'split_file_max_chunks': tap.get('split_file_max_chunks', 20),
                'archive_load_files': tap.get('archive_load_files', False),
                'archive_load_files_s3_bucket': tap.get(
                    'archive_load_files_s3_bucket', None
                ),
                'archive_load_files_s3_prefix': tap.get(
                    'archive_load_files_s3_prefix', None
                ),
                'archive_load_files_gcs_bucket': tap.get(
                    'archive_load_files_gcs_bucket', None
                ),
                'archive_load_files_gcs_prefix': tap.get(
                    'archive_load_files_gcs_prefix', None
                ),
            }
        )

        return tap_inheritable_config
