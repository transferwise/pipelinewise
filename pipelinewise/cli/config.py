"""
PipelineWise CLI - Configuration class
"""
import logging
import os
import sys

from pipelinewise.fastsync.commons.utils import safe_column_name

from . import utils


class Config:
    """PipelineWise Configuration Class"""

    def __init__(self, config_dir):
        """
        Class Constructor

        Initialising a configuration with an empty list of data flows
        """
        self.logger = logging.getLogger('Pipelinewise CLI')
        self.config_dir = config_dir
        self.config_path = os.path.join(self.config_dir, 'config.json')

        self.targets = []

    @classmethod
    # pylint: disable=too-many-locals
    def from_yamls(cls, config_dir, yaml_dir='.', vault_secret=None):
        """
        Class Constructor

        Initialising a configuration from YAML files.

        Pipelinewise can import and generate singer configurations files
        from human friendly easy to understand YAML files.
        """
        config = cls(config_dir)
        targets = {}
        taps = {}

        config.logger.info('Searching YAML config files in %s', yaml_dir)
        tap_yamls, target_yamls = utils.get_tap_target_names(yaml_dir)

        target_schema = utils.load_schema('target')
        tap_schema = utils.load_schema('tap')

        # Load every target yaml into targets dictionary
        for yaml_file in target_yamls:
            config.logger.info('LOADING TARGET: %s', yaml_file)
            target_data = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            utils.validate(instance=target_data, schema=target_schema)

            # Add generated extra keys that not available in the YAML
            target_id = target_data['id']

            target_data['files'] = config.get_connector_files(config.get_target_dir(target_id))
            target_data['taps'] = []

            # Add target to list
            targets[target_id] = target_data

        # Load every tap yaml into targets dictionary
        for yaml_file in tap_yamls:
            config.logger.info('LOADING TAP: %s', yaml_file)
            tap_data = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            utils.validate(instance=tap_data, schema=tap_schema)

            tap_id = tap_data['id']
            target_id = tap_data['target']
            if target_id not in targets:
                config.logger.error("Can't find the target with the ID \"%s\" but it's referenced in %s", target_id,
                                    yaml_file)
                sys.exit(1)

            # Add generated extra keys that not available in the YAML
            tap_data['files'] = config.get_connector_files(config.get_tap_dir(target_id, tap_id))

            # Add tap to list
            taps[tap_id] = tap_data

        # Link taps to targets
        for target_key in targets:
            for tap_key in taps:
                if taps.get(tap_key).get('target') == targets.get(target_key).get('id'):
                    targets.get(target_key).get('taps').append(taps.get(tap_key))

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
    def get_connector_files(connector_dir):
        """
        Returns the absolute paths of a tap/target configuration files
        """
        return {
            'config': os.path.join(connector_dir, 'config.json'),
            'inheritable_config': os.path.join(connector_dir, 'inheritable_config.json'),
            'properties': os.path.join(connector_dir, 'properties.json'),
            'state': os.path.join(connector_dir, 'state.json'),
            'transformation': os.path.join(connector_dir, 'transformation.json'),
            'selection': os.path.join(connector_dir, 'selection.json'),
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
        for target_key in self.targets:
            target = self.targets[target_key]
            self.save_target_jsons(target)

            # Save every tap JSON files
            # pylint: disable=unused-variable
            for i, tap in enumerate(target['taps']):
                extra_config_keys = utils.get_tap_extra_config_keys(tap)
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
        for key in self.targets:
            taps = []
            for tap in self.targets[key].get('taps'):
                taps.append({
                    'id': tap.get('id'),
                    'name': tap.get('name'),
                    'type': tap.get('type'),
                    'owner': tap.get('owner'),
                    'enabled': True
                })

            targets.append({
                'id': self.targets[key].get('id'),
                'name': self.targets[key].get('name'),
                'status': 'ready',
                'type': self.targets[key].get('type'),
                'taps': taps
            })
        main_config = {'targets': targets}

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
            2. properties.json         :(Singer spec):  Tap schema properties (generated)
            3. state.json              :(Singer spec):  Bookmark for incremental and log_based
                                                        replications

            4. selection.json          :(Pipelinewise): List of streams/tables to replicate
            5. inheritabel_config.json :(Pipelinewise): Extra config keys for the linked
                                                        singer target connector that
                                                        pipelinewise will pass at run time
            6. transformation.json     :(Pipelinewise): Column transformations between the
                                                        tap and target
        """
        if extra_config_keys is None:
            extra_config_keys = {}
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

        # Generate tap config dict: a merged dictionary of db_connection and optional extra_keys
        tap_config = {**tap.get('db_conn'), **extra_config_keys}

        # Get additional properties will be needed later to generate tap_stream_id
        tap_dbname = tap_config.get('dbname')

        # Generate tap selection
        selection = []
        for schema in tap.get('schemas', []):
            schema_name = schema.get('source_schema')
            for table in schema.get('tables', []):
                table_name = table.get('table_name')
                replication_method = table.get('replication_method', utils.get_tap_default_replication_method(tap))
                selection.append(utils.delete_empty_keys({
                    'tap_stream_id': utils.get_tap_stream_id(tap, tap_dbname, schema_name, table_name),
                    'replication_method': replication_method,

                    # Add replication_key only if replication_method is INCREMENTAL
                    'replication_key': table.get('replication_key') if replication_method == 'INCREMENTAL' else None
                }))
        tap_selection = {'selection': selection}

        # Generate tap transformation
        transformations = []
        for schema in tap.get('schemas', []):
            schema_name = schema.get('source_schema')
            for table in schema.get('tables', []):
                table_name = table.get('table_name')
                for trans in table.get('transformations', []):
                    transformations.append({
                        'tap_stream_name': utils.get_tap_stream_name(tap, tap_dbname, schema_name, table_name),
                        'field_id': trans['column'],
                        # Make column name safe by wrapping it in quotes, it's useful when a field_id is a reserved word
                        # to be used by target snowflake in fastsync
                        'safe_field_id': safe_column_name(trans['column']),
                        'type': trans['type'],
                        'when': trans.get('when')
                    })
        tap_transformation = {
            'transformations': transformations
        }

        # Generate stream to schema mapping
        schema_mapping = {}
        for schema in tap.get('schemas', []):
            source_schema = schema.get('source_schema')
            target_schema = schema.get('target_schema')
            target_schema_select_perms = schema.get('target_schema_select_permissions', [])

            schema_mapping[source_schema] = {
                'target_schema': target_schema,
                'target_schema_select_permissions': target_schema_select_perms
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
        tap_inheritable_config = utils.delete_empty_keys({
            'batch_size_rows': tap.get('batch_size_rows'),
            'hard_delete': tap.get('hard_delete', True),
            'flush_all_streams': tap.get('flush_all_streams', False),
            'primary_key_required': tap.get('primary_key_required', True),
            'default_target_schema': tap.get('default_target_schema'),
            'default_target_schema_select_permissions': tap.get('default_target_schema_select_permissions'),
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
            #  The value can be set in mutliple place and evaluated in the following order:
            # ------------
            #   1: First we try to find it in the tap YAML
            #   2: Second we try to get the tap type specific default value
            #   3: Otherwise we set flattening level to 0 (disabled)
            "data_flattening_max_level": tap.get('data_flattening_max_level',
                                                 utils.get_tap_property(tap, 'default_data_flattening_max_level') or 0),
            "validate_records": tap.get('validate_records', False)
        })

        # Save the generated JSON files
        utils.save_json(tap_config, tap_config_path)
        utils.save_json(tap_inheritable_config, tap_inheritable_config_path)
        utils.save_json(tap_transformation, tap_transformation_path)
        utils.save_json(tap_selection, tap_selection_path)
