import os
import logging

from . import utils


class Config(object):
    def __init__(self, config_dir):
        '''
        Class Constructor

        Initialising a configuration with an empty list of data flows
        '''
        self.logger = logging.getLogger('Pipelinewise CLI')
        self.config_dir = config_dir
        self.config_path = os.path.join(self.config_dir, "config.json")

        self.targets = []


    @classmethod
    def from_yamls(cls, config_dir, yaml_dir=".", vault_secret=None):
        '''
        Class Constructor

        Initialising a configuration from YAML files.

        Pipelinewise can import and generate singer configurations files
        from human friendly easy to understand YAML files.
        '''
        config = cls(config_dir)
        config.logger.info("Searching YAML config files in {}".format(yaml_dir))
        targets = {}
        taps = {}

        # YAML files must match one of the patterns:
        #   target_*.yml
        #   tap_*.yml        
        yamls = [f for f in os.listdir(yaml_dir) if os.path.isfile(os.path.join(yaml_dir, f)) and f.endswith(".yml")]
        target_yamls = list(filter(lambda y: y.startswith("target_"), yamls))
        tap_yamls = list(filter(lambda y: y.startswith("tap_"), yamls))

        # Load every target yaml into targets dictionary        
        for yaml_file in target_yamls:
            config.logger.info("LOADING TARGET: {}".format(yaml_file))
            target_data = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            utils.validate(instance=target_data, schema=utils.load_schema("target"))

            # Add generated extra keys that not available in the YAML
            target_id = target_data['id']

            target_data['files'] = config.get_connector_files(config.get_target_dir(target_id))
            target_data['taps'] = []

            # Add target to list
            targets[target_id] = target_data

        # Load every tap yaml into targets dictionary
        for yaml_file in tap_yamls:
            config.logger.info("LOADING TAP: {}".format(yaml_file))
            tap_data = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            utils.validate(instance=tap_data, schema=utils.load_schema("tap"))

            # Add generated extra keys that not available in the YAML
            tap_id = tap_data['id']
            target_id = tap_data['target']

            tap_data['files'] = config.get_connector_files(config.get_tap_dir(target_id, tap_id))

            # Add tap to list
            taps[tap_id] = tap_data

        # Link taps to targets
        for target_key in targets.keys():
            for tap_key in taps.keys():
                if taps.get(tap_key).get('target') == targets.get(target_key).get('id'):
                    targets.get(target_key).get('taps').append(taps.get(tap_key))

        # Final structure is ready
        config.targets = targets
        
        return config


    def get_target_dir(self, target_id):
        '''
        Returns the absolute path of a target configuration directory
        ''' 
        return os.path.join(self.config_dir, target_id)


    def get_tap_dir(self, target_id, tap_id):
        '''
        Returns the absolute path of a tap configuration directory 
        ''' 
        return os.path.join(self.config_dir, target_id, tap_id)


    def get_connector_files(self, connector_dir):
        '''
        Returns the absolute paths of a tap/target configuration files
        '''
        return {
            'config': os.path.join(connector_dir, 'config.json'),
            'inheritable_config': os.path.join(connector_dir, 'inheritable_config.json'),
            'properties': os.path.join(connector_dir, 'properties.json'),
            'state': os.path.join(connector_dir, 'state.json'),
            'transformation': os.path.join(connector_dir, 'transformation.json'),
            'selection': os.path.join(connector_dir, 'selection.json'),
        }


    def save(self):
        '''
        Generating pipelinewise configuration directory layout on the disk.

        The pipelinewise configuration is a group of JSON files organised
        into a common directory structure and usually deployed into
        ~/.pipelinewise
        ''' 
        self.logger.info("SAVING CONFIG")
        self.save_main_config_json()

        # Save every target config json
        for target_key in self.targets.keys():
            target = self.targets[target_key]
            self.save_target_jsons(target)

            # Save every tap JSON files
            for i, tap in enumerate(target['taps']):
                extra_config_keys = utils.get_tap_extra_config_keys(tap)
                self.save_tap_jsons(target, tap, extra_config_keys)


    def save_main_config_json(self):
        '''
        Generating pipelinewise main config.json file

        This is in the main config directory, usually at ~/.pipelinewise/config.json
        and has the list of targets and its taps with some basic information.
        '''
        self.logger.info("SAVING MAIN CONFIG JSON to {}".format(self.config_path))
        targets = []

        # Generate dictionary for config.json
        for tk in self.targets.keys():
            taps = []
            for tap in self.targets[tk].get('taps'):
                taps.append({
                    "id": tap.get('id'),
                    "name": tap.get('name'),
                    "type": tap.get('type'),
                    "owner": tap.get('owner'),
                    "sync_period": tap.get('sync_period'),
                    "enabled": True
                })

            targets.append({
                "id": self.targets[tk].get('id'),
                "name": self.targets[tk].get('name'),
                "status": "ready",
                "type": self.targets[tk].get('type'),
                "taps": taps
            })
        main_config = {
            "targets": targets
        }

        # Create config dir if not exists
        if not os.path.exists(self.config_dir):
            os.mkdir(self.config_dir)

        # Save to JSON
        utils.save_json(main_config, self.config_path)


    def save_target_jsons(self, target):
        '''
        Generating JSON config files for a singer target connector:
            1. config.json             :(Singer spec):  Tap connection details
        '''
        target_dir = self.get_target_dir(target.get('id'))
        target_config_path = os.path.join(target_dir, "config.json")
        self.logger.info("SAVING TARGET JSONS to {}".format(target_config_path))

        # Create target dir if not exists
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

        # Save target config.json
        utils.save_json(target.get('db_conn'), target_config_path)


    def save_tap_jsons(self, target, tap, extra_config_keys={}):
        '''
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
        '''
        target_dir = self.get_target_dir(target.get('id'))
        tap_dir = self.get_tap_dir(target.get('id'), tap.get('id'))
        self.logger.info("SAVING TAP JSONS to {}".format(tap_dir))

        # Define tap JSON file paths
        tap_config_path = os.path.join(tap_dir, "config.json")
        tap_selection_path = os.path.join(tap_dir, "selection.json")
        tap_transformation_path = os.path.join(tap_dir, "transformation.json")
        tap_inheritable_config_path = os.path.join(tap_dir, "inheritable_config.json")

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
                selection.append(utils.delete_empty_keys({
                    "tap_stream_id": utils.get_tap_stream_id(tap, tap_dbname, schema_name, table_name),

                    # Default replication_method is LOG_BASED
                    "replication_method": table.get('replication_method', 'LOG_BASED'),

                    # Add replication_key only if replication_method is INCREMENTAL
                    "replication_key": table.get('replication_key') if table.get('replication_method') == 'INCREMENTAL' else None
                }))
        tap_selection = {
            "selection": selection
        }

        # Generate tap transformation
        transformations = []
        for schema in tap.get('schemas', []):
            schema_name = schema.get('source_schema')
            for table in schema.get('tables', []):
                table_name = table.get('table_name')
                for trans in table.get('transformations', []):
                    transformations.append({
                        "tap_stream_name": utils.get_tap_stream_name(tap, tap_dbname, schema_name, table_name),
                        "field_id": trans.get('column'),
                        "type": trans.get('type')
                    })
        tap_transformation = {
            "transformations": transformations
        }

        # Generate stream to schema mapping
        schema_mapping = {}
        for schema in tap.get('schemas', []):
            source_schema = schema.get('source_schema')
            target_schema = schema.get('target_schema')
            target_schema_select_permissions = schema.get('target_schema_select_permissions')

            schema_mapping[source_schema] = {
                "target_schema": target_schema,
                "target_schema_select_permissions": target_schema_select_permissions
            }
        tap_schema_mapping = {
            "schema_mapping": schema_mapping
        }

        # Generate tap inheritable_config dict
        tap_inheritable_config = utils.delete_empty_keys({
            "batch_size_rows": tap.get('batch_size_rows'),
            "hard_delete": tap.get('hard_delete', True),
            "primary_key_required": tap.get('primary_key_required', True),
            "default_target_schema": tap.get('default_target_schema'),
            "default_target_schema_select_permissions": tap.get('default_target_schema_select_permissions'),
            "schema_mapping": schema_mapping
        })

        # Save the generated JSON files
        utils.save_json(tap_config, tap_config_path)
        utils.save_json(tap_inheritable_config, tap_inheritable_config_path)
        utils.save_json(tap_transformation, tap_transformation_path)
        utils.save_json(tap_selection, tap_selection_path)
