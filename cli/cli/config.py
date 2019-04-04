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
            yaml_data = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            for t in yaml_data.keys():
                # Add generated extra keys that not available in the YAML
                yaml_data[t]['files'] = config.get_connector_files(config.get_target_dir(t))
                yaml_data[t]['taps'] = []
                targets[t] = yaml_data[t]

        # Load every tap yaml into targets dictionary
        for yaml_file in tap_yamls:
            config.logger.info("LOADING TAP: {}".format(yaml_file))
            yaml_data = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            for t in yaml_data.keys():
                # Add generated extra keys that not available directly in YAML
                yaml_data[t]['files'] = config.get_connector_files(config.get_tap_dir(yaml_data[t].get('target'), yaml_data[t].get('id')))
                taps[t] = yaml_data[t]

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
                # Add unique server_id to db_conn when tap type is mysql
                if tap.get('type') == 'tap-mysql':
                    extra_config_keys = {'server_id': 900000000 + i}

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
        tap_inheritable_config_path = os.path.join(tap_dir, "inheritable_config.json")
        tap_transformation_path = os.path.join(tap_dir, "transformation.json")
        tap_selection_path = os.path.join(tap_dir, "selection.json")

        # Create tap dir if not exists
        if not os.path.exists(tap_dir):
            os.mkdir(tap_dir)

        # Generate tap config dict: a merged dictionary of db_connection and optional extra_keys
        tap_config = {**tap.get('db_conn'), **extra_config_keys}

        # Generate tap inheritable_config dict
        tap_inheritable_config = utils.delete_empty_keys({
            "batch_size": tap.get('batch_size'),
            "schema": tap.get('target_schema'),
            "dynamic_schema_name": tap.get('dynamic_schema_name'),
            "dynamic_schema_name_postfix": tap.get('dynamic_schema_name_postfix'),
            "hard_delete": tap.get('hard_delete', True),
            "grant_select_to": tap.get('grant_select_to')
        })

        # Generate tap transformation
        transformations = []
        for table in tap.get('tables', []):
            for trans in table.get('transformations', []):
                transformations.append({
                    "stream": table.get('table_name'),
                    "fieldId": trans.get('column'),
                    "type": trans.get('type')
                })
        tap_transformation = {
            "transformations": transformations
        }

        # Generate tap selection
        tap_selection = []
        for table in tap.get('tables', []):
            tap_selection.append(utils.delete_empty_keys({
                "table_name": table.get('table_name'),

                # Default replication_method is LOG_BASED
                "replication_method": table.get('replication_method', 'LOG_BASED'),

                # Add replication_key only if replication_method is INCREMENTAL
                "replication_key": table.get('replication_key') if table.get('replication_method') == 'INCREMENTAL' else None
            }))

        # Save the generated JSON files
        utils.save_json(tap_config, tap_config_path)
        utils.save_json(tap_inheritable_config, tap_inheritable_config_path)
        utils.save_json(tap_transformation, tap_transformation_path)
        utils.save_json(tap_selection, tap_selection_path)
