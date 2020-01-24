"""
PipelineWise CLI - Pipelinewise class
"""
import os
import shutil
import signal
import sys
import logging
import json
import copy
import pidfile

from datetime import datetime
from time import time
from typing import Dict

from joblib import Parallel, delayed, parallel_backend
from tabulate import tabulate

from . import utils
from .config import Config


# pylint: disable=too-many-lines,too-many-instance-attributes,too-many-public-methods
class PipelineWise:
    """PipelineWise main Class"""

    INCREMENTAL = 'INCREMENTAL'
    LOG_BASED = 'LOG_BASED'
    FULL_TABLE = 'FULL_TABLE'
    STATUS_SUCCESS = 'SUCCESS'
    STATUS_FAILED = 'FAILED'

    def __init_logger(self, logger_name, log_file=None):
        """
        Initialise logger
        """
        self.logger = logging.getLogger(logger_name)

        # Default log level is less verbose
        level = logging.INFO

        # Increase log level if debug mode needed
        if self.args.debug:
            level = logging.DEBUG

        # Set the log level
        self.logger.setLevel(level)

        # Set log formatter and add file and line number in case of DEBUG level
        if level == logging.DEBUG:
            str_format = '%(asctime)s %(processName)s %(levelname)s %(filename)s (%(lineno)s): %(message)s'
        else:
            str_format = '%(asctime)s %(levelname)s: %(message)s'
        formatter = logging.Formatter(str_format, '%Y-%m-%d %H:%M:%S')

        # Create console handler
        streamhandler = logging.StreamHandler(sys.stdout)
        streamhandler.setLevel(level)
        streamhandler.setFormatter(formatter)
        self.logger.addHandler(streamhandler)

        # Create log file handler if required
        if log_file and log_file != '*':
            filehandler = logging.FileHandler(log_file)
            filehandler.setLevel(level)
            filehandler.setFormatter(formatter)
            self.logger.addHandler(filehandler)

    def __init__(self, args, config_dir, venv_dir):
        self.args = args
        self.__init_logger('Pipelinewise CLI', log_file=args.log)

        self.config_dir = config_dir
        self.venv_dir = venv_dir
        self.extra_log = args.extra_log
        self.pipelinewise_bin = os.path.join(self.venv_dir, 'cli', 'bin', 'pipelinewise')
        self.config_path = os.path.join(self.config_dir, 'config.json')
        self.load_config()

        if args.tap != '*':
            self.tap = self.get_tap(args.target, args.tap)
            self.tap_bin = self.get_connector_bin(self.tap['type'])

        if args.target != '*':
            self.target = self.get_target(args.target)
            self.target_bin = self.get_connector_bin(self.target['type'])

        self.tranform_field_bin = self.get_connector_bin('transform-field')
        self.tap_run_log_file = None

        # Catch SIGINT and SIGTERM to exit gracefully
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, self._exit_gracefully)

    def create_consumable_target_config(self, target_config, tap_inheritable_config):
        """
        Create consumable target config by appending "inheritable" config to the common target config
        """
        dict_a, dict_b = {}, {}
        try:
            dict_a = utils.load_json(target_config)
            dict_b = utils.load_json(tap_inheritable_config)

            # Copy everything from dictB into dictA - Not a real merge
            dict_a.update(dict_b)

            # Save the new dict as JSON into a temp file
            tempfile_path = utils.create_temp_file(dir=self.get_temp_dir(),
                                                   prefix='target_config_',
                                                   suffix='.json')[1]
            utils.save_json(dict_a, tempfile_path)

            return tempfile_path
        except Exception as exc:
            raise Exception(f'Cannot merge JSON files {dict_a} {dict_b} - {exc}')

    # pylint: disable=too-many-statements,too-many-branches,too-many-nested-blocks,too-many-locals,too-many-arguments
    def create_filtered_tap_properties(self, target_type, tap_type, tap_properties, tap_state, filters,
                                       create_fallback=False):
        """
        Create a filtered version of tap properties file based on specific filter conditions.

        Return values:
            1) A temporary JSON file where only those tables are selected to
                sync which meet the filter criteria
            2) List of tap_stream_ids where filter criteria matched
            3) OPTIONAL when create_fallback is True:
                Temporary JSON file with table that don't meet the
                filter criteria
            4) OPTIONAL when create_fallback is True:
                List of tap_stream_ids where filter criteria don't match
        """
        # Get filter conditions with default values from input dictionary
        # Nothing selected by default
        f_selected = filters.get('selected', None)
        f_target_type = filters.get('target_type', None)
        f_tap_type = filters.get('tap_type', None)
        f_replication_method = filters.get('replication_method', None)
        f_initial_sync_required = filters.get('initial_sync_required', None)

        # Lists of tables that meet and don't meet the filter criteria
        filtered_tap_stream_ids = []
        fallback_filtered_stream_ids = []

        self.logger.debug('Filtering properties JSON by conditions: %s', filters)
        try:
            # Load JSON files
            properties = utils.load_json(tap_properties)
            state = utils.load_json(tap_state)

            # Create a dictionary for tables that don't meet filter criteria
            fallback_properties = copy.deepcopy(properties) if create_fallback else {}

            # Foreach stream (table) in the original properties
            for stream_idx, stream in enumerate(properties.get('streams', tap_properties)):
                initial_sync_required = False

                # Collect required properties from the properties file
                tap_stream_id = stream.get('tap_stream_id')
                table_name = stream.get('table_name')
                metadata = stream.get('metadata', [])

                # Collect further properties from the properties file under the metadata key
                table_meta = {}
                meta_idx = 0
                for meta_idx, meta in enumerate(metadata):
                    if isinstance(meta, dict) and len(meta.get('breadcrumb', [])) == 0:
                        table_meta = meta.get('metadata')
                        break

                # Can we make sure that the stream has the right metadata?
                # To be safe, check if no right metadata has been found, then throw an exception.
                if not table_meta:
                    self.logger.error('Stream %s has no metadata with no breadcrumbs: %s.', tap_stream_id, metadata)
                    raise Exception(f'Missing metadata in stream {tap_stream_id}')

                selected = table_meta.get('selected', False)
                replication_method = table_meta.get('replication-method', None)

                # Detect if initial sync is required. Look into the state file, get the bookmark
                # for the current stream (table) and if valid bookmark doesn't exist then
                # initial sync is required
                bookmarks = state.get('bookmarks', {}) if isinstance(state, dict) else {}

                new_stream = False

                # if stream not in bookmarks, then it's a new table
                if tap_stream_id not in bookmarks:
                    new_stream = True
                    initial_sync_required = True
                else:
                    stream_bookmark = bookmarks[tap_stream_id]

                    if self._is_initial_sync_required(replication_method, stream_bookmark):
                        initial_sync_required = True

                # Compare actual values to the filter conditions.
                # Set the "selected" key to True if actual values meet the filter criteria
                # Set the "selected" key to False if the actual values don't meet the filter criteria
                # pylint: disable=too-many-boolean-expressions,bad-continuation
                if (
                        (f_selected is None or selected == f_selected) and
                        (f_target_type is None or target_type in f_target_type) and
                        (f_tap_type is None or tap_type in f_tap_type) and
                        (f_replication_method is None or replication_method in f_replication_method) and
                        (f_initial_sync_required is None or initial_sync_required == f_initial_sync_required)
                ):
                    self.logger.debug("""Filter condition(s) matched:
                        Table              : %s
                        Tap Stream ID      : %s
                        Selected           : %s
                        Replication Method : %s
                        Init Sync Required : %s
                    """, table_name, tap_stream_id, selected, replication_method, initial_sync_required)

                    # Filter condition matched: mark table as selected to sync
                    properties['streams'][stream_idx]['metadata'][meta_idx]['metadata']['selected'] = True
                    filtered_tap_stream_ids.append(tap_stream_id)

                    # Filter condition matched:
                    # if the stream is a new table and is a singer stream, then mark it as selected to sync in the
                    # the fallback properties as well if the table is selected in the original properties.
                    # Otherwise, mark it as not selected
                    if create_fallback:
                        if new_stream and replication_method in [self.INCREMENTAL, self.LOG_BASED]:
                            fallback_properties['streams'][stream_idx]['metadata'][meta_idx]['metadata'][
                                'selected'] = True
                            if selected:
                                fallback_filtered_stream_ids.append(tap_stream_id)
                        else:
                            fallback_properties['streams'][stream_idx]['metadata'][meta_idx]['metadata'][
                                'selected'] = False
                else:
                    # Filter condition didn't match: mark table as not selected to sync
                    properties['streams'][stream_idx]['metadata'][meta_idx]['metadata']['selected'] = False

                    # Filter condition didn't match: mark table as selected to sync in the fallback properties
                    # Fallback only if the table is selected in the original properties
                    if create_fallback and selected is True:
                        fallback_properties['streams'][stream_idx]['metadata'][meta_idx]['metadata']['selected'] = True
                        fallback_filtered_stream_ids.append(tap_stream_id)

            # Save the generated properties file(s) and return
            # Fallback required: Save filtered and fallback properties JSON
            if create_fallback:
                # Save to files: filtered and fallback properties
                temp_properties_path = utils.create_temp_file(dir=self.get_temp_dir(),
                                                              prefix='properties_',
                                                              suffix='.json')[1]
                utils.save_json(properties, temp_properties_path)

                temp_fallback_properties_path = utils.create_temp_file(dir=self.get_temp_dir(),
                                                                       prefix='properties_',
                                                                       suffix='.json')[1]
                utils.save_json(fallback_properties, temp_fallback_properties_path)

                return temp_properties_path, \
                       filtered_tap_stream_ids, \
                       temp_fallback_properties_path, \
                       fallback_filtered_stream_ids

            # Fallback not required: Save only the filtered properties JSON
            temp_properties_path = utils.create_temp_file(dir=self.get_temp_dir(),
                                                          prefix='properties_',
                                                          suffix='.json')[1]
            utils.save_json(properties, temp_properties_path)

            return temp_properties_path, filtered_tap_stream_ids

        except Exception as exc:
            raise Exception(f'Cannot create JSON file - {exc}')

    def load_config(self):
        """
        Load configuration
        """
        self.logger.debug('Loading config at %s', self.config_path)
        config = utils.load_json(self.config_path)

        if config:
            self.config = config
        else:
            self.config = {}

    def get_temp_dir(self):
        """
        Returns the tap specific temp directory
        """
        return os.path.join(self.config_dir, 'tmp')

    def get_tap_dir(self, target_id, tap_id):
        """
        Get absolute path of a tap directory
        """
        return os.path.join(self.config_dir, target_id, tap_id)

    def get_tap_log_dir(self, target_id, tap_id):
        """
        Get absolute path of a tap log directory
        """
        return os.path.join(self.get_tap_dir(target_id, tap_id), 'log')

    def get_target_dir(self, target_id):
        """
        Get absolute path of a target directory
        """
        return os.path.join(self.config_dir, target_id)

    def get_connector_bin(self, connector_type):
        """
        Get absolute path of a connector executable
        """
        return os.path.join(self.venv_dir, connector_type, 'bin', connector_type)

    @classmethod
    def get_connector_files(cls, connector_dir):
        """
        Get connector file paths
        """
        return {
            'config': os.path.join(connector_dir, 'config.json'),
            'inheritable_config': os.path.join(connector_dir, 'inheritable_config.json'),
            'properties': os.path.join(connector_dir, 'properties.json'),
            'state': os.path.join(connector_dir, 'state.json'),
            'transformation': os.path.join(connector_dir, 'transformation.json'),
            'selection': os.path.join(connector_dir, 'selection.json'),
            'pidfile': os.path.join(connector_dir, 'pipelinewise.pid')
        }

    def get_targets(self):
        """
        Get every target
        """
        self.logger.debug('Getting targets from %s', self.config_path)
        self.load_config()
        try:
            targets = self.config.get('targets', [])
        except Exception:
            raise Exception('Targets not defined')

        return targets

    def get_target(self, target_id):
        """
        Get target by id
        """
        self.logger.debug('Getting %s target', target_id)
        targets = self.get_targets()

        target = next((item for item in targets if item['id'] == target_id), False)

        if not target:
            raise Exception(f'Cannot find {target_id} target')

        target_dir = self.get_target_dir(target_id)
        if os.path.isdir(target_dir):
            target['files'] = self.get_connector_files(target_dir)
        else:
            raise Exception(f'Cannot find target at {target_dir}')

        return target

    def get_taps(self, target_id):
        """
        Get every tap from a specific target
        """
        self.logger.debug('Getting taps from %s target', target_id)
        target = self.get_target(target_id)

        try:
            taps = target['taps']

            # Add tap status
            for tap_idx, tap in enumerate(taps):
                taps[tap_idx]['status'] = self.detect_tap_status(target_id, tap['id'])

        except Exception:
            raise Exception(f'No taps defined for {target_id} target')

        return taps

    def get_tap(self, target_id, tap_id):
        """
        Get tap by id from a specific target
        """
        self.logger.debug('Getting %s tap from target %s', tap_id, target_id)
        taps = self.get_taps(target_id)

        tap = next((item for item in taps if item['id'] == tap_id), False)

        if not tap:
            raise Exception(f'Cannot find {tap_id} tap in {target_id} target')

        tap_dir = self.get_tap_dir(target_id, tap_id)
        if os.path.isdir(tap_dir):
            tap['files'] = self.get_connector_files(tap_dir)
        else:
            raise Exception(f'Cannot find tap at {tap_dir}')

        # Add target and status details
        tap['target'] = self.get_target(target_id)
        tap['status'] = self.detect_tap_status(target_id, tap_id)

        return tap

    # pylint: disable=too-many-branches,too-many-statements,too-many-nested-blocks,too-many-locals
    def merge_schemas(self, old_schema, new_schema):
        """
        Merge two schemas
        """
        schema_with_diff = new_schema

        if not old_schema:
            schema_with_diff = new_schema
        else:
            new_streams = new_schema['streams']
            old_streams = old_schema['streams']
            for new_stream_idx, new_stream in enumerate(new_streams):
                new_tap_stream_id = new_stream['tap_stream_id']

                old_stream = False
                old_stream = next((item for item in old_streams if item['tap_stream_id'] == new_tap_stream_id), False)

                # Is this a new stream?
                if not old_stream:
                    new_schema['streams'][new_stream_idx]['is-new'] = True

                # Copy stream selection from the old properties
                else:
                    # Find table specific metadata entries in the old and new streams
                    new_stream_table_mdata_idx = 0
                    old_stream_table_mdata_idx = 0
                    try:
                        new_stream_table_mdata_idx = \
                            [i for i, md in enumerate(new_stream['metadata']) if md['breadcrumb'] == []][0]
                        old_stream_table_mdata_idx = \
                            [i for i, md in enumerate(old_stream['metadata']) if md['breadcrumb'] == []][0]
                    except Exception:
                        pass

                    # Copy is-new flag from the old stream
                    try:
                        new_schema['streams'][new_stream_idx]['is-new'] = old_stream['is-new']
                    except Exception:
                        pass

                    # Copy selected from the old stream
                    try:
                        new_schema['streams'][new_stream_idx]['metadata'][new_stream_table_mdata_idx]['metadata'][
                            'selected'] = old_stream['metadata'][old_stream_table_mdata_idx]['metadata']['selected']
                    except Exception:
                        pass

                    # Copy replication method from the old stream
                    try:
                        new_schema['streams'][new_stream_idx]['metadata'][new_stream_table_mdata_idx]['metadata'][
                            'replication-method'] = old_stream['metadata'][old_stream_table_mdata_idx]['metadata'][
                                'replication-method']
                    except Exception:
                        pass

                    # Copy replication key from the old stream
                    try:
                        new_schema['streams'][new_stream_idx]['metadata'][new_stream_table_mdata_idx]['metadata'][
                            'replication-key'] = old_stream['metadata'][old_stream_table_mdata_idx]['metadata'][
                                'replication-key']
                    except Exception:
                        pass

                    # Is this new or modified field?
                    new_fields = new_schema['streams'][new_stream_idx]['schema']['properties']
                    old_fields = old_stream['schema']['properties']
                    for new_field_key in new_fields:
                        new_field = new_fields[new_field_key]
                        new_field_mdata_idx = -1

                        # Find new field metadata index
                        for i, mdata in enumerate(new_schema['streams'][new_stream_idx]['metadata']):
                            if len(mdata['breadcrumb']) == 2 and mdata['breadcrumb'][0] == 'properties' and \
                                    mdata['breadcrumb'][1] == new_field_key:
                                new_field_mdata_idx = i

                        # Field exists
                        if new_field_key in old_fields.keys():
                            old_field = old_fields[new_field_key]
                            old_field_mdata_idx = -1

                            # Find old field metadata index
                            for i, mdata in enumerate(old_stream['metadata']):
                                if len(mdata['breadcrumb']) == 2 and mdata['breadcrumb'][0] == 'properties' and \
                                        mdata['breadcrumb'][1] == new_field_key:
                                    old_field_mdata_idx = i

                            new_mdata = new_schema['streams'][new_stream_idx]['metadata'][new_field_mdata_idx][
                                'metadata']
                            old_mdata = old_stream['metadata'][old_field_mdata_idx]['metadata']

                            # Copy is-new flag from the old properties
                            try:
                                new_mdata['is-new'] = old_mdata['is-new']
                            except Exception:
                                pass

                            # Copy is-modified flag from the old properties
                            try:
                                new_mdata['is-modified'] = old_mdata['is-modified']
                            except Exception:
                                pass

                            # Copy field selection from the old properties
                            try:
                                new_mdata['selected'] = old_mdata['selected']
                            except Exception:
                                pass

                            # Field exists and type is the same - Do nothing more in the schema
                            if new_field == old_field:
                                self.logger.debug('Field exists in %s stream with the same type: %s: %s',
                                                  new_tap_stream_id, new_field_key, new_field)

                            # Field exists but types are different - Mark the field as modified in the metadata
                            else:
                                self.logger.debug('Field exists in %s stream but types are different: %s: %s}',
                                                  new_tap_stream_id, new_field_key, new_field)
                                try:
                                    new_schema['streams'][new_stream_idx]['metadata'][new_field_mdata_idx]['metadata'][
                                        'is-modified'] = True
                                    new_schema['streams'][new_stream_idx]['metadata'][new_field_mdata_idx]['metadata'][
                                        'is-new'] = False
                                except Exception:
                                    pass

                        # New field - Mark the field as new in the metadata
                        else:
                            self.logger.debug('New field in stream %s: %s: %s', new_tap_stream_id, new_field_key,
                                              new_field)
                            try:
                                new_schema['streams'][new_stream_idx]['metadata'][new_field_mdata_idx]['metadata'][
                                    'is-new'] = True
                            except Exception:
                                pass

            schema_with_diff = new_schema

        return schema_with_diff

    def make_default_selection(self, schema, selection_file):
        """
        Select the streams to sync in schema from a selection JSON file
        """
        if os.path.isfile(selection_file):
            self.logger.info('Loading pre defined selection from %s', selection_file)
            tap_selection = utils.load_json(selection_file)
            selection = tap_selection['selection']

            streams = schema['streams']
            for stream_idx, stream in enumerate(streams):
                tap_stream_id = stream.get('tap_stream_id')
                tap_stream_sel = False
                for sel in selection:
                    if 'tap_stream_id' in sel and tap_stream_id == sel['tap_stream_id']:
                        tap_stream_sel = sel

                # Find table specific metadata entries in the old and new streams
                try:
                    stream_table_mdata_idx = [i for i, md in enumerate(stream['metadata']) if md['breadcrumb'] == []][0]
                except Exception:
                    pass

                if tap_stream_sel:
                    self.logger.info('Mark %s tap_stream_id as selected with properties %s', tap_stream_id,
                                     tap_stream_sel)
                    schema['streams'][stream_idx]['metadata'][stream_table_mdata_idx]['metadata']['selected'] = True
                    if 'replication_method' in tap_stream_sel:
                        schema['streams'][stream_idx]['metadata'][stream_table_mdata_idx]['metadata'][
                            'replication-method'] = tap_stream_sel['replication_method']
                    if 'replication_key' in tap_stream_sel:
                        schema['streams'][stream_idx]['metadata'][stream_table_mdata_idx]['metadata'][
                            'replication-key'] = tap_stream_sel['replication_key']
                else:
                    self.logger.info('Mark %s tap_stream_id as not selected', tap_stream_id)
                    schema['streams'][stream_idx]['metadata'][stream_table_mdata_idx]['metadata']['selected'] = False

        return schema

    def init(self):
        """
        Initialise and create a sample project. The project will contain sample YAML configuration for every
        supported tap and target connects.
        """
        self.logger.info('Initialising new project %s...', self.args.name)
        project_dir = os.path.join(os.getcwd(), self.args.name)

        # Create project dir if not exists
        if os.path.exists(project_dir):
            self.logger.error('Directory exists and cannot create new project: %s', self.args.name)
            sys.exit(1)
        else:
            os.mkdir(project_dir)

        for yaml in sorted(utils.get_sample_file_paths()):
            yaml_basename = os.path.basename(yaml)
            dst = os.path.join(project_dir, yaml_basename)

            self.logger.info('  - Creating %s...', yaml_basename)
            shutil.copyfile(yaml, dst)

    def test_tap_connection(self):
        """
        Test the tap connection. It will connect to the data source that is defined in the tap and will return
        success if it’s available.
        """
        tap_id = self.tap['id']
        tap_type = self.tap['type']
        target_id = self.target['id']
        target_type = self.target['type']

        self.logger.info('Testing %s (%s) tap connection in %s (%s) target', tap_id, tap_type, target_id, target_type)

        # Generate and run the command to run the tap directly
        # We will use the discover option to test connection
        tap_config = self.tap['files']['config']
        command = f'{self.tap_bin} --config {tap_config} --discover'
        result = utils.run_command(command)

        # Get output and errors from tap
        # pylint: disable=unused-variable
        returncode, new_schema, tap_output = result

        if returncode != 0:
            self.logger.error('Testing tap connection (%s - %s) FAILED', target_id, tap_id)
            sys.exit(1)

        # If the connection success then the response needs to be a valid JSON string
        if not utils.is_json(new_schema):
            self.logger.error('Schema discovered by %s (%s) is not a valid JSON.', tap_id, tap_type)
            sys.exit(1)
        else:
            self.logger.info('Testing tap connection (%s - %s) PASSED', target_id, tap_id)

    # pylint: disable=too-many-locals,inconsistent-return-statements
    def discover_tap(self, tap=None, target=None):
        """
        Run a specific tap in discovery mode. Discovery mode is connecting to the data source
        and collecting information that is required for running the tap.
        """
        if tap is None:
            tap = self.tap
        if target is None:
            target = self.target

        # Define tap props
        tap_id = tap.get('id')
        tap_type = tap.get('type')
        tap_config_file = tap.get('files', {}).get('config')
        tap_properties_file = tap.get('files', {}).get('properties')
        tap_selection_file = tap.get('files', {}).get('selection')
        tap_bin = self.get_connector_bin(tap_type)

        # Define target props
        target_id = target.get('id')
        target_type = target.get('type')

        self.logger.info('Discovering %s (%s) tap in {%s (%s) target...', tap_id, tap_type, target_id, target_type)

        # Generate and run the command to run the tap directly
        command = f'{tap_bin} --config {tap_config_file} --discover'
        result = utils.run_command(command)

        # Get output and errors from tap
        # pylint: disable=unused-variable
        returncode, new_schema, output = result

        if returncode != 0:
            return f'{target_id} - {tap_id}'

        # Convert JSON string to object
        try:
            new_schema = json.loads(new_schema)
        except Exception as exc:
            return f'Schema discovered by {tap_id} ({tap_type}) is not a valid JSON.'

        # Merge the old and new schemas and diff changes
        old_schema = utils.load_json(tap_properties_file)
        if old_schema:
            schema_with_diff = self.merge_schemas(old_schema, new_schema)
        else:
            schema_with_diff = new_schema

        # Make selection from selection.json if exists
        try:
            schema_with_diff = self.make_default_selection(schema_with_diff, tap_selection_file)
            schema_with_diff = utils.delete_keys_from_dict(
                self.make_default_selection(schema_with_diff, tap_selection_file),

                # Removing multipleOf json schema validations from properties.json,
                # that's causing run time issues
                ['multipleOf'])

        except Exception as exc:
            return f'Cannot load selection JSON at {tap_selection_file}. {str(exc)}'

        # Post import checks
        post_import_errors = self._run_post_import_tap_checks(tap, schema_with_diff, target)
        if len(post_import_errors) > 0:
            return f'Post import tap checks failed in tap {tap_id}: {post_import_errors}'

        # Save the new catalog into the tap
        try:
            self.logger.info('Writing new properties file with changes into %s', tap_properties_file)
            utils.save_json(schema_with_diff, tap_properties_file)
        except Exception as exc:
            return f'Cannot save file. {str(exc)}'

    def detect_tap_status(self, target_id, tap_id):
        """
        Detect status of a tap
        """
        self.logger.debug('Detecting %s tap status in %s target', tap_id, target_id)
        tap_dir = self.get_tap_dir(target_id, tap_id)
        log_dir = self.get_tap_log_dir(target_id, tap_id)
        connector_files = self.get_connector_files(tap_dir)
        status = {
            'currentStatus': 'unknown',
            'lastStatus': 'unknown',
            'lastTimestamp': None
        }

        # Tap exists but configuration not completed
        if not os.path.isfile(connector_files['config']):
            status['currentStatus'] = 'not-configured'

        # Tap exists and has log in running status
        elif os.path.isdir(log_dir) and len(utils.search_files(log_dir, patterns=['*.log.running'])) > 0:
            status['currentStatus'] = 'running'

        # Configured and not running
        else:
            status['currentStatus'] = 'ready'

        # Get last run instance
        if os.path.isdir(log_dir):
            log_files = utils.search_files(log_dir, patterns=['*.log.success', '*.log.failed'], sort=True)
            if len(log_files) > 0:
                last_log_file = log_files[0]
                log_attr = utils.extract_log_attributes(last_log_file)
                status['lastStatus'] = log_attr['status']
                status['lastTimestamp'] = log_attr['timestamp']

        return status

    def status(self):
        """
        Prints a status summary table of every imported pipeline with their tap and target.
        """
        targets = self.get_targets()

        tab_headers = [
            'Tap ID',
            'Tap Type',
            'Target ID',
            'Target Type',
            'Enabled',
            'Status',
            'Last Sync',
            'Last Sync Result'
        ]
        tab_body = []
        pipelines = 0
        for target in targets:
            taps = self.get_taps(target['id'])

            for tap in taps:
                tab_body.append([
                    tap.get('id', '<Unknown>'),
                    tap.get('type', '<Unknown>'),
                    target.get('id', '<Unknown>'),
                    target.get('type', '<Unknown>'),
                    tap.get('enabled', '<Unknown>'),
                    tap.get('status', {}).get('currentStatus', '<Unknown>'),
                    tap.get('status', {}).get('lastTimestamp', '<Unknown>'),
                    tap.get('status', {}).get('lastStatus', '<Unknown>')
                ])
                pipelines += 1

        print(tabulate(tab_body, headers=tab_headers, tablefmt='simple'))
        print(f'{pipelines} pipeline(s)')

    # pylint: disable=too-many-locals,too-many-arguments
    def run_tap_singer(self, tap_type, tap_config, tap_properties, tap_state, tap_transformation, target_config):
        """
        Generating and running piped shell command to sync tables using singer taps and targets
        """
        # Following the singer spec the catalog JSON file needs to be passed by the --catalog argument
        # However some tap (i.e. tap-mysql and tap-postgres) requires it as --properties
        # This is probably for historical reasons and need to clarify on Singer slack channels
        tap_catalog_argument = utils.get_tap_property_by_tap_type(tap_type, 'tap_catalog_argument')

        # Add state argument if exists to extract data incrementally
        tap_state_arg = ''
        if os.path.isfile(tap_state):
            tap_state_arg = f'--state {tap_state}'

        # Detect if transformation is needed
        has_transformation = False
        if os.path.isfile(tap_transformation):
            trans = utils.load_json(tap_transformation)
            if 'transformations' in trans and len(trans['transformations']) > 0:
                has_transformation = True

        # Run without transformation in the middle
        if not has_transformation:
            command = ' '.join(
                (f'  {self.tap_bin} --config {tap_config} {tap_catalog_argument} {tap_properties} {tap_state_arg}',
                 f'| {self.target_bin} --config {target_config}'))

        # Run with transformation in the middle
        else:
            command = ' '.join(
                (f'  {self.tap_bin} --config {tap_config} {tap_catalog_argument} {tap_properties} {tap_state_arg}',
                 f'| {self.tranform_field_bin} --config {tap_transformation}',
                 f'| {self.target_bin} --config {target_config}'))

        # Do not run if another instance is already running
        log_dir = os.path.dirname(self.tap_run_log_file)
        if os.path.isdir(log_dir) and len(utils.search_files(log_dir, patterns=['*.log.running'])) > 0:
            self.logger.info(
                'Failed to run. Another instance of the same tap is already running. '
                'Log file detected in running status at %s', log_dir)
            sys.exit(1)

        start = None
        state = None

        def update_state_file(line: str) -> str:
            # Update state variable with latest state
            if utils.is_state_message(line):
                # if it has been more than 2 seconds since we last updated the state file
                # update it again with newly received state
                nonlocal start, state

                if start is None or time() - start >= 2:
                    with open(tap_state, 'w') as state_file:
                        state_file.write(line)

                    # Update start time to be the current time.
                    start = time()

                # Keep track of state message so that we do one last file update at the end of the run_tap_singer
                # function. This is to avoid the edge case where the last state message and the one before it are
                # less than 2 sec apart.
                state = line

                # update line and return it
                # for better readability in logs
                return 'INFO STATE emitted from target: %s' % line

            return line

        def update_state_file_with_extra_log(line: str) -> str:
            self.logger.info(line.rstrip('\n'))
            return update_state_file(line)

        # Run command with update_state_file as a callback to call for every stdout line
        if self.extra_log:
            utils.run_command(command, self.tap_run_log_file, update_state_file_with_extra_log)
        else:
            utils.run_command(command, self.tap_run_log_file, update_state_file)

        # update the state file one last time to make sure it always has the last state message.
        if state is not None:
            with open(tap_state, 'w') as statefile:
                statefile.write(state)

    # pylint: disable=too-many-arguments
    def run_tap_fastsync(self, tap_type, target_type, tap_config, tap_properties, tap_state, tap_transformation,
                         target_config):
        """
        Generating and running shell command to sync tables using the native fastsync components
        """
        fastsync_bin = utils.get_fastsync_bin(self.venv_dir, tap_type, target_type)

        # Add state argument if exists to extract data incrementally
        tap_transform_arg = ''
        if os.path.isfile(tap_transformation):
            tap_transform_arg = f'--transform {tap_transformation}'

        tables_command = f'--tables {self.args.tables}' if self.args.tables else ''
        command = ' '.join((
            f'  {fastsync_bin} ',
            f'--tap {tap_config}',
            f'--properties {tap_properties}',
            f'--state {tap_state}',
            f'--target {target_config}',
            f'--temp_dir {self.get_temp_dir()}',
            f'{tap_transform_arg}',
            f'{tables_command}'
        ))

        # Do not run if another instance is already running
        log_dir = os.path.dirname(self.tap_run_log_file)
        if os.path.isdir(log_dir) and len(utils.search_files(log_dir, patterns=['*.log.running'])) > 0:
            self.logger.info(
                'Failed to run. Another instance of the same tap is already running. '
                'Log file detected in running status at %s', log_dir)
            sys.exit(1)

        def add_fastsync_output_to_main_logger(line: str) -> str:
            self.logger.info(line.rstrip('\n'))
            return line

        if self.extra_log:
            # Run command and copy fastsync output to main logger
            utils.run_command(command, self.tap_run_log_file, add_fastsync_output_to_main_logger)
        else:
            # Run command
            utils.run_command(command, self.tap_run_log_file)

    # pylint: disable=too-many-statements,too-many-locals
    def run_tap(self):
        """
        Generating command(s) to run tap to sync data from source to target

        The generated commands can use one or multiple commands of:
            1. Fastsync:
                    Native and optimised component to sync table from a
                    specific type of tap into a specific type of target.
                    This command will be used automatically when FULL_TABLE
                    replication method selected or when initial sync is required.

            2. Singer Taps and Targets:
                    Dynamic components following the singer specification to
                    sync tables from multiple sources to multiple targets.
                    This command will be used automatically when INCREMENTAL
                    and LOG_BASED replication method selected. FULL_TABLE
                    replication are not using the singer components because
                    they are too slow to sync large tables.
        """
        tap_id = self.tap['id']
        tap_type = self.tap['type']
        target_id = self.target['id']
        target_type = self.target['type']

        self.logger.info('Running %s tap in %s target', tap_id, target_id)

        # Run only if tap enabled
        if not self.tap.get('enabled', False):
            self.logger.info('Tap %s is not enabled. Do nothing and exit normally.', self.tap['name'])
            sys.exit(0)

        # Run only if not running
        tap_status = self.detect_tap_status(target_id, tap_id)
        if tap_status['currentStatus'] == 'running':
            self.logger.info('Tap %s is currently running. Do nothing and exit normally.', self.tap['name'])
            sys.exit(0)

        # Generate and run the command to run the tap directly
        tap_config = self.tap['files']['config']
        tap_inheritable_config = self.tap['files']['inheritable_config']
        tap_properties = self.tap['files']['properties']
        tap_state = self.tap['files']['state']
        tap_transformation = self.tap['files']['transformation']
        target_config = self.target['files']['config']

        # Some target attributes can be passed and override by tap (aka. inheritable config)
        # We merge the two configs and use that with the target
        cons_target_config = self.create_consumable_target_config(target_config, tap_inheritable_config)

        # Output will be redirected into target and tap specific log directory
        log_dir = self.get_tap_log_dir(target_id, tap_id)
        current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        # Create fastsync and singer specific filtered tap properties that contains only
        # the the tables that needs to be synced by the specific command
        (
            tap_properties_fastsync,
            fastsync_stream_ids,
            tap_properties_singer,
            singer_stream_ids
        ) = self.create_filtered_tap_properties(
            target_type,
            tap_type,
            tap_properties,
            tap_state, {
                'selected': True,
                'target_type': ['target-snowflake', 'target-redshift'],
                'tap_type': ['tap-mysql', 'tap-postgres', 'tap-s3-csv'],
                'initial_sync_required': True
            },
            create_fallback=True)

        start_time = datetime.now()
        try:
            with pidfile.PIDFile(self.tap['files']['pidfile']):
                # Run fastsync for FULL_TABLE replication method
                if len(fastsync_stream_ids) > 0:
                    self.logger.info('Table(s) selected to sync by fastsync: %s', fastsync_stream_ids)
                    self.tap_run_log_file = os.path.join(log_dir, f'{target_id}-{tap_id}-{current_time}.fastsync.log')
                    self.run_tap_fastsync(
                        tap_type,
                        target_type,
                        tap_config,
                        tap_properties_fastsync,
                        tap_state,
                        tap_transformation,
                        cons_target_config
                    )
                else:
                    self.logger.info('No table available that needs to be sync by fastsync')

                # Run singer tap for INCREMENTAL and LOG_BASED replication methods
                if len(singer_stream_ids) > 0:
                    self.logger.info('Table(s) selected to sync by singer: %s', singer_stream_ids)
                    self.tap_run_log_file = os.path.join(log_dir, f'{target_id}-{tap_id}-{current_time}.singer.log')
                    self.run_tap_singer(
                        tap_type,
                        tap_config,
                        tap_properties_singer,
                        tap_state,
                        tap_transformation,
                        cons_target_config
                    )
                else:
                    self.logger.info('No table available that needs to be sync by singer')

        except pidfile.AlreadyRunningError:
            self.logger.error('Another instance of the tap is already running.')
            utils.silentremove(cons_target_config)
            utils.silentremove(tap_properties_fastsync)
            utils.silentremove(tap_properties_singer)
            sys.exit(1)
        # Delete temp files if there is any
        except utils.RunCommandException as exc:
            self.logger.error(exc)
            utils.silentremove(cons_target_config)
            utils.silentremove(tap_properties_fastsync)
            utils.silentremove(tap_properties_singer)
            self._print_tap_run_summary(self.STATUS_FAILED, start_time, datetime.now())
            sys.exit(1)
        except Exception as exc:
            utils.silentremove(cons_target_config)
            utils.silentremove(tap_properties_fastsync)
            utils.silentremove(tap_properties_singer)
            self._print_tap_run_summary(self.STATUS_FAILED, start_time, datetime.now())
            raise exc

        utils.silentremove(cons_target_config)
        utils.silentremove(tap_properties_fastsync)
        utils.silentremove(tap_properties_singer)
        self._print_tap_run_summary(self.STATUS_SUCCESS, start_time, datetime.now())

    def stop_tap(self):
        """
        Stop running tap

        The command finds the tap specific pidfile that was created by run_tap command and sends
        a SIGINT to the process. The SIGINT signal triggers _exit_gracefully function automatically and
        the tap stops running.
        """
        pidfile_path = self.tap['files']['pidfile']
        try:
            with open(pidfile_path) as pidf:
                pid = pidf.read()
                self.logger.info('Sending SIGINT to pid %s...', pid)
                os.kill(int(pid), signal.SIGINT)
        except ProcessLookupError:
            self.logger.error('Pid %s not found. Is the tap running on this machine? '
                              'Stopping taps remotely is not supported.', pid)
            sys.exit(1)
        except FileNotFoundError:
            self.logger.error('No pidfile found at %s. Tap does not seem to be running.', pidfile_path)
            sys.exit(1)

    # pylint: disable=too-many-locals
    def sync_tables(self):
        """
        Sync every or a list of selected tables from a specific tap.
        It performs an initial sync and resets the table bookmarks to their new location.

        The function is using the fastsync components hence it's only
        available for taps and targets where the native and optimised
        fastsync component is implemented.
        """
        tap_id = self.tap['id']
        tap_type = self.tap['type']
        target_id = self.target['id']
        target_type = self.target['type']
        fastsync_bin = utils.get_fastsync_bin(self.venv_dir, tap_type, target_type)

        self.logger.info('Syncing tables from %s (%s) to %s (%s)...', tap_id, tap_type, target_id, target_type)

        # Run only if tap enabled
        if not self.tap.get('enabled', False):
            self.logger.info('Tap %s is not enabled. Do nothing and exit normally.', self.tap['name'])
            sys.exit(0)

        # Run only if tap not running
        tap_status = self.detect_tap_status(target_id, tap_id)
        if tap_status['currentStatus'] == 'running':
            self.logger.info('Tap %s is currently running and cannot sync. Stop the tap and try again.',
                             self.tap['name'])
            sys.exit(1)

        # Tap exists but configuration not completed
        if not os.path.isfile(fastsync_bin):
            self.logger.error('Table sync function is not implemented from %s datasources to %s type of targets',
                              tap_type, target_type)
            sys.exit(1)

        # Generate and run the command to run the tap directly
        tap_config = self.tap['files']['config']
        tap_inheritable_config = self.tap['files']['inheritable_config']
        tap_properties = self.tap['files']['properties']
        tap_state = self.tap['files']['state']
        tap_transformation = self.tap['files']['transformation']
        target_config = self.target['files']['config']

        # Some target attributes can be passed and override by tap (aka. inheritable config)
        # We merge the two configs and use that with the target
        cons_target_config = self.create_consumable_target_config(target_config, tap_inheritable_config)

        # Output will be redirected into target and tap specific log directory
        log_dir = self.get_tap_log_dir(target_id, tap_id)
        current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        # sync_tables command always using fastsync
        try:
            with pidfile.PIDFile(self.tap['files']['pidfile']):
                self.tap_run_log_file = os.path.join(log_dir, f'{target_id}-{tap_id}-{current_time}.fastsync.log')
                self.run_tap_fastsync(
                    tap_type,
                    target_type,
                    tap_config,
                    tap_properties,
                    tap_state,
                    tap_transformation,
                    cons_target_config
                )

        except pidfile.AlreadyRunningError:
            self.logger.error('Another instance of the tap is already running.')
            utils.silentremove(cons_target_config)
            sys.exit(1)
        # Delete temp file if there is any
        except utils.RunCommandException as exc:
            self.logger.error(exc)
            utils.silentremove(cons_target_config)
            sys.exit(1)
        except Exception as exc:
            utils.silentremove(cons_target_config)
            raise exc

        utils.silentremove(cons_target_config)

    def validate(self):
        """
        Validates a project directory with YAML tap and target files.
        """
        yaml_dir = self.args.dir
        self.logger.info('Searching YAML config files in %s', yaml_dir)
        tap_yamls, target_yamls = utils.get_tap_target_names(yaml_dir)
        self.logger.info('Detected taps: %s', tap_yamls)
        self.logger.info('Detected targets: %s', target_yamls)

        target_schema = utils.load_schema('target')
        tap_schema = utils.load_schema('tap')

        vault_secret = self.args.secret

        target_ids = set()
        # Validate target json schemas
        for yaml_file in target_yamls:
            self.logger.info('Started validating %s', yaml_file)
            loaded_yaml = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            utils.validate(loaded_yaml, target_schema)
            target_ids.add(loaded_yaml['id'])
            self.logger.info('Finished validating %s', yaml_file)

        # Validate tap json schemas and check that every tap has valid 'target'
        for yaml_file in tap_yamls:
            self.logger.info('Started validating %s', yaml_file)
            loaded_yaml = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            utils.validate(loaded_yaml, tap_schema)

            if loaded_yaml['target'] not in target_ids:
                self.logger.error("Can'f find the target with the ID '%s' referenced in '%s'. Available target IDs: %s",
                                  loaded_yaml['target'], yaml_file, target_ids)
                sys.exit(1)

            self.logger.info('Finished validating %s', yaml_file)

        self.logger.info('Validation successful')

    def import_project(self):
        """
        Take a list of YAML files from a directory and use it as the source to build
        singer compatible json files and organise them into pipeline directory structure
        """
        # Read the YAML config files and transform/save into singer compatible
        # JSON files in a common directory structure
        config = Config.from_yamls(self.config_dir, self.args.dir, self.args.secret)
        config.save()

        # Activating tap stream selections
        #
        # Run every tap in discovery mode to generate the singer specific
        # properties.json files for the taps. The properties file than
        # updated to replicate only the tables that is defined in the YAML
        # files and to use the required replication methods
        #
        # The tap Discovery mode needs to connect to each source databases and
        # doing that sequentially is slow. For a better performance we do it
        # in parallel.
        self.logger.info('ACTIVATING TAP STREAM SELECTIONS...')
        total_targets = 0
        total_taps = 0
        discover_excs = []

        # Import every tap from every target
        start_time = datetime.now()
        for target in config.targets.values():
            total_targets += 1
            total_taps += len(target.get('taps'))

            with parallel_backend('threading', n_jobs=-1):
                # Discover taps in parallel and return the list
                #  of exception of the failed ones
                discover_excs.extend(list(filter(None,
                                                 Parallel(verbose=100)(delayed(self.discover_tap)(
                                                     tap=tap,
                                                     target=target
                                                 ) for tap in target.get('taps')))))

        # Log summary
        end_time = datetime.now()
        # pylint: disable=logging-too-many-args
        self.logger.info(
            """
            -------------------------------------------------------
            IMPORTING YAML CONFIGS FINISHED
            -------------------------------------------------------
                Total targets to import        : %s
                Total taps to import           : %s
                Taps imported successfully     : %s
                Taps failed to import          : %s
                Runtime                        : %s
            -------------------------------------------------------
            """,
            total_targets,
            total_taps,
            total_taps - len(discover_excs),
            str(discover_excs),
            end_time - start_time
        )
        if len(discover_excs) > 0:
            sys.exit(1)

    def encrypt_string(self):
        """
        Encrypt the supplied string using the provided vault secret
        """
        b_ciphertext = utils.vault_encrypt(self.args.string, self.args.secret)
        yaml_text = utils.vault_format_ciphertext_yaml(b_ciphertext)

        print(yaml_text)
        print('Encryption successful')

    def _is_initial_sync_required(self, replication_method: str, stream_bookmark: Dict) -> bool:
        """
            Detects if a stream needs initial sync or not.
            Initial sync is required for INCREMENTAL and LOG_BASED tables
            where the state file has no valid bookmark.

            Valid bookmark keys:
              'replication_key_value' key created for INCREMENTAL tables
              'log_pos' key created by MySQL LOG_BASED tables
              'lsn' key created by PostgreSQL LOG_BASED tables
              'modified_since' key created by CSV S3 tables

            FULL_TABLE replication method is taken as initial sync required
        :param replication_method: stream replication method
        :param stream_bookmark: stream state bookmark
        :return: Boolean, True if needs initial sync, False otherwise
        """
        return replication_method == self.FULL_TABLE or (
            (replication_method in [self.INCREMENTAL, self.LOG_BASED]) and
            (not ('replication_key_value' in stream_bookmark or
                  'log_pos' in stream_bookmark or
                  'lsn' in stream_bookmark or
                  'modified_since' in stream_bookmark  # this is replication key for tap-s3-csv used by Singer
                  )))

    # pylint: disable=unused-argument
    def _exit_gracefully(self, sig, frame, exit_code=1):
        self.logger.info('Stopping gracefully...')

        # Rename log files from running to terminated status
        if self.tap_run_log_file:
            tap_run_log_file_running = f'{self.tap_run_log_file}.running'
            tap_run_log_file_terminated = f'{self.tap_run_log_file}.terminated'

            if os.path.isfile(tap_run_log_file_running):
                os.rename(tap_run_log_file_running, tap_run_log_file_terminated)

        sys.exit(exit_code)

    def _print_tap_run_summary(self, status, start_time, end_time):
        summary = f"""
-------------------------------------------------------
TAP RUN SUMMARY
-------------------------------------------------------
    Status  : {status}
    Runtime : {end_time - start_time}
-------------------------------------------------------
"""

        # Print summary to stdout
        self.logger.info(summary)

        # Add summary to tap run log file
        if self.tap_run_log_file:
            tap_run_log_file_success = f'{self.tap_run_log_file}.success'
            tap_run_log_file_failed = f'{self.tap_run_log_file}.failed'

            # Find which log file we need to write the summary
            log_file_to_write_summary = None
            if os.path.isfile(tap_run_log_file_success):
                log_file_to_write_summary = tap_run_log_file_success
            elif os.path.isfile(tap_run_log_file_failed):
                log_file_to_write_summary = tap_run_log_file_failed

            # Append the summary to the right log file
            if log_file_to_write_summary:
                with open(log_file_to_write_summary, 'a') as logfile:
                    logfile.write(summary)

    # pylint: disable=unused-variable
    def _run_post_import_tap_checks(self, tap, catalog, target) -> list:
        """
            Run post import checks on a tap properties object.

        :param tap_properties: tap properties object
        :return: List of errors. If no error returns an empty list
        """
        errors = []

        # Foreach stream (table) in the original properties
        for stream_idx, stream in enumerate(catalog.get('streams', catalog)):
            # Collect required properties from the properties file
            tap_stream_id = stream.get('tap_stream_id')
            metadata = stream.get('metadata', [])

            # Collect further properties from the tap and target properties
            table_meta = {}
            for meta_idx, meta in enumerate(metadata):
                if isinstance(meta, dict) and len(meta.get('breadcrumb', [])) == 0:
                    table_meta = meta.get('metadata')
                    break

            selected = table_meta.get('selected', False)
            replication_method = table_meta.get('replication-method')
            table_key_properties = table_meta.get('table-key-properties', [])
            primary_key_required = tap.get('primary_key_required', True)

            # Check if primary key is set for INCREMENTAL and LOG_BASED replications
            if (selected and replication_method in [self.INCREMENTAL, self.LOG_BASED] and
                    len(table_key_properties) == 0 and primary_key_required):
                errors.append(f'No primary key set for {tap_stream_id} stream ({replication_method})')
                break

        return errors
