#!/usr/bin/env python3

import os
import shutil
import tempfile
import sys
import logging
import json
import copy

from datetime import datetime
from crontab import CronTab, CronSlices
from tabulate import tabulate
from joblib import Parallel, delayed, parallel_backend

from . import utils
from .config import Config


class PipelineWise(object):
    '''...'''

    def __init_logger(self, logger_name, level=logging.INFO):
        self.logger = logging.getLogger(logger_name)
        level = logging.INFO

        if self.args.debug:
            level = logging.DEBUG

        self.logger.setLevel(level)

        # Add file and line number in case of DEBUG level
        if level == logging.DEBUG:
            str_format = "%(asctime)s %(processName)s %(levelname)s %(filename)s (%(lineno)s): %(message)s"
        else:
            str_format = "%(asctime)s %(levelname)s: %(message)s"
        formatter = logging.Formatter(str_format, "%Y-%m-%d %H:%M:%S")

        # Init stdout handler
        fh = logging.StreamHandler(sys.stdout)
        fh.setFormatter(formatter)

        self.logger.addHandler(fh)

    def __init__(self, args, config_dir, venv_dir):
        self.args = args
        self.__init_logger('Pipelinewise CLI')

        self.config_dir = config_dir
        self.venv_dir = venv_dir
        self.pipelinewise_bin = os.path.join(self.venv_dir, "cli", "bin", "pipelinewise")
        self.config_path = os.path.join(self.config_dir, "config.json")
        self.load_config()

        if args.tap != '*':
            self.tap = self.get_tap(args.target, args.tap)
            self.tap_bin = self.get_connector_bin(self.tap["type"])

        if args.target != '*':
            self.target = self.get_target(args.target)
            self.target_bin = self.get_connector_bin(self.target["type"])

        self.tranform_field_bin = self.get_connector_bin("transform-field")


    def create_consumable_target_config(self, target_config, tap_inheritable_config):
        try:
            dictA = utils.load_json(target_config)
            dictB = utils.load_json(tap_inheritable_config)

            # Copy everything from dictB into dictA - Not a real merge
            dictA.update(dictB)

            # Save the new dict as JSON into a temp file
            tempfile_path = tempfile.mkstemp()[1]
            utils.save_json(dictA, tempfile_path)

            return tempfile_path
        except Exception as exc:
            raise Exception("Cannot merge JSON files {} {} - {}".format(dictA, dictB, exc))


    def create_filtered_tap_properties(self, tap_type, tap_properties, tap_state, filters, create_fallback=False):
        """
        Create a filtered version of tap properties file based on specific filter conditions.

        Return values:
            1) A temporary JSON file where only those tables are selected to
                sync which meet the filter criterias
            2) List of tap_stream_ids where filter criterias matched
            3) OPTIONAL when create_fallback is True:
                Temporary JSON file with table that don't meet the
                filter criterias
            4) OPTIONAL when create_fallback is True:
                List of tap_stream_ids where filter criteries don't match
        """
        # Get filer conditions with default values from input dictionary
        # Nothing selected by default
        f_selected = filters.get("selected", None)
        f_tap_type = filters.get("tap_type", None)
        f_replication_method = filters.get("replication_method", None)
        f_initial_sync_required = filters.get("initial_sync_required", None)

        # Lists of tables that meet and don't meet the filter criterias
        filtered_tap_stream_ids = []
        fallback_filtered_tap_stream_ids = []

        self.logger.debug("Filtering properties JSON by conditions: {}".format(filters))
        try:
            # Load JSON files
            properties = utils.load_json(tap_properties)
            state = utils.load_json(tap_state)

            # Create a dictionary for tables that don't meet filter criterias
            fallback_properties = copy.deepcopy(properties) if create_fallback else None

            # Foreach every stream (table) in the original properties
            for stream_idx, stream in enumerate(properties.get("streams", tap_properties)):
                selected = False
                replication_method = None
                initial_sync_required = False

                # Collect required properties from the properties file
                tap_stream_id = stream.get("tap_stream_id")
                table_name = stream.get("table_name")
                metadata = stream.get("metadata", [])

                # Collect further properties from the properties file under the metadata key
                table_meta = {}
                for meta_idx, meta in enumerate(metadata):
                    if type(meta) == dict and len(meta.get("breadcrumb", [])) == 0:
                        table_meta = meta.get("metadata")
                        break

                #table_meta = next((i for i in metadata if type(i) == dict and len(i.get("breadcrumb", [])) == 0), {}).get("metadata")
                selected = table_meta.get("selected")
                replication_method = table_meta.get("replication-method")

                # Detect if initial sync is required. Look into the state file, get the bookmark
                # for the current stream (table) and if valid bookmark doesn't exist then
                # initial sync is required
                bookmarks = state.get("bookmarks", {}) if type(state) == dict else {}
                stream_bookmark = bookmarks.get(tap_stream_id, {})
                if (
                    # Initial sync is required for INCREMENTAL and LOG_BASED tables
                    # where the state file has no valid bookmark.
                    #
                    # Valid bookmark keys:
                    #   'replication_key_value' key created for INCREMENTAL tables
                    #   'log_pos' key created by MySQL LOG_BASED tables
                    #    TODO: Add key for Postgres logical replication
                    #
                    # FULL_TABLE replication method is taken as initial sync required
                    replication_method == 'FULL_TABLE' or
                    (
                        (replication_method in ['INCREMENTAL', 'LOG_BASED']) and
                        (not ('replication_key_value' in stream_bookmark or 'log_pos' in stream_bookmark))
                    )
                   ):
                    initial_sync_required = True

                # Compare actual values to the filter conditions.
                # Set the "selected" key to True if actual values meet the filter criterias
                # Set the "selected" key to False if the actual values don't meet the filter criterias
                if (
                    (f_selected == None or selected == f_selected) and
                    (f_tap_type == None or tap_type in f_tap_type) and
                    (f_replication_method == None or replication_method in f_replication_method) and
                    (f_initial_sync_required == None or initial_sync_required == f_initial_sync_required)
                   ):
                    self.logger.debug("""Filter condition(s) matched:
                        Table              : {}
                        Tap Stream ID      : {}
                        Selected           : {}
                        Replication Method : {}
                        Init Sync Required : {}
                    """.format(table_name, tap_stream_id, initial_sync_required, selected, replication_method))

                    # Filter condition matched: mark table as selected to sync
                    properties["streams"][stream_idx]["metadata"][meta_idx]["metadata"]["selected"] = True
                    filtered_tap_stream_ids.append(tap_stream_id)

                    # Filter ocndition matched: mark table as not selected to sync in the fallback properties
                    if create_fallback:
                        fallback_properties["streams"][stream_idx]["metadata"][meta_idx]["metadata"]["selected"] = False
                else:
                    # Filter condition didn't match: mark table as not selected to sync
                    properties["streams"][stream_idx]["metadata"][meta_idx]["metadata"]["selected"] = False

                    # Filter condition didn't match: mark table as selected to sync in the fallback properties
                    # Fallback only if the table is selected in the original properties
                    if create_fallback and selected == True:
                        fallback_properties["streams"][stream_idx]["metadata"][meta_idx]["metadata"]["selected"] = True
                        fallback_filtered_tap_stream_ids.append(tap_stream_id)


            # Save the generated properties file(s) and return
            # Fallback required: Save filtered and fallback properties JSON
            if create_fallback:
                # Save to files: filtered and fallback properties
                temp_properties_path = tempfile.mkstemp()[1]
                utils.save_json(properties, temp_properties_path)

                temp_fallback_properties_path = tempfile.mkstemp()[1]
                utils.save_json(fallback_properties, temp_fallback_properties_path)

                return temp_properties_path, filtered_tap_stream_ids, temp_fallback_properties_path, fallback_filtered_tap_stream_ids

            # Fallback not required: Save only the filtered properties JSON
            else:
                # Save eed to save
                temp_properties_path = tempfile.mkstemp()[1]
                utils.save_json(properties, temp_properties_path)

                return temp_properties_path, filtered_tap_stream_ids

        except Exception as exc:
            raise Exception("Cannot create JSON file - {}".format(exc))


    def load_config(self):
        self.logger.debug('Loading config at {}'.format(self.config_path))
        self.config = utils.load_json(self.config_path)

    def get_tap_dir(self, target_id, tap_id):
        return os.path.join(self.config_dir, target_id, tap_id)

    def get_tap_log_dir(self, target_id, tap_id):
        return os.path.join(self.get_tap_dir(target_id, tap_id), 'log')

    def get_target_dir(self, target_id):
        return os.path.join(self.config_dir, target_id)
    
    def get_connector_bin(self, connector_type):
        return os.path.join(self.venv_dir, connector_type, "bin", connector_type)
  
    def get_connector_files(self, connector_dir):
        return {
            'config': os.path.join(connector_dir, 'config.json'),
            'inheritable_config': os.path.join(connector_dir, 'inheritable_config.json'),
            'properties': os.path.join(connector_dir, 'properties.json'),
            'state': os.path.join(connector_dir, 'state.json'),
            'transformation': os.path.join(connector_dir, 'transformation.json'),
            'selection': os.path.join(connector_dir, 'selection.json'),
        }
        
    def get_targets(self):
        self.logger.debug('Getting targets from {}'.format(self.config_path))
        self.load_config()
        try:
            targets = self.config['targets']
        except Exception as exc:
            raise Exception("Targets not defined")

        return targets

    def get_target(self, target_id):
        self.logger.debug('Getting {} target'.format(target_id))
        targets = self.get_targets()

        target = False
        target = next((item for item in targets if item["id"] == target_id), False)
        
        if target == False:
            raise Exception("Cannot find {} target".format(target_id))

        target_dir = self.get_target_dir(target_id)
        if os.path.isdir(target_dir):
            target['files'] = self.get_connector_files(target_dir)
        else:
            raise Exception("Cannot find target at {}".format(target_dir))

        return target
    
    def get_taps(self, target_id):
        self.logger.debug('Getting taps from {} target'.format(target_id))
        target = self.get_target(target_id)

        try:
            taps = target['taps']

            # Add tap status
            for tap_idx, tap in enumerate(taps):
                taps[tap_idx]['status'] = self.detect_tap_status(target_id, tap["id"])

        except Exception as exc:
            raise Exception("No taps defined for {} target".format(target_id))
        
        return taps
    
    def get_tap(self, target_id, tap_id):
        self.logger.debug('Getting {} tap from target {}'.format(tap_id, target_id))
        taps = self.get_taps(target_id)

        tap = False
        tap = next((item for item in taps if item["id"] == tap_id), False)

        if tap == False:
            raise Exception("Cannot find {} tap in {} target".format(tap_id, target_id))
        
        tap_dir = self.get_tap_dir(target_id, tap_id)
        if os.path.isdir(tap_dir):
            tap['files'] = self.get_connector_files(tap_dir)
        else:
            raise Exception("Cannot find tap at {}".format(tap_dir))
        
        # Add target and status details
        tap['target'] = self.get_target(target_id)
        tap['status'] = self.detect_tap_status(target_id, tap_id)

        return tap
    

    def merge_schemas(self, old_schema, new_schema):
        schema_with_diff = new_schema

        if not old_schema:
            schema_with_diff = new_schema
        else:
            new_streams = new_schema["streams"]
            old_streams = old_schema["streams"]
            for new_stream_idx, new_stream in enumerate(new_streams):
                new_tap_stream_id = new_stream["tap_stream_id"]

                old_stream = False
                old_stream = next((item for item in old_streams if item["tap_stream_id"] == new_tap_stream_id), False)

                # Is this a new stream?
                if not old_stream:
                    new_schema["streams"][new_stream_idx]["is-new"] = True

                # Copy stream selection from the old properties
                else:
                    # Find table specific metadata entries in the old and new streams
                    new_stream_table_mdata_idx = 0
                    old_stream_table_mdata_idx = 0
                    try:
                        new_stream_table_mdata_idx = [i for i, md in enumerate(new_stream["metadata"]) if md["breadcrumb"] == []][0]
                        old_stream_table_mdata_idx = [i for i, md in enumerate(old_stream["metadata"]) if md["breadcrumb"] == []][0]
                    except Exception:
                        False

                    # Copy is-new flag from the old stream
                    try:
                        new_schema["streams"][new_stream_idx]["is-new"] = old_stream["is-new"]
                    except Exception:
                        False

                    # Copy selected from the old stream
                    try:
                        new_schema["streams"][new_stream_idx]["metadata"][new_stream_table_mdata_idx]["metadata"]["selected"] = old_stream["metadata"][old_stream_table_mdata_idx]["metadata"]["selected"]
                    except Exception:
                        False

                    # Copy replication method from the old stream
                    try:
                        new_schema["streams"][new_stream_idx]["metadata"][new_stream_table_mdata_idx]["metadata"]["replication-method"] = old_stream["metadata"][old_stream_table_mdata_idx]["metadata"]["replication-method"]
                    except Exception:
                        False

                    # Copy replication key from the old stream
                    try:
                        new_schema["streams"][new_stream_idx]["metadata"][new_stream_table_mdata_idx]["metadata"]["replication-key"] = old_stream["metadata"][old_stream_table_mdata_idx]["metadata"]["replication-key"]
                    except Exception:
                        False

                    # Is this new or modified field?
                    new_fields = new_schema["streams"][new_stream_idx]["schema"]["properties"]
                    old_fields = old_stream["schema"]["properties"]
                    for new_field_key in new_fields:
                        new_field = new_fields[new_field_key]
                        new_field_mdata_idx = -1

                        # Find new field metadata index
                        for i, mdata in enumerate(new_schema["streams"][new_stream_idx]["metadata"]):
                            if len(mdata["breadcrumb"]) == 2 and mdata["breadcrumb"][0] == "properties" and mdata["breadcrumb"][1] == new_field_key:
                                new_field_mdata_idx = i

                        # Field exists
                        if new_field_key in old_fields.keys():
                            old_field = old_fields[new_field_key]
                            old_field_mdata_idx = -1

                            # Find old field metadata index
                            for i, mdata in enumerate(old_stream["metadata"]):
                                if len(mdata["breadcrumb"]) == 2 and mdata["breadcrumb"][0] == "properties" and mdata["breadcrumb"][1] == new_field_key:
                                    old_field_mdata_idx = i

                            new_mdata = new_schema["streams"][new_stream_idx]["metadata"][new_field_mdata_idx]["metadata"]
                            old_mdata = old_stream["metadata"][old_field_mdata_idx]["metadata"]

                            # Copy is-new flag from the old properties
                            try:
                                new_mdata["is-new"] = old_mdata["is-new"]
                            except Exception:
                                False

                            # Copy is-modified flag from the old properties
                            try:
                                new_mdata["is-modified"] = old_mdata["is-modified"]
                            except Exception:
                                False

                            # Copy field selection from the old properties
                            try:
                                new_mdata["selected"] = old_mdata["selected"]
                            except Exception:
                                False

                            # Field exists and type is the same - Do nothing more in the schema
                            if new_field == old_field:
                                self.logger.debug("Field exists in {} stream with the same type: {} : {}".format(new_tap_stream_id, new_field_key, new_field))

                            # Field exists but types are different - Mark the field as modified in the metadata
                            else:
                                self.logger.debug("Field exists in {} stream but types are different: {} : {}".format(new_tap_stream_id, new_field_key, new_field))
                                try:
                                    new_schema["streams"][new_stream_idx]["metadata"][new_field_mdata_idx]["metadata"]["is-modified"] = True
                                    new_schema["streams"][new_stream_idx]["metadata"][new_field_mdata_idx]["metadata"]["is-new"] = False
                                except Exception:
                                    False

                        # New field - Mark the field as new in the metadata
                        else:
                            self.logger.debug("New field in stream {}: {} : {}".format(new_tap_stream_id, new_field_key, new_field))
                            try:
                                new_schema["streams"][new_stream_idx]["metadata"][new_field_mdata_idx]["metadata"]["is-new"] = True
                            except Exception:
                                False

            schema_with_diff = new_schema

        return schema_with_diff

    def make_default_selection(self, schema, selection_file):
        if os.path.isfile(selection_file):
            self.logger.info("Loading pre defined selection from {}".format(selection_file))
            tap_selection = utils.load_json(selection_file)
            selection = tap_selection["selection"]

            streams = schema["streams"]
            for stream_idx, stream in enumerate(streams):
                table_name = stream.get("table_name") or stream.get("stream")
                table_sel = False
                for sel in selection:
                        if 'table_name' in sel and table_name == sel['table_name']:
                            table_sel = sel

                # Find table specific metadata entries in the old and new streams
                new_stream_table_mdata_idx = 0
                old_stream_table_mdata_idx = 0
                try:
                    stream_table_mdata_idx = [i for i, md in enumerate(stream["metadata"]) if md["breadcrumb"] == []][0]
                except Exception:
                    False

                if table_sel:
                    self.logger.info("Mark {} table as selected with properties {}".format(table_name, table_sel))
                    schema["streams"][stream_idx]["metadata"][stream_table_mdata_idx]["metadata"]["selected"] = True
                    if "replication_method" in table_sel:
                        schema["streams"][stream_idx]["metadata"][stream_table_mdata_idx]["metadata"]["replication-method"] = table_sel["replication_method"]
                    if "replication_key" in table_sel:
                        schema["streams"][stream_idx]["metadata"][stream_table_mdata_idx]["metadata"]["replication-key"] = table_sel["replication_key"]
                else:
                    self.logger.info("Mark {} table as not selected".format(table_name))
                    schema["streams"][stream_idx]["metadata"][stream_table_mdata_idx]["metadata"]["selected"] = False

        return schema


    def test_tap_connection(self):
        tap_id = self.tap["id"]
        tap_type = self.tap["type"]
        target_id = self.target["id"]
        target_type = self.target["type"]

        self.logger.info("Testing {} ({}) tap connection in {} ({}) target".format(tap_id, tap_type, target_id, target_type))

        # Generate and run the command to run the tap directly
        # We will use the discover option to test connection
        tap_config = self.tap["files"]["config"]
        command = "{} --config {} --discover".format(self.tap_bin, tap_config)
        result = utils.run_command(command)

        # Get output and errors from tap
        rc, new_schema, tap_output = result

        if rc != 0:
            self.logger.error("Testing tap connection ({} - {}) FAILED".format(target_id, tap_id))
            sys.exit(1)

        # If the connection success then the response needs to be a valid JSON string
        if not utils.is_json(new_schema):
            self.logger.error("Schema discovered by {} ({}) is not a valid JSON.".format(tap_id, tap_type))
            sys.exit(1)
        else:
            self.logger.info("Testing tap connection ({} - {}) PASSED".format(target_id, tap_id))

    def discover_tap(self, tap=None, target=None):
        # Define tap props
        if tap is None:
            tap_id = self.tap.get('id')
            tap_type = self.tap.get('type')
            tap_config_file = self.tap.get('files', {}).get('config')
            tap_properties_file = self.tap.get('files', {}).get('properties')
            tap_selection_file = self.tap.get('files', {}).get('selection')
            tap_bin = self.tap_bin

        else:
            tap_id = tap.get('id')
            tap_type = tap.get('type')
            tap_config_file = tap.get('files', {}).get('config')
            tap_properties_file = tap.get('files', {}).get('properties')
            tap_selection_file = tap.get('files', {}).get('selection')
            tap_bin = self.get_connector_bin(tap_type)

        # Define target props
        if target is None:
            target_id = self.target.get('id')
            target_type = self.target.get('type')
        else:
            target_id = target.get('id')
            target_type = target.get('type')

        self.logger.info("Discovering {} ({}) tap in {} ({}) target...".format(tap_id, tap_type, target_id, target_type))

        # Generate and run the command to run the tap directly
        command = "{} --config {} --discover".format(tap_bin, tap_config_file)
        result = utils.run_command(command)

        # Get output and errors from tap
        rc, new_schema, output = result

        if rc != 0:
            return "{} - {}".format(target_id, tap_id)

        # Convert JSON string to object
        try:
            new_schema = json.loads(new_schema)
        except Exception as exc:
            return "Schema discovered by {} ({}) is not a valid JSON.".format(tap_id, tap_type)

        # Merge the old and new schemas and diff changes
        old_schema = utils.load_json(tap_properties_file)
        if old_schema:
            schema_with_diff = self.merge_schemas(old_schema, new_schema)
        else :
            schema_with_diff = new_schema

        # Make selection from selectection.json if exists
        try:
            schema_with_diff = self.make_default_selection(schema_with_diff, tap_selection_file)
        except Exception as exc:
            return "Cannot load selection JSON at {}. {}".format(tap_selection_file, str(exc))


        # Save the new catalog into the tap
        try:
            self.logger.info("Writing new properties file with changes into {}".format(tap_properties_file))
            utils.save_json(schema_with_diff, tap_properties_file)
        except Exception as exc:
            return "Cannot save file. {}".format(str(exc))


    def detect_tap_status(self, target_id, tap_id):
        self.logger.debug('Detecting {} tap status in {} target'.format(tap_id, target_id))
        tap_dir = self.get_tap_dir(target_id, tap_id)
        log_dir = self.get_tap_log_dir(target_id, tap_id)
        connector_files = self.get_connector_files(tap_dir)
        status = {
            'currentStatus': 'unknown',
            'lastStatus': 'unknown',
            'lastTimestamp': None
        }

        # Tap exists but configuration not completed
        if not os.path.isfile(connector_files["config"]):
            status["currentStatus"] = "not-configured"

        # Tap exists and has log in running status
        elif os.path.isdir(log_dir) and len(utils.search_files(log_dir, patterns=['*.log.running'])) > 0:
            status["currentStatus"] = "running"

        # Configured and not running
        else:
            status["currentStatus"] = 'ready'

        # Get last run instance
        if os.path.isdir(log_dir):
            log_files = utils.search_files(log_dir, patterns=['*.log.success','*.log.failed'], sort=True)
            if len(log_files) > 0:
                last_log_file = log_files[0]
                log_attr = utils.extract_log_attributes(last_log_file)
                status["lastStatus"] = log_attr["status"]
                status["lastTimestamp"] = log_attr["timestamp"]

        return status

    def show_status(self):
        targets = self.get_targets()

        tab_headers = ['Warehouse ID', 'Source ID', 'Enabled', 'Type', 'Status', 'Last Sync', 'Last Sync Result']
        tab_body = []
        for target in targets:
            taps = self.get_taps(target["id"])

            for tap in taps:
                tab_body.append([
                    target.get('id', '<Unknown>'),
                    tap.get('id', '<Unknown>'),
                    tap.get('enabled', '<Unknown>'),
                    tap.get('type', '<Unknown>'),
                    tap.get('status', {}).get('currentStatus', '<Unknown>'),
                    tap.get('status', {}).get('lastTimestamp', '<Unknown>'),
                    tap.get('status', {}).get('lastStatus', '<Unknown>')
                ])

        print(tabulate(tab_body, headers=tab_headers, tablefmt="simple"))

    def clear_crontab(self):
        self.logger.info("Removing jobs from crontab")

        # Remove every existing pipelinewise entry from crontab
        cron = CronTab(user=True)
        for j in cron.find_command('pipelinewise'):
            cron.remove(j)

        cron.write()

    def init_crontab(self):
        self.logger.info("Initialising crontab")
        self.clear_crontab()

        cron = CronTab(user=True)

        # Find tap schedules and add entries to crontab
        for target in (x for x in self.config["targets"] if "targets" in self.config):
            for tap in (x for x in target["taps"] if "taps" in target):

                if "sync_period" in tap:
                    target_id = target["id"]
                    tap_id = tap["id"]
                    sync_period = tap["sync_period"]
                    command = ' '.join([
                        "{} run_tap --target {} --tap {}".format(self.pipelinewise_bin, target_id, tap_id)
                    ])
                    self.logger.info("Adding: {} {}".format(sync_period, command))

                    if CronSlices.is_valid(sync_period):
                        job = cron.new(command=command)

                        if job.is_valid():
                            job.setall(tap["sync_period"])
                        else:
                            self.logger.info("Cannot schedule job. Target: [{}] Tap [{}]. No changes made.".format(target_id, tap_id))
                            sys.exit(1)

                    else:
                        self.logger.info("Cannot schedule job. Invalid sync period: [{}]. Target: [{}] Tap: [{}]. No changes made.".format(sync_period, target_id, tap_id))
                        sys.exit(1)

        # Every job valid, write crontab
        cron.write()
        self.logger.info("Jobs written to crontab")


    def run_tap_singer(self, tap_type, tap_config, tap_properties, tap_state, tap_transformation, target_config, log_file):
        """
        Generating and running piped shell command to sync tables using singer taps and targets
        """
        new_tap_state = tempfile.mkstemp()[1]

        # Following the singer spec the catalog JSON file needs to be passed by the --catalog argument
        # However some tap (i.e. tap-mysql and tap-postgres) requires it as --properties
        # This is problably for historical reasons and need to clarify on Singer slack channels
        if tap_type == 'tap-mysql':
            tap_catalog_argument = '--properties'
        elif tap_type == 'tap-postgres':
            tap_catalog_argument = '--properties'
        elif tap_type == 'tap-zendesk':
            tap_catalog_argument = '--catalog'
        elif tap_type == 'tap-kafka':
            tap_catalog_argument = '--properties'
        elif tap_type == 'tap-adwords':
            tap_catalog_argument = '--properties'
        else:
            tap_catalog_argument = '--catalog'

        # Add state arugment if exists to extract data incrementally
        tap_state_arg = ""
        if os.path.isfile(tap_state):
            tap_state_arg = "--state {}".format(tap_state)

        # Detect if transformation is needed
        has_transformation = False
        if os.path.isfile(tap_transformation):
            tr = utils.load_json(tap_transformation)
            if 'transformations' in tr and len(tr['transformations']) > 0:
                has_transformation = True

        # Run without transformation in the middle
        if not has_transformation:
            command = ' '.join((
                "  {} --config {} {} {} {}".format(self.tap_bin, tap_config, tap_catalog_argument, tap_properties, tap_state_arg),
                "| {} --config {}".format(self.target_bin, target_config),
                "> {}".format(new_tap_state)
            ))

        # Run with transformation in the middle
        else:
            command = ' '.join((
                "  {} --config {} {} {} {}".format(self.tap_bin, tap_config, tap_catalog_argument, tap_properties, tap_state_arg),
                "| {} --config {}".format(self.tranform_field_bin, tap_transformation),
                "| {} --config {}".format(self.target_bin, target_config),
                "> {}".format(new_tap_state)
            ))

        # Do not run if another instance is already running
        log_dir = os.path.dirname(log_file)
        if os.path.isdir(log_dir) and len(utils.search_files(log_dir, patterns=['*.log.running'])) > 0:
            self.logger.info("Failed to run. Another instance of the same tap is already running. Log file detected in running status at {} ".format(log_dir))
            sys.exit(1)

        # Run command
        result = utils.run_command(command, log_file)

        # Save the new state file if created correctly
        if utils.is_json_file(new_tap_state):
            shutil.copyfile(new_tap_state, tap_state)
            os.remove(new_tap_state)


    def run_tap_fastsync(self, tap_type, target_type, tap_config, tap_properties, tap_state, tap_transformation, target_config, log_file):
        """
        Generating and running shell command to sync tables using the native fastsync components
        """
        fastsync_bin = utils.get_fastsync_bin(self.venv_dir, tap_type, target_type)

        # Add state arugment if exists to extract data incrementally
        tap_transform_arg = ""
        if os.path.isfile(tap_transformation):
            tap_transform_arg = "--transform {}".format(tap_transformation)

        command = ' '.join((
            "  {} ".format(fastsync_bin),
            "--tap {}".format(tap_config),
            "--properties {}".format(tap_properties),
            "--state {}".format(tap_state),
            "--target {}".format(target_config),
            "{}".format(tap_transform_arg),
            "{}".format("--tables {}".format(self.args.tables) if self.args.tables else "")
        ))

        # Do not run if another instance is already running
        log_dir = os.path.dirname(log_file)
        if os.path.isdir(log_dir) and len(utils.search_files(log_dir, patterns=['*.log.running'])) > 0:
            self.logger.info("Failed to run. Another instance of the same tap is already running. Log file detected in running status at {} ".format(log_dir))
            sys.exit(1)

        # Run command
        result = utils.run_command(command, log_file)


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
        tap_id = self.tap["id"]
        tap_type = self.tap["type"]
        target_id = self.target["id"]
        target_type = self.target['type']

        self.logger.info("Running {} tap in {} target".format(tap_id, target_id))

        # Run only if tap enabled
        if not self.tap.get("enabled", False):
            self.logger.info("Tap {} is not enabled. Do nothing and exit normally.".format(self.tap["name"]))
            sys.exit(0)

        # Run only if not running
        tap_status = self.detect_tap_status(target_id, tap_id)
        if tap_status["currentStatus"] == "running":
            self.logger.info("Tap {} is currently running. Do nothing and exit normally.".format(self.tap["name"]))
            sys.exit(0)

        # Generate and run the command to run the tap directly
        tap_config = self.tap["files"]["config"]
        tap_inheritable_config = self.tap["files"]["inheritable_config"]
        tap_properties = self.tap["files"]["properties"]
        tap_state = self.tap["files"]["state"]
        tap_transformation = self.tap["files"]["transformation"]
        target_config = self.target["files"]["config"]

        # Some target attributes can be passed and override by tap (aka. inheritable config)
        # We merge the two configs and use that with the target
        cons_target_config = self.create_consumable_target_config(target_config, tap_inheritable_config)

        # Output will be redirected into target and tap specific log directory
        log_dir = self.get_tap_log_dir(target_id, tap_id)
        current_time = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Create fastsync and singer specific filtered tap properties that contains only
        # the the tables that needs to be synced by the specific command
        (
            tap_properties_fastsync,
            fastsync_stream_ids,
            tap_properties_singer,
            singer_stream_ids
        ) = self.create_filtered_tap_properties(
            tap_type,
            tap_properties,
            tap_state,
            {
                "selected": True,
                "tap_type": ["tap-mysql", "tap-postgres"],
                "initial_sync_required": True
            },
            create_fallback = True)

        log_file_fastsync = os.path.join(log_dir, "{}-{}-{}.fastsync.log".format(target_id, tap_id, current_time))
        log_file_singer = os.path.join(log_dir, "{}-{}-{}.singer.log".format(target_id, tap_id, current_time))

        try:
            # Run fastsync for FULL_TABLE replication method
            if len(fastsync_stream_ids) > 0:
                self.logger.info("Table(s) selected to sync by fastsync: {}".format(fastsync_stream_ids))
                self.run_tap_fastsync(
                    tap_type,
                    target_type,
                    tap_config,
                    tap_properties_fastsync,
                    tap_state,
                    tap_transformation,
                    cons_target_config,
                    log_file_fastsync
                )
            else:
                self.logger.info("No table available that needs to be sync by fastsync")

            # Run singer tap for INCREMENTAL and LOG_BASED replication methods
            if len(singer_stream_ids) > 0:
                self.logger.info("Table(s) selected to sync by singer: {}".format(singer_stream_ids))
                self.run_tap_singer(
                    tap_type,
                    tap_config,
                    tap_properties_singer,
                    tap_state,
                    tap_transformation,
                    cons_target_config,
                    log_file_singer
                )
            else:
                self.logger.info("No table available that needs to be sync by singer")

        # Delete temp files if there is any
        except utils.RunCommandException as exc:
            self.logger.error(exc)
            utils.silentremove(cons_target_config)
            utils.silentremove(tap_properties_fastsync)
            utils.silentremove(tap_properties_singer)
            sys.exit(1)
        except Exception as exc:
            utils.silentremove(cons_target_config)
            utils.silentremove(tap_properties_fastsync)
            utils.silentremove(tap_properties_singer)
            raise exc

        utils.silentremove(cons_target_config)
        utils.silentremove(tap_properties_fastsync)
        utils.silentremove(tap_properties_singer)


    def sync_tables(self):
        """
        Sync every or a list of selected tables from a specific tap.

        The function is using the fastsync components hence it's only
        available for taps and targets where the native and optimised
        fastsync component is implemented.
        """
        tap_id = self.tap["id"]
        tap_type = self.tap["type"]
        target_id = self.target["id"]
        target_type = self.target['type']
        fastsync_bin = utils.get_fastsync_bin(self.venv_dir, tap_type, target_type)

        self.logger.info("Syncing tables from {} ({}) to {} ({})...".format(tap_id, tap_type, target_id, target_type))

        # Run only if tap enabled
        if not self.tap.get("enabled", False):
            self.logger.info("Tap {} is not enabled. Do nothing and exit normally.".format(self.tap["name"]))
            sys.exit(0)

        # Run only if tap not running
        tap_status = self.detect_tap_status(target_id, tap_id)
        if tap_status["currentStatus"] == "running":
            self.logger.info("Tap {} is currently running and cannot sync. Stop the tap and try again.".format(self.tap["name"]))
            sys.exit(1)

        # Tap exists but configuration not completed
        if not os.path.isfile(fastsync_bin):
            self.logger.error("Table sync function is not implemented from {} datasources to {} type of targets".format(tap_type, target_type))
            sys.exit(1)

        # Generate and run the command to run the tap directly
        tap_config = self.tap["files"]["config"]
        tap_inheritable_config = self.tap["files"]["inheritable_config"]
        tap_properties = self.tap["files"]["properties"]
        tap_state = self.tap["files"]["state"]
        tap_transformation = self.tap["files"]["transformation"]
        target_config = self.target["files"]["config"]

        # Some target attributes can be passed and override by tap (aka. inheritable config)
        # We merge the two configs and use that with the target
        cons_target_config = self.create_consumable_target_config(target_config, tap_inheritable_config)

        # Output will be redirected into target and tap specific log directory
        log_dir = self.get_tap_log_dir(target_id, tap_id)
        current_time = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, "{}-{}-{}.fastsync.log".format(target_id, tap_id, current_time))

        # sync_tables command always using fastsync
        try:
            self.run_tap_fastsync(
                tap_type,
                target_type,
                tap_config,
                tap_properties,
                tap_state,
                tap_transformation,
                cons_target_config,
                log_file
            )

        # Delete temp file if there is any
        except utils.RunCommandException as exc:
            self.logger.error(exc)
            utils.silentremove(cons_target_config)
            sys.exit(1)
        except Exception as exc:
            utils.silentremove(cons_target_config)
            raise exc

        utils.silentremove(cons_target_config)


    def import_config(self):
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
        self.logger.info("ACTIVATING TAP STREAM SELECTIONS...")
        for tk in config.targets.keys():
            target = config.targets.get(tk)
            start_time = datetime.now()
            with parallel_backend('threading', n_jobs=-1):
                # Discover taps in parallel and return the list
                # of exception of the failed ones
                discover_excs = list(filter(None,
                    Parallel(verbose=100)(delayed(self.discover_tap)(
                        tap=tap,
                        target=target
                    ) for (tap) in target.get('taps'))))

            # Log summary
            end_time = datetime.now()
            self.logger.info("""
                -------------------------------------------------------
                IMPORTING YAML CONFIGS FINISHED - TARGET: [{}]
                -------------------------------------------------------
                    Total taps to import           : {}
                    Tables loaded successfully     : {}
                    Taps failed imported           : {}

                    Runtime                        : {}
                -------------------------------------------------------
                """.format(
                    tk,
                    len(target.get('taps')),
                    len(target.get('taps')) - len(discover_excs),
                    str(discover_excs),
                    end_time  - start_time
                ))
            if len(discover_excs) > 0:
                sys.exit(1)

