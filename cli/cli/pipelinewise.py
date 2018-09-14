#!/usr/bin/env python3

import os
from subprocess import Popen, PIPE, STDOUT
import shlex
import sys
import logging
import json

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
            str_format = "%(asctime)s %(processName)s %(levelname)s: %(message)s"
        formatter = logging.Formatter(str_format, "%Y-%m-%d %H:%M:%S")

        # Init stdout handler
        fh = logging.StreamHandler(sys.stdout)
        fh.setFormatter(formatter)

        self.logger.addHandler(fh)

    def __init__(self, args, config_dir):
        self.args = args
        self.__init_logger('TransferData CLI')

        self.config_dir = config_dir
        self.config_path = os.path.join(self.config_dir, "config.json")
        self.load_config()

        self.tap = self.get_tap(args.target, args.tap)
        self.target = self.get_target(args.target)
        
        self.tap_bin = self.get_connector_bin(self.tap["type"])
        self.target_bin = self.get_connector_bin(self.target["type"])

    def is_json(self, string):
        try:
            json_object = json.loads(string)
        except Exception as exc:
            return False
        return True

    def load_json(self, file):
        try:
            self.logger.debug('Parsing file at {}'.format(file))
            if os.path.isfile(file):
                with open(file) as f:
                    return json.load(f)
            else:
                return None
        except Exception as exc:
            raise Exception("Error parsing {} {}".format(file, exc))

    def save_json(self, data, file):
        try:
            self.logger.info("Saving JSON {}".format(file))
            with open(file, 'w') as f:
                return json.dump(data, f, indent=2, sort_keys=True)
        except Exception as exc:
            raise Exception("Cannot save JSON {} {}".format(file, exc))

    def load_config(self):
        self.logger.debug('Loading config at {}'.format(self.config_path))
        self.config = self.load_json(self.config_path)
        self.venv_dir = os.path.join(os.getcwd(), '../.virtualenvs')

    def get_tap_dir(self, target_id, tap_id):
        return os.path.join(self.config_dir, target_id, tap_id)

    def get_tap_log_dir(self, target_id, tap_id):
        return os.path.join(self.get_tap_dir(target_id, tap_id), 'log')
    
    def get_connector_bin(self, connector_type):
        return os.path.join(self.venv_dir, connector_type, "bin", connector_type)
  
    def get_connector_files(self, connector_dir):
        return {
            'config': os.path.join(connector_dir, 'config.json'),
            'properties': os.path.join(connector_dir, 'properties.json'),
            'state': os.path.join(connector_dir, 'state.json'),
            'transformation': os.path.join(connector_dir, 'transformation.json'),
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

        return target
    
    def get_taps(self, target_id):
        self.logger.debug('Getting taps from {} target'.format(target_id))
        target = self.get_target(target_id)

        try:
            taps = target['taps']
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
        
        # Add target details
        tap['target'] = self.get_target(target_id)

        return tap
    
    def run_command(self, command, polling=False):
        self.logger.info('Running command with polling [{}] : {}'.format(polling, command))

        if polling:
            proc = Popen(shlex.split(command), stdout=PIPE, stderr=STDOUT)
            stdout = ''
            while True:
                line = proc.stdout.readline()
                if proc.poll() is not None:
                    break
                if line:
                    stdout += line.decode('utf-8')
            
            rc = proc.poll()
            if rc != 0:
                self.logger.error(stdout)
                sys.exit(rc)
            
            return [stdout, None]     
        
        else:
            proc = Popen(shlex.split(command), stdout=PIPE, stderr=PIPE)
            x = proc.communicate()
            rc = proc.returncode
            stdout = x[0].decode('utf-8')
            stderr = x[1].decode('utf-8')

            if rc != 0:
              self.logger.error(stderr)
              sys.exit(rc)
            
            return [stdout, stderr]

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
                    # Copy is-new flag from the old stream
                    try:
                        new_schema["streams"][new_stream_idx]["is-new"] = old_stream["is-new"]
                    except Exception:
                        False

                    # Copy selected from the old stream
                    try:
                        new_schema["streams"][new_stream_idx]["metadata"][0]["metadata"]["selected"] = old_stream["metadata"][0]["metadata"]["selected"]
                    except Exception:
                        False

                    # Copy replication method from the old stream
                    try:
                        new_schema["streams"][new_stream_idx]["metadata"][0]["metadata"]["replication-method"] = old_stream["metadata"][0]["metadata"]["replication-method"]
                    except Exception:
                        False

                    # Is this new or modified field?
                    new_fields = new_schema["streams"][new_stream_idx]["schema"]["properties"]
                    old_fields = old_schema["streams"][new_stream_idx]["schema"]["properties"]
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
        result = self.run_command(command, False)

        # Get output and errors from tap
        new_schema, tap_output = result
        self.logger.info("Tap output: {}".format(tap_output))

        # If the connection success then the response needs to be a valid JSON string
        try:
            new_schema = json.loads(new_schema)
        except Exception as exc:
            self.logger.error("Schema discovered by {} ({}) is not a valid JSON.".format(tap_id, tap_type))
            sys.exit(1)

    def discover_tap(self):
        tap_id = self.tap["id"]
        tap_type = self.tap["type"]
        target_id = self.target["id"]
        target_type = self.target["type"]
        old_schema_path = self.tap["files"]["properties"]

        self.logger.info("Discovering {} ({}) tap in {} ({}) target".format(tap_id, tap_type, target_id, target_type))

        # Generate and run the command to run the tap directly
        tap_config = self.tap["files"]["config"]
        command = "{} --config {} --discover".format(self.tap_bin, tap_config)
        result = self.run_command(command, False)

        # Get output and errors from tap
        new_schema, tap_output = result
        self.logger.info("Tap output: {}".format(tap_output))

        # Convert JSON string to object
        try:
            new_schema = json.loads(new_schema)
        except Exception as exc:
            self.logger.error("Schema discovered by {} ({}) is not a valid JSON.".format(tap_id, tap_type))
            sys.exit(1)

        # Merge the old and new schemas and diff changes
        old_schema = self.load_json(old_schema_path)
        if old_schema:
            schema_with_diff = self.merge_schemas(old_schema, new_schema)
        else :
            schema_with_diff = new_schema

        # Save the new catalog into the tap
        try:
            tap_properties_path = self.tap["files"]["properties"]
            self.logger.info("Writing new properties file with changes into {}".format(tap_properties_path))
            self.save_json(schema_with_diff, tap_properties_path)
        except Exception as exc:
            self.logger.error("Cannot save file. {}".format(str(exc)))
            sys.exit(1)

    def run_tap(self):
        self.logger.info("Running {} tap in {} target".format(self.tap, self.target))