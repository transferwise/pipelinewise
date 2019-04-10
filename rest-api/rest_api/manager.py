#!/usr/bin/env python3

import os
import shutil
from subprocess import Popen, PIPE, STDOUT
import shlex
import logging
import json
import re
import glob
from datetime import datetime
from crontab import CronTab


class Manager(object):
    '''...'''
    flows = []

    def __init__(self, config_dir, venv_dir, logger):
        self.logger = logger
        self.config_dir = config_dir
        self.venv_dir = venv_dir
        self.pipelinewise_bin = os.path.join(self.venv_dir, "cli", "bin", "pipelinewise")
        self.config_path = os.path.join(self.config_dir, "config.json")
    
    def load_json(self, file):
        try:
            self.logger.info('Parsing file at {}'.format(file))
            if os.path.isfile(file):
                with open(file) as f:
                    return json.load(f)
            else:
                return {}
        except Exception as exc:
            raise Exception("Error parsing {} {}".format(file, exc))

    def save_json(self, data, file):
        try:
            self.logger.info("Saving file {}".format(file))
            with open(file, 'w') as f:
                return json.dump(data, f, indent=2, sort_keys=True)
        except Exception as exc:
            raise Exception("Cannot save to JSON {} {}".format(file, exc))

    def load_config(self):
        self.logger.info('Loading config at {}'.format(self.config_path))

        try:
            # Check if config file exists
            if not os.path.isdir(self.config_dir) or not os.path.isfile(self.config_path):
                self.logger.info('Config file not found. Creating default config at {}'.format(self.config_path))

            # Create config directory if not exists
            if not os.path.isdir(self.config_dir):
                os.makedirs(self.config_dir)

            # Create minimal config file
            if not os.path.isfile(self.config_path):
                self.save_json({ "targets": [] }, self.config_path)

        except Exception as exc:
            raise Exception("Config file not exists and cannot create at {} - Check permission".format(self.config_path))

        self.config = self.load_json(self.config_path)

    def save_config(self):
        self.logger.info('Saving config at {}'.format(self.config_path))
        self.save_json(self.config, self.config_path)

    def init_crontab(self):
        self.logger.info('Initialising crontab')
        command = "{} init_crontab".format(self.pipelinewise_bin)
        result = self.run_command(command)
        return result

    def run_command(self, command, background=False):
        self.logger.debug('Running command [Background Mode: {}] : {}'.format(background, command))

        # Run command synchronously
        # Function will return once the command finished with return code, stdout and stderr
        if not background:
            proc = Popen(shlex.split(command), stdout=PIPE, stderr=PIPE)
            x = proc.communicate()
            rc = proc.returncode
            stdout = x[0].decode('utf-8')
            stderr = x[1].decode('utf-8')

            if rc != 0:
              self.logger.error(stderr)

            return { 'stdout': stdout, 'stderr': stderr, 'returncode': rc }

        # Run the command in the background
        # Function will return immediatedly as success with empty stdout and stderr
        else:
            proc = Popen(shlex.split(command))

            return { 'stdout': None, 'stderr': None, 'returncode': 0 }

    def gen_id_by_name(self, tap_name):
        return re.sub('\(|\)|\[|\]', '__', ''.join(tap_name.strip().split()).lower())

    def get_target_dir(self, target_id):
        return os.path.join(self.config_dir, target_id)

    def get_tap_dir(self, target_id, tap_id):
        return os.path.join(self.config_dir, target_id, tap_id)

    def get_tap_log_dir(self, target_id, tap_id):
        return os.path.join(self.get_tap_dir(target_id, tap_id), 'log')
  
    def get_connector_files(self, connector_dir):
        return {
            'config': os.path.join(connector_dir, 'config.json'),
            'inheritable_config': os.path.join(connector_dir, 'inheritable_config.json'),
            'properties': os.path.join(connector_dir, 'properties.json'),
            'state': os.path.join(connector_dir, 'state.json'),
            'transformation': os.path.join(connector_dir, 'transformation.json'),
        }
    
    def parse_connector_files(self, connector_dir):
        connector_files = self.get_connector_files(connector_dir)
        return {
            'config': self.load_json(connector_files['config']),
            'inheritable_config': self.load_json(connector_files['inheritable_config']),
            'properties': self.load_json(connector_files['properties']),
            'state': self.load_json(connector_files['state']),
            'transformation': self.load_json(connector_files['transformation']),
        }

    def search_files(self, search_dir, patterns=['*'], sort=False):
        files = []
        if os.path.isdir(search_dir):
            # Search files and sort if required
            p_files = []
            for pattern in patterns:
                p_files.extend(filter(os.path.isfile, glob.glob(os.path.join(search_dir, pattern))))
            if sort:
                p_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

            # Cut the whole paths, we only need the filenames
            files = list(map(lambda x: os.path.basename(x), p_files))

        return files

    def detect_target_status(self, target_id):
        self.logger.info('Detecting {} target status'.format(target_id))
        target_dir = self.get_target_dir(target_id)
        connector_files = self.get_connector_files(target_dir)

        # Target exists but configuration not completed
        if not os.path.isfile(connector_files["config"]):
            return "not-configured"

        return 'ready'

    def detect_tap_status(self, target_id, tap_id):
        self.logger.info('Detecting {} tap status in {} target'.format(tap_id, target_id))
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
        elif os.path.isdir(log_dir) and len(self.search_files(log_dir, patterns=['*.log.running'])) > 0:
            status["currentStatus"] = "running"

        # Configured and not running
        else:
            status["currentStatus"] = 'ready'

        # Get last run instance
        if os.path.isdir(log_dir):
            log_files = self.search_files(log_dir, patterns=['*.log.success','*.log.failed'], sort=True)
            if len(log_files) > 0:
                last_log_file = log_files[0]
                log_attr = self.extract_log_attributes(last_log_file)
                status["lastStatus"] = log_attr["status"]
                status["lastTimestamp"] = log_attr["timestamp"]

        return status

    def extract_log_attributes(self, log_file):
        self.logger.info('Extracting attributes from log file {}'.format(log_file))
        target_id = 'unknown'
        tap_id = 'unknown'
        timestamp = datetime.utcfromtimestamp(0).isoformat()
        status = 'unkown'
        sync_engine = 'unknown'

        try:
            # Extract attributes from log file name
            log_attr = re.search('(.*)-(.*)-(.*).log.(.*)', log_file)
            target_id = log_attr.group(1)
            tap_id = log_attr.group(2)

            # Detect timestamp and engine
            # Singer log file format  : target-tap-20181217_150101.log.success
            # Fastsync dump log format: target-tap-20181217_150101.fastsync.log.success
            x = log_attr.group(3).split('.')
            if len(x) == 2 and x[1] == 'fastsync':
                timestamp = datetime.strptime(x[0], '%Y%m%d_%H%M%S').isoformat()
                sync_engine = x[1]
            else:
                timestamp = datetime.strptime(log_attr.group(3), '%Y%m%d_%H%M%S').isoformat()
                sync_engine = 'singer'

            status = log_attr.group(4)

        # Ignore exception when attributes cannot be extracted - Defaults will be used
        except Exception:
            pass

        # Return as a dictionary
        return {
            'filename': log_file,
            'target_id': target_id,
            'tap_id': tap_id,
            'timestamp': timestamp,
            'sync_engine': sync_engine,
            'status': status
        }

    def get_config(self):
        self.load_config()
        return self.config 
    
    def add_target(self, target):
        self.logger.info('Adding target')
        targets = self.get_targets()

        try:
            target_name = target["name"]
            target_type = target["type"]
            target_id = self.gen_id_by_name(target["name"])

            if target_type in ["target-postgres", "target-snowflake"]:
                target_dir = self.get_target_dir(target_id)

                if not os.path.isdir(target_dir):
                    new_tap = { 'id': target_id, 'name': target_name, 'taps': [], 'type': target_type }

                    target = False
                    target = next((item for item in targets if item["id"] == target_id), False)

                    if not target:
                        # Add new tap to config
                        self.config["targets"].append(new_tap)

                        # Create tap dir
                        os.makedirs(target_dir)

                        # Save configuration
                        self.save_config()
                    else:
                        raise Exception("Target already exists in target config: {}".format(target_id))
                else:
                    raise Exception("Target directory already exists: {}".format(target_id))
            else:
                raise Exception("Invalid target type: {}".format(target_type))

            return self.get_target(target_id)
        except Exception as exc:
            raise Exception("Failed to add target. {}".format(exc))

    def get_targets(self):
        self.logger.info('Getting targets from {}'.format(self.config_path))
        self.load_config()
        try:
            targets = self.config['targets']

            # Add target status
            for target_idx, target in enumerate(targets):
                targets[target_idx]['status'] = self.detect_target_status(target["id"])

        except Exception as exc:
            raise Exception("Targets not defined")

        return targets

    def get_target(self, target_id):
        self.logger.info('Getting {} target'.format(target_id))
        targets = self.get_targets()

        target = False
        target = next((item for item in targets if item["id"] == target_id), False)
        
        if target == False:
            raise Exception("Cannot find {} target".format(target_id))

        target_dir = self.get_target_dir(target_id)
        if os.path.isdir(target_dir):
            target['files'] = self.parse_connector_files(target_dir)
        else:
            raise Exception("Cannot find target at {}".format(target_dir))

        # Add target and status details
        target['status'] = self.detect_target_status(target_id)

        return target

    def delete_target(self, target_id):
        self.logger.info('Deleting {} target'.format(target_id))
        target_dir = self.get_target_dir(target_id)

        try:
            # Remove target from config
            new_targets =[]
            for target_idx, target in enumerate(self.config["targets"]):
                if target["id"] != target_id:
                    new_targets.append(target)

            self.config["targets"] = new_targets

            # Delete target dir if exists
            if os.path.isdir(target_dir):
                shutil.rmtree(target_dir)

            # Save configuration
            self.save_config()

            return "Target deleted successfully"
        except Exception as exc:
            raise Exception("Failed to delete {} target. {}".format(target_id, exc))

    def get_target_config(self, target_id):
        self.logger.info('Getting {} target config'.format(target_id))
        target = self.get_target(target_id)
        return target["files"]["config"]

    def update_target_config(self, target_id, target_config):
        self.logger.info('Updating {} target config'.format(target_id))

        try:
            target_dir = self.get_target_dir(target_id)
            target_connector_files = self.get_connector_files(target_dir)
            self.save_json(target_config, target_connector_files['config'])

            return "Target config updated successfully"
        except Exception as exc:
            raise Exception("Failed to update {} target config: {}".format(target_id, exc))

    def get_taps(self, target_id):
        self.logger.info('Getting taps from {} target'.format(target_id))
        target = self.get_target(target_id)

        try:
            taps = target['taps']

            # Add tap status
            for tap_idx, tap in enumerate(taps):
                taps[tap_idx]['status'] = self.detect_tap_status(target_id, tap["id"])

        except Exception as exc:
            raise Exception("No taps defined for {} target. {}".format(target_id, exc))
        
        return taps
    
    def get_tap(self, target_id, tap_id):
        self.logger.info('Getting {} tap from target {}'.format(tap_id, target_id))
        taps = self.get_taps(target_id)

        tap = False
        tap = next((item for item in taps if item["id"] == tap_id), False)

        if tap == False:
            raise Exception("Cannot find {} tap in {} target".format(tap_id, target_id))
        
        tap_dir = self.get_tap_dir(target_id, tap_id)
        if os.path.isdir(tap_dir):
            tap['files'] = self.parse_connector_files(tap_dir)
        else:
            raise Exception("Cannot find tap at {}".format(tap_dir))
        
        # Add target and status details
        tap['target'] = self.get_target(target_id)
        tap['status'] = self.detect_tap_status(target_id, tap_id)

        return tap

    def update_tap(self, target_id, tap_id, params):
        self.logger.info('Updating {} tap in {} target'.format(tap_id, target_id))
        tap = self.get_tap(target_id, tap_id)

        try:
            for target_idx, target in enumerate(self.config["targets"]):
                if target["id"] == target_id:
                    for tap_idx, tap in enumerate(target["taps"]):
                        if tap["id"] == tap_id:
                            update_key = params["update"]["key"]
                            update_value = params["update"]["value"]

                            self.config["targets"][target_idx]["taps"][tap_idx][update_key] = update_value
                            self.save_config()

                            # Update cron schedule if required
                            if update_key == "sync_period":
                                self.init_crontab()

            return "Tap updated successfully"
        except Exception as exc:
            raise Exception("Failed to update {} tap in {} target. Invalid updated parameters: {} - {}".format(tap_id, target_id, params, exc))

    def add_tap(self, target_id, tap):
        self.logger.info('Adding tap to target {}'.format(target_id))
        target = self.get_target(target_id)

        try:
            tap_name = tap["name"]
            tap_type = tap["type"]
            tap_owner = tap["owner"]
            tap_id = self.gen_id_by_name(tap["name"])

            if tap_type in ["tap-postgres", "tap-mysql", "tap-zendesk", "tap-kafka", "tap-adwords", "tap-s3-csv"]:

                tap_dir = self.get_tap_dir(target_id, tap_id)
                if not os.path.isdir(tap_dir):
                    tap_exists = False
                    target_i = -1

                    # Check if the tap already exists
                    for target_idx, target in enumerate(self.config["targets"]):
                        if target["id"] == target_id:
                            target_i = target_idx
                            for tap_idx, tap in enumerate(target["taps"]):
                                if tap["id"] == tap_id:
                                    tap_exists = True

                    if not tap_exists:
                        new_tap = { 'enabled': False, 'id': tap_id, 'name': tap_name, 'type': tap_type, 'owner': tap_owner }

                        # Add new tap to config
                        self.config["targets"][target_i]["taps"].append(new_tap)

                        # Create tap dir
                        os.makedirs(tap_dir)

                        # Save configuration
                        self.save_config()
                    else:
                        raise Exception("Tap already exists in target config: {}".format(tap_id))
                else:
                    raise Exception("Tap directory already exists: {}".format(tap_id))
            else:
                raise Exception("Invalid tap type: {}".format(tap_type))

            return self.get_tap(target_id, tap_id)
        except Exception as exc:
            raise Exception("Failed to add tap to target {}. {}".format(target_id, exc))

    def discover_tap(self, target_id, tap_id):
        self.logger.info('Discovering {} tap from target {}'.format(tap_id, target_id))
        command = "{} discover_tap --target {} --tap {}".format(self.pipelinewise_bin, target_id, tap_id)
        result = self.run_command(command)
        return result

    def run_tap(self, target_id, tap_id):
        self.logger.info('Running {} tap in target {}'.format(tap_id, target_id))
        command = "{} run_tap --target {} --tap {}".format(self.pipelinewise_bin, target_id, tap_id)
        result = self.run_command(command, background=True)
        return result

    def delete_tap(self, target_id, tap_id):
        self.logger.info('Deleting {} tap from target {}'.format(tap_id, target_id))
        tap_dir = self.get_tap_dir(target_id, tap_id)

        try:
            # Find and delete tap from config
            new_taps =[]
            for target_idx, target in enumerate(self.config["targets"]):
                if target["id"] == target_id:
                    new_taps = []
                    for tap in target["taps"]:
                        if tap["id"] != tap_id:
                            new_taps.append(tap)

                    self.config["targets"][target_idx]["taps"] = new_taps

            # Delete tap dir if exists
            if os.path.isdir(tap_dir):
                shutil.rmtree(tap_dir)

            # Save configuration
            self.save_config()

            return "Tap deleted successfully"
        except Exception as exc:
            raise Exception("Failed to delete {} tap from target {}. {}".format(tap_id, target_id, exc))

    def get_tap_config(self, target_id, tap_id):
        self.logger.info('Getting {} tap config from target {}'.format(tap_id, target_id))
        tap = self.get_tap(target_id, tap_id)
        return tap["files"]["config"]

    def update_tap_config(self, target_id, tap_id, tap_config):
        self.logger.info('Updating {} tap config in target {}'.format(tap_id, target_id))

        try:
            tap_dir = self.get_tap_dir(target_id, tap_id)
            tap_connector_files = self.get_connector_files(tap_dir)
            self.save_json(tap_config, tap_connector_files['config'])

            return "Tap config updated successfully"
        except Exception as exc:
            raise Exception("Failed to update {} tap config in {} target: {}".format(tap_id, target_id, exc))

    def get_tap_inheritable_config(self, target_id, tap_id):
        self.logger.info('Getting {} tap inheritable config from target {}'.format(tap_id, target_id))
        tap = self.get_tap(target_id, tap_id)
        print(tap["files"]["inheritable_config"])
        return tap["files"]["inheritable_config"]

    def update_tap_inheritable_config(self, target_id, tap_id, tap_config):
        self.logger.info('Updating {} tap inheritable config in target {}'.format(tap_id, target_id))

        try:
            tap_dir = self.get_tap_dir(target_id, tap_id)
            tap_connector_files = self.get_connector_files(tap_dir)
            self.save_json(tap_config, tap_connector_files['inheritable_config'])

            return "Tap config updated successfully"
        except Exception as exc:
            raise Exception("Failed to update {} tap inheritable config in {} target: {}".format(tap_id, target_id, exc))

    def test_tap_connection(self, target_id, tap_id):
        self.logger.info('Testing {} tap connection in target {}'.format(tap_id, target_id))
        command = "{} test_tap_connection --target {} --tap {}".format(self.pipelinewise_bin, target_id, tap_id)
        result = self.run_command(command)
        self.logger.info(result)
        return result

    def get_streams(self, target_id, tap_id):
        self.logger.info('Getting {} tap streams from {} target'.format(tap_id, target_id))
        tap = self.get_tap(target_id, tap_id)

        try:
            streams = tap['files']['properties']['streams']

            # Add transformations
            for idx, stream in enumerate(streams):
                stream_id = stream.get("table_name") or stream.get("stream")
                if stream_id:
                    transformations = self.get_transformations(target_id, tap_id, stream_id)
                    streams[idx]["transformations"] = transformations

        except Exception as exc:
            raise Exception("Cannot find streams for {} tap in {} target. {}".format(tap_id, target_id, exc))
        
        return streams
    
    def get_stream(self, target_id, tap_id, stream_id):
        self.logger.info('Getting {} stream in {} tap in {} target'.format(stream_id, tap_id, target_id))
        streams = self.get_streams(target_id, tap_id)

        stream = False
        stream = next((item for item in streams if item["tap_stream_id"] == stream_id), False)

        if stream == False:
            raise Exception("Cannot find {} stream in {} tap in {} target".format(stream_id, tap_id, target_id))
        
        return stream

    def update_streams(self, target_id, tap_id, params):
        self.logger.info('Updating every stream in {} tap in {} target'.format(tap_id, target_id))
        streams = self.get_streams(target_id, tap_id)

        for stream in streams:
            stream_id = stream["tap_stream_id"]
            self.update_stream(target_id, tap_id, stream_id, params)

        return "Every tap stream updated successfully"

    def update_stream(self, target_id, tap_id, stream_id, params):
        self.logger.info('Updating {} stream in {} tap in {} target'.format(stream_id, tap_id, target_id))
        stream = self.get_stream(target_id, tap_id, stream_id)
        
        try:
            tap_dir = self.get_tap_dir(target_id, tap_id)
            properties_file = os.path.join(tap_dir, 'properties.json')
            properties = self.load_json(properties_file)
            tap_type = params["tapType"]

            if tap_type in ["tap-postgres", "tap-mysql", "tap-zendesk", "tap-kafka", "tap-adwords", "tap-s3-csv"]:
                streams = properties["streams"]
                
                # Find the stream by stream_id
                for stream_idx, stream in enumerate(streams):
                    if stream["tap_stream_id"] == stream_id:
                        # Find the breadcrumb in metadata that needs to be updated 
                        for idx, mdata in enumerate(stream["metadata"]):
                            if stream["metadata"][idx]["breadcrumb"] == params["breadcrumb"]:
                                # Breadcrumb found, do the update
                                update_key = params["update"]["key"]
                                update_value = params["update"]["value"]

                                # Do only certain updates - SELECT to replicate
                                if (update_key == "selected" and isinstance(update_value, bool)):
                                    stream["metadata"][idx]["metadata"]["selected"] = update_value
                                    # Set default replication method
                                    if stream["metadata"][idx]["breadcrumb"] == []:
                                        if ("replication-method" not in stream["metadata"][idx]["metadata"]
                                                or not stream["metadata"][idx]["metadata"]["replication-method"]):
                                            stream["metadata"][idx]["metadata"]["replication-method"] = "FULL_TABLE"

                                # Do only certain updates - SET replication method
                                elif (update_key == "replication-method"):
                                    stream["metadata"][idx]["metadata"]["replication-method"] = update_value

                                # Do only certain updates - SET replication-key
                                elif (update_key == "replication-key"):
                                    stream["metadata"][idx]["metadata"]["replication-key"] = update_value

                                else:
                                    raise Exception("Unknown method to update")

                        # Save the new stream propertes
                        properties["streams"][stream_idx] = stream
                        self.save_json(properties, properties_file)

                return "Tap stream updated successfully"
            else:
                raise Exception("Not supported tap type {}".format(tap_type))
        except Exception as exc:
            raise Exception("Failed to update {} stream in {} tap in {} target. Invalid updated parameters: {} - {}".format(stream_id, tap_id, target_id, params, exc))


    def get_transformations(self, target_id, tap_id, stream):
        self.logger.info('Getting transformations from {} stream in {} tap in {} target'.format(stream, tap_id, target_id))
        transformations = []

        try:
            tap_dir = self.get_tap_dir(target_id, tap_id)

            if os.path.isdir(tap_dir):
                transformation_file = os.path.join(tap_dir, 'transformation.json')
                transformation = self.load_json(transformation_file)

                # Get only the stream specific transformations
                every_transformation = transformation.get("transformations", [])
                for t in every_transformation:
                    if t["targetId"] == target_id and t["tapId"] == tap_id and t["stream"] == stream:
                        transformations.append(t)

        except Exception as exc:
            raise Exception("Cannot find transformations for {} stream in {} tap in {} target. {}".format(stream, tap_id, target_id, exc))

        return transformations

    def update_transformation(self, target_id, tap_id, stream, field_id, params):
        self.logger.info('Updating {} field transformation in {} stream in {} tap in {} target'.format(field_id, stream, tap_id, target_id))
        tap_dir = self.get_tap_dir(target_id, tap_id)

        if os.path.isdir(tap_dir):
            transformation_file = os.path.join(tap_dir, 'transformation.json')
            transformation = self.load_json(transformation_file)
            transformations = transformation.get("transformations", [])

            try:
                transformation_type = params["type"]

                if any(transformation_type in t for t in [
                    "HASH",
                    "HASH-SKIP-FIRST-1",
                    "HASH-SKIP-FIRST-2",
                    "HASH-SKIP-FIRST-3",
                    "HASH-SKIP-FIRST-4",
                    "HASH-SKIP-FIRST-5",
                    "HASH-SKIP-FIRST-6",
                    "HASH-SKIP-FIRST-7",
                    "HASH-SKIP-FIRST-8",
                    "HASH-SKIP-FIRST-9",
                    "SET-NULL",
                    "MASK-DATE",
                    "MASK-NUMBER"
                    ]):
                    # Delete the previous transformation on this field if exists
                    transformations = [t for t in transformations if not (t["targetId"] == target_id and t["tapId"] == tap_id and t["stream"] == stream and t["fieldId"] == field_id)]

                    # Add new transformation
                    transformations.append({ 'targetId': target_id, "tapId": tap_id, "stream": stream, "fieldId": field_id, "type": transformation_type })

                    # Save the new transformation file
                    transformation["transformations"] = transformations
                    self.save_json(transformation, transformation_file)

                elif transformation_type == "STRAIGHT_COPY":
                    cleaned_transformations = [t for t in transformations if not (t["targetId"] == target_id and t["tapId"] == tap_id and t["stream"] == stream and t["fieldId"] == field_id)]

                    # Save the new transformation file
                    transformation["transformations"] = cleaned_transformations
                    self.save_json(transformation, transformation_file)
                else:
                    raise Exception("Not supported transformation type {}".format(transformation_type))

                # Delete transformation file if empty
                if len(transformation["transformations"]) == 0 and os.path.isfile(transformation_file):
                    os.remove(transformation_file)

            except Exception as exc:
                raise Exception("Failed to update {} field transformation in {} stream in {} tap in {} target. Invalid updated parameters: {} - {}".format(field_id, stream, tap_id, target_id, params, exc))
        else:
            raise Exception("Cannot find tap at {}".format(tap_dir))

    def get_tap_logs(self, target_id, tap_id, patterns=['*.log.*']):
        self.logger.info('Getting {} tap logs from {} target'.format(tap_id, target_id))
        logs = []

        try:
            log_dir = self.get_tap_log_dir(target_id, tap_id)
            log_files = self.search_files(log_dir, patterns=patterns)
            for log_file in log_files:
                logs.append(self.extract_log_attributes(log_file))

        except Exception as exc:
            raise Exception("Cannot find logs for {} tap in {} target. {}".format(tap_id, target_id, exc))

        return logs

    def get_tap_log(self, target_id, tap_id, log_id):
        self.logger.info('Getting {} tap log from {} tap in {} target'.format(log_id, target_id, tap_id))
        log_content = '_EMPTY_FILE_'
        try:
            log_file = os.path.join(self.get_tap_log_dir(target_id, tap_id), log_id)
            log = open(log_file, 'r')
            log_content = log.read()
            log.close()
        except Exception as exc:
            raise Exception("Error reading log file. {}".format(exc))
    
        return log_content

    def get_tap_lag(self, target_id, tap_id):
        self.logger.info('Getting {} tap lag in {} target by finding when the last log file was modified'.format(tap_id, target_id))

        try:
            # Find the most recent not failed log file
            tap_logs = self.get_tap_logs(target_id, tap_id, patterns=['*.log.success'])
            last_log = max([x['filename'] for x in tap_logs])
            last_log_file = os.path.join(self.get_tap_log_dir(target_id, tap_id), last_log)
            log_attrs = self.extract_log_attributes(last_log_file)

            # Get the time of last modification and calculate the difference to the current time
            last_modif_time = os.stat(last_log_file).st_mtime
            epoch = datetime.now().timestamp()
            lag = epoch - last_modif_time
            sync_engine = self.extract_log_attributes(last_log_file).get('sync_engine', 'unknown')

            return {
                'lag': lag,
                'sync_engine': sync_engine
            }
        except Exception as exc:
            return {
                'lag': None,
                'sync_engine': 'unknown'
            }


    def get_tap_lags(self):
        self.logger.info('Getting metrics')
        targets = self.get_targets()
        metrics = []

        for target in targets:
            target_id = target.get('id', 'unknown')
            taps_detailed = self.get_taps(target_id)
            for tap in target['taps']:
                tap_id = tap.get('id', 'unknown')
                tap_type = tap.get('type')
                tap_lag = self.get_tap_lag(target_id, tap_id)
                lag = tap_lag.get('lag')
                sync_engine = tap_lag.get('sync_engine', 'unknown')
                tap_detailed = self.get_tap(target_id, tap_id)
                tap_current_status = tap_detailed.get('status', {}).get('currentStatus', 'unknown')
                tap_last_status = tap_detailed.get('status', {}).get('lastStatus', 'unknown')

                # Add status metric in prometheus exporter compatible format
                metric_name = "etl_state{{tap=\"{}\",target=\"{}\",type=\"{}\",engine=\"{}\",current=\"{}\",last=\"{}\"}}".format(tap_id.replace('-','_'), target_id.replace('-','_'), tap_type, sync_engine, tap_current_status, tap_last_status)
                metric = "{} 1".format(metric_name)
                metrics.append(metric)

                # Add lag metric in prometheus exporter compatible format
                if lag:
                    metric_name = "etl_lag_seconds{{tap=\"{}\",target=\"{}\",type=\"{}\",engine=\"{}\"}}".format(tap_id.replace('-','_'), target_id.replace('-','_'), tap_type, sync_engine)
                    metric = "{} {}".format(metric_name, lag)
                    metrics.append(metric)

        return metrics
