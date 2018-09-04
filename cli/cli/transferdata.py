#!/usr/bin/env python3

import os
from subprocess import Popen, PIPE, STDOUT
import shlex
import sys
import logging
import json

class TransferData(object):
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
        name = os.path.basename(connector_dir)

        return {
            'config': os.path.join(connector_dir, 'config.json'),
            'properties': os.path.join(connector_dir, 'properties.json'),
            'state': os.path.join(connector_dir, 'state.json'),
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
        self.logger.debug('Running command with polling [{}] : {} with'.format(polling, command))

        if polling:
            proc = Popen(shlex.split(command), stdout=PIPE, stderr=STDOUT)
            stdout = 'xx'
            while True:
                line = proc.stdout.readline()
                if proc.poll() is not None:
                    break
                if line:
                    stdout += line.decode('utf-8')
            
            rc = proc.poll()
            if rc != 0:
                self.logger.warning(stdout)
                sys.exit(rc)
            
            return [stdout, None]     
        
        else:
            proc = Popen(shlex.split(command), stdout=PIPE, stderr=PIPE)
            x = proc.communicate()
            rc = proc.returncode
            stdout = x[0].decode('utf-8')
            stderr = x[1].decode('utf-8')

            if rc != 0:
              self.logger.warning(stderr)
              sys.exit(rc)
            
            return [stdout, stderr]

    def discover_tap(self):
        tap_id = self.tap["id"]
        tap_type = self.tap["type"]
        target_id = self.target["id"]
        target_type = self.target["type"]

        self.logger.info("Discovering {} ({}) tap in {} ({}) target".format(tap_id, tap_type, target_id, target_type))

        tap_config = self.tap["files"]["config"]
        command = "{} --config {} --discover".format(self.tap_bin, tap_config)
        schema = self.run_command(command, False)

        print("OUTPUT: {}".format(schema[0]))
        print("STDERR: {}".format(schema[1]))

    def run_tap(self):
        self.logger.info("Running {} tap in {} target".format(self.tap, self.target))