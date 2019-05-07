import os
import sys
import json
import yaml
import errno
import glob
import shlex
import copy
import re
import logging
import jsonschema

from subprocess import Popen, PIPE, STDOUT
from datetime import date, datetime
from collections import MutableMapping
from contextlib import suppress

from ansible.parsing.vault import VaultLib, get_file_vault_secret, is_encrypted_file
from ansible.parsing.yaml.loader import AnsibleLoader
from ansible.parsing.dataloader import DataLoader
from ansible.parsing.yaml.objects import AnsibleVaultEncryptedUnicode
from ansible.utils.unsafe_proxy import AnsibleUnsafe
from ansible.module_utils._text import to_text
from ansible.module_utils.common._collections_compat import Mapping

from . import tap_properties

logger = logging.getLogger('Pipelinewise CLI')


class AnsibleJSONEncoder(json.JSONEncoder):
    '''
    Simple encoder class to deal with JSON encoding of Ansible internal types
    
    This is required to convert YAML files with vault encrypted inline values to
    singer JSON configuration files
    '''
    def default(self, o):
        if isinstance(o, AnsibleVaultEncryptedUnicode):
            # vault object - serialise the decrypted value as a string
            value = str(o)
        elif isinstance(o, Mapping):
            # hostvars and other objects
            value = dict(o)
        elif isinstance(o, (date, datetime)):
            # date object
            value = o.isoformat()
        else:
            # use default encoder
            value = super(AnsibleJSONEncoder, self).default(o)
        return value


class RunCommandException(Exception):
    '''
    '''
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def is_json(string):
    '''
    Detects if a string is a valid json or not
    '''
    try:
        json_object = json.loads(string)
    except Exception as exc:
        return False
    return True


def is_json_file(path):
    '''
    Detects if a file is a valid json file or not
    '''
    try:
        if os.path.isfile(path):
            with open(path) as f:
                if json.load(f):
                    return True
        return False
    except Exception as exc:
        return False


def load_json(path):
    '''
    Deserialise JSON file to python object
    '''
    try:
        logger.debug('Parsing file at {}'.format(path))
        if os.path.isfile(path):
            with open(path) as f:
                return json.load(f)
        else:
            logger.debug("No file at {}".format(path))
            return None
    except Exception as exc:
        raise Exception("Error parsing {} {}".format(path, exc))


def save_json(data, path):
    '''
    Serializes and saves any data structure to JSON files 
    '''
    try:
        logger.debug("Saving JSON {}".format(path))
        with open(path, 'w') as f:
            return json.dump(data, f, cls=AnsibleJSONEncoder, indent=4, sort_keys=True)
    except Exception as exc:
        raise Exception("Cannot save JSON {} {}".format(path, exc))


def load_yaml(yaml_file, vault_secret=None):
    '''
    Load a YAML file into a python dictionary.

    The YAML file can be fully encrypted by Ansible-Vault or can contain
    multiple inline Ansible-Vault encrypted values. Ansible Vault
    encryption is ideal to store passwords or encrypt the entire file
    with sensitive data if required.
    '''
    secret_file = get_file_vault_secret(filename=vault_secret, loader=DataLoader())
    secret_file.load()

    vault = VaultLib()
    vault.secrets = [('default', secret_file)]

    data = None
    with open(yaml_file, 'r') as stream:
        try:
            if is_encrypted_file(stream):
                file_data = stream.read()
                data = yaml.load(vault.decrypt(file_data, None))
            else:
                loader = AnsibleLoader(stream, None, vault.secrets)
                try:
                    data = loader.get_single_data()
                except Exception as exc:
                    logger.critical("Error when loading YAML config at {} {}".format(yaml_file, exc))
                    sys.exit(1)
                finally:
                    loader.dispose()
        except yaml.YAMLError as exc:
            logger.critical("Error when loading YAML config at {} {}".format(yaml_file, exc))
            sys.exit(1)

    return data


def load_schema(name):
    '''
    Load a json schema
    '''
    path = "cli/schemas/{}.json".format(name)
    schema = load_json(path)

    if not schema:
        logger.critical("Cannot load schema at {}".format(path))
        sys.exit(1)

    return schema


def validate(instance, schema):
    '''
    Validate an instance under a given json schema
    '''
    try:
        # Serialise vault encrypted objects to string
        schema_safe_inst = json.loads(json.dumps(instance, cls=AnsibleJSONEncoder))
        jsonschema.validate(instance=schema_safe_inst, schema=schema)
    except Exception as exc:
        logger.critical("Invalid object. {}".format(exc))
        sys.exit(1)


def delete_empty_keys(d):
    '''
    Deleting every key from a dictionary where the values are empty
    '''
    return {k: v for k, v in d.items() if v is not None}


def delete_keys_from_dict(d, keys):
    '''
    Delete specific keys from a nested dictionary
    '''
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [v for v in (delete_keys_from_dict(v, keys) for v in d) if v]
    return {k: v for k, v in ((k, delete_keys_from_dict(v, keys)) for k, v in d.items()) if k not in keys}


def silentremove(path):
    '''
    Deleting file with no error message if the file not exists
    '''
    logger.debug('Removing file at {}'.format(path))
    try:
        os.remove(path)
    except OSError as e:

        # errno.ENOENT = no such file or directory
        if e.errno != errno.ENOENT:
            raise


def search_files(search_dir, patterns=['*'], sort=False):
    '''
    Searching files in a specific directory that match a pattern
    '''
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


def extract_log_attributes(log_file):
    '''
    Extracting common properties from a log file name
    '''
    logger.debug('Extracting attributes from log file {}'.format(log_file))
    target_id = 'unknown'
    tap_id = 'unknown'
    timestamp = datetime.utcfromtimestamp(0).isoformat()
    status = 'unknown'

    try:
        # Extract attributes from log file name
        log_attr = re.search('(.*)-(.*)-(.*).log.(.*)', log_file)
        target_id = log_attr.group(1)
        tap_id = log_attr.group(2)
        timestamp = datetime.strptime(log_attr.group(3), '%Y%m%d_%H%M%S').isoformat()
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
        'status': status
    }


def get_tap_property_value(tap_type, property_key):
    '''
    Get a tap specific property value
    '''
    tap_props = tap_properties.tap_properties
    tap = tap_props.get(tap_type, tap_props.get('DEFAULT', {}))

    return tap.get(property_key)


def get_tap_stream_id(tap_type, database_name, schema_name, table_name):
    '''
    Generate tap_stream_id in the same format as a specific
    tap generating it. They are not consistent.
    '''
    pattern = get_tap_property_value(tap_type, 'tap_stream_id_pattern')

    return pattern \
        .replace("{{database_name}}", "{}".format(database_name)) \
        .replace("{{schema_name}}", "{}".format(schema_name)) \
        .replace("{{table_name}}", "{}".format(table_name))


def get_fastsync_bin(venv_dir, tap_type, target_type):
    '''
    Get the absolute path of a fastsync executable
    '''
    source = tap_type.replace('tap-', '')
    target = target_type.replace('target-', '')
    fastsync_name = "{}-to-{}".format(source, target)

    return os.path.join(venv_dir, fastsync_name, "bin", fastsync_name)


def run_command(command, log_file=False):
    '''
    Runs a shell command with or without log file with STDOUT and STDERR
    '''
    piped_command = "/bin/bash -o pipefail -c '{}'".format(command)
    logger.debug('Running command: {}'.format(piped_command))

    # Logfile is needed: Continuously polling STDOUT and STDERR and writing into a log file
    # Once the command finished STDERR redirects to STDOUT and returns _only_ STDOUT
    if log_file:
        logger.info('Writing output into {}'.format(log_file))

        # Create log dir if not exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        # Status embedded in the log file name
        log_file_running = "{}.running".format(log_file)
        log_file_failed = "{}.failed".format(log_file)
        log_file_success = "{}.success".format(log_file)

        # Start command
        proc = Popen(shlex.split(piped_command), stdout=PIPE, stderr=STDOUT)
        f = open("{}".format(log_file_running), "w+")
        stdout = ''
        while True:
            line = proc.stdout.readline()
            if proc.poll() is not None:
                break
            if line:
                decoded_line = line.decode('utf-8')
                stdout += decoded_line
                f.write(decoded_line)
                f.flush()

        f.close()
        rc = proc.poll()
        if rc != 0:
            # Add failed status to the log file name
            os.rename(log_file_running, log_file_failed)

            # Raise run command exception
            raise RunCommandException("Command failed. Return code: {}".format(rc))
        else:
            # Add success status to the log file name
            os.rename(log_file_running, log_file_success)

        return [rc, stdout, None]

    # No logfile needed: STDOUT and STDERR returns in an array once the command finished
    else:
        proc = Popen(shlex.split(piped_command), stdout=PIPE, stderr=PIPE)
        x = proc.communicate()
        rc = proc.returncode
        stdout = x[0].decode('utf-8')
        stderr = x[1].decode('utf-8')

        if rc != 0:
          logger.error(stderr)

        return [rc, stdout, stderr]
