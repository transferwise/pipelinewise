"""
PipelineWise CLI - Utilities
"""
import errno
import glob
import json
import logging
import os
import re
import secrets
import string
import sys
import tempfile
import warnings

import jsonschema
import yaml

from io import StringIO
from datetime import date, datetime
from jinja2 import Template
from ansible.errors import AnsibleError
from ansible.module_utils._text import to_text
from ansible.module_utils.common._collections_compat import Mapping
from ansible.parsing.dataloader import DataLoader
from ansible.parsing.vault import VaultLib, get_file_vault_secret, is_encrypted_file
from ansible.parsing.yaml.loader import AnsibleLoader
from ansible.parsing.yaml.objects import AnsibleMapping, AnsibleVaultEncryptedUnicode

from . import tap_properties

LOGGER = logging.getLogger(__name__)


class AnsibleJSONEncoder(json.JSONEncoder):
    """
    Simple encoder class to deal with JSON encoding of Ansible internal types

    This is required to convert YAML files with vault encrypted inline values to
    singer JSON configuration files
    """

    # pylint: disable=method-hidden,assignment-from-no-return
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
            value = super().default(o)
        return value


def is_json(stringss):
    """
    Detects if a string is a valid json or not
    """
    try:
        json.loads(stringss)
    except Exception:
        return False
    return True


def is_json_file(path):
    """
    Detects if a file is a valid json file or not
    """
    try:
        if os.path.isfile(path):
            with open(path, encoding='utf-8') as jsonfile:
                if json.load(jsonfile):
                    return True
        return False
    except Exception:
        return False


def load_json(path):
    """
    Deserialize JSON file to python object
    """
    try:
        LOGGER.debug('Parsing file at %s', path)
        if os.path.isfile(path):
            with open(path, encoding='utf-8') as jsonfile:
                return json.load(jsonfile)
        else:
            LOGGER.debug('No file at %s', path)
            return None
    except Exception as exc:
        raise Exception(f'Error parsing {path} {exc}') from exc


def is_state_message(line: str) -> bool:
    """
    Detects if a string is a valid state message
    """
    try:
        json_object = json.loads(line)
        return 'bookmarks' in json_object
    except Exception:
        return False


def save_json(data, path):
    """
    Serializes and saves any data structure to JSON files
    """
    try:
        LOGGER.debug('Saving JSON %s', path)
        with open(path, 'w', encoding='utf-8') as jsonfile:
            return json.dump(
                data, jsonfile, cls=AnsibleJSONEncoder, indent=4, sort_keys=True
            )
    except Exception as exc:
        raise Exception(f'Cannot save JSON {path} {exc}') from exc


def is_yaml(strings):
    """
    Detects if a string is a valid yaml or not
    """
    try:
        yaml.safe_load(strings)
    except Exception:
        return False
    return True


def is_yaml_file(path):
    """
    Detects if a file is a valid yaml file or not
    """
    try:
        if os.path.isfile(path):
            with open(path, encoding='utf-8') as yamlfile:
                if yaml.safe_load(yamlfile):
                    return True
        return False
    except Exception:
        return False


def get_tap_target_names(yaml_dir):
    """Retrieves names of taps and targets inside yaml_dir.

    Args:
        yaml_dir (str): Path to the directory, which contains taps and targets files with .yml extension.

    Returns:
        (tap_yamls, target_yamls): tap_yamls is a list of names inside yaml_dir with "tap_*.y(a)ml" pattern.
                                   target_yamls is a list of names inside yaml_dir with "target_*.y(a)ml" pattern.
    """
    yamls = [
        f
        for f in os.listdir(yaml_dir)
        if os.path.isfile(os.path.join(yaml_dir, f))
        and (f.endswith('.yml') or f.endswith('.yaml'))
    ]
    target_yamls = set(filter(lambda y: y.startswith('target_'), yamls))
    tap_yamls = set(filter(lambda y: y.startswith('tap_'), yamls))

    return tap_yamls, target_yamls


def load_yaml(yaml_file, vault_secret=None):
    """
    Load a YAML file into a python dictionary.

    The YAML file can be fully encrypted by Ansible-Vault or can contain
    multiple inline Ansible-Vault encrypted values. Ansible Vault
    encryption is ideal to store passwords or encrypt the entire file
    with sensitive data if required.
    """
    vault = VaultLib()

    if vault_secret:
        secret_file = get_file_vault_secret(filename=vault_secret, loader=DataLoader())
        secret_file.load()
        vault.secrets = [('default', secret_file)]

    data = None
    if os.path.isfile(yaml_file):
        with open(yaml_file, 'r', encoding='utf-8') as stream:
            # Render environment variables using jinja templates
            contents = stream.read()
            template = Template(contents)
            stream = StringIO(template.render(env_var=os.environ))
            try:
                if is_encrypted_file(stream):
                    file_data = stream.read()
                    data = yaml.safe_load(vault.decrypt(file_data, None))
                else:
                    loader = AnsibleLoader(stream, None, vault.secrets)
                    try:
                        data = loader.get_single_data()
                    except Exception as exc:
                        raise Exception(
                            f'Error when loading YAML config at {yaml_file} {exc}'
                        ) from exc
                    finally:
                        loader.dispose()
            except yaml.YAMLError as exc:
                raise Exception(
                    f'Error when loading YAML config at {yaml_file} {exc}'
                ) from exc
    else:
        LOGGER.debug('No file at %s', yaml_file)

    if isinstance(data, AnsibleMapping):
        data = dict(data)

    return data


def vault_encrypt(plaintext, secret):
    """
    Vault encrypt a piece of data.
    """
    try:
        vault = VaultLib()
        secret_file = get_file_vault_secret(filename=secret, loader=DataLoader())
        secret_file.load()
        vault.secrets = [('default', secret_file)]

        return vault.encrypt(plaintext)
    except AnsibleError as exc:
        LOGGER.critical('Cannot encrypt string: %s', exc)
        sys.exit(1)


def vault_format_ciphertext_yaml(b_ciphertext, indent=None, name=None):
    """
    Format a ciphertext to YAML compatible string
    """
    indent = indent or 10

    block_format_var_name = ''
    if name:
        block_format_var_name = '%s: ' % name

    block_format_header = '%s!vault |' % block_format_var_name
    lines = []
    vault_ciphertext = to_text(b_ciphertext)

    lines.append(block_format_header)
    for line in vault_ciphertext.splitlines():
        lines.append('%s%s' % (' ' * indent, line))

    yaml_ciphertext = '\n'.join(lines)
    return yaml_ciphertext


def load_schema(name):
    """
    Load a json schema
    """
    path = f'{os.path.dirname(__file__)}/schemas/{name}.json'
    schema = load_json(path)

    if not schema:
        LOGGER.critical('Cannot load schema at %s', path)
        sys.exit(1)

    return schema


def get_sample_file_paths():
    """
    Get list of every available sample files (YAML, etc.) with absolute paths
    """
    samples_dir = os.path.join(os.path.dirname(__file__), 'samples')
    return search_files(
        samples_dir, patterns=['config.yml', '*.yml.sample', 'README.md'], abs_path=True
    )


def validate(instance, schema):
    """
    Validate an instance under a given json schema
    """
    try:
        # Serialise vault encrypted objects to string
        schema_safe_inst = json.loads(json.dumps(instance, cls=AnsibleJSONEncoder))
        jsonschema.validate(instance=schema_safe_inst, schema=schema)
    except jsonschema.exceptions.ValidationError:
        LOGGER.critical('json object doesn\'t match schema %s', schema)
        sys.exit(1)


def delete_empty_keys(dic):
    """
    Deleting every key from a dictionary where the values are empty
    """
    return {k: v for k, v in dic.items() if v is not None}


def delete_keys_from_dict(dic, keys):
    """
    Delete specific keys from a nested dictionary
    """
    if not isinstance(dic, (dict, list)):
        return dic
    if isinstance(dic, list):
        return [v for v in (delete_keys_from_dict(v, keys) for v in dic) if v]
    # pylint: disable=C0325  # False positive on tuples
    return {
        k: v
        for k, v in ((k, delete_keys_from_dict(v, keys)) for k, v in dic.items())
        if k not in keys
    }


def silentremove(path):
    """
    Deleting file with no error message if the file not exists
    """
    LOGGER.debug('Removing file at %s', path)
    try:
        os.remove(path)
    except OSError as exc:

        # errno.ENOENT = no such file or directory
        if exc.errno != errno.ENOENT:
            raise


def search_files(search_dir, patterns=None, sort=False, abs_path=False):
    """
    Searching files in a specific directory that match a pattern
    """
    if patterns is None:
        patterns = ['*']
    files = []
    if os.path.isdir(search_dir):
        # Search files and sort if required
        p_files = []
        for pattern in patterns:
            p_files.extend(
                filter(os.path.isfile, glob.glob(os.path.join(search_dir, pattern)))
            )
        if sort:
            p_files.sort(key=os.path.getmtime, reverse=True)

        # Cut the whole paths, we only need the filenames
        files = list(map(lambda x: os.path.basename(x) if not abs_path else x, p_files))

    return files


def extract_log_attributes(log_file):
    """
    Extracting common properties from a log file name
    """
    LOGGER.debug('Extracting attributes from log file %s', log_file)
    target_id = 'unknown'
    tap_id = 'unknown'
    timestamp = datetime.utcfromtimestamp(0).isoformat()
    sync_engine = 'unknown'
    status = 'unknown'

    try:
        # Extract attributes from log file name
        log_attr = re.search(r'(.*)-(.*)-(.*)\.(.*)\.log\.(.*)', log_file)
        target_id = log_attr.group(1)
        tap_id = log_attr.group(2)
        timestamp = datetime.strptime(log_attr.group(3), '%Y%m%d_%H%M%S').isoformat()
        sync_engine = log_attr.group(4)
        status = log_attr.group(5)

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
        'status': status,
    }


def get_tap_property(tap, property_key, temp_dir=None):
    """
    Get a tap specific property value
    """
    tap_props_inst = tap_properties.get_tap_properties(tap, temp_dir)
    tap_props = tap_props_inst.get(tap.get('type'), tap_props_inst.get('DEFAULT', {}))

    return tap_props.get(property_key)


def get_tap_property_by_tap_type(tap_type, property_key):
    """
    Get a tap specific property value by a tap type.

    Some attributes cannot derived only by tap type. These
    properties might not be returned as expected.
    """
    tap_props_inst = tap_properties.get_tap_properties()
    tap_props = tap_props_inst.get(tap_type, tap_props_inst.get('DEFAULT', {}))

    return tap_props.get(property_key)


def get_tap_extra_config_keys(tap, temp_dir=None):
    """
    Get tap extra config property
    """
    return get_tap_property(tap, 'tap_config_extras', temp_dir)


def get_tap_stream_id(tap, database_name, schema_name, table_name):
    """
    Generate tap_stream_id in the same format as a specific
    tap generating it. They are not consistent.

    Stream id is the string that tha tap's discovery mode puts
    into the properties.json file
    """
    pattern = get_tap_property(tap, 'tap_stream_id_pattern')

    return (
        pattern.replace('{{database_name}}', f'{database_name}')
        .replace('{{schema_name}}', f'{schema_name}')
        .replace('{{table_name}}', f'{table_name}')
    )


def get_tap_stream_name(tap, database_name, schema_name, table_name):
    """
    Generate tap_stream_name in the same format as a specific
    tap generating it. They are not consistent.

    Stream name is the string that the tap puts into the output
    singer messages
    """
    pattern = get_tap_property(tap, 'tap_stream_name_pattern')

    return (
        pattern.replace('{{database_name}}', f'{database_name}')
        .replace('{{schema_name}}', f'{schema_name}')
        .replace('{{table_name}}', f'{table_name}')
    )


def get_tap_default_replication_method(tap):
    """
    Get the default replication method for a tap
    """
    return get_tap_property(tap, 'default_replication_method')


def get_fastsync_bin(venv_dir, tap_type, target_type):
    """
    Get the absolute path of a fastsync executable
    """
    source = tap_type.replace('tap-', '')
    target = target_type.replace('target-', '')
    fastsync_name = f'{source}-to-{target}'

    return os.path.join(venv_dir, 'pipelinewise', 'bin', fastsync_name)


def get_pipelinewise_python_bin(venv_dir: str) -> str:
    """
    Get the absolute path of a PPW python executable
    Args:
        venv_dir: path to the ppw virtual env

    Returns: path to python executable
    """
    return os.path.join(venv_dir, 'pipelinewise', 'bin', 'python')


# pylint: disable=redefined-builtin
def create_temp_file(suffix=None, prefix=None, dir=None, text=None):
    """
    Create temp file with parent directories if not exists
    """
    if dir:
        os.makedirs(dir, exist_ok=True)
    return tempfile.mkstemp(suffix, prefix, dir, text)


def find_errors_in_log_file(file, max_errors=10, error_pattern=None):
    """
    Find error lines in a log file

    Args:
        file: file to read
        max_errors: max number of errors to find
        error_pattern: Custom exception pattern

    Returns:
        List of error messages found in the file
    """
    # List of known exception patterns in logs
    known_error_patterns = re.compile(
        # PPW error log patterns
        r'CRITICAL|'
        r'EXCEPTION|'
        r'ERROR|'
        # Basic tap and target connector exception patterns
        r'pymysql\.err|'
        r'psycopg2\.*Error|'
        r'snowflake\.connector\.errors|'
        r'botocore\.exceptions\.|'
        # Generic python exceptions
        r'\.[E|e]xception|'
        r'\.[E|e]rror'
    )

    # Use known error patterns by default
    if not error_pattern:
        error_pattern = re.compile(known_error_patterns)

    errors = []
    if file and os.path.isfile(file):
        with open(file, encoding='utf-8') as file_object:
            for line in file_object:
                if len(re.findall(error_pattern, line)) > 0:
                    errors.append(line)

                    # Seek to the end of the file, if max_errors found
                    if len(errors) >= max_errors:
                        file_object.seek(0, 2)

    return errors


def generate_random_string(length: int = 8) -> str:
    """
    Generate cryptographically secure random strings
    Uses best practice from Python doc https://docs.python.org/3/library/secrets.html#recipes-and-best-practices
    Args:
        length: length of the string to generate
    Returns: random string
    """

    if length < 1:
        raise Exception('Length must be at least 1!')

    if 0 < length < 8:
        warnings.warn('Length is too small! consider 8 or more characters')

    return ''.join(
        secrets.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )
