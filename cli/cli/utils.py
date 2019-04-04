import os
import sys
import json
import yaml
import logging

from datetime import date, datetime

from ansible.parsing.vault import VaultLib, get_file_vault_secret, is_encrypted_file
from ansible.parsing.yaml.loader import AnsibleLoader
from ansible.parsing.dataloader import DataLoader
from ansible.parsing.yaml.objects import AnsibleVaultEncryptedUnicode
from ansible.utils.unsafe_proxy import AnsibleUnsafe
from ansible.module_utils._text import to_text
from ansible.module_utils.common._collections_compat import Mapping

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


def load_json(path):
    try:
        logger.debug('Parsing file at {}'.format(path))
        if os.path.isfile(path):
            with open(path) as f:
                return json.load(f)
        else:
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


def delete_empty_keys(d):
    '''
    Deleting every key from a dictionary where the values are empty
    '''
    return {k: v for k, v in d.items() if v is not None}