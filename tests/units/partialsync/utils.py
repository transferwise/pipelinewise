import json
import os

from argparse import Namespace
from unittest.mock import patch

from pipelinewise.fastsync.partialsync import mysql_to_snowflake, postgres_to_snowflake
from pipelinewise.fastsync.partialsync.utils import parse_args_for_partial_sync


# pylint: disable=too-many-instance-attributes, too-few-public-methods
class PartialSync2SFArgs:
    """Arguments for using in mysql to snowflake tests"""
    def __init__(self, temp_test_dir, table='email', start_value='FOO_START', end_value='FOO_END', state='state.json'):
        resources_dir = f'{os.path.dirname(__file__)}/resources'
        config_dir = f'{resources_dir}/test_partial_sync'
        tap_config = self._load_json_config(f'{config_dir}/target_snowflake/tap_mysql/config.json')
        target_config = self._load_json_config(f'{config_dir}/tmp/target_config_tmp.json')
        transform_config = self._load_json_config(f'{config_dir}/target_snowflake/tap_mysql/transformation.json')
        properties_config = self._load_json_config(f'{config_dir}/target_snowflake/tap_mysql/properties.json')

        self.table = f'{tap_config["dbname"]}.{table}'
        self.column = 'FOO_COLUMN'
        self.start_value = start_value
        self.end_value = end_value
        self.tap = tap_config
        self.target = target_config
        self.transform = transform_config
        self.temp_dir = temp_test_dir
        self.properties = properties_config
        self.state = state

    @staticmethod
    def _load_json_config(file_name):
        with open(file_name, 'r', encoding='utf8') as config_file:
            return json.load(config_file)


def run_mysql_to_snowflake(arguments_dict: dict) -> Namespace:
    """Running the mysql_to_snowflake module"""

    argv_list = _get_argv_list(arguments_dict)
    with patch('sys.argv', argv_list):
        args = parse_args_for_partial_sync(mysql_to_snowflake.REQUIRED_CONFIG_KEYS)
        mysql_to_snowflake.main()

    return args


def run_postgres_to_snowflake(arguments_dict: dict) -> Namespace:
    """Running PS to SF"""
    argv_list = _get_argv_list(arguments_dict)
    with patch('sys.argv', argv_list):
        args = parse_args_for_partial_sync(postgres_to_snowflake.REQUIRED_CONFIG_KEYS)
        postgres_to_snowflake.main()

    return args


def _get_argv_list(arguments_dict):
    argv_list = ['main']
    if arguments_dict.get('tap'):
        argv_list.extend(['--tap', arguments_dict['tap']])
    if arguments_dict.get('target'):
        argv_list.extend(['--target', arguments_dict['target']])
    if arguments_dict.get('table'):
        argv_list.extend(['--table', arguments_dict['table']])
    if arguments_dict.get('column'):
        argv_list.extend(['--column', arguments_dict['column']])
    if arguments_dict.get('start_value'):
        argv_list.extend(['--start_value', arguments_dict['start_value']])
    if arguments_dict.get('end_value'):
        argv_list.extend(['--end_value', arguments_dict['end_value']])
    if arguments_dict.get('temp_dir'):
        argv_list.extend(['--temp_dir', arguments_dict['temp_dir']])
    if arguments_dict.get('state'):
        argv_list.extend(['--state', arguments_dict['state']])

    return argv_list
