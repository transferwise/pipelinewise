import json
import os


# pylint: disable=too-many-instance-attributes, too-few-public-methods
class PartialSync2SFArgs:
    """Arguments for using in mysql to snowflake tests"""
    # pylint: disable=too-many-arguments
    def __init__(self, temp_test_dir, table='email',
                 start_value='FOO_START', end_value='FOO_END', state='state.json',
                 hard_delete=None, drop_target_table=False):
        resources_dir = f'{os.path.dirname(__file__)}/resources'
        config_dir = f'{resources_dir}/test_partial_sync'
        tap_config = self._load_json_config(f'{config_dir}/target_snowflake/tap_mysql/config.json')
        target_config = self._load_json_config(f'{config_dir}/tmp/target_config_tmp.json')
        transform_config = self._load_json_config(f'{config_dir}/target_snowflake/tap_mysql/transformation.json')
        properties_config = self._load_json_config(f'{config_dir}/target_snowflake/tap_mysql/properties.json')
        if hard_delete is not None:
            target_config['hard_delete'] = hard_delete

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
        self.drop_target_table = drop_target_table

    @staticmethod
    def _load_json_config(file_name):
        with open(file_name, 'r', encoding='utf8') as config_file:
            return json.load(config_file)


def get_argv_list(arguments_dict):
    """Get list of argv"""
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
