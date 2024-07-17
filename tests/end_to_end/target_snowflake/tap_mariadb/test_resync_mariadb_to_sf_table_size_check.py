import os

from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB
from tests.end_to_end.helpers import tasks
from tests.end_to_end.target_snowflake import TEST_PROJECTS_DIR_PATH


TAP_ID = 'mariadb_to_sf'
TARGET_ID = 'snowflake'


def _create_ppw_config_file(table_byte):
    with open(f'{TEST_PROJECTS_DIR_PATH}/config.yml', 'w', encoding='utf-8') as config_file:
        config_file.write('allowed_resync_max_size:\n')
        config_file.write(f'  table_bytes: {table_byte}\n')

    tasks.run_command(f'pipelinewise import_config --dir {TEST_PROJECTS_DIR_PATH}')


class TestResyncMariaDBToSF(TapMariaDB):
    """Test Resync MariaDB to SF."""
    def setUp(self, *args, **kwargs):  # pylint: disable = unused-argument
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def tearDown(self):
        try:
            os.remove(f'{TEST_PROJECTS_DIR_PATH}/config.yml')
        except OSError:
            pass
        super().tearDown()

    def test_resync_mariadb_to_sf_if_table_size_greater_than_limit(self):  # pylint: disable = no-self-use
        """test resync mariadb to SF returns error 1 if table size is greater than the limit"""

        a_small_number = 3
        _create_ppw_config_file(table_byte=a_small_number)

        command = f'pipelinewise sync_tables --tap {TAP_ID} --target {TARGET_ID}'

        [return_code, _, _] = tasks.run_command(command)

        assert return_code == 1

    def test_resync_mariadb_to_sf_if_table_size_less_than_limit(self):  # pylint: disable = no-self-use
        """test resync mariadb to SF returns error if table size is less than the limit"""
        a_big_number = 100000000000
        _create_ppw_config_file(table_byte=a_big_number)

        command = f'pipelinewise sync_tables --tap {TAP_ID} --target {TARGET_ID}'
        [return_code, _, _] = tasks.run_command(command)

        assert return_code == 0

    def test_resync_mariadb_to_sf_if_table_size_greater_than_limit_and_force(self):  # pylint: disable = no-self-use
        """test resync mariadb to SF returns error if table size is greater than the limit and --force is used"""
        a_small_number = 3
        _create_ppw_config_file(table_byte=a_small_number)

        command = f'pipelinewise sync_tables --tap {TAP_ID} --target {TARGET_ID} --force'

        [return_code, _, _] = tasks.run_command(command)

        assert return_code == 0
