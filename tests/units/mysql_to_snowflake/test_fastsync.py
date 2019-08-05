import os
import pytest

from pipelinewise.mysql_to_snowflake import utils

RESOURCES_DIR="{}/resources".format(os.path.dirname(__file__))


def test_tablename_to_dict():
    """Test identifying schema and table names from fully qualified table names"""

    # Mysql schema.table format
    assert utils.tablename_to_dict('my_schama.my_table') == \
        {"schema": 'my_schama', "name": 'my_table', "temp_name": "my_table_temp"}

    # Giving table name only should return mixed values
    # TODO: This is not great behaviour, Consolidate this function across every
    #       component
    assert utils.tablename_to_dict('my_table') == \
        {"schema": 'my_table', "name": None, "temp_name": "None_temp"}


def test_get_tables_from_properties():
    """..."""
    properties = utils.load_json("{}/properties.json".format(RESOURCES_DIR))